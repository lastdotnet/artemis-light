use std::fmt::Display;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt, ready};
use tokio::pin;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::{self, Receiver, Sender};
use tokio::task::JoinSet;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_util::sync::{CancellationToken, ReusableBoxFuture};
use tracing::{error, info, warn};

use crate::types::{Collector, Executor, Strategy};

use thiserror::Error;

#[derive(Debug)]
pub enum Channel {
    Events,
    Actions,
}

impl Display for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Channel::Events => "events",
                Channel::Actions => "actions",
            }
        )
    }
}

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("{0} sender closed")]
    SenderClosed(Channel),
    #[error("all {0} receivers dropped")]
    ReceiverDropped(Channel),
    #[error("error syncing state: {0}")]
    SyncState(#[from] Box<dyn std::error::Error>),
    #[error("event processing error: {0}")]
    ProcessEvent(#[source] Box<dyn std::error::Error>),
    #[error("error executing action: {0}")]
    Execute(#[source] anyhow::Error),
}
/// The main engine of Artemis. This struct is responsible for orchestrating the
/// data flow between collectors, strategies, and executors.
pub struct Engine<E, A, R = ()> {
    /// The set of collectors that the engine will use to collect events.
    collectors: Vec<Box<dyn Collector<E>>>,

    /// The set of strategies that the engine will use to process events.
    strategies: Vec<Box<dyn Strategy<E, A>>>,

    /// The set of executors that the engine will use to execute actions.
    executors: Vec<Box<dyn Executor<A, R>>>,

    /// The capacity of the event channel.
    event_channel_capacity: usize,

    /// The capacity of the action channel.
    action_channel_capacity: usize,
}

impl<E, A, R> Default for Engine<E, A, R> {
    fn default() -> Self {
        Self {
            collectors: vec![],
            strategies: vec![],
            executors: vec![],
            event_channel_capacity: 512,
            action_channel_capacity: 512,
        }
    }
}

impl<E, A, R> Engine<E, A, R> {
    pub fn new(
        collectors: Vec<Box<dyn Collector<E>>>,
        strategies: Vec<Box<dyn Strategy<E, A>>>,
        executors: Vec<Box<dyn Executor<A, R>>>,
        event_channel_capacity: usize,
        action_channel_capacity: usize,
    ) -> Self {
        Self {
            collectors,
            strategies,
            executors,
            event_channel_capacity,
            action_channel_capacity,
        }
    }

    pub fn with_event_channel_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = capacity;
        self
    }

    pub fn with_action_channel_capacity(mut self, capacity: usize) -> Self {
        self.action_channel_capacity = capacity;
        self
    }
}
struct BroadcastStream<T>(
    ReusableBoxFuture<'static, (Result<T, RecvError>, broadcast::Receiver<T>)>,
);

impl<T: 'static + Clone + Send> BroadcastStream<T> {
    /// Create a new `BroadcastStream`.
    pub fn new(rx: Receiver<T>) -> Self {
        BroadcastStream(ReusableBoxFuture::new(make_future(rx)))
    }
}

async fn make_future<T: Clone>(mut rx: Receiver<T>) -> (Result<T, RecvError>, Receiver<T>) {
    let result = rx.recv().await;
    (result, rx)
}

impl<T: 'static + Clone + Send> Stream for BroadcastStream<T> {
    type Item = Result<T, BroadcastStreamRecvError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (result, rx) = ready!(self.0.poll(cx));
        self.0.set(make_future(rx));
        match result {
            Ok(item) => Poll::Ready(Some(Ok(item))),
            Err(RecvError::Closed) => Poll::Ready(None),
            Err(RecvError::Lagged(n)) => {
                Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(n))))
            }
        }
    }
}

impl<E, A, R> Engine<E, A, R>
where
    E: Send + Sync + Clone + std::fmt::Debug + 'static,
    A: Send + Sync + Clone + std::fmt::Debug + 'static,
    R: Send + Clone + std::fmt::Debug + 'static,
{
    /// Adds a collector to be used by the engine.
    pub fn add_collector(&mut self, collector: Box<dyn Collector<E>>) {
        self.collectors.push(collector);
    }

    /// Adds a strategy to be used by the engine.
    pub fn add_strategy(&mut self, strategy: Box<dyn Strategy<E, A>>) {
        self.strategies.push(strategy);
    }

    /// Adds an executor to be used by the engine.
    pub fn add_executor(&mut self, executor: Box<dyn Executor<A, R>>) {
        self.executors.push(executor);
    }

    fn strip_and_log_error<T>(
        stream: std::pin::Pin<
            Box<dyn futures::Stream<Item = Result<T, BroadcastStreamRecvError>> + Send>,
        >,
    ) -> std::pin::Pin<Box<dyn futures::Stream<Item = T> + Send + 'static>>
    where
        T: Send + Sync + Clone + 'static,
    {
        Box::pin(stream.filter_map(move |item| async {
            item.inspect_err(|BroadcastStreamRecvError::Lagged(n)| {
                warn!("action receiver skipped {n} messages")
            })
            .ok()
        }))
    }

    /// The core run loop of the engine. This function will spawn a thread for
    /// each collector, strategy, and executor. It will then orchestrate the
    /// data flow between them.
    pub async fn run(mut self, _cancel_token: CancellationToken) -> Result<JoinSet<()>, EngineError> {
        let (event_sender, _): (Sender<E>, _) = broadcast::channel(self.event_channel_capacity);
        let (action_sender, _): (Sender<A>, _) = broadcast::channel(self.action_channel_capacity);

        let mut set = JoinSet::new();

        // Spawn executors in separate threads.
        for executor in self.executors {
            let receiver = action_sender.subscribe();
            set.spawn(async move {
                info!("starting executor... ");
                Self::strip_and_log_error(Box::pin(BroadcastStream::new(receiver))
                    as Pin<
                        Box<dyn futures::Stream<Item = Result<A, BroadcastStreamRecvError>> + Send>,
                    >)
                .fold(executor, |mut executor, action| async {
                    if let Err(e) = executor.execute(action).await {
                        error!("error executing action: {}", e);
                    }
                    executor
                })
                .await;
            });
        }

        let strategies = futures::stream::iter(self.strategies.iter_mut());

        strategies.map(|strategy| async {
            strategy.sync_state().await;
            strategy
        });

        // Spawn strategies in separate threads.
        for mut strategy in self.strategies {
            let event_receiver = event_sender.subscribe();
            let action_sender = action_sender.clone();
            strategy.sync_state().await?;

            set.spawn(async move {
                info!("starting strategy... ");
                let event_stream = BroadcastStream::new(event_receiver);
                let mut event_stream = Self::strip_and_log_error(Box::pin(event_stream));

                while let Some(event) = event_stream.next().await {
                    if let Ok(actions) = strategy.process_event(event).await {
                        actions
                            .filter_map(|maybe_action| async move { maybe_action.ok() })
                            .for_each(|action| async {
                                match action_sender.send(action) {
                                    Ok(_) => {}
                                    Err(e) => error!("error sending action: {}", e),
                                }
                            })
                            .await;
                    }
                }
            });
        }

        // Spawn collectors in separate threads.
        for collector in self.collectors {
            let event_sender = event_sender.clone();

            set.spawn(async move {
                info!("starting collector... ");
                let mut event_stream = collector.get_event_stream().await.unwrap();
                while let Some(event) = event_stream.next().await {
                    match event_sender.send(event) {
                        Ok(_) => {}
                        Err(e) => error!("error sending event: {}", e),
                    }
                }
            });
        }
        Ok(set)
    }
}
