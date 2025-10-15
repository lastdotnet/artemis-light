use crate::collectors::NewBlock;
use crate::executors::mempool_executor::SubmitTxToMempool;
use alloy::eips::BlockNumberOrTag;
use alloy::rpc::types::Transaction;
use anyhow::Result;
use async_trait::async_trait;
use std::pin::Pin;
use tokio_stream::Stream;

/// A stream of events emitted by a [Collector].
pub type CollectorStream<'a, E> = Pin<Box<dyn Stream<Item = E> + Send + 'a>>;

pub enum Offset {
    Genesis,
    Latest,
    Number(u64),
}

impl From<Offset> for BlockNumberOrTag {
    fn from(value: Offset) -> Self {
        match value {
            Offset::Genesis => BlockNumberOrTag::Earliest,
            Offset::Latest => BlockNumberOrTag::Latest,
            Offset::Number(n) => BlockNumberOrTag::Number(n),
        }
    }
}

impl TryFrom<Offset> for u64 {
    type Error = anyhow::Error;

    fn try_from(value: Offset) -> Result<Self, Self::Error> {
        match value {
            Offset::Genesis => Ok(0),
            Offset::Latest => Err(anyhow::anyhow!("Latest offset cannot be converted to u64")),
            Offset::Number(n) => Ok(n),
        }
    }
}

/// Collector trait, which defines a source of events.
#[async_trait]
pub trait Collector<E>: Send + Sync {
    /// Returns the core event stream for the collector.

    async fn subscribe(&self) -> Result<CollectorStream<'_, E>>;

    #[deprecated(since = "0.1.0", note = "Use subscribe instead")]
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E>> {
        self.subscribe().await
    }
}

#[async_trait]
pub trait Archive<E>: Send + Sync {
    async fn replay_from(&self, n: u64) -> anyhow::Result<CollectorStream<'_, E>>;
}

#[async_trait]
pub trait ArchiveCollector<E>: Send + Sync {
    async fn subscribe_from(&self, from: u64) -> Result<CollectorStream<'_, E>>;
}

/// Strategy trait, which defines the core logic for each opportunity.
#[async_trait]
pub trait Strategy<E, A>: Send + Sync {
    /// Sync the initial state of the strategy if needed, usually by fetching
    /// onchain data.
    async fn sync_state(&mut self) -> Result<()>;

    /// Process an event, and return an action if needed.
    async fn process_event(&mut self, event: E) -> Vec<A>;
}

/// Executor trait, responsible for executing actions returned by strategies.
#[async_trait]
pub trait Executor<A>: Send + Sync {
    /// Execute an action.
    async fn execute(&self, action: A) -> Result<()>;
}

#[async_trait]
pub trait Storage<E> {
    async fn write(&self, record: &E) -> anyhow::Result<E>;
    async fn replay_from(&self, n: u64) -> anyhow::Result<CollectorStream<'_, E>>;
    async fn head(&self) -> anyhow::Result<u64>;
}
/// ExecutorMap is a wrapper around an [Executor] that maps incoming
/// actions to a different type.
pub struct ExecutorMap<A, F> {
    executor: Box<dyn Executor<A>>,
    f: F,
}

impl<A, F> ExecutorMap<A, F> {
    pub fn new(executor: Box<dyn Executor<A>>, f: F) -> Self {
        Self { executor, f }
    }
}
#[async_trait]
impl<A1, A2, F> Executor<A1> for ExecutorMap<A2, F>
where
    A1: Send + Sync + 'static,
    A2: Send + Sync + 'static,
    F: Fn(A1) -> Option<A2> + Send + Sync + Clone + 'static,
{
    async fn execute(&self, action: A1) -> Result<()> {
        let action = (self.f)(action);
        match action {
            Some(action) => self.executor.execute(action).await,
            None => Ok(()),
        }
    }
}

/// Convenience enum containing all the events that can be emitted by collectors.
pub enum Events {
    NewBlock(NewBlock),
    Transaction(Box<Transaction>),
}

/// Convenience enum containing all the actions that can be executed by executors.
pub enum Actions {
    //    FlashbotsBundle(FlashbotsBundle),
    SubmitTxToMempool(SubmitTxToMempool),
}
