use async_trait::async_trait;
use futures::StreamExt;

use crate::types::{ActionStream, Metrics, Strategy};
pub struct StrategyInstrument<S, M> {
    strategy: S,
    metrics: M,
    ignore_errors: bool,
}

impl<S, M: Metrics<S>> StrategyInstrument<S, M> {
    pub fn new(strategy: S, metrics: M, ignore_errors: bool) -> Self {
        Self {
            strategy,
            metrics,
            ignore_errors,
        }
    }
}

#[async_trait]
impl<E, A, S, M> Strategy<E, A> for StrategyInstrument<S, M>
where
    S: Strategy<E, A>,
    M: Metrics<A> + Send + Sync,
    E: Send + Sync + 'static,
    A: Send + Sync + Clone + 'static,
{
    async fn process_event(&mut self, event: E) -> anyhow::Result<ActionStream<'_, A>> {
        let res = self.strategy.process_event(event).await;

        match res {
            Ok(actions) => {
                let actions = actions.then(|action| async {
                    self.metrics
                        .collect_metrics::<anyhow::Error>(&Ok(action.clone()))
                        .await;
                    action
                });
                Ok(Box::pin(actions))
            }
            ref e @ Err(err) => {
                let err = Err(err);
                self.metrics.collect_metrics(e).await;
                err
            }
        }
    }

    async fn sync_state(&mut self) -> anyhow::Result<()> {
        let res = self.strategy.sync_state().await;
        match res {
            Ok(_) => Ok(()),
            Err(err) => {
                if let Err(inner) = self.metrics.collect_metrics(&Err(err)).await {
                    tracing::error!("error collecting metrics: {inner}");
                }
                // Err(err)
                todo!()
            }
        }
    }
}
