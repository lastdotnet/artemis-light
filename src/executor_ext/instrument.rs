use async_trait::async_trait;

use crate::types::{Executor, Metrics};

pub struct ExecutorInstrument<E, M> {
    executor: E,
    metrics: M,
}

impl<E, M> ExecutorInstrument<E, M> {
    pub fn new(executor: E, metrics: M) -> Self {
        Self { executor, metrics }
    }
}

#[async_trait]
impl<A, R, E, M> Executor<A, R> for ExecutorInstrument<E, M>
where
    E: Executor<A, R> + 'static,
    M: Metrics<R> + Send + Sync + 'static,
    A: Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    async fn execute(&mut self, action: A) -> anyhow::Result<Option<R>> {
        match self.executor.execute(action).await {
            Ok(Some(result)) => {
                let _ = self.metrics.collect_metrics(Ok(&result)).await;
                Ok(Some(result))
            }
            Err(err) => {
                let _ = self.metrics.collect_metrics(Err(&err)).await;
                Err(err)
            }
            _ => Ok(None),
        }
    }
}
