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
impl<A, E, M, R: Default, Err> Executor<A, R, Err> for ExecutorInstrument<E, M>
where
    E: Executor<A, R, Err> + 'static,
    M: Metrics<R, Err> + Send + Sync + 'static,
    A: Send + Sync + 'static,
    R: Sync + Send,
    Err: Send + Sync,
{
    async fn execute(&mut self, action: A) -> Result<R, Err> {
        let result = self.executor.execute(action).await;
        let _ = self.metrics.collect_metrics(&result).await;
        result
    }
}
