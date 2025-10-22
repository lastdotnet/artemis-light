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
impl<A, E, M> Executor<A> for ExecutorInstrument<E, M>
where
    E: Executor<A> + 'static,
    M: Metrics<E::Output> + Send + Sync + 'static,
    A: Send + Sync + 'static,
{
    type Output = E::Output;
    async fn execute(&self, action: A) -> anyhow::Result<Option<Self::Output>> {
        let state = self.executor.execute(action).await?;

        if let Some(ref state) = state
            && let Err(e) = self.metrics.collect_metrics(state).await {
                tracing::warn!("error collecting metrics: {}", e);
            }
        Ok(state)
    }
}
