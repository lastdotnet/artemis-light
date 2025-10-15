use crate::types::{Archive, Collector, CollectorStream};
use async_trait::async_trait;
use futures::StreamExt;

pub struct LoadFrom<C> {
    collector: C,
    n: u64,
}

impl<C> LoadFrom<C> {
    pub fn new(collector: C, n: u64) -> Self {
        Self { collector, n }
    }
}

#[async_trait]
impl<E, C> Collector<E> for LoadFrom<C>
where
    C: Collector<E> + Archive<E> + Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    async fn subscribe(&self) -> anyhow::Result<CollectorStream<'_, E>> {
        let archive = self.collector.replay_from(self.n).await?;
        let stream = self.collector.subscribe().await?;
        Ok(Box::pin(archive.chain(stream)) as CollectorStream<'_, E>)
    }
}

#[async_trait]
impl<E: Send + Sync + 'static, C> Archive<E> for LoadFrom<C>
where
    C: Archive<E> + Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    async fn replay_from(&self, n: u64) -> anyhow::Result<CollectorStream<'_, E>> {
        self.collector.replay_from(n).await
    }
}
