use std::sync::Arc;

use crate::types::{Archive, Collector, CollectorStream};
use async_trait::async_trait;
use futures::StreamExt;

pub struct LoadFrom<C> {
    collector: Arc<C>,
    n: u64,
    chunk_size: Option<u64>,
}

impl<C> LoadFrom<C> {
    pub fn new(collector: Arc<C>, n: u64, chunk_size: Option<u64>) -> Self {
        Self { collector, n, chunk_size }
    }
}

#[async_trait]
impl<E, C> Collector<E> for LoadFrom<C>
where
    C: Collector<E> + Archive<E> + Send + Sync,
    E: Send + Sync + 'static,
{
    async fn subscribe(&self) -> anyhow::Result<CollectorStream<'_, E>> {
        let archive = self.collector.replay_from(self.n, self.chunk_size).await?;
        let stream = self.collector.subscribe().await?;
        Ok(Box::pin(archive.chain(stream)))
    }
}

#[async_trait]
impl<E, C> Archive<E> for LoadFrom<C>
where
    C: Archive<E> + Send + Sync,
    E: Send + Sync,
{
    async fn replay_from(&self, n: u64, chunk_size: Option<u64>) -> anyhow::Result<CollectorStream<'_, E>> {
        let chunk_size = chunk_size.or(self.chunk_size);
        self.collector.replay_from(n, chunk_size).await
    }
}
