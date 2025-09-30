use crate::types::{Collector, CollectorStream};
use anyhow::Result;
use async_trait::async_trait;
use jsonrpsee::core::DeserializeOwned;

pub struct Merge<C1, C2> {
    this: C1,
    other: C2,
}

impl<C1, C2> Merge<C1, C2> {
    pub fn new(this: C1, other: C2) -> Self {
        Self { this, other }
    }
}

#[async_trait]
impl<C1, C2, E> Collector<E> for Merge<C1, C2>
where
    C1: Collector<E> + Send + Sync + 'static,
    C2: Collector<E> + Send + Sync + 'static,
    E: Send + Sync + DeserializeOwned + 'static,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, E>> {
        let this_stream = self.this.subscribe().await?;
        let other_stream = self.other.subscribe().await?;

        let merged =
            Box::pin(futures::stream::select(this_stream, other_stream)) as CollectorStream<'_, E>;
        // let merged = Box::pin(this_stream.select(other_stream)) as CollectorStream<'_, E>;
        Ok(merged)
    }
}