use crate::types::{Collector, CollectorStream};
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use jsonrpsee::core::DeserializeOwned;

/// CollectorMap is a wrapper around a [Collector] that maps outgoing
/// events to a different type.
pub struct Map<E, F> {
    collector: Box<dyn Collector<E>>,
    f: F,
}

impl<E, F> Map<E, F> {
    pub fn new(collector: Box<dyn Collector<E>>, f: F) -> Self {
        Self { collector, f }
    }
}

#[async_trait]
impl<E1, E2, F> Collector<E2> for Map<E1, F>
where
    E1: Send + Sync + DeserializeOwned + 'static,
    E2: Send + Sync + DeserializeOwned + 'static,
    F: Fn(E1) -> E2 + Send + Sync + Clone + 'static,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, E2>> {
        let stream = self.collector.subscribe().await?;
        let f = self.f.clone();
        let stream = stream.map(f);
        Ok(Box::pin(stream))
    }
}
