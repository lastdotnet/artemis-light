use crate::types::{Collector, CollectorStream};
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use jsonrpsee::core::DeserializeOwned;

/// FilterCollectorMap is a wrapper around a [Collector] that maps outgoing
/// events to a different type.
pub struct FilterMap<E, F> {
    collector: Box<dyn Collector<E>>,
    f: F,
}
impl<E, F> FilterMap<E, F> {
    pub fn new(collector: Box<dyn Collector<E>>, f: F) -> Self {
        Self { collector, f }
    }
}

#[async_trait]
impl<E1, E2, F> Collector<E2> for FilterMap<E1, F>
where
    E1: Send + Sync + DeserializeOwned + 'static,
    E2: Send + Sync + DeserializeOwned + 'static,
    F: FnMut(E1) -> Option<E2> + Send + Sync + Clone + 'static + Copy,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, E2>> {
        let stream = self.collector.subscribe().await?;
        let mut f = self.f.clone();
        let stream = stream.filter_map(move |event| async move { f(event) });
        Ok(Box::pin(stream))
    }
}
