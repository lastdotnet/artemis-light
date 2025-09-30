use crate::types::{Collector, CollectorStream};
use alloy::{contract::Event, providers::Provider, sol_types::SolEvent};
use anyhow::Result;
use async_trait::async_trait;
use tokio_stream::StreamExt;

/// A collector that listens for new blockchain event logs based on a [Event],
/// and generates a stream of events of type `(E, u64)` - the event and the block number of the block that produced it. 
pub struct IndexedEventCollector<P, E> {
    event: Event<P, E>,
}

impl<P, E> IndexedEventCollector<P, E> {
    pub fn new(event: Event<P, E>) -> Self {
        Self { event }
    }
}

/// Implementation of the [Collector](Collector) trait for the [EventCollector](EventCollector).
#[async_trait]
impl<P, E> Collector<(E, u64)> for IndexedEventCollector<P, E>
where
    P: Provider,
    E: SolEvent + Send + Sync,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, (E, u64)>> {
        let stream = self.event.watch().await?.into_stream();
        let stream = stream.filter_map(|el| {
            el.ok()
                .and_then(|(e, log)| log.block_number.map(|n| (e, n)))
        });
        Ok(Box::pin(stream))
    }
}