use crate::types::{Collector, CollectorStream};
use alloy::{
    providers::Provider,
    rpc::types::{Filter, Log},
    sol_types::SolEvent,
};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio_stream::StreamExt;

/// A collector that listens for new blockchain event logs based on a [Filter],
/// and generates a stream of [events](Log).
pub struct LogCollector<M> {
    provider: Arc<M>,
    filter: Filter,
}

impl<M> LogCollector<M> {
    pub fn new(provider: Arc<M>, filter: Filter) -> Self {
        Self { provider, filter }
    }
}

/// Implementation of the [Collector](Collector) trait for the [LogCollector](LogCollector).
#[async_trait]
impl<M> Collector<Log> for LogCollector<M>
where
    M: Provider,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, Log>> {
        let stream = self.provider.subscribe_logs(&self.filter).await?;
        let stream = stream.into_stream().filter_map(Some);
        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl<E, M> Collector<(E, Log)> for LogCollector<M>
where
    M: Provider,
    E: SolEvent,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, (E, Log)>> {
        let stream = self.provider.subscribe_logs(&self.filter).await?;
        let stream = stream.into_stream().filter_map(|log| {
            let event = E::decode_log(&log.inner).ok();
            event.map(|e| (e.data, log))
        });
        Ok(Box::pin(stream))
    }
}
