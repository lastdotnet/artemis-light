use crate::types::{Collector, CollectorStream};
use alloy::primitives::LogData;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use jsonrpsee::core::DeserializeOwned;
use std::marker::PhantomData;

pub struct Enumerate<E> {
    collector: Box<dyn Collector<Log>>,
    e: PhantomData<E>,
}
impl<E> Enumerate<E> {
    pub fn new(collector: Box<dyn Collector<Log>>) -> Self {
        Self {
            collector,
            e: PhantomData,
        }
    }
}

impl<E: SolEvent> Enumerate<E> {
    fn decode_log(log: &Log) -> Result<E> {
        let log_data: &LogData = log.as_ref();
        E::decode_raw_log(log_data.topics().iter().copied(), &log_data.data)
            .map_err(anyhow::Error::msg)
    }
}

#[async_trait]
impl<E> Collector<(u64, E)> for Enumerate<E>
where
    E: SolEvent + Send + Sync + DeserializeOwned + 'static,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, (u64, E)>> {
        let stream = self.collector.subscribe().await?;
        let enumerated = stream.filter_map(|log| async move {
            let e = Self::decode_log(&log).ok();
            let block_number = log.block_number;
            match (e, block_number) {
                (Some(e), Some(block_number)) => Some((block_number, e)),
                _ => None,
            }
        });
        Ok(Box::pin(enumerated))
    }
}
