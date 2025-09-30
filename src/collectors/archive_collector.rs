use std::marker::PhantomData;

use crate::types::{Collector, CollectorStream};
use alloy::{
    contract::Event, network::Ethereum, providers::Provider, rpc::types::Filter,
    sol_types::SolEvent,
};
use anyhow::{Ok, Result};
use async_trait::async_trait;
use futures::stream;
use tokio_stream::StreamExt;

pub struct ArchiveCollector<P, E> {
    provider: P,
    filter: Filter,
    chunk_size: u64,
    from: u64,
    e: PhantomData<E>,
}

impl<'a, P, E> ArchiveCollector<P, E>
where
    P: Provider,
    E: SolEvent + Send + Sync + Clone + 'a,
{
    pub fn new(event: Event<P, E>, from: u64, chunk_size: u64) -> Self {
        let filter = event.filter;
        let provider = event.provider;
        Self {
            provider,
            filter,
            chunk_size,
            from,
            e: PhantomData,
        }
    }

    pub async fn load(&self, from: u64, to: u64) -> Result<CollectorStream<'a, E>> {
        let mut events = vec![];
        let (_, chunk) = self.load_chunked(from, to).await?;
        events.extend(chunk);
        let stream = stream::iter(events);
        Ok(Box::pin(stream))
    }

    async fn load_chunked(&self, from: u64, to: u64) -> Result<(u64, Vec<E>)> {
        let mut last_offset = from;
        let mut events = vec![];

        for offset in (from..to).step_by(self.chunk_size as usize) {
            let to = (from + self.chunk_size).min(to);
            let chunk = self.load_chunk(offset, to).await?;
            last_offset = to;
            events.extend(chunk);
        }
        Ok((last_offset, events))
    }

    async fn load_chunk(&self, from: u64, to: u64) -> Result<Vec<E>> {
        let filter = self.filter.clone().from_block(from).to_block(to);
        let event = Event::<&P, E, Ethereum>::new(&self.provider, filter);
        let events = event.query().await?;
        Ok(events
            .into_iter()
            .map(|(e, _)| e.clone())
            .collect::<Vec<_>>())
    }
}

/// Implementation of the [Collector](Collector) trait for the [EventCollector](EventCollector).
#[async_trait]
impl<'a, P, E> Collector<E> for ArchiveCollector<P, E>
where
    P: Provider,
    E: SolEvent + Send + Sync + Clone + 'a,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, E>> {
        let event = Event::<&P, E, Ethereum>::new(&self.provider, self.filter.clone());
        let to = self.provider.get_block_number().await?;
        let load_stream = self.load(self.from, to).await?;
        let subscription_stream = event
            .watch()
            .await?
            .into_stream()
            .filter_map(|el| el.map(|(e, _)| e).ok());

        Ok(Box::pin(load_stream.chain(subscription_stream)))
    }
}
