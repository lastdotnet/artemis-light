use std::{marker::PhantomData, sync::Arc};

use crate::types::{Archive, Collector, CollectorStream};
use alloy::{
    contract::Event, network::Ethereum, providers::Provider, rpc::types::Filter,
    sol_types::SolEvent,
};
use anyhow::{Ok, Result};
use async_trait::async_trait;
use futures::stream;
use std::fmt::Debug;
use tokio_stream::StreamExt;
pub struct ArchiveCollector<P, E> {
    provider: Arc<P>,
    filter: Filter,
    chunk_size: u64,
    from: u64,
    e: PhantomData<E>,
}

pub trait EventType<E, P> {
    type Event: SolEvent;
    type Provider: Provider;
}

impl<P, E> ArchiveCollector<P, E>
where
    P: Provider,
    E: SolEvent + Send + Sync + Clone,
{
    pub fn new(event: Event<P, E>, from: u64, chunk_size: u64) -> Self {
        let provider = Arc::new(event.provider);
        let filter = event.filter;  
        Self { provider, filter, chunk_size, from, e: PhantomData }
    }

    pub async fn load(&self, from: u64, to: u64) -> Result<CollectorStream<'_, E>> {
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
            println!("Loading chunk from: {} to: {}", offset, to);
            let chunk = self.load_chunk(offset, to).await?;
            println!("#Chunks: {:?}", chunk.len());
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
impl<P, E> Collector<E> for ArchiveCollector<P, E>
where
    P: Provider,
    E: SolEvent + Send + Sync + Clone,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, E>> {
        let event = Event::<&P, E, Ethereum>::new(&self.provider, self.filter.clone());
        let stream = event
            .watch()
            .await?
            .into_stream()
            .filter_map(|el| el.map(|(e, _)| e).ok());
        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl<P, E> Archive<E> for ArchiveCollector<P, E>
where
    P: Provider,
    E: SolEvent + Send + Sync + Clone + Debug,
{
    async fn replay_from(&self, n: u64) -> Result<CollectorStream<'_, E>> {
        let provider = Arc::clone(&self.provider);
        let to = provider.get_block_number().await?;
        let historical = self.load(n, to).await?;
        Ok(Box::pin(historical))
    }
}

// trait Foo<E>: Archive<E> + Collector<E> {}

// impl<E, T> Foo<E> for T where T: Archive<E> + Collector<E> {

// }

// fn bar<E, C: Archive<E>>(c: C) {}
// fn baz<E, C: Foo<E>>(c: C) {
//     foo(c);
// }

// impl<T, E> Collector<E> for T where T: Archive<E> {}
