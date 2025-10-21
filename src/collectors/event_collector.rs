use crate::{collectors::RpcConfig, types::{Archive, Collector, CollectorStream}};
use alloy::{
    contract::Event,
    providers::Provider,
    rpc::types::{Filter, Log},
    sol_types::SolEvent,
};
use anyhow::Result;
use async_trait::async_trait;
use futures::stream;
use std::{marker::PhantomData, sync::Arc};
use tokio_stream::StreamExt;

/// A collector that listens for new blockchain event logs based on a [Event],
/// and generates a stream of events of type `E`.
pub struct EventCollector<P, E> {
    filter: Filter,
    provider: Arc<P>,
    _e: PhantomData<E>,
}

pub struct EventAux<P> {
    _p: PhantomData<P>,
}

impl<P: Provider> EventAux<P> {
    pub fn new_event<E: SolEvent>(provider: Arc<P>, filter: Filter) -> Event<Arc<P>, E> {
        Event::<Arc<P>, E>::new(provider, filter)
    }
}

impl<P, E> EventCollector<P, E>
where
    P: Provider,
    E: SolEvent + Send + Sync + std::fmt::Debug,
{
    pub fn new(event: Event<P, E>) -> Self {
        Self {
            filter: event.filter,
            provider: Arc::new(event.provider),
            _e: PhantomData,
        }
    }

    pub async fn load(&self, start_block: u64, chunk_size: Option<u64>) -> Result<CollectorStream<'_, (E, Log)>> {
        // TODO: Binary search for the chunk size
        let end_block = self.provider.get_block_number().await?;
        let chunk_size = chunk_size.unwrap_or(end_block - start_block);
        let mut events = vec![];
        let (_, chunk) = self.load_chunks(start_block, end_block, chunk_size).await?;
        events.extend(chunk);
        let stream = stream::iter(events);
        Ok(Box::pin(stream))
    }

    async fn load_chunks(
        &self,
        from: u64,
        to: u64,
        chunk_size: u64,
    ) -> Result<(u64, Vec<(E, Log)>)> {
        let mut last_offset = from;
        let mut events = vec![];

        for offset in (from..to).step_by(chunk_size as usize) {
            let to = (offset + chunk_size).min(to);
            println!("Loading chunk from: {} to: {}", offset, to);
            let chunk = self.load_chunk(offset, to).await?;
            chunk.iter().for_each(|(e, log)| {
                println!("event: {:?}", (e, log.block_number.unwrap_or_default()))
            });
            last_offset = to;
            events.extend(chunk);
        }
        // let last_offset = to;
        Ok((last_offset, events))
    }

    fn select(&self, from: u64, to: u64) -> Event<&P, E> {
        let mut filter = self.filter.clone();
        // AWL: The filter statement is inclusive so we need to subtract 1 from the to block
        filter = filter.from_block(from).to_block(to - 1);
        Event::<&P, E>::new(&self.provider, filter)
    }

    async fn load_chunk(&self, from: u64, to: u64) -> Result<Vec<(E, Log)>> {
        let event = self.select(from, to);
        event.query().await.map_err(anyhow::Error::from)
    }
}

/// Implementation of the [Collector](Collector) trait for the [EventCollector](EventCollector).
#[async_trait]
impl<P, E> Collector<(E, Log)> for EventCollector<P, E>
where
    P: Provider,
    E: SolEvent + Send + Sync,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, (E, Log)>> {
        let event = Event::<&P, E>::new(&self.provider, self.filter.clone());
        let stream = event.watch().await?.into_stream();
        let stream = stream.filter_map(|el| el.ok());
        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl<P, E> Archive<(E, Log)> for EventCollector<P, E>
where
    P: Provider,
    E: SolEvent + Send + Sync + std::fmt::Debug,
{
    async fn replay_from(&self, n: u64, chunk_size: Option<u64>) -> Result<CollectorStream<'_, (E, Log)>> {
        let historical = self.load(n, chunk_size).await?;
        Ok(Box::pin(historical))
    }
}
