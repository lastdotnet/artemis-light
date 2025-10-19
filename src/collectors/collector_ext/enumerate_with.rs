use crate::collectors::collector_ext::Archive;
use crate::types::{Collector, CollectorStream};
use alloy::rpc::types::Log;
use anyhow::Result;
use async_trait::async_trait;
use std::marker::PhantomData;
use tokio_stream::StreamExt;

pub trait WithIndex {
    type Index;
    fn index(&self) -> Self::Index;
}

impl WithIndex for Log {
    type Index = Option<u64>;

    fn index(&self) -> Self::Index {
        self.block_number
    }
}

impl<E> WithIndex for (E, Log) {
    type Index = Option<u64>;

    fn index(&self) -> Self::Index {
        self.1.block_number
    }
}

pub struct EnumerateWith<E, F, C> {
    collector: C,
    f: F,
    e: PhantomData<E>,
}

impl<E, F, C> EnumerateWith<E, F, C> {
    pub fn new(collector: C, f: F) -> Self {
        Self {
            collector,
            f,
            e: PhantomData,
        }
    }
}

#[async_trait]
impl<E, C, F, I> Collector<(I, E)> for EnumerateWith<E, F, C>
where
    E: Send + Sync + WithIndex<Index = I>,
    C: Collector<E>,
    I: Send + Sync + 'static,
    F: Fn(&E) -> I + Clone + Send + Sync,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, (I, E)>> {
        let stream = self.collector.subscribe().await?;
        let f = self.f.clone();
        let enumerated = stream.map(move |e| (f(&e), e));
        Ok(Box::pin(enumerated))
    }
}

#[async_trait]
impl<E, C, F, I> Archive<(I, E)> for EnumerateWith<E, F, C>
where
    C: Archive<E> + 'static,
    E: Send + Sync + WithIndex + 'static,
    F: Fn(&E) -> I + Clone + Send + Sync,
{
    async fn replay_from(&self, n: u64, chunk_size: Option<u64>) -> anyhow::Result<CollectorStream<'_, (I, E)>> {
        let stream = self.collector.replay_from(n, chunk_size).await?;
        let f = self.f.clone();
        let enumerated = stream.map(move |e| (f(&e), e));
        Ok(Box::pin(enumerated))
    }
}
