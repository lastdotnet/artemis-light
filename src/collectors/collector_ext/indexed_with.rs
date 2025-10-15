use crate::collectors::collector_ext::Archive;
use crate::types::{Collector, CollectorStream};
use alloy::rpc::types::Log;
use anyhow::Result;
use async_trait::async_trait;
use std::marker::PhantomData;
use tokio_stream::StreamExt;

pub trait Indexed {
    type Index;
    fn index(&self) -> Self::Index;
}

impl Indexed for Log {
    type Index = Option<u64>;

    fn index(&self) -> Self::Index {
        self.block_number
    }
}

impl<E> Indexed for (E, Log) {
    type Index = Option<u64>;

    fn index(&self) -> Self::Index {
        self.1.block_number
    }
}

pub struct IndexedWith<E, C> {
    collector: C,
    e: PhantomData<E>,
}

impl<E, C> IndexedWith<E, C> {
    pub fn new(collector: C) -> Self {
        Self {
            collector,
            e: PhantomData,
        }
    }
}

#[async_trait]
impl<E, C, I> Collector<(I, E)> for IndexedWith<E, C>
where
    E: Send + Sync + Indexed<Index = I>,
    C: Collector<E>,
    I: Send + Sync + 'static,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, (I, E)>> {
        let stream = self.collector.subscribe().await?;
        let enumerated = stream.map(move |e| {
            let i = e.index();
            (i, e)
        });
        Ok(Box::pin(enumerated))
    }
}

#[async_trait]
impl<E, C, I> Archive<(I, E)> for IndexedWith<E, C>
where
    C: Archive<(I, E)> + 'static,
    E: Send + Sync + Indexed + 'static,
{
    async fn replay_from(&self, n: u64) -> anyhow::Result<CollectorStream<'_, (I, E)>> {
        self.collector.replay_from(n).await
    }
}
