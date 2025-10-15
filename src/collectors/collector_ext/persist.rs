use crate::types::{Archive, Collector, CollectorStream, Storage};
use async_trait::async_trait;
use futures::StreamExt;

pub struct Persist<C, S> {
    collector: C,
    storage: S,
    skip_errors: bool,
}

impl<C, S> Persist<C, S> {
    pub fn new(collector: C, storage: S, skip_errors: bool) -> Self {
        Self {
            collector,
            storage,
            skip_errors,
        }
    }
}

#[async_trait]
impl<C, S, E> Collector<E> for Persist<C, S>
where
    C: Collector<E> + 'static,
    S: Storage<E> + Send + Sync,
    E: Send + Sync + 'static,
{
    async fn subscribe(&self) -> anyhow::Result<CollectorStream<'_, E>> {
        println!("Persist Subscribe from persist");
        let stream = self.collector.subscribe().await?;
        let stream = stream
            .then(async move |e| match self.storage.write(&e).await {
                Ok(e) => {
                    println!("Persisted event");
                    Some(e)
                }
                Err(err) => {
                    tracing::error!("Error persisting event: {:?}", &err);
                    if !self.skip_errors { None } else { Some(e) }
                }
            })
            .filter_map(|e| async move { std::convert::identity(e) });
        Ok(Box::pin(stream) as CollectorStream<'_, E>)
    }
}

#[async_trait]
impl<E, C, S> Archive<E> for Persist<C, S>
where
    C: Archive<E> + 'static,
    S: Storage<E> + Send + Sync,
    E: Send + Sync + 'static,
{
    async fn replay_from(&self, i: u64) -> anyhow::Result<CollectorStream<'_, E>> {
        let h = self.storage.head().await?;
        if h >= i {
            // scenario 1: We storages first 4 values of stream s = [0,1,2,3,4,5,6,7,8,9] and want to replay from event 2.
            // Thus: h = 5, i = 3, stored_archive=[0,1,2,3]
            //     i
            // 0 1 2 3 4 5 6 7 8 9
            //         h
            // The resulting stream contains [2, 3, 4, 5, 6, 7, 8, 9]

            let n: usize = (h - i) as usize;
            let mut c = 0;
            let from_archive = self.collector.replay_from(i).await?;
            let from_archive = from_archive.filter_map(move |e| async move {
                c += 1;
                if c > n {
                    self.storage.write(&e).await.ok()?;
                };
                Some(e)
            });
            Ok(Box::pin(from_archive))
        } else {
            // scenario 1: We storages first 4 values of stream s = [0,1,2,3,4,5,6,7,8,9] and want to replay from event 2.
            // Thus: h = 5, i = 3, stored_archive=[0,1,2,3]
            //         i
            // 0 1 2 3 4 5 6 7 8 9
            //     h
            // The resulting stream contains [4, 5, 6, 7, 8, 9]

            let from_storage = self.storage.replay_from(i).await?;
            let from_archive = self
                .collector
                .replay_from(i)
                .await?
                .filter_map(move |e| async move { self.storage.write(&e).await.ok() });
            Ok(Box::pin(from_storage.chain(from_archive)))
            // // if head is behind n, we can replay from the storage
            // let stream = self.collector.replay_from(n).await?;
            // // if head is ahead of n, we need replay from the archive and backfill the storage
            // let stream = stream.zip(stream::repeat(head));

            // let stream = stream.then(async move |(e, head)| {
            //     let index = e.index().into();
            //     if head > index {
            //         self.storage.write(&e).await.ok();
            //     }
            //     e
            // });
            // Ok(Box::pin(stream))
        }
    }
}
