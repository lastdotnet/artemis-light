use alloy::rpc::types::Log;

mod enumerate;
mod filter_map;
mod map;
mod merge;

pub use enumerate::*;
pub use filter_map::*;
pub use map::*;
pub use merge::*;

use crate::types::Collector;

/// Extension trait that provides additional functionality for types implementing [`Collector`].
///
/// This trait adds methods for transforming and combining collector streams:
pub trait ArchiveCollectorExt<E>: Collector<E> + Send + Sync + Sized + 'static {
    /// Map events from type `E` to type `E2` using a function `f`.
    ///
    /// # Example
    ///
    /// ```no-run
    /// # tokio_test::block_on(async {
    /// let collector = TestCollector::new(vec![1, 2, 3]);
    /// let collector = collector.map(|n| n + 1);
    /// let res = collector.get_event_stream().await.unwrap().collect::<Vec<_>>().await;
    /// assert_eq!(res, vec![2, 3, 4]);
    /// # })
    /// ```
    ///
    fn map<F, E2>(self, f: F) -> Map<E, F>
    where
        F: Fn(E) -> E2 + Send + Sync + Clone + 'static,
    {
        Map::new(Box::new(self), f)
    }

    /// Filter and transform events from type `E` to type `E2` using a function `f`.
    ///
    /// # Example
    ///
    /// ```no-run
    /// # tokio_test::block_on(async {
    /// let collector = TestCollector::new(vec![1, 2, 3, 4]);
    /// let collector = collector.filter_map(|n| if n % 2 == 0 { Some(n) } else { None });
    /// let res = collector.get_event_stream().await.unwrap().collect::<Vec<_>>().await;
    /// assert_eq!(res, vec![2, 4]);
    /// # })
    /// ```
    ///
    fn filter_map<F, E2>(self, f: F) -> FilterMap<E, F>
    where
        F: Fn(E) -> Option<E2> + Send + Sync + Clone + 'static,
    {
        FilterMap::new(Box::new(self), f)
    }

    /// Merge two collectors into a single collector that emits events from both.
    ///
    /// # Example
    ///
    /// ```no-run
    /// # tokio_test::block_on(async {
    /// let collector = TestCollector::new(vec![1, 2, 3]);
    /// let collector_2 = TestCollector::new(vec![4, 5, 6]);
    /// let merged = collector.merge(collector_2);
    /// let mut res = merged.get_event_stream().await.unwrap().collect::<Vec<_>>().await;
    /// res.sort();
    /// assert_eq!(res, vec![1, 2, 3, 4, 5, 6]);
    /// # })
    /// ```
    ///
    fn merge<C>(self, other: C) -> Merge<Self, C>
    where
        C: Collector<E> + Send + Sync + 'static,
    {
        Merge::new(self, other)
    }

    fn enumerate<E2>(self) -> Enumerate<E2>
    where
        Self: Collector<Log> + Send + Sync + 'static,
    {
        Enumerate::new(Box::new(self))
    }
}

impl<T: Collector<E> + 'static, E> ArchiveCollectorExt<E> for T {}

#[cfg(doctest)]
pub mod doctest {
    use anyhow::Result;
    use artemis_light::types::{Collector, CollectorStream};
    use async_trait::async_trait;
    use futures::stream;
    pub struct TestCollector {
        data: Vec<u8>,
    }
    impl TestCollector {
        pub fn new(data: Vec<u8>) -> Self {
            Self { data }
        }
    }
    #[async_trait]
    impl Collector<u8> for TestCollector {
        async fn get_event_stream(&self) -> Result<CollectorStream<'_, u8>> {
            Ok(Box::pin(stream::iter(self.data.clone())))
        }
    }
}

#[cfg(test)]
mod test {
    use super::ArchiveCollectorExt;
    use crate::types::{Collector, CollectorStream};
    use alloy::{
        primitives::{Address, B256, Bytes, Log as PrimitivesLog, LogData},
        rpc::types::Log,
        sol_types::SolEvent,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use futures::stream;
    use serde::Deserialize;
    use tokio_stream::StreamExt;
    use tokio_stream::{self};

    pub struct TestCollector {
        data: Vec<u8>,
    }

    impl TestCollector {
        pub fn new(data: Vec<u8>) -> Self {
            Self { data }
        }
    }

    /// Implementation of the [Collector](Collector) trait for the [BlockCollector](BlockCollector).
    #[async_trait]
    impl Collector<u8> for TestCollector {
        async fn subscribe(&self) -> Result<CollectorStream<'_, u8>> {
            Ok(Box::pin(stream::iter(self.data.clone())))
        }
    }

    pub struct LogCollector {
        data: Vec<Log>,
    }
    impl LogCollector {
        pub fn new(data: Vec<u64>) -> Self {
            let data = data.into_iter().map(Self::make_log).collect();
            Self { data }
        }
        fn make_log(block_number: u64) -> Log {
            Log {
                inner: PrimitivesLog {
                    address: Address::with_last_byte(0x69),
                    data: LogData::new_unchecked(
                        vec![B256::with_last_byte(0x69)],
                        Bytes::from_static(&[0x69]),
                    ),
                },
                block_hash: Some(B256::with_last_byte(0x69)),
                block_number: Some(block_number),
                block_timestamp: None,
                transaction_hash: Some(B256::with_last_byte(0x69)),
                transaction_index: Some(0x69),
                log_index: Some(0x69),
                removed: false,
            }
        }
    }

    #[async_trait]
    impl Collector<Log> for LogCollector {
        async fn subscribe(&self) -> Result<CollectorStream<'_, Log>> {
            Ok(Box::pin(stream::iter(self.data.clone())))
        }
    }

    #[derive(Debug, Clone, Default, Deserialize)]
    struct TestEvent {}

    impl SolEvent for TestEvent {
        type DataTuple<'a> = ();

        type DataToken<'a> = ();

        type TopicList = ();

        const SIGNATURE: &'static str = "";

        const SIGNATURE_HASH: alloy::primitives::FixedBytes<32> =
            alloy::primitives::FixedBytes::ZERO;

        const ANONYMOUS: bool = true;

        fn new(
            _topics: <Self::TopicList as alloy::dyn_abi::SolType>::RustType,
            _data: <Self::DataTuple<'_> as alloy::dyn_abi::SolType>::RustType,
        ) -> Self {
            Self {}
        }

        fn tokenize_body(&self) -> Self::DataToken<'_> {
            unimplemented!()
        }

        fn topics(&self) -> <Self::TopicList as alloy::dyn_abi::SolType>::RustType {
            unimplemented!()
        }

        fn encode_topics_raw(
            &self,
            _out: &mut [alloy::dyn_abi::abi::token::WordToken],
        ) -> alloy::sol_types::Result<()> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_collector_enumerate() {
        let indices = vec![1, 2, 3];
        let collector = LogCollector::new(indices.clone());

        let enumerated = collector.enumerate::<TestEvent>();
        let stream = enumerated.subscribe().await.unwrap();
        let event = stream.map(|(i, e)| i).collect::<Vec<_>>().await;
        assert_eq!(event, indices);
    }

    #[tokio::test]
    async fn test_collector_map() {
        let collector = TestCollector {
            data: vec![1, 2, 3],
        };
        let collector = collector.map(|n| n + 1);
        let stream = collector.subscribe().await.unwrap();
        let event = stream.collect::<Vec<_>>().await;
        assert_eq!(event, vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn test_filter_collector_map() {
        let collector = TestCollector {
            data: vec![1, 2, 3, 4],
        };
        let collector = collector.filter_map(|n| if n % 2 == 0 { Some(n) } else { None });
        let stream = collector.subscribe().await.unwrap();
        let event = stream.collect::<Vec<_>>().await;
        assert_eq!(event, vec![2, 4]);
    }

    #[tokio::test]
    async fn test_vec_merge_collector() {
        let block_collector = TestCollector::new(vec![1, 3, 5]);
        let block_collector_2 = TestCollector::new(vec![2, 4, 6]);
        let block_collector_3 = TestCollector::new(vec![7]);

        let collectors = vec![
            Box::new(block_collector),
            Box::new(block_collector_2),
            Box::new(block_collector_3),
        ];

        let mut res = collectors
            .subscribe()
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        res.sort();
        assert_eq!(res, vec![1, 2, 3, 4, 5, 6, 7])
    }

    #[tokio::test]
    async fn test_merge_collector() {
        let block_collector = TestCollector::new(vec![1, 3, 5]);
        let block_collector_2 = TestCollector::new(vec![2, 4, 6]);
        let merged = block_collector.merge(block_collector_2);
        let stream = merged.subscribe().await.unwrap();
        let mut res = stream.collect::<Vec<_>>().await;
        res.sort();
        assert_eq!(res, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_collector_map_filter_merge() {
        let collector = TestCollector::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let collector = collector
            .map(|n| n + 1)
            .filter_map(|n| if n % 2 == 0 { Some(n) } else { None })
            .merge(TestCollector::new(vec![11, 12, 13, 14, 15]));
        let stream = collector.subscribe().await.unwrap();
        let mut res = stream.collect::<Vec<_>>().await;
        res.sort();
        assert_eq!(res, vec![2, 4, 6, 8, 10, 11, 12, 13, 14, 15]);
    }
}
