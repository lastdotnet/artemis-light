use alloy::{providers::Provider, rpc::types::Log};

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
pub trait CollectorExt<E>: Collector<E> + Send + Sync + Sized + 'static {
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

    fn subscribe_from_archive<P: Provider>(
        self,
        archive_provider: P,
        from: u64,
    ) -> ArchiveCollectorImpl<Self, P> {
        ArchiveCollectorImpl::new(self, archive_provider, from)
    }

    fn enumerate<E2>(self) -> Enumerate<E2>
    where
        Self: Collector<Log> + Send + Sync + 'static,
    {
        Enumerate::new(Box::new(self))
    }
}

pub struct ArchiveCollectorImpl<C, P> {
    provider: P,
    collector: C,
    from: u64,
}

impl<C, P> ArchiveCollectorImpl<C, P> {
    pub fn new(collector: C, archive_provider: P, from: u64) -> Self {
        Self {
            collector,
            provider: archive_provider,
            from,
        }
    }
}

impl<T: Collector<E> + 'static, E> CollectorExt<E> for T {}

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
    use std::{sync::Arc, time::Duration};

    use super::CollectorExt;
    use crate::{
        collectors::{ArchiveCollector, BlockCollector, EventCollector},
        types::{Collector, CollectorStream},
    };
    use alloy::{
        contract::Event,
        network::{Ethereum, Network},
        primitives::{Address, B256, BlockNumber, Bytes, Log as PrimitivesLog, LogData, U64, U256},
        providers::{FilterPollerBuilder, Provider, ProviderBuilder, ProviderCall, RootProvider},
        rpc::{
            client::{ClientRef, NoParams, PollerBuilder},
            types::{Filter, FilterBlockOption, Log},
        },
        sol,
        sol_types::SolEvent,
        transports::TransportResult,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use futures::stream;
    use serde::{Deserialize, de::DeserializeOwned};
    use tokio::sync::Mutex;
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

    pub struct LogCollector {
        data: Vec<Log>,
    }
    impl LogCollector {
        pub fn new(data: Vec<u64>) -> Self {
            let data = data.into_iter().map(make_log).collect();
            Self { data }
        }
    }

    #[async_trait]
    impl Collector<Log> for LogCollector {
        async fn subscribe(&self) -> Result<CollectorStream<'_, Log>> {
            Ok(Box::pin(stream::iter(self.data.clone())))
        }
    }

    #[derive(Debug, Clone, Default, Deserialize)]
    struct TestEvent {
        id: u8,
    }

    impl TestEvent {
        pub fn new(id: u8) -> Self {
            Self { id }
        }
    }

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
            Self { id: 0 }
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

    pub struct TestArchiveCollector {
        history: Vec<TestEvent>,
        data: Vec<TestEvent>,
        from: u64,
    }

    impl TestArchiveCollector {
        pub fn new(history: Vec<u8>, data: Vec<u8>, from: u64) -> Self {
            Self {
                history: history.into_iter().map(TestEvent::new).collect(),
                data: data.into_iter().map(TestEvent::new).collect(),
                from,
            }
        }
    }

    fn text_provider() -> impl Provider {
        RootProvider::<Ethereum>::builder()
            .with_recommended_fillers()
            .connect_anvil()
    }

    pub struct TestProvider {
        history: Vec<Log>,
        data: Vec<Log>,
    }

    impl TestProvider {
        pub fn new(history: Vec<u64>, data: Vec<u64>) -> Self {
            Self {
                history: history.into_iter().map(make_log).collect(),
                data: data.into_iter().map(make_log).collect(),
            }
        }
    }

    sol! {
        #[sol(rpc, bytecode="6080806040523460135760b1908160188239f35b5f80fdfe60808060405260043610156011575f80fd5b5f3560e01c63b0e0b9ed146023575f80fd5b34607757602036600319011260775760043567ffffffffffffffff81168091036077577f606260b72639023369b8c3e5815aa6c02d57668d01bc55d84f28de6e1dc97dff60208383829552a1604051908152f35b5f80fdfea2646970667358221220437ddae43f164f5ed03de65753d1191d0e176f064936dff9afe62708d7106fba64736f6c634300081e0033")]
        contract TestContract {
            #[derive(Debug)]
            event TestEvent(uint64 n);
            function test(uint64 n) public returns (uint64) {
                emit TestEvent(n);
                return n;
            }
        }
    }

    #[tokio::test]
    async fn test_archive_collector() {
        let provider = ProviderBuilder::new()
            .connect_anvil_with_wallet_and_config(|config| config.block_time_f64(0.1))
            .unwrap();
        let provider = Arc::new(provider);

        let contract = Arc::new(TestContract::deploy(Arc::clone(&provider)).await.unwrap());
        let mut n: u64 = 0;
        let block_collector = BlockCollector::new(Arc::clone(&provider));

        let mut block_stream = block_collector.subscribe().await.unwrap();
        // Emit the first 5 events
        let mut stage1_block_number = 0;
        let clone_contract = Arc::clone(&contract);
        while let Some(block) = block_stream.next().await {
            if n > 5 {
                stage1_block_number = block.number as u64;
                break;
            }
            let _ = clone_contract.test(n).send().await.unwrap();
            n += 1;
        }

        let clone_contract = Arc::clone(&contract);
        // Continue to emit events every 100ms
        let writer_handle = tokio::spawn(async move {
            loop {
                println!("Writing event: {:?}", n);
                if n >= 20 {
                    break;
                }
                let _ = clone_contract.test(n).send().await.unwrap();
                n += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;  
            }
        });

        let ids = Arc::new(Mutex::new(vec![]));
        let ids_clone = Arc::clone(&ids);

        let clone_contract = Arc::clone(&contract);
        let reader_handle = tokio::spawn(async move {
            let mut ids = ids_clone.lock().await;
            let filter = clone_contract.TestEvent_filter(); 
            let archive_collector = ArchiveCollector::new(filter, 0, 100);
            let mut archive_stream = archive_collector.subscribe().await.unwrap();
            while let Some(event) = archive_stream.next().await {
                println!("Event: {:?}", event);
                ids.push(event.n);
                // if event.n >= 10 {
                //     break;
                // }
            }
        });

        let _ = tokio::select! {
            _ = writer_handle => {},
            _ = reader_handle => {},
        };

        println!("Ids: {:?}", Arc::clone(&ids).lock().await);

        // let events = collector.collect::<Vec<_>>().await;
        // println!("Event: {:?}", events);
        // contract.test(1);

        // let collector = TestArchiveCollector::new(vec![1, 2, 3], vec![4, 5, 6], 0);
        // let stream = collector.subscribe().await.unwrap();
        // let event = stream.collect::<Vec<_>>().await;
        // assert_eq!(event, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_collector_enumerate() {
        let indices = vec![1, 2, 3];
        let collector = LogCollector::new(indices.clone());

        let enumerated = collector.enumerate::<TestEvent>();
        let stream = enumerated.subscribe().await.unwrap();
        let event = stream.map(|(i, _)| i).collect::<Vec<_>>().await;
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
