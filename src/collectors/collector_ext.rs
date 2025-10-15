use crate::types::{Archive, Collector, Storage};

mod enumerate;
mod filter_map;
mod indexed_with;
mod load_from;
mod map;
mod merge;
mod persist;

pub use enumerate::*;
pub use filter_map::*;
pub use indexed_with::*;
pub use load_from::*;
pub use map::*;
pub use merge::*;
pub use persist::*;

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

    fn enumerate(self) -> Enumerate<E, Self> {
        Enumerate::new(self)
    }

    fn persist<S>(self, storage: S) -> Persist<Self, S>
    where
        S: Storage<E>,
    {
        Persist::new(self, storage, false)
    }

    fn load_from(self, n: u64) -> LoadFrom<Self> {
        LoadFrom::new(self, n)
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
        collectors::{ArchiveCollector, BlockCollector, WithIndex, collector_ext::Archive},
        types::{Collector, CollectorStream, Storage},
    };
    use alloy::{
        network::Ethereum,
        primitives::{Address, B256, Bytes, Log as PrimitivesLog, LogData},
        providers::{Provider, ProviderBuilder, RootProvider},
        rpc::types::Log,
        sol,
        sol_types::SolEvent,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use futures::stream;
    use serde::Deserialize;
    use tokio::sync::Mutex;
    use tokio_stream::{self, StreamExt as _};

    pub struct TestCollector {
        archive: Vec<u8>,
        data: Vec<u8>,
    }

    impl TestCollector {
        pub fn new(archive: Vec<u8>, data: Vec<u8>) -> Self {
            Self { archive, data }
        }
    }

    /// Implementation of the [Collector](Collector) trait for the [BlockCollector](BlockCollector).
    #[async_trait]
    impl Collector<u8> for TestCollector {
        async fn subscribe(&self) -> Result<CollectorStream<'_, u8>> {
            Ok(Box::pin(stream::iter(self.data.clone())))
        }
    }

    #[async_trait]
    impl Archive<u8> for TestCollector {
        async fn replay_from(&self, n: u64) -> Result<CollectorStream<'_, u8>> {
            let data = self.archive[n as usize..].to_vec();
            Ok(Box::pin(stream::iter(data)))
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

    pub struct TestEventCollector<E> {
        data: Vec<(E, Log)>,
    }
    impl TestEventCollector<TestEvent> {
        pub fn new(data: Vec<u8>) -> Self {
            let data = data
                .into_iter()
                .map(|e| (TestEvent::new(e), make_log(e as u64)))
                .collect();
            Self { data }
        }
    }

    #[async_trait]
    impl<E: SolEvent + Send + Sync + Clone> Collector<(E, Log)> for TestEventCollector<E> {
        async fn subscribe(&self) -> Result<CollectorStream<'_, (E, Log)>> {
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

    #[derive(Clone)]
    pub struct TestStorage<E> {
        pub data: Arc<Mutex<Vec<E>>>,
    }

    impl<E> TestStorage<E> {
        pub fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(vec![])),
            }
        }

        pub fn with_data(data: Vec<E>) -> Self {
            Self {
                data: Arc::new(Mutex::new(data)),
            }
        }
    }

    #[async_trait]
    impl<E> Storage<E> for TestStorage<E>
    where
        E: Send + Sync + Clone,
    {
        async fn write(&self, event: &E) -> anyhow::Result<E> {
            println!("Write event");
            let mut data = self.data.lock().await;
            data.push(event.clone());
            Ok(event.clone())
        }
        async fn replay_from(&self, n: u64) -> anyhow::Result<CollectorStream<'_, E>> {
            let data = self.data.lock().await;

            if n >= data.len() as u64 {
                return Ok(Box::pin(stream::iter(vec![])));
            }
            let data = data[n as usize..].to_vec();
            let stream = tokio_stream::iter(data);
            Ok(Box::pin(stream) as CollectorStream<'_, E>)
        }
        async fn head(&self) -> anyhow::Result<u64> {
            let data = self.data.lock().await;
            let enumerated = data.iter().enumerate().collect::<Vec<_>>();
            let head = enumerated
                .last()
                .and_then(|(i, _)| Some(*i as u64))
                .unwrap_or(0);
            Ok(head)
        }
    }

    fn _text_provider() -> impl Provider {
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
        let block_collector = BlockCollector::new(Arc::clone(&provider));
        let mut block_stream = block_collector.subscribe().await.unwrap();

        // Emit the first 5 events
        let mut stage1_block_number = 0;
        let mut n: u64 = 0;
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
                if n > 20 {
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
                ids.push(event.n);
                if event.n >= 20 {
                    break;
                }
            }
        });

        let _ = tokio::select! {
            _ = writer_handle => {},
            _ = reader_handle => {},
        };
        
        let ids = ids.lock().await;
        assert_eq!(
            ids.clone(),
            vec![
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
            ]
        );

        // let events = collector.collect::<Vec<_>>().await;
        // println!("Event: {:?}", events);
        // contract.test(1);

        // let collector = TestArchiveCollector::new(vec![1, 2, 3], vec![4, 5, 6], 0);
        // let stream = collector.subscribe().await.unwrap();
        // let event = stream.collect::<Vec<_>>().await;
        // assert_eq!(event, vec![1, 2, 3, 4, 5, 6]);
    }

    impl WithIndex for u8 {
        type Index = u8;
        fn index(&self) -> Self::Index {
            *self
        }
    }

    #[tokio::test]
    async fn test_collector_enumerate() {
        let indices = vec![1, 2, 3];
        let collector = TestCollector::new(vec![], indices.clone());
        let enumerated = collector.enumerate();
        let stream = enumerated.subscribe().await.unwrap();
        let event = stream.map(|(i, _)| i).collect::<Vec<_>>().await;
        assert_eq!(event, indices);
    }

    #[tokio::test]
    async fn test_collector_map() {
        let collector = TestCollector::new(vec![], vec![1, 2, 3]);
        let collector = collector.map(|n| n + 1);
        let stream = collector.subscribe().await.unwrap();
        let event = stream.collect::<Vec<_>>().await;
        assert_eq!(event, vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn test_collector_filter_map() {
        let collector = TestCollector::new(vec![], vec![1, 2, 3, 4]);
        let collector = collector.filter_map(|n| if n % 2 == 0 { Some(n) } else { None });
        let stream = collector.subscribe().await.unwrap();
        let event = stream.collect::<Vec<_>>().await;
        assert_eq!(event, vec![2, 4]);
    }

    #[tokio::test]
    async fn test_collector_vec_merge() {
        let block_collector = TestCollector::new(vec![], vec![1, 3, 5]);
        let block_collector_2 = TestCollector::new(vec![], vec![2, 4, 6]);
        let block_collector_3 = TestCollector::new(vec![], vec![7]);

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
    async fn test_collector_merge() {
        let block_collector = TestCollector::new(vec![], vec![1, 3, 5]);
        let block_collector_2 = TestCollector::new(vec![], vec![2, 4, 6]);
        let merged = block_collector.merge(block_collector_2);
        let stream = merged.subscribe().await.unwrap();
        let mut res = stream.collect::<Vec<_>>().await;
        res.sort();
        assert_eq!(res, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_collector_map_filter_merge() {
        let collector = TestCollector::new(vec![], vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let collector = collector
            .map(|a| a + 1)
            .filter_map(|a| if a % 2 == 0 { Some(a) } else { None })
            .merge(TestCollector::new(vec![], vec![11, 12, 13, 14, 15]));
        let stream = collector.subscribe().await.unwrap();
        let mut res = stream.collect::<Vec<_>>().await;
        res.sort();
        assert_eq!(res, vec![2, 4, 6, 8, 10, 11, 12, 13, 14, 15]);
    }

    #[tokio::test]
    async fn test_collector_archive() {
        let collector = TestCollector::new(vec![1, 2, 3], vec![4, 5, 6]);
        let collector = collector.load_from(2);
        let stream = collector.subscribe().await.unwrap();
        let res = stream.collect::<Vec<_>>().await;
        assert_eq!(res, vec![3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_collector_storage() {
        let storage = TestStorage::<u8>::new();
        let collector = TestCollector::new(vec![1, 2, 3], vec![4, 5, 6]);
        let collector = collector.persist(storage.clone());
        let stream = collector.subscribe().await.unwrap();
        let data = stream.map(|e| e).collect::<Vec<_>>().await;
        let stored = storage.data.lock().await.clone();
        let stored = stored.into_iter().collect::<Vec<_>>();
        assert_eq!(data, stored);
        assert_eq!(data, vec![4, 5, 6]);
    }

    #[tokio::test]
    async fn test_collector_from_storage_scenario_1() {
        let storage = TestStorage::<u8>::with_data(vec![0, 1, 2, 3, 4]);
        let collector = TestCollector::new(vec![0, 1, 2, 3, 4], vec![5, 6, 7, 8, 9]);
        let collector = collector.persist(storage.clone()).load_from(2);
        let res = collector
            .subscribe()
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let stored = storage.data.lock().await.clone();
        assert_eq!(res, vec![2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(stored, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[tokio::test]
    async fn test_collector_from_storage_scenario_2() {
        let storage = TestStorage::<u8>::with_data(vec![0, 1, 2, 3, 4]);
        let collector = TestCollector::new(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9], vec![10]);
        let collector = collector.persist(storage.clone()).load_from(6);
        let res = collector
            .subscribe()
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let stored = storage.data.lock().await.clone();
        assert_eq!(res, vec![6, 7, 8, 9, 10]);
        assert_eq!(stored, vec![0, 1, 2, 3, 4, 6, 7, 8, 9, 10]);
    }

    // #[tokio::test]
    // async fn test_collector_enumerate_storage() {
    //     let storage = TestStorage::new();
    //     let collector = TestCollector::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]); // (E, Log)
    //     let enumerated = collector.enumerate();
    //     let archive = enumerated.from(0); // (Option<u64>, E, Log)
    //     let collector = collector.storage(storage.clone());
    //     let stream = collector.subscribe().await.unwrap();
    //     let data = stream.map(|(e, _)| e).collect::<Vec<_>>().await;
    //     let stored = storage.data.lock().await.clone();
    //     let stored = stored.into_iter().map(|(_, e, _)| e).collect::<Vec<_>>();
    //     assert_eq!(stored, data);
    // }
}
