//! Collectors are responsible for collecting data from external sources and
//! turning them into internal events. For example, a collector might listen to
//! a stream of new blocks, and turn them into a stream of `NewBlock` events.

/// This collector listens to a stream of new blocks.
mod block_collector;

/// This collector listens to a stream of new event logs.
mod log_collector;

/// This collector listens to a stream of new pending transactions.
mod mempool_collector;

/// This collector listens to a stream of new events.
mod event_collector;

/// This module contains syntax extensions for the `Collector` trait.
mod collector_ext;

use std::sync::Arc;

use alloy::{
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::Log,
};

pub use block_collector::*;
pub use event_collector::*;
pub use log_collector::*;
pub use mempool_collector::*;

pub use collector_ext::*;
use url::Url;

/// Convenience enum containing all the events that can be emitted by collectors.
pub enum Events<E> {
    NewBlock(NewBlock),
    Log(Log),
    EthEvent((E, Log)),
}

#[derive(Debug, Clone)]
pub struct RpcConfig {
    provider: Url,
    websocket_provider: Option<Url>,
    archive_provider: Option<Url>,
    pub chunk_size: u64,
}

impl RpcConfig {
    pub fn new(
        provider: Url,
        websocket_provider: Option<Url>,
        archive_provider: Option<Url>,
        chunk_size: u64,
    ) -> Self {
        Self {
            provider,
            websocket_provider,
            archive_provider,
            chunk_size,
        }
    }

    pub fn alchemy(provider: Url) -> anyhow::Result<Self> {
        let archive_provider = Some(provider.clone());
        let mut websocket_provider = provider.clone();
        websocket_provider
            .set_scheme("wss")
            .map_err(|_| anyhow::anyhow!("Failed to set scheme"))?;

        Ok(Self::new(
            provider,
            Some(websocket_provider),
            archive_provider,
            10000,
        ))
    }

    pub async fn get_provider(&self) -> anyhow::Result<Arc<dyn Provider>> {
        let provider = ProviderBuilder::new().connect_http(self.provider.clone());
        Ok(Arc::new(provider))
    }

    pub async fn get_archive_provider(&self) -> anyhow::Result<Arc<dyn Provider>> {
        let provider = self.archive_provider.clone().ok_or(anyhow::anyhow!("Archive provider not set"))?;
        let provider = ProviderBuilder::new().connect_http(provider);
        Ok(Arc::new(provider))
    }

    pub async fn get_websocket_provider(&self) -> anyhow::Result<Arc<dyn Provider>> {
        let ws = self
            .websocket_provider
            .clone()
            .ok_or(anyhow::anyhow!("Websocket provider not set"))
            .map(|url| WsConnect::new(url));

        if let Ok(ws) = ws {
            let provider = ProviderBuilder::new().connect_ws(ws).await?;
            return Ok(Arc::new(provider));
        }
        Err(anyhow::anyhow!("Failed to connect to websocket provider"))
    }
}
