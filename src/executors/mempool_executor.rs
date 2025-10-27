use std::{
    ops::{Div, Mul},
    sync::Arc,
};

use crate::types::Executor;
use anyhow::Result;
use async_trait::async_trait;

use alloy::{
    network::TransactionBuilder,
    providers::{PendingTransactionConfig, Provider},
    rpc::types::eth::TransactionRequest,
};

/// An executor that sends transactions to the mempool.
pub struct MempoolExecutor<M> {
    client: Arc<M>,
}

impl<M: Provider> MempoolExecutor<M> {
    pub fn new(client: Arc<M>) -> Self {
        Self { client }
    }
}

/// Information about the gas bid for a transaction.
#[derive(Debug, Clone)]
pub struct GasBidInfo {
    /// Total profit expected from opportunity
    pub total_profit: u128,

    /// Percentage of bid profit to use for gas
    pub bid_percentage: u64,
}

#[derive(Debug)]
pub struct SubmitTxToMempool {
    pub tx: TransactionRequest,
    pub gas_bid_info: Option<GasBidInfo>,
    pub pending_tx_sender: Option<tokio::sync::oneshot::Sender<PendingTransactionConfig>>,
}

#[async_trait]
impl<M> Executor<SubmitTxToMempool> for MempoolExecutor<M>
where
    M: Provider,
{
    /// Send a transaction to the mempool.
    async fn execute(&mut self, mut action: SubmitTxToMempool) -> Result<()> {
        let gas_usage = self
            .client
            .estimate_gas(action.tx.clone())
            .await
            .map_err(|e| anyhow::anyhow!("Error estimating gas usage: {}", e))?;

        let bid_gas_price;
        if let Some(gas_bid_info) = action.gas_bid_info {
            // gas price at which we'd break even, meaning 100% of profit goes to validator
            let breakeven_gas_price = gas_bid_info.total_profit / gas_usage as u128;
            // gas price corresponding to bid percentage
            bid_gas_price = breakeven_gas_price
                .mul(gas_bid_info.bid_percentage as u128)
                .div(100);
        } else {
            bid_gas_price = self
                .client
                .get_gas_price()
                .await
                .map_err(|e| anyhow::anyhow!("Error getting gas price: {}", e))?;
        }
        action.tx.set_gas_price(bid_gas_price);
        let res = self.client.send_transaction(action.tx).await?;

        if let Some(pending_tx_sender) = action.pending_tx_sender {
            let res = pending_tx_sender.send(res.inner().clone());
            if let Err(e) = res {
                tracing::warn!(
                    "Error sending pending transaction config [hash]: {:?}",
                    e.tx_hash()
                );
            }
        }
        Ok(())
    }
}
