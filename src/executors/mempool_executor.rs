use std::{
    marker::PhantomData,
    ops::{Div, Mul},
    sync::Arc,
};

use crate::types::Executor;
use async_trait::async_trait;

use alloy::{
    network::TransactionBuilder,
    providers::Provider,
    rpc::types::{TransactionReceipt, eth::TransactionRequest},
};

/// An executor that sends transactions to the mempool.
pub struct MempoolExecutor<M, Err> {
    client: Arc<M>,
    _err: PhantomData<Err>,
}

impl<M: Provider, Err> MempoolExecutor<M, Err> {
    pub fn new(client: Arc<M>) -> Self {
        Self {
            client,
            _err: PhantomData,
        }
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

#[derive(Debug, Clone)]
pub struct SubmitTxToMempool {
    pub tx: TransactionRequest,
    pub gas_bid_info: Option<GasBidInfo>,
}

#[async_trait]
impl<M> Executor<SubmitTxToMempool, TransactionReceipt, ()> for MempoolExecutor<M, ()>
where
    M: Provider,
{
    /// Send a transaction to the mempool.
    async fn execute(&mut self, mut action: SubmitTxToMempool) -> Result<TransactionReceipt, ()> {
        let gas_usage = self
            .client
            .estimate_gas(action.tx.clone())
            .await
            .map_err(|_| ())?;

        let bid_gas_price;
        if let Some(gas_bid_info) = action.gas_bid_info {
            // gas price at which we'd break even, meaning 100% of profit goes to validator
            let breakeven_gas_price = gas_bid_info.total_profit / gas_usage as u128;
            // gas price corresponding to bid percentage
            bid_gas_price = breakeven_gas_price
                .mul(gas_bid_info.bid_percentage as u128)
                .div(100);
        } else {
            bid_gas_price = self.client.get_gas_price().await.map_err(|_| ())?;
        }
        action.tx.set_gas_price(bid_gas_price);
        let pending_tx = self
            .client
            .send_transaction(action.tx)
            .await
            .map_err(|_| ())?;
        let tx_receipt = pending_tx.get_receipt().await.map_err(|_| ())?;
        Ok(tx_receipt)
    }
}
