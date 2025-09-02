use std::sync::Arc;

use async_trait::async_trait;
use atlas_core::datasource::{
    AccountUpdate, BlockDetails, Datasource, DatasourceId, ReappliedTransactionsEvent,
    RolledbackTransactionsEvent, TransactionUpdate, UpdateType, Updates,
};
use atlas_core::error::IndexerResult;
use atlas_core::metrics::MetricsCollection;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

use arch_program::pubkey::Pubkey;
use arch_sdk::{AsyncArchRpcClient, BackoffStrategy, Event, EventTopic, WebSocketClient};
use std::str::FromStr as _;
use tracing::error;

use atlas_core::sync::LiveSource;

#[derive(Clone)]
pub struct ArchLiveDatasource {
    websocket_url: String,
    rpc_client: AsyncArchRpcClient,
    id: DatasourceId,
}

impl ArchLiveDatasource {
    pub fn new(websocket_url: &str, rpc_url: &str, id: DatasourceId) -> Self {
        Self {
            websocket_url: websocket_url.to_string(),
            rpc_client: AsyncArchRpcClient::new(rpc_url),
            id,
        }
    }
}

#[async_trait]
impl LiveSource for ArchLiveDatasource {
    async fn consume_live(
        &self,
        _start_from_height_exclusive: u64,
        sender: Sender<(Updates, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        let mut client = WebSocketClient::new(&self.websocket_url);

        // Enable auto-reconnect with exponential backoff (infinite attempts)
        client
            .set_reconnect_options(true, BackoffStrategy::default_exponential(), 0)
            .await;

        // Track connection state in metrics
        let metrics_conn = metrics.clone();
        client
            .on_connection_change(move |connected| {
                let metrics = metrics_conn.clone();
                tokio::spawn(async move {
                    let _ = metrics
                        .update_gauge("arch_live_connected", if connected { 1.0 } else { 0.0 })
                        .await;
                });
            })
            .await;

        // Best-effort connect and enable basic keep-alives
        if let Err(e) = client.connect().await {
            return Err(atlas_core::error::Error::Custom(format!(
                "arch_live: failed to connect websocket: {}",
                e
            )));
        }
        let _ = client
            .enable_keep_alive(std::time::Duration::from_secs(15))
            .await;
        metrics.update_gauge("arch_live_connected", 1.0).await.ok();

        let live_id = self.id.clone();
        let sender_reapplied = sender.clone();
        let metrics_reapplied = metrics.clone();
        client
            .on_event_async(
                EventTopic::ReappliedTransactions,
                None,
                move |event: Event| {
                    let sender = sender_reapplied.clone();
                    let id = live_id.clone();
                    let metrics = metrics_reapplied.clone();
                    async move {
                        if let Event::ReappliedTransactions(ev) = event {
                            let _ = sender
                                .send((
                                    Updates::ReappliedTransactions(vec![
                                        ReappliedTransactionsEvent {
                                            transaction_hashes: ev.transaction_hashes,
                                            height: ev.block_height,
                                        },
                                    ]),
                                    id,
                                ))
                                .await;
                            let _ = metrics
                                .increment_counter("arch_live_reapplied_events", 1)
                                .await;
                        }
                    }
                },
            )
            .await
            .map_err(|e| {
                atlas_core::error::Error::Custom(format!(
                    "arch_live: failed to subscribe ReappliedTransactions: {}",
                    e
                ))
            })?;

        let live_id = self.id.clone();
        let sender_rolledback = sender.clone();
        let metrics_rolledback = metrics.clone();
        client
            .on_event_async(
                EventTopic::RolledbackTransactions,
                None,
                move |event: Event| {
                    let sender = sender_rolledback.clone();
                    let id = live_id.clone();
                    let metrics = metrics_rolledback.clone();
                    async move {
                        if let Event::RolledbackTransactions(ev) = event {
                            let _ = sender
                                .send((
                                    Updates::RolledbackTransactions(vec![
                                        RolledbackTransactionsEvent {
                                            transaction_hashes: ev.transaction_hashes,
                                            height: ev.block_height,
                                        },
                                    ]),
                                    id,
                                ))
                                .await;
                            let _ = metrics
                                .increment_counter("arch_live_rolledback_events", 1)
                                .await;
                        }
                    }
                },
            )
            .await
            .map_err(|e| {
                atlas_core::error::Error::Custom(format!(
                    "arch_live: failed to subscribe RolledbackTransactions: {}",
                    e
                ))
            })?;

        // Block events → fetch full block and emit BlockDetails + Transactions
        let live_id = self.id.clone();
        let sender_block = sender.clone();
        let metrics_block = metrics.clone();
        let rpc_block = self.rpc_client.clone();
        client
            .on_event_async(EventTopic::Block, None, move |event: Event| {
                let sender = sender_block.clone();
                let id = live_id.clone();
                let metrics = metrics_block.clone();
                let rpc = rpc_block.clone();
                async move {
                    if let Event::Block(be) = event {
                        match rpc.get_full_block_by_hash(&be.hash).await {
                            Ok(Some(full_block)) => {
                                // Emit block details first
                                let block_height = full_block.block_height;
                                let details = BlockDetails {
                                    height: block_height,
                                    block_hash: Some(full_block.hash()),
                                    previous_block_hash: Some(full_block.previous_block_hash),
                                    block_time: Some(
                                        (full_block.timestamp as i128).min(i64::MAX as i128) as i64,
                                    ),
                                    block_height: Some(block_height),
                                };
                                let _ = sender
                                    .send((Updates::BlockDetails(vec![details]), id.clone()))
                                    .await;
                                let _ =
                                    metrics.increment_counter("arch_live_block_events", 1).await;

                                // Emit transactions in this block
                                if !full_block.transactions.is_empty() {
                                    let mut tx_updates =
                                        Vec::with_capacity(full_block.transactions.len());
                                    for tx in full_block.transactions.into_iter() {
                                        tx_updates.push(TransactionUpdate {
                                            transaction: tx,
                                            height: block_height,
                                        });
                                    }
                                    let _ = sender
                                        .send((Updates::Transactions(tx_updates), id.clone()))
                                        .await;
                                }
                            }
                            Ok(None) => {
                                let _ = metrics
                                    .increment_counter("arch_live_block_fetch_miss", 1)
                                    .await;
                            }
                            Err(e) => {
                                error!("arch_live: failed to fetch block: {}", e);
                                let _ = metrics
                                    .increment_counter("arch_live_block_fetch_error", 1)
                                    .await;
                            }
                        }
                    }
                }
            })
            .await
            .map_err(|e| {
                atlas_core::error::Error::Custom(format!(
                    "arch_live: failed to subscribe Block: {}",
                    e
                ))
            })?;

        // Transaction events → fetch processed tx; height unknown (0)
        let live_id = self.id.clone();
        let sender_tx = sender.clone();
        let metrics_tx = metrics.clone();
        let rpc_tx = self.rpc_client.clone();
        client
            .on_event_async(EventTopic::Transaction, None, move |event: Event| {
                let sender = sender_tx.clone();
                let id = live_id.clone();
                let metrics = metrics_tx.clone();
                let rpc = rpc_tx.clone();
                async move {
                    if let Event::Transaction(te) = event {
                        if let Ok(Some(tx)) = rpc.get_processed_transaction(&te.hash).await {
                            tracing::info!(
                                "Transaction processed: {:?} from tx {}",
                                te.hash,
                                te.block_height
                            );

                            let update = TransactionUpdate {
                                transaction: tx,
                                height: te.block_height,
                            };
                            let _ = sender.send((Updates::Transactions(vec![update]), id)).await;
                            let _ = metrics
                                .increment_counter("arch_live_transaction_events", 1)
                                .await;
                        }
                    }
                }
            })
            .await
            .map_err(|e| {
                atlas_core::error::Error::Custom(format!(
                    "arch_live: failed to subscribe Transaction: {}",
                    e
                ))
            })?;

        // AccountUpdate events → fetch account info
        let live_id = self.id.clone();
        let sender_acct = sender.clone();
        let metrics_acct = metrics.clone();
        let rpc_acct = self.rpc_client.clone();
        client
            .on_event_async(EventTopic::AccountUpdate, None, move |event: Event| {
                let sender = sender_acct.clone();
                let id = live_id.clone();
                let metrics = metrics_acct.clone();
                let rpc = rpc_acct.clone();
                async move {
                    if let Event::AccountUpdate(ae) = event {
                        if let Ok(pubkey) = Pubkey::from_str(&ae.account) {
                            if let Ok(info) = rpc.read_account_info(pubkey).await {
                                tracing::info!(
                                    "Account updated: {:?} from tx {}",
                                    ae.account,
                                    ae.transaction_hash
                                );

                                let update = AccountUpdate {
                                    pubkey,
                                    account: info,
                                    height: ae.block_height,
                                };
                                let _ = sender.send((Updates::Accounts(vec![update]), id)).await;
                                let _ = metrics
                                    .increment_counter("arch_live_account_update_events", 1)
                                    .await;
                            }
                        }
                    }
                }
            })
            .await
            .map_err(|e| {
                atlas_core::error::Error::Custom(format!(
                    "arch_live: failed to subscribe AccountUpdate: {}",
                    e
                ))
            })?;

        // Wait for cancellation and then close the client
        cancellation_token.cancelled().await;
        let _ = client.close().await;
        metrics.update_gauge("arch_live_connected", 0.0).await.ok();

        Ok(())
    }

    fn update_types(&self) -> Vec<UpdateType> {
        vec![
            UpdateType::BlockDetails,
            UpdateType::Transaction,
            UpdateType::AccountUpdate,
            UpdateType::RolledbackTransactions,
            UpdateType::ReappliedTransactions,
        ]
    }
}

#[async_trait]
impl Datasource for ArchLiveDatasource {
    async fn consume(
        &self,
        _id: DatasourceId,
        sender: Sender<(Updates, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        // Start live consumption from height 0 (let downstream filtering handle cutoff if any)
        self.consume_live(0, sender, cancellation_token, metrics)
            .await
    }

    fn update_types(&self) -> Vec<UpdateType> {
        <Self as LiveSource>::update_types(self)
    }
}
