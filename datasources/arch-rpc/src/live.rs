use std::sync::Arc;

use async_trait::async_trait;
use atlas_arch::datasource::{
    AccountUpdate, BlockDetails, Datasource, DatasourceId, ReappliedTransactionsEvent,
    RolledbackTransactionsEvent, TransactionUpdate, UpdateType, Updates,
};
use atlas_arch::error::IndexerResult;
use atlas_arch::metrics::MetricsCollection;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

use arch_program::pubkey::Pubkey;
use arch_sdk::{AsyncArchRpcClient, BackoffStrategy, Event, EventTopic, WebSocketClient};
use std::str::FromStr as _;
use tracing::error;

use atlas_arch::sync::LiveSource;

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

    /// Sets up the WebSocket client with auto-reconnect enabled
    async fn setup_websocket_client(&self) -> WebSocketClient {
        let client = WebSocketClient::new(&self.websocket_url);
        // Enable auto-reconnect with exponential backoff (infinite attempts)
        let _ = client
            .set_reconnect_options(true, BackoffStrategy::default_exponential(), 0)
            .await;
        client
    }

    /// Sets up the connection change handler to track reconnections and update metrics
    async fn setup_connection_handler(
        client: &mut WebSocketClient,
        metrics: Arc<MetricsCollection>,
        reconnection_notifier: tokio::sync::mpsc::Sender<()>,
    ) {
        let was_connected = Arc::new(std::sync::Mutex::new(false));
        let is_first_connection = Arc::new(std::sync::Mutex::new(true));
        let metrics_conn = metrics.clone();
        let notifier = reconnection_notifier.clone();

        client
            .on_connection_change(move |connected| {
                let metrics = metrics_conn.clone();
                let notifier = notifier.clone();
                let was_connected = was_connected.clone();
                let is_first_connection = is_first_connection.clone();

                // Check if this is a reconnection (transition from disconnected to connected, but not the first connection)
                let is_reconnection = {
                    let mut was = was_connected.lock().unwrap();
                    let mut is_first = is_first_connection.lock().unwrap();
                    let prev = *was;
                    *was = connected;

                    if *is_first && connected {
                        // First connection - don't treat as reconnection
                        *is_first = false;
                        false
                    } else {
                        // Subsequent connections - check if transitioning from disconnected to connected
                        !prev && connected
                    }
                };

                tokio::spawn(async move {
                    let _ = metrics
                        .update_gauge("arch_live_connected", if connected { 1.0 } else { 0.0 })
                        .await;

                    // Send reconnection notification if this is a reconnection
                    if is_reconnection {
                        let _ = notifier.send(()).await;
                    }
                });
            })
            .await;
    }

    /// Connects the WebSocket client and enables keep-alive
    async fn connect_websocket(
        client: &mut WebSocketClient,
        metrics: &Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        client.connect().await.map_err(|e| {
            atlas_arch::error::Error::Custom(format!(
                "arch_live: failed to connect websocket: {}",
                e
            ))
        })?;

        let _ = client
            .enable_keep_alive(std::time::Duration::from_secs(15))
            .await;
        metrics.update_gauge("arch_live_connected", 1.0).await.ok();

        Ok(())
    }

    /// Subscribes to ReappliedTransactions events
    async fn subscribe_reapplied_transactions(
        client: &mut WebSocketClient,
        sender: Sender<(Updates, DatasourceId)>,
        id: DatasourceId,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        let sender_reapplied = sender.clone();
        let metrics_reapplied = metrics.clone();
        let live_id = id.clone();

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
                atlas_arch::error::Error::Custom(format!(
                    "arch_live: failed to subscribe ReappliedTransactions: {}",
                    e
                ))
            })
    }

    /// Subscribes to RolledbackTransactions events
    async fn subscribe_rolledback_transactions(
        client: &mut WebSocketClient,
        sender: Sender<(Updates, DatasourceId)>,
        id: DatasourceId,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        let sender_rolledback = sender.clone();
        let metrics_rolledback = metrics.clone();
        let live_id = id.clone();

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
                atlas_arch::error::Error::Custom(format!(
                    "arch_live: failed to subscribe RolledbackTransactions: {}",
                    e
                ))
            })
    }

    /// Subscribes to Block events and fetches full block details
    async fn subscribe_block_events(
        client: &mut WebSocketClient,
        sender: Sender<(Updates, DatasourceId)>,
        id: DatasourceId,
        metrics: Arc<MetricsCollection>,
        rpc_client: AsyncArchRpcClient,
    ) -> IndexerResult<()> {
        let sender_block = sender.clone();
        let metrics_block = metrics.clone();
        let live_id = id.clone();
        let rpc_block = rpc_client.clone();

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
                atlas_arch::error::Error::Custom(format!(
                    "arch_live: failed to subscribe Block: {}",
                    e
                ))
            })
    }

    /// Subscribes to AccountUpdate events and fetches account info
    async fn subscribe_account_updates(
        client: &mut WebSocketClient,
        sender: Sender<(Updates, DatasourceId)>,
        id: DatasourceId,
        metrics: Arc<MetricsCollection>,
        rpc_client: AsyncArchRpcClient,
    ) -> IndexerResult<()> {
        let sender_acct = sender.clone();
        let metrics_acct = metrics.clone();
        let live_id = id.clone();
        let rpc_acct = rpc_client.clone();

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
                atlas_arch::error::Error::Custom(format!(
                    "arch_live: failed to subscribe AccountUpdate: {}",
                    e
                ))
            })
    }

    /// Waits for cancellation signal and cleans up the WebSocket connection
    async fn wait_for_cancellation(
        client: &mut WebSocketClient,
        cancellation_token: CancellationToken,
        metrics: &Arc<MetricsCollection>,
    ) {
        cancellation_token.cancelled().await;
        let _ = client.close().await;
        metrics.update_gauge("arch_live_connected", 0.0).await.ok();
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
        reconnection_notifier: tokio::sync::mpsc::Sender<()>,
    ) -> IndexerResult<()> {
        // Setup WebSocket client with auto-reconnect
        let mut client = self.setup_websocket_client().await;

        // Setup connection change handler for reconnection tracking
        Self::setup_connection_handler(&mut client, metrics.clone(), reconnection_notifier).await;

        // Connect the WebSocket and enable keep-alive
        Self::connect_websocket(&mut client, &metrics).await?;

        // Subscribe to all event types
        Self::subscribe_reapplied_transactions(
            &mut client,
            sender.clone(),
            self.id.clone(),
            metrics.clone(),
        )
        .await?;

        Self::subscribe_rolledback_transactions(
            &mut client,
            sender.clone(),
            self.id.clone(),
            metrics.clone(),
        )
        .await?;

        Self::subscribe_block_events(
            &mut client,
            sender.clone(),
            self.id.clone(),
            metrics.clone(),
            self.rpc_client.clone(),
        )
        .await?;

        Self::subscribe_account_updates(
            &mut client,
            sender,
            self.id.clone(),
            metrics.clone(),
            self.rpc_client.clone(),
        )
        .await?;

        // Wait for cancellation signal and cleanup
        Self::wait_for_cancellation(&mut client, cancellation_token, &metrics).await;

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
        // Create a dummy reconnection notifier channel (not used in direct Datasource usage)
        let (_reconnect_tx, _reconnect_rx) = tokio::sync::mpsc::channel::<()>(1);
        // Start live consumption from height 0 (let downstream filtering handle cutoff if any)
        self.consume_live(0, sender, cancellation_token, metrics, _reconnect_tx)
            .await
    }

    fn update_types(&self) -> Vec<UpdateType> {
        <Self as LiveSource>::update_types(self)
    }
}
