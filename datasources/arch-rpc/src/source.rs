use std::sync::Arc;

use async_trait::async_trait;
use atlas_arch::datasource::AccountUpdate;
use atlas_arch::datasource::{
    BlockDetails, Datasource, DatasourceId, TransactionUpdate, UpdateType, Updates,
};
use atlas_arch::error::IndexerResult;
use atlas_arch::metrics::MetricsCollection;
use atlas_arch::sync::{BackfillSource, TipSource};
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

use super::fetcher::{BlockFetcher, BlockFetcherConfig};
use super::{ArchRpc, ArchRpcClient};
use std::sync::atomic::AtomicBool;

#[derive(Clone, Debug)]
pub struct ArchDatasourceConfig {
    pub max_concurrency: usize,
    pub batch_emit_size: usize,
    pub fetch_window_size: usize,
    pub initial_backoff_ms: u64,
    pub max_retries: usize,
}

impl Default for ArchDatasourceConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 32,
            batch_emit_size: 256,
            fetch_window_size: 512,
            initial_backoff_ms: 50,
            max_retries: 5,
        }
    }
}

#[derive(Clone)]
pub struct ArchBackfillDatasource<C: ArchRpc + Clone + Send + Sync + 'static> {
    client: C,
    config: ArchDatasourceConfig,
}

impl ArchBackfillDatasource<ArchRpcClient> {
    pub fn new(url: &str, config: ArchDatasourceConfig) -> Self {
        Self {
            client: ArchRpcClient::new(url),
            config,
        }
    }
}

impl<C: ArchRpc + Clone + Send + Sync + 'static> ArchBackfillDatasource<C> {
    pub fn with_client(client: C, config: ArchDatasourceConfig) -> Self {
        Self { client, config }
    }
}

#[async_trait]
impl<C: ArchRpc + Clone + Send + Sync + 'static> Datasource for ArchBackfillDatasource<C> {
    async fn consume(
        &self,
        id: DatasourceId,
        sender: Sender<(Updates, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        // Initialize starting point at current best height and poll for new blocks
        let mut last_seen_height = self.client.get_block_count().await.map_err(|e| {
            atlas_arch::error::Error::Custom(format!("arch_rpc get_block_count: {}", e))
        })?;

        metrics
            .update_gauge("arch_rpc_best_height", last_seen_height as f64)
            .await
            .ok();

        let running = Arc::new(AtomicBool::new(true));
        let fetcher_cfg = BlockFetcherConfig {
            block_fetcher_threads: self.config.max_concurrency,
            block_buffer_size: self.config.batch_emit_size * 2,
            fetch_window_size: self.config.fetch_window_size,
            initial_backoff_ms: self.config.initial_backoff_ms,
            max_retries: self.config.max_retries,
        };
        let fetcher = BlockFetcher::new(fetcher_cfg, &self.client, Arc::clone(&running));

        // Poll loop for new blocks beyond last_seen_height
        loop {
            if cancellation_token.is_cancelled() {
                break;
            }

            let current_best = match self.client.get_block_count().await {
                Ok(h) => h,
                Err(_e) => {
                    // Backoff on error and continue
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                    continue;
                }
            };

            metrics
                .update_gauge("arch_rpc_best_height", current_best as f64)
                .await
                .ok();

            if current_best > last_seen_height {
                let start = last_seen_height.saturating_add(1);
                let end = current_best;
                let mut rx = fetcher
                    .fetch_blocks(start, end, cancellation_token.clone())
                    .await;

                while let Some(result) = rx.recv().await {
                    match result {
                        Ok((height, _hash, full_block)) => {
                            // Emit AccountUpdates for writable accounts in this block
                            if !full_block.transactions.is_empty() {
                                let mut writable_keys: HashSet<arch_program::pubkey::Pubkey> =
                                    HashSet::new();
                                for tx in full_block.transactions.iter() {
                                    let msg = &tx.runtime_transaction.message;
                                    for (idx, key) in msg.account_keys.iter().enumerate() {
                                        if msg.is_writable_index(idx) {
                                            writable_keys.insert(*key);
                                        }
                                    }
                                }

                                if !writable_keys.is_empty() {
                                    let keys_vec: Vec<arch_program::pubkey::Pubkey> =
                                        writable_keys.iter().cloned().collect();
                                    match self.client.get_multiple_accounts(&keys_vec).await {
                                        Ok(accounts_map) => {
                                            let mut updates: Vec<AccountUpdate> =
                                                Vec::with_capacity(accounts_map.len());
                                            for (pubkey, maybe_info) in accounts_map.into_iter() {
                                                if let Some(account) = maybe_info {
                                                    updates.push(AccountUpdate {
                                                        pubkey,
                                                        account,
                                                        height,
                                                    });
                                                }
                                            }
                                            if !updates.is_empty() {
                                                let _ = sender
                                                    .send((Updates::Accounts(updates), id.clone()))
                                                    .await;
                                                metrics
                                                    .increment_counter(
                                                        "arch_rpc_backfill_account_updates_emitted",
                                                        1,
                                                    )
                                                    .await
                                                    .ok();
                                            }
                                        }
                                        Err(_e) => {
                                            metrics
                                                .increment_counter(
                                                    "arch_rpc_backfill_account_updates_failed",
                                                    1,
                                                )
                                                .await
                                                .ok();
                                        }
                                    }
                                }
                            }

                            // Emit block details
                            let details = BlockDetails {
                                height,
                                block_hash: Some(full_block.hash()),
                                previous_block_hash: Some(full_block.previous_block_hash),
                                block_time: Some(full_block.timestamp as i64),
                                block_height: Some(full_block.block_height),
                            };
                            let _ = sender
                                .send((Updates::BlockDetails(vec![details]), id.clone()))
                                .await;
                            metrics
                                .increment_counter("arch_rpc_block_details_emitted", 1)
                                .await
                                .ok();

                            // Emit transactions
                            let mut chunk: Vec<TransactionUpdate> = Vec::new();
                            for tx in full_block.transactions.into_iter() {
                                let update = TransactionUpdate {
                                    transaction: tx,
                                    height,
                                };
                                chunk.push(update);
                                if chunk.len() >= self.config.batch_emit_size {
                                    let _ = sender
                                        .send((
                                            Updates::Transactions(std::mem::take(&mut chunk)),
                                            id.clone(),
                                        ))
                                        .await;
                                }
                            }
                            if !chunk.is_empty() {
                                let _ = sender
                                    .send((Updates::Transactions(chunk), id.clone()))
                                    .await;
                            }
                            metrics
                                .increment_counter("arch_rpc_blocks_processed", 1)
                                .await
                                .ok();
                        }
                        Err(_e) => {
                            metrics
                                .increment_counter("arch_rpc_blocks_failed", 1)
                                .await
                                .ok();
                        }
                    }
                }

                last_seen_height = current_best;
            } else {
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        }

        Ok(())
    }

    fn update_types(&self) -> Vec<UpdateType> {
        vec![
            UpdateType::Transaction,
            UpdateType::AccountUpdate,
            UpdateType::BlockDetails,
        ]
    }
}

// TipSource implementation to integrate with the syncing orchestrator
#[async_trait]
impl<C: ArchRpc + Clone + Send + Sync + 'static> TipSource for ArchBackfillDatasource<C> {
    async fn best_height(&self) -> IndexerResult<u64> {
        self.client.get_block_count().await.map_err(|e| {
            atlas_arch::error::Error::Custom(format!("arch_rpc get_block_count: {}", e))
        })
    }
}

// BackfillSource implementation for a height range
#[async_trait]
impl<C: ArchRpc + Clone + Send + Sync + 'static> BackfillSource for ArchBackfillDatasource<C> {
    async fn backfill_range(
        &self,
        start_height_inclusive: u64,
        end_height_inclusive: u64,
        sender: Sender<(Updates, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        let running = Arc::new(AtomicBool::new(true));
        let fetcher_cfg = BlockFetcherConfig {
            block_fetcher_threads: self.config.max_concurrency,
            block_buffer_size: self.config.batch_emit_size * 2,
            fetch_window_size: self.config.fetch_window_size,
            initial_backoff_ms: self.config.initial_backoff_ms,
            max_retries: self.config.max_retries,
        };
        let fetcher = BlockFetcher::new(fetcher_cfg, &self.client, Arc::clone(&running));
        let mut rx = fetcher
            .fetch_blocks(
                start_height_inclusive,
                end_height_inclusive,
                cancellation_token.clone(),
            )
            .await;

        let datasource_id = DatasourceId::new_named("arch_rpc_backfill");
        while let Some(result) = rx.recv().await {
            match result {
                Ok((height, _hash, full_block)) => {
                    // Emit AccountUpdates for all writable accounts across txs in this block
                    if !full_block.transactions.is_empty() {
                        let mut writable_keys: HashSet<arch_program::pubkey::Pubkey> =
                            HashSet::new();
                        for tx in full_block.transactions.iter() {
                            let msg = &tx.runtime_transaction.message;
                            for (idx, key) in msg.account_keys.iter().enumerate() {
                                if msg.is_writable_index(idx) {
                                    writable_keys.insert(*key);
                                }
                            }
                        }

                        if !writable_keys.is_empty() {
                            let keys_vec: Vec<arch_program::pubkey::Pubkey> =
                                writable_keys.iter().cloned().collect();
                            match self.client.get_multiple_accounts(&keys_vec).await {
                                Ok(accounts_map) => {
                                    let mut updates: Vec<AccountUpdate> =
                                        Vec::with_capacity(accounts_map.len());
                                    for (pubkey, maybe_info) in accounts_map.into_iter() {
                                        if let Some(account) = maybe_info {
                                            updates.push(AccountUpdate {
                                                pubkey,
                                                account,
                                                height,
                                            });
                                        }
                                    }
                                    if !updates.is_empty() {
                                        let _ = sender
                                            .send((
                                                Updates::Accounts(updates),
                                                datasource_id.clone(),
                                            ))
                                            .await;
                                        metrics
                                            .increment_counter(
                                                "arch_rpc_backfill_account_updates_emitted",
                                                1,
                                            )
                                            .await
                                            .ok();
                                    }
                                }
                                Err(_e) => {
                                    metrics
                                        .increment_counter(
                                            "arch_rpc_backfill_account_updates_failed",
                                            1,
                                        )
                                        .await
                                        .ok();
                                }
                            }
                        }
                    }

                    // Emit block details
                    let details = BlockDetails {
                        height,
                        block_hash: Some(full_block.hash()),
                        previous_block_hash: Some(full_block.previous_block_hash),
                        block_time: Some(full_block.timestamp as i64),
                        block_height: Some(full_block.block_height),
                    };
                    let _ = sender
                        .send((Updates::BlockDetails(vec![details]), datasource_id.clone()))
                        .await;
                    metrics
                        .increment_counter("arch_rpc_backfill_block_details_emitted", 1)
                        .await
                        .ok();

                    // Emit transactions in chunks
                    let mut chunk: Vec<TransactionUpdate> = Vec::new();
                    for tx in full_block.transactions.into_iter() {
                        let update = TransactionUpdate {
                            transaction: tx,
                            height,
                        };
                        chunk.push(update);
                        if chunk.len() >= self.config.batch_emit_size {
                            let _ = sender
                                .send((
                                    Updates::Transactions(std::mem::take(&mut chunk)),
                                    datasource_id.clone(),
                                ))
                                .await;
                        }
                    }
                    if !chunk.is_empty() {
                        let _ = sender
                            .send((Updates::Transactions(chunk), datasource_id.clone()))
                            .await;
                    }
                    metrics
                        .increment_counter("arch_rpc_backfill_blocks_processed", 1)
                        .await
                        .ok();
                }
                Err(_e) => {
                    metrics
                        .increment_counter("arch_rpc_backfill_blocks_failed", 1)
                        .await
                        .ok();
                }
            }
        }

        Ok(())
    }
}
