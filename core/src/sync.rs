use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::datasource::{Datasource, DatasourceId, UpdateType, Updates};
use crate::error::IndexerResult;
use crate::metrics::MetricsCollection;

// SyncMode no longer needed; the datasource manages modes internally

#[derive(Debug, Clone)]
pub struct SyncConfig {
    pub batch_size: u64,
    /// Max number of live updates to buffer while backfilling
    pub max_live_buffer: usize,
    /// Start live stream when we are within this many blocks from tip
    pub live_start_distance: u64,
    /// Log progress (with ETA) every N backfill batches
    pub progress_log_every_batches: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            batch_size: 1_000,
            max_live_buffer: 10_000,
            live_start_distance: 128,
            progress_log_every_batches: 10,
        }
    }
}

/// Provides the current best chain height for synchronization decisions.
///
/// Implementors should return the node's best known height quickly and may
/// cache internally to avoid excessive I/O.
#[async_trait]
pub trait TipSource: Send + Sync {
    /// Returns the current best height (tip) of the canonical chain.
    async fn best_height(&self) -> IndexerResult<u64>;
}

/// Persists and retrieves the last fully indexed height.
///
/// Implementations must be durable and safe for concurrent callers.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Read the last successfully indexed height.
    async fn last_indexed_height(&self) -> IndexerResult<u64>;
    /// Persist the last successfully indexed height atomically.
    async fn set_last_indexed_height(&self, height: u64) -> IndexerResult<()>;
}

/// Produces historical updates in height ranges for backfilling.
///
/// Implementations should chunk internally as needed and respect
/// `cancellation_token` promptly. All emitted updates must have heights within
/// the requested inclusive range.
#[async_trait]
pub trait BackfillSource: Send + Sync {
    /// Backfill updates from `start_height_inclusive` to `end_height_inclusive`.
    async fn backfill_range(
        &self,
        start_height_inclusive: u64,
        end_height_inclusive: u64,
        sender: tokio::sync::mpsc::Sender<(Updates, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()>;
}

/// Streams live updates as the chain advances.
///
/// Implementations should begin from the provided height (exclusive) and emit
/// only newer updates, resuming seamlessly across restarts if possible.
#[async_trait]
pub trait LiveSource: Send + Sync {
    /// Start live streaming from the specified height (exclusive).
    async fn consume_live(
        &self,
        start_from_height_exclusive: u64,
        sender: tokio::sync::mpsc::Sender<(Updates, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()>;

    /// Advertise live update kinds this source will emit.
    fn update_types(&self) -> Vec<UpdateType>;
}

pub struct SyncingDatasource {
    tip: Arc<dyn TipSource>,
    checkpoint: Arc<dyn CheckpointStore>,
    backfill: Arc<dyn BackfillSource>,
    live: Arc<dyn LiveSource>,
    update_types: Vec<UpdateType>,
    config: SyncConfig,
}

impl SyncingDatasource {
    async fn catch_up_to_current_tip(
        &self,
        mut last_indexed: u64,
        sender: tokio::sync::mpsc::Sender<(Updates, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
        cutoff_height_value: Arc<tokio::sync::Mutex<u64>>,
    ) -> IndexerResult<u64> {
        // After finishing backfill to the initial snapshot tip, the chain may have advanced.
        // Perform a final catch-up range to the latest best height to close any gap before live.
        let best_now = self.tip.best_height().await?;
        if best_now > last_indexed {
            let start = last_indexed.saturating_add(1);
            let end = best_now;
            log::info!(
                "sync: catch-up backfill from {} to current best {} before enabling live",
                start,
                end
            );

            let (bf_tx, mut bf_rx) = mpsc::channel::<(Updates, DatasourceId)>(1_000);
            let forward_sender_clone = sender.clone();
            let forward_handle = tokio::spawn(async move {
                while let Some((u, ds)) = bf_rx.recv().await {
                    let _ = forward_sender_clone.send((u, ds)).await;
                }
            });

            self.backfill
                .backfill_range(
                    start,
                    end,
                    bf_tx,
                    cancellation_token.clone(),
                    metrics.clone(),
                )
                .await?;
            let _ = forward_handle.await;

            self.checkpoint.set_last_indexed_height(end).await?;
            last_indexed = end;
            metrics
                .update_gauge("sync_last_indexed", last_indexed as f64)
                .await?;

            // Update cutoff so buffered live events at or below end are dropped on flush
            {
                let mut guard = cutoff_height_value.lock().await;
                *guard = last_indexed;
            }
        }

        Ok(last_indexed)
    }
    pub fn new(
        tip: Arc<dyn TipSource>,
        checkpoint: Arc<dyn CheckpointStore>,
        backfill: Arc<dyn BackfillSource>,
        live: Arc<dyn LiveSource>,
        update_types: Vec<UpdateType>,
        config: SyncConfig,
    ) -> Self {
        Self {
            tip,
            checkpoint,
            backfill,
            live,
            update_types,
            config,
        }
    }
}

impl SyncingDatasource {
    async fn initialize_state_and_metrics(
        &self,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<(u64, u64, u64)> {
        let last_indexed = self.checkpoint.last_indexed_height().await?;
        let best_height = self.tip.best_height().await?;
        let safe_tip = best_height;

        metrics
            .update_gauge("sync_best_height", best_height as f64)
            .await?;
        metrics
            .update_gauge("sync_safe_tip", safe_tip as f64)
            .await?;
        metrics
            .update_gauge("sync_last_indexed", last_indexed as f64)
            .await?;

        Ok((last_indexed, best_height, safe_tip))
    }

    fn spawn_live_forwarder(
        &self,
        live_rx: tokio::sync::mpsc::Receiver<(Updates, DatasourceId)>,
        pass_through: Arc<AtomicBool>,
        cutoff_height_value: Arc<tokio::sync::Mutex<u64>>,
        forward_sender: tokio::sync::mpsc::Sender<(Updates, DatasourceId)>,
        max_buffer: usize,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
    ) {
        let pass_through_clone = Arc::clone(&pass_through);
        let cutoff_height_for_forwarder = Arc::clone(&cutoff_height_value);
        let live_token_forwarder = cancellation_token;
        let live_metrics_forwarder = metrics;

        tokio::spawn(async move {
            let mut buffer: VecDeque<(Updates, DatasourceId)> = VecDeque::new();
            let mut flushed_after_enable = false;
            let mut live_rx = live_rx;
            loop {
                tokio::select! {
                    _ = live_token_forwarder.cancelled() => {
                        break;
                    }
                    maybe_msg = live_rx.recv() => {
                        match maybe_msg {
                            Some((updates, datasource_id)) => {
                                if pass_through_clone.load(Ordering::Relaxed) {
                                    if !flushed_after_enable {
                                        let cutoff_snapshot = {
                                            let guard = cutoff_height_for_forwarder.lock().await;
                                            *guard
                                        };
                                        while let Some((mut u, ds_id)) = buffer.pop_front() {
                                            if let Some(filtered) = filter_by_cutoff(&mut u, cutoff_snapshot) {
                                                let _ = forward_sender.send((filtered, ds_id)).await;
                                            }
                                            let _ = live_metrics_forwarder.increment_counter("sync_live_buffer_flushed", 1).await;
                                        }
                                        flushed_after_enable = true;
                                    }
                                    let cutoff_snapshot = {
                                        let guard = cutoff_height_for_forwarder.lock().await;
                                        *guard
                                    };
                                    let mut u = updates;
                                    if let Some(filtered) = filter_by_cutoff(&mut u, cutoff_snapshot) {
                                        let _ = forward_sender.send((filtered, datasource_id)).await;
                                    }
                                } else {
                                    if buffer.len() >= max_buffer {
                                        buffer.pop_front();
                                        let _ = live_metrics_forwarder.increment_counter("sync_live_buffer_overflow", 1).await;
                                    }
                                    buffer.push_back((updates, datasource_id));
                                    let _ = live_metrics_forwarder.increment_counter("sync_live_buffered", 1).await;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });
    }

    fn spawn_live_stream(
        &self,
        start_from_height_exclusive: u64,
        live_tx: tokio::sync::mpsc::Sender<(Updates, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
    ) {
        let live_source = Arc::clone(&self.live);
        tokio::spawn(async move {
            let _ = live_source
                .consume_live(
                    start_from_height_exclusive,
                    live_tx,
                    cancellation_token,
                    metrics,
                )
                .await;
        });
    }

    async fn backfill_if_needed(
        &self,
        mut last_indexed: u64,
        final_target_tip: u64,
        sender: tokio::sync::mpsc::Sender<(Updates, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
        cutoff_height_value: Arc<tokio::sync::Mutex<u64>>,
    ) -> IndexerResult<u64> {
        if last_indexed < final_target_tip {
            log::info!(
                "sync: entering backfill mode from {} toward safe tip (best={}, initial_safe_tip={})",
                last_indexed + 1,
                final_target_tip,
                final_target_tip
            );

            let mut current = last_indexed.saturating_add(1);
            let mut batches_processed: u64 = 0;
            let start_time = std::time::Instant::now();
            loop {
                if cancellation_token.is_cancelled() {
                    log::info!("sync: cancellation received during backfill");
                    return Ok(last_indexed);
                }

                // Update metrics with current best, but do not move the backfill target beyond final_target_tip
                let best_height_now = self.tip.best_height().await?;
                metrics
                    .update_gauge("sync_best_height", best_height_now as f64)
                    .await?;
                metrics
                    .update_gauge("sync_safe_tip", final_target_tip as f64)
                    .await?;

                if current > final_target_tip {
                    break;
                }

                let end = std::cmp::min(
                    final_target_tip,
                    current.saturating_add(self.config.batch_size.saturating_sub(1)),
                );

                metrics
                    .increment_counter("sync_backfill_batches", 1)
                    .await?;
                batches_processed = batches_processed.saturating_add(1);

                let (bf_tx, mut bf_rx) = mpsc::channel::<(Updates, DatasourceId)>(1_000);
                let forward_sender_clone = sender.clone();
                let forward_handle = tokio::spawn(async move {
                    while let Some((u, ds)) = bf_rx.recv().await {
                        let _ = forward_sender_clone.send((u, ds)).await;
                    }
                });

                self.backfill
                    .backfill_range(
                        current,
                        end,
                        bf_tx,
                        cancellation_token.clone(),
                        metrics.clone(),
                    )
                    .await?;
                let _ = forward_handle.await;

                self.checkpoint.set_last_indexed_height(end).await?;
                last_indexed = end;
                metrics
                    .update_gauge("sync_last_indexed", last_indexed as f64)
                    .await?;

                current = end.saturating_add(1);

                // Update dynamic cutoff for the forwarder
                {
                    let mut guard = cutoff_height_value.lock().await;
                    *guard = last_indexed;
                }

                // Periodic progress log with ETA
                if batches_processed % self.config.progress_log_every_batches == 0 {
                    let elapsed = start_time.elapsed().as_secs_f64();
                    let heights_done =
                        (last_indexed.saturating_sub(current.saturating_sub(1))).max(1);
                    let total_remaining = final_target_tip.saturating_sub(last_indexed);
                    let rate_hps = (heights_done as f64 / elapsed.max(0.001)).max(0.000_001);
                    let eta_secs = (total_remaining as f64 / rate_hps).round() as u64;
                    log::info!(
                        "sync: backfill progress heights=[{}..{}], remaining={}, rate={:.0} h/s, eta={}s",
                        current.saturating_sub(self.config.batch_size.saturating_sub(1)),
                        last_indexed,
                        total_remaining,
                        rate_hps,
                        eta_secs
                    );
                }

                // Live stream is started at the beginning of sync to buffer new events.
                // We no longer defer starting live based on distance to tip here.
            }
        } else {
            log::info!(
                "sync: already at or past safe tip (last_indexed={}, safe_tip={}), entering live",
                last_indexed,
                final_target_tip
            );
        }

        Ok(last_indexed)
    }

    async fn enable_live_passthrough(
        &self,
        last_indexed: u64,
        pass_through: Arc<AtomicBool>,
        cutoff_height_value: Arc<tokio::sync::Mutex<u64>>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        // Set cutoff to the last fully backfilled height and enable pass-through
        {
            let mut guard = cutoff_height_value.lock().await;
            *guard = last_indexed;
        }
        pass_through.store(true, Ordering::Relaxed);
        metrics
            .update_gauge("sync_live_passthrough_enabled", 1.0)
            .await?;
        log::info!("sync: live pass-through enabled; backfill complete");
        Ok(())
    }
}

#[async_trait]
impl Datasource for SyncingDatasource {
    async fn consume(
        &self,
        _id: DatasourceId,
        sender: mpsc::Sender<(Updates, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        let (mut last_indexed, _best_height, safe_tip) =
            self.initialize_state_and_metrics(metrics.clone()).await?;
        let pass_through = Arc::new(AtomicBool::new(false));
        let cutoff_height_value: Arc<tokio::sync::Mutex<u64>> =
            Arc::new(tokio::sync::Mutex::new(0));
        // Prepare live channel and forwarder (buffer until pass-through)
        let (live_tx, live_rx) = mpsc::channel::<(Updates, DatasourceId)>(1_000);
        self.spawn_live_forwarder(
            live_rx,
            Arc::clone(&pass_through),
            Arc::clone(&cutoff_height_value),
            sender.clone(),
            self.config.max_live_buffer,
            cancellation_token.clone(),
            metrics.clone(),
        );
        // Start live immediately to buffer new events while we backfill.
        self.spawn_live_stream(
            last_indexed,
            live_tx.clone(),
            cancellation_token.clone(),
            metrics.clone(),
        );

        last_indexed = self
            .backfill_if_needed(
                last_indexed,
                safe_tip,
                sender.clone(),
                cancellation_token.clone(),
                metrics.clone(),
                Arc::clone(&cutoff_height_value),
            )
            .await?;

        // Before enabling passthrough, close any gap that formed while syncing to the initial snapshot tip.
        last_indexed = self
            .catch_up_to_current_tip(
                last_indexed,
                sender.clone(),
                cancellation_token.clone(),
                metrics.clone(),
                Arc::clone(&cutoff_height_value),
            )
            .await?;

        self.enable_live_passthrough(
            last_indexed,
            pass_through,
            cutoff_height_value,
            metrics.clone(),
        )
        .await?;

        // Live already started at the beginning; no action needed here.

        cancellation_token.cancelled().await;
        Ok(())
    }

    fn update_types(&self) -> Vec<UpdateType> {
        self.update_types.clone()
    }
}

// Height-based filtering without signature dedup: we rely on the height present in updates.
fn filter_by_cutoff(updates: &mut Updates, cutoff_height: u64) -> Option<Updates> {
    match updates {
        Updates::Transactions(txs) => {
            let mut filtered = Vec::new();
            for t in txs.iter() {
                if t.height > cutoff_height {
                    filtered.push(t.clone());
                }
            }
            if filtered.is_empty() {
                None
            } else {
                Some(Updates::Transactions(filtered))
            }
        }
        Updates::BlockDetails(blocks) => {
            let filtered: Vec<crate::datasource::BlockDetails> = blocks
                .iter()
                .cloned()
                .filter(|b| b.height > cutoff_height)
                .collect();
            if filtered.is_empty() {
                None
            } else {
                Some(Updates::BlockDetails(filtered))
            }
        }
        Updates::Accounts(accounts) => {
            let filtered: Vec<crate::datasource::AccountUpdate> = accounts
                .iter()
                .cloned()
                .filter(|a| a.height > cutoff_height)
                .collect();
            if filtered.is_empty() {
                None
            } else {
                Some(Updates::Accounts(filtered))
            }
        }
        Updates::AccountDeletions(deletions) => {
            let filtered: Vec<crate::datasource::AccountDeletion> = deletions
                .iter()
                .cloned()
                .filter(|d| d.height > cutoff_height)
                .collect();
            if filtered.is_empty() {
                None
            } else {
                Some(Updates::AccountDeletions(filtered))
            }
        }
        Updates::BitcoinBlocks(blocks) => {
            let filtered: Vec<crate::datasource::BitcoinBlock> = blocks
                .iter()
                .cloned()
                .filter(|b| b.block_height > cutoff_height)
                .collect();
            if filtered.is_empty() {
                None
            } else {
                Some(Updates::BitcoinBlocks(filtered))
            }
        }
        Updates::RolledbackTransactions(events) => {
            Some(Updates::RolledbackTransactions(events.clone()))
        }
        Updates::ReappliedTransactions(events) => {
            Some(Updates::ReappliedTransactions(events.clone()))
        }
    }
}
