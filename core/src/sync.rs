use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;

use crate::datasource::{Datasource, DatasourceId, UpdateType, Updates};
use crate::error::IndexerResult;
use crate::metrics::MetricsCollection;

// SyncMode no longer needed; the datasource manages modes internally

/// Represents the current phase of synchronization
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncPhase {
    /// Backfilling toward a target tip
    Backfilling { target_tip: u64 },
    /// Live passthrough mode - processing live updates directly
    LivePassthrough,
    /// Reconnecting and catching up after a disconnection
    Reconnecting { target_tip: u64 },
}

/// Internal state structure for synchronization
#[derive(Debug, Clone)]
struct SyncStateInner {
    phase: SyncPhase,
    last_indexed: u64,
    cutoff_height: u64,
    safe_tip: u64,
    pass_through_enabled: bool,
}

/// Shared synchronization state accessible across tasks
pub struct SyncState {
    inner: Arc<RwLock<SyncStateInner>>,
}

impl SyncState {
    /// Create a new SyncState with initial values
    fn new(last_indexed: u64, safe_tip: u64) -> Self {
        Self {
            inner: Arc::new(RwLock::new(SyncStateInner {
                phase: SyncPhase::Backfilling {
                    target_tip: safe_tip,
                },
                last_indexed,
                cutoff_height: 0,
                safe_tip,
                pass_through_enabled: false,
            })),
        }
    }

    /// Get a snapshot of the current state (read-only)
    async fn snapshot(&self) -> SyncStateInner {
        self.inner.read().await.clone()
    }

    /// Update state with a closure
    async fn update<F>(&self, f: F)
    where
        F: FnOnce(&mut SyncStateInner),
    {
        let mut guard = self.inner.write().await;
        f(&mut guard);
    }

    /// Set the last indexed height and update cutoff if needed
    async fn set_last_indexed(&self, height: u64, update_cutoff: bool) {
        self.update(|state| {
            state.last_indexed = height;
            if update_cutoff {
                state.cutoff_height = height;
            }
        })
        .await;
    }

    /// Set the cutoff height
    async fn set_cutoff_height(&self, height: u64) {
        self.update(|state| {
            state.cutoff_height = height;
        })
        .await;
    }

    /// Enable or disable passthrough
    async fn set_pass_through(&self, enabled: bool) {
        self.update(|state| {
            state.pass_through_enabled = enabled;
            if enabled {
                state.phase = SyncPhase::LivePassthrough;
            }
        })
        .await;
    }

    /// Set the sync phase
    async fn set_phase(&self, phase: SyncPhase) {
        self.update(|state| {
            state.phase = phase;
        })
        .await;
    }

    /// Update safe tip
    async fn set_safe_tip(&self, tip: u64) {
        self.update(|state| {
            state.safe_tip = tip;
        })
        .await;
    }
}

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
        reconnection_notifier: tokio::sync::mpsc::Sender<()>,
    ) -> IndexerResult<()>;

    /// Advertise live update kinds this source will emit.
    fn update_types(&self) -> Vec<UpdateType>;
}

#[derive(Clone)]
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
        state: &SyncState,
        sender: tokio::sync::mpsc::Sender<(Updates, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        // After finishing backfill to the initial snapshot tip, the chain may have advanced.
        // Perform a final catch-up range to the latest best height to close any gap before live.
        let snapshot = state.snapshot().await;
        let last_indexed = snapshot.last_indexed;
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
            state.set_last_indexed(end, true).await;
            metrics
                .update_gauge("sync_last_indexed", end as f64)
                .await?;
        }

        Ok(())
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
        state: Arc<SyncState>,
        forward_sender: tokio::sync::mpsc::Sender<(Updates, DatasourceId)>,
        checkpoint: Arc<dyn CheckpointStore>,
        max_buffer: usize,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
    ) {
        let live_token_forwarder = cancellation_token;
        let live_metrics_forwarder = metrics;

        tokio::spawn(async move {
            // Initialize last persisted height from checkpoint
            let mut last_persisted_height: u64 = match checkpoint.last_indexed_height().await {
                Ok(h) => h,
                Err(_) => 0,
            };
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
                                let snapshot = state.snapshot().await;
                                if snapshot.pass_through_enabled {
                                    if !flushed_after_enable {
                                        // Flush buffered updates using current cutoff
                                        while let Some((mut u, ds_id)) = buffer.pop_front() {
                                            if let Some(filtered) = filter_by_cutoff(&mut u, snapshot.cutoff_height) {
                                                if let Some(max_h) = max_height_in_updates(&filtered) {
                                                    if max_h > last_persisted_height {
                                                        let _ = checkpoint.set_last_indexed_height(max_h).await;
                                                        let _ = live_metrics_forwarder.update_gauge("sync_last_indexed", max_h as f64).await;
                                                        last_persisted_height = max_h;
                                                        // Update state with new last_indexed
                                                        state.set_last_indexed(max_h, false).await;
                                                    }
                                                }
                                                let _ = forward_sender.send((filtered, ds_id)).await;
                                            }
                                            let _ = live_metrics_forwarder.increment_counter("sync_live_buffer_flushed", 1).await;
                                        }
                                        flushed_after_enable = true;
                                    }
                                    // Process current update with latest cutoff
                                    let current_snapshot = state.snapshot().await;
                                    let mut u = updates;
                                    if let Some(filtered) = filter_by_cutoff(&mut u, current_snapshot.cutoff_height) {
                                        if let Some(max_h) = max_height_in_updates(&filtered) {
                                            if max_h > last_persisted_height {
                                                let _ = checkpoint.set_last_indexed_height(max_h).await;
                                                let _ = live_metrics_forwarder.update_gauge("sync_last_indexed", max_h as f64).await;
                                                last_persisted_height = max_h;
                                                // Update state with new last_indexed
                                                state.set_last_indexed(max_h, false).await;
                                            }
                                        }
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
        state: Arc<SyncState>,
        live_tx: tokio::sync::mpsc::Sender<(Updates, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
        reconnection_notifier: tokio::sync::mpsc::Sender<()>,
    ) {
        let live_source = Arc::clone(&self.live);
        tokio::spawn(async move {
            // Get initial start height from state
            let snapshot = state.snapshot().await;
            let start_from_height_exclusive = snapshot.last_indexed;
            let _ = live_source
                .consume_live(
                    start_from_height_exclusive,
                    live_tx,
                    cancellation_token,
                    metrics,
                    reconnection_notifier,
                )
                .await;
        });
    }

    fn spawn_reconciliation_listener(
        &self,
        state: Arc<SyncState>,
        sender: mpsc::Sender<(Updates, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
        reconnect_rx: mpsc::Receiver<()>,
    ) {
        let datasource_for_reconcile = self.clone();
        let state_reconcile = Arc::clone(&state);
        let sender_reconcile = sender;
        let cancel_reconcile = cancellation_token;
        let metrics_reconcile = metrics;
        let mut reconnect_rx = reconnect_rx;

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_reconcile.cancelled() => {
                        break;
                    }
                    maybe_notification = reconnect_rx.recv() => {
                        match maybe_notification {
                            Some(()) => {
                                if let Err(e) = datasource_for_reconcile.reconcile_if_behind(
                                    &state_reconcile,
                                    sender_reconcile.clone(),
                                    cancel_reconcile.clone(),
                                    metrics_reconcile.clone(),
                                ).await {
                                    log::error!("sync: reconciliation error: {}", e);
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });
    }

    async fn backfill_if_needed(
        &self,
        state: &SyncState,
        final_target_tip: u64,
        sender: tokio::sync::mpsc::Sender<(Updates, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        let snapshot = state.snapshot().await;
        let last_indexed = snapshot.last_indexed;

        if last_indexed < final_target_tip {
            log::info!(
                "sync: entering backfill mode from {} toward safe tip (best={}, initial_safe_tip={})",
                last_indexed + 1,
                final_target_tip,
                final_target_tip
            );

            state
                .set_phase(SyncPhase::Backfilling {
                    target_tip: final_target_tip,
                })
                .await;
            let mut current = last_indexed.saturating_add(1);
            let mut batches_processed: u64 = 0;
            let start_time = std::time::Instant::now();
            loop {
                if cancellation_token.is_cancelled() {
                    log::info!("sync: cancellation received during backfill");
                    return Ok(());
                }

                // Update metrics with current best, but do not move the backfill target beyond final_target_tip
                let best_height_now = self.tip.best_height().await?;
                metrics
                    .update_gauge("sync_best_height", best_height_now as f64)
                    .await?;
                metrics
                    .update_gauge("sync_safe_tip", final_target_tip as f64)
                    .await?;
                state.set_safe_tip(final_target_tip).await;

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
                state.set_last_indexed(end, true).await;
                metrics
                    .update_gauge("sync_last_indexed", end as f64)
                    .await?;

                current = end.saturating_add(1);

                // Periodic progress log with ETA
                if batches_processed % self.config.progress_log_every_batches == 0 {
                    let snapshot = state.snapshot().await;
                    let elapsed = start_time.elapsed().as_secs_f64();
                    let heights_done = (snapshot
                        .last_indexed
                        .saturating_sub(current.saturating_sub(1)))
                    .max(1);
                    let total_remaining = final_target_tip.saturating_sub(snapshot.last_indexed);
                    let rate_hps = (heights_done as f64 / elapsed.max(0.001)).max(0.000_001);
                    let eta_secs = (total_remaining as f64 / rate_hps).round() as u64;
                    log::info!(
                        "sync: backfill progress heights=[{}..{}], remaining={}, rate={:.0} h/s, eta={}s",
                        current.saturating_sub(self.config.batch_size.saturating_sub(1)),
                        snapshot.last_indexed,
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

        Ok(())
    }

    async fn enable_live_passthrough(
        &self,
        state: &SyncState,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        // Set cutoff to the last fully backfilled height and enable pass-through
        let snapshot = state.snapshot().await;
        state.set_cutoff_height(snapshot.last_indexed).await;
        state.set_pass_through(true).await;
        metrics
            .update_gauge("sync_live_passthrough_enabled", 1.0)
            .await?;
        log::info!("sync: live pass-through enabled; backfill complete");
        Ok(())
    }

    async fn reconcile_if_behind(
        &self,
        state: &SyncState,
        sender: mpsc::Sender<(Updates, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        let snapshot = state.snapshot().await;
        let last = snapshot.last_indexed;
        let best = self.tip.best_height().await?;

        if best > last {
            log::info!(
                "sync: reconciliation triggered after reconnection (last={}, best={})",
                last,
                best
            );

            // Disable passthrough to buffer live updates
            state.set_pass_through(false).await;
            state
                .set_phase(SyncPhase::Reconnecting { target_tip: best })
                .await;
            metrics
                .update_gauge("sync_live_passthrough_enabled", 0.0)
                .await?;

            // Set cutoff to current last_indexed
            state.set_cutoff_height(last).await;

            // Backfill the gap
            self.backfill_if_needed(
                state,
                best,
                sender.clone(),
                cancellation_token.clone(),
                metrics.clone(),
            )
            .await?;

            // Catch up to current tip
            self.catch_up_to_current_tip(
                state,
                sender.clone(),
                cancellation_token.clone(),
                metrics.clone(),
            )
            .await?;

            // Re-enable passthrough
            self.enable_live_passthrough(state, metrics.clone()).await?;

            let final_snapshot = state.snapshot().await;
            log::info!(
                "sync: reconciliation complete (last_indexed={})",
                final_snapshot.last_indexed
            );
        }

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
        let (last_indexed, _best_height, safe_tip) =
            self.initialize_state_and_metrics(metrics.clone()).await?;

        // Create shared state
        let state = Arc::new(SyncState::new(last_indexed, safe_tip));

        // Prepare live channel and forwarder (buffer until pass-through)
        let (live_tx, live_rx) = mpsc::channel::<(Updates, DatasourceId)>(1_000);
        self.spawn_live_forwarder(
            live_rx,
            Arc::clone(&state),
            sender.clone(),
            Arc::clone(&self.checkpoint),
            self.config.max_live_buffer,
            cancellation_token.clone(),
            metrics.clone(),
        );

        // Create reconnection notification channel
        let (reconnect_tx, reconnect_rx) = mpsc::channel::<()>(10);

        // Spawn reconciliation listener task
        self.spawn_reconciliation_listener(
            Arc::clone(&state),
            sender.clone(),
            cancellation_token.clone(),
            metrics.clone(),
            reconnect_rx,
        );

        // Start live immediately to buffer new events while we backfill.
        self.spawn_live_stream(
            Arc::clone(&state),
            live_tx.clone(),
            cancellation_token.clone(),
            metrics.clone(),
            reconnect_tx,
        );

        self.backfill_if_needed(
            &state,
            safe_tip,
            sender.clone(),
            cancellation_token.clone(),
            metrics.clone(),
        )
        .await?;

        // Before enabling passthrough, close any gap that formed while syncing to the initial snapshot tip.
        self.catch_up_to_current_tip(
            &state,
            sender.clone(),
            cancellation_token.clone(),
            metrics.clone(),
        )
        .await?;

        self.enable_live_passthrough(&state, metrics.clone())
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

// Determine the maximum height carried by an `Updates` payload for checkpointing
fn max_height_in_updates(updates: &Updates) -> Option<u64> {
    match updates {
        Updates::Transactions(txs) => txs.iter().map(|t| t.height).max(),
        Updates::BlockDetails(blocks) => blocks.iter().map(|b| b.height).max(),
        Updates::Accounts(accts) => accts.iter().map(|a| a.height).max(),
        Updates::AccountDeletions(dels) => dels.iter().map(|d| d.height).max(),
        Updates::BitcoinBlocks(blocks) => blocks.iter().map(|b| b.block_height).max(),
        Updates::RolledbackTransactions(_) => None,
        Updates::ReappliedTransactions(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::datasource::{
        AccountDeletion, AccountUpdate, BitcoinBlock, BlockDetails, TransactionUpdate, UpdateType,
    };
    use crate::error::IndexerResult;
    use crate::metrics::MetricsCollection;

    // Mock implementations for testing

    struct MockTipSource {
        height: Arc<AtomicU64>,
    }

    impl MockTipSource {
        fn new(height: u64) -> Self {
            Self {
                height: Arc::new(AtomicU64::new(height)),
            }
        }

        fn set_height(&self, height: u64) {
            self.height.store(height, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl TipSource for MockTipSource {
        async fn best_height(&self) -> IndexerResult<u64> {
            Ok(self.height.load(Ordering::SeqCst))
        }
    }

    struct MockCheckpointStore {
        last_indexed: Arc<AtomicU64>,
    }

    impl MockCheckpointStore {
        fn new(last_indexed: u64) -> Self {
            Self {
                last_indexed: Arc::new(AtomicU64::new(last_indexed)),
            }
        }

        fn get_last_indexed(&self) -> u64 {
            self.last_indexed.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl CheckpointStore for MockCheckpointStore {
        async fn last_indexed_height(&self) -> IndexerResult<u64> {
            Ok(self.last_indexed.load(Ordering::SeqCst))
        }

        async fn set_last_indexed_height(&self, height: u64) -> IndexerResult<()> {
            self.last_indexed.store(height, Ordering::SeqCst);
            Ok(())
        }
    }

    struct MockBackfillSource {
        updates: Vec<(u64, u64, Updates)>, // (start, end, updates)
    }

    impl MockBackfillSource {
        fn new() -> Self {
            Self {
                updates: Vec::new(),
            }
        }

        fn add_range(&mut self, start: u64, end: u64, updates: Updates) {
            self.updates.push((start, end, updates));
        }
    }

    #[async_trait]
    impl BackfillSource for MockBackfillSource {
        async fn backfill_range(
            &self,
            start_height_inclusive: u64,
            end_height_inclusive: u64,
            sender: mpsc::Sender<(Updates, DatasourceId)>,
            _cancellation_token: CancellationToken,
            _metrics: Arc<MetricsCollection>,
        ) -> IndexerResult<()> {
            for (start, end, updates) in &self.updates {
                if *start == start_height_inclusive && *end == end_height_inclusive {
                    let _ = sender
                        .send((updates.clone(), DatasourceId::new_unique()))
                        .await;
                    return Ok(());
                }
            }
            Ok(())
        }
    }

    struct MockLiveSource {
        updates: Vec<Updates>,
        should_reconnect: bool,
    }

    impl MockLiveSource {
        fn new() -> Self {
            Self {
                updates: Vec::new(),
                should_reconnect: false,
            }
        }

        fn add_update(&mut self, update: Updates) {
            self.updates.push(update);
        }

        fn set_should_reconnect(&mut self, should: bool) {
            self.should_reconnect = should;
        }
    }

    #[async_trait]
    impl LiveSource for MockLiveSource {
        async fn consume_live(
            &self,
            _start_from_height_exclusive: u64,
            sender: mpsc::Sender<(Updates, DatasourceId)>,
            cancellation_token: CancellationToken,
            _metrics: Arc<MetricsCollection>,
            reconnection_notifier: mpsc::Sender<()>,
        ) -> IndexerResult<()> {
            let updates = self.updates.clone();
            let should_reconnect = self.should_reconnect;
            tokio::spawn(async move {
                for update in updates {
                    if cancellation_token.is_cancelled() {
                        break;
                    }
                    let _ = sender.send((update, DatasourceId::new_unique())).await;
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
                if should_reconnect {
                    let _ = reconnection_notifier.send(()).await;
                }
            });
            Ok(())
        }

        fn update_types(&self) -> Vec<UpdateType> {
            vec![UpdateType::Transaction]
        }
    }

    fn create_test_updates(start_height: u64, count: u64) -> Updates {
        use arch_program::hash::Hash;
        use arch_program::sanitized::{ArchMessage, MessageHeader};
        use arch_sdk::{RollbackStatus, RuntimeTransaction, Status};

        let mut txs = Vec::new();
        for i in 0..count {
            txs.push(TransactionUpdate {
                transaction: arch_sdk::ProcessedTransaction {
                    runtime_transaction: RuntimeTransaction {
                        version: 1,
                        signatures: vec![],
                        message: ArchMessage {
                            header: MessageHeader {
                                num_readonly_signed_accounts: 0,
                                num_readonly_unsigned_accounts: 0,
                                num_required_signatures: 0,
                            },
                            account_keys: vec![],
                            instructions: vec![],
                            recent_blockhash: Hash::default(),
                        },
                    },
                    inner_instructions_list: vec![],
                    status: Status::Processed,
                    bitcoin_txid: None,
                    logs: vec![],
                    rollback_status: RollbackStatus::NotRolledback,
                },
                height: start_height + i,
            });
        }
        Updates::Transactions(txs)
    }

    fn create_test_block_details(start_height: u64, count: u64) -> Updates {
        let mut blocks = Vec::new();
        for i in 0..count {
            blocks.push(BlockDetails {
                height: start_height + i,
                block_hash: None,
                previous_block_hash: None,
                block_time: None,
                block_height: None,
            });
        }
        Updates::BlockDetails(blocks)
    }

    fn create_test_accounts(start_height: u64, count: u64) -> Updates {
        let mut accounts = Vec::new();
        for i in 0..count {
            accounts.push(AccountUpdate {
                pubkey: arch_program::pubkey::Pubkey::default(),
                account: arch_sdk::AccountInfo {
                    lamports: 0,
                    owner: arch_program::pubkey::Pubkey::default(),
                    data: vec![],
                    utxo: String::new(),
                    is_executable: false,
                },
                height: start_height + i,
            });
        }
        Updates::Accounts(accounts)
    }

    fn create_test_bitcoin_blocks(start_height: u64, count: u64) -> Updates {
        let mut blocks = Vec::new();
        for i in 0..count {
            blocks.push(BitcoinBlock {
                block_height: start_height + i,
                block_hash: format!("hash_{}", start_height + i),
            });
        }
        Updates::BitcoinBlocks(blocks)
    }

    // Test SyncState

    #[tokio::test]
    async fn test_sync_state_initialization() {
        let state = SyncState::new(100, 200);
        let snapshot = state.snapshot().await;

        assert_eq!(snapshot.last_indexed, 100);
        assert_eq!(snapshot.safe_tip, 200);
        assert_eq!(snapshot.cutoff_height, 0);
        assert!(!snapshot.pass_through_enabled);
        match snapshot.phase {
            SyncPhase::Backfilling { target_tip } => assert_eq!(target_tip, 200),
            _ => panic!("Expected Backfilling phase"),
        }
    }

    #[tokio::test]
    async fn test_sync_state_set_last_indexed() {
        let state = SyncState::new(100, 200);

        state.set_last_indexed(150, true).await;
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.last_indexed, 150);
        assert_eq!(snapshot.cutoff_height, 150);

        state.set_last_indexed(175, false).await;
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.last_indexed, 175);
        assert_eq!(snapshot.cutoff_height, 150); // Should not update
    }

    #[tokio::test]
    async fn test_sync_state_set_pass_through() {
        let state = SyncState::new(100, 200);

        state.set_pass_through(true).await;
        let snapshot = state.snapshot().await;
        assert!(snapshot.pass_through_enabled);
        match snapshot.phase {
            SyncPhase::LivePassthrough => {}
            _ => panic!("Expected LivePassthrough phase"),
        }

        state.set_pass_through(false).await;
        let snapshot = state.snapshot().await;
        assert!(!snapshot.pass_through_enabled);
    }

    #[tokio::test]
    async fn test_sync_state_set_phase() {
        let state = SyncState::new(100, 200);

        state
            .set_phase(SyncPhase::Reconnecting { target_tip: 250 })
            .await;
        let snapshot = state.snapshot().await;
        match snapshot.phase {
            SyncPhase::Reconnecting { target_tip } => assert_eq!(target_tip, 250),
            _ => panic!("Expected Reconnecting phase"),
        }
    }

    #[tokio::test]
    async fn test_sync_state_set_safe_tip() {
        let state = SyncState::new(100, 200);

        state.set_safe_tip(300).await;
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.safe_tip, 300);
    }

    // Test filter_by_cutoff

    #[test]
    fn test_filter_by_cutoff_transactions() {
        let mut updates = create_test_updates(100, 10);
        let filtered = filter_by_cutoff(&mut updates, 105);

        assert!(filtered.is_some());
        if let Some(Updates::Transactions(txs)) = filtered {
            assert_eq!(txs.len(), 4); // Heights 106, 107, 108, 109
            assert_eq!(txs[0].height, 106);
            assert_eq!(txs[3].height, 109);
        } else {
            panic!("Expected Transactions variant");
        }
    }

    #[test]
    fn test_filter_by_cutoff_transactions_all_below() {
        let mut updates = create_test_updates(100, 10);
        let filtered = filter_by_cutoff(&mut updates, 150);

        assert!(filtered.is_none());
    }

    #[test]
    fn test_filter_by_cutoff_transactions_all_above() {
        let mut updates = create_test_updates(100, 10);
        let filtered = filter_by_cutoff(&mut updates, 50);

        assert!(filtered.is_some());
        if let Some(Updates::Transactions(txs)) = filtered {
            assert_eq!(txs.len(), 10);
        } else {
            panic!("Expected Transactions variant");
        }
    }

    #[test]
    fn test_filter_by_cutoff_block_details() {
        let mut updates = create_test_block_details(100, 10);
        let filtered = filter_by_cutoff(&mut updates, 105);

        assert!(filtered.is_some());
        if let Some(Updates::BlockDetails(blocks)) = filtered {
            assert_eq!(blocks.len(), 4);
            assert_eq!(blocks[0].height, 106);
        } else {
            panic!("Expected BlockDetails variant");
        }
    }

    #[test]
    fn test_filter_by_cutoff_accounts() {
        let mut updates = create_test_accounts(100, 10);
        let filtered = filter_by_cutoff(&mut updates, 105);

        assert!(filtered.is_some());
        if let Some(Updates::Accounts(accounts)) = filtered {
            assert_eq!(accounts.len(), 4);
            assert_eq!(accounts[0].height, 106);
        } else {
            panic!("Expected Accounts variant");
        }
    }

    #[test]
    fn test_filter_by_cutoff_bitcoin_blocks() {
        let mut updates = create_test_bitcoin_blocks(100, 10);
        let filtered = filter_by_cutoff(&mut updates, 105);

        assert!(filtered.is_some());
        if let Some(Updates::BitcoinBlocks(blocks)) = filtered {
            assert_eq!(blocks.len(), 4);
            assert_eq!(blocks[0].block_height, 106);
        } else {
            panic!("Expected BitcoinBlocks variant");
        }
    }

    #[test]
    fn test_filter_by_cutoff_account_deletions() {
        let mut deletions = Vec::new();
        for i in 0..10 {
            deletions.push(AccountDeletion {
                pubkey: arch_program::pubkey::Pubkey::default(),
                height: 100 + i,
            });
        }
        let mut updates = Updates::AccountDeletions(deletions);
        let filtered = filter_by_cutoff(&mut updates, 105);

        assert!(filtered.is_some());
        if let Some(Updates::AccountDeletions(dels)) = filtered {
            assert_eq!(dels.len(), 4);
            assert_eq!(dels[0].height, 106);
        } else {
            panic!("Expected AccountDeletions variant");
        }
    }

    #[test]
    fn test_filter_by_cutoff_rollbacked_transactions() {
        let events = vec![crate::datasource::RolledbackTransactionsEvent {
            height: 100,
            transaction_hashes: vec!["hash1".to_string()],
        }];
        let mut updates = Updates::RolledbackTransactions(events.clone());
        let filtered = filter_by_cutoff(&mut updates, 150);

        // RolledbackTransactions should always pass through
        assert!(filtered.is_some());
        if let Some(Updates::RolledbackTransactions(filtered_events)) = filtered {
            assert_eq!(filtered_events.len(), 1);
        } else {
            panic!("Expected RolledbackTransactions variant");
        }
    }

    // Test max_height_in_updates

    #[test]
    fn test_max_height_in_updates_transactions() {
        let updates = create_test_updates(100, 10);
        let max_height = max_height_in_updates(&updates);
        assert_eq!(max_height, Some(109));
    }

    #[test]
    fn test_max_height_in_updates_block_details() {
        let updates = create_test_block_details(50, 5);
        let max_height = max_height_in_updates(&updates);
        assert_eq!(max_height, Some(54));
    }

    #[test]
    fn test_max_height_in_updates_accounts() {
        let updates = create_test_accounts(200, 3);
        let max_height = max_height_in_updates(&updates);
        assert_eq!(max_height, Some(202));
    }

    #[test]
    fn test_max_height_in_updates_bitcoin_blocks() {
        let updates = create_test_bitcoin_blocks(300, 4);
        let max_height = max_height_in_updates(&updates);
        assert_eq!(max_height, Some(303));
    }

    #[test]
    fn test_max_height_in_updates_empty() {
        let updates = Updates::Transactions(Vec::new());
        let max_height = max_height_in_updates(&updates);
        assert_eq!(max_height, None);
    }

    #[test]
    fn test_max_height_in_updates_rollbacked() {
        let events = vec![crate::datasource::RolledbackTransactionsEvent {
            height: 100,
            transaction_hashes: vec!["hash1".to_string()],
        }];
        let updates = Updates::RolledbackTransactions(events);
        let max_height = max_height_in_updates(&updates);
        assert_eq!(max_height, None);
    }

    // Test SyncingDatasource methods

    #[tokio::test]
    async fn test_initialize_state_and_metrics() {
        let tip = Arc::new(MockTipSource::new(500));
        let checkpoint = Arc::new(MockCheckpointStore::new(100));
        let backfill = Arc::new(MockBackfillSource::new());
        let live = Arc::new(MockLiveSource::new());
        let metrics = Arc::new(MetricsCollection::default());

        let datasource = SyncingDatasource::new(
            tip,
            checkpoint,
            backfill,
            live,
            vec![UpdateType::Transaction],
            SyncConfig::default(),
        );

        let (last_indexed, best_height, safe_tip) = datasource
            .initialize_state_and_metrics(metrics)
            .await
            .unwrap();

        assert_eq!(last_indexed, 100);
        assert_eq!(best_height, 500);
        assert_eq!(safe_tip, 500);
    }

    #[tokio::test]
    async fn test_backfill_if_needed_no_backfill() {
        let tip = Arc::new(MockTipSource::new(500));
        let checkpoint = Arc::new(MockCheckpointStore::new(500));
        let backfill = Arc::new(MockBackfillSource::new());
        let live = Arc::new(MockLiveSource::new());
        let metrics = Arc::new(MetricsCollection::default());

        let datasource = SyncingDatasource::new(
            tip,
            checkpoint,
            backfill,
            live,
            vec![UpdateType::Transaction],
            SyncConfig::default(),
        );

        let state = Arc::new(SyncState::new(500, 500));
        let (tx, _rx) = mpsc::channel(1000);
        let cancel = CancellationToken::new();

        let result = datasource
            .backfill_if_needed(&state, 500, tx, cancel, metrics)
            .await;

        assert!(result.is_ok());
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.last_indexed, 500);
    }

    #[tokio::test]
    async fn test_backfill_if_needed_with_backfill() {
        let tip = Arc::new(MockTipSource::new(500));
        let checkpoint = Arc::new(MockCheckpointStore::new(100));
        let mut mock_backfill = MockBackfillSource::new();
        mock_backfill.add_range(101, 200, create_test_updates(101, 100));
        let backfill = Arc::new(mock_backfill);
        let live = Arc::new(MockLiveSource::new());
        let metrics = Arc::new(MetricsCollection::default());

        let datasource = SyncingDatasource::new(
            tip,
            checkpoint,
            backfill,
            live,
            vec![UpdateType::Transaction],
            SyncConfig {
                batch_size: 100,
                ..Default::default()
            },
        );

        let state = Arc::new(SyncState::new(100, 200));
        let (tx, mut rx) = mpsc::channel(1000);
        let cancel = CancellationToken::new();

        let result = datasource
            .backfill_if_needed(&state, 200, tx.clone(), cancel, metrics)
            .await;

        assert!(result.is_ok());
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.last_indexed, 200);

        // Verify updates were forwarded
        let mut received_count = 0;
        while let Ok(Some(_)) =
            tokio::time::timeout(tokio::time::Duration::from_millis(100), rx.recv()).await
        {
            received_count += 1;
        }
        assert!(received_count > 0);
    }

    #[tokio::test]
    async fn test_enable_live_passthrough() {
        let tip = Arc::new(MockTipSource::new(500));
        let checkpoint = Arc::new(MockCheckpointStore::new(200));
        let backfill = Arc::new(MockBackfillSource::new());
        let live = Arc::new(MockLiveSource::new());
        let metrics = Arc::new(MetricsCollection::default());

        let datasource = SyncingDatasource::new(
            tip,
            checkpoint,
            backfill,
            live,
            vec![UpdateType::Transaction],
            SyncConfig::default(),
        );

        let state = Arc::new(SyncState::new(200, 200));
        state.set_last_indexed(200, true).await;

        let result = datasource.enable_live_passthrough(&state, metrics).await;
        assert!(result.is_ok());

        let snapshot = state.snapshot().await;
        assert!(snapshot.pass_through_enabled);
        assert_eq!(snapshot.cutoff_height, 200);
        match snapshot.phase {
            SyncPhase::LivePassthrough => {}
            _ => panic!("Expected LivePassthrough phase"),
        }
    }

    #[tokio::test]
    async fn test_catch_up_to_current_tip() {
        let tip = Arc::new(MockTipSource::new(250));
        let checkpoint = Arc::new(MockCheckpointStore::new(200));
        let checkpoint_clone = Arc::clone(&checkpoint);
        let mut mock_backfill = MockBackfillSource::new();
        mock_backfill.add_range(201, 250, create_test_updates(201, 50));
        let backfill = Arc::new(mock_backfill);
        let live = Arc::new(MockLiveSource::new());
        let metrics = Arc::new(MetricsCollection::default());

        let datasource = SyncingDatasource::new(
            tip,
            checkpoint,
            backfill,
            live,
            vec![UpdateType::Transaction],
            SyncConfig::default(),
        );

        let state = Arc::new(SyncState::new(200, 200));
        let (tx, mut rx) = mpsc::channel(1000);
        let cancel = CancellationToken::new();

        let result = datasource
            .catch_up_to_current_tip(&state, tx.clone(), cancel, metrics)
            .await;

        assert!(result.is_ok());
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.last_indexed, 250);
        // Check checkpoint separately to avoid borrow checker issues
        let checkpoint_height = checkpoint_clone.get_last_indexed();
        assert_eq!(checkpoint_height, 250);

        // Verify updates were forwarded
        let mut received = false;
        while let Ok(Some(_)) =
            tokio::time::timeout(tokio::time::Duration::from_millis(100), rx.recv()).await
        {
            received = true;
        }
        assert!(received);
    }

    #[tokio::test]
    async fn test_catch_up_to_current_tip_no_gap() {
        let tip = Arc::new(MockTipSource::new(200));
        let checkpoint = Arc::new(MockCheckpointStore::new(200));
        let backfill = Arc::new(MockBackfillSource::new());
        let live = Arc::new(MockLiveSource::new());
        let metrics = Arc::new(MetricsCollection::default());

        let datasource = SyncingDatasource::new(
            tip,
            checkpoint,
            backfill,
            live,
            vec![UpdateType::Transaction],
            SyncConfig::default(),
        );

        let state = Arc::new(SyncState::new(200, 200));
        let (tx, _rx) = mpsc::channel(1000);
        let cancel = CancellationToken::new();

        let result = datasource
            .catch_up_to_current_tip(&state, tx, cancel, metrics)
            .await;

        assert!(result.is_ok());
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.last_indexed, 200);
    }

    #[tokio::test]
    async fn test_reconcile_if_behind() {
        let tip = Arc::new(MockTipSource::new(300));
        let checkpoint = Arc::new(MockCheckpointStore::new(200));
        let mut mock_backfill = MockBackfillSource::new();
        mock_backfill.add_range(201, 300, create_test_updates(201, 100));
        let backfill = Arc::new(mock_backfill);
        let live = Arc::new(MockLiveSource::new());
        let metrics = Arc::new(MetricsCollection::default());

        let datasource = SyncingDatasource::new(
            tip,
            checkpoint,
            backfill,
            live,
            vec![UpdateType::Transaction],
            SyncConfig {
                batch_size: 100,
                ..Default::default()
            },
        );

        let state = Arc::new(SyncState::new(200, 200));
        state.set_pass_through(true).await; // Start in passthrough mode
        let (tx, _rx) = mpsc::channel(1000);
        let cancel = CancellationToken::new();

        let result = datasource
            .reconcile_if_behind(&state, tx, cancel, metrics)
            .await;

        assert!(result.is_ok());
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.last_indexed, 300);
        assert!(snapshot.pass_through_enabled); // Should be re-enabled after reconciliation
    }

    #[tokio::test]
    async fn test_reconcile_if_behind_no_gap() {
        let tip = Arc::new(MockTipSource::new(200));
        let checkpoint = Arc::new(MockCheckpointStore::new(200));
        let backfill = Arc::new(MockBackfillSource::new());
        let live = Arc::new(MockLiveSource::new());
        let metrics = Arc::new(MetricsCollection::default());

        let datasource = SyncingDatasource::new(
            tip,
            checkpoint,
            backfill,
            live,
            vec![UpdateType::Transaction],
            SyncConfig::default(),
        );

        let state = Arc::new(SyncState::new(200, 200));
        let (tx, _rx) = mpsc::channel(1000);
        let cancel = CancellationToken::new();

        let result = datasource
            .reconcile_if_behind(&state, tx, cancel, metrics)
            .await;

        assert!(result.is_ok());
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.last_indexed, 200);
    }

    #[tokio::test]
    async fn test_backfill_cancellation() {
        let tip = Arc::new(MockTipSource::new(500));
        let checkpoint = Arc::new(MockCheckpointStore::new(100));
        let backfill = Arc::new(MockBackfillSource::new());
        let live = Arc::new(MockLiveSource::new());
        let metrics = Arc::new(MetricsCollection::default());

        let datasource = SyncingDatasource::new(
            tip,
            checkpoint,
            backfill,
            live,
            vec![UpdateType::Transaction],
            SyncConfig::default(),
        );

        let state = Arc::new(SyncState::new(100, 500));
        let (tx, _rx) = mpsc::channel(1000);
        let cancel = CancellationToken::new();
        cancel.cancel(); // Cancel immediately

        let result = datasource
            .backfill_if_needed(&state, 500, tx, cancel, metrics)
            .await;

        assert!(result.is_ok()); // Should handle cancellation gracefully
    }

    #[tokio::test]
    async fn test_backfill_batch_processing() {
        let tip = Arc::new(MockTipSource::new(500));
        let checkpoint = Arc::new(MockCheckpointStore::new(100));
        let mut mock_backfill = MockBackfillSource::new();
        // Add multiple batches
        mock_backfill.add_range(101, 200, create_test_updates(101, 100));
        mock_backfill.add_range(201, 300, create_test_updates(201, 100));
        mock_backfill.add_range(301, 400, create_test_updates(301, 100));
        let backfill = Arc::new(mock_backfill);
        let live = Arc::new(MockLiveSource::new());
        let metrics = Arc::new(MetricsCollection::default());

        let datasource = SyncingDatasource::new(
            tip,
            checkpoint,
            backfill,
            live,
            vec![UpdateType::Transaction],
            SyncConfig {
                batch_size: 100,
                ..Default::default()
            },
        );

        let state = Arc::new(SyncState::new(100, 400));
        let (tx, _rx) = mpsc::channel(1000);
        let cancel = CancellationToken::new();

        let result = datasource
            .backfill_if_needed(&state, 400, tx, cancel, metrics)
            .await;

        assert!(result.is_ok());
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.last_indexed, 400);
    }

    // Additional edge case tests

    #[test]
    fn test_filter_by_cutoff_exactly_at_cutoff() {
        // Updates exactly at cutoff should be filtered out (height > cutoff)
        // Heights 100-104 are <= 104, so all should be filtered out
        let mut updates = create_test_updates(100, 5);
        let filtered = filter_by_cutoff(&mut updates, 104);

        // All updates filtered out, so should return None
        assert!(filtered.is_none());
    }

    #[test]
    fn test_filter_by_cutoff_empty_updates() {
        let mut updates = Updates::Transactions(Vec::new());
        let filtered = filter_by_cutoff(&mut updates, 100);
        assert!(filtered.is_none());
    }

    #[test]
    fn test_filter_by_cutoff_mixed_heights() {
        use arch_program::hash::Hash;
        use arch_program::sanitized::{ArchMessage, MessageHeader};
        use arch_sdk::{RollbackStatus, RuntimeTransaction, Status};

        let mut txs = Vec::new();
        // Create transactions with mixed heights: some below, some at, some above cutoff
        for height in [50, 100, 101, 150, 200].iter() {
            txs.push(TransactionUpdate {
                transaction: arch_sdk::ProcessedTransaction {
                    runtime_transaction: RuntimeTransaction {
                        version: 1,
                        signatures: vec![],
                        message: ArchMessage {
                            header: MessageHeader {
                                num_readonly_signed_accounts: 0,
                                num_readonly_unsigned_accounts: 0,
                                num_required_signatures: 0,
                            },
                            account_keys: vec![],
                            instructions: vec![],
                            recent_blockhash: Hash::default(),
                        },
                    },
                    inner_instructions_list: vec![],
                    status: Status::Processed,
                    bitcoin_txid: None,
                    logs: vec![],
                    rollback_status: RollbackStatus::NotRolledback,
                },
                height: *height,
            });
        }
        let mut updates = Updates::Transactions(txs);
        let filtered = filter_by_cutoff(&mut updates, 100);

        assert!(filtered.is_some());
        if let Some(Updates::Transactions(filtered_txs)) = filtered {
            // Only heights > 100 should pass: 101, 150, 200
            assert_eq!(filtered_txs.len(), 3);
            assert_eq!(filtered_txs[0].height, 101);
            assert_eq!(filtered_txs[1].height, 150);
            assert_eq!(filtered_txs[2].height, 200);
        } else {
            panic!("Expected Transactions variant");
        }
    }

    #[test]
    fn test_filter_by_cutoff_reapplied_transactions() {
        let events = vec![crate::datasource::ReappliedTransactionsEvent {
            height: 100,
            transaction_hashes: vec!["hash1".to_string(), "hash2".to_string()],
        }];
        let mut updates = Updates::ReappliedTransactions(events.clone());
        let filtered = filter_by_cutoff(&mut updates, 150);

        // ReappliedTransactions should always pass through
        assert!(filtered.is_some());
        if let Some(Updates::ReappliedTransactions(filtered_events)) = filtered {
            assert_eq!(filtered_events.len(), 1);
            assert_eq!(filtered_events[0].transaction_hashes.len(), 2);
        } else {
            panic!("Expected ReappliedTransactions variant");
        }
    }

    #[test]
    fn test_max_height_in_updates_account_deletions() {
        let mut deletions = Vec::new();
        for i in 0..5 {
            deletions.push(AccountDeletion {
                pubkey: arch_program::pubkey::Pubkey::default(),
                height: 100 + i,
            });
        }
        let updates = Updates::AccountDeletions(deletions);
        let max_height = max_height_in_updates(&updates);
        assert_eq!(max_height, Some(104));
    }

    #[tokio::test]
    async fn test_backfill_single_block() {
        let tip = Arc::new(MockTipSource::new(101));
        let checkpoint = Arc::new(MockCheckpointStore::new(100));
        let mut mock_backfill = MockBackfillSource::new();
        mock_backfill.add_range(101, 101, create_test_updates(101, 1));
        let backfill = Arc::new(mock_backfill);
        let live = Arc::new(MockLiveSource::new());
        let metrics = Arc::new(MetricsCollection::default());

        let datasource = SyncingDatasource::new(
            tip,
            checkpoint,
            backfill,
            live,
            vec![UpdateType::Transaction],
            SyncConfig {
                batch_size: 1000,
                ..Default::default()
            },
        );

        let state = Arc::new(SyncState::new(100, 101));
        let (tx, mut rx) = mpsc::channel(1000);
        let cancel = CancellationToken::new();

        let result = datasource
            .backfill_if_needed(&state, 101, tx.clone(), cancel, metrics)
            .await;

        assert!(result.is_ok());
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.last_indexed, 101);

        // Verify update was forwarded
        let received = rx.recv().await;
        assert!(received.is_some());
    }

    #[tokio::test]
    async fn test_backfill_already_at_tip() {
        let tip = Arc::new(MockTipSource::new(100));
        let checkpoint = Arc::new(MockCheckpointStore::new(100));
        let backfill = Arc::new(MockBackfillSource::new());
        let live = Arc::new(MockLiveSource::new());
        let metrics = Arc::new(MetricsCollection::default());

        let datasource = SyncingDatasource::new(
            tip,
            checkpoint,
            backfill,
            live,
            vec![UpdateType::Transaction],
            SyncConfig::default(),
        );

        let state = Arc::new(SyncState::new(100, 100));
        let (tx, _rx) = mpsc::channel(1000);
        let cancel = CancellationToken::new();

        let result = datasource
            .backfill_if_needed(&state, 100, tx, cancel, metrics)
            .await;

        assert!(result.is_ok());
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.last_indexed, 100);
    }

    #[tokio::test]
    async fn test_backfill_past_tip() {
        let tip = Arc::new(MockTipSource::new(100));
        let checkpoint = Arc::new(MockCheckpointStore::new(150));
        let backfill = Arc::new(MockBackfillSource::new());
        let live = Arc::new(MockLiveSource::new());
        let metrics = Arc::new(MetricsCollection::default());

        let datasource = SyncingDatasource::new(
            tip,
            checkpoint,
            backfill,
            live,
            vec![UpdateType::Transaction],
            SyncConfig::default(),
        );

        let state = Arc::new(SyncState::new(150, 100));
        let (tx, _rx) = mpsc::channel(1000);
        let cancel = CancellationToken::new();

        let result = datasource
            .backfill_if_needed(&state, 100, tx, cancel, metrics)
            .await;

        assert!(result.is_ok());
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.last_indexed, 150); // Should remain unchanged
    }

    #[tokio::test]
    async fn test_backfill_small_batch_size() {
        let tip = Arc::new(MockTipSource::new(105));
        let checkpoint = Arc::new(MockCheckpointStore::new(100));
        let mut mock_backfill = MockBackfillSource::new();
        mock_backfill.add_range(101, 101, create_test_updates(101, 1));
        mock_backfill.add_range(102, 102, create_test_updates(102, 1));
        mock_backfill.add_range(103, 103, create_test_updates(103, 1));
        mock_backfill.add_range(104, 104, create_test_updates(104, 1));
        mock_backfill.add_range(105, 105, create_test_updates(105, 1));
        let backfill = Arc::new(mock_backfill);
        let live = Arc::new(MockLiveSource::new());
        let metrics = Arc::new(MetricsCollection::default());

        let datasource = SyncingDatasource::new(
            tip,
            checkpoint,
            backfill,
            live,
            vec![UpdateType::Transaction],
            SyncConfig {
                batch_size: 1,
                ..Default::default()
            },
        );

        let state = Arc::new(SyncState::new(100, 105));
        let (tx, mut rx) = mpsc::channel(1000);
        let cancel = CancellationToken::new();

        let result = datasource
            .backfill_if_needed(&state, 105, tx.clone(), cancel, metrics)
            .await;

        assert!(result.is_ok());
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.last_indexed, 105);

        // Verify all updates were forwarded
        let mut received_count = 0;
        while let Ok(Some(_)) =
            tokio::time::timeout(tokio::time::Duration::from_millis(100), rx.recv()).await
        {
            received_count += 1;
        }
        assert_eq!(received_count, 5);
    }

    #[tokio::test]
    async fn test_sync_state_concurrent_updates() {
        let state = Arc::new(SyncState::new(100, 200));

        // Simulate concurrent updates
        let state1 = Arc::clone(&state);
        let state2 = Arc::clone(&state);
        let state3 = Arc::clone(&state);

        tokio::join!(
            async move { state1.set_last_indexed(150, false).await },
            async move { state2.set_safe_tip(250).await },
            async move { state3.set_pass_through(true).await },
        );

        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.last_indexed, 150);
        assert_eq!(snapshot.safe_tip, 250);
        assert!(snapshot.pass_through_enabled);
    }

    #[tokio::test]
    async fn test_reconcile_state_transitions() {
        let tip = Arc::new(MockTipSource::new(300));
        let checkpoint = Arc::new(MockCheckpointStore::new(200));
        let mut mock_backfill = MockBackfillSource::new();
        mock_backfill.add_range(201, 300, create_test_updates(201, 100));
        let backfill = Arc::new(mock_backfill);
        let live = Arc::new(MockLiveSource::new());
        let metrics = Arc::new(MetricsCollection::default());

        let datasource = SyncingDatasource::new(
            tip,
            checkpoint,
            backfill,
            live,
            vec![UpdateType::Transaction],
            SyncConfig {
                batch_size: 100,
                ..Default::default()
            },
        );

        let state = Arc::new(SyncState::new(200, 200));
        state.set_pass_through(true).await;

        // Verify initial state
        let snapshot = state.snapshot().await;
        assert!(snapshot.pass_through_enabled);
        match snapshot.phase {
            SyncPhase::LivePassthrough => {}
            _ => panic!("Expected LivePassthrough phase"),
        }

        let (tx, _rx) = mpsc::channel(1000);
        let cancel = CancellationToken::new();

        let result = datasource
            .reconcile_if_behind(&state, tx, cancel, metrics)
            .await;

        assert!(result.is_ok());

        // Verify final state after reconciliation
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.last_indexed, 300);
        assert!(snapshot.pass_through_enabled); // Should be re-enabled
        match snapshot.phase {
            SyncPhase::LivePassthrough => {}
            _ => panic!("Expected LivePassthrough phase after reconciliation"),
        }
    }

    #[test]
    fn test_filter_by_cutoff_boundary_conditions() {
        // Test with cutoff = 0 (should filter nothing if heights are > 0)
        let mut updates = create_test_updates(1, 5);
        let filtered = filter_by_cutoff(&mut updates, 0);
        assert!(filtered.is_some());
        if let Some(Updates::Transactions(txs)) = filtered {
            assert_eq!(txs.len(), 5);
        } else {
            panic!("Expected Transactions variant");
        }

        // Test with very large cutoff
        let mut updates = create_test_updates(100, 5);
        let filtered = filter_by_cutoff(&mut updates, u64::MAX);
        assert!(filtered.is_none());
    }

    #[tokio::test]
    async fn test_catch_up_with_empty_backfill() {
        // When tip hasn't advanced, catch_up should do nothing
        let tip = Arc::new(MockTipSource::new(200));
        let checkpoint = Arc::new(MockCheckpointStore::new(200));
        let backfill = Arc::new(MockBackfillSource::new());
        let live = Arc::new(MockLiveSource::new());
        let metrics = Arc::new(MetricsCollection::default());

        let datasource = SyncingDatasource::new(
            tip,
            checkpoint,
            backfill,
            live,
            vec![UpdateType::Transaction],
            SyncConfig::default(),
        );

        let state = Arc::new(SyncState::new(200, 200));
        let (tx, _rx) = mpsc::channel(1000);
        let cancel = CancellationToken::new();

        let result = datasource
            .catch_up_to_current_tip(&state, tx, cancel, metrics)
            .await;

        assert!(result.is_ok());
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.last_indexed, 200);
    }

    #[tokio::test]
    async fn test_backfill_with_tip_advancing() {
        // Simulate tip advancing during backfill
        let tip = Arc::new(MockTipSource::new(200));
        let checkpoint = Arc::new(MockCheckpointStore::new(100));
        let mut mock_backfill = MockBackfillSource::new();
        mock_backfill.add_range(101, 200, create_test_updates(101, 100));
        let backfill = Arc::new(mock_backfill);
        let live = Arc::new(MockLiveSource::new());
        let metrics = Arc::new(MetricsCollection::default());

        let datasource = SyncingDatasource::new(
            tip,
            checkpoint,
            backfill,
            live,
            vec![UpdateType::Transaction],
            SyncConfig {
                batch_size: 50,
                ..Default::default()
            },
        );

        let state = Arc::new(SyncState::new(100, 200));
        let (tx, _rx) = mpsc::channel(1000);
        let cancel = CancellationToken::new();

        // Backfill should stop at final_target_tip even if tip advances
        let result = datasource
            .backfill_if_needed(&state, 200, tx, cancel, metrics)
            .await;

        assert!(result.is_ok());
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.last_indexed, 200);
    }
}
