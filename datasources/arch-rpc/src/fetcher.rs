use std::sync::Arc;
use std::time::Duration;

use arch_sdk::FullBlock;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use super::client::ArchRpcError;
use super::ArchRpc;

pub type BlockFetchResult =
    Result<(u64, arch_sdk::arch_program::hash::Hash, FullBlock), BlockFetchError>;

#[derive(Debug, thiserror::Error)]
pub enum BlockFetchError {
    #[error("Failed to fetch block at height {height} after {retries} retries: {source}")]
    FetchFailed {
        height: u64,
        retries: usize,
        source: ArchRpcError,
    },
    #[error("Fetch operation interrupted")]
    Interrupted,
}

#[derive(Debug, Clone)]
pub struct BlockFetcherConfig {
    pub block_fetcher_threads: usize,
    pub block_buffer_size: usize,
    pub fetch_window_size: usize,
    pub initial_backoff_ms: u64,
    pub max_retries: usize,
}

impl Default for BlockFetcherConfig {
    fn default() -> Self {
        Self {
            block_fetcher_threads: 16,
            block_buffer_size: 1024,
            fetch_window_size: 1024,
            initial_backoff_ms: 50,
            max_retries: 5,
        }
    }
}

pub struct BlockFetcher<'a, A: ArchRpc> {
    config: BlockFetcherConfig,
    running: Arc<std::sync::atomic::AtomicBool>,
    arch: &'a A,
}

impl<'a, A: ArchRpc> BlockFetcher<'a, A> {
    pub fn new(
        config: BlockFetcherConfig,
        arch: &'a A,
        running: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self {
            config,
            running,
            arch,
        }
    }

    fn is_shutdown_requested(&self) -> bool {
        !self.running.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn fetch_blocks(
        &self,
        start_height: u64,
        end_height: u64,
        cancellation_token: CancellationToken,
    ) -> mpsc::Receiver<BlockFetchResult> {
        info!(
            "Starting block fetcher with {} concurrent tasks for blocks {}-{}",
            self.config.block_fetcher_threads, start_height, end_height
        );

        let (tx_blocks, rx_blocks) = mpsc::channel(self.config.block_buffer_size);

        // Move required state into a background orchestrator so the receiver can be consumed immediately.
        let config = self.config.clone();
        let arch = self.arch.clone();
        let running = Arc::clone(&self.running);

        tokio::spawn(async move {
            let fetch_start_time = std::time::Instant::now();
            let mut last_log_time = fetch_start_time;
            let mut blocks_queued = 0usize;

            let mut current_start = start_height;
            while current_start <= end_height
                && running.load(std::sync::atomic::Ordering::SeqCst)
                && !cancellation_token.is_cancelled()
            {
                let current_end = std::cmp::min(
                    current_start + config.fetch_window_size as u64 - 1,
                    end_height,
                );
                let heights: Vec<u64> = (current_start..=current_end).collect();
                blocks_queued += heights.len();

                if last_log_time.elapsed() > Duration::from_secs(5) {
                    let elapsed = fetch_start_time.elapsed();
                    let total = end_height.saturating_sub(start_height).saturating_add(1);
                    let rate = if elapsed.as_secs() > 0 {
                        blocks_queued as f64 / elapsed.as_secs_f64()
                    } else {
                        0.0
                    };
                    info!(
                        "Queuing blocks {}-{} (progress: {}/{}, rate: {:.1} blocks/sec)",
                        current_start, current_end, blocks_queued, total, rate
                    );
                    last_log_time = std::time::Instant::now();
                }

                let semaphore = Arc::new(tokio::sync::Semaphore::new(config.block_fetcher_threads));
                let mut tasks = Vec::new();

                for height in heights {
                    let tx = tx_blocks.clone();
                    let arch_clone = arch.clone();
                    let cfg = config.clone();
                    let running_local = Arc::clone(&running);
                    let semaphore_clone = semaphore.clone();
                    let cancel = cancellation_token.clone();

                    let task = tokio::spawn(async move {
                        let _permit = semaphore_clone.acquire().await.unwrap();
                        if !running_local.load(std::sync::atomic::Ordering::SeqCst)
                            || cancel.is_cancelled()
                        {
                            let _ = tx.send(Err(BlockFetchError::Interrupted)).await;
                            return;
                        }

                        let mut retries = 0usize;
                        let mut _last_err: Option<ArchRpcError> = None;
                        loop {
                            if !running_local.load(std::sync::atomic::Ordering::SeqCst)
                                || cancel.is_cancelled()
                            {
                                let _ = tx.send(Err(BlockFetchError::Interrupted)).await;
                                return;
                            }

                            match arch_clone.get_block_by_height(height).await {
                                Ok(block) => {
                                    let block_hash = block.hash();
                                    let _ = tx.send(Ok((height, block_hash, block))).await;
                                    return;
                                }
                                Err(e) => {
                                    warn!(
                                        "Error fetching block {}: {} (retry {}/{})",
                                        height,
                                        e,
                                        retries + 1,
                                        cfg.max_retries
                                    );
                                    _last_err = Some(e);
                                }
                            }

                            retries += 1;
                            if retries >= cfg.max_retries {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(
                                cfg.initial_backoff_ms * (1 << retries),
                            ))
                            .await;
                        }

                        let source =
                            _last_err.unwrap_or(ArchRpcError::Unknown("unknown error".to_string()));
                        let _ = tx
                            .send(Err(BlockFetchError::FetchFailed {
                                height,
                                retries: cfg.max_retries,
                                source,
                            }))
                            .await;
                    });

                    tasks.push(task);
                }

                for t in tasks {
                    let _ = t.await;
                }
                current_start = current_end.saturating_add(1);
            }

            // All tasks/windows done; drop sender to close the channel
            drop(tx_blocks);
        });

        rx_blocks
    }
}
