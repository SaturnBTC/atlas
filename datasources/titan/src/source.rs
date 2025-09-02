use std::str::FromStr;
use std::sync::Arc;
use std::thread;

use arch_program::hash::Hash;
use async_trait::async_trait;
use atlas_core::datasource::{
    BitcoinBlock, BitcoinDatasource, Datasource, DatasourceId, UpdateType, Updates,
};
use atlas_core::error::IndexerResult;
use atlas_core::metrics::MetricsCollection;
use futures::future::join_all;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Datasource that streams Bitcoin block headers/heights from a Titan TCP subscription
/// and forwards them into the pipeline as `Updates::BitcoinBlocks`.
pub struct TitanBitcoinBlocksDatasource {
    address: String,
}

impl TitanBitcoinBlocksDatasource {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
        }
    }
}

#[async_trait]
impl Datasource for TitanBitcoinBlocksDatasource {
    async fn consume(
        &self,
        id: DatasourceId,
        sender: Sender<(Updates, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        let (event_tx, mut event_rx) = mpsc::channel::<(u64, String)>(1024);
        let address = self.address.clone();
        let ct = cancellation_token.clone();
        let metrics_for_thread = metrics.clone();

        let _handle = thread::Builder::new()
            .name("titan-blocks-sub".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to build Tokio runtime");

                runtime.block_on(async move {
                    metrics_for_thread
                        .increment_counter("titan_bitcoin_blocks_thread_start", 1)
                        .await
                        .ok();

                    if ct.is_cancelled() {
                        info!("TitanBitcoinBlocksDatasource thread observed cancellation before subscribe");
                        metrics_for_thread
                            .increment_counter("titan_bitcoin_blocks_thread_exit", 1)
                            .await
                            .ok();
                        return;
                    }

                    let titan = titan_client::TitanTcpClient::new();
                    let subscription = titan_client::TcpSubscriptionRequest {
                        subscribe: vec![titan_client::EventType::NewBlock],
                    };

                    match titan.subscribe(&address, subscription).await {
                        Ok(mut events_rx) => {
                            info!(%address, "Titan TCP subscribe success (NewBlock)");
                            metrics_for_thread
                                .increment_counter("titan_bitcoin_blocks_subscribe_success", 1)
                                .await
                                .ok();

                            loop {
                                tokio::select! {
                                    _ = ct.cancelled() => {
                                        info!("TitanBitcoinBlocksDatasource thread cancelled");
                                        break;
                                    }
                                    maybe_event = events_rx.recv() => {
                                        match maybe_event {
                                            Some(titan_client::Event::NewBlock { block_height, block_hash }) => {
                                                if event_tx.send((block_height, block_hash.to_string())).await.is_err() {
                                                    debug!("Event channel receiver dropped; shutting down Titan thread");
                                                    break;
                                                }
                                            }
                                            Some(other) => {
                                                debug!(?other, "Ignoring non-NewBlock Titan event");
                                                metrics_for_thread
                                                    .increment_counter("titan_bitcoin_block_events_ignored", 1)
                                                    .await
                                                    .ok();
                                            }
                                            None => {
                                                // If the client implements internal reconnection, this should not occur
                                                // unless the stream is permanently closed.
                                                warn!("Titan TCP event stream ended");
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, %address, "Titan TCP subscribe error");
                            metrics_for_thread
                                .increment_counter("titan_bitcoin_blocks_subscribe_error", 1)
                                .await
                                .ok();
                        }
                    }

                    metrics_for_thread
                        .increment_counter("titan_bitcoin_blocks_thread_exit", 1)
                        .await
                        .ok();
                });
            })
            .expect("Failed to spawn titan-blocks-sub thread");

        metrics
            .increment_counter("titan_bitcoin_blocks_consumer_start", 1)
            .await
            .ok();

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("TitanBitcoinBlocksDatasource cancelled");
                    break;
                }
                next = event_rx.recv() => {
                    match next {
                        Some((block_height, block_hash)) => {
                            let update = Updates::BitcoinBlocks(vec![BitcoinBlock{ block_height, block_hash }]);
                            let _ = sender.send((update, id.clone())).await;
                            metrics.increment_counter("titan_bitcoin_block_events", 1).await.ok();
                        }
                        None => {
                            warn!("TitanBitcoinBlocksDatasource event stream closed");
                            break;
                        }
                    }
                }
            }
        }

        // Let the TCP client be dropped by the owner; returning stops the stream consumer.
        Ok(())
    }

    fn update_types(&self) -> Vec<UpdateType> {
        // This datasource only emits Bitcoin blocks
        vec![UpdateType::BitcoinBlock]
    }
}

/// Provider that fetches raw Bitcoin transactions by txid using a Titan HTTP client.
pub struct TitanBitcoinTxProvider {
    client: titan_client::TitanClient,
}

impl TitanBitcoinTxProvider {
    pub fn new(http_url: impl AsRef<str>) -> Self {
        Self {
            client: titan_client::TitanClient::new(http_url.as_ref()),
        }
    }

    pub fn from_client(client: titan_client::TitanClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl BitcoinDatasource for TitanBitcoinTxProvider {
    async fn get_transactions(
        &self,
        txids: &[Hash],
    ) -> IndexerResult<std::collections::HashMap<Hash, atlas_core::bitcoin::Transaction>> {
        // Fetch in parallel
        let futs = txids.iter().map(|h| {
            let txid_str = h.to_string();
            async move {
                let parsed = bitcoin::Txid::from_str(&txid_str)
                    .map_err(|e| atlas_core::error::Error::Custom(e.to_string()))?;
                use titan_client::TitanApi;
                let tx = self
                    .client
                    .get_transaction(&parsed)
                    .await
                    .map_err(|e| atlas_core::error::Error::Custom(e.to_string()))?;
                Ok::<(Hash, atlas_core::bitcoin::Transaction), atlas_core::error::Error>((*h, tx))
            }
        });

        let mut map = std::collections::HashMap::new();
        for result in join_all(futs).await {
            match result {
                Ok((hash, tx)) => {
                    map.insert(hash, tx);
                }
                Err(e) => {
                    // Ignore individual fetch errors; caller can proceed without enrichment
                    warn!(error = %e, "titan get_transaction failed for txid");
                }
            }
        }

        Ok(map)
    }
}
