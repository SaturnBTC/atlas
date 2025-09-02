use std::{collections::HashMap, str::FromStr as _, sync::Arc};

use arch_program::hash::Hash;
use arch_sdk::{ArchError as SdkArchError, ArchRpcClient as SdkArchRpcClient};
use async_trait::async_trait;
use futures::future::join_all;
use tokio::sync::Semaphore;

use super::initializer::{ArchProvider, ArchTransactionSummary};

// Maximum number of concurrent requests
const MAX_CONCURRENT_REQUESTS: usize = 50;

#[derive(Clone)]
pub struct ArchRollbackClient {
    client: SdkArchRpcClient,
    semaphore: Arc<Semaphore>,
}

impl ArchRollbackClient {
    pub fn new(url: &str) -> Self {
        Self {
            client: SdkArchRpcClient::new(url),
            semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)),
        }
    }
}

#[async_trait]
impl ArchProvider for ArchRollbackClient {
    async fn get_transactions_by_hashes(
        &self,
        ids: Vec<String>,
    ) -> Result<HashMap<String, ArchTransactionSummary>, String> {
        let semaphore = self.semaphore.clone();

        let futures = ids.into_iter().map(|hash| {
            let client = self.client.clone();
            let semaphore = semaphore.clone();
            async move {
                let _permit = semaphore.acquire().await.unwrap();

                let result = tokio::task::spawn_blocking(move || {
                    match client.get_processed_transaction(&Hash::from_str(&hash).unwrap()) {
                        Ok(Some(tx)) => Some((hash, tx)),
                        Ok(None) => None,
                        Err(e) => match e {
                            SdkArchError::NotFound(_) => None,
                            _ => None,
                        },
                    }
                })
                .await
                .unwrap();

                result
            }
        });

        let results = join_all(futures).await;

        let mut txs_map: HashMap<String, ArchTransactionSummary> = HashMap::new();
        for result in results.into_iter().flatten() {
            txs_map.insert(
                result.0.clone(),
                ArchTransactionSummary {
                    id: result.0,
                    rollback_status: result.1.rollback_status,
                },
            );
        }

        Ok(txs_map)
    }

    async fn get_current_arch_height(&self) -> Result<u64, String> {
        let client = self.client.clone();
        tokio::task::spawn_blocking(move || client.get_block_count())
            .await
            .map_err(|e| e.to_string())
            .and_then(|r| r.map_err(|e| e.to_string()))
    }
}
