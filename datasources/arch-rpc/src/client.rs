use std::collections::HashMap;

use arch_program::pubkey::Pubkey;
use arch_sdk::{
    AccountInfo, ArchError as SdkArchError, ArchRpcClient as SdkArchRpcClient, Config, FullBlock,
};
use async_trait::async_trait;
use atlas_arch::datasource::AccountDatasource;
use atlas_arch::error::IndexerResult;
use tokio::task;
use tracing::error;

#[derive(Debug, thiserror::Error)]
pub enum ArchRpcError {
    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Failed to perform RPC request: {0}")]
    RpcError(String),

    #[error("Task join error: {0}")]
    TaskJoinError(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

pub type Result<T> = std::result::Result<T, ArchRpcError>;

#[async_trait]
pub trait ArchRpc: Clone + Send + Sync + 'static {
    async fn get_account_address(&self, account_pubkey: Pubkey) -> Result<String>;

    async fn get_block_count(&self) -> Result<u64>;

    async fn get_multiple_accounts(
        &self,
        pubkeys: &Vec<Pubkey>,
    ) -> Result<HashMap<Pubkey, Option<AccountInfo>>>;

    async fn get_block_by_height(&self, height: u64) -> Result<FullBlock>;

    async fn get_block_hash_by_height(&self, height: u64) -> Result<String>;
}

#[derive(Clone)]
pub struct ArchRpcClient {
    client: SdkArchRpcClient,
}

impl ArchRpcClient {
    pub fn new(url: &str) -> Self {
        let mut config = Config::localnet();
        config.arch_node_url = url.to_string();
        Self {
            client: SdkArchRpcClient::new(&config),
        }
    }
}

#[async_trait]
impl ArchRpc for ArchRpcClient {
    async fn get_account_address(&self, account_pubkey: Pubkey) -> Result<String> {
        let client = self.client.clone();
        task::spawn_blocking(move || client.get_account_address(&account_pubkey))
            .await
            .map_err(|e| ArchRpcError::TaskJoinError(e.to_string()))?
            .map_err(|e| match e {
                SdkArchError::NotFound(msg) => {
                    ArchRpcError::NotFound(format!("Account address not available: {}", msg))
                }
                _ => ArchRpcError::RpcError(format!(
                    "Error getting account address for pubkey: {}",
                    hex::encode(account_pubkey)
                )),
            })
    }

    async fn get_block_count(&self) -> Result<u64> {
        let client = self.client.clone();
        task::spawn_blocking(move || client.get_block_count())
            .await
            .map_err(|e| ArchRpcError::TaskJoinError(e.to_string()))?
            .map_err(|e| match e {
                SdkArchError::NotFound(msg) => {
                    ArchRpcError::NotFound(format!("Block count not available: {}", msg))
                }
                _ => ArchRpcError::RpcError(format!("Error getting block count: {}", e)),
            })
    }

    async fn get_multiple_accounts(
        &self,
        pubkeys: &Vec<Pubkey>,
    ) -> Result<HashMap<Pubkey, Option<AccountInfo>>> {
        let client = self.client.clone();
        let pubkeys_clone = pubkeys.clone();
        let accounts = task::spawn_blocking(move || client.get_multiple_accounts(pubkeys_clone))
            .await
            .map_err(|e| ArchRpcError::TaskJoinError(e.to_string()))?
            .map_err(|e| match e {
                SdkArchError::NotFound(msg) => {
                    ArchRpcError::NotFound(format!("Accounts not available: {}", msg))
                }
                _ => ArchRpcError::RpcError(format!("Error getting accounts: {}", e)),
            })?;

        let accounts_map = accounts
            .into_iter()
            .zip(pubkeys.iter())
            .map(|(account, pubkey)| {
                if let Some(account) = account {
                    (*pubkey, Some(account.into()))
                } else {
                    (*pubkey, None)
                }
            })
            .collect::<HashMap<Pubkey, Option<AccountInfo>>>();

        Ok(accounts_map)
    }

    async fn get_block_by_height(&self, height: u64) -> Result<FullBlock> {
        let client = self.client.clone();
        task::spawn_blocking(move || client.get_full_block_by_height(height))
            .await
            .map_err(|e| ArchRpcError::TaskJoinError(e.to_string()))?
            .map_err(|e| match e {
                SdkArchError::NotFound(msg) => {
                    ArchRpcError::NotFound(format!("Block not found: {}", msg))
                }
                _ => ArchRpcError::RpcError(format!("Error getting block by height: {}", e)),
            })?
            .ok_or(ArchRpcError::NotFound("Block not found".to_string()))
    }

    async fn get_block_hash_by_height(&self, height: u64) -> Result<String> {
        let client = self.client.clone();
        task::spawn_blocking(move || client.get_block_hash(height))
            .await
            .map_err(|e| ArchRpcError::TaskJoinError(e.to_string()))?
            .map_err(|e| match e {
                SdkArchError::NotFound(msg) => {
                    ArchRpcError::NotFound(format!("Block hash not available: {}", msg))
                }
                _ => ArchRpcError::RpcError(format!("Error getting block hash by height: {}", e)),
            })
    }
}

#[async_trait]
impl AccountDatasource for ArchRpcClient {
    async fn get_multiple_accounts(
        &self,
        pubkeys: &[Pubkey],
    ) -> IndexerResult<HashMap<Pubkey, Option<AccountInfo>>> {
        let keys = pubkeys.to_vec();
        ArchRpc::get_multiple_accounts(self, &keys)
            .await
            .map_err(|e| {
                atlas_arch::error::Error::Custom(format!("arch_rpc get_multiple_accounts: {}", e))
            })
    }
}
