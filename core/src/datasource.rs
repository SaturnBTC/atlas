use std::collections::HashMap;
use std::sync::Arc;

use arch_program::{hash::Hash, pubkey::Pubkey};
use arch_sdk::{AccountInfo, ProcessedTransaction};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::{error::IndexerResult, metrics::MetricsCollection};

/// A source of update streams that the pipeline can consume from.
///
/// Implement this trait if you provide data into the indexing pipeline
/// (e.g., blocks, transactions, accounts, deletions, or Bitcoin blocks).
/// Implementations should:
/// - Be cancel-safe: return promptly when `cancellation_token` is cancelled
/// - Backpressure-aware: respect the `sender` capacity, awaiting sends
/// - Metrics-friendly: update gauges/counters to aid observability
#[async_trait]
pub trait Datasource: Send + Sync {
    /// Start producing updates and forward them to the provided `sender`.
    ///
    /// Implementations may batch updates and can send any of the `Updates`
    /// variants. The method should exit when `cancellation_token` is
    /// triggered or when a fatal error occurs.
    async fn consume(
        &self,
        id: DatasourceId,
        sender: tokio::sync::mpsc::Sender<(Updates, DatasourceId)>,
        cancellation_token: CancellationToken,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()>;

    /// Advertise which update kinds this datasource will emit.
    ///
    /// This is used by the pipeline to route updates efficiently and to
    /// activate only the relevant pipes/filters.
    fn update_types(&self) -> Vec<UpdateType>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatasourceId(String);

impl DatasourceId {
    pub fn new_unique() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    pub fn new_named(name: &str) -> Self {
        Self(name.to_string())
    }
}

#[derive(Debug, Clone)]
pub enum Update {
    Account(AccountUpdate),
    Transaction(Box<TransactionUpdate>),
    AccountDeletion(AccountDeletion),
    BlockDetails(BlockDetails),
    BitcoinBlock(BitcoinBlock),
    RolledbackTransactions(RolledbackTransactionsEvent),
    ReappliedTransactions(ReappliedTransactionsEvent),
}

#[derive(Debug)]
pub enum Updates {
    Accounts(Vec<AccountUpdate>),
    Transactions(Vec<TransactionUpdate>),
    AccountDeletions(Vec<AccountDeletion>),
    BlockDetails(Vec<BlockDetails>),
    BitcoinBlocks(Vec<BitcoinBlock>),
    RolledbackTransactions(Vec<RolledbackTransactionsEvent>),
    ReappliedTransactions(Vec<ReappliedTransactionsEvent>),
}

impl Updates {
    pub fn len(&self) -> usize {
        match self {
            Updates::Accounts(updates) => updates.len(),
            Updates::Transactions(updates) => updates.len(),
            Updates::AccountDeletions(updates) => updates.len(),
            Updates::BlockDetails(updates) => updates.len(),
            Updates::BitcoinBlocks(updates) => updates.len(),
            Updates::RolledbackTransactions(updates) => updates.len(),
            Updates::ReappliedTransactions(updates) => updates.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn push(&mut self, update: Update) {
        match (self, update) {
            (Updates::Accounts(updates), Update::Account(account_update)) => {
                updates.push(account_update)
            }
            (Updates::Transactions(updates), Update::Transaction(tx_update)) => {
                updates.push(*tx_update)
            }
            (Updates::AccountDeletions(updates), Update::AccountDeletion(account_deletion)) => {
                updates.push(account_deletion)
            }
            (Updates::BlockDetails(updates), Update::BlockDetails(block_details)) => {
                updates.push(block_details)
            }
            (Updates::BitcoinBlocks(updates), Update::BitcoinBlock(block)) => updates.push(block),
            (Updates::RolledbackTransactions(updates), Update::RolledbackTransactions(event)) => {
                updates.push(event)
            }
            (Updates::ReappliedTransactions(updates), Update::ReappliedTransactions(event)) => {
                updates.push(event)
            }
            // Mismatched variant: ignore
            _ => {}
        }
    }
}

impl Clone for Updates {
    fn clone(&self) -> Self {
        match self {
            Updates::Accounts(updates) => Updates::Accounts(updates.clone()),
            Updates::Transactions(updates) => Updates::Transactions(updates.clone()),
            Updates::AccountDeletions(updates) => Updates::AccountDeletions(updates.clone()),
            Updates::BlockDetails(updates) => Updates::BlockDetails(updates.clone()),
            Updates::BitcoinBlocks(updates) => Updates::BitcoinBlocks(updates.clone()),
            Updates::RolledbackTransactions(updates) => {
                Updates::RolledbackTransactions(updates.clone())
            }
            Updates::ReappliedTransactions(updates) => {
                Updates::ReappliedTransactions(updates.clone())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum UpdateType {
    AccountUpdate,
    Transaction,
    AccountDeletion,
    BlockDetails,
    BitcoinBlock,
    RolledbackTransactions,
    ReappliedTransactions,
}

#[derive(Debug, Clone)]
pub struct AccountUpdate {
    pub pubkey: Pubkey,
    pub account: AccountInfo,
    pub height: u64,
}

#[derive(Debug, Clone)]
pub struct BlockDetails {
    pub height: u64,
    pub block_hash: Option<Hash>,
    pub previous_block_hash: Option<Hash>,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct AccountDeletion {
    pub pubkey: Pubkey,
    pub height: u64,
}

#[derive(Debug, Clone)]
pub struct TransactionUpdate {
    pub transaction: ProcessedTransaction,
    pub height: u64,
}

#[derive(Debug, Clone)]
pub struct BitcoinBlock {
    pub block_height: u64,
    pub block_hash: String,
}

/// Transactions that were rolled back
#[derive(Debug, Clone)]
pub struct RolledbackTransactionsEvent {
    /// The height at which the transactions were rolled back
    pub height: u64,
    /// The transaction hashes that were rolled back
    pub transaction_hashes: Vec<String>,
}

/// Transactions that were reapplied
#[derive(Debug, Clone)]
pub struct ReappliedTransactionsEvent {
    /// The height at which the transactions were reapplied
    pub height: u64,
    /// The transaction hashes that were reapplied
    pub transaction_hashes: Vec<String>,
}

/// Optional provider interface for fetching Bitcoin transactions by txid.
///
/// If supplied to the pipeline, this will be used to enrich transaction updates
/// with the corresponding serialized Bitcoin transaction bytes when available.
#[async_trait]
pub trait BitcoinDatasource: Send + Sync {
    /// Batch fetch Bitcoin transactions by their txids (big-endian hash).
    /// Implementations should attempt to fetch all in one request if possible.
    async fn get_transactions(
        &self,
        txids: &[Hash],
    ) -> IndexerResult<HashMap<Hash, crate::bitcoin::Transaction>>;
}

/// Optional provider interface for fetching accounts on demand by pubkey.
#[async_trait]
pub trait AccountDatasource: Send + Sync {
    /// Batch fetch accounts by pubkeys at the latest canonical height.
    async fn get_multiple_accounts(
        &self,
        pubkeys: &[Pubkey],
    ) -> IndexerResult<HashMap<Pubkey, Option<AccountInfo>>>;
}
