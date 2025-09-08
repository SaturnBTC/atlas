use std::collections::HashMap;

use async_trait::async_trait;
use atlas_arch::datasource::{
    Datasource, DatasourceId, ReappliedTransactionsEvent, RolledbackTransactionsEvent, UpdateType,
    Updates,
};
use atlas_arch::error::IndexerResult;
use atlas_arch::metrics::MetricsCollection;
use tokio_util::sync::CancellationToken;

use arch_sdk::RollbackStatus;

#[derive(Debug, thiserror::Error)]
pub enum RollbackInitializerDatasourceError {
    #[error("Arch error: {0}")]
    Arch(String),
}

#[async_trait]
pub trait ArchProvider: Send + Sync + Clone + 'static {
    async fn get_transactions_by_hashes(
        &self,
        ids: Vec<String>,
    ) -> std::result::Result<HashMap<String, ArchTransactionSummary>, String>;
    async fn get_current_arch_height(&self) -> std::result::Result<u64, String>;
}

#[derive(Debug, Clone)]
pub struct ArchTransactionSummary {
    pub id: String,
    pub rollback_status: RollbackStatus,
}

/// A lightweight datasource that checks a provided list of transaction ids against
/// Arch and emits rolledback/reapplied events based on differences with prior statuses.
///
/// - Agnostic of repositories. The caller provides prior statuses and heights via constructor.
pub struct RollbackInitializerDatasource<A>
where
    A: ArchProvider,
{
    arch: A,
    /// txid -> previous rollback status
    prior_status_map: HashMap<String, RollbackStatus>,
    /// All txids that should be checked
    txids_to_check: Vec<String>,
}

impl<A> RollbackInitializerDatasource<A>
where
    A: ArchProvider,
{
    pub fn new(
        arch: A,
        prior_status_map: HashMap<String, RollbackStatus>,
        txids_to_check: Vec<String>,
    ) -> Self {
        Self {
            arch,
            prior_status_map,
            txids_to_check,
        }
    }
}

#[async_trait]
impl<A> Datasource for RollbackInitializerDatasource<A>
where
    A: ArchProvider + Send + Sync + 'static,
{
    async fn consume(
        &self,
        id: DatasourceId,
        sender: tokio::sync::mpsc::Sender<(Updates, DatasourceId)>,
        _cancellation_token: CancellationToken,
        _metrics: std::sync::Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        if self.txids_to_check.is_empty() {
            return Ok(());
        }

        // Fetch current statuses from Arch
        let mut arch_map: HashMap<String, ArchTransactionSummary> = self
            .arch
            .get_transactions_by_hashes(self.txids_to_check.clone())
            .await
            .map_err(|e| atlas_arch::error::Error::Custom(e))?;

        // Fetch current arch height
        let arch_height = self
            .arch
            .get_current_arch_height()
            .await
            .map_err(|e| atlas_arch::error::Error::Custom(e))?;

        let mut replaced_ids: Vec<String> = Vec::new();
        let mut added_ids: Vec<String> = Vec::new();

        for txid in self.txids_to_check.iter() {
            if let Some(atx) = arch_map.remove(txid) {
                if let Some(prev) = self.prior_status_map.get(txid) {
                    if &atx.rollback_status != prev {
                        match atx.rollback_status {
                            RollbackStatus::Rolledback(_) => replaced_ids.push(txid.clone()),
                            RollbackStatus::NotRolledback => added_ids.push(txid.clone()),
                        }
                    }
                }
            }
        }

        if !replaced_ids.is_empty() {
            let event = RolledbackTransactionsEvent {
                height: arch_height,
                transaction_hashes: replaced_ids,
            };
            let _ = sender
                .send((Updates::RolledbackTransactions(vec![event]), id.clone()))
                .await;
        }
        if !added_ids.is_empty() {
            let event = ReappliedTransactionsEvent {
                height: arch_height,
                transaction_hashes: added_ids,
            };
            let _ = sender
                .send((Updates::ReappliedTransactions(vec![event]), id))
                .await;
        }

        Ok(())
    }

    fn update_types(&self) -> Vec<UpdateType> {
        vec![
            UpdateType::RolledbackTransactions,
            UpdateType::ReappliedTransactions,
        ]
    }
}
