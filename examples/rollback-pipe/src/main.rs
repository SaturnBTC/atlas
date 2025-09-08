use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arch_program::pubkey::Pubkey;
use async_trait::async_trait;
use atlas_arch as core;
use core::datasource::{
    Datasource, DatasourceId, ReappliedTransactionsEvent, RolledbackTransactionsEvent, UpdateType,
    Updates,
};
use core::metrics::{Metrics, MetricsCollection};
use core::pipeline::{Pipeline, ShutdownStrategy};
use core::processor::Processor;

// --- Minimal Metrics implementation for the example ---
struct LogMetrics;

#[async_trait]
impl Metrics for LogMetrics {
    async fn initialize(&self) -> core::error::IndexerResult<()> {
        Ok(())
    }
    async fn flush(&self) -> core::error::IndexerResult<()> {
        Ok(())
    }
    async fn shutdown(&self) -> core::error::IndexerResult<()> {
        Ok(())
    }
    async fn update_gauge(&self, _key: &str, _value: f64) -> core::error::IndexerResult<()> {
        Ok(())
    }
    async fn increment_counter(&self, _key: &str, _n: u64) -> core::error::IndexerResult<()> {
        Ok(())
    }
    async fn record_histogram(&self, _key: &str, _value: f64) -> core::error::IndexerResult<()> {
        Ok(())
    }
}

// --- Example TransactionStore-like trait and in-memory impl ---
#[async_trait]
trait TransactionStore: Send + Sync + Clone + 'static {
    async fn get_transactions_by_ids(&self, ids: &[String]) -> Result<Vec<Tx>, String>;
}

#[derive(Clone, Debug)]
struct InMemoryTxStore {
    // map id -> Tx
    txs: Arc<std::sync::Mutex<HashMap<String, Tx>>>,
}

#[async_trait]
impl TransactionStore for InMemoryTxStore {
    async fn get_transactions_by_ids(&self, ids: &[String]) -> Result<Vec<Tx>, String> {
        let guard = self.txs.lock().map_err(|_| "lock poisoned".to_string())?;
        Ok(ids.iter().filter_map(|id| guard.get(id).cloned()).collect())
    }
}

// --- Minimal Transaction model used by the processors ---
#[derive(Clone, Debug)]
struct Tx {
    id: String,
    pool: Pubkey,
    position: Option<Pubkey>,
    writable_accounts: Vec<Pubkey>,
}

impl Tx {
    fn id(&self) -> &str {
        &self.id
    }
    fn pool_pubkey(&self) -> Pubkey {
        self.pool
    }
    fn position_id(&self) -> Option<Pubkey> {
        self.position
    }
    fn writable_accounts_pubkeys(&self) -> impl Iterator<Item = Pubkey> + '_ {
        self.writable_accounts.iter().copied()
    }
}

// --- Rollback service adapted to the example types ---
#[derive(Clone)]
struct RollbackService<T>
where
    T: TransactionStore,
{
    transaction_store: T,
}

impl<T> RollbackService<T>
where
    T: TransactionStore,
{
    fn new(transaction_store: T) -> Self {
        Self { transaction_store }
    }

    async fn fetch_transactions_for_events(
        &self,
        rolled: &[RolledbackTransactionsEvent],
        reapplied: &[ReappliedTransactionsEvent],
    ) -> Result<(Vec<Tx>, Vec<Tx>), String> {
        let mut rolled_ids = vec![];
        for e in rolled {
            rolled_ids.extend(e.transaction_hashes.iter().cloned());
        }
        let mut reapplied_ids = vec![];
        for e in reapplied {
            reapplied_ids.extend(e.transaction_hashes.iter().cloned());
        }

        let rolled = if rolled_ids.is_empty() {
            vec![]
        } else {
            self.transaction_store
                .get_transactions_by_ids(&rolled_ids)
                .await?
        };
        let reapplied = if reapplied_ids.is_empty() {
            vec![]
        } else {
            self.transaction_store
                .get_transactions_by_ids(&reapplied_ids)
                .await?
        };
        Ok((rolled, reapplied))
    }
}

// --- Processors for rollback and reapply ---
struct RolledbackProcessor<T: TransactionStore> {
    service: RollbackService<T>,
}

#[async_trait]
impl<T: TransactionStore> Processor for RolledbackProcessor<T> {
    type InputType = RolledbackTransactionsEvent;
    type OutputType = HashSet<Pubkey>;

    async fn process(
        &mut self,
        data: Vec<Self::InputType>,
        _m: Arc<MetricsCollection>,
    ) -> core::error::IndexerResult<Self::OutputType> {
        let (rb, _) = self
            .service
            .fetch_transactions_for_events(&data, &[])
            .await
            .map_err(|e| core::error::Error::Custom(e))?;

        let mut pubkeys: HashSet<Pubkey> = HashSet::new();
        for tx in rb.iter() {
            let pool_pk = tx.pool_pubkey();
            pubkeys.insert(pool_pk);
            if let Some(pos) = tx.position_id() {
                pubkeys.insert(pos);
            }
        }
        for tx in rb.iter() {
            for pk in tx.writable_accounts_pubkeys() {
                pubkeys.insert(pk);
            }
        }
        Ok(pubkeys)
    }
}

struct ReappliedProcessor<T: TransactionStore> {
    service: RollbackService<T>,
}

#[async_trait]
impl<T: TransactionStore> Processor for ReappliedProcessor<T> {
    type InputType = ReappliedTransactionsEvent;
    type OutputType = HashSet<Pubkey>;

    async fn process(
        &mut self,
        data: Vec<Self::InputType>,
        _m: Arc<MetricsCollection>,
    ) -> core::error::IndexerResult<Self::OutputType> {
        let (_, rp) = self
            .service
            .fetch_transactions_for_events(&[], &data)
            .await
            .map_err(|e| core::error::Error::Custom(e))?;

        let mut pubkeys: HashSet<Pubkey> = HashSet::new();
        for tx in rp.iter() {
            let pool_pk = tx.pool_pubkey();
            pubkeys.insert(pool_pk);
            if let Some(pos) = tx.position_id() {
                pubkeys.insert(pos);
            }
        }
        for tx in rp.iter() {
            for pk in tx.writable_accounts_pubkeys() {
                pubkeys.insert(pk);
            }
        }
        Ok(pubkeys)
    }
}

// --- A tiny datasource that emits one rollback and one reapply event ---
struct DemoDatasource;

#[async_trait]
impl Datasource for DemoDatasource {
    async fn consume(
        &self,
        id: DatasourceId,
        sender: tokio::sync::mpsc::Sender<(Updates, DatasourceId)>,
        cancellation: tokio_util::sync::CancellationToken,
        _metrics: Arc<MetricsCollection>,
    ) -> core::error::IndexerResult<()> {
        // send a rollback event
        let rb = RolledbackTransactionsEvent {
            height: 100,
            transaction_hashes: vec!["tx-1".to_string()],
        };
        let _ = sender
            .send((Updates::RolledbackTransactions(vec![rb]), id.clone()))
            .await;
        // send a reapply event
        let rp = ReappliedTransactionsEvent {
            height: 101,
            transaction_hashes: vec!["tx-2".to_string()],
        };
        let _ = sender
            .send((Updates::ReappliedTransactions(vec![rp]), id))
            .await;

        // wait for shutdown
        let _ = cancellation.cancelled().await;
        Ok(())
    }

    fn update_types(&self) -> Vec<UpdateType> {
        vec![
            UpdateType::RolledbackTransactions,
            UpdateType::ReappliedTransactions,
        ]
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> core::error::IndexerResult<()> {
    // seed in-memory txs referenced by the datasource events
    let txs = vec![
        Tx {
            id: "tx-1".to_string(),
            pool: Pubkey::new_unique(),
            position: Some(Pubkey::new_unique()),
            writable_accounts: vec![Pubkey::new_unique(), Pubkey::new_unique()],
        },
        Tx {
            id: "tx-2".to_string(),
            pool: Pubkey::new_unique(),
            position: None,
            writable_accounts: vec![Pubkey::new_unique()],
        },
    ];
    let mut map = HashMap::new();
    for t in txs {
        map.insert(t.id.clone(), t);
    }
    let store = InMemoryTxStore {
        txs: Arc::new(std::sync::Mutex::new(map)),
    };

    let service = RollbackService::new(store.clone());
    let rolledback_proc = RolledbackProcessor {
        service: service.clone(),
    };
    let reapplied_proc = ReappliedProcessor { service };

    let mut pipeline: Pipeline = Pipeline::builder()
        .datasource(DemoDatasource)
        .metrics(Arc::new(LogMetrics))
        .rolledback_transactions(rolledback_proc)
        .reapplied_transactions(reapplied_proc)
        .shutdown_strategy(ShutdownStrategy::Immediate)
        .build()?;

    pipeline.run().await
}
