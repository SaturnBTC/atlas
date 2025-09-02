use crate::datasource::{ReappliedTransactionsEvent, RolledbackTransactionsEvent};
use crate::error::IndexerResult;
use crate::filter::Filter;
use crate::metrics::MetricsCollection;
use crate::processor::Processor;
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;

pub struct RolledbackTransactionsPipe {
    pub processor: Box<
        dyn Processor<
                InputType = RolledbackTransactionsEvent,
                OutputType = HashSet<arch_program::pubkey::Pubkey>,
            > + Send
            + Sync,
    >,
    pub filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
}

#[async_trait]
/// Handles rollback events (reorged-away transactions) and produces any
/// side-effects, such as reindexing affected accounts.
pub trait RolledbackTransactionsPipes: Send + Sync {
    async fn run(
        &mut self,
        events: Vec<RolledbackTransactionsEvent>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<HashSet<arch_program::pubkey::Pubkey>>;

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>>;
}

#[async_trait]
impl RolledbackTransactionsPipes for RolledbackTransactionsPipe {
    async fn run(
        &mut self,
        events: Vec<RolledbackTransactionsEvent>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<HashSet<arch_program::pubkey::Pubkey>> {
        log::trace!("RolledbackTransactions::run(events: {:?}, metrics)", events);
        let out = self.processor.process(events, metrics).await?;
        Ok(out)
    }

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>> {
        &self.filters
    }
}

pub struct ReappliedTransactionsPipe {
    pub processor: Box<
        dyn Processor<
                InputType = ReappliedTransactionsEvent,
                OutputType = HashSet<arch_program::pubkey::Pubkey>,
            > + Send
            + Sync,
    >,
    pub filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
}

#[async_trait]
/// Handles reapplication events (transactions restored after a reorg) and
/// returns any affected accounts to update downstream.
pub trait ReappliedTransactionsPipes: Send + Sync {
    async fn run(
        &mut self,
        events: Vec<ReappliedTransactionsEvent>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<HashSet<arch_program::pubkey::Pubkey>>;

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>>;
}

#[async_trait]
impl ReappliedTransactionsPipes for ReappliedTransactionsPipe {
    async fn run(
        &mut self,
        events: Vec<ReappliedTransactionsEvent>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<HashSet<arch_program::pubkey::Pubkey>> {
        log::trace!("ReappliedTransactions::run(events: {:?}, metrics)", events);
        let out = self.processor.process(events, metrics).await?;
        Ok(out)
    }

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>> {
        &self.filters
    }
}
