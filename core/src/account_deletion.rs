use {
    crate::{
        datasource::AccountDeletion, error::IndexerResult, filter::Filter,
        metrics::MetricsCollection, processor::Processor,
    },
    async_trait::async_trait,
    std::sync::Arc,
};

pub struct AccountDeletionPipe {
    pub processor: Box<dyn Processor<InputType = AccountDeletion, OutputType = ()> + Send + Sync>,
    pub filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
}

#[async_trait]
/// Forwards account deletion updates to a processor.
pub trait AccountDeletionPipes: Send + Sync {
    async fn run(
        &mut self,
        account_deletion: Vec<AccountDeletion>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()>;

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>>;
}

#[async_trait]
impl AccountDeletionPipes for AccountDeletionPipe {
    async fn run(
        &mut self,
        account_deletion: Vec<AccountDeletion>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        log::trace!(
            "AccountDeletionPipe::run(account_deletion: {:?}, metrics)",
            account_deletion,
        );

        self.processor.process(account_deletion, metrics).await?;

        Ok(())
    }

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>> {
        &self.filters
    }
}
