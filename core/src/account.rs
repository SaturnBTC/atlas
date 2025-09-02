use std::sync::Arc;

use arch_program::pubkey::Pubkey;
use arch_sdk::AccountInfo;
use async_trait::async_trait;

use crate::{
    error::IndexerResult, filter::Filter, metrics::MetricsCollection, processor::Processor,
};

#[derive(Debug, Clone)]
pub struct AccountMetadata {
    pub height: u64,
    pub pubkey: Pubkey,
}

#[derive(Debug, Clone)]
pub struct DecodedAccount<T> {
    pub lamports: u64,
    pub owner: Pubkey,
    pub data: T,
    pub utxo: String,
    pub executable: bool,
}

/// Decodes raw `AccountInfo` into a typed account model.
///
/// Implementors should return `Some(DecodedAccount<..>)` only when the data
/// layout matches the target type; otherwise return `None`.
pub trait AccountDecoder<'a> {
    type AccountType;

    fn decode_account(&self, account: &'a AccountInfo)
        -> Option<DecodedAccount<Self::AccountType>>;
}

pub type AccountProcessorInputType<T> = (AccountMetadata, DecodedAccount<T>, AccountInfo);

pub struct AccountPipe<T: Send> {
    pub decoder: Box<dyn for<'a> AccountDecoder<'a, AccountType = T> + Send + Sync + 'static>,
    pub processor:
        Box<dyn Processor<InputType = AccountProcessorInputType<T>, OutputType = ()> + Send + Sync>,
    pub filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
}

#[async_trait]
/// Runs account decoders and forwards decoded accounts to a processor.
///
/// Implementations should respect filters and be prepared to process large
/// batches efficiently.
pub trait AccountPipes: Send + Sync {
    async fn run(
        &mut self,
        account_with_metadata: Vec<(AccountMetadata, AccountInfo)>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()>;

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>>;
}

#[async_trait]
impl<T: Send> AccountPipes for AccountPipe<T> {
    async fn run(
        &mut self,
        account_with_metadata: Vec<(AccountMetadata, AccountInfo)>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        log::trace!(
            "AccountPipe::run(account_with_metadata: {:?}, metrics)",
            account_with_metadata
        );

        let mut decoded_accounts = Vec::new();
        for (account_metadata, account_info) in account_with_metadata.iter() {
            if let Some(decoded_account) = self.decoder.decode_account(account_info) {
                metrics.increment_counter("decoded_accounts", 1).await?;

                decoded_accounts.push((
                    account_metadata.clone(),
                    decoded_account,
                    account_info.clone(),
                ));
            }
        }

        // Account processors do not emit any outcome
        let _ = self
            .processor
            .process(decoded_accounts, metrics.clone())
            .await?;

        Ok(())
    }

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>> {
        &self.filters
    }
}
