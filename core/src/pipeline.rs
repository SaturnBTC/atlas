use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;

use crate::{
    account::{
        AccountDecoder, AccountMetadata, AccountPipe, AccountPipes, AccountProcessorInputType,
    },
    account_deletion::{AccountDeletionPipe, AccountDeletionPipes},
    block_details::{BitcoinBlockPipe, BitcoinBlockPipes, BlockDetailsPipe, BlockDetailsPipes},
    collection::InstructionDecoderCollection,
    datasource::{
        AccountDatasource, AccountDeletion, BitcoinBlock, BitcoinDatasource, BlockDetails,
        Datasource, DatasourceId, ReappliedTransactionsEvent, RolledbackTransactionsEvent, Updates,
    },
    error::IndexerResult,
    filter::Filter,
    instruction::{
        InstructionDecoder, InstructionPipe, InstructionPipes, InstructionProcessorInputType,
        Instructions,
    },
    metrics::{Metrics, MetricsCollection},
    processor::Processor,
    rollback::{
        ReappliedTransactionsPipe, ReappliedTransactionsPipes, RolledbackTransactionsPipe,
        RolledbackTransactionsPipes,
    },
    schema::TransactionSchema,
    transaction::{TransactionPipe, TransactionPipes, TransactionProcessorInputType},
    transformers,
};
use core::time;
use std::{collections::HashSet, sync::Arc, time::Instant};

#[derive(Default, PartialEq, Debug)]
pub enum ShutdownStrategy {
    Immediate,
    #[default]
    ProcessPending,
}

pub const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 1_000;

pub struct Pipeline {
    pub datasources: Vec<(DatasourceId, Arc<dyn Datasource + Send + Sync>)>,
    pub account_pipes: Vec<Box<dyn AccountPipes>>,
    pub account_deletion_pipes: Vec<Box<dyn AccountDeletionPipes>>,
    pub block_details_pipes: Vec<Box<dyn BlockDetailsPipes>>,
    pub bitcoin_block_pipes: Vec<Box<dyn BitcoinBlockPipes>>,
    pub rolledback_transactions_pipes: Vec<Box<dyn RolledbackTransactionsPipes>>,
    pub reapplied_transactions_pipes: Vec<Box<dyn ReappliedTransactionsPipes>>,
    pub instruction_pipes: Vec<Box<dyn for<'a> InstructionPipes<'a>>>,
    pub transaction_pipes: Vec<Box<dyn for<'a> TransactionPipes<'a>>>,
    pub metrics: Arc<MetricsCollection>,
    pub metrics_flush_interval: Option<u64>,
    pub datasource_cancellation_token: Option<CancellationToken>,
    pub shutdown_strategy: ShutdownStrategy,
    pub channel_buffer_size: usize,
    pub bitcoin_datasource: Option<Arc<dyn BitcoinDatasource + Send + Sync>>, // optional bitcoin provider
    pub account_datasource: Option<Arc<dyn AccountDatasource + Send + Sync>>, // optional account fetcher
}

impl Pipeline {
    pub fn builder() -> PipelineBuilder {
        log::trace!("Pipeline::builder()");
        PipelineBuilder {
            datasources: Vec::new(),
            account_pipes: Vec::new(),
            account_deletion_pipes: Vec::new(),
            block_details_pipes: Vec::new(),
            bitcoin_block_pipes: Vec::new(),
            rolledback_transactions_pipes: Vec::new(),
            reapplied_transactions_pipes: Vec::new(),
            instruction_pipes: Vec::new(),
            transaction_pipes: Vec::new(),
            metrics: MetricsCollection::default(),
            metrics_flush_interval: None,
            datasource_cancellation_token: None,
            shutdown_strategy: ShutdownStrategy::default(),
            channel_buffer_size: DEFAULT_CHANNEL_BUFFER_SIZE,
            bitcoin_datasource: None,
            account_datasource: None,
        }
    }

    pub async fn run(&mut self) -> IndexerResult<()> {
        log::info!("starting pipeline. num_datasources: {}, num_metrics: {}, num_account_pipes: {}, num_account_deletion_pipes: {}, num_instruction_pipes: {}, num_transaction_pipes: {}",
            self.datasources.len(),
            self.metrics.metrics.len(),
            self.account_pipes.len(),
            self.account_deletion_pipes.len(),
            self.instruction_pipes.len(),
            self.transaction_pipes.len(),
        );

        log::trace!("run(self)");

        self.metrics.initialize_metrics().await?;
        let (update_sender, mut update_receiver) =
            tokio::sync::mpsc::channel::<(Updates, DatasourceId)>(self.channel_buffer_size);

        let datasource_cancellation_token = self
            .datasource_cancellation_token
            .clone()
            .unwrap_or_default();

        for datasource in &self.datasources {
            let datasource_cancellation_token_clone = datasource_cancellation_token.clone();
            let sender_clone = update_sender.clone();
            let datasource_clone = Arc::clone(&datasource.1);
            let datasource_id = datasource.0.clone();
            let metrics_collection = self.metrics.clone();

            tokio::spawn(async move {
                if let Err(e) = datasource_clone
                    .consume(
                        datasource_id,
                        sender_clone,
                        datasource_cancellation_token_clone,
                        metrics_collection,
                    )
                    .await
                {
                    log::error!("error consuming datasource: {:?}", e);
                }
            });
        }

        drop(update_sender);

        let mut interval = tokio::time::interval(time::Duration::from_secs(
            self.metrics_flush_interval.unwrap_or(5),
        ));

        loop {
            tokio::select! {
                _ = datasource_cancellation_token.cancelled() => {
                    log::trace!("datasource cancellation token cancelled, shutting down.");
                    self.metrics.flush_metrics().await?;
                    self.metrics.shutdown_metrics().await?;
                    break;
                }
                _ = tokio::signal::ctrl_c() => {
                    log::trace!("received SIGINT, shutting down.");
                    datasource_cancellation_token.cancel();

                    if self.shutdown_strategy == ShutdownStrategy::Immediate {
                        log::info!("shutting down the pipeline immediately.");
                        self.metrics.flush_metrics().await?;
                        self.metrics.shutdown_metrics().await?;
                        break;
                    } else {
                        log::info!("shutting down the pipeline after processing pending updates.");
                    }
                }
                _ = interval.tick() => {
                    self.metrics.flush_metrics().await?;
                }
                update = update_receiver.recv() => {
                    match update {
                        Some((update, datasource_id)) => {
                            self
                                .metrics.increment_counter("updates_received", 1)
                                .await?;

                            let start = Instant::now();
                            let process_result = self.process(update.clone(), datasource_id.clone()).await;
                            let time_taken_nanoseconds = start.elapsed().as_nanos();
                            let time_taken_milliseconds = time_taken_nanoseconds / 1_000_000;

                            self
                                .metrics
                                .record_histogram("updates_process_time_nanoseconds", time_taken_nanoseconds as f64)
                                .await?;

                            self
                                .metrics
                                .record_histogram("updates_process_time_milliseconds", time_taken_milliseconds as f64)
                                .await?;

                            match process_result {
                                Ok(_) => {
                                    self
                                        .metrics.increment_counter("updates_successful", 1)
                                        .await?;

                                    log::trace!("processed update")
                                }
                                Err(error) => {
                                    log::error!("error processing update ({:?}): {:?}", update, error);
                                    self.metrics.increment_counter("updates_failed", 1).await?;
                                }
                            };

                            self
                                .metrics.increment_counter("updates_processed", 1)
                                .await?;

                            self
                                .metrics.update_gauge("updates_queued", update_receiver.len() as f64)
                                .await?;
                        }
                        None => {
                            log::info!("update_receiver closed, shutting down.");
                            self.metrics.flush_metrics().await?;
                            self.metrics.shutdown_metrics().await?;
                            break;
                        }
                    }
                }
            }
        }

        log::info!("pipeline shutdown complete.");

        Ok(())
    }

    async fn process(
        &mut self,
        updates: Updates,
        datasource_id: DatasourceId,
    ) -> IndexerResult<()> {
        log::trace!(
            "process(self, updates: {:?}, datasource_id: {:?})",
            updates,
            datasource_id
        );

        match updates {
            Updates::Accounts(account_updates) => {
                // Batch by pipe: filter first, then run once per pipe
                for pipe in self.account_pipes.iter_mut() {
                    let mut filtered: Vec<(AccountMetadata, arch_sdk::AccountInfo)> = Vec::new();
                    for account_update in account_updates.iter() {
                        let account_metadata = AccountMetadata {
                            height: account_update.height,
                            pubkey: account_update.pubkey,
                        };
                        if pipe.filters().iter().all(|filter| {
                            filter.filter_account(
                                &datasource_id,
                                &account_metadata,
                                &account_update.account,
                            )
                        }) {
                            filtered.push((account_metadata, account_update.account.clone()));
                        }
                    }
                    if !filtered.is_empty() {
                        pipe.run(filtered, self.metrics.clone()).await?;
                    }
                }

                self.metrics
                    .increment_counter("account_updates_processed", account_updates.len() as u64)
                    .await?;
            }
            Updates::Transactions(transaction_updates) => {
                // Prepare all instructions for instruction pipes
                let mut all_instructions: Vec<arch_program::instruction::Instruction> = Vec::new();
                // For transaction pipes we need to store instruction vectors to keep slices alive
                let mut instructions_storage: Vec<crate::instruction::Instructions> = Vec::new();

                // Also collect corresponding metadatas in parallel for convenience
                let mut metadatas: Vec<Arc<crate::transaction::TransactionMetadata>> = Vec::new();

                // Collect all needed bitcoin txids to batch fetch
                let mut needed_txids: HashSet<arch_program::hash::Hash> = HashSet::new();
                if let Some(_) = &self.bitcoin_datasource {
                    for transaction_update in transaction_updates.iter() {
                        if matches!(
                            transaction_update.transaction.rollback_status,
                            arch_sdk::RollbackStatus::NotRolledback
                        ) {
                            if let Some(txid) = &transaction_update.transaction.bitcoin_txid {
                                needed_txids.insert(*txid);
                            }
                        }
                    }
                }

                // Batch request
                let mut fetched: std::collections::HashMap<
                    arch_program::hash::Hash,
                    crate::bitcoin::Transaction,
                > = std::collections::HashMap::new();
                if let Some(provider) = &self.bitcoin_datasource {
                    if !needed_txids.is_empty() {
                        fetched = provider
                            .get_transactions(&needed_txids.into_iter().collect::<Vec<_>>())
                            .await?;
                    }
                }

                for transaction_update in transaction_updates.iter() {
                    let mut metadata: crate::transaction::TransactionMetadata =
                        transaction_update.clone().try_into()?;

                    if let Some(txid) = &transaction_update.transaction.bitcoin_txid {
                        if let Some(tx) = fetched.get(txid) {
                            metadata.bitcoin_tx = Some(tx.clone());
                        }
                    }

                    let transaction_metadata = Arc::new(metadata);
                    let instructions: Instructions =
                        transformers::extract_instructions_with_metadata(
                            &transaction_metadata,
                            transaction_update,
                        )?;

                    // Accumulate for instruction pipes
                    all_instructions.extend(instructions.clone().into_iter());

                    // Store for transaction pipes; keep backing storage around
                    metadatas.push(transaction_metadata);
                    instructions_storage.push(instructions);
                }

                // Run instruction pipes with filtered batches
                for pipe in self.instruction_pipes.iter_mut() {
                    let filtered_instructions: Vec<arch_program::instruction::Instruction> =
                        all_instructions
                            .iter()
                            .cloned()
                            .filter(|ix| {
                                pipe.filters()
                                    .iter()
                                    .all(|filter| filter.filter_instruction(&datasource_id, ix))
                            })
                            .collect();
                    if !filtered_instructions.is_empty() {
                        pipe.run(&filtered_instructions, self.metrics.clone())
                            .await?;
                    }
                }

                // Run transaction pipes with filtered transaction batches
                for pipe in self.transaction_pipes.iter_mut() {
                    let mut batch: Vec<(
                        Arc<crate::transaction::TransactionMetadata>,
                        &[arch_program::instruction::Instruction],
                    )> = Vec::new();

                    for (idx, transaction_metadata) in metadatas.iter().enumerate() {
                        let instructions = &instructions_storage[idx];
                        if pipe.filters().iter().all(|filter| {
                            filter.filter_transaction(
                                &datasource_id,
                                transaction_metadata,
                                instructions,
                            )
                        }) {
                            let slice: &[arch_program::instruction::Instruction] = &instructions;
                            batch.push((transaction_metadata.clone(), slice));
                        }
                    }

                    if !batch.is_empty() {
                        pipe.run(batch, self.metrics.clone()).await?;
                    }
                }

                self.metrics
                    .increment_counter(
                        "transaction_updates_processed",
                        transaction_updates.len() as u64,
                    )
                    .await?;
            }
            Updates::AccountDeletions(account_deletions) => {
                for pipe in self.account_deletion_pipes.iter_mut() {
                    let mut filtered: Vec<crate::datasource::AccountDeletion> = Vec::new();
                    for account_deletion in account_deletions.iter() {
                        if pipe.filters().iter().all(|filter| {
                            filter.filter_account_deletion(&datasource_id, account_deletion)
                        }) {
                            filtered.push(account_deletion.clone());
                        }
                    }
                    if !filtered.is_empty() {
                        pipe.run(filtered, self.metrics.clone()).await?;
                    }
                }

                self.metrics
                    .increment_counter(
                        "account_deletions_processed",
                        account_deletions.len() as u64,
                    )
                    .await?;
            }
            Updates::BlockDetails(block_details_list) => {
                for pipe in self.block_details_pipes.iter_mut() {
                    let mut filtered: Vec<crate::datasource::BlockDetails> = Vec::new();
                    for block_details in block_details_list.iter() {
                        if pipe.filters().iter().all(|filter| {
                            filter.filter_block_details(&datasource_id, block_details)
                        }) {
                            filtered.push(block_details.clone());
                        }
                    }
                    if !filtered.is_empty() {
                        pipe.run(filtered, self.metrics.clone()).await?;
                    }
                }

                self.metrics
                    .increment_counter("block_details_processed", block_details_list.len() as u64)
                    .await?;
            }
            Updates::BitcoinBlocks(blocks) => {
                for pipe in self.bitcoin_block_pipes.iter_mut() {
                    let mut filtered: Vec<BitcoinBlock> = Vec::new();
                    for block in blocks.iter() {
                        if pipe
                            .filters()
                            .iter()
                            .all(|filter| filter.filter_bitcoin_block(&datasource_id, block))
                        {
                            filtered.push(block.clone());
                        }
                    }
                    if !filtered.is_empty() {
                        pipe.run(filtered, self.metrics.clone()).await?;
                    }
                }

                self.metrics
                    .increment_counter("bitcoin_blocks_processed", blocks.len() as u64)
                    .await?;
            }
            Updates::RolledbackTransactions(events) => {
                // Collect pubkeys from all rollback pipes and refresh those accounts
                let mut pubkeys_to_refresh: std::collections::HashSet<
                    arch_program::pubkey::Pubkey,
                > = std::collections::HashSet::new();
                for pipe in self.rolledback_transactions_pipes.iter_mut() {
                    let mut filtered: Vec<RolledbackTransactionsEvent> = Vec::new();
                    for event in events.iter() {
                        if pipe.filters().iter().all(|filter| {
                            filter.filter_rolledback_transactions(&datasource_id, event)
                        }) {
                            filtered.push(event.clone());
                        }
                    }
                    if !filtered.is_empty() {
                        pubkeys_to_refresh.extend(pipe.run(filtered, self.metrics.clone()).await?);
                    }
                }

                self.metrics
                    .increment_counter(
                        "rolledback_transactions_events_processed",
                        events.len() as u64,
                    )
                    .await?;

                // If we have an AccountDatasource and there are pubkeys to refresh, fetch and dispatch
                if let Some(provider) = &self.account_datasource {
                    if !pubkeys_to_refresh.is_empty() {
                        let keys: Vec<arch_program::pubkey::Pubkey> =
                            pubkeys_to_refresh.into_iter().collect();
                        let accounts_map = provider.get_multiple_accounts(&keys).await?;
                        let height = events.iter().map(|event| event.height).max().unwrap_or(0);

                        // Build deletions for accounts that became system-owned with zero lamports
                        let mut deletions: Vec<crate::datasource::AccountDeletion> = Vec::new();
                        for (pubkey, maybe_info) in accounts_map.iter() {
                            match maybe_info {
                                Some(account) => {
                                    if account.owner
                                        == arch_program::pubkey::Pubkey::system_program()
                                        && account.lamports == 0
                                    {
                                        deletions.push(crate::datasource::AccountDeletion {
                                            pubkey: *pubkey,
                                            height,
                                        });
                                    }
                                }
                                None => {
                                    // Treat missing accounts as deleted as well
                                    deletions.push(crate::datasource::AccountDeletion {
                                        pubkey: *pubkey,
                                        height,
                                    });
                                }
                            }
                        }

                        // Dispatch account deletion events
                        if !deletions.is_empty() {
                            for pipe in self.account_deletion_pipes.iter_mut() {
                                let mut filtered: Vec<crate::datasource::AccountDeletion> =
                                    Vec::new();
                                for deletion in deletions.iter() {
                                    if pipe.filters().iter().all(|filter| {
                                        filter.filter_account_deletion(&datasource_id, deletion)
                                    }) {
                                        filtered.push(deletion.clone());
                                    }
                                }
                                if !filtered.is_empty() {
                                    pipe.run(filtered, self.metrics.clone()).await?;
                                }
                            }

                            self.metrics
                                .increment_counter(
                                    "account_deletions_processed",
                                    deletions.len() as u64,
                                )
                                .await?;
                        }

                        for pipe in self.account_pipes.iter_mut() {
                            let mut accounts: Vec<(AccountMetadata, arch_sdk::AccountInfo)> =
                                Vec::new();
                            for (pubkey, maybe_info) in accounts_map.iter() {
                                if let Some(account) = maybe_info.clone() {
                                    let account_metadata = AccountMetadata {
                                        height,
                                        pubkey: *pubkey,
                                    };

                                    accounts.push((account_metadata, account));
                                }
                            }
                            if !accounts.is_empty() {
                                pipe.run(accounts, self.metrics.clone()).await?;
                            }
                        }
                        self.metrics
                            .increment_counter(
                                "account_updates_processed",
                                accounts_map.len() as u64,
                            )
                            .await?;
                    }
                }
            }
            Updates::ReappliedTransactions(events) => {
                let mut pubkeys_to_refresh: std::collections::HashSet<
                    arch_program::pubkey::Pubkey,
                > = std::collections::HashSet::new();
                for pipe in self.reapplied_transactions_pipes.iter_mut() {
                    let mut filtered: Vec<ReappliedTransactionsEvent> = Vec::new();
                    for event in events.iter() {
                        if pipe.filters().iter().all(|filter| {
                            filter.filter_reapplied_transactions(&datasource_id, event)
                        }) {
                            filtered.push(event.clone());
                        }
                    }
                    if !filtered.is_empty() {
                        pubkeys_to_refresh.extend(pipe.run(filtered, self.metrics.clone()).await?);
                    }
                }

                self.metrics
                    .increment_counter(
                        "reapplied_transactions_events_processed",
                        events.len() as u64,
                    )
                    .await?;

                if let Some(provider) = &self.account_datasource {
                    if !pubkeys_to_refresh.is_empty() {
                        let keys: Vec<arch_program::pubkey::Pubkey> =
                            pubkeys_to_refresh.into_iter().collect();
                        let accounts_map = provider.get_multiple_accounts(&keys).await?;
                        let height = events.iter().map(|event| event.height).max().unwrap_or(0);
                        for pipe in self.account_pipes.iter_mut() {
                            let mut filtered: Vec<(AccountMetadata, arch_sdk::AccountInfo)> =
                                Vec::new();
                            for (pubkey, maybe_info) in accounts_map.iter() {
                                if let Some(account) = maybe_info.clone() {
                                    let account_metadata = AccountMetadata {
                                        height,
                                        pubkey: *pubkey,
                                    };

                                    filtered.push((account_metadata, account));
                                }
                            }
                            if !filtered.is_empty() {
                                pipe.run(filtered, self.metrics.clone()).await?;
                            }
                        }

                        self.metrics
                            .increment_counter(
                                "account_updates_processed",
                                accounts_map.len() as u64,
                            )
                            .await?;
                    }
                }
            }
        };

        Ok(())
    }
}

#[derive(Default)]
pub struct PipelineBuilder {
    pub datasources: Vec<(DatasourceId, Arc<dyn Datasource + Send + Sync>)>,
    pub account_pipes: Vec<Box<dyn AccountPipes>>,
    pub account_deletion_pipes: Vec<Box<dyn AccountDeletionPipes>>,
    pub block_details_pipes: Vec<Box<dyn BlockDetailsPipes>>,
    pub bitcoin_block_pipes: Vec<Box<dyn BitcoinBlockPipes>>,
    pub rolledback_transactions_pipes: Vec<Box<dyn RolledbackTransactionsPipes>>,
    pub reapplied_transactions_pipes: Vec<Box<dyn ReappliedTransactionsPipes>>,
    pub instruction_pipes: Vec<Box<dyn for<'a> InstructionPipes<'a>>>,
    pub transaction_pipes: Vec<Box<dyn for<'a> TransactionPipes<'a>>>,
    pub metrics: MetricsCollection,
    pub metrics_flush_interval: Option<u64>,
    pub datasource_cancellation_token: Option<CancellationToken>,
    pub shutdown_strategy: ShutdownStrategy,
    pub channel_buffer_size: usize,
    pub bitcoin_datasource: Option<Arc<dyn BitcoinDatasource + Send + Sync>>,
    pub account_datasource: Option<Arc<dyn AccountDatasource + Send + Sync>>,
}

impl PipelineBuilder {
    /// Creates a new `PipelineBuilder` with empty collections for datasources,
    /// pipes, and metrics.
    ///
    /// This method initializes a `PipelineBuilder` instance, allowing you to
    /// configure each component of a `Pipeline` before building it. The
    /// builder pattern offers flexibility in adding data sources, account
    /// and transaction handling pipes, deletion processing, and metrics
    /// collection features.
    ///
    /// # Example
    ///
    /// ```rust
    /// use atlas_core::pipeline::PipelineBuilder;
    ///
    /// let builder = PipelineBuilder::new();
    /// ```
    pub fn new() -> Self {
        log::trace!("PipelineBuilder::new()");
        Self::default()
    }

    /// Adds a datasource to the pipeline.
    ///
    /// The datasource is responsible for providing updates, such as account and
    /// transaction data, to the pipeline. Multiple datasources can be added
    /// to handle various types of updates.
    ///
    /// # Parameters
    ///
    /// - `datasource`: The data source to add, implementing the `Datasource`
    ///   trait.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use atlas_core::pipeline::PipelineBuilder;
    ///
    /// let builder = PipelineBuilder::new()
    ///     .datasource(MyDatasource::new());
    /// ```
    pub fn datasource(mut self, datasource: impl Datasource + 'static) -> Self {
        log::trace!("datasource(self, datasource: {:?})", stringify!(datasource));
        self.datasources
            .push((DatasourceId::new_unique(), Arc::new(datasource)));
        self
    }

    /// Adds a datasource to the pipeline with a specific ID.
    ///
    /// This method allows you to assign a custom ID to a datasource, which is
    /// useful for filtering updates based on their source. The ID can be used
    /// with filters to selectively process updates from specific datasources.
    ///
    /// # Parameters
    ///
    /// - `datasource`: The data source to add, implementing the `Datasource`
    ///   trait
    /// - `id`: The `DatasourceId` to assign to this datasource
    ///
    /// # Example
    ///
    /// ```ignore
    /// use atlas_core::{pipeline::PipelineBuilder, datasource::DatasourceId};
    ///
    /// let mainnet_id = DatasourceId::new_named("mainnet");
    /// let builder = PipelineBuilder::new()
    ///     .datasource_with_id(mainnet_id, MyDatasource::new());
    /// ```
    pub fn datasource_with_id(
        mut self,
        datasource: impl Datasource + 'static,
        id: DatasourceId,
    ) -> Self {
        log::trace!(
            "datasource_with_id(self, id: {:?}, datasource: {:?})",
            id,
            stringify!(datasource)
        );
        self.datasources.push((id, Arc::new(datasource)));
        self
    }

    /// Sets the shutdown strategy for the pipeline.
    ///
    /// This method configures how the pipeline should handle shutdowns. The
    /// shutdown strategy defines whether the pipeline should terminate
    /// immediately or continue processing pending updates after terminating
    /// the data sources.
    ///
    /// # Parameters
    ///
    /// - `shutdown_strategy`: A variant of [`ShutdownStrategy`] that determines
    ///   how the pipeline should handle shutdowns.
    ///
    /// # Returns
    ///
    /// Returns `Self`, allowing for method chaining.
    ///
    /// # Notes
    ///
    /// - Use `ShutdownStrategy::Immediate` to stop the entire pipeline
    ///   instantly, including all active processing tasks.
    /// - Use `ShutdownStrategy::ProcessPending` (the default) to terminate data
    ///   sources first and allow the pipeline to finish processing any updates
    ///   that are still pending.
    pub fn shutdown_strategy(mut self, shutdown_strategy: ShutdownStrategy) -> Self {
        log::trace!(
            "shutdown_strategy(self, shutdown_strategy: {:?})",
            shutdown_strategy
        );
        self.shutdown_strategy = shutdown_strategy;
        self
    }

    /// Adds an account pipe to process account updates.
    ///
    /// Account pipes decode and process updates to accounts within the
    /// pipeline. This method requires both an `AccountDecoder` and a
    /// `Processor` to handle decoded account data.
    ///
    /// # Parameters
    ///
    /// - `decoder`: An `AccountDecoder` that decodes the account data.
    /// - `processor`: A `Processor` that processes the decoded account data.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use atlas_core::pipeline::PipelineBuilder;
    ///
    /// let builder = PipelineBuilder::new()
    ///     .account(MyAccountDecoder, MyAccountProcessor);
    /// ```
    pub fn account<T: Send + Sync + 'static>(
        mut self,
        decoder: impl for<'a> AccountDecoder<'a, AccountType = T> + Send + Sync + 'static,
        processor: impl Processor<InputType = AccountProcessorInputType<T>, OutputType = ()>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        log::trace!(
            "account(self, decoder: {:?}, processor: {:?})",
            stringify!(decoder),
            stringify!(processor)
        );
        self.account_pipes.push(Box::new(AccountPipe {
            decoder: Box::new(decoder),
            processor: Box::new(processor),
            filters: vec![],
        }));
        self
    }

    /// Adds an account pipe with filters to process account updates selectively.
    ///
    /// This method creates an account pipe that only processes updates that pass
    /// all the specified filters. Filters can be used to selectively process
    /// updates based on criteria such as datasource ID, account properties, or
    /// other custom logic.
    ///
    /// # Parameters
    ///
    /// - `decoder`: An `AccountDecoder` that decodes the account data
    /// - `processor`: A `Processor` that processes the decoded account data
    /// - `filters`: A collection of filters that determine which account updates
    ///   should be processed
    ///
    /// # Example
    ///
    /// ```ignore
    /// use atlas_core::{
    ///     pipeline::PipelineBuilder,
    ///     datasource::DatasourceId,
    ///     filter::DatasourceFilter,
    /// };
    ///
    /// let mainnet_id = DatasourceId::new_named("mainnet");
    /// let filter = DatasourceFilter::new(mainnet_id);
    /// let filters = vec![Box::new(filter) as Box<dyn atlas_core::filter::Filter>];
    ///
    /// let builder = PipelineBuilder::new()
    ///     .account_with_filters(MyAccountDecoder, MyAccountProcessor, filters);
    /// ```
    pub fn account_with_filters<T: Send + Sync + 'static>(
        mut self,
        decoder: impl for<'a> AccountDecoder<'a, AccountType = T> + Send + Sync + 'static,
        processor: impl Processor<InputType = AccountProcessorInputType<T>, OutputType = ()>
            + Send
            + Sync
            + 'static,
        filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
    ) -> Self {
        log::trace!(
            "account_with_filters(self, decoder: {:?}, processor: {:?}, filters: {:?})",
            stringify!(decoder),
            stringify!(processor),
            stringify!(filters)
        );
        self.account_pipes.push(Box::new(AccountPipe {
            decoder: Box::new(decoder),
            processor: Box::new(processor),
            filters,
        }));
        self
    }

    /// Adds an account deletion pipe to handle account deletion events.
    ///
    /// Account deletion pipes process deletions of accounts, with a `Processor`
    /// to handle the deletion events as they occur.
    ///
    /// # Parameters
    ///
    /// - `processor`: A `Processor` that processes account deletion events.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use atlas_core::pipeline::PipelineBuilder;
    ///
    /// let builder = PipelineBuilder::new()
    ///     .account_deletions(MyAccountDeletionProcessor);
    /// ```
    pub fn account_deletions(
        mut self,
        processor: impl Processor<InputType = AccountDeletion, OutputType = ()> + Send + Sync + 'static,
    ) -> Self {
        log::trace!(
            "account_deletions(self, processor: {:?})",
            stringify!(processor)
        );
        self.account_deletion_pipes
            .push(Box::new(AccountDeletionPipe {
                processor: Box::new(processor),
                filters: vec![],
            }));
        self
    }

    /// Adds an account deletion pipe with filters to handle account deletion events selectively.
    ///
    /// This method creates an account deletion pipe that only processes deletion
    /// events that pass all the specified filters. Filters can be used to
    /// selectively process deletions based on criteria such as datasource ID or
    /// other custom logic.
    ///
    /// # Parameters
    ///
    /// - `processor`: A `Processor` that processes account deletion events
    /// - `filters`: A collection of filters that determine which account deletion
    ///   events should be processed
    ///
    /// # Example
    ///
    /// ```ignore
    /// use atlas_core::{
    ///     pipeline::PipelineBuilder,
    ///     datasource::DatasourceId,
    ///     filter::DatasourceFilter,
    /// };
    ///
    /// let mainnet_id = DatasourceId::new_named("mainnet");
    /// let filter = DatasourceFilter::new(mainnet_id);
    /// let filters = vec![Box::new(filter) as Box<dyn atlas_core::filter::Filter>];
    ///
    /// let builder = PipelineBuilder::new()
    ///     .account_deletions_with_filters(MyAccountDeletionProcessor, filters);
    /// ```
    pub fn account_deletions_with_filters(
        mut self,
        processor: impl Processor<InputType = AccountDeletion, OutputType = ()> + Send + Sync + 'static,
        filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
    ) -> Self {
        log::trace!(
            "account_deletions_with_filters(self, processor: {:?}, filters: {:?})",
            stringify!(processor),
            stringify!(filters)
        );
        self.account_deletion_pipes
            .push(Box::new(AccountDeletionPipe {
                processor: Box::new(processor),
                filters,
            }));
        self
    }

    /// Adds a block details pipe to handle block details updates.
    ///
    /// Block details pipes process updates related to block metadata, such as
    /// slot, block hash, and rewards, with a `Processor` to handle the updates.
    ///
    /// # Parameters
    ///
    /// - `processor`: A `Processor` that processes block details updates.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use atlas_core::pipeline::PipelineBuilder;
    ///
    /// let builder = PipelineBuilder::new()
    ///     .block_details(MyBlockDetailsProcessor);
    /// ```
    pub fn block_details(
        mut self,
        processor: impl Processor<InputType = BlockDetails, OutputType = ()> + Send + Sync + 'static,
    ) -> Self {
        log::trace!(
            "block_details(self, processor: {:?})",
            stringify!(processor)
        );
        self.block_details_pipes.push(Box::new(BlockDetailsPipe {
            processor: Box::new(processor),
            filters: vec![],
        }));
        self
    }

    /// Adds a bitcoin block pipe to handle bitcoin block updates.
    pub fn bitcoin_blocks(
        mut self,
        processor: impl Processor<InputType = BitcoinBlock, OutputType = ()> + Send + Sync + 'static,
    ) -> Self {
        log::trace!(
            "bitcoin_blocks(self, processor: {:?})",
            stringify!(processor)
        );
        self.bitcoin_block_pipes.push(Box::new(BitcoinBlockPipe {
            processor: Box::new(processor),
            filters: vec![],
        }));
        self
    }

    /// Adds a bitcoin block pipe with filters to handle bitcoin block updates selectively.
    pub fn bitcoin_blocks_with_filters(
        mut self,
        processor: impl Processor<InputType = BitcoinBlock, OutputType = ()> + Send + Sync + 'static,
        filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
    ) -> Self {
        log::trace!(
            "bitcoin_blocks_with_filters(self, processor: {:?}, filters: {:?})",
            stringify!(processor),
            stringify!(filters)
        );
        self.bitcoin_block_pipes.push(Box::new(BitcoinBlockPipe {
            processor: Box::new(processor),
            filters,
        }));
        self
    }

    /// Adds a pipe to handle rolled back transactions events.
    pub fn rolledback_transactions(
        mut self,
        processor: impl Processor<
                InputType = RolledbackTransactionsEvent,
                OutputType = HashSet<arch_program::pubkey::Pubkey>,
            > + Send
            + Sync
            + 'static,
    ) -> Self {
        log::trace!(
            "rolledback_transactions(self, processor: {:?})",
            stringify!(processor)
        );
        self.rolledback_transactions_pipes
            .push(Box::new(RolledbackTransactionsPipe {
                processor: Box::new(processor),
                filters: vec![],
            }));
        self
    }

    /// Adds a pipe with filters to handle rolled back transactions events selectively.
    pub fn rolledback_transactions_with_filters(
        mut self,
        processor: impl Processor<
                InputType = RolledbackTransactionsEvent,
                OutputType = HashSet<arch_program::pubkey::Pubkey>,
            > + Send
            + Sync
            + 'static,
        filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
    ) -> Self {
        log::trace!(
            "rolledback_transactions_with_filters(self, processor: {:?}, filters: {:?})",
            stringify!(processor),
            stringify!(filters)
        );
        self.rolledback_transactions_pipes
            .push(Box::new(RolledbackTransactionsPipe {
                processor: Box::new(processor),
                filters,
            }));
        self
    }

    /// Adds a pipe to handle reapplied transactions events.
    pub fn reapplied_transactions(
        mut self,
        processor: impl Processor<
                InputType = ReappliedTransactionsEvent,
                OutputType = HashSet<arch_program::pubkey::Pubkey>,
            > + Send
            + Sync
            + 'static,
    ) -> Self {
        log::trace!(
            "reapplied_transactions(self, processor: {:?})",
            stringify!(processor)
        );
        self.reapplied_transactions_pipes
            .push(Box::new(ReappliedTransactionsPipe {
                processor: Box::new(processor),
                filters: vec![],
            }));
        self
    }

    /// Adds a pipe with filters to handle reapplied transactions events selectively.
    pub fn reapplied_transactions_with_filters(
        mut self,
        processor: impl Processor<
                InputType = ReappliedTransactionsEvent,
                OutputType = HashSet<arch_program::pubkey::Pubkey>,
            > + Send
            + Sync
            + 'static,
        filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
    ) -> Self {
        log::trace!(
            "reapplied_transactions_with_filters(self, processor: {:?}, filters: {:?})",
            stringify!(processor),
            stringify!(filters)
        );
        self.reapplied_transactions_pipes
            .push(Box::new(ReappliedTransactionsPipe {
                processor: Box::new(processor),
                filters,
            }));
        self
    }

    /// Adds a block details pipe with filters to handle block details updates selectively.
    ///
    /// This method creates a block details pipe that only processes updates that
    /// pass all the specified filters. Filters can be used to selectively process
    /// block details updates based on criteria such as datasource ID, block height,
    /// or other custom logic.
    ///
    /// # Parameters
    ///
    /// - `processor`: A `Processor` that processes block details updates
    /// - `filters`: A collection of filters that determine which block details
    ///   updates should be processed
    ///
    /// # Example
    ///
    /// ```ignore
    /// use atlas_core::{
    ///     pipeline::PipelineBuilder,
    ///     datasource::DatasourceId,
    ///     filter::DatasourceFilter,
    /// };
    ///
    /// let mainnet_id = DatasourceId::new_named("mainnet");
    /// let filter = DatasourceFilter::new(mainnet_id);
    /// let filters = vec![Box::new(filter) as Box<dyn atlas_core::filter::Filter>];
    ///
    /// let builder = PipelineBuilder::new()
    ///     .block_details_with_filters(MyBlockDetailsProcessor, filters);
    /// ```
    pub fn block_details_with_filters(
        mut self,
        processor: impl Processor<InputType = BlockDetails, OutputType = ()> + Send + Sync + 'static,
        filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
    ) -> Self {
        log::trace!(
            "block_details_with_filters(self, processor: {:?}, filters: {:?})",
            stringify!(processor),
            stringify!(filters)
        );
        self.block_details_pipes.push(Box::new(BlockDetailsPipe {
            processor: Box::new(processor),
            filters,
        }));
        self
    }

    /// Adds an instruction pipe to process instructions within transactions.
    ///
    /// Instruction pipes decode and process individual instructions,
    /// enabling specialized handling of various instruction types.
    ///
    /// # Parameters
    ///
    /// - `decoder`: An `InstructionDecoder` for decoding instructions from
    ///   transaction data.
    /// - `processor`: A `Processor` that processes decoded instruction data.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use atlas_core::pipeline::PipelineBuilder;
    ///
    /// let builder = PipelineBuilder::new()
    ///     .instruction(MyDecoder, MyInstructionProcessor);
    /// ```
    pub fn instruction<T: Send + Sync + 'static>(
        mut self,
        decoder: impl for<'a> InstructionDecoder<'a, InstructionType = T> + Send + Sync + 'static,
        processor: impl Processor<InputType = InstructionProcessorInputType<T>, OutputType = ()>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        log::trace!(
            "instruction(self, decoder: {:?}, processor: {:?})",
            stringify!(decoder),
            stringify!(processor)
        );
        self.instruction_pipes.push(Box::new(InstructionPipe {
            decoder: Box::new(decoder),
            processor: Box::new(processor),
            filters: vec![],
        }));
        self
    }

    /// Adds an instruction pipe with filters to process instructions selectively.
    ///
    /// This method creates an instruction pipe that only processes instructions
    /// that pass all the specified filters. Filters can be used to selectively
    /// process instructions based on criteria such as datasource ID, instruction
    /// type, or other custom logic.
    ///
    /// # Parameters
    ///
    /// - `decoder`: An `InstructionDecoder` for decoding instructions from
    ///   transaction data
    /// - `processor`: A `Processor` that processes decoded instruction data
    /// - `filters`: A collection of filters that determine which instructions
    ///   should be processed
    ///
    /// # Example
    ///
    /// ```ignore
    /// use atlas_core::{
    ///     pipeline::PipelineBuilder,
    ///     datasource::DatasourceId,
    ///     filter::DatasourceFilter,
    /// };
    ///
    /// let mainnet_id = DatasourceId::new_named("mainnet");
    /// let filter = DatasourceFilter::new(mainnet_id);
    /// let filters = vec![Box::new(filter) as Box<dyn atlas_core::filter::Filter>];
    ///
    /// let builder = PipelineBuilder::new()
    ///     .instruction_with_filters(MyDecoder, MyInstructionProcessor, filters);
    /// ```
    pub fn instruction_with_filters<T: Send + Sync + 'static>(
        mut self,
        decoder: impl for<'a> InstructionDecoder<'a, InstructionType = T> + Send + Sync + 'static,
        processor: impl Processor<InputType = InstructionProcessorInputType<T>, OutputType = ()>
            + Send
            + Sync
            + 'static,
        filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
    ) -> Self {
        log::trace!(
            "instruction_with_filters(self, decoder: {:?}, processor: {:?}, filters: {:?})",
            stringify!(decoder),
            stringify!(processor),
            stringify!(filters)
        );
        self.instruction_pipes.push(Box::new(InstructionPipe {
            decoder: Box::new(decoder),
            processor: Box::new(processor),
            filters,
        }));
        self
    }

    /// Adds a transaction pipe for processing full transaction data.
    ///
    /// This method requires a transaction schema for decoding and a `Processor`
    /// to handle the processed transaction data.
    ///
    /// # Parameters
    ///
    /// - `schema`: A `TransactionSchema` used to match and interpret
    ///   transaction data.
    /// - `processor`: A `Processor` that processes the decoded transaction
    ///   data.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use atlas_core::pipeline::PipelineBuilder;
    ///
    /// let builder = PipelineBuilder::new()
    ///     .transaction(MyTransactionProcessor, Some(MY_SCHEMA.clone()));
    /// ```
    pub fn transaction<T, U>(
        mut self,
        processor: impl Processor<InputType = TransactionProcessorInputType<T, U>, OutputType = ()>
            + Send
            + Sync
            + 'static,
        schema: Option<TransactionSchema<T>>,
    ) -> Self
    where
        T: InstructionDecoderCollection + 'static,
        U: DeserializeOwned + Send + Sync + 'static,
    {
        log::trace!(
            "transaction(self, schema: {:?}, processor: {:?})",
            stringify!(schema),
            stringify!(processor)
        );
        self.transaction_pipes
            .push(Box::new(TransactionPipe::<T, U>::new(
                schema,
                processor,
                vec![],
            )));
        self
    }

    /// Adds a transaction pipe with filters for processing full transaction data selectively.
    ///
    /// This method creates a transaction pipe that only processes transactions
    /// that pass all the specified filters. Filters can be used to selectively
    /// process transactions based on criteria such as datasource ID, transaction
    /// type, or other custom logic.
    ///
    /// # Parameters
    ///
    /// - `processor`: A `Processor` that processes the decoded transaction data
    /// - `schema`: A `TransactionSchema` used to match and interpret
    ///   transaction data
    /// - `filters`: A collection of filters that determine which transactions
    ///   should be processed
    ///
    /// # Example
    ///
    /// ```ignore
    /// use atlas_core::{
    ///     pipeline::PipelineBuilder,
    ///     datasource::DatasourceId,
    ///     filter::DatasourceFilter,
    /// };
    ///
    /// let mainnet_id = DatasourceId::new_named("mainnet");
    /// let filter = DatasourceFilter::new(mainnet_id);
    /// let filters = vec![Box::new(filter) as Box<dyn atlas_core::filter::Filter>];
    ///
    /// let builder = PipelineBuilder::new()
    ///     .transaction_with_filters(MyTransactionProcessor, Some(MY_SCHEMA.clone()), filters);
    /// ```
    pub fn transaction_with_filters<T, U>(
        mut self,
        processor: impl Processor<InputType = TransactionProcessorInputType<T, U>, OutputType = ()>
            + Send
            + Sync
            + 'static,
        schema: Option<TransactionSchema<T>>,
        filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
    ) -> Self
    where
        T: InstructionDecoderCollection + 'static,
        U: DeserializeOwned + Send + Sync + 'static,
    {
        log::trace!(
            "transaction_with_filters(self, schema: {:?}, processor: {:?}, filters: {:?})",
            stringify!(schema),
            stringify!(processor),
            stringify!(filters)
        );
        self.transaction_pipes
            .push(Box::new(TransactionPipe::<T, U>::new(
                schema, processor, filters,
            )));
        self
    }

    /// Adds a metrics component to the pipeline for performance tracking.
    ///
    /// This component collects and reports on pipeline metrics, providing
    /// insights into performance and operational statistics.
    ///
    /// # Parameters
    ///
    /// - `metrics`: An instance of a `Metrics` implementation, used to gather
    ///   and report metrics.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::sync::Arc;
    /// use atlas_core::pipeline::PipelineBuilder;
    ///
    /// let builder = PipelineBuilder::new()
    ///     .metrics(Arc::new(LogMetrics::new()));
    /// ```
    pub fn metrics(mut self, metrics: Arc<dyn Metrics>) -> Self {
        log::trace!("metrics(self, metrics: {:?})", stringify!(metrics));
        self.metrics.metrics.push(metrics);
        self
    }

    /// Sets the optional Bitcoin datasource used to fetch raw Bitcoin transactions by txid.
    pub fn bitcoin_datasource(
        mut self,
        datasource: Arc<dyn BitcoinDatasource + Send + Sync>,
    ) -> Self {
        log::trace!(
            "bitcoin_datasource(self, datasource: {:?})",
            stringify!(datasource)
        );
        self.bitcoin_datasource = Some(datasource);
        self
    }

    /// Sets the optional Account datasource used to fetch accounts by pubkey on demand.
    pub fn account_datasource(
        mut self,
        datasource: Arc<dyn AccountDatasource + Send + Sync>,
    ) -> Self {
        log::trace!(
            "account_datasource(self, datasource: {:?})",
            stringify!(datasource)
        );

        self.account_datasource = Some(datasource);
        self
    }

    /// Sets the interval for flushing metrics data.
    ///
    /// This value defines the frequency, in seconds, at which metrics data is
    /// flushed from memory. If not set, a default interval is used.
    ///
    /// # Parameters
    ///
    /// - `interval`: The flush interval for metrics, in seconds.
    ///
    /// # Example
    ///
    /// ```rust
    /// use atlas_core::pipeline::PipelineBuilder;
    ///
    /// let builder = PipelineBuilder::new()
    ///     .metrics_flush_interval(60);
    /// ```
    pub fn metrics_flush_interval(mut self, interval: u64) -> Self {
        log::trace!("metrics_flush_interval(self, interval: {:?})", interval);
        self.metrics_flush_interval = Some(interval);
        self
    }

    /// Sets the cancellation token for cancelling datasource on demand.
    ///
    /// This value is used to cancel datasource on demand.
    /// If not set, a default `CancellationToken` is used.
    ///
    /// # Parameters
    ///
    /// - `cancellation_token`: An instance of `CancellationToken`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use atlas_core::pipeline::PipelineBuilder;
    /// use tokio_util::sync::CancellationToken;
    ///
    /// let builder = PipelineBuilder::new()
    ///     .datasource_cancellation_token(CancellationToken::new());
    /// ```
    pub fn datasource_cancellation_token(mut self, cancellation_token: CancellationToken) -> Self {
        log::trace!(
            "datasource_cancellation_token(self, cancellation_token: {:?})",
            cancellation_token
        );
        self.datasource_cancellation_token = Some(cancellation_token);
        self
    }

    /// Sets the size of the channel buffer for the pipeline.
    ///
    /// This value defines the maximum number of updates that can be queued in
    /// the pipeline's channel buffer. If not set, a default size of 10_000
    /// will be used.
    ///
    /// # Parameters
    ///
    /// - `size`: The size of the channel buffer for the pipeline.
    ///
    /// # Example
    ///
    /// ```rust
    /// use atlas_core::pipeline::PipelineBuilder;
    ///
    /// let builder = PipelineBuilder::new()
    ///     .channel_buffer_size(1000);
    /// ```
    pub fn channel_buffer_size(mut self, size: usize) -> Self {
        log::trace!("channel_buffer_size(self, size: {:?})", size);
        self.channel_buffer_size = size;
        self
    }

    /// Builds and returns a `Pipeline` configured with the specified
    /// components.
    ///
    /// After configuring the `PipelineBuilder` with data sources, pipes, and
    /// metrics, call this method to create the final `Pipeline` instance
    /// ready for operation.
    ///
    /// # Returns
    ///
    /// Returns an `IndexerResult<Pipeline>` containing the configured `Pipeline`,
    /// or an error if any part of the configuration is invalid.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::sync::Arc;
    ///
    /// atlas_core::pipeline::Pipeline::builder()
    /// .datasource(transaction_crawler)
    /// .metrics(Arc::new(LogMetrics::new()))
    /// .metrics(Arc::new(PrometheusMetrics::new()))
    /// .instruction(
    ///    TestProgramDecoder,
    ///    TestProgramProcessor
    /// )
    /// .account(
    ///     TestProgramDecoder,
    ///     TestProgramAccountProcessor
    /// )
    /// .transaction(TEST_SCHEMA.clone(), TestProgramTransactionProcessor)
    /// .account_deletions(TestProgramAccountDeletionProcessor)
    /// .channel_buffer_size(1000)
    /// .build()?;
    ///
    ///  Ok(())
    /// ```
    pub fn build(self) -> IndexerResult<Pipeline> {
        log::trace!("build(self)");
        Ok(Pipeline {
            datasources: self.datasources,
            account_pipes: self.account_pipes,
            account_deletion_pipes: self.account_deletion_pipes,
            block_details_pipes: self.block_details_pipes,
            bitcoin_block_pipes: self.bitcoin_block_pipes,
            rolledback_transactions_pipes: self.rolledback_transactions_pipes,
            reapplied_transactions_pipes: self.reapplied_transactions_pipes,
            instruction_pipes: self.instruction_pipes,
            transaction_pipes: self.transaction_pipes,
            shutdown_strategy: self.shutdown_strategy,
            metrics: Arc::new(self.metrics),
            metrics_flush_interval: self.metrics_flush_interval,
            datasource_cancellation_token: self.datasource_cancellation_token,
            channel_buffer_size: self.channel_buffer_size,
            bitcoin_datasource: self.bitcoin_datasource,
            account_datasource: self.account_datasource,
        })
    }
}
