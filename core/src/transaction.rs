use crate::filter::Filter;
use crate::instruction::{DecodedInstruction, InstructionMetadata};
use crate::transformers::unnest_parsed_instructions;
use crate::{error::IndexerResult, instruction::NestedInstruction};
use arch_program::sanitized::ArchMessage;

use arch_program::pubkey::Pubkey;
pub use arch_sdk::Signature;
use arch_sdk::{InnerInstructionsList, RollbackStatus, Status};
use {
    crate::{
        collection::InstructionDecoderCollection,
        metrics::MetricsCollection,
        processor::Processor,
        schema::{ParsedInstruction, TransactionSchema},
    },
    async_trait::async_trait,
    core::convert::TryFrom,
    serde::de::DeserializeOwned,
    std::sync::Arc,
};

#[derive(Debug, Clone)]
pub struct TransactionMetadata {
    pub id: String,
    pub fee_payer: Pubkey,
    pub message: ArchMessage,
    pub status: Status,
    pub log_messages: Vec<String>,
    pub rollback_status: RollbackStatus,
    pub block_height: u64,
    pub bitcoin_txid: Option<String>,
    pub bitcoin_tx: Option<crate::bitcoin::Transaction>,
    pub inner_instructions_list: InnerInstructionsList,
}

impl TryFrom<crate::datasource::TransactionUpdate> for TransactionMetadata {
    type Error = crate::error::Error;

    fn try_from(value: crate::datasource::TransactionUpdate) -> Result<Self, Self::Error> {
        log::trace!("try_from(transaction_update: {:?})", value);
        let accounts = value
            .transaction
            .runtime_transaction
            .message
            .get_unique_instruction_account_keys();

        Ok(TransactionMetadata {
            id: value.transaction.txid().to_string(),
            fee_payer: *accounts
                .iter()
                .next()
                .ok_or(crate::error::Error::MissingFeePayer)?,
            message: value.transaction.runtime_transaction.message.clone(),
            status: value.transaction.status,
            log_messages: value.transaction.logs,
            rollback_status: value.transaction.rollback_status,
            block_height: value.height,
            bitcoin_txid: value.transaction.bitcoin_txid.map(|txid| txid.to_string()),
            bitcoin_tx: None,
            inner_instructions_list: value.transaction.inner_instructions_list,
        })
    }
}

pub type TransactionProcessorInputType<T, U = ()> = (
    Arc<TransactionMetadata>,
    Vec<(InstructionMetadata, DecodedInstruction<T>)>,
    Option<U>,
);

pub struct TransactionPipe<T: InstructionDecoderCollection, U> {
    schema: Option<TransactionSchema<T>>,
    processor: Box<
        dyn Processor<InputType = TransactionProcessorInputType<T, U>, OutputType = ()>
            + Send
            + Sync,
    >,
    filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
}

/// Represents a parsed transaction, including its metadata and parsed
/// instructions.
pub struct ParsedTransaction<I: InstructionDecoderCollection> {
    pub metadata: TransactionMetadata,
    pub instructions: Vec<ParsedInstruction<I>>,
}

impl<T: InstructionDecoderCollection, U> TransactionPipe<T, U> {
    pub fn new(
        schema: Option<TransactionSchema<T>>,
        processor: impl Processor<InputType = TransactionProcessorInputType<T, U>, OutputType = ()>
            + Send
            + Sync
            + 'static,
        filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
    ) -> Self {
        log::trace!(
            "TransactionPipe::new(schema: {:?}, processor: {:?})",
            schema,
            stringify!(processor)
        );
        Self {
            schema,
            processor: Box::new(processor),
            filters,
        }
    }

    fn matches_schema(&self, instructions: &[ParsedInstruction<T>]) -> Option<U>
    where
        U: DeserializeOwned,
    {
        match self.schema {
            Some(ref schema) => schema.match_schema(instructions),
            None => None,
        }
    }
}

pub fn parse_instructions<T: InstructionDecoderCollection>(
    nested_ixs: &[NestedInstruction],
) -> Vec<ParsedInstruction<T>> {
    log::trace!("parse_instructions(nested_ixs: {:?})", nested_ixs);

    let mut parsed_instructions: Vec<ParsedInstruction<T>> = Vec::new();

    for nested_ix in nested_ixs {
        if let Some(instruction) = T::parse_instruction(&nested_ix.instruction) {
            parsed_instructions.push(ParsedInstruction {
                program_id: nested_ix.instruction.program_id,
                instruction,
                inner_instructions: parse_instructions(&nested_ix.inner_instructions),
            });
        } else {
            for inner_ix in nested_ix.inner_instructions.iter() {
                parsed_instructions.extend(parse_instructions(&[inner_ix.clone()]));
            }
        }
    }

    parsed_instructions
}

#[async_trait]
/// Parses instructions for each transaction and forwards to a processor.
///
/// Implementations should apply `Filter`s before processing and handle
/// large batches efficiently. Implementors are expected to only process
/// instructions newer than their configured cutoff when used with sync.
pub trait TransactionPipes<'a>: Send + Sync {
    async fn run(
        &mut self,
        transactions: Vec<(Arc<TransactionMetadata>, &[NestedInstruction])>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()>;

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>>;
}

#[async_trait]
impl<T, U> TransactionPipes<'_> for TransactionPipe<T, U>
where
    T: InstructionDecoderCollection + Sync + 'static,
    U: DeserializeOwned + Send + Sync + 'static,
{
    async fn run(
        &mut self,
        transactions: Vec<(Arc<TransactionMetadata>, &[NestedInstruction])>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        log::trace!(
            "TransactionPipe::run(transactions: {:?}, metrics)",
            transactions,
        );

        let mut processed_transactions: Vec<TransactionProcessorInputType<T, U>> = Vec::new();
        for (transaction_metadata, instructions) in transactions {
            let parsed_instructions = parse_instructions(instructions);

            let unnested_instructions = unnest_parsed_instructions(
                transaction_metadata.clone(),
                parsed_instructions.clone(),
                0,
                vec![],
            );

            let matched_data = self.matches_schema(&parsed_instructions);
            processed_transactions.push((
                transaction_metadata,
                unnested_instructions,
                matched_data,
            ));
        }
        self.processor
            .process(processed_transactions, metrics)
            .await?;

        Ok(())
    }

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>> {
        &self.filters
    }
}
