//! Provides utility functions to transform transaction data into various
//! representations within the `atlas_arch` framework.
//!
//! This module includes functions for extracting transaction metadata, parsing
//! instructions, and nesting instructions based on stack depth. It also offers
//! transformations for Solana transaction components into suitable formats for
//! the framework, enabling flexible processing of transaction data.
//!
//! ## Key Components
//!
//! - **Metadata Extraction**: Extracts essential transaction metadata for
//!   processing.
//! - **Instruction Parsing**: Parses both top-level and nested instructions
//!   from transactions.
//! - **Account Metadata**: Converts account data into a standardized format for
//!   transactions.
//!
//! ## Notes
//!
//! - The module supports both legacy and v0 transactions, including handling of
//!   loaded addresses and inner instructions.

use {
    crate::{
        datasource::TransactionUpdate,
        error::{Error, IndexerResult},
        instruction::Instructions,
        transaction::TransactionMetadata,
    },
    arch_program::{
        account::AccountMeta,
        instruction::Instruction,
        sanitized::{ArchMessage, SanitizedInstruction},
    },
    std::sync::Arc,
};

use arch_program::pubkey::Pubkey;

pub fn extract_instructions_with_metadata(
    transaction_metadata: &Arc<TransactionMetadata>,
    transaction_update: &TransactionUpdate,
) -> IndexerResult<Instructions> {
    log::trace!(
        "extract_instructions_with_metadata(transaction_metadata: {:?}, transaction_update: {:?})",
        transaction_metadata,
        transaction_update
    );

    let message = &transaction_update.transaction.runtime_transaction.message;
    let mut instructions_with_metadata = Vec::with_capacity(32);

    process_instructions(
        &message.account_keys,
        &message.instructions,
        &mut instructions_with_metadata,
        |_, idx| message.is_writable_index(idx),
        |_, idx| message.is_signer(idx),
    );

    Ok(Instructions(instructions_with_metadata))
}

fn process_instructions<F1, F2>(
    account_keys: &[Pubkey],
    instructions: &[SanitizedInstruction],
    result: &mut Vec<Instruction>,
    is_writable: F1,
    is_signer: F2,
) where
    F1: Fn(&Pubkey, usize) -> bool,
    F2: Fn(&Pubkey, usize) -> bool,
{
    for compiled_instruction in instructions.iter() {
        result.push(build_instruction(
            account_keys,
            compiled_instruction,
            &is_writable,
            &is_signer,
        ));
    }
}

fn build_instruction<F1, F2>(
    account_keys: &[Pubkey],
    instruction: &SanitizedInstruction,
    is_writable: &F1,
    is_signer: &F2,
) -> Instruction
where
    F1: Fn(&Pubkey, usize) -> bool,
    F2: Fn(&Pubkey, usize) -> bool,
{
    let program_id = *account_keys
        .get(instruction.program_id_index as usize)
        .unwrap_or(&Pubkey::default());

    let accounts = instruction
        .accounts
        .iter()
        .filter_map(|account_idx| {
            account_keys
                .get(*account_idx as usize)
                .map(|key| AccountMeta {
                    pubkey: *key,
                    is_writable: is_writable(key, *account_idx as usize),
                    is_signer: is_signer(key, *account_idx as usize),
                })
        })
        .collect();

    Instruction {
        program_id,
        accounts,
        data: instruction.data.clone(),
    }
}

/// Extracts account metadata from a compiled instruction and transaction
/// message.
///
/// This function converts each account index within the instruction into an
/// `AccountMeta` struct, providing details on account keys, signer status, and
/// write permissions.
///
/// # Parameters
///
/// - `compiled_instruction`: The compiled instruction to extract accounts from.
/// - `message`: The transaction message containing the account keys.
///
/// # Returns
///
/// An `IndexerResult<Vec<AccountMeta>>` containing
/// metadata for each account involved in the instruction.
///
/// # Errors
///
/// Returns an error if any referenced account key is missing from the
/// transaction.
pub fn extract_account_metas(
    compiled_instruction: &SanitizedInstruction,
    message: &ArchMessage,
) -> IndexerResult<Vec<AccountMeta>> {
    log::trace!(
        "extract_account_metas(compiled_instruction: {:?}, message: {:?})",
        compiled_instruction,
        message
    );
    let mut accounts = Vec::<AccountMeta>::with_capacity(compiled_instruction.accounts.len());

    for account_index in compiled_instruction.accounts.iter() {
        accounts.push(AccountMeta {
            pubkey: *message
                .account_keys
                .get(*account_index as usize)
                .ok_or(Error::MissingAccountInTransaction)?,
            is_signer: message.is_signer(*account_index as usize),
            is_writable: message.is_writable_index(*account_index as usize),
        });
    }

    Ok(accounts)
}
