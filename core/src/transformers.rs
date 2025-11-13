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
        collection::InstructionDecoderCollection,
        datasource::TransactionUpdate,
        error::{Error, IndexerResult},
        instruction::{DecodedInstruction, InstructionMetadata, MAX_INSTRUCTION_STACK_DEPTH},
        schema::ParsedInstruction,
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
) -> IndexerResult<Vec<(InstructionMetadata, Instruction)>> {
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
        transaction_metadata,
        &mut instructions_with_metadata,
        |_, idx| message.is_writable_index(idx),
        |_, idx| message.is_signer(idx),
    );

    Ok(instructions_with_metadata)
}

fn process_instructions<F1, F2>(
    account_keys: &[Pubkey],
    instructions: &[SanitizedInstruction],
    transaction_metadata: &Arc<TransactionMetadata>,
    result: &mut Vec<(InstructionMetadata, Instruction)>,
    is_writable: F1,
    is_signer: F2,
) where
    F1: Fn(&Pubkey, usize) -> bool,
    F2: Fn(&Pubkey, usize) -> bool,
{
    for (i, compiled_instruction) in instructions.iter().enumerate() {
        let i: Option<u8> = i.try_into().ok();

        if i.is_none() {
            continue;
        }

        let i = i.unwrap();

        result.push((
            InstructionMetadata {
                transaction_metadata: transaction_metadata.clone(),
                stack_height: 1,
                index: i as u32,
                absolute_path: vec![i as u8],
            },
            build_instruction(account_keys, compiled_instruction, &is_writable, &is_signer),
        ));

        if let Some(inner_instructions) =
            transaction_metadata.inner_instructions_list.get(i as usize)
        {
            let mut prev_height = 0;

            for inner_ix in inner_instructions {
                let mut path_stack = [0u8; MAX_INSTRUCTION_STACK_DEPTH];
                path_stack[0] = i;

                let stack_height = inner_ix.stack_height as usize;
                if stack_height > prev_height {
                    path_stack[stack_height - 1] = 0;
                } else {
                    path_stack[stack_height - 1] += 1;
                }

                result.push((
                    InstructionMetadata {
                        transaction_metadata: transaction_metadata.clone(),
                        stack_height: stack_height as u32,
                        index: i as u32,
                        absolute_path: path_stack[..stack_height].into(),
                    },
                    build_instruction(
                        account_keys,
                        &inner_ix.instruction,
                        &is_writable,
                        &is_signer,
                    ),
                ));

                prev_height = stack_height;
            }
        }
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

/// Unnests parsed instructions, producing an array of `(InstructionMetadata,
/// DecodedInstruction<T>)` tuple
///
/// This function takes a vector of `ParsedInstruction` and unnests them into a
/// vector of `(InstructionMetadata, DecodedInstruction<T>)` tuples.
/// It recursively processes nested instructions, increasing the stack height
/// for each level of nesting.
///
/// # Parameters
///
/// - `transaction_metadata`: The metadata of the transaction containing the
///   instructions.
/// - `instructions`: The vector of `ParsedInstruction` to be unnested.
/// - `stack_height`: The current stack height.
///
/// # Returns
///
/// A vector of `(InstructionMetadata, DecodedInstruction<T>)` tuples
/// representing the unnested instructions.
pub fn unnest_parsed_instructions<T: InstructionDecoderCollection>(
    transaction_metadata: Arc<TransactionMetadata>,
    instructions: Vec<ParsedInstruction<T>>,
    stack_height: u32,
    mut absolute_path: Vec<u8>,
) -> Vec<(InstructionMetadata, DecodedInstruction<T>)> {
    log::trace!(
        "unnest_parsed_instructions(instructions: {:?})",
        instructions
    );

    let mut result = Vec::new();

    for (ix_idx, parsed_instruction) in instructions.into_iter().enumerate() {
        while absolute_path.len() <= stack_height as usize {
            absolute_path.push(0);
        }

        if stack_height == 0 {
            if absolute_path.is_empty() {
                absolute_path.push(ix_idx as u8);
            } else {
                absolute_path[0] = ix_idx as u8;
            }
        } else {
            absolute_path[stack_height as usize] = ix_idx as u8;
        }

        result.push((
            InstructionMetadata {
                transaction_metadata: transaction_metadata.clone(),
                stack_height,
                index: ix_idx as u32 + 1,
                absolute_path: absolute_path.clone(),
            },
            parsed_instruction.instruction,
        ));
        result.extend(unnest_parsed_instructions(
            transaction_metadata.clone(),
            parsed_instruction.inner_instructions,
            stack_height + 1,
            absolute_path.clone(),
        ));
    }

    result
}
