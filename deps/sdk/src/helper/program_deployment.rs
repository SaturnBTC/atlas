use crate::arch_program::pubkey::Pubkey;
use crate::arch_program::system_instruction;
use crate::build_and_sign_transaction;
use crate::client::ArchError;
use crate::client::ArchRpcClient;
use crate::MAX_TX_BATCH_SIZE;
use crate::{
    types::{RuntimeTransaction, Signature, RUNTIME_TX_SIZE_LIMIT},
    Status,
};
use anyhow::Result;
use arch_program::bpf_loader::{LoaderState, BPF_LOADER_ID};
use arch_program::hash::Hash;
use arch_program::hash::HashError;
use arch_program::instruction::InstructionError;
use arch_program::loader_instruction;
use arch_program::sanitized::ArchMessage;
use bitcoin::key::Keypair;
use bitcoin::Network;
use indicatif::{ProgressBar, ProgressStyle};
use std::fs;
use tracing::debug;

use super::sign_message_bip322;

/* -------------------------------------------------------------------------- */
/*                               ERROR HANDLING                               */
/* -------------------------------------------------------------------------- */
/// Error type for program deployment operations
#[derive(Debug, thiserror::Error)]
pub enum ProgramDeployerError {
    /// Error reading or processing the ELF file
    #[error("ELF file error: {0}")]
    ElfFileError(String),
    /// Error with the program keypair file
    #[error("Keypair error: {0}")]
    KeypairError(String),
    /// Error sending UTXO to create program account
    #[error("UTXO error: {0}")]
    UtxoError(String),
    /// Error interacting with the Arch blockchain
    #[error("Arch blockchain error: {0}")]
    ArchError(#[from] ArchError),
    /// Error when verifying deployed program
    #[error("Verification error: {0}")]
    VerificationError(String),
    /// Error when deploying program chunks
    #[error("Deployment error: {0}")]
    DeploymentError(String),
    /// Error when building or sending transactions
    #[error("Transaction error: {0}")]
    TransactionError(String),
    /// Generic error that doesn't fit other categories
    #[error("Error: {0}")]
    Other(String),
    #[error("Hash error: {0}")]
    HashError(#[from] HashError),
}

impl From<std::io::Error> for ProgramDeployerError {
    fn from(err: std::io::Error) -> Self {
        ProgramDeployerError::ElfFileError(format!("I/O error: {}", err))
    }
}

impl From<anyhow::Error> for ProgramDeployerError {
    fn from(err: anyhow::Error) -> Self {
        ProgramDeployerError::TransactionError(format!("Transaction building error: {}", err))
    }
}

pub fn get_state(data: &[u8]) -> Result<&LoaderState, InstructionError> {
    unsafe {
        let data = data
            .get(0..LoaderState::program_data_offset())
            .ok_or(InstructionError::AccountDataTooSmall)?
            .try_into()
            .map_err(|_| InstructionError::AccountDataTooSmall)?;
        Ok(std::mem::transmute::<
            &[u8; LoaderState::program_data_offset()],
            &LoaderState,
        >(data))
    }
}

/// Program deployment service
pub struct ProgramDeployer {
    client: ArchRpcClient,
    network: Network,
}

impl ProgramDeployer {
    /// Create a new program deployer
    pub fn new(node_url: &str, network: Network) -> Self {
        Self {
            client: ArchRpcClient::new(node_url),
            network,
        }
    }

    /// Try to deploy a program
    pub fn try_deploy_program(
        &self,
        program_name: String,
        program_keypair: Keypair,
        authority_keypair: Keypair,
        elf_path: &String,
    ) -> Result<Pubkey, ProgramDeployerError> {
        print_title(&format!("PROGRAM DEPLOYMENT {}", program_name), 5);

        let elf = fs::read(elf_path).map_err(|e| {
            ProgramDeployerError::ElfFileError(format!("Failed to read ELF file: {}", e))
        })?;

        let program_pubkey = Pubkey::from_slice(&program_keypair.x_only_public_key().0.serialize());
        let authority_pubkey =
            Pubkey::from_slice(&authority_keypair.x_only_public_key().0.serialize());

        if let Ok(account_info_result) = self.client.read_account_info(program_pubkey) {
            println!(
                "\x1b[32m Step 1/3 Successful :\x1b[0m Account already exists, skipping account creation\x1b[0m"
            );
            if account_info_result.data.len() < LoaderState::program_data_offset() {
                println!("\x1b[33m Account is not initialized ! Redeploying \x1b[0m");
            } else if account_info_result.data[LoaderState::program_data_offset()..] == elf {
                println!("\x1b[33m Same program already deployed ! Skipping deployment. \x1b[0m");
                print_title(
                    &format!(
                        "PROGRAM DEPLOYMENT : OK Program account : {:?} !",
                        program_pubkey.0
                    ),
                    5,
                );
                return Ok(program_pubkey);
            }
            println!("\x1b[33m ELF mismatch with account content ! Redeploying \x1b[0m");
        } else {
            let recent_blockhash = self.client.get_best_finalized_block_hash()?;

            let create_account_tx = build_and_sign_transaction(
                ArchMessage::new(
                    &[system_instruction::create_account(
                        &authority_pubkey,
                        &program_pubkey,
                        arch_program::rent::minimum_rent(
                            LoaderState::program_data_offset() + elf.len(),
                        ),
                        0,
                        &BPF_LOADER_ID,
                    )],
                    Some(authority_pubkey),
                    recent_blockhash,
                ),
                vec![authority_keypair, program_keypair],
                self.network,
            )?;

            let create_account_txid = self.client.send_transaction(create_account_tx)?;
            let tx = self
                .client
                .wait_for_processed_transaction(&create_account_txid)?;

            if let Status::Failed(e) = tx.status {
                return Err(ProgramDeployerError::TransactionError(format!(
                    "Program account creation transaction failed: {}",
                    e
                )));
            }

            println!(
                "\x1b[32m Step 1/3 Successful :\x1b[0m Program account creation transaction successfully processed ! Tx Id : {}.\x1b[0m",
                create_account_txid
            );
        }

        self.deploy_program_elf(program_keypair, authority_keypair, &elf)?;

        let program_info_after_deployment = self.client.read_account_info(program_pubkey)?;

        assert!(program_info_after_deployment.data[LoaderState::program_data_offset()..] == elf);

        debug!(
            "Current Program Account {:x}: \n   Owner : {}, \n   Data length : {} Bytes,\n   Anchoring UTXO : {}, \n   Executable? : {}",
            program_pubkey, program_info_after_deployment.owner,
            program_info_after_deployment.data.len(),
            program_info_after_deployment.utxo,
            program_info_after_deployment.is_executable
        );

        println!("\x1b[32m Step 2/3 Successful :\x1b[0m Sent ELF file as transactions, and verified program account's content against local ELF file!");

        if program_info_after_deployment.is_executable {
            println!(
                "\x1b[32m Step 3/3 Successful :\x1b[0m Program account is already executable !"
            );
        } else {
            let recent_blockhash = self.client.get_best_finalized_block_hash()?;
            let executability_tx = build_and_sign_transaction(
                ArchMessage::new(
                    &[loader_instruction::deploy(program_pubkey, authority_pubkey)],
                    Some(authority_pubkey),
                    recent_blockhash,
                ),
                vec![authority_keypair],
                self.network,
            )?;

            let executability_txid = self.client.send_transaction(executability_tx)?;
            let tx = self
                .client
                .wait_for_processed_transaction(&executability_txid)?;

            if let Status::Failed(e) = tx.status {
                return Err(ProgramDeployerError::TransactionError(format!(
                    "Program account creation transaction failed: {}",
                    e
                )));
            }
            println!("\x1b[32m Step 3/3 Successful :\x1b[0m Made program account executable!");
        }

        let program_info_after_making_executable = self.client.read_account_info(program_pubkey)?;

        debug!(
            "Current Program Account {:x}: \n   Owner : {:x}, \n   Data length : {} Bytes,\n   Anchoring UTXO : {}, \n   Executable? : {}",
            program_pubkey,
            program_info_after_making_executable.owner,
            program_info_after_making_executable.data.len(),
            program_info_after_making_executable.utxo,
            program_info_after_making_executable.is_executable
        );

        assert!(program_info_after_making_executable.is_executable);

        print_title(
            &format!(
                "PROGRAM DEPLOYMENT : OK Program account : {:?} !",
                program_pubkey.0
            ),
            5,
        );

        println!("\x1b[33m\x1b[1m Program account Info :\x1b[0m");
        println!(
            "\x1b[33mAccount Pubkey : \x1b[0m {} // {}",
            hex::encode(program_pubkey.0),
            program_pubkey,
        );
        println!(
            "\x1b[33mOwner : \x1b[0m{} // {:?}",
            hex::encode(program_info_after_making_executable.owner.0),
            program_info_after_making_executable.owner.0,
        );
        println!(
            "\x1b[33m\x1b[1mIs executable : \x1b[0m{}",
            program_info_after_making_executable.is_executable
        );
        println!(
            "\x1b[33m\x1b[1mUtxo details : \x1b[0m{}",
            program_info_after_making_executable.utxo
        );
        println!(
            "\x1b[33m\x1b[1mELF Size : \x1b[0m{} Bytes",
            program_info_after_making_executable.data.len()
        );

        Ok(program_pubkey)
    }

    /// Deploy a program ELF
    pub fn deploy_program_elf(
        &self,
        program_keypair: Keypair,
        authority_keypair: Keypair,
        elf: &[u8],
    ) -> Result<(), ProgramDeployerError> {
        let program_pubkey = Pubkey::from_slice(&program_keypair.x_only_public_key().0.serialize());
        let authority_pubkey =
            Pubkey::from_slice(&authority_keypair.x_only_public_key().0.serialize());

        println!("program_pubkey in deploy_program_elf: {:?}", program_pubkey);

        let account_info = self.client.read_account_info(program_pubkey)?;

        println!(
            "account_info in deploy_program_elf: executable {:?}",
            account_info.is_executable
        );
        println!(
            "account_info in deploy_program_elf: data length {:?}",
            account_info.data.len()
        );
        println!(
            "account_info in deploy_program_elf: utxo {:?}",
            account_info.utxo
        );
        println!(
            "account_info in deploy_program_elf: owner {:?}",
            account_info.owner
        );

        if account_info.is_executable {
            let recent_blockhash = self.client.get_best_finalized_block_hash()?;
            let retract_tx = build_and_sign_transaction(
                ArchMessage::new(
                    &[loader_instruction::retract(
                        program_pubkey,
                        authority_pubkey,
                    )],
                    Some(authority_pubkey),
                    recent_blockhash,
                ),
                vec![authority_keypair],
                self.network,
            )?;

            let retract_txid = self.client.send_transaction(retract_tx)?;
            let _processed_tx = self.client.wait_for_processed_transaction(&retract_txid)?;
        }

        if account_info.data.len() != LoaderState::program_data_offset() + elf.len() {
            println!("Truncating program account to size of ELF file");
            let recent_blockhash = self.client.get_best_finalized_block_hash()?;
            let truncate_tx = build_and_sign_transaction(
                ArchMessage::new(
                    &[loader_instruction::truncate(
                        program_pubkey,
                        authority_pubkey,
                        elf.len() as u32,
                    )],
                    Some(authority_pubkey),
                    recent_blockhash,
                ),
                vec![program_keypair, authority_keypair],
                self.network,
            )?;

            let truncate_txid = self.client.send_transaction(truncate_tx)?;
            let _processed_tx = self.client.wait_for_processed_transaction(&truncate_txid)?;
        }

        let txs = elf
            .chunks(extend_bytes_max_len())
            .enumerate()
            .map(|(i, chunk)| {
                let offset: u32 = (i * extend_bytes_max_len()) as u32;

                let recent_blockhash = self.client.get_best_finalized_block_hash()?;
                let message = ArchMessage::new(
                    &[loader_instruction::write(
                        program_pubkey,
                        authority_pubkey,
                        offset,
                        chunk.to_vec(),
                    )],
                    Some(authority_pubkey),
                    recent_blockhash,
                );

                let digest_slice = message.hash();

                Ok(RuntimeTransaction {
                    version: 0,
                    signatures: vec![Signature(sign_message_bip322(
                        &authority_keypair,
                        &digest_slice,
                        self.network,
                    ))],
                    message,
                })
            })
            .collect::<Result<Vec<RuntimeTransaction>, ProgramDeployerError>>()?;

        let pb = ProgressBar::new(txs.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] Sending ELF file as transactions [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
                )
                .expect("Failed to set progress bar style")
                .progress_chars("#>-"),
        );

        pb.set_message("Successfully Processed Deployment Transactions :");

        let batch = txs
            .chunks(MAX_TX_BATCH_SIZE)
            .map(|chunk| self.client.send_transactions(chunk.to_vec()))
            .collect::<Result<Vec<Vec<Hash>>, ArchError>>()?;

        let tx_ids = batch.iter().flatten().collect::<Vec<&Hash>>();

        for tx_id in tx_ids.iter() {
            let _processed_tx = self.client.wait_for_processed_transaction(tx_id)?;
            pb.inc(1);
        }

        pb.finish_with_message("Successfully Processed Deployment Transactions");

        Ok(())
    }
}

/// Print a title with decorative formatting
fn print_title(title: &str, length: usize) {
    let dec = "=".repeat(length);
    println!("\n{} {} {}\n", dec, title, dec);
}

/// Returns the remaining space in an account's data storage
pub fn extend_bytes_max_len() -> usize {
    let message = ArchMessage::new(
        &[loader_instruction::write(
            Pubkey::system_program(),
            Pubkey::system_program(),
            0,
            vec![0_u8; 256],
        )],
        None,
        Hash::from([0; 32]),
    );

    RUNTIME_TX_SIZE_LIMIT
        - RuntimeTransaction {
            version: 0,
            signatures: vec![Signature([0_u8; 64])],
            message,
        }
        .serialize()
        .len()
}
