use crate::hash::Hash;
use crate::sanitize::{Sanitize, SanitizeError};
use crate::serde_error::{get_const_slice, get_slice, SerialisationErrors};
use crate::{compiled_keys::CompiledKeys, instruction::Instruction, pubkey::Pubkey};
use bitcode::{Decode, Encode};
use borsh::{BorshDeserialize, BorshSerialize};
#[cfg(feature = "fuzzing")]
use libfuzzer_sys::arbitrary;
use serde::{Deserialize, Serialize};
use sha256::digest;
use std::collections::HashSet;
pub const MAX_INSTRUCTION_COUNT_PER_TRANSACTION: usize = u8::MAX as usize;
pub const MAX_PUBKEYS_ALLOWED: u32 = u8::MAX as u32;
/// A sanitized message that has been checked for validity and processed to improve
/// runtime performance.
///
/// This struct wraps an `ArchMessage` and provides additional caching of account
/// permissions for more efficient runtime access.
#[derive(Debug, Clone)]
pub struct SanitizedMessage {
    /// The underlying message containing instructions, account keys, and header information
    pub message: ArchMessage,
    /// List of boolean with same length as account_keys(), each boolean value indicates if
    /// corresponding account key is writable or not.
    pub is_writable_account_cache: Vec<bool>,
}

impl SanitizedMessage {
    /// Creates a new `SanitizedMessage` by processing the provided `ArchMessage`.
    ///
    /// This constructor will initialize the writable account cache for faster permission checks.
    ///
    /// # Arguments
    ///
    /// * `message` - The `ArchMessage` to wrap and process
    ///
    /// # Returns
    ///
    /// A new `SanitizedMessage` instance
    pub fn new(message: ArchMessage) -> Self {
        let is_writable_account_cache = message
            .account_keys
            .iter()
            .enumerate()
            .map(|(i, _key)| message.is_writable_index(i))
            .collect::<Vec<_>>();
        Self {
            message,
            is_writable_account_cache,
        }
    }

    /// Checks if the account at the given index is a signer.
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the account in the account keys list
    ///
    /// # Returns
    ///
    /// `true` if the account is a signer, `false` otherwise
    pub fn is_signer(&self, index: usize) -> bool {
        self.message.is_signer(index)
    }

    /// Checks if the account at the given index is writable.
    ///
    /// This method uses the pre-computed writable account cache for efficiency.
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the account in the account keys list
    ///
    /// # Returns
    ///
    /// `true` if the account is writable, `false` otherwise
    pub fn is_writable(&self, index: usize) -> bool {
        *self.is_writable_account_cache.get(index).unwrap_or(&false)
    }

    /// Returns a reference to the instructions in the message.
    ///
    /// # Returns
    ///
    /// A reference to the vector of `SanitizedInstruction`s
    pub fn instructions(&self) -> &Vec<SanitizedInstruction> {
        &self.message.instructions
    }
}

/// A message in the Arch Network that contains instructions to be executed,
/// account keys involved in the transaction, and metadata in the header.
#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    Default,
    Hash,
    Encode,
    Decode,
)]
#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]

pub struct ArchMessage {
    /// Header containing metadata about the message
    pub header: MessageHeader,
    /// List of all account public keys used in this message
    pub account_keys: Vec<Pubkey>,
    /// The id of a recent ledger entry.
    pub recent_blockhash: Hash,
    /// List of instructions to execute
    pub instructions: Vec<SanitizedInstruction>,
}

impl Sanitize for ArchMessage {
    fn sanitize(&self) -> Result<(), SanitizeError> {
        // Check for duplicate account keys
        let mut unique_keys = HashSet::new();
        for key in &self.account_keys {
            if !unique_keys.insert(key) {
                return Err(SanitizeError::DuplicateAccount);
            }
        }

        // signing area and read-only non-signing area should not overlap
        if self.header.num_required_signatures as usize
            + self.header.num_readonly_unsigned_accounts as usize
            > self.account_keys.len()
        {
            return Err(SanitizeError::IndexOutOfBounds);
        }

        // there should be at least 1 RW fee-payer account.
        if self.header.num_readonly_signed_accounts >= self.header.num_required_signatures {
            return Err(SanitizeError::IndexOutOfBounds);
        }

        for ci in &self.instructions {
            if ci.program_id_index as usize >= self.account_keys.len() {
                return Err(SanitizeError::IndexOutOfBounds);
            }
            // A program cannot be a payer.
            if ci.program_id_index == 0 {
                return Err(SanitizeError::IndexOutOfBounds);
            }
            for ai in &ci.accounts {
                if *ai as usize >= self.account_keys.len() {
                    return Err(SanitizeError::IndexOutOfBounds);
                }
            }
        }
        Ok(())
    }
}

impl ArchMessage {
    /// Returns true if the account at the specified index was requested to be
    /// writable. This method should not be used directly.
    ///
    /// # Arguments
    ///
    /// * `i` - The index of the account to check
    ///
    /// # Returns
    ///
    /// `true` if the account is writable, `false` otherwise
    pub fn is_writable_index(&self, i: usize) -> bool {
        i < (self.header.num_required_signatures - self.header.num_readonly_signed_accounts)
            as usize
            || (i >= self.header.num_required_signatures as usize
                && i < self.account_keys.len()
                    - self.header.num_readonly_unsigned_accounts as usize)
    }

    /// Returns a reference to the message header.
    ///
    /// # Returns
    ///
    /// A reference to the `MessageHeader`
    pub fn header(&self) -> &MessageHeader {
        &self.header
    }

    /// Checks if the account at the given index is a signer.
    ///
    /// An account is a signer if its index is less than the number of required signatures
    /// specified in the message header.
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the account in the account keys list
    ///
    /// # Returns
    ///
    /// `true` if the account is a signer, `false` otherwise
    pub fn is_signer(&self, index: usize) -> bool {
        index < usize::from(self.header().num_required_signatures)
    }

    pub fn get_account_key(&self, index: usize) -> Option<&Pubkey> {
        self.account_keys.get(index)
    }
    /// Collects all unique Pubkeys used in instructions, including program_ids and account references
    pub fn get_unique_instruction_account_keys(&self) -> HashSet<Pubkey> {
        let mut unique_keys = HashSet::new();

        for instruction in &self.instructions {
            // Add all account references
            for account_index in &instruction.accounts {
                let pubkey = self
                    .account_keys
                    .get(*account_index as usize)
                    .expect("Account index out of bounds"); // Panic if index doesn't exist
                unique_keys.insert(*pubkey);
            }
        }

        unique_keys
    }

    pub fn get_recent_blockhash(&self) -> Hash {
        self.recent_blockhash
    }

    pub fn new(
        instructions: &[Instruction],
        payer: Option<Pubkey>,
        recent_blockhash: Hash,
    ) -> Self {
        let compiled_keys = CompiledKeys::compile(instructions, payer);
        let (header, account_keys) = compiled_keys
            .try_into_message_components()
            .expect("overflow when compiling message keys");
        let instructions = compile_instructions(instructions, &account_keys);
        Self::new_with_compiled_instructions(
            header.num_required_signatures,
            header.num_readonly_signed_accounts,
            header.num_readonly_unsigned_accounts,
            account_keys,
            recent_blockhash,
            instructions,
        )
    }

    pub fn new_with_compiled_instructions(
        num_required_signatures: u8,
        num_readonly_signed_accounts: u8,
        num_readonly_unsigned_accounts: u8,
        account_keys: Vec<Pubkey>,
        recent_blockhash: Hash,
        instructions: Vec<SanitizedInstruction>,
    ) -> Self {
        Self {
            header: MessageHeader {
                num_required_signatures,
                num_readonly_signed_accounts,
                num_readonly_unsigned_accounts,
            },
            account_keys,
            recent_blockhash,
            instructions,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        // Serialize MessageHeader
        buffer.extend_from_slice(&[
            self.header.num_required_signatures,
            self.header.num_readonly_signed_accounts,
            self.header.num_readonly_unsigned_accounts,
        ]);

        // Serialize account_keys
        buffer.extend_from_slice(&(self.account_keys.len() as u32).to_le_bytes());
        for key in &self.account_keys {
            buffer.extend_from_slice(key.as_ref());
        }

        // Serialize recent_blockhash
        buffer.extend_from_slice(&self.recent_blockhash.to_array());

        // Serialize instructions
        buffer.extend_from_slice(&(self.instructions.len() as u32).to_le_bytes());
        for instruction in &self.instructions {
            buffer.extend(instruction.serialize());
        }

        buffer
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, SerialisationErrors> {
        let mut cursor: usize = 0;

        const HEADER_SIZE: usize = 3;
        let header_bytes = get_const_slice::<HEADER_SIZE>(bytes, cursor)?;
        let header = MessageHeader {
            num_required_signatures: header_bytes[0],
            num_readonly_signed_accounts: header_bytes[1],
            num_readonly_unsigned_accounts: header_bytes[2],
        };
        cursor += HEADER_SIZE;

        // Deserialize account_keys length
        const U32_SIZE: usize = 4;
        let num_keys_bytes = get_const_slice::<U32_SIZE>(bytes, cursor)?;
        let num_keys = u32::from_le_bytes(num_keys_bytes);
        if num_keys > MAX_PUBKEYS_ALLOWED {
            return Err(SerialisationErrors::MoreThanMaxAllowedKeys);
        }
        cursor += U32_SIZE;

        // Calculate total size needed for account keys with overflow protection
        const PUBKEY_SIZE: usize = 32;
        let account_keys_size = num_keys
            .checked_mul(PUBKEY_SIZE as u32)
            .ok_or(SerialisationErrors::OverFlow)?;

        // Read all account key bytes at once
        let account_keys_slice = get_slice(bytes, cursor, account_keys_size as usize)?;
        cursor += account_keys_size as usize;

        // Process account keys using safe iteration - no manual indexing
        let mut account_keys = Vec::with_capacity(num_keys as usize);
        for key_bytes in account_keys_slice.chunks_exact(PUBKEY_SIZE) {
            account_keys.push(Pubkey::from_slice(key_bytes));
        }

        // Deserialize recent blockhash
        let blockhash_bytes = get_const_slice::<PUBKEY_SIZE>(bytes, cursor)?;
        let recent_blockhash = Hash::from(blockhash_bytes);
        cursor += PUBKEY_SIZE;

        // Deserialize instructions length
        let num_instructions_bytes = get_const_slice::<U32_SIZE>(bytes, cursor)?;
        let num_instructions = u32::from_le_bytes(num_instructions_bytes);
        cursor += U32_SIZE;

        // Validate instruction count
        if num_instructions as usize > MAX_INSTRUCTION_COUNT_PER_TRANSACTION {
            return Err(SerialisationErrors::MoreThanMaxInstructionsAllowed);
        }

        // Deserialize instructions - using safe remaining slice access
        let mut instructions = Vec::with_capacity(num_instructions as usize);
        for _ in 0..num_instructions {
            // Get remaining bytes safely without direct slicing
            let remaining_bytes = get_slice(bytes, cursor, bytes.len() - cursor)?;
            let (instruction, bytes_read) = SanitizedInstruction::deserialize(remaining_bytes)?;
            instructions.push(instruction);
            cursor += bytes_read;
        }

        Ok(Self {
            header,
            account_keys,
            recent_blockhash,
            instructions,
        })
    }

    pub fn hash(&self) -> Vec<u8> {
        let serialized_message = self.serialize();
        let first_hash = digest(serialized_message);
        digest(first_hash.as_bytes()).as_bytes().to_vec()
    }

    /// Program instructions iterator which includes each instruction's program
    /// id.
    pub fn program_instructions_iter(
        &self,
    ) -> impl Iterator<Item = (&Pubkey, &SanitizedInstruction)> {
        self.instructions.iter().map(|ix| {
            (
                self.account_keys
                    .get(usize::from(ix.program_id_index))
                    .expect("program id index is sanitized"),
                ix,
            )
        })
    }
}

fn position(keys: &[Pubkey], key: &Pubkey) -> u8 {
    keys.iter().position(|k| k == key).unwrap() as u8
}

fn compile_instruction(ix: &Instruction, keys: &[Pubkey]) -> SanitizedInstruction {
    let accounts: Vec<_> = ix
        .accounts
        .iter()
        .map(|account_meta| position(keys, &account_meta.pubkey))
        .collect();

    SanitizedInstruction {
        program_id_index: position(keys, &ix.program_id),
        data: ix.data.clone(),
        accounts,
    }
}

fn compile_instructions(ixs: &[Instruction], keys: &[Pubkey]) -> Vec<SanitizedInstruction> {
    ixs.iter().map(|ix| compile_instruction(ix, keys)).collect()
}

/// A sanitized instruction included in an `ArchMessage`.
///
/// This struct contains information about a single instruction including
/// the program to execute, the accounts to operate on, and the instruction data.
#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    Hash,
    Encode,
    Decode,
)]
#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]

pub struct SanitizedInstruction {
    /// The public key of the program that will process this instruction
    pub program_id_index: u8,
    /// Ordered indices into the message's account keys, indicating which accounts
    /// this instruction will operate on
    pub accounts: Vec<u8>,
    /// The program-specific instruction data
    pub data: Vec<u8>,
}

/// The header of an `ArchMessage` that contains metadata about the message
/// and its authorization requirements.
#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    Default,
    Hash,
    Encode,
    Decode,
)]
#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]

pub struct MessageHeader {
    /// The number of signatures required for this message to be considered
    /// valid
    pub num_required_signatures: u8,

    /// The last `num_readonly_signed_accounts` of the signed keys are read-only
    /// accounts.
    pub num_readonly_signed_accounts: u8,

    /// The last `num_readonly_unsigned_accounts` of the unsigned keys are
    /// read-only accounts.
    pub num_readonly_unsigned_accounts: u8,
}

impl SanitizedInstruction {
    pub fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        // Write program_id_index
        buffer.push(self.program_id_index);

        // Write accounts (now using u8 instead of u16)
        buffer.extend_from_slice(&(self.accounts.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&self.accounts); // Direct extend since accounts are now u8

        // Write data
        buffer.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&self.data);

        buffer
    }

    pub fn deserialize(bytes: &[u8]) -> Result<(Self, usize), SerialisationErrors> {
        if bytes.is_empty() {
            return Err(SerialisationErrors::SizeTooSmall);
        }

        let mut cursor: usize = 0;
        const PROGRAM_ID_SIZE: usize = 1;
        let program_id_bytes = get_const_slice::<PROGRAM_ID_SIZE>(bytes, cursor)?;
        let program_id_index = program_id_bytes[0];
        cursor += PROGRAM_ID_SIZE;

        const U32_SIZE: usize = 4;
        let num_accounts_bytes = get_const_slice::<U32_SIZE>(bytes, cursor)?;
        let num_accounts = u32::from_le_bytes(num_accounts_bytes);
        cursor += U32_SIZE;

        let accounts_slice = get_slice(bytes, cursor, num_accounts as usize)?;
        let accounts = accounts_slice.to_vec();
        cursor += num_accounts as usize;

        let data_len_bytes = get_const_slice::<U32_SIZE>(bytes, cursor)?;
        let data_len = u32::from_le_bytes(data_len_bytes);
        cursor += U32_SIZE;

        let data_slice = get_slice(bytes, cursor, data_len as usize)?;
        let data = data_slice.to_vec();
        cursor += data_len as usize;

        Ok((
            Self {
                program_id_index,
                accounts,
                data,
            },
            cursor, // Return total bytes consumed
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::account::AccountMeta;

    use super::*;

    #[test]
    fn test_message_with_mixed_signer_privileges() {
        let signer_pubkey = Pubkey::new_unique();
        let account_1 = Pubkey::new_unique();
        let account_2 = Pubkey::new_unique();

        // Instruction 1: signer_pubkey is a signer
        let instruction_1 = Instruction {
            program_id: Pubkey::new_unique(),
            accounts: vec![
                AccountMeta::new(account_1, false),
                AccountMeta::new(signer_pubkey, true), // signer here
            ],
            data: vec![],
        };

        // Instruction 2: signer_pubkey is not a signer
        let instruction_2 = Instruction {
            program_id: Pubkey::new_unique(),
            accounts: vec![
                AccountMeta::new(account_2, false),
                AccountMeta::new(signer_pubkey, false), // not a signer here
            ],
            data: vec![],
        };

        // Create ArchMessage from instructions
        let message = ArchMessage::new(&[instruction_1, instruction_2], None, Hash::from([0; 32]));

        // Print message details
        println!("Message Header:");
        println!(
            "  Required signatures: {}",
            message.header.num_required_signatures
        );
        println!(
            "  Readonly signed accounts: {}",
            message.header.num_readonly_signed_accounts
        );
        println!(
            "  Readonly unsigned accounts: {}",
            message.header.num_readonly_unsigned_accounts
        );

        println!("\nAccount Keys:");
        for (i, key) in message.account_keys.iter().enumerate() {
            println!(
                "  {}: {} (writable: {})",
                i,
                key,
                message.is_writable_index(i)
            );
        }

        println!("\nInstructions:");
        for (i, instruction) in message.instructions.iter().enumerate() {
            println!("  Instruction {}:", i);
            println!("    Program ID Index: {}", instruction.program_id_index);
            println!("    Account Indices: {:?}", instruction.accounts);
            println!("    Data: {:?}", instruction.data);
        }

        // Add some assertions to verify the message structure
        assert!(
            message.account_keys.contains(&signer_pubkey),
            "Signer pubkey should be in account keys"
        );
        assert!(
            message.account_keys.contains(&account_1),
            "Account 1 should be in account keys"
        );
        assert!(
            message.account_keys.contains(&account_2),
            "Account 2 should be in account keys"
        );
    }

    #[test]
    fn test_message_serialization_deserialization() {
        // Create a sample message
        let header = MessageHeader {
            num_required_signatures: 2,
            num_readonly_signed_accounts: 1,
            num_readonly_unsigned_accounts: 1,
        };

        let account_keys = vec![
            Pubkey::new_unique(), // signer 1
            Pubkey::new_unique(), // signer 2
            Pubkey::new_unique(), // non-signer program
            Pubkey::new_unique(), // non-signer data account
        ];

        let instruction1 = SanitizedInstruction {
            program_id_index: 2,     // Using the third account as program
            accounts: vec![0, 1, 3], // Using signers and data account
            data: vec![1, 2, 3, 4],  // Some instruction data
        };

        let instruction2 = SanitizedInstruction {
            program_id_index: 2,
            accounts: vec![1, 3],
            data: vec![5, 6, 7],
        };

        let original_message = ArchMessage {
            header,
            account_keys,
            recent_blockhash: Hash::from([0; 32]),
            instructions: vec![instruction1, instruction2],
        };

        // Serialize the message
        let serialized = original_message.serialize();

        // Deserialize back
        let deserialized_message =
            ArchMessage::deserialize(&serialized).expect("Failed to deserialize message");

        // Verify all fields match
        assert_eq!(
            deserialized_message.header.num_required_signatures,
            original_message.header.num_required_signatures
        );
        assert_eq!(
            deserialized_message.header.num_readonly_signed_accounts,
            original_message.header.num_readonly_signed_accounts
        );
        assert_eq!(
            deserialized_message.header.num_readonly_unsigned_accounts,
            original_message.header.num_readonly_unsigned_accounts
        );

        // Check account keys
        assert_eq!(
            deserialized_message.account_keys.len(),
            original_message.account_keys.len()
        );
        for (original, deserialized) in original_message
            .account_keys
            .iter()
            .zip(deserialized_message.account_keys.iter())
        {
            assert_eq!(original, deserialized);
        }

        // Check instructions
        assert_eq!(
            deserialized_message.instructions.len(),
            original_message.instructions.len()
        );
        for (original, deserialized) in original_message
            .instructions
            .iter()
            .zip(deserialized_message.instructions.iter())
        {
            assert_eq!(original.program_id_index, deserialized.program_id_index);
            assert_eq!(original.accounts, deserialized.accounts);
            assert_eq!(original.data, deserialized.data);
        }
    }

    #[test]
    fn test_instruction_deserialization_error_cases() {
        // Test empty buffer
        let result = SanitizedInstruction::deserialize(&[]);
        assert!(result.is_err());

        assert_eq!(
            result.unwrap_err().to_string(),
            "Data is not large enough for the requested operation"
        );

        // Test buffer too small for accounts length
        let result = SanitizedInstruction::deserialize(&[0]);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Data corrupted, unable to decode"
        );

        // Test impossibly large accounts length
        let invalid_instruction = vec![
            0, // program_id_index
            255, 255, 255, 255, // impossibly large number of accounts
        ];
        let result = SanitizedInstruction::deserialize(&invalid_instruction);
        assert!(result.is_err());

        // Test truncated account indices
        let truncated_accounts = vec![
            0, // program_id_index
            2, 0, 0, 0, // 2 accounts
            0, // first account index
               // missing second account index
        ];
        let result = SanitizedInstruction::deserialize(&truncated_accounts);
        assert!(result.is_err());

        // Test invalid data length
        let invalid_data = vec![
            0, // program_id_index
            1, 0, 0, 0, // 1 account
            0, // account index
            255, 255, 255, 255, // impossibly large data length
        ];
        let result = SanitizedInstruction::deserialize(&invalid_data);
        assert!(result.is_err());

        // Test truncated data
        let truncated_data = vec![
            0, // program_id_index
            1, 0, 0, 0, // 1 account
            0, // account index
            2, 0, 0, 0, // data length of 2
            1, // only 1 byte of data instead of 2
        ];
        let result = SanitizedInstruction::deserialize(&truncated_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_account_privileges_and_ordering() {
        let header = MessageHeader {
            num_required_signatures: 3,        // Total number of signers
            num_readonly_signed_accounts: 1,   // Last 1 signer is read-only
            num_readonly_unsigned_accounts: 2, // Last 2 non-signers are read-only
        };

        // Create accounts with different privileges
        let account_keys = vec![
            Pubkey::new_unique(), // Account 0: Writable and Signer
            Pubkey::new_unique(), // Account 1: Writable and Signer
            Pubkey::new_unique(), // Account 2: Read-only and Signer
            Pubkey::new_unique(), // Account 3: Writable and non-Signer
            Pubkey::new_unique(), // Account 4: Read-only and non-Signer
            Pubkey::new_unique(), // Account 5: Read-only and non-Signer
        ];

        let instruction = SanitizedInstruction {
            program_id_index: 3,           // Using a non-signer writable account as program
            accounts: vec![0, 1, 2, 4, 5], // Mix of different privilege accounts
            data: vec![1, 2, 3],
        };

        let message = ArchMessage {
            header,
            account_keys: account_keys.clone(),
            recent_blockhash: Hash::from([0; 32]),
            instructions: vec![instruction],
        };

        // Test account privileges
        assert!(
            message.is_writable_index(0),
            "First signer should be writable"
        );
        assert!(
            message.is_writable_index(1),
            "Second signer should be writable"
        );
        assert!(
            !message.is_writable_index(2),
            "Third signer should be read-only"
        );
        assert!(
            message.is_writable_index(3),
            "First non-signer should be writable"
        );
        assert!(
            !message.is_writable_index(4),
            "Second non-signer should be read-only"
        );
        assert!(
            !message.is_writable_index(5),
            "Third non-signer should be read-only"
        );

        // Test signer privileges
        assert!(message.is_signer(0), "Account 0 should be a signer");
        assert!(message.is_signer(1), "Account 1 should be a signer");
        assert!(message.is_signer(2), "Account 2 should be a signer");
        assert!(!message.is_signer(3), "Account 3 should not be a signer");
        assert!(!message.is_signer(4), "Account 4 should not be a signer");
        assert!(!message.is_signer(5), "Account 5 should not be a signer");

        // Test serialization/deserialization preserves privileges
        let serialized = message.serialize();
        let deserialized = ArchMessage::deserialize(&serialized).unwrap();

        // Verify header values are preserved
        assert_eq!(deserialized.header.num_required_signatures, 3);
        assert_eq!(deserialized.header.num_readonly_signed_accounts, 1);
        assert_eq!(deserialized.header.num_readonly_unsigned_accounts, 2);

        // Verify account ordering is preserved
        assert_eq!(deserialized.account_keys, account_keys);

        // Verify privileges are correctly interpreted in deserialized message
        for i in 0..6 {
            assert_eq!(
                message.is_writable_index(i),
                deserialized.is_writable_index(i),
                "Writable privilege mismatch for account {}",
                i
            );
            assert_eq!(
                message.is_signer(i),
                deserialized.is_signer(i),
                "Signer privilege mismatch for account {}",
                i
            );
        }
    }

    #[test]
    fn test_sanitized_message_privilege_cache() {
        let header = MessageHeader {
            num_required_signatures: 2,
            num_readonly_signed_accounts: 1,
            num_readonly_unsigned_accounts: 1,
        };

        let account_keys = vec![
            Pubkey::new_unique(), // Writable signer
            Pubkey::new_unique(), // Read-only signer
            Pubkey::new_unique(), // Writable non-signer
            Pubkey::new_unique(), // Read-only non-signer
        ];

        let message = ArchMessage {
            header,
            account_keys,
            recent_blockhash: Hash::from([0; 32]),
            instructions: vec![],
        };

        let sanitized_message = SanitizedMessage::new(message);

        // Test writable cache
        assert!(
            sanitized_message.is_writable(0),
            "First account should be writable"
        );
        assert!(
            !sanitized_message.is_writable(1),
            "Second account should be read-only"
        );
        assert!(
            sanitized_message.is_writable(2),
            "Third account should be writable"
        );
        assert!(
            !sanitized_message.is_writable(3),
            "Fourth account should be read-only"
        );

        // Test signer cache
        assert!(
            sanitized_message.is_signer(0),
            "First account should be signer"
        );
        assert!(
            sanitized_message.is_signer(1),
            "Second account should be signer"
        );
        assert!(
            !sanitized_message.is_signer(2),
            "Third account should not be signer"
        );
        assert!(
            !sanitized_message.is_signer(3),
            "Fourth account should not be signer"
        );
    }

    #[test]
    fn test_get_unique_account_keys() {
        let header = MessageHeader {
            num_required_signatures: 2,
            num_readonly_signed_accounts: 1,
            num_readonly_unsigned_accounts: 1,
        };

        let account_keys = vec![
            Pubkey::new_unique(), // 0: signer
            Pubkey::new_unique(), // 1: signer
            Pubkey::new_unique(), // 2: program
            Pubkey::new_unique(), // 3: unused account
            Pubkey::new_unique(), // 4: data account
        ];

        let instruction1 = SanitizedInstruction {
            program_id_index: 2,
            accounts: vec![0, 1, 4], // Using signers and data account
            data: vec![1, 2, 3],
        };

        let instruction2 = SanitizedInstruction {
            program_id_index: 2,  // Same program
            accounts: vec![1, 4], // Subset of accounts from instruction1
            data: vec![4, 5, 6],
        };

        let message = ArchMessage {
            header,
            account_keys: account_keys.clone(),
            recent_blockhash: Hash::from([0; 32]),
            instructions: vec![instruction1, instruction2],
        };

        let unique_keys = message.get_unique_instruction_account_keys();

        // Should contain: 2 signers +  1 data account = 3 unique keys
        assert_eq!(unique_keys.len(), 3);
        assert!(unique_keys.contains(&account_keys[0])); // signer 1
        assert!(unique_keys.contains(&account_keys[1])); // signer 2
        assert!(!unique_keys.contains(&account_keys[2])); // program
        assert!(!unique_keys.contains(&account_keys[3])); // unused account
        assert!(unique_keys.contains(&account_keys[4])); // data account
    }

    #[test]
    fn test_arch_message_serde_roundtrip_basic() {
        let header = MessageHeader {
            num_required_signatures: 2,
            num_readonly_signed_accounts: 1,
            num_readonly_unsigned_accounts: 1,
        };

        let account_keys = vec![
            Pubkey::new_unique(), // fee payer (signer, writable)
            Pubkey::new_unique(), // signer (readonly)
            Pubkey::new_unique(), // program (non-signer, readonly)
            Pubkey::new_unique(), // data account (non-signer, writable)
        ];

        let instruction = SanitizedInstruction {
            program_id_index: 2,
            accounts: vec![0, 1, 3],
            data: vec![1, 2, 3, 4, 5],
        };

        let original_message = ArchMessage {
            header,
            account_keys,
            recent_blockhash: Hash::from([0xab; 32]),
            instructions: vec![instruction],
        };

        // Serialize the message
        let serialized = original_message.serialize();

        // Deserialize it back
        let deserialized =
            ArchMessage::deserialize(&serialized).expect("Deserialization should succeed");

        // Verify complete equality
        assert_eq!(original_message, deserialized);
    }

    #[test]
    fn test_arch_message_serde_roundtrip_empty_instructions() {
        let original_message = ArchMessage {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![Pubkey::new_unique()],
            recent_blockhash: Hash::from([0xff; 32]),
            instructions: vec![], // Empty instructions
        };

        let serialized = original_message.serialize();
        let deserialized = ArchMessage::deserialize(&serialized)
            .expect("Deserialization should succeed with empty instructions");

        assert_eq!(original_message, deserialized);
    }

    #[test]
    fn test_arch_message_serde_roundtrip_max_instructions() {
        // testing at the boundary of maximum allowed instructions
        let header = MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 1,
        };

        let account_keys = vec![
            Pubkey::new_unique(), // fee payer
            Pubkey::new_unique(), // program
        ];

        // Create maximum allowed number of instructions
        let mut instructions = Vec::new();
        for i in 0..MAX_INSTRUCTION_COUNT_PER_TRANSACTION {
            instructions.push(SanitizedInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: vec![i as u8], // Different data for each instruction
            });
        }

        let original_message = ArchMessage {
            header,
            account_keys,
            recent_blockhash: Hash::from([0x42; 32]),
            instructions,
        };

        let serialized = original_message.serialize();
        let deserialized = ArchMessage::deserialize(&serialized)
            .expect("Deserialization should succeed with max instructions");

        assert_eq!(original_message, deserialized);
    }

    #[test]
    fn test_arch_message_serde_roundtrip_large_instruction_data() {
        // testing with large instruction data
        let original_message = ArchMessage {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![Pubkey::new_unique(), Pubkey::new_unique()],
            recent_blockhash: Hash::from([0x33; 32]),
            instructions: vec![SanitizedInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: vec![0xaa; 1024], // Large instruction data
            }],
        };

        let serialized = original_message.serialize();
        let deserialized = ArchMessage::deserialize(&serialized)
            .expect("Deserialization should succeed with large instruction data");

        assert_eq!(original_message, deserialized);
    }

    #[test]
    fn test_arch_message_deserialize_insufficient_data() {
        // testing resilience against truncated data
        let original_message = ArchMessage {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![Pubkey::new_unique()],
            recent_blockhash: Hash::from([0x11; 32]),
            instructions: vec![SanitizedInstruction {
                program_id_index: 0,
                accounts: vec![],
                data: vec![1, 2, 3],
            }],
        };

        let serialized = original_message.serialize();

        // Test with various truncated lengths
        for i in 0..serialized.len() {
            let truncated = &serialized[..i];
            assert!(
                ArchMessage::deserialize(truncated).is_err(),
                "Deserialization should fail for truncated data of length {}",
                i
            );
        }
    }

    #[test]
    fn test_arch_message_deserialize_too_many_instructions() {
        let mut malicious_data = Vec::new();

        // Header (3 bytes)
        malicious_data.extend_from_slice(&[1, 0, 0]); // 1 required sig, 0 readonly

        // Account keys count (4 bytes) - 1 account
        malicious_data.extend_from_slice(&1u32.to_le_bytes());

        // Account key (32 bytes)
        malicious_data.extend_from_slice(&[0x11; 32]);

        // Recent blockhash (32 bytes)
        malicious_data.extend_from_slice(&[0x22; 32]);

        // Instructions count - exceed maximum
        let excessive_count = (MAX_INSTRUCTION_COUNT_PER_TRANSACTION + 1) as u32;
        malicious_data.extend_from_slice(&excessive_count.to_le_bytes());

        let result = ArchMessage::deserialize(&malicious_data);
        assert!(result.is_err(), "Should reject excessive instruction count");
    }

    #[test]
    fn test_arch_message_deserialize_account_key_overflow() {
        let mut malicious_data = Vec::new();

        // Header
        malicious_data.extend_from_slice(&[1, 0, 0]);

        // Massive account key count that would overflow when multiplied by 32
        let overflow_count = u32::MAX;
        malicious_data.extend_from_slice(&overflow_count.to_le_bytes());

        let result = ArchMessage::deserialize(&malicious_data);
        assert!(
            result.is_err(),
            "Should reject account key count that causes overflow"
        );
    }

    #[test]
    fn test_arch_message_serde_roundtrip_multiple_account_types() {
        let original_message = ArchMessage {
            header: MessageHeader {
                num_required_signatures: 3,
                num_readonly_signed_accounts: 1,
                num_readonly_unsigned_accounts: 2,
            },
            account_keys: vec![
                Pubkey::new_unique(), // signer, writable (fee payer)
                Pubkey::new_unique(), // signer, writable
                Pubkey::new_unique(), // signer, readonly
                Pubkey::new_unique(), // non-signer, writable
                Pubkey::new_unique(), // non-signer, readonly (program)
                Pubkey::new_unique(), // non-signer, readonly
            ],
            recent_blockhash: Hash::from([0x99; 32]),
            instructions: vec![
                SanitizedInstruction {
                    program_id_index: 4,
                    accounts: vec![0, 1, 2, 3],
                    data: vec![0x01, 0x02],
                },
                SanitizedInstruction {
                    program_id_index: 5,
                    accounts: vec![1, 3],
                    data: vec![],
                },
            ],
        };

        let serialized = original_message.serialize();
        let deserialized = ArchMessage::deserialize(&serialized)
            .expect("Complex message deserialization should succeed");

        assert_eq!(original_message, deserialized);
    }
}

#[cfg(test)]
mod sanitize_tests {
    use super::*;

    // Helper function to create a basic valid message
    fn create_valid_message() -> ArchMessage {
        ArchMessage {
            header: MessageHeader {
                num_required_signatures: 2,
                num_readonly_signed_accounts: 1,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![
                Pubkey::new_unique(), // fee-payer (writable, signer)
                Pubkey::new_unique(), // readonly signer
                Pubkey::new_unique(), // program
                Pubkey::new_unique(), // writable non-signer
                Pubkey::new_unique(), // readonly non-signer
            ],
            recent_blockhash: Hash::from([0; 32]),
            instructions: vec![SanitizedInstruction {
                program_id_index: 2,
                accounts: vec![0, 1, 3, 4],
                data: vec![1, 2, 3],
            }],
        }
    }

    #[test]
    fn test_valid_message() {
        let message = create_valid_message();
        assert!(message.sanitize().is_ok());
    }

    #[test]
    fn test_overlapping_signing_and_readonly_areas() {
        let mut message = create_valid_message();
        // Set num_required_signatures + num_readonly_unsigned_accounts > account_keys.len()
        message.header.num_required_signatures = 3;
        message.header.num_readonly_unsigned_accounts = 3;

        assert_eq!(message.sanitize(), Err(SanitizeError::IndexOutOfBounds));
    }

    #[test]
    fn test_no_writable_fee_payer() {
        let mut message = create_valid_message();
        // Make all signed accounts readonly
        message.header.num_readonly_signed_accounts = message.header.num_required_signatures;

        assert_eq!(message.sanitize(), Err(SanitizeError::IndexOutOfBounds));
    }

    #[test]
    fn test_invalid_program_id_index() {
        let mut message = create_valid_message();
        message.instructions[0].program_id_index = message.account_keys.len() as u8;

        assert_eq!(message.sanitize(), Err(SanitizeError::IndexOutOfBounds));
    }

    #[test]
    fn test_program_as_payer() {
        let mut message = create_valid_message();
        message.instructions[0].program_id_index = 0;

        assert_eq!(message.sanitize(), Err(SanitizeError::IndexOutOfBounds));
    }

    #[test]
    fn test_invalid_account_index() {
        let mut message = create_valid_message();
        message.instructions[0]
            .accounts
            .push(message.account_keys.len() as u8);

        assert_eq!(message.sanitize(), Err(SanitizeError::IndexOutOfBounds));
    }

    #[test]
    fn test_complex_valid_message() {
        let message = ArchMessage {
            header: MessageHeader {
                num_required_signatures: 3,
                num_readonly_signed_accounts: 1,
                num_readonly_unsigned_accounts: 2,
            },
            account_keys: vec![
                Pubkey::new_unique(), // writable signer (fee-payer)
                Pubkey::new_unique(), // writable signer
                Pubkey::new_unique(), // readonly signer
                Pubkey::new_unique(), // program 1
                Pubkey::new_unique(), // program 2
                Pubkey::new_unique(), // writable non-signer
                Pubkey::new_unique(), // readonly non-signer
                Pubkey::new_unique(), // readonly non-signer
            ],
            recent_blockhash: Hash::from([0; 32]),
            instructions: vec![
                SanitizedInstruction {
                    program_id_index: 3,
                    accounts: vec![0, 1, 5],
                    data: vec![1, 2, 3],
                },
                SanitizedInstruction {
                    program_id_index: 4,
                    accounts: vec![1, 2, 6, 7],
                    data: vec![4, 5, 6],
                },
            ],
        };

        assert!(message.sanitize().is_ok());
    }

    #[test]
    fn test_duplicate_account_in_instr() {
        let message = ArchMessage {
            header: MessageHeader {
                num_required_signatures: 2,
                num_readonly_signed_accounts: 1,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![
                Pubkey::new_unique(), // fee-payer (writable, signer)
                Pubkey::new_unique(), // readonly signer
                Pubkey::new_unique(), // program
                Pubkey::new_unique(), // writable non-signer
                Pubkey::new_unique(), // readonly non-signer
            ],
            recent_blockhash: Hash::from([0; 32]),
            instructions: vec![SanitizedInstruction {
                program_id_index: 2,
                accounts: vec![0, 1, 3, 3],
                data: vec![1, 2, 3],
            }],
        };
        // Duplicate index are allowed in instructions
        assert!(message.sanitize().is_ok());
    }

    #[test]
    fn test_duplicate_account_in_keys_list() {
        let malicious = Pubkey::new_unique();
        let message = ArchMessage {
            header: MessageHeader {
                num_required_signatures: 2,
                num_readonly_signed_accounts: 1,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![
                Pubkey::new_unique(), // fee-payer (writable, signer)
                Pubkey::new_unique(), // readonly signer
                Pubkey::new_unique(), // program
                malicious,
                malicious,
            ],
            recent_blockhash: Hash::from([0; 32]),
            instructions: vec![SanitizedInstruction {
                program_id_index: 2,
                accounts: vec![0, 1, 3, 4],
                data: vec![1, 2, 3],
            }],
        };
        assert_eq!(
            message.sanitize().unwrap_err(),
            SanitizeError::DuplicateAccount
        );
    }

    #[test]
    fn test_sanitized_instruction_serde_roundtrip_basic() {
        let original_instruction = SanitizedInstruction {
            program_id_index: 5,
            accounts: vec![0, 1, 2, 3],
            data: vec![0xaa, 0xbb, 0xcc, 0xdd],
        };

        let serialized = original_instruction.serialize();
        let (deserialized, bytes_read) =
            SanitizedInstruction::deserialize(&serialized).expect("Deserialization should succeed");

        assert_eq!(original_instruction, deserialized);
        assert_eq!(bytes_read, serialized.len());
    }

    #[test]
    fn test_sanitized_instruction_serde_roundtrip_empty_data() {
        let original_instruction = SanitizedInstruction {
            program_id_index: 1,
            accounts: vec![0],
            data: vec![], // Empty data
        };

        let serialized = original_instruction.serialize();
        let (deserialized, bytes_read) = SanitizedInstruction::deserialize(&serialized)
            .expect("Deserialization should succeed with empty data");

        assert_eq!(original_instruction, deserialized);
        assert_eq!(bytes_read, serialized.len());
    }

    #[test]
    fn test_sanitized_instruction_serde_roundtrip_empty_accounts() {
        let original_instruction = SanitizedInstruction {
            program_id_index: 42,
            accounts: vec![], // Empty accounts
            data: vec![1, 2, 3],
        };

        let serialized = original_instruction.serialize();
        let (deserialized, bytes_read) = SanitizedInstruction::deserialize(&serialized)
            .expect("Deserialization should succeed with empty accounts");

        assert_eq!(original_instruction, deserialized);
        assert_eq!(bytes_read, serialized.len());
    }

    #[test]
    fn test_sanitized_instruction_serde_roundtrip_large_data() {
        let original_instruction = SanitizedInstruction {
            program_id_index: 255,
            accounts: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            data: vec![0x42; 2048], // Large data payload
        };

        let serialized = original_instruction.serialize();
        let (deserialized, bytes_read) = SanitizedInstruction::deserialize(&serialized)
            .expect("Deserialization should succeed with large data");

        assert_eq!(original_instruction, deserialized);
        assert_eq!(bytes_read, serialized.len());
    }

    #[test]
    fn test_sanitized_instruction_deserialize_insufficient_data() {
        let original_instruction = SanitizedInstruction {
            program_id_index: 10,
            accounts: vec![1, 2, 3],
            data: vec![0xaa, 0xbb, 0xcc],
        };

        let serialized = original_instruction.serialize();

        // Test with various truncated lengths
        for i in 0..serialized.len() {
            let truncated = &serialized[..i];
            assert!(
                SanitizedInstruction::deserialize(truncated).is_err(),
                "Deserialization should fail for truncated instruction data of length {}",
                i
            );
        }
    }

    #[test]
    fn test_sanitized_instruction_deserialize_empty_buffer() {
        let result = SanitizedInstruction::deserialize(&[]);
        assert!(result.is_err(), "Should fail with empty buffer");
    }

    #[test]
    fn test_sanitized_instruction_deserialize_malformed_lengths() {
        let mut malicious_data = Vec::new();

        // Program ID
        malicious_data.push(1);

        malicious_data.extend_from_slice(&1000u32.to_le_bytes());

        malicious_data.extend_from_slice(&[1, 2, 3]); // Only 3 bytes instead of 1000

        let result = SanitizedInstruction::deserialize(&malicious_data);
        assert!(
            result.is_err(),
            "Should fail with insufficient account data"
        );
    }

    #[test]
    fn test_sanitized_instruction_deserialize_data_length_mismatch() {
        let mut malicious_data = Vec::new();

        // Program ID
        malicious_data.push(5);

        // Accounts length (0 accounts)
        malicious_data.extend_from_slice(&0u32.to_le_bytes());

        // Data length - claim we have large data
        malicious_data.extend_from_slice(&1000u32.to_le_bytes());

        // But provide insufficient data
        malicious_data.extend_from_slice(&[0xaa, 0xbb]); // Only 2 bytes instead of 1000

        let result = SanitizedInstruction::deserialize(&malicious_data);
        assert!(
            result.is_err(),
            "Should fail with insufficient instruction data"
        );
    }

    #[test]
    fn test_sanitized_instruction_bytes_consumed_accuracy() {
        let instructions = vec![
            SanitizedInstruction {
                program_id_index: 1,
                accounts: vec![],
                data: vec![],
            },
            SanitizedInstruction {
                program_id_index: 2,
                accounts: vec![0, 1, 2],
                data: vec![0xaa],
            },
            SanitizedInstruction {
                program_id_index: 3,
                accounts: vec![0],
                data: vec![0xbb; 100],
            },
        ];

        for instruction in instructions {
            let serialized = instruction.serialize();
            let (_, bytes_read) = SanitizedInstruction::deserialize(&serialized)
                .expect("Deserialization should succeed");

            assert_eq!(
                bytes_read,
                serialized.len(),
                "Bytes read should exactly match serialized length"
            );
        }
    }
}
