use std::{array::TryFromSliceError, string::FromUtf8Error};

use anyhow::Result;
use arch_program::hash::Hash;
use bitcode::{Decode, Encode};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::RUNTIME_TX_SIZE_LIMIT;

use super::{RuntimeTransaction, RuntimeTransactionError};

#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum ParseProcessedTransactionError {
    #[error("from hex error: {0}")]
    FromHexError(#[from] hex::FromHexError),

    #[error("from utf8 error: {0}")]
    FromUtf8Error(#[from] FromUtf8Error),

    #[error("try from slice error")]
    TryFromSliceError,

    #[error("buffer too short for deserialization")]
    BufferTooShort,

    #[error("runtime transaction error: {0}")]
    RuntimeTransactionError(#[from] RuntimeTransactionError),

    #[error("rollback message too long")]
    RollbackMessageTooLong,

    #[error("runtime transaction size exceeds limit: {0} > {1}")]
    RuntimeTransactionSizeExceedsLimit(usize, usize),

    #[error("log message too long")]
    LogMessageTooLong,

    #[error("log messages too long")]
    TooManyLogMessages,

    #[error("status failed message too long")]
    StatusFailedMessageTooLong,
}

impl From<TryFromSliceError> for ParseProcessedTransactionError {
    fn from(_e: TryFromSliceError) -> Self {
        ParseProcessedTransactionError::TryFromSliceError
    }
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Serialize,
    BorshDeserialize,
    BorshSerialize,
    PartialEq,
    Encode,
    Decode,
    Eq,
)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type", content = "message")]
pub enum Status {
    Queued,
    Processed,
    Failed(String),
}

impl Status {
    pub fn from_value(value: &Value) -> Option<Self> {
        if let Some(status_str) = value.as_str() {
            match status_str {
                "Queued" => return Some(Status::Queued),
                _ => return Some(Status::Processed),
            }
        } else if let Some(obj) = value.as_object() {
            if let Some(failed_message) = obj.get("Failed").and_then(|v| v.as_str()) {
                return Some(Status::Failed(failed_message.to_string()));
            } else {
                return None;
            }
        }
        None
    }
}

#[derive(
    Clone,
    PartialEq,
    Debug,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    Encode,
    Decode,
    Eq,
)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type", content = "message")]
pub enum RollbackStatus {
    Rolledback(String),
    NotRolledback,
}

impl RollbackStatus {
    pub fn to_fixed_array(
        &self,
    ) -> Result<[u8; ROLLBACK_MESSAGE_BUFFER_SIZE], ParseProcessedTransactionError> {
        let mut buffer = [0; ROLLBACK_MESSAGE_BUFFER_SIZE];

        if let RollbackStatus::Rolledback(msg) = self {
            buffer[0] = 1;
            let message_bytes = msg.as_bytes();
            buffer[1..9].copy_from_slice(&(msg.len() as u64).to_le_bytes());

            if message_bytes.len() > ROLLBACK_MESSAGE_BUFFER_SIZE - 9 {
                return Err(ParseProcessedTransactionError::RollbackMessageTooLong);
            }
            buffer[9..(9 + message_bytes.len())].copy_from_slice(message_bytes);
        }

        Ok(buffer)
    }

    pub fn from_fixed_array(
        data: &[u8; ROLLBACK_MESSAGE_BUFFER_SIZE],
    ) -> Result<Self, ParseProcessedTransactionError> {
        if data[0] == 1 {
            let msg_len = u64::from_le_bytes(
                data[1..9]
                    .try_into()
                    .map_err(|_| ParseProcessedTransactionError::TryFromSliceError)?,
            ) as usize;
            // Check that msg_len doesn't exceed the available space in the fixed buffer
            if 9 + msg_len > ROLLBACK_MESSAGE_BUFFER_SIZE {
                return Err(ParseProcessedTransactionError::BufferTooShort);
            }
            let msg = String::from_utf8(data[9..(9 + msg_len)].to_vec())
                .map_err(ParseProcessedTransactionError::FromUtf8Error)?;
            Ok(RollbackStatus::Rolledback(msg))
        } else {
            Ok(RollbackStatus::NotRolledback)
        }
    }
}

#[derive(
    Clone,
    PartialEq,
    Debug,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    Encode,
    Decode,
    Eq,
)]
pub struct ProcessedTransaction {
    pub runtime_transaction: RuntimeTransaction,
    pub status: Status,
    pub bitcoin_txid: Option<Hash>,
    pub logs: Vec<String>,
    pub rollback_status: RollbackStatus,
}

const ROLLBACK_MESSAGE_BUFFER_SIZE: usize = 1033;
const LOG_MESSAGES_BYTES_LIMIT: usize = 255;
const MAX_LOG_MESSAGES: usize = 100;
const MAX_STATUS_FAILED_MESSAGE_SIZE: usize = 1000;

impl ProcessedTransaction {
    pub fn max_serialized_size() -> usize {
        ROLLBACK_MESSAGE_BUFFER_SIZE  // rollback status (fixed size buffer)
            + 8  // runtime_transaction length field
            + RUNTIME_TX_SIZE_LIMIT  // max runtime transaction size
            + 1  // bitcoin_txid variant flag (None/Some)
            + 32  // bitcoin_txid hash (when Some)
            + 8  // logs length field
            + MAX_LOG_MESSAGES * (8 + LOG_MESSAGES_BYTES_LIMIT)  // max logs data
            + 1  // status variant flag (Queued/Processed/Failed)
            + 8  // error message length field (for Failed status)
            + MAX_STATUS_FAILED_MESSAGE_SIZE // reasonable max error message size
    }

    pub fn txid(&self) -> Hash {
        self.runtime_transaction.txid()
    }

    pub fn to_vec(&self) -> Result<Vec<u8>, ParseProcessedTransactionError> {
        let mut serialized = vec![];

        serialized.extend(self.rollback_status.to_fixed_array()?);

        let serialized_runtime_transaction = self.runtime_transaction.serialize();
        if serialized_runtime_transaction.len() > RUNTIME_TX_SIZE_LIMIT {
            return Err(
                ParseProcessedTransactionError::RuntimeTransactionSizeExceedsLimit(
                    serialized_runtime_transaction.len(),
                    RUNTIME_TX_SIZE_LIMIT,
                ),
            );
        }
        serialized.extend((serialized_runtime_transaction.len() as u64).to_le_bytes());
        serialized.extend(serialized_runtime_transaction);

        serialized.extend(match &self.bitcoin_txid {
            Some(txid) => {
                let mut res = vec![1];
                res.extend(txid.to_array());
                res
            }
            None => vec![0],
        });

        if self.logs.len() > MAX_LOG_MESSAGES {
            return Err(ParseProcessedTransactionError::TooManyLogMessages);
        }

        serialized.extend((self.logs.len() as u64).to_le_bytes());
        for log in &self.logs {
            let log_len = std::cmp::min(log.len(), LOG_MESSAGES_BYTES_LIMIT);
            serialized.extend((log_len as u64).to_le_bytes());
            serialized.extend(log.as_bytes()[..log_len].to_vec());
        }

        serialized.extend(match &self.status {
            Status::Queued => vec![0_u8],
            Status::Processed => vec![1_u8],
            Status::Failed(err) => {
                let mut result = vec![2_u8];
                if err.len() > 1000 {
                    return Err(ParseProcessedTransactionError::StatusFailedMessageTooLong);
                }
                result.extend((err.len() as u64).to_le_bytes());
                result.extend(err.as_bytes());
                result
            }
        });
        Ok(serialized)
    }

    pub fn from_vec(data: &[u8]) -> Result<Self, ParseProcessedTransactionError> {
        let mut size = 0;

        let rollback_buffer: [u8; ROLLBACK_MESSAGE_BUFFER_SIZE] = data
            [size..(size + ROLLBACK_MESSAGE_BUFFER_SIZE)]
            .try_into()
            .map_err(|_| ParseProcessedTransactionError::TryFromSliceError)?;
        let rollback_status = RollbackStatus::from_fixed_array(&rollback_buffer)?;

        size += ROLLBACK_MESSAGE_BUFFER_SIZE;
        let data_bytes = data[size..(size + 8)].try_into()?;
        let runtime_transaction_len = u64::from_le_bytes(data_bytes) as usize;
        size += 8;
        let runtime_transaction =
            RuntimeTransaction::from_slice(&data[size..(size + runtime_transaction_len)])?;
        size += runtime_transaction_len;

        let bitcoin_txid = if data[size] == 1 {
            size += 1;
            let bytes: [u8; 32] = data[(size)..(size + 32)].try_into()?;
            let res = Some(Hash::from(bytes));
            size += 32;
            res
        } else {
            size += 1;
            None
        };

        let data_bytes = data[size..(size + 8)].try_into()?;
        let logs_len = u64::from_le_bytes(data_bytes) as usize;
        if logs_len > MAX_LOG_MESSAGES {
            return Err(ParseProcessedTransactionError::TooManyLogMessages);
        }

        size += 8;
        let mut logs = vec![];
        for _ in 0..logs_len {
            let log_len = u64::from_le_bytes(data[size..(size + 8)].try_into()?) as usize;
            size += 8;
            if log_len > LOG_MESSAGES_BYTES_LIMIT {
                return Err(ParseProcessedTransactionError::LogMessageTooLong);
            }
            logs.push(String::from_utf8(data[size..(size + log_len)].to_vec())?);
            size += log_len;
        }

        let status = match data[size] {
            0 => Status::Queued,
            1 => Status::Processed,
            2 => {
                let data_bytes = data[(size + 1)..(size + 9)].try_into()?;
                let error_len = u64::from_le_bytes(data_bytes) as usize;
                if error_len > 1000 {
                    return Err(ParseProcessedTransactionError::StatusFailedMessageTooLong);
                }
                size += 9;
                let error = String::from_utf8(data[size..(size + error_len)].to_vec())?;
                Status::Failed(error)
            }
            _ => unreachable!("status doesn't exist"),
        };

        Ok(ProcessedTransaction {
            runtime_transaction,
            status,
            bitcoin_txid,
            logs,
            rollback_status,
        })
    }

    pub fn compute_units_consumed(&self) -> Option<&str> {
        self.logs[self.logs.len() - 2].get(82..86)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::ParseProcessedTransactionError;
    use crate::Signature;
    use arch_program::pubkey::Pubkey;
    use arch_program::sanitized::SanitizedInstruction;
    // use crate::processed_transaction::ProcessedTransaction;
    // use crate::processed_transaction::RollbackStatus;
    // use crate::processed_transaction::Status;
    // use crate::runtime_transaction::RuntimeTransaction;
    // use crate::signature::Signature;
    // use arch_program::instruction::Instruction;
    // use arch_program::message::Message;
    // use arch_program::pubkey::Pubkey;
    // use arch_program::sanitized::ArchMessage;
    // use arch_program::sanitized::MessageHeader;
    // use proptest::prelude::*;

    // use proptest::strategy::Just;
    //TODO: fix this to work with new ArchMessage

    //     proptest! {
    //         #[test]
    //         fn fuzz_serialize_deserialize_processed_transaction(
    //             version in any::<u32>(),
    //             signatures in prop::collection::vec(prop::collection::vec(any::<u8>(), 64), 0..10),
    //             signers in prop::collection::vec(any::<[u8; 32]>(), 0..10),
    //             instructions in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..100), 0..10),
    //             bitcoin_txid in "[0-9a-f]{64}",
    //             accounts_tags in prop::collection::vec("[0-9a-f]{64}", 0..10)
    //         ) {
    //             // Generate a random RuntimeTransaction
    //             let signatures: Vec<Signature> = signatures.into_iter()
    //                 .map(|sig_bytes| Signature::from_slice(&sig_bytes))
    //                 .collect();

    //             let signers: Vec<Pubkey> = signers.into_iter()
    //                 .map(Pubkey::from)
    //                 .collect();

    //             let instructions: Vec<Instruction> = instructions.into_iter()
    //                 .map(|data| Instruction {
    //                     program_id: Pubkey::system_program(),
    //                     accounts: vec![],
    //                     data,
    //                 })
    //                 .collect();

    //             // Create ArchMessage instead of Message
    //             let message = ArchMessage

    //             let runtime_transaction = RuntimeTransaction {
    //                 version,
    //                 signatures,
    //                 message,
    //             };

    //             let processed_transaction = ProcessedTransaction {
    //                 runtime_transaction,
    //                 status: Status::Queued,
    //                 bitcoin_txid: Some(bitcoin_txid.to_string()),
    //                 accounts_tags: accounts_tags.iter().map(|s| s.to_string()).collect(),
    //                 logs: vec![],
    //                 rollback_status: false,
    //             };

    //             let serialized = processed_transaction.to_vec().unwrap();
    //             let deserialized = ProcessedTransaction::from_vec(&serialized).unwrap();

    //             let reserialized = deserialized.to_vec().unwrap();
    //             assert_eq!(serialized, reserialized);
    //         }
    //     }

    use arch_program::hash::Hash;
    use arch_program::sanitized::{ArchMessage, MessageHeader};

    use crate::{
        types::processed_transaction::ROLLBACK_MESSAGE_BUFFER_SIZE, RollbackStatus, Status,
    };

    use super::ProcessedTransaction;

    #[test]
    fn test_rollback_with_message() {
        let rollback_message = "a".repeat(ROLLBACK_MESSAGE_BUFFER_SIZE - 10);
        let processed_transaction = ProcessedTransaction {
            runtime_transaction: crate::RuntimeTransaction {
                version: 1,
                signatures: vec![],
                message: ArchMessage {
                    header: MessageHeader {
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                        num_required_signatures: 0,
                    },
                    account_keys: vec![],
                    instructions: vec![],
                    recent_blockhash: Hash::from_str(
                        "0000000000000000000000000000000000000000000000000000000000000000",
                    )
                    .unwrap(),
                },
            },
            status: Status::Processed,
            bitcoin_txid: None,
            logs: vec![],
            rollback_status: RollbackStatus::Rolledback(rollback_message),
        };

        let serialized = processed_transaction.to_vec().unwrap();
        let deserialized = ProcessedTransaction::from_vec(&serialized).unwrap();
        assert_eq!(processed_transaction, deserialized);
    }

    #[test]
    fn test_rollback_with_message_too_long() {
        let rollback_message = "a".repeat(ROLLBACK_MESSAGE_BUFFER_SIZE);
        let processed_transaction = ProcessedTransaction {
            runtime_transaction: crate::RuntimeTransaction {
                version: 1,
                signatures: vec![],
                message: ArchMessage {
                    header: MessageHeader {
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                        num_required_signatures: 0,
                    },
                    account_keys: vec![],
                    instructions: vec![],
                    recent_blockhash: Hash::from_str(
                        "0000000000000000000000000000000000000000000000000000000000000000",
                    )
                    .unwrap(),
                },
            },
            status: Status::Processed,
            bitcoin_txid: None,
            logs: vec![],
            rollback_status: RollbackStatus::Rolledback(rollback_message),
        };

        let serialized = processed_transaction.to_vec();
        assert!(serialized.is_err());
    }

    #[test]
    fn test_serialization_not_rolledback() {
        let processed_transaction = ProcessedTransaction {
            runtime_transaction: crate::RuntimeTransaction {
                version: 1,
                signatures: vec![],
                message: ArchMessage {
                    header: MessageHeader {
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                        num_required_signatures: 0,
                    },
                    account_keys: vec![],
                    instructions: vec![],
                    recent_blockhash: Hash::from_str(
                        "0000000000000000000000000000000000000000000000000000000000000000",
                    )
                    .unwrap(),
                },
            },
            status: Status::Processed,
            bitcoin_txid: None,
            logs: vec![],
            rollback_status: RollbackStatus::NotRolledback,
        };

        let serialized = processed_transaction.to_vec().unwrap();
        let deserialized = ProcessedTransaction::from_vec(&serialized).unwrap();
        assert_eq!(processed_transaction, deserialized);
    }

    #[test]
    fn rollback_default_message_size() {
        let message = "Transaction rolled back in Bitcoin";
        println!("Message size as bytes : {}", message.len());
    }

    // Tests for log validation checks
    fn create_minimal_processed_transaction() -> ProcessedTransaction {
        ProcessedTransaction {
            runtime_transaction: crate::RuntimeTransaction {
                version: 1,
                signatures: vec![],
                message: ArchMessage {
                    header: MessageHeader {
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                        num_required_signatures: 0,
                    },
                    account_keys: vec![],
                    instructions: vec![],
                    recent_blockhash: Hash::from_str(
                        "0000000000000000000000000000000000000000000000000000000000000000",
                    )
                    .unwrap(),
                },
            },
            status: Status::Processed,
            bitcoin_txid: None,
            logs: vec![],
            rollback_status: RollbackStatus::NotRolledback,
        }
    }

    #[test]
    fn test_serialization_too_many_log_messages() {
        let mut processed_transaction = create_minimal_processed_transaction();

        // Create MAX_LOG_MESSAGES + 1 log messages
        processed_transaction.logs = (0..=super::MAX_LOG_MESSAGES)
            .map(|i| format!("Log message {}", i))
            .collect();

        let result = processed_transaction.to_vec();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            super::ParseProcessedTransactionError::TooManyLogMessages
        );
    }

    #[test]
    fn test_serialization_exactly_max_log_messages() {
        let mut processed_transaction = create_minimal_processed_transaction();

        // Create exactly MAX_LOG_MESSAGES log messages
        processed_transaction.logs = (0..super::MAX_LOG_MESSAGES)
            .map(|i| format!("Log message {}", i))
            .collect();

        let result = processed_transaction.to_vec();
        assert!(result.is_ok());

        // Verify round-trip
        let serialized = result.unwrap();
        let deserialized = ProcessedTransaction::from_vec(&serialized).unwrap();
        assert_eq!(processed_transaction, deserialized);
    }

    #[test]
    fn test_deserialization_too_many_log_messages() {
        // Create a valid processed transaction first
        let mut processed_transaction = create_minimal_processed_transaction();
        processed_transaction.logs = vec!["Valid log".to_string()];

        let mut serialized = processed_transaction.to_vec().unwrap();

        // Manually corrupt the serialized data to have too many log messages
        // Find the position where logs length is stored
        let rollback_size = super::ROLLBACK_MESSAGE_BUFFER_SIZE;
        let runtime_tx_len_pos = rollback_size;
        let runtime_tx_len = u64::from_le_bytes(
            serialized[runtime_tx_len_pos..runtime_tx_len_pos + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let bitcoin_txid_pos = runtime_tx_len_pos + 8 + runtime_tx_len;
        let logs_len_pos = bitcoin_txid_pos + 1; // +1 for the bitcoin_txid flag (0 in this case)

        // Set logs_len to MAX_LOG_MESSAGES + 1
        let corrupted_logs_len = (super::MAX_LOG_MESSAGES + 1) as u64;
        serialized[logs_len_pos..logs_len_pos + 8]
            .copy_from_slice(&corrupted_logs_len.to_le_bytes());

        let result = ProcessedTransaction::from_vec(&serialized);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            super::ParseProcessedTransactionError::TooManyLogMessages
        );
    }

    #[test]
    fn test_serialization_log_message_gets_truncated() {
        let mut processed_transaction = create_minimal_processed_transaction();

        // Create a log message longer than LOG_MESSAGES_BYTES_LIMIT
        let long_message = "a".repeat(super::LOG_MESSAGES_BYTES_LIMIT + 10);
        processed_transaction.logs = vec![long_message.clone()];

        let result = processed_transaction.to_vec();
        assert!(result.is_ok());

        // Verify that the message was truncated during serialization
        let serialized = result.unwrap();
        let deserialized = ProcessedTransaction::from_vec(&serialized).unwrap();

        assert_eq!(deserialized.logs.len(), 1);
        assert_eq!(deserialized.logs[0].len(), super::LOG_MESSAGES_BYTES_LIMIT);
        assert_eq!(
            deserialized.logs[0],
            "a".repeat(super::LOG_MESSAGES_BYTES_LIMIT)
        );
    }

    #[test]
    fn test_deserialization_log_message_too_long() {
        // Create a valid processed transaction first
        let mut processed_transaction = create_minimal_processed_transaction();
        processed_transaction.logs = vec!["Valid log".to_string()];

        let mut serialized = processed_transaction.to_vec().unwrap();

        // Find the position where the first log message length is stored
        let rollback_size = super::ROLLBACK_MESSAGE_BUFFER_SIZE;
        let runtime_tx_len_pos = rollback_size;
        let runtime_tx_len = u64::from_le_bytes(
            serialized[runtime_tx_len_pos..runtime_tx_len_pos + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let bitcoin_txid_pos = runtime_tx_len_pos + 8 + runtime_tx_len;
        let logs_len_pos = bitcoin_txid_pos + 1; // +1 for the bitcoin_txid flag
        let first_log_len_pos = logs_len_pos + 8; // +8 for logs_len

        // Set the first log message length to exceed LOG_MESSAGES_BYTES_LIMIT
        let corrupted_log_len = (super::LOG_MESSAGES_BYTES_LIMIT + 1) as u64;
        serialized[first_log_len_pos..first_log_len_pos + 8]
            .copy_from_slice(&corrupted_log_len.to_le_bytes());

        let result = ProcessedTransaction::from_vec(&serialized);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            super::ParseProcessedTransactionError::LogMessageTooLong
        );
    }

    #[test]
    fn test_log_message_exactly_at_limit() {
        let mut processed_transaction = create_minimal_processed_transaction();

        // Create a log message exactly at the LOG_MESSAGES_BYTES_LIMIT
        let max_message = "a".repeat(super::LOG_MESSAGES_BYTES_LIMIT);
        processed_transaction.logs = vec![max_message.clone()];

        let result = processed_transaction.to_vec();
        assert!(result.is_ok());

        // Verify round-trip
        let serialized = result.unwrap();
        let deserialized = ProcessedTransaction::from_vec(&serialized).unwrap();
        assert_eq!(processed_transaction, deserialized);
        assert_eq!(deserialized.logs[0], max_message);
    }

    #[test]
    fn test_multiple_log_messages_within_limits() {
        let mut processed_transaction = create_minimal_processed_transaction();

        // Create multiple log messages within both limits
        processed_transaction.logs = (0..10)
            .map(|i| format!("Log message number {} with some content", i))
            .collect();

        let result = processed_transaction.to_vec();
        assert!(result.is_ok());

        // Verify round-trip
        let serialized = result.unwrap();
        let deserialized = ProcessedTransaction::from_vec(&serialized).unwrap();
        assert_eq!(processed_transaction, deserialized);
    }

    #[test]
    fn test_empty_log_messages() {
        let mut processed_transaction = create_minimal_processed_transaction();

        // Test with some empty log messages
        processed_transaction.logs = vec!["".to_string(), "Valid log".to_string(), "".to_string()];

        let result = processed_transaction.to_vec();
        assert!(result.is_ok());

        // Verify round-trip
        let serialized = result.unwrap();
        let deserialized = ProcessedTransaction::from_vec(&serialized).unwrap();
        assert_eq!(processed_transaction, deserialized);
    }

    #[test]
    fn test_no_log_messages() {
        let processed_transaction = create_minimal_processed_transaction();
        // logs is already empty in the minimal transaction

        let result = processed_transaction.to_vec();
        assert!(result.is_ok());

        // Verify round-trip
        let serialized = result.unwrap();
        let deserialized = ProcessedTransaction::from_vec(&serialized).unwrap();
        assert_eq!(processed_transaction, deserialized);
    }

    #[test]
    fn test_biggest_processed_transaction_within_max_size() {
        // Now let's create a truly maximum transaction with all fields maximized
        let runtime_transaction = crate::RuntimeTransaction {
            version: 0,
            signatures: vec![Signature::from([0xFF; 64]); 10], // Some signatures
            message: ArchMessage {
                header: MessageHeader {
                    num_required_signatures: 10,
                    num_readonly_signed_accounts: 5,
                    num_readonly_unsigned_accounts: 5,
                },
                account_keys: (0..50)
                    .map(|i| {
                        let mut bytes = [0u8; 32];
                        bytes[0] = i as u8;
                        Pubkey::from(bytes)
                    })
                    .collect(),
                instructions: vec![SanitizedInstruction {
                    program_id_index: 0,
                    accounts: (0..20).collect(),
                    data: vec![0xAA; 7923],
                }],
                recent_blockhash: Hash::from([0xFF; 32]),
            },
        };

        println!(
            "runtime_transaction.serialize().len(): {}",
            runtime_transaction.serialize().len()
        );

        // Ensure the runtime transaction is within its limit
        assert!(runtime_transaction.check_tx_size_limit().is_ok());

        let processed_transaction = ProcessedTransaction {
            runtime_transaction: runtime_transaction.clone(),
            status: Status::Failed("X".repeat(super::MAX_STATUS_FAILED_MESSAGE_SIZE)), // Large error message
            bitcoin_txid: Some(Hash::from([0xFF; 32])),
            logs: (0..super::MAX_LOG_MESSAGES)
                .map(|_| "X".repeat(super::LOG_MESSAGES_BYTES_LIMIT))
                .collect(),
            rollback_status: RollbackStatus::Rolledback(
                "X".repeat(ROLLBACK_MESSAGE_BUFFER_SIZE - 9),
            ),
        };

        let processed_transaction_serialized_len = processed_transaction.to_vec().unwrap();

        println!(
            "processed_transaction_serialized_len: {}",
            processed_transaction_serialized_len.len()
        );
        println!(
            "max_serialized_size: {}",
            ProcessedTransaction::max_serialized_size()
        );

        assert!(
            processed_transaction_serialized_len.len()
                <= ProcessedTransaction::max_serialized_size()
        );
    }

    #[test]
    fn test_from_fixed_array_invalid_utf8() {
        let mut data = [0u8; ROLLBACK_MESSAGE_BUFFER_SIZE];
        data[0] = 1;
        data[1..9].copy_from_slice(&(3u64.to_le_bytes()));
        data[9..12].copy_from_slice(&[0xff, 0xff, 0xff]); // Invalid UTF-8
        let result = RollbackStatus::from_fixed_array(&data);
        assert!(matches!(
            result,
            Err(ParseProcessedTransactionError::FromUtf8Error(_))
        ));
    }

    #[test]
    fn test_from_fixed_array_msg_len_exceeds_buffer() {
        let mut data = [0u8; ROLLBACK_MESSAGE_BUFFER_SIZE];
        data[0] = 1;
        // Set msg_len to exceed available space (buffer size - 9 bytes for header)
        let invalid_msg_len = ROLLBACK_MESSAGE_BUFFER_SIZE - 8; // This will exceed when we add 9
        data[1..9].copy_from_slice(&(invalid_msg_len as u64).to_le_bytes());
        let result = RollbackStatus::from_fixed_array(&data);
        assert!(matches!(
            result,
            Err(ParseProcessedTransactionError::BufferTooShort)
        ));
    }
}
