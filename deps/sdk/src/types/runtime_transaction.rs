use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use super::Signature;
use arch_program::sanitized::ArchMessage;
use arch_program::{
    hash::Hash,
    serde_error::{get_const_slice, get_slice, SerialisationErrors},
};
use arch_program::{
    sanitize::{Sanitize, SanitizeError},
    MAX_SIGNERS,
};
use bitcode::{Decode, Encode};
use borsh::{BorshDeserialize, BorshSerialize};
#[cfg(feature = "fuzzing")]
use libfuzzer_sys::arbitrary;
use serde::{Deserialize, Serialize};
use sha256::digest;

pub const RUNTIME_TX_SIZE_LIMIT: usize = 10240;

/// Allowed versions for RuntimeTransaction
pub const ALLOWED_VERSIONS: [u32; 1] = [0];

#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum RuntimeTransactionError {
    #[error("runtime transaction size exceeds limit: {0} > {1}")]
    RuntimeTransactionSizeExceedsLimit(usize, usize),

    #[error("insufficient bytes for message")]
    InsufficientBytesForMessage,

    #[error("sanitize error: {0}")]
    SanitizeError(#[from] SanitizeError),

    #[error("invalid recent blockhash")]
    InvalidRecentBlockhash,

    #[error("too many signatures: allowed {0}, found {1}")]
    TooManySignatures(usize, usize),

    #[error("SerialisationError: {0}")]
    SerialisationError(#[from] SerialisationErrors),
}

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    Encode,
    Decode,
    Hash,
)]
#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]

pub struct RuntimeTransaction {
    pub version: u32,
    pub signatures: Vec<Signature>,
    pub message: ArchMessage,
}

impl Sanitize for RuntimeTransaction {
    fn sanitize(&self) -> Result<(), SanitizeError> {
        // Size check
        self.check_tx_size_limit().map_err(|err| match err {
            RuntimeTransactionError::RuntimeTransactionSizeExceedsLimit(serialized_len, limit) => {
                SanitizeError::InvalidSize {
                    serialized_len,
                    limit,
                }
            }
            _ => SanitizeError::InvalidValue,
        })?;

        // Check if version is allowed
        if !ALLOWED_VERSIONS.contains(&self.version) {
            return Err(SanitizeError::InvalidVersion);
        }

        // Check if number of signatures matches required signers
        if self.signatures.len() != self.message.header().num_required_signatures as usize {
            return Err(SanitizeError::SignatureCountMismatch {
                expected: self.message.header().num_required_signatures as usize,
                actual: self.signatures.len(),
            });
        }
        // Continue with message sanitization
        self.message.sanitize()
    }
}

impl Display for RuntimeTransaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RuntimeTransaction {{ version: {}, signatures: {}, message: {:?} }}",
            self.version,
            self.signatures.len(),
            self.message
        )
    }
}

impl RuntimeTransaction {
    pub fn txid(&self) -> Hash {
        self.hash()
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut serilized = vec![];

        serilized.extend(self.version.to_le_bytes());
        serilized.push(self.signatures.len() as u8);
        for signature in self.signatures.iter() {
            serilized.extend(&signature.0);
        }
        serilized.extend(self.message.serialize());

        serilized
    }

    pub fn from_slice(data: &[u8]) -> Result<Self, RuntimeTransactionError> {
        let mut cursor: usize = 0;

        // Read version
        const VERSION_SIZE: usize = 4;
        let version_bytes = get_const_slice::<VERSION_SIZE>(data, cursor)?;
        let version = u32::from_le_bytes(version_bytes);
        cursor += VERSION_SIZE;

        // Read signatures length
        const SIG_LEN_SIZE: usize = 1;
        let signatures_len_byte = get_const_slice::<SIG_LEN_SIZE>(data, cursor)?;
        let signatures_len = signatures_len_byte[0] as usize;
        cursor += SIG_LEN_SIZE;

        if signatures_len > MAX_SIGNERS {
            return Err(RuntimeTransactionError::TooManySignatures(
                MAX_SIGNERS,
                signatures_len,
            ));
        }

        // Read signatures
        const SIGNATURE_SIZE: usize = 64;
        let signatures_data_len = signatures_len
            .checked_mul(SIGNATURE_SIZE)
            .ok_or(SanitizeError::InvalidValue)?;
        let signatures_slice = get_slice(data, cursor, signatures_data_len)?;

        let mut signatures = Vec::with_capacity(signatures_len);
        for sig_bytes in signatures_slice.chunks_exact(SIGNATURE_SIZE) {
            let sig_array = get_const_slice::<SIGNATURE_SIZE>(sig_bytes, 0)?;
            signatures.push(Signature::from(sig_array));
            cursor += SIGNATURE_SIZE;
        }

        // Deserialize the rest of the message from the corrected cursor position
        let message_slice = data
            .get(cursor..)
            .ok_or(RuntimeTransactionError::InsufficientBytesForMessage)?;
        let message = ArchMessage::deserialize(message_slice)?;

        Ok(Self {
            version,
            signatures,
            message,
        })
    }

    pub fn hash(&self) -> Hash {
        let hash_string = digest(digest(self.serialize()));
        Hash::from_str(&hash_string).expect("SHA256 always produces valid hex")
    }

    pub fn check_tx_size_limit(&self) -> Result<(), RuntimeTransactionError> {
        let serialized_tx = self.serialize();
        if serialized_tx.len() > RUNTIME_TX_SIZE_LIMIT {
            Err(RuntimeTransactionError::RuntimeTransactionSizeExceedsLimit(
                serialized_tx.len(),
                RUNTIME_TX_SIZE_LIMIT,
            ))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RuntimeTransaction, Signature, ALLOWED_VERSIONS};
    use arch_program::hash::Hash;
    use arch_program::{
        pubkey::Pubkey,
        sanitize::{Sanitize as _, SanitizeError},
        sanitized::{ArchMessage, MessageHeader, SanitizedInstruction},
    };

    fn create_test_transaction(
        version: u32,
        num_signatures: usize,
        num_accounts: usize,
    ) -> RuntimeTransaction {
        RuntimeTransaction {
            version,
            signatures: vec![Signature::from([1; 64]); num_signatures],
            message: ArchMessage {
                header: MessageHeader {
                    num_required_signatures: 2,
                    num_readonly_signed_accounts: 1,
                    num_readonly_unsigned_accounts: 1,
                },
                account_keys: (0..num_accounts).map(|_| Pubkey::new_unique()).collect(),
                recent_blockhash: Hash::from([0; 32]),
                instructions: vec![SanitizedInstruction {
                    program_id_index: 2,
                    accounts: vec![0, 1, 3],
                    data: vec![1, 2, 3],
                }],
            },
        }
    }

    #[test]
    fn test_all_allowed_versions_are_valid() {
        for &version in ALLOWED_VERSIONS.iter() {
            let transaction = create_test_transaction(version, 2, 4);
            assert!(
                transaction.sanitize().is_ok(),
                "Version {} should be valid",
                version
            );
        }
    }

    #[test]
    fn test_version_not_in_allowed_versions() {
        // Find a version that's not in ALLOWED_VERSIONS
        let invalid_version = (0..u32::MAX)
            .find(|&v| !ALLOWED_VERSIONS.contains(&v))
            .expect("Should find at least one invalid version");

        let transaction = create_test_transaction(invalid_version, 2, 2);
        assert_eq!(
            transaction.sanitize().unwrap_err(),
            SanitizeError::InvalidVersion,
            "Version {} should be invalid",
            invalid_version
        );
    }

    #[test]
    fn test_serde_roundtrip() {
        let original_transaction = create_test_transaction(0, 2, 4);

        // Serialize the transaction.
        let serialized_data = original_transaction.serialize();

        // Deserialize the data back into a transaction.
        let deserialized_transaction =
            RuntimeTransaction::from_slice(&serialized_data).expect("Deserialization failed");

        // The deserialized transaction must be identical to the original.
        assert_eq!(original_transaction, deserialized_transaction);
    }

    #[test]
    fn test_from_slice_insufficient_data() {
        let original_transaction = create_test_transaction(0, 2, 4);
        let serialized_data = original_transaction.serialize();

        // Test with data that is too short.
        for i in 0..serialized_data.len() {
            let truncated_data = &serialized_data[..i];
            assert!(
                RuntimeTransaction::from_slice(truncated_data).is_err(),
                "Deserialization should fail for truncated data of length {}",
                i
            );
        }
    }
}
