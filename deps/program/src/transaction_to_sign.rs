//! Transaction to sign representation for serialization and deserialization.
//!
//! This module provides the `TransactionToSign` struct which represents a transaction
//! along with the inputs that need to be signed.

use crate::input_to_sign::InputToSign;
use crate::program_error::ProgramError;
use crate::pubkey::Pubkey;

/// Represents a transaction that needs to be signed with associated inputs.
///
/// This struct holds the raw transaction bytes and a list of inputs that need to be
/// signed, each with their own index and signer public key.
#[repr(C)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TransactionToSign<'a> {
    /// The raw transaction bytes to be signed.
    pub tx_bytes: &'a [u8],
    /// List of inputs within the transaction that need signatures.
    pub inputs_to_sign: &'a [InputToSign],
}

impl<'a> TransactionToSign<'a> {
    /// Serializes the `TransactionToSign` into a byte vector.
    ///
    /// The serialized format is:
    /// - 4 bytes: length of tx_bytes (u32, little endian)
    /// - N bytes: tx_bytes content
    /// - 4 bytes: number of inputs to sign (u32, little endian)
    /// - For each input:
    ///   - 4 bytes: input index (u32, little endian)
    ///   - 32 bytes: signer public key
    ///
    /// # Returns
    ///
    /// A vector of bytes containing the serialized transaction.
    pub fn serialise(&self) -> Vec<u8> {
        let mut serialized = vec![];

        serialized.extend_from_slice(&(self.tx_bytes.len() as u32).to_le_bytes());
        serialized.extend_from_slice(self.tx_bytes);
        serialized.extend_from_slice(&(self.inputs_to_sign.len() as u32).to_le_bytes());
        for input_to_sign in self.inputs_to_sign.iter() {
            match input_to_sign {
                InputToSign::Sign { index, signer } => {
                    serialized.push(0);
                    serialized.extend_from_slice(&index.to_le_bytes());
                    serialized.extend_from_slice(&signer.serialize());
                } // InputToSign::SignWithSeeds {
                  //     index,
                  //     program_id,
                  //     signers_seeds,
                  // } => {
                  //     serialized.push(1);
                  //     serialized.extend_from_slice(&index.to_le_bytes());
                  //     serialized.extend_from_slice(&program_id.serialize());
                  //     serialized.extend_from_slice(&(signers_seeds.len() as u32).to_le_bytes());
                  //     for seed in signers_seeds.iter() {
                  //         serialized.extend_from_slice(&(seed.len() as u32).to_le_bytes());
                  //         serialized.extend_from_slice(seed);
                  //     }
                  // }
            }
        }

        serialized
    }

    pub fn serialise_inputs_to_sign(inputs_to_sign: &[InputToSign]) -> Vec<u8> {
        let mut serialized = vec![];
        serialized.extend_from_slice(&(inputs_to_sign.len() as u32).to_le_bytes());
        for input_to_sign in inputs_to_sign.iter() {
            match input_to_sign {
                InputToSign::Sign { index, signer } => {
                    serialized.push(0);
                    serialized.extend_from_slice(&index.to_le_bytes());
                    serialized.extend_from_slice(&signer.serialize());
                } // InputToSign::SignWithSeeds {
                  //     index,
                  //     program_id,
                  //     signers_seeds,
                  // } => {
                  //     serialized.push(1);
                  //     serialized.extend_from_slice(&index.to_le_bytes());
                  //     serialized.extend_from_slice(&program_id.serialize());
                  //     serialized.extend_from_slice(&(signers_seeds.len() as u32).to_le_bytes());
                  //     for seed in signers_seeds.iter() {
                  //         serialized.extend_from_slice(&(seed.len() as u32).to_le_bytes());
                  //         serialized.extend_from_slice(seed);
                  //     }
                  // }
            }
        }

        serialized
    }

    /// Serializes a Transaction and inputs_to_sign directly into a single buffer
    /// without intermediate allocations
    ///
    /// # Parameters
    ///
    /// * `tx` - A reference to the Bitcoin transaction to serialize.
    /// * `inputs_to_sign` - A slice of `InputToSign` structs representing the inputs to sign.
    ///
    /// # Returns
    ///
    /// A vector of bytes containing the serialized transaction.
    pub fn serialise_with_tx(tx: &bitcoin::Transaction, inputs_to_sign: &[InputToSign]) -> Vec<u8> {
        use bitcoin::consensus::Encodable;

        let inputs_count = inputs_to_sign.len();
        let mut serialized = vec![];

        // Reserve 4 bytes for tx length (we'll write this after we know the actual length)
        let tx_len_pos = serialized.len();
        serialized.extend_from_slice(&[0u8; 4]); // Placeholder for tx length

        // Serialize transaction directly into the buffer
        let tx_start = serialized.len();
        tx.consensus_encode(&mut serialized)
            .expect("Transaction encoding should not fail");
        let tx_end = serialized.len();
        let tx_len = tx_end - tx_start;

        // Write the actual tx length back to the reserved space
        serialized[tx_len_pos..tx_len_pos + 4].copy_from_slice(&(tx_len as u32).to_le_bytes());

        // Serialize inputs_to_sign
        serialized.extend_from_slice(&(inputs_count as u32).to_le_bytes());
        for input_to_sign in inputs_to_sign.iter() {
            match input_to_sign {
                InputToSign::Sign { index, signer } => {
                    serialized.push(0);
                    serialized.extend_from_slice(&index.to_le_bytes());
                    serialized.extend_from_slice(&signer.serialize());
                } // InputToSign::SignWithSeeds {
                  //     index,
                  //     program_id,
                  //     signers_seeds,
                  // } => {
                  //     serialized.push(1);
                  //     serialized.extend_from_slice(&index.to_le_bytes());
                  //     serialized.extend_from_slice(&program_id.serialize());
                  //     serialized.extend_from_slice(&(signers_seeds.len() as u32).to_le_bytes());
                  //     for seed in signers_seeds.iter() {
                  //         serialized.extend_from_slice(&(seed.len() as u32).to_le_bytes());
                  //         serialized.extend_from_slice(seed);
                  //     }
                  // }
            }
        }

        serialized
    }

    /// Deserializes a byte slice into a `TransactionToSign`.
    ///
    /// # Parameters
    ///
    /// * `data` - A byte slice containing the serialized transaction.
    ///
    /// # Returns
    ///
    /// A new `TransactionToSign` instance.
    ///
    /// # Panics
    ///
    /// This function will panic if the input data is malformed or doesn't contain
    /// enough bytes for the expected format.
    pub fn from_slice(data: &'a [u8]) -> Result<Self, ProgramError> {
        fn get_const_slice<const N: usize>(
            data: &[u8],
            offset: usize,
        ) -> Result<[u8; N], ProgramError> {
            let end = offset + N;
            let slice = data
                .get(offset..end)
                .ok_or(ProgramError::InsufficientDataLength)?;
            let array_ref = slice
                .try_into()
                .map_err(|_| ProgramError::IncorrectLength)?;
            Ok(array_ref)
        }

        fn get_slice(data: &[u8], start: usize, len: usize) -> Result<&[u8], ProgramError> {
            data.get(start..start + len)
                .ok_or(ProgramError::InsufficientDataLength)
        }

        let mut offset = 0;

        let tx_bytes_len = u32::from_le_bytes(get_const_slice(data, offset)?) as usize;
        offset += 4;

        let tx_bytes = get_slice(data, offset, tx_bytes_len)?;
        offset += tx_bytes_len;

        let inputs_to_sign_len = u32::from_le_bytes(get_const_slice(data, offset)?) as usize;
        offset += 4;

        let mut inputs_to_sign = Vec::with_capacity(inputs_to_sign_len);

        for _ in 0..inputs_to_sign_len {
            let input_to_sign_type = data
                .get(offset)
                .ok_or(ProgramError::InsufficientDataLength)?;
            offset += 1;

            let index = u32::from_le_bytes(get_const_slice(data, offset)?);
            offset += 4;

            let signer = Pubkey(get_const_slice(data, offset)?);
            offset += 32;

            match input_to_sign_type {
                0 => {
                    inputs_to_sign.push(InputToSign::Sign { index, signer });
                }
                // 1 => {
                //     let signers_seeds_len =
                //         u32::from_le_bytes(get_const_slice(data, offset)?) as usize;
                //     offset += 4;

                //     if signers_seeds_len > MAX_SEEDS {
                //         return Err(ProgramError::MaxSeedsExceeded);
                //     }

                //     let mut signers_seeds: Vec<&[u8]> = Vec::with_capacity(signers_seeds_len);
                //     for _ in 0..signers_seeds_len {
                //         let seed_len = u32::from_le_bytes(get_const_slice(data, offset)?) as usize;
                //         offset += 4;

                //         if seed_len > MAX_SEED_LEN {
                //             return Err(ProgramError::MaxSeedLengthExceeded);
                //         }

                //         let seed = get_slice(data, offset, seed_len)?;
                //         offset += seed_len;
                //         signers_seeds.push(seed);
                //     }

                //     inputs_to_sign.push(InputToSign::SignWithSeeds {
                //         index,
                //         program_id: signer,
                //         signers_seeds: signers_seeds.leak(),
                //     });
                // }
                _ => {
                    return Err(ProgramError::InvalidInputToSignType);
                }
            }
        }

        Ok(TransactionToSign {
            tx_bytes,
            inputs_to_sign: inputs_to_sign.leak(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        input_to_sign::InputToSign, pubkey::Pubkey, transaction_to_sign::TransactionToSign,
    };
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn fuzz_serialize_deserialize_transaction_to_sign(
            tx_bytes in prop::collection::vec(any::<u8>(), 0..64),
            input_indices in prop::collection::vec(any::<u32>(), 0..10),
            input_pubkeys in prop::collection::vec(any::<[u8; 32]>(), 0..10)
        ) {
            let inputs_to_sign: Vec<InputToSign> = input_indices.into_iter()
                .zip(input_pubkeys.into_iter())
                .map(|(index, pubkey_bytes)| {
                    InputToSign::Sign {
                        index,
                        signer: Pubkey::from(pubkey_bytes),
                    }
                })
                .collect();

            let transaction = TransactionToSign {
                tx_bytes: &tx_bytes,
                inputs_to_sign: &inputs_to_sign,
            };

            let serialized = transaction.serialise();
            let deserialized = TransactionToSign::from_slice(&serialized).unwrap();

            assert_eq!(transaction.tx_bytes, deserialized.tx_bytes);
            assert_eq!(transaction.inputs_to_sign, deserialized.inputs_to_sign);
        }
    }

    #[test]
    fn test_serialise_with_tx_consistency() {
        use bitcoin::consensus::serialize;
        use bitcoin::{Amount, OutPoint, ScriptBuf, Transaction, TxIn, TxOut, Witness};

        // Create a simple test transaction
        let tx = Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::default(),
                script_sig: ScriptBuf::new(),
                sequence: bitcoin::Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(5000),
                script_pubkey: ScriptBuf::new(),
            }],
        };

        let inputs_to_sign = vec![
            InputToSign::Sign {
                index: 0,
                signer: Pubkey::from([1u8; 32]),
            },
            InputToSign::Sign {
                index: 1,
                signer: Pubkey::from([2u8; 32]),
            },
        ];

        // Method 1: Original approach (serialize -> TransactionToSign -> serialise)
        let tx_bytes = serialize(&tx);
        let transaction_to_sign = TransactionToSign {
            tx_bytes: &tx_bytes,
            inputs_to_sign: &inputs_to_sign,
        };
        let serialized1 = transaction_to_sign.serialise();

        // Method 2: New approach (serialise_with_tx)
        let serialized2 = TransactionToSign::serialise_with_tx(&tx, &inputs_to_sign);

        // They should produce identical results
        assert_eq!(serialized1, serialized2);

        // Verify both can be deserialized correctly
        let deserialized1 = TransactionToSign::from_slice(&serialized1).unwrap();
        let deserialized2 = TransactionToSign::from_slice(&serialized2).unwrap();

        assert_eq!(deserialized1.tx_bytes, deserialized2.tx_bytes);
        assert_eq!(deserialized1.inputs_to_sign, deserialized2.inputs_to_sign);
        assert_eq!(deserialized1.tx_bytes, &tx_bytes);
        assert_eq!(deserialized1.inputs_to_sign, &inputs_to_sign);
    }
}
