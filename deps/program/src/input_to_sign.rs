//! Input requiring signature.
//!
//! `InputToSign` represents a transaction input
//! that needs a signature from a specific key.
#[cfg(feature = "fuzzing")]
use libfuzzer_sys::arbitrary;

use crate::{
    program_error::ProgramError,
    pubkey::{Pubkey, MAX_SEEDS, MAX_SEED_LEN},
};

/// Represents a transaction input that needs to be signed.
///
/// An `InputToSign` contains the index of the input within a transaction
/// and the public key of the signer that should sign this input.
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]

pub enum InputToSign {
    Sign { index: u32, signer: Pubkey },
    // SignWithSeeds {
    //     index: u32,
    //     program_id: Pubkey,
    //     signers_seeds: &'a [&'a [u8]],
    // },
}

impl InputToSign {
    pub fn get_signer(&self) -> Pubkey {
        let signer = match self {
            InputToSign::Sign { signer, .. } => *signer,
            // InputToSign::SignWithSeeds {
            //     signers_seeds,
            //     program_id,
            //     ..
            // } => Pubkey::create_program_address(signers_seeds, program_id)?,
        };

        signer
    }

    pub fn get_index(&self) -> u32 {
        match self {
            InputToSign::Sign { index, .. } => *index,
            // InputToSign::SignWithSeeds { index, .. } => *index,
        }
    }

    pub fn set_index(&mut self, index: u32) {
        match self {
            InputToSign::Sign {
                index: index_mut, ..
            } => *index_mut = index,
            // InputToSign::SignWithSeeds { index: index_mut, .. } => *index_mut = index,
        }
    }

    pub fn from_slice(data: &[u8]) -> Result<Self, ProgramError> {
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

        let input_to_sign_type = data
            .get(offset)
            .ok_or(ProgramError::InsufficientDataLength)?;
        offset += 1;

        let index = u32::from_le_bytes(get_const_slice(data, offset)?);
        offset += 4;

        let signer = Pubkey(get_const_slice(data, offset)?);
        offset += 32;

        match input_to_sign_type {
            0 => Ok(InputToSign::Sign { index, signer }),
            // 1 => {
            //     let signers_seeds_len = u32::from_le_bytes(get_const_slice(data, offset)?) as usize;
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

            //     Ok(InputToSign::SignWithSeeds {
            //         index,
            //         program_id: signer,
            //         signers_seeds: signers_seeds.leak(),
            //     })
            // }
            _ => Err(ProgramError::InvalidInputToSignType),
        }
    }

    pub fn serialise(&self) -> Vec<u8> {
        let mut serialized = vec![];
        match self {
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
        serialized
    }
}
