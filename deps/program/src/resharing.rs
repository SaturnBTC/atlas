//! Resharing account details.

use crate::pubkey::Pubkey;
use borsh::{BorshDeserialize, BorshSerialize};

pub const RESHARING_PROGRAM_ID: Pubkey = Pubkey(*b"Resharing11111111111111111111111");
pub const RESHARING_DATA_ACCOUNT_ID: Pubkey = Pubkey(*b"ResharingData1111111111111111111");
pub const RESHARING_STAGING_ACCOUNT_ID: Pubkey = Pubkey(*b"ResharingStaing11111111111111111");

pub const CHUNK_SIZE: u64 = 8192;

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, PartialEq, Eq, Clone)]
pub struct ResharingInstruction {
    /// If this is the first chunk.
    pub first_chunk: bool,

    /// If this is the last chunk.
    pub last_chunk: bool,

    /// Start offset of the chunk.
    pub start_offset: u64,

    /// Chunk data.
    pub chunk: Vec<u8>,
}

pub fn check_id(id: &Pubkey) -> bool {
    id == &RESHARING_PROGRAM_ID
}
