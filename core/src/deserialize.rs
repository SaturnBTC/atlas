use std::{
    io::{Error, ErrorKind, Read, Result},
    ops::Deref,
};

use arch_program::account::AccountMeta;
use borsh::BorshDeserialize;

/// Trait for Borsh-deserializable types with a fixed discriminator prefix.
///
/// Implement for account or instruction data that begins with a static
/// discriminator. `deserialize` should validate and strip the discriminator
/// before deserializing the remaining bytes.
pub trait AtlasDeserialize
where
    Self: Sized + BorshDeserialize,
{
    const DISCRIMINATOR: &'static [u8];

    fn deserialize(data: &[u8]) -> Option<Self>;
}

pub fn extract_discriminator(length: usize, data: &[u8]) -> Option<(&[u8], &[u8])> {
    log::trace!(
        "extract_discriminator(length: {:?}, data: {:?})",
        length,
        data
    );

    if data.len() < length {
        return None;
    }

    Some((&data[..length], &data[length..]))
}

/// Rearranges instruction `AccountMeta`s into a typed layout.
///
/// Use this to map positional accounts into a more descriptive structure,
/// returning `None` when the layout is not satisfied.
pub trait ArrangeAccounts {
    type ArrangedAccounts;

    fn arrange_accounts(accounts: &[AccountMeta]) -> Option<Self::ArrangedAccounts>;
}

#[derive(serde::Serialize, serde::Deserialize, Default, PartialEq, Eq, Clone)]
pub struct PrefixString(pub String);

impl Deref for PrefixString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<PrefixString> for String {
    fn from(val: PrefixString) -> Self {
        val.0
    }
}

impl std::fmt::Debug for PrefixString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self.0))
    }
}

impl BorshDeserialize for PrefixString {
    #[inline]
    fn deserialize_reader<R: Read>(reader: &mut R) -> Result<Self> {
        // read the length of the String
        let mut buffer = vec![0u8; 4];
        reader.read_exact(&mut buffer)?;
        let length = u32::deserialize(&mut buffer.as_slice())?;
        let mut buffer = vec![0u8; length as usize];
        reader.read_exact(&mut buffer)?;

        Ok(Self(String::from_utf8(buffer).map_err(|_| {
            Error::new(ErrorKind::InvalidData, "invalid utf8")
        })?))
    }
}

#[derive(serde::Serialize, Default, serde::Deserialize, PartialEq, Eq, Clone)]
pub struct U64PrefixString(pub String);

impl Deref for U64PrefixString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<U64PrefixString> for String {
    fn from(val: U64PrefixString) -> Self {
        val.0
    }
}

impl std::fmt::Debug for U64PrefixString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self.0))
    }
}

impl BorshDeserialize for U64PrefixString {
    #[inline]
    fn deserialize_reader<R: Read>(reader: &mut R) -> Result<Self> {
        // read the length of the String
        let mut buffer = vec![0u8; 8];
        reader.read_exact(&mut buffer)?;
        let length = u64::deserialize(&mut buffer.as_slice())?;
        let mut buffer = vec![0u8; length as usize];
        reader.read_exact(&mut buffer)?;

        Ok(Self(String::from_utf8(buffer).map_err(|_| {
            Error::new(ErrorKind::InvalidData, "invalid utf8")
        })?))
    }
}
