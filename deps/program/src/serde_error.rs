use thiserror::Error;

#[derive(Error, Debug, Clone, PartialEq)]
pub enum SerialisationErrors {
    #[error("Arithmetic Overflow, unallowed access, check data again")]
    OverFlow,

    #[error("Data is not large enough for the requested operation")]
    SizeTooSmall,

    #[error("Data corrupted, unable to decode")]
    CorruptedData,

    #[error("Max instruction len crossed")]
    MoreThanMaxInstructionsAllowed,

    #[error("Max account len crossed")]
    MoreThanMaxAccountsAllowed,

    #[error("More than Allowed Signers")]
    MoreThanMaxSigners,

    #[error("More than Max allowed Keys")]
    MoreThanMaxAllowedKeys,
}

/// Safely gets a slice of a constant size N from data at a given offset.
pub fn get_const_slice<const N: usize>(
    data: &[u8],
    offset: usize,
) -> Result<[u8; N], SerialisationErrors> {
    let end = offset.checked_add(N).ok_or(SerialisationErrors::OverFlow)?;
    let slice = data
        .get(offset..end)
        .ok_or(SerialisationErrors::CorruptedData)?;
    slice
        .try_into()
        .map_err(|_| SerialisationErrors::CorruptedData)
}

/// Safely gets a slice of a given length from data at a given start position.
pub fn get_slice(data: &[u8], start: usize, len: usize) -> Result<&[u8], SerialisationErrors> {
    let end = start
        .checked_add(len)
        .ok_or(SerialisationErrors::OverFlow)?;
    data.get(start..end)
        .ok_or(SerialisationErrors::CorruptedData)
}
