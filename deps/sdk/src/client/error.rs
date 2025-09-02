use arch_program::pubkey::Pubkey;
use hex::FromHexError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, thiserror::Error)]
pub enum ArchError {
    #[error("RPC request failed: {0}")]
    RpcRequestFailed(String),

    #[error("Failed to parse response: {0}")]
    ParseError(String),

    #[error("Operation timed out: {0}")]
    TimeoutError(String),

    #[error("Transaction error: {0}")]
    TransactionError(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Hash error: {0}")]
    HashError(#[from] arch_program::hash::HashError),

    #[error("Unknown error: {0}")]
    UnknownError(String),

    #[error("FromHexError: {0}")]
    FromHexError(String),

    #[error("Required signer not found for key: {0}")]
    RequiredSignerNotFound(Pubkey),
}

impl From<serde_json::Error> for ArchError {
    fn from(err: serde_json::Error) -> Self {
        ArchError::ParseError(err.to_string())
    }
}

impl From<std::io::Error> for ArchError {
    fn from(err: std::io::Error) -> Self {
        ArchError::NetworkError(err.to_string())
    }
}

impl From<reqwest::Error> for ArchError {
    fn from(err: reqwest::Error) -> Self {
        ArchError::NetworkError(err.to_string())
    }
}

impl From<String> for ArchError {
    fn from(err: String) -> Self {
        ArchError::UnknownError(err)
    }
}

impl From<&str> for ArchError {
    fn from(err: &str) -> Self {
        ArchError::UnknownError(err.to_string())
    }
}

impl From<FromHexError> for ArchError {
    fn from(err: FromHexError) -> Self {
        ArchError::FromHexError(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, ArchError>;
