use std::{
    fmt::Display,
    io::{Error, ErrorKind},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockTransactionFilter {
    #[serde(rename = "full")]
    Full,
    #[serde(rename = "signatures")]
    Signatures,
}

impl Display for BlockTransactionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockTransactionFilter::Full => write!(f, "full"),
            BlockTransactionFilter::Signatures => write!(f, "signatures"),
        }
    }
}

impl FromStr for BlockTransactionFilter {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "full" => Ok(BlockTransactionFilter::Full),
            "signatures" => Ok(BlockTransactionFilter::Signatures),
            _ => Err(Error::new(
                ErrorKind::InvalidInput,
                "Invalid block transaction filter",
            )),
        }
    }
}
