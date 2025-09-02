use arch_program::pubkey::Pubkey;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AccountInfo {
    pub lamports: u64,
    pub owner: Pubkey,
    pub data: Vec<u8>,
    pub utxo: String,
    pub is_executable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AccountInfoWithPubkey {
    pub key: Pubkey,
    pub lamports: u64,
    pub owner: Pubkey,
    pub data: Vec<u8>,
    pub utxo: String,
    pub is_executable: bool,
}

impl From<(Pubkey, AccountInfo)> for AccountInfoWithPubkey {
    fn from(info: (Pubkey, AccountInfo)) -> Self {
        AccountInfoWithPubkey {
            key: info.0,
            lamports: info.1.lamports,
            owner: info.1.owner,
            data: info.1.data,
            utxo: info.1.utxo,
            is_executable: info.1.is_executable,
        }
    }
}

impl From<AccountInfoWithPubkey> for AccountInfo {
    fn from(info: AccountInfoWithPubkey) -> Self {
        AccountInfo {
            lamports: info.lamports,
            owner: info.owner,
            data: info.data,
            utxo: info.utxo,
            is_executable: info.is_executable,
        }
    }
}
