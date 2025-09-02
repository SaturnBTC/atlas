use arch_program::pubkey::Pubkey;
use serde::{Deserialize, Serialize};

use super::AccountInfo;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramAccount {
    pub pubkey: Pubkey,
    pub account: AccountInfo,
}
