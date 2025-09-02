use arch_program::pubkey::Pubkey;
use arch_sdk::AccountInfo;
use atlas_core::account::{AccountDecoder, DecodedAccount};
use tracing::info;

use crate::mocks::{HasDiscriminator, LiquidityPoolConfig, LiquidityPoolShard, Owner, Position};

#[derive(Debug, Clone)]
pub enum SaturnAmmAccountType {
    LiquidityPoolConfig(LiquidityPoolConfig),
    LiquidityPoolShard(LiquidityPoolShard),
    Position(Position),
}

#[derive(Debug, Clone)]
pub struct SaturnAmmAccountDecoder {
    pub owner: Pubkey,
}

impl SaturnAmmAccountDecoder {
    pub fn new(owner: Pubkey) -> Self {
        Self { owner }
    }
}

fn try_load_pod<T: Copy + HasDiscriminator + Owner>(owner: &Pubkey, data: &[u8]) -> Option<T> {
    if owner != &T::owner() {
        return None;
    }

    let disc = T::DISCRIMINATOR;
    let expected_len = disc.len() + core::mem::size_of::<T>();
    if data.len() != expected_len {
        return None;
    }

    if &data[..disc.len()] != disc {
        return None;
    }

    let payload = &data[disc.len()..disc.len() + core::mem::size_of::<T>()];
    let mut tmp = core::mem::MaybeUninit::<T>::zeroed();
    let tmp_bytes = unsafe {
        core::slice::from_raw_parts_mut(tmp.as_mut_ptr() as *mut u8, core::mem::size_of::<T>())
    };
    tmp_bytes.copy_from_slice(payload);
    Some(unsafe { tmp.assume_init() })
}

impl<'a> AccountDecoder<'a> for SaturnAmmAccountDecoder {
    type AccountType = SaturnAmmAccountType;

    fn decode_account(
        &self,
        account: &'a AccountInfo,
    ) -> Option<DecodedAccount<Self::AccountType>> {
        if account.owner != self.owner {
            return None;
        }

        if let Some(config) = try_load_pod::<LiquidityPoolConfig>(&account.owner, &account.data) {
            info!("Decoded config");
            return Some(DecodedAccount {
                lamports: account.lamports,
                owner: account.owner,
                data: SaturnAmmAccountType::LiquidityPoolConfig(config),
                utxo: account.utxo.clone(),
                executable: account.is_executable,
            });
        }

        if let Some(shard) = try_load_pod::<LiquidityPoolShard>(&account.owner, &account.data) {
            info!("Decoded shard");
            return Some(DecodedAccount {
                lamports: account.lamports,
                owner: account.owner,
                data: SaturnAmmAccountType::LiquidityPoolShard(shard),
                utxo: account.utxo.clone(),
                executable: account.is_executable,
            });
        }

        if let Some(position) = try_load_pod::<Position>(&account.owner, &account.data) {
            info!("Decoded position");
            return Some(DecodedAccount {
                lamports: account.lamports,
                owner: account.owner,
                data: SaturnAmmAccountType::Position(position),
                utxo: account.utxo.clone(),
                executable: account.is_executable,
            });
        }

        None
    }
}


