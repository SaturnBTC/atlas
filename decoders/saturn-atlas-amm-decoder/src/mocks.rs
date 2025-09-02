use arch_program::pubkey::Pubkey;

// Minimal replacement for satellite_lang::Owner
pub trait Owner {
    fn owner() -> Pubkey;
}

// Minimal replacement for satellite_lang::ZeroCopy discriminator pattern
// We'll model each mock POD with a static discriminant prefix and fixed-size struct
pub trait HasDiscriminator {
    const DISCRIMINATOR: &'static [u8];
}

// Mock Saturn AMM state types
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct LiquidityPoolConfig {
    pub field_a: u64,
    pub field_b: u64,
}

impl HasDiscriminator for LiquidityPoolConfig {
    const DISCRIMINATOR: &'static [u8] = b"LP_CONFIG__"; // 11 bytes
}

impl Owner for LiquidityPoolConfig {
    fn owner() -> Pubkey {
        Pubkey::new_from_array([1u8; 32])
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct LiquidityPoolShard {
    pub shard_id: u64,
    pub liquidity: u128,
}

impl HasDiscriminator for LiquidityPoolShard {
    const DISCRIMINATOR: &'static [u8] = b"LP_SHARD___"; // 11 bytes
}

impl Owner for LiquidityPoolShard {
    fn owner() -> Pubkey {
        Pubkey::new_from_array([1u8; 32])
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct Position {
    pub owner_index: u64,
    pub amount: u128,
}

impl HasDiscriminator for Position {
    const DISCRIMINATOR: &'static [u8] = b"LP_POSITION"; // 11 bytes
}

impl Owner for Position {
    fn owner() -> Pubkey {
        Pubkey::new_from_array([1u8; 32])
    }
}

// Mock instruction params mirror
#[derive(Debug, Clone, serde::Serialize, borsh::BorshDeserialize)]
pub enum LiquidityPoolParams {
    InitializePool(InitPoolParams),
    OpenPosition(OpenPositionParams),
    IncreaseLiquidity(u64),
    DecreaseLiquidity(u64),
    Swap(u64),
    CollectProtocolFees(u64),
    AddPoolShards(u64),
}

#[derive(Debug, Clone, serde::Serialize, borsh::BorshDeserialize)]
pub struct InitPoolParams {
    pub a: u64,
}

#[derive(Debug, Clone, serde::Serialize, borsh::BorshDeserialize)]
pub struct OpenPositionParams {
    pub size: u64,
}


