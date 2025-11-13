use arch_program::{instruction::Instruction, pubkey::Pubkey};
use atlas_arch::collection::InstructionDecoderCollection;
use atlas_arch::instruction::{DecodedInstruction, InstructionDecoder};

use crate::mocks::LiquidityPoolParams;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub enum SaturnAmmIxType {
    InitializePool,
    OpenPosition,
    IncreaseLiquidity,
    DecreaseLiquidity,
    Swap,
    CollectProtocolFees,
    AddPoolShards,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SaturnAmmInstruction {
    pub ix_type: SaturnAmmIxType,
    pub params: LiquidityPoolParams,
}

impl SaturnAmmInstruction {
    pub fn get_type(&self) -> SaturnAmmIxType {
        self.ix_type.clone()
    }
}

#[derive(Debug, Clone)]
pub struct SaturnAmmInstructionDecoder {
    pub program_id: Pubkey,
}

impl SaturnAmmInstructionDecoder {
    pub fn new(program_id: Pubkey) -> Self {
        Self { program_id }
    }
}

impl<'a> InstructionDecoder<'a> for SaturnAmmInstructionDecoder {
    type InstructionType = SaturnAmmInstruction;

    fn decode_instruction(
        &self,
        instruction: &'a Instruction,
    ) -> Option<DecodedInstruction<Self::InstructionType>> {
        if instruction.program_id != self.program_id {
            return None;
        }

        let params = borsh::from_slice::<LiquidityPoolParams>(&instruction.data).ok()?;
        let ix_type = match &params {
            LiquidityPoolParams::InitializePool(_) => SaturnAmmIxType::InitializePool,
            LiquidityPoolParams::OpenPosition(_) => SaturnAmmIxType::OpenPosition,
            LiquidityPoolParams::IncreaseLiquidity(_) => SaturnAmmIxType::IncreaseLiquidity,
            LiquidityPoolParams::DecreaseLiquidity(_) => SaturnAmmIxType::DecreaseLiquidity,
            LiquidityPoolParams::Swap(_) => SaturnAmmIxType::Swap,
            LiquidityPoolParams::CollectProtocolFees(_) => SaturnAmmIxType::CollectProtocolFees,
            LiquidityPoolParams::AddPoolShards(_) => SaturnAmmIxType::AddPoolShards,
        };

        Some(DecodedInstruction {
            program_id: instruction.program_id,
            data: SaturnAmmInstruction { ix_type, params },
            accounts: instruction.accounts.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub enum SaturnAmmInstructionCollection {
    InitializePool,
    OpenPosition,
    IncreaseLiquidity,
    DecreaseLiquidity,
    Swap,
    CollectProtocolFees,
    AddPoolShards,
}

impl InstructionDecoderCollection for SaturnAmmInstructionCollection {
    type InstructionType = SaturnAmmIxType;

    fn parse_instruction(instruction: &Instruction) -> Option<DecodedInstruction<Self>> {
        let params = borsh::from_slice::<LiquidityPoolParams>(&instruction.data).ok()?;
        let variant = match params {
            LiquidityPoolParams::InitializePool(_) => Self::InitializePool,
            LiquidityPoolParams::OpenPosition(_) => Self::OpenPosition,
            LiquidityPoolParams::IncreaseLiquidity(_) => Self::IncreaseLiquidity,
            LiquidityPoolParams::DecreaseLiquidity(_) => Self::DecreaseLiquidity,
            LiquidityPoolParams::Swap(_) => Self::Swap,
            LiquidityPoolParams::CollectProtocolFees(_) => Self::CollectProtocolFees,
            LiquidityPoolParams::AddPoolShards(_) => Self::AddPoolShards,
        };

        Some(DecodedInstruction {
            program_id: instruction.program_id,
            data: variant,
            accounts: instruction.accounts.clone(),
        })
    }

    fn get_type(&self) -> Self::InstructionType {
        match self {
            Self::InitializePool => SaturnAmmIxType::InitializePool,
            Self::OpenPosition => SaturnAmmIxType::OpenPosition,
            Self::IncreaseLiquidity => SaturnAmmIxType::IncreaseLiquidity,
            Self::DecreaseLiquidity => SaturnAmmIxType::DecreaseLiquidity,
            Self::Swap => SaturnAmmIxType::Swap,
            Self::CollectProtocolFees => SaturnAmmIxType::CollectProtocolFees,
            Self::AddPoolShards => SaturnAmmIxType::AddPoolShards,
        }
    }
}
