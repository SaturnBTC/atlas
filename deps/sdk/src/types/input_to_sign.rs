use arch_program::{program_error::ProgramError, pubkey::Pubkey};

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
pub enum InputToSign {
    Sign {
        index: u32,
        signer: Pubkey,
    },
    SignWithSeeds {
        index: u32,
        program_id: Pubkey,
        signers_seeds: Vec<Vec<u8>>,
    },
}

impl InputToSign {
    pub fn get_signer(&self) -> Result<Pubkey, ProgramError> {
        let signer = match self {
            InputToSign::Sign { signer, .. } => *signer,
            InputToSign::SignWithSeeds {
                signers_seeds,
                program_id,
                ..
            } => Pubkey::create_program_address(
                signers_seeds
                    .iter()
                    .map(|seed| seed.as_slice())
                    .collect::<Vec<_>>()
                    .as_slice(),
                program_id,
            )?,
        };

        Ok(signer)
    }

    pub fn get_index(&self) -> u32 {
        match self {
            InputToSign::Sign { index, .. } => *index,
            InputToSign::SignWithSeeds { index, .. } => *index,
        }
    }
}
