use {
    crate::{
        error::IndexerResult, filter::Filter, metrics::MetricsCollection, processor::Processor,
    },
    arch_program::{account::AccountMeta, instruction::Instruction, pubkey::Pubkey},
    async_trait::async_trait,
    serde::{Deserialize, Serialize},
    std::{
        ops::{Deref, DerefMut},
        sync::Arc,
    },
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DecodedInstruction<T> {
    pub program_id: Pubkey,
    pub data: T,
    pub accounts: Vec<AccountMeta>,
}

/// Decodes a raw `Instruction` into a typed representation.
///
/// Implement this for each program you wish to support. A decoder should
/// return `Some(DecodedInstruction<..>)` only when it recognizes the
/// instruction format for its program, otherwise return `None`.
pub trait InstructionDecoder<'a> {
    type InstructionType;

    fn decode_instruction(
        &self,
        instruction: &'a Instruction,
    ) -> Option<DecodedInstruction<Self::InstructionType>>;
}

pub type InstructionProcessorInputType<T> = (DecodedInstruction<T>, Instruction);

pub struct InstructionPipe<T: Send> {
    pub decoder:
        Box<dyn for<'a> InstructionDecoder<'a, InstructionType = T> + Send + Sync + 'static>,
    pub processor: Box<
        dyn Processor<InputType = InstructionProcessorInputType<T>, OutputType = ()>
            + Send
            + Sync
            + 'static,
    >,
    pub filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
}

#[async_trait]
/// Drives a set of instruction decoders and forwards decoded items to a
/// processor.
///
/// Implementors should apply any configured `Filter`s before decoding or
/// processing, and must be safe to call concurrently.
pub trait InstructionPipes<'a>: Send + Sync {
    async fn run(
        &mut self,
        instruction: &Vec<Instruction>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()>;
    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>>;
}

#[async_trait]
impl<T: Send + 'static> InstructionPipes<'_> for InstructionPipe<T> {
    async fn run(
        &mut self,
        instructions: &Vec<Instruction>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        log::trace!(
            "InstructionPipe::run(instructions: {:?}, metrics)",
            instructions,
        );

        let mut decoded_instructions: Vec<InstructionProcessorInputType<T>> = Vec::new();
        for instruction in instructions {
            if let Some(decoded_instruction) = self.decoder.decode_instruction(instruction) {
                decoded_instructions.push((decoded_instruction, instruction.clone()));
            }
        }

        self.processor
            .process(decoded_instructions, metrics.clone())
            .await?;

        Ok(())
    }

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>> {
        &self.filters
    }
}

#[derive(Debug, Default)]
pub struct Instructions(pub Vec<Instruction>);

impl Instructions {
    /// Number of contained instructions.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there are no contained instructions.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Append a raw instruction.
    pub fn push(&mut self, instruction: Instruction) {
        self.0.push(instruction);
    }
}

impl Deref for Instructions {
    type Target = [Instruction];

    fn deref(&self) -> &[Instruction] {
        &self.0[..]
    }
}

impl DerefMut for Instructions {
    fn deref_mut(&mut self) -> &mut [Instruction] {
        &mut self.0[..]
    }
}

impl Clone for Instructions {
    fn clone(&self) -> Self {
        Instructions(self.0.clone())
    }
}

impl IntoIterator for Instructions {
    type Item = Instruction;
    type IntoIter = std::vec::IntoIter<Instruction>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
