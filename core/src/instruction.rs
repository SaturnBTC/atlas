use {
    crate::{
        error::IndexerResult, filter::Filter, metrics::MetricsCollection, processor::Processor,
        transaction::TransactionMetadata,
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

pub type InstructionProcessorInputType<T> = (DecodedInstruction<T>, NestedInstruction);

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
        instruction: &Vec<NestedInstruction>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()>;
    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>>;
}

#[async_trait]
impl<T: Send + 'static> InstructionPipes<'_> for InstructionPipe<T> {
    async fn run(
        &mut self,
        nested_instructions: &Vec<NestedInstruction>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        log::trace!(
            "InstructionPipe::run(instructions: {:?}, metrics)",
            nested_instructions,
        );

        let mut decoded_instructions: Vec<InstructionProcessorInputType<T>> = Vec::new();
        for nested_instruction in nested_instructions {
            if let Some(decoded_instruction) = self
                .decoder
                .decode_instruction(&nested_instruction.instruction)
            {
                decoded_instructions.push((decoded_instruction, nested_instruction.clone()));
            }

            for nested_inner_instruction in nested_instruction.inner_instructions.iter() {
                if let Some(decoded_instruction) = self
                    .decoder
                    .decode_instruction(&nested_inner_instruction.instruction)
                {
                    decoded_instructions
                        .push((decoded_instruction, nested_inner_instruction.clone()));
                }
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

#[derive(Debug, Clone)]
pub struct InstructionMetadata {
    pub transaction_metadata: Arc<TransactionMetadata>,
    pub stack_height: u32,
    pub index: u32,
    pub absolute_path: Vec<u8>,
}

pub type InstructionsWithMetadata = Vec<(InstructionMetadata, Instruction)>;

/// Represents a nested instruction with metadata, including potential inner
/// instructions.
///
/// The `NestedInstruction` struct allows for recursive instruction handling,
/// where each instruction may have associated metadata and a list of nested
/// instructions.
///
/// # Fields
///
/// - `metadata`: The metadata associated with the instruction.
/// - `instruction`: The Solana instruction being processed.
/// - `inner_instructions`: A vector of `NestedInstruction`, representing any
///   nested instructions.
#[derive(Debug, Clone)]
pub struct NestedInstruction {
    pub metadata: InstructionMetadata,
    pub instruction: Instruction,
    pub inner_instructions: NestedInstructions,
}

#[derive(Debug, Default)]
pub struct NestedInstructions(pub Vec<NestedInstruction>);

impl NestedInstructions {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn push(&mut self, nested_instruction: NestedInstruction) {
        self.0.push(nested_instruction);
    }
}

impl Deref for NestedInstructions {
    type Target = [NestedInstruction];

    fn deref(&self) -> &[NestedInstruction] {
        &self.0[..]
    }
}

impl DerefMut for NestedInstructions {
    fn deref_mut(&mut self) -> &mut [NestedInstruction] {
        &mut self.0[..]
    }
}

impl Clone for NestedInstructions {
    fn clone(&self) -> Self {
        NestedInstructions(self.0.clone())
    }
}

impl IntoIterator for NestedInstructions {
    type Item = NestedInstruction;
    type IntoIter = std::vec::IntoIter<NestedInstruction>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Nests instructions based on stack height, producing a hierarchy of
/// `NestedInstruction`.
///
/// This function organizes instructions into a nested structure, enabling
/// hierarchical transaction analysis. Instructions are nested according to
/// their stack height, forming a tree-like structure.
///
/// # Parameters
///
/// - `instructions`: A list of tuples containing `InstructionMetadata` and
///   instructions.
///
/// # Returns
///
/// A vector of `NestedInstruction`, representing the instructions organized by
/// stack depth.
impl From<InstructionsWithMetadata> for NestedInstructions {
    fn from(instructions: InstructionsWithMetadata) -> Self {
        log::trace!("from(instructions: {:?})", instructions);

        // To avoid reallocations that result in dangling pointers.
        // Therefore the number of "push"s must be calculated to set the capacity
        let estimated_capacity = instructions
            .iter()
            .filter(|(meta, _)| meta.stack_height == 1)
            .count();

        UnsafeNestedBuilder::new(estimated_capacity).build(instructions)
    }
}

// https://github.com/anza-xyz/agave/blob/master/program-runtime/src/execution_budget.rs#L7
pub const MAX_INSTRUCTION_STACK_DEPTH: usize = 5;

pub struct UnsafeNestedBuilder {
    nested_ixs: Vec<NestedInstruction>,
    level_ptrs: [Option<*mut NestedInstruction>; MAX_INSTRUCTION_STACK_DEPTH],
}

impl UnsafeNestedBuilder {
    /// ## SAFETY:
    /// Make sure `capacity` is large enough to avoid capacity expansion caused
    /// by `push`
    pub fn new(capacity: usize) -> Self {
        Self {
            nested_ixs: Vec::with_capacity(capacity),
            level_ptrs: [None; MAX_INSTRUCTION_STACK_DEPTH],
        }
    }

    pub fn build(mut self, instructions: InstructionsWithMetadata) -> NestedInstructions {
        for (metadata, instruction) in instructions {
            let stack_height = metadata.stack_height as usize;

            assert!(stack_height > 0);
            assert!(stack_height <= MAX_INSTRUCTION_STACK_DEPTH);

            for ptr in &mut self.level_ptrs[stack_height..] {
                *ptr = None;
            }

            let new_instruction = NestedInstruction {
                metadata,
                instruction,
                inner_instructions: NestedInstructions::default(),
            };

            // SAFETY:The following operation is safe.
            // because:
            // 1. All pointers come from pre-allocated Vec (no extension)
            // 2. level_ptr does not guarantee any aliasing
            // 3. Lifecycle is limited to the build() method
            unsafe {
                if stack_height == 1 {
                    self.nested_ixs.push(new_instruction);
                    let ptr = self.nested_ixs.last_mut().unwrap_unchecked() as *mut _;
                    self.level_ptrs[0] = Some(ptr);
                } else if let Some(parent_ptr) = self.level_ptrs[stack_height - 2] {
                    (*parent_ptr).inner_instructions.push(new_instruction);
                    let ptr = (*parent_ptr)
                        .inner_instructions
                        .last_mut()
                        .unwrap_unchecked() as *mut _;
                    self.level_ptrs[stack_height - 1] = Some(ptr);
                }
            }
        }

        NestedInstructions(self.nested_ixs)
    }
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        arch_program::sanitized::ArchMessage,
        arch_sdk::{RollbackStatus, Status},
        Instruction,
    };

    fn create_instruction_with_metadata(
        index: u32,
        stack_height: u32,
    ) -> (InstructionMetadata, Instruction) {
        let metadata = InstructionMetadata {
            transaction_metadata: Arc::new(TransactionMetadata {
                id: String::from(" "),
                fee_payer: Pubkey::default(),
                message: ArchMessage::default(),
                status: Status::Processed,
                log_messages: vec![],
                rollback_status: RollbackStatus::NotRolledback,
                block_height: 0,
                bitcoin_txid: None,
                bitcoin_tx: None,
                inner_instructions_list: vec![],
            }),
            stack_height,
            index,
            absolute_path: vec![],
        };
        let instruction = Instruction {
            program_id: Pubkey::new_unique(),
            accounts: vec![AccountMeta::new(Pubkey::new_unique(), false)],
            data: vec![],
        };
        (metadata, instruction)
    }

    #[test]
    fn test_nested_instructions_single_level() {
        let instructions = vec![
            create_instruction_with_metadata(1, 1),
            create_instruction_with_metadata(2, 1),
        ];
        let nested_instructions: NestedInstructions = instructions.into();
        assert_eq!(nested_instructions.len(), 2);
        assert!(nested_instructions[0].inner_instructions.is_empty());
        assert!(nested_instructions[1].inner_instructions.is_empty());
    }

    #[test]
    fn test_nested_instructions_empty() {
        let instructions: InstructionsWithMetadata = vec![];
        let nested_instructions: NestedInstructions = instructions.into();
        assert!(nested_instructions.is_empty());
    }

    #[test]
    fn test_deep_nested_instructions() {
        let instructions = vec![
            create_instruction_with_metadata(0, 1),
            create_instruction_with_metadata(0, 1),
            create_instruction_with_metadata(1, 2),
            create_instruction_with_metadata(1, 3),
            create_instruction_with_metadata(1, 3),
            create_instruction_with_metadata(1, 3),
        ];

        let nested_instructions: NestedInstructions = instructions.into();
        assert_eq!(nested_instructions.len(), 2);
        assert_eq!(nested_instructions.0[1].inner_instructions.len(), 1);
    }
}
