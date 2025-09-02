use {
    crate::instruction::DecodedInstruction, arch_program::instruction::Instruction,
    serde::Serialize,
};

/// A set of instruction decoders used to parse nested instructions.
///
/// Implementors typically enumerate all supported instruction variants for a
/// given program and provide a way to parse and tag them with a type.
pub trait InstructionDecoderCollection:
    Clone + std::fmt::Debug + Send + Sync + Eq + std::hash::Hash + Serialize + 'static
{
    type InstructionType: Clone + std::fmt::Debug + PartialEq + Eq + Send + Sync + 'static;

    fn parse_instruction(instruction: &Instruction) -> Option<DecodedInstruction<Self>>;
    fn get_type(&self) -> Self::InstructionType;
}
