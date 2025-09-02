use arch_program::account::AccountMeta;
use arch_program::pubkey::Pubkey;
use {
    crate::{collection::InstructionDecoderCollection, instruction::DecodedInstruction},
    serde::de::DeserializeOwned,
    std::collections::HashMap,
};

#[derive(Debug, Clone)]
pub enum SchemaNode<T: InstructionDecoderCollection> {
    Instruction(InstructionSchemaNode<T>),
    Any,
}

#[derive(Debug, Clone)]
pub struct InstructionSchemaNode<T: InstructionDecoderCollection> {
    pub ix_type: T::InstructionType,
    pub name: String,
}

#[derive(Debug)]
pub struct ParsedInstruction<T: InstructionDecoderCollection> {
    pub program_id: Pubkey,
    pub instruction: DecodedInstruction<T>,
}

#[derive(Debug, Clone)]
pub struct TransactionSchema<T: InstructionDecoderCollection> {
    pub root: Vec<SchemaNode<T>>,
}

impl<T: InstructionDecoderCollection> TransactionSchema<T> {
    pub fn match_schema<U>(&self, instructions: &[ParsedInstruction<T>]) -> Option<U>
    where
        U: DeserializeOwned,
    {
        log::trace!(
            "Schema::match_schema(self: {:?}, instructions: {:?})",
            self,
            instructions
        );
        let value = serde_json::to_value(self.match_nodes(instructions)).ok()?;

        log::trace!("Schema::match_schema: deserializing value: {:?}", value);
        serde_json::from_value::<U>(value).ok()
    }

    pub fn match_nodes(
        &self,
        instructions: &[ParsedInstruction<T>],
    ) -> Option<HashMap<String, (T, Vec<AccountMeta>)>> {
        log::trace!(
            "Schema::match_nodes(self: {:?}, instructions: {:?})",
            self,
            instructions
        );
        let mut output = HashMap::<String, (T, Vec<AccountMeta>)>::new();

        let mut node_index = 0;
        let mut instruction_index = 0;

        let mut any = false;

        while let Some(node) = self.root.get(node_index) {
            log::trace!(
                "Schema::match_nodes: current node ({}): {:?}",
                node_index,
                node
            );

            if let SchemaNode::Any = node {
                log::trace!("Schema::match_nodes: Any node detected, skipping");
                any = true;
                node_index += 1;
                continue;
            }

            let mut matched = false;

            while let Some(current_instruction) = instructions.get(instruction_index) {
                log::trace!(
                    "Schema::match_nodes: current instruction ({}): {:?}",
                    instruction_index,
                    current_instruction
                );

                let SchemaNode::Instruction(instruction_node) = node else {
                    return None;
                };

                if current_instruction.instruction.data.get_type() != instruction_node.ix_type
                    && !any
                {
                    log::trace!(
                        "Schema::match_nodes: instruction type mismatch, returning (any = false)"
                    );
                    return None;
                }

                if current_instruction.instruction.data.get_type() != instruction_node.ix_type
                    && any
                {
                    log::trace!(
                        "Schema::match_nodes: instruction type mismatch, skipping (any = true)"
                    );
                    instruction_index += 1;
                    continue;
                }

                output.insert(
                    instruction_node.name.clone(),
                    (
                        current_instruction.instruction.data.clone(),
                        current_instruction.instruction.accounts.clone(),
                    ),
                );

                log::trace!(
                    "Schema::match_nodes: instruction matched, output: {:?}",
                    output
                );

                instruction_index += 1;
                node_index += 1;
                any = false;
                matched = true;
                break;
            }

            if !matched {
                log::trace!("Schema::match_nodes: node not matched, returning");
                return None;
            }
        }

        log::trace!("Schema::match_nodes: final output: {:?}", output);

        Some(output)
    }
}

pub fn merge_hashmaps<K, V>(
    a: HashMap<K, (V, Vec<AccountMeta>)>,
    b: HashMap<K, (V, Vec<AccountMeta>)>,
) -> HashMap<K, (V, Vec<AccountMeta>)>
where
    K: std::cmp::Eq + std::hash::Hash,
{
    log::trace!("merge_hashmaps(a, b)");
    let mut output = a;
    for (key, value) in b {
        output.insert(key, value);
    }
    output
}
