use crate::{ArchError, RuntimeTransaction, Signature};

use super::sign_message_bip322;
use arch_program::sanitized::ArchMessage;
use bitcoin::{key::Keypair, Network};

/// Sign and send a transaction
pub fn build_and_sign_transaction(
    message: ArchMessage,
    signers: Vec<Keypair>,
    bitcoin_network: Network,
) -> Result<RuntimeTransaction, ArchError> {
    let digest_slice = message.hash();
    let signatures = message
        .account_keys
        .iter()
        .take(message.header.num_required_signatures as usize)
        .map(|key| {
            let signature_array = sign_message_bip322(
                signers
                    .iter()
                    .find(|signer| signer.x_only_public_key().0.serialize() == key.serialize())
                    .ok_or(ArchError::RequiredSignerNotFound(*key))?,
                &digest_slice,
                bitcoin_network,
            );
            Ok(Signature(signature_array))
        })
        .collect::<Result<Vec<Signature>, ArchError>>()?;

    Ok(RuntimeTransaction {
        version: 0,
        signatures,
        message,
    })
}
