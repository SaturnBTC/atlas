use anyhow::{anyhow, Result};
use bip322::{create_to_sign, create_to_spend, verify_simple};
use bitcoin::{
    address::Address,
    key::{Keypair, TapTweak},
    secp256k1::{Secp256k1, SecretKey},
    sighash::{self, SighashCache},
    Amount, PrivateKey, Psbt, TapSighashType, Transaction, TxOut, Witness,
};

use bitcoin::key::UntweakedKeypair;
use bitcoin::XOnlyPublicKey;

/* -------------------------------------------------------------------------- */
/*                      SIGNS A MESSAGE FOLLOWING BIP322                      */
/* -------------------------------------------------------------------------- */
/// Signs a message following the BIP 322 Standard :
/// https://github.com/bitcoin/bips/blob/master/bip-0322.mediawiki
pub fn sign_message_bip322(
    keypair: &UntweakedKeypair,
    msg: &[u8],
    network: bitcoin::Network,
) -> [u8; 64] {
    let secp = Secp256k1::new();
    let xpubk = XOnlyPublicKey::from_keypair(keypair).0;
    let private_key = PrivateKey::new(SecretKey::from_keypair(keypair), network);

    let address = Address::p2tr(&secp, xpubk, None, network);

    let to_spend = create_to_spend(&address, msg).unwrap();
    let mut to_sign = create_to_sign(&to_spend, None).unwrap();

    let witness = match address.witness_program() {
        Some(witness_program) => {
            let version = witness_program.version().to_num();
            let program_len = witness_program.program().len();

            match version {
                1 => {
                    if program_len != 32 {
                        panic!("not key spend path");
                    }
                    create_message_signature_taproot(&to_spend, &to_sign, private_key)
                }
                _ => {
                    panic!("unsuported address");
                }
            }
        }
        None => {
            panic!("unsuported address");
        }
    };

    to_sign.inputs[0].final_script_witness = Some(witness);

    let signature = to_sign.extract_tx().unwrap().input[0].witness.clone();

    signature.to_vec()[0][..64].try_into().unwrap()
}

/* -------------------------------------------------------------------------- */
/*                       PROVIDES THE WITNESS FOR A PSBT                      */
/* -------------------------------------------------------------------------- */
/// Helper function to sign in taproot format
fn create_message_signature_taproot(
    to_spend_tx: &Transaction,
    to_sign: &Psbt,
    private_key: PrivateKey,
) -> Witness {
    let mut to_sign = to_sign.clone();

    let secp = Secp256k1::new();
    let key_pair = Keypair::from_secret_key(&secp, &private_key.inner);

    let (x_only_public_key, _parity) = XOnlyPublicKey::from_keypair(&key_pair);
    to_sign.inputs[0].tap_internal_key = Some(x_only_public_key);

    let sighash_type = TapSighashType::All;

    let mut sighash_cache = SighashCache::new(to_sign.unsigned_tx.clone());

    let sighash = sighash_cache
        .taproot_key_spend_signature_hash(
            0,
            &sighash::Prevouts::All(&[TxOut {
                value: Amount::from_sat(0),
                script_pubkey: to_spend_tx.output[0].clone().script_pubkey,
            }]),
            sighash_type,
        )
        .expect("signature hash should compute");

    let key_pair = key_pair
        .tap_tweak(&secp, to_sign.inputs[0].tap_merkle_root)
        .to_inner();

    let sig = secp.sign_schnorr(
        &bitcoin::secp256k1::Message::from_digest_slice(sighash.as_ref())
            .expect("should be cryptographically secure hash"),
        &key_pair,
    );

    let witness = sighash_cache
        .witness_mut(0)
        .expect("getting mutable witness reference should work");

    witness.push(
        bitcoin::taproot::Signature {
            signature: sig,
            sighash_type,
        }
        .to_vec(),
    );

    witness.to_owned()
}

/* -------------------------------------------------------------------------- */
/*               VERIFY A MESSAGE SIGNATURE ACCORDING TO BIP322               */
/* -------------------------------------------------------------------------- */
/// Verifies a BIP 322 signature for a particular message
/// https://github.com/bitcoin/bips/blob/master/bip-0322.mediawiki
pub fn verify_message_bip322(
    msg: &[u8],
    pubkey: [u8; 32],
    signature: [u8; 64],
    uses_sighash_all: bool,
    network: bitcoin::Network,
) -> Result<()> {
    let mut signature = signature.to_vec();
    if uses_sighash_all {
        signature.push(1);
    }
    let mut witness = Witness::new();
    witness.push(&signature);

    let secp = Secp256k1::new();
    let xpubk = XOnlyPublicKey::from_slice(&pubkey)?;
    let address = Address::p2tr(&secp, xpubk, None, network);

    verify_simple(&address, msg, witness).map_err(|e| anyhow!("BIP-322 verification failed: {}", e))
}
#[cfg(test)]
mod bip322_tests {
    use crate::helper::{sign_message_bip322, with_secret_key_file};

    #[test]
    fn test_sign_with_random_nonce() {
        let (first_account_keypair, _first_account_pubkey) =
            with_secret_key_file(".first_account.json")
                .expect("getting first account info should not fail");

        let signature1 = sign_message_bip322(
            &first_account_keypair,
            b"helloworld",
            bitcoin::Network::Testnet,
        );
        let signature2 = sign_message_bip322(
            &first_account_keypair,
            b"helloworld",
            bitcoin::Network::Testnet,
        );

        println!("signature1 {:?}", signature1);
        println!("signature2 {:?}", signature2);
        assert_ne!(signature1, signature2);
    }
}
