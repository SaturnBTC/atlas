mod bip322;
mod keys;
mod program_deployment;
mod transaction_building;
mod utxo;

pub use bip322::*;
pub use keys::*;
pub use program_deployment::*;
pub use transaction_building::*;
pub use utxo::*;

use anyhow::{anyhow, Result};
use bitcoin::{
    self,
    address::Address,
    key::{Parity, UntweakedKeypair, XOnlyPublicKey},
    secp256k1::{self, Secp256k1, SecretKey},
};
use rand_core::OsRng;
use std::fs;
use std::str::FromStr;

use bitcoin::{
    absolute::LockTime,
    key::{TapTweak, TweakedKeypair},
    sighash::{Prevouts, SighashCache},
    transaction::Version,
    Amount, OutPoint, ScriptBuf, Sequence, TapSighashType, Transaction, TxIn, Witness,
};
use bitcoincore_rpc::{Auth, Client, RawTx, RpcApi};

/// Get the explorer URL based on the Bitcoin network
pub fn get_explorer_url(network: bitcoin::Network) -> String {
    match network {
        // bitcoin::Network::Bitcoin => test_config.explorer_url_mainnet.to_string(),
        bitcoin::Network::Bitcoin => Config::mainnet().explorer_url,
        bitcoin::Network::Testnet => Config::testnet().explorer_url,
        _ => Config::devnet().explorer_url,
    }
}

/// Get the transaction URL for the explorer
pub fn get_explorer_tx_url(network: bitcoin::Network, tx_id: &str) -> String {
    format!("{}/tx/{}", get_explorer_url(network), tx_id)
}

/// Get the address URL for the explorer
pub fn get_explorer_address_url(network: bitcoin::Network, address: &str) -> String {
    format!("{}/address/{}", get_explorer_url(network), address)
}

/// Get the API URL based on the Bitcoin network
pub fn get_api_url(network: bitcoin::Network) -> String {
    match network {
        bitcoin::Network::Bitcoin => Config::mainnet().api_url,
        bitcoin::Network::Testnet => Config::testnet().api_url,
        _ => Config::devnet().api_url,
    }
}

/// Get the API endpoint URL
pub fn get_api_endpoint_url(network: bitcoin::Network, endpoint: &str) -> String {
    format!("{}/{}", get_api_url(network), endpoint)
}

/// Represents a party or node secret and address information
pub struct CallerInfo {
    pub key_pair: UntweakedKeypair,
    pub public_key: XOnlyPublicKey,
    pub parity: Parity,
    pub address: Address,
}

impl CallerInfo {
    /// Create a [CallerInfo] from the specified file path
    /// If the file does not exist, generate a random secret key
    /// and use that instead.
    pub fn with_secret_key_file(file_path: &str) -> Result<CallerInfo> {
        let test_config = Config::localnet();
        let secp = Secp256k1::new();
        let secret_key = match fs::read_to_string(file_path) {
            Ok(key) => SecretKey::from_str(&key).unwrap(),
            Err(_) => {
                let (key, _) = secp.generate_keypair(&mut OsRng);
                fs::write(file_path, key.display_secret().to_string())
                    .map_err(|_| anyhow!("Unable to write file"))?;
                key
            }
        };
        let key_pair = UntweakedKeypair::from_secret_key(&secp, &secret_key);
        let (public_key, parity) = XOnlyPublicKey::from_keypair(&key_pair);
        let address = Address::p2tr(&secp, public_key, None, test_config.network);
        Ok(CallerInfo {
            key_pair,
            public_key,
            parity,
            address,
        })
    }
}

/* -------------------------------------------------------------------------- */
/*                             PREPARES A FEE PSBT                            */
/* -------------------------------------------------------------------------- */
/// This function sends the caller BTC, then prepares a fee PSBT and returns
/// the said PSBT in HEX encoding
pub fn prepare_fees() -> String {
    let test_config = Config::localnet();

    let userpass = Auth::UserPass(
        test_config.node_username.to_string(),
        test_config.node_password.to_string(),
    );
    let rpc = Client::new(&test_config.node_endpoint, userpass)
        .expect("rpc shouldn not fail to be initiated");

    let caller =
        CallerInfo::with_secret_key_file(".program").expect("getting caller info should not fail");

    let txid = rpc
        .send_to_address(
            &caller.address,
            Amount::from_sat(100000),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("SATs should be sent to address");

    let sent_tx = rpc
        .get_raw_transaction(&txid, None)
        .expect("should get raw transaction");
    let mut vout: u32 = 0;

    for (index, output) in sent_tx.output.iter().enumerate() {
        if output.script_pubkey == caller.address.script_pubkey() {
            vout = index as u32;
        }
    }

    let mut tx = Transaction {
        version: Version::TWO,
        input: vec![TxIn {
            previous_output: OutPoint { txid, vout },
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![],
        lock_time: LockTime::ZERO,
    };

    let sighash_type = TapSighashType::NonePlusAnyoneCanPay;
    let raw_tx = rpc
        .get_raw_transaction(&txid, None)
        .expect("raw transaction should not fail");
    let prevouts = vec![raw_tx.output[vout as usize].clone()];
    let prevouts = Prevouts::All(&prevouts);

    let mut sighasher = SighashCache::new(&mut tx);
    let sighash = sighasher
        .taproot_key_spend_signature_hash(0, &prevouts, sighash_type)
        .expect("should not fail to construct sighash");

    // Sign the sighash using the secp256k1 library (exported by rust-bitcoin).
    let secp = Secp256k1::new();
    let tweaked: TweakedKeypair = caller.key_pair.tap_tweak(&secp, None);
    let msg = secp256k1::Message::from(sighash);
    let signature = secp.sign_schnorr(&msg, &tweaked.to_inner());

    // Update the witness stack.
    let signature = bitcoin::taproot::Signature {
        signature,
        sighash_type,
    };
    tx.input[0].witness.push(signature.to_vec());

    tx.raw_hex()
}
