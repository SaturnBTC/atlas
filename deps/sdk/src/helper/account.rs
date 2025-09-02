use anyhow::{anyhow, Result};

use bitcoin::key::Keypair;

use bitcoin::Network;
use serde::Deserialize;
use serde::Serialize;

use crate::arch_program::pubkey::Pubkey;
use crate::ArchRpcClient;

use super::send_utxo;
use super::sign_message_bip322;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AccountInfoResult {
    pub owner: Pubkey,
    pub lamports: u64,
    pub data: Vec<u8>,
    pub utxo: String,
    pub is_executable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CreateAccountWithFaucetParams {
    pub pubkey: Pubkey,
    pub txid: [u8; 32],
    pub vout: u32,
}

pub fn create_and_fund_account_with_faucet(
    client: &ArchRpcClient,
    keypair: &Keypair,
    bitcoin_network: Network,
) -> Result<()> {
    let pubkey = Pubkey::from_slice(&keypair.x_only_public_key().0.serialize());

    if let Ok(_) = client.read_account_info(pubkey) {
        let processed_tx = client.request_airdrop(pubkey)?;
    } else {
        let (txid, vout) = send_utxo(pubkey, bitcoin_network).unwrap();
        let txid = hex::decode(txid).unwrap();
        let txid: [u8; 32] = txid.try_into().unwrap();

        let result = client.process_result(post_data(
            NODE1_ADDRESS,
            CREATE_ACCOUNT_WITH_FAUCET,
            CreateAccountWithFaucetParams { pubkey, txid, vout },
        ))?;
        let mut runtime_tx: RuntimeTransaction =
            serde_json::from_value(result).expect("Unable to decode create_account result");

        let message_hash = runtime_tx.message.hash();
        let signature = Signature::from_slice(&sign_message_bip322(
            &keypair,
            &message_hash,
            BITCOIN_NETWORK,
        ));

        runtime_tx.signatures.push(signature);

        let result = process_result(post_data(NODE1_ADDRESS, "send_transaction", runtime_tx))
            .expect("send_transaction should not fail")
            .as_str()
            .expect("cannot convert result to string")
            .to_string();

        let _processed_txs = fetch_processed_transactions(vec![result.clone()]).unwrap();
    }
    let account_info = read_account_info(NODE1_ADDRESS, pubkey).unwrap();

    // assert_eq!(account_info.owner, Pubkey::system_program());
    assert!(account_info.lamports >= 1_000_000_000);

    Ok(())
}
