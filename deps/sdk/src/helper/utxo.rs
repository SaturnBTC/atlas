use crate::arch_program::pubkey::Pubkey;
use crate::client::ArchRpcClient;
use bitcoin::{address::Address, Amount, Network};
use bitcoincore_rpc::{Auth, Client, RpcApi};
use std::str::FromStr;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

#[derive(Clone)]
pub struct Config {
    pub node_endpoint: String,
    pub node_username: String,
    pub node_password: String,
    pub network: Network,
    pub arch_node_url: String,
    pub api_url: String,
    pub explorer_url: String,
}
impl Config {
    pub fn localnet() -> Self {
        Self {
            node_endpoint: "http://127.0.0.1:18443/wallet/testwallet".to_string(),
            node_username: "bitcoin".to_string(),
            node_password: "bitcoinpass".to_string(),
            network: Network::Regtest,
            arch_node_url: "http://localhost:9002/".to_string(),
            api_url: "".to_string(),
            explorer_url: "".to_string(),
        }
    }
    pub fn devnet() -> Self {
        Self {
            node_endpoint: "".to_string(),
            node_username: "bitcoin".to_string(),
            node_password: "bitcoinpass".to_string(),
            network: Network::Testnet4,
            arch_node_url: "".to_string(),
            api_url: "ttps://mempool.dev.aws.archnetwork.xyz/api/v1".to_string(),
            explorer_url: "https://mempool.dev.aws.archnetwork.xyz".to_string(),
        }
    }
    pub fn testnet() -> Self {
        Self {
            node_endpoint: "".to_string(),
            node_username: "bitcoin".to_string(),
            node_password: "bitcoinpass".to_string(),
            network: Network::Testnet4,
            arch_node_url: "".to_string(),
            api_url: "https://mempool.space/testnet4/api/v1".to_string(),
            explorer_url: "https://mempool.space/testnet4".to_string(),
        }
    }
    pub fn mainnet() -> Self {
        Self {
            node_endpoint: "".to_string(),
            node_username: "bitcoin".to_string(),
            node_password: "bitcoinpass".to_string(),
            network: Network::Bitcoin,
            arch_node_url: "".to_string(),
            api_url: "https://mempool.space/api/v1".to_string(),
            explorer_url: "https://mempool.space".to_string(),
        }
    }
    // TODO: Add devnet, testnet and mainnet configs
}

/// Helper struct for Bitcoin operations
#[derive(Clone)]
pub struct BitcoinHelper {
    /// Bitcoin network (Mainnet, Testnet, Regtest)
    network: Network,
    /// Bitcoin RPC client
    rpc_client: Arc<Client>,
    /// Arch RPC client
    arch_client: ArchRpcClient,
}

impl BitcoinHelper {
    /// Create a new BitcoinHelper
    pub fn new(config: &Config) -> Self {
        let userpass = Auth::UserPass(config.node_username.clone(), config.node_password.clone());
        let rpc_client = Arc::new(
            Client::new(&config.node_endpoint, userpass)
                .expect("Failed to initialize Bitcoin RPC client"),
        );
        let arch_client = ArchRpcClient::new(&config.arch_node_url);
        Self {
            network: config.network,
            rpc_client,
            arch_client,
        }
    }

    /// Get the account address for the given pubkey
    fn get_account_address(&self, pubkey: Pubkey) -> String {
        self.arch_client
            .get_account_address(&pubkey)
            .expect("Failed to get account address")
    }

    /// Used to send a utxo to the taptweaked account address corresponding to the
    /// network's joint pubkey
    pub fn send_utxo(&self, pubkey: Pubkey) -> Result<(String, u32), String> {
        let address = self.get_account_address(pubkey);

        let account_address = match Address::from_str(&address) {
            Ok(addr) => match addr.require_network(self.network) {
                Ok(addr) => addr,
                Err(e) => return Err(format!("Network mismatch for address: {}", e)),
            },
            Err(e) => return Err(format!("Failed to parse address: {}", e)),
        };

        let txid = match self.rpc_client.send_to_address(
            &account_address,
            Amount::from_sat(3000),
            None,
            None,
            None,
            None,
            None,
            None,
        ) {
            Ok(txid) => txid,
            Err(e) => return Err(format!("Failed to send to address: {}", e)),
        };

        let sent_tx = match self.rpc_client.get_raw_transaction(&txid, None) {
            Ok(tx) => tx,
            Err(e) => return Err(format!("Failed to get raw transaction: {}", e)),
        };

        let mut vout = 0;
        for (index, output) in sent_tx.output.iter().enumerate() {
            if output.script_pubkey == account_address.script_pubkey() {
                vout = index as u32;
            }
        }

        sleep(Duration::from_millis(200));

        Ok((txid.to_string(), vout))
    }
}
