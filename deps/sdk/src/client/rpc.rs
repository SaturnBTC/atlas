use crate::arch_program::pubkey::Pubkey;
use crate::client::error::{ArchError, Result};
use crate::{
    sign_message_bip322, AccountInfoWithPubkey, BlockTransactionFilter, FullBlock,
    MAX_TX_BATCH_SIZE, NOT_FOUND_CODE,
};
use arch_program::hash::Hash;
use bitcoin::key::Keypair;
use bitcoin::Network;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{from_str, json, Value};
use std::str::FromStr;
use std::time::Duration;

// Import the appropriate result types
use crate::types::{
    AccountFilter, AccountInfo, Block, ProcessedTransaction, ProgramAccount, RuntimeTransaction,
    Status,
};

// RPC method constants
const READ_ACCOUNT_INFO: &str = "read_account_info";
const GET_MULTIPLE_ACCOUNTS: &str = "get_multiple_accounts";
const SEND_TRANSACTION: &str = "send_transaction";
const SEND_TRANSACTIONS: &str = "send_transactions";
const GET_BLOCK: &str = "get_block";
const GET_BLOCK_BY_HEIGHT: &str = "get_block_by_height";
const GET_BLOCK_COUNT: &str = "get_block_count";
const GET_BLOCK_HASH: &str = "get_block_hash";
const GET_BEST_BLOCK_HASH: &str = "get_best_block_hash";
const GET_BEST_FINALIZED_BLOCK_HASH: &str = "get_best_finalized_block_hash";
const GET_PROCESSED_TRANSACTION: &str = "get_processed_transaction";
const GET_ACCOUNT_ADDRESS: &str = "get_account_address";
const GET_PROGRAM_ACCOUNTS: &str = "get_program_accounts";
const START_DKG: &str = "start_dkg";

/// ArchRpcClient provides a simple interface for making RPC calls to the Arch blockchain
#[derive(Clone)]
pub struct ArchRpcClient {
    url: String,
}

impl ArchRpcClient {
    /// Create a new ArchRpcClient with the specified URL
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
        }
    }

    /// Make a raw RPC call with no parameters and parse the result
    /// Returns None if the item was not found (404)
    pub fn call_method<R: DeserializeOwned>(&self, method: &str) -> Result<Option<R>> {
        match self.process_result(self.post(method)?)? {
            Some(value) => {
                let result = serde_json::from_value(value).map_err(|e| {
                    ArchError::ParseError(format!("Failed to deserialize response: {}", e))
                })?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    /// Make a raw RPC call with parameters and parse the result
    /// Returns None if the item was not found (404)
    pub fn call_method_with_params<T: Serialize + std::fmt::Debug, R: DeserializeOwned>(
        &self,
        method: &str,
        params: T,
    ) -> Result<Option<R>> {
        match self.process_result(self.post_data(method, params)?)? {
            Some(value) => {
                let result = serde_json::from_value(value).map_err(|e| {
                    ArchError::ParseError(format!("Failed to deserialize response: {}", e))
                })?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    /// Get raw value from a method call
    /// Returns None if the item was not found (404)
    pub fn call_method_raw(&self, method: &str) -> Result<Option<Value>> {
        self.process_result(self.post(method)?)
    }

    /// Get raw value from a method call with parameters
    /// Returns None if the item was not found (404)
    pub fn call_method_with_params_raw<T: Serialize + std::fmt::Debug>(
        &self,
        method: &str,
        params: T,
    ) -> Result<Option<Value>> {
        self.process_result(self.post_data(method, params)?)
    }

    /// Read account information for the specified public key
    pub fn read_account_info(&self, pubkey: Pubkey) -> Result<AccountInfo> {
        match self.call_method_with_params(READ_ACCOUNT_INFO, pubkey)? {
            Some(info) => Ok(info),
            None => Err(ArchError::NotFound(format!(
                "Account not found for pubkey: {}",
                pubkey
            ))),
        }
    }

    /// Read account information for multiple public keys
    pub fn get_multiple_accounts(
        &self,
        pubkeys: Vec<Pubkey>,
    ) -> Result<Vec<Option<AccountInfoWithPubkey>>> {
        match self.call_method_with_params(GET_MULTIPLE_ACCOUNTS, pubkeys.clone())? {
            Some(info) => Ok(info),
            None => Err(ArchError::NotFound(format!(
                "Accounts not found for pubkeys: {:?}",
                pubkeys
            ))),
        }
    }

    /// Request an airdrop for a given public key
    pub fn request_airdrop(&self, pubkey: Pubkey) -> Result<ProcessedTransaction> {
        let result = self
            .process_result(self.post_data("request_airdrop", pubkey)?)?
            .ok_or(ArchError::RpcRequestFailed(
                "request_airdrop failed".to_string(),
            ))?;

        // Handle the result parsing with proper error handling
        let txid_str = result.as_str().ok_or_else(|| {
            ArchError::ParseError("Failed to get transaction ID as string".to_string())
        })?;

        let txid = Hash::from_str(txid_str)?;
        let processed_tx = self.wait_for_processed_transaction(&txid)?;
        Ok(processed_tx)
    }

    /// Create an account with lamports
    pub fn create_and_fund_account_with_faucet(
        &self,
        keypair: &Keypair,
        bitcoin_network: Network,
    ) -> Result<()> {
        let pubkey = Pubkey::from_slice(&keypair.x_only_public_key().0.serialize());

        if self.read_account_info(pubkey).is_ok() {
            let _processed_tx = self.request_airdrop(pubkey)?;
        } else {
            let result = self
                .process_result(self.post_data("create_account_with_faucet", pubkey)?)?
                .ok_or(ArchError::RpcRequestFailed(
                    "create_account_with_faucet failed".to_string(),
                ))?;
            let mut runtime_tx: RuntimeTransaction = serde_json::from_value(result)?;

            let message_hash = runtime_tx.message.hash();
            let signature = crate::Signature::from(sign_message_bip322(
                keypair,
                &message_hash,
                bitcoin_network,
            ));

            runtime_tx.signatures.push(signature);

            let result = self.send_transaction(runtime_tx)?;

            let _processed_tx = self.wait_for_processed_transaction(&result)?;
        }
        let account_info = self.read_account_info(pubkey)?;

        // assert_eq!(account_info.owner, Pubkey::system_program());
        assert!(account_info.lamports >= 1_000_000_000);

        Ok(())
    }

    /// Get a processed transaction by ID
    pub fn get_processed_transaction(&self, tx_id: &Hash) -> Result<Option<ProcessedTransaction>> {
        self.call_method_with_params(GET_PROCESSED_TRANSACTION, tx_id.to_string())
    }

    /// Waits for a transaction to be processed, polling until it reaches "Processed" or "Failed" status
    /// Will timeout after 60 seconds
    pub fn wait_for_processed_transaction(&self, tx_id: &Hash) -> Result<ProcessedTransaction> {
        let mut wait_time = 1;

        // First try to get the transaction, retry if null
        let mut tx = match self.get_processed_transaction(tx_id) {
            Ok(Some(tx)) => tx,
            Ok(None) => {
                // Transaction not found, start polling
                loop {
                    std::thread::sleep(Duration::from_secs(wait_time));
                    match self.get_processed_transaction(tx_id)? {
                        Some(tx) => break tx,
                        None => {
                            wait_time += 1;
                            if wait_time >= 60 {
                                return Err(ArchError::TimeoutError(
                                    "Failed to retrieve processed transaction after 60 seconds"
                                        .to_string(),
                                ));
                            }
                            continue;
                        }
                    }
                }
            }
            Err(e) => return Err(e),
        };

        // Now wait for the transaction to finish processing
        while !is_transaction_finalized(&tx) {
            std::thread::sleep(Duration::from_secs(wait_time));
            match self.get_processed_transaction(tx_id)? {
                Some(updated_tx) => {
                    tx = updated_tx;
                    if is_transaction_finalized(&tx) {
                        break;
                    }
                }
                None => {
                    return Err(ArchError::TransactionError(
                        "Transaction disappeared after being found".to_string(),
                    ));
                }
            }

            wait_time += 1;
            if wait_time >= 60 {
                return Err(ArchError::TimeoutError(
                    "Transaction did not reach final status after 60 seconds".to_string(),
                ));
            }
        }

        Ok(tx)
    }

    /// Waits for multiple transactions to be processed, showing progress with a progress bar
    /// Returns a vector of processed transactions in the same order as the input transaction IDs
    pub fn wait_for_processed_transactions(
        &self,
        tx_ids: Vec<Hash>,
    ) -> Result<Vec<ProcessedTransaction>> {
        let mut processed_transactions: Vec<ProcessedTransaction> =
            Vec::with_capacity(tx_ids.len());

        for tx_id in tx_ids {
            match self.wait_for_processed_transaction(&tx_id) {
                Ok(tx) => processed_transactions.push(tx),
                Err(e) => {
                    return Err(ArchError::TransactionError(format!(
                        "Failed to process transaction {}: {}",
                        tx_id, e
                    )))
                }
            }
        }

        Ok(processed_transactions)
    }

    /// Get the best block hash
    pub fn get_best_block_hash(&self) -> Result<Hash> {
        match self.call_method_raw(GET_BEST_BLOCK_HASH)? {
            Some(value) => {
                let hash_str = value.as_str().ok_or_else(|| {
                    ArchError::ParseError("Failed to get best block hash as string".to_string())
                })?;
                Ok(Hash::from_str(hash_str)?)
            }
            None => Err(ArchError::NotFound("Best block hash not found".to_string())),
        }
    }

    /// Get the best block hash
    pub fn get_best_finalized_block_hash(&self) -> Result<Hash> {
        match self.call_method_raw(GET_BEST_FINALIZED_BLOCK_HASH)? {
            Some(value) => {
                let hash_str = value.as_str().ok_or_else(|| {
                    ArchError::ParseError("Failed to get best block hash as string".to_string())
                })?;
                Ok(Hash::from_str(hash_str)?)
            }
            None => Err(ArchError::NotFound("Best block hash not found".to_string())),
        }
    }

    /// Get the block hash for a given height
    pub fn get_block_hash(&self, block_height: u64) -> Result<String> {
        match self.call_method_with_params_raw(GET_BLOCK_HASH, block_height)? {
            Some(value) => value.as_str().map(|s| s.to_string()).ok_or_else(|| {
                ArchError::ParseError("Failed to get block hash as string".to_string())
            }),
            None => Err(ArchError::NotFound(format!(
                "Block hash not found for height: {}",
                block_height
            ))),
        }
    }

    /// Get the current block count
    pub fn get_block_count(&self) -> Result<u64> {
        match self.call_method(GET_BLOCK_COUNT)? {
            Some(count) => Ok(count),
            None => Err(ArchError::NotFound("Block count not found".to_string())),
        }
    }

    /// Get block by hash with signatures only
    pub fn get_block_by_hash(&self, block_hash: &str) -> Result<Option<Block>> {
        // For signatures only, we can just pass the block hash directly
        self.call_method_with_params(GET_BLOCK, block_hash)
    }

    /// Get full block by hash with complete transaction details
    pub fn get_full_block_by_hash(&self, block_hash: &str) -> Result<Option<FullBlock>> {
        // Create parameters array with block_hash and full filter
        let params = vec![
            serde_json::to_value(block_hash)?,
            serde_json::to_value(BlockTransactionFilter::Full)?,
        ];

        // Process the response - first get the raw value
        match self.process_result(self.post_data(GET_BLOCK, params)?)? {
            Some(value) => {
                // Deserialize into a FullBlock
                let result = serde_json::from_value(value).map_err(|e| {
                    ArchError::ParseError(format!("Failed to deserialize FullBlock: {}", e))
                })?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    /// Get block by height with signatures only
    pub fn get_block_by_height(&self, block_height: u64) -> Result<Option<Block>> {
        // For signatures only, we can just pass the block hash directly
        self.call_method_with_params(GET_BLOCK_BY_HEIGHT, block_height)
    }

    /// Get full block by hash with complete transaction details
    pub fn get_full_block_by_height(&self, block_height: u64) -> Result<Option<FullBlock>> {
        // Create parameters array with block_hash and full filter
        let params = vec![
            serde_json::to_value(block_height)?,
            serde_json::to_value(BlockTransactionFilter::Full)?,
        ];

        // Process the response - first get the raw value
        match self.process_result(self.post_data(GET_BLOCK_BY_HEIGHT, params)?)? {
            Some(value) => {
                // Deserialize into a FullBlock
                let result = serde_json::from_value(value).map_err(|e| {
                    ArchError::ParseError(format!("Failed to deserialize FullBlock: {}", e))
                })?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    /// Get account address for a public key
    pub fn get_account_address(&self, pubkey: &Pubkey) -> Result<String> {
        match self.process_result(self.post_data(GET_ACCOUNT_ADDRESS, pubkey.serialize())?)? {
            Some(value) => value.as_str().map(|s| s.to_string()).ok_or_else(|| {
                ArchError::ParseError("Failed to get account address as string".to_string())
            }),
            None => Err(ArchError::NotFound(format!(
                "Account address not found for pubkey: {}",
                pubkey
            ))),
        }
    }

    /// Get program accounts for a given program ID
    pub fn get_program_accounts(
        &self,
        program_id: &Pubkey,
        filters: Option<Vec<AccountFilter>>,
    ) -> Result<Vec<ProgramAccount>> {
        // Format params as [program_id, filters]
        let params = json!([program_id.serialize(), filters]);
        match self.call_method_with_params(GET_PROGRAM_ACCOUNTS, params)? {
            Some(accounts) => Ok(accounts),
            None => Err(ArchError::NotFound(format!(
                "Program accounts not found for program ID: {}",
                program_id
            ))),
        }
    }

    /// Start distributed key generation
    pub fn start_dkg(&self) -> Result<()> {
        self.call_method_raw(START_DKG)?;
        Ok(())
    }

    /// Get the network pubkey from the network
    pub fn get_network_pubkey(&self) -> Result<String> {
        match self.call_method::<String>("get_network_pubkey")? {
            Some(key) => Ok(key),
            None => Err(ArchError::NotFound("Network pubkey not found".to_string())),
        }
    }

    /// Send a single transaction
    pub fn send_transaction(&self, transaction: RuntimeTransaction) -> Result<Hash> {
        match self.process_result(self.post_data(SEND_TRANSACTION, transaction)?)? {
            Some(value) => {
                let tx_id_str = value.as_str().ok_or_else(|| {
                    ArchError::ParseError("Failed to get transaction ID as string".to_string())
                })?;
                Ok(Hash::from_str(tx_id_str)?)
            }
            None => Err(ArchError::TransactionError(
                "Failed to send transaction".to_string(),
            )),
        }
    }

    /// Send multiple transactions
    pub fn send_transactions(&self, transactions: Vec<RuntimeTransaction>) -> Result<Vec<Hash>> {
        if transactions.len() > MAX_TX_BATCH_SIZE {
            return Err(ArchError::TransactionError(
                "Batch size exceeds maximum".to_string(),
            ));
        }

        match self.call_method_with_params::<Vec<RuntimeTransaction>, Vec<String>>(
            SEND_TRANSACTIONS,
            transactions,
        )? {
            Some(tx_ids) => {
                let mut parsed_tx_ids = Vec::new();
                for id in tx_ids {
                    let hash = Hash::from_str(&id)?;
                    parsed_tx_ids.push(hash);
                }
                Ok(parsed_tx_ids)
            }
            None => Err(ArchError::TransactionError(
                "Failed to send transactions".to_string(),
            )),
        }
    }

    /// Helper methods for RPC communication
    pub fn process_result(&self, response: String) -> Result<Option<Value>> {
        let result = from_str::<Value>(&response).map_err(|e| {
            ArchError::ParseError(format!(
                "Failed to parse JSON: {} - response: {}",
                e, response
            ))
        })?;

        let result = match result {
            Value::Object(object) => object,
            _ => {
                return Err(ArchError::ParseError(
                    "Unexpected JSON structure".to_string(),
                ))
            }
        };

        if let Some(err) = result.get("error") {
            if let Value::Object(err_obj) = err {
                if let (Some(Value::Number(code)), Some(Value::String(message))) =
                    (err_obj.get("code"), err_obj.get("message"))
                {
                    if code.as_i64() == Some(NOT_FOUND_CODE) {
                        return Ok(None);
                    }
                    return Err(ArchError::RpcRequestFailed(format!(
                        "Code: {}, Message: {}",
                        code, message
                    )));
                }
            }
            return Err(ArchError::RpcRequestFailed(format!("{:?}", err)));
        }

        Ok(Some(result["result"].clone()))
    }

    fn post(&self, method: &str) -> Result<String> {
        let client = reqwest::blocking::Client::new();
        match client
            .post(&self.url)
            .header("content-type", "application/json")
            .json(&json!({
                "jsonrpc": "2.0",
                "id": "curlycurl",
                "method": method,
            }))
            .send()
        {
            Ok(res) => match res.text() {
                Ok(text) => Ok(text),
                Err(e) => Err(ArchError::NetworkError(format!(
                    "Failed to read response text: {}",
                    e
                ))),
            },
            Err(e) => Err(ArchError::NetworkError(format!("Request failed: {}", e))),
        }
    }

    pub fn post_data<T: Serialize + std::fmt::Debug>(
        &self,
        method: &str,
        params: T,
    ) -> Result<String> {
        let client = reqwest::blocking::Client::new();
        match client
            .post(&self.url)
            .header("content-type", "application/json")
            .json(&json!({
                "jsonrpc": "2.0",
                "id": "curlycurl",
                "method": method,
                "params": params,
            }))
            .send()
        {
            Ok(res) => match res.text() {
                Ok(text) => Ok(text),
                Err(e) => Err(ArchError::NetworkError(format!(
                    "Failed to get response text: {}",
                    e
                ))),
            },
            Err(e) => Err(ArchError::NetworkError(format!("Request failed: {}", e))),
        }
    }
}

/// Helper function to check if a transaction has reached a final status
fn is_transaction_finalized(tx: &ProcessedTransaction) -> bool {
    matches!(&tx.status, Status::Processed | Status::Failed(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arch_program::pubkey::Pubkey;
    use arch_program::hash::Hash;
    use arch_program::{account::MIN_ACCOUNT_LAMPORTS, sanitized::ArchMessage};
    use mockito::Server;

    // Helper to create a test client with the mockito server
    fn get_test_client(server: &Server) -> ArchRpcClient {
        ArchRpcClient::new(&server.url())
    }

    // Helper to create a mock RPC response
    fn mock_rpc_response(server: &mut Server, method: &str, result: Value) -> mockito::Mock {
        server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                json!({
                    "jsonrpc": "2.0",
                    "id": "curlycurl",
                    "result": result
                })
                .to_string(),
            )
            .match_body(mockito::Matcher::Json(json!({
                "jsonrpc": "2.0",
                "id": "curlycurl",
                "method": method
            })))
            .create()
    }

    // Helper to create a mock RPC response with params
    fn mock_rpc_response_with_params<T: Serialize>(
        server: &mut Server,
        method: &str,
        params: T,
        result: Value,
    ) -> mockito::Mock {
        server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                json!({
                    "jsonrpc": "2.0",
                    "id": "curlycurl",
                    "result": result
                })
                .to_string(),
            )
            .match_body(mockito::Matcher::Json(json!({
                "jsonrpc": "2.0",
                "id": "curlycurl",
                "method": method,
                "params": params
            })))
            .create()
    }

    // Helper to create a mock RPC error response
    fn mock_rpc_error(
        server: &mut Server,
        method: &str,
        error_code: i64,
        error_message: &str,
    ) -> mockito::Mock {
        server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                json!({
                    "jsonrpc": "2.0",
                    "id": "curlycurl",
                    "error": {
                        "code": error_code,
                        "message": error_message
                    }
                })
                .to_string(),
            )
            .match_body(mockito::Matcher::Json(json!({
                "jsonrpc": "2.0",
                "id": "curlycurl",
                "method": method
            })))
            .create()
    }

    #[test]
    fn test_get_best_block_hash() {
        let mut server = Server::new();

        let expected_hash = Hash::from([1; 32]);
        let mock = mock_rpc_response(
            &mut server,
            GET_BEST_BLOCK_HASH,
            json!(expected_hash.to_string()),
        );

        let client = get_test_client(&server);
        let result = client.get_best_block_hash().unwrap();

        assert_eq!(result, expected_hash);
        mock.assert();
    }

    #[test]
    fn test_get_block_count() {
        let mut server = Server::new();
        let mock = mock_rpc_response(&mut server, GET_BLOCK_COUNT, json!(123456));

        let client = get_test_client(&server);
        let result = client.get_block_count().unwrap();

        assert_eq!(result, 123456);
        mock.assert();
    }

    #[test]
    fn test_read_account_info() {
        let mut server = Server::new();
        let pubkey = Pubkey::new_unique();

        // Create account info according to the actual struct definition
        let account_info = AccountInfo {
            lamports: MIN_ACCOUNT_LAMPORTS,
            owner: Pubkey::new_unique(),
            data: vec![1, 2, 3, 4],
            utxo: "utxo123".to_string(),
            is_executable: false,
        };

        let mock = mock_rpc_response_with_params(
            &mut server,
            READ_ACCOUNT_INFO,
            pubkey,
            serde_json::to_value(account_info.clone()).unwrap(),
        );

        let client = get_test_client(&server);
        let result = client.read_account_info(pubkey).unwrap();

        assert_eq!(result.owner, account_info.owner);
        assert_eq!(result.data, account_info.data);
        assert_eq!(result.utxo, account_info.utxo);
        assert_eq!(result.is_executable, account_info.is_executable);
        mock.assert();
    }

    #[test]
    fn test_not_found_error() {
        let mut server = Server::new();
        let mock = mock_rpc_error(
            &mut server,
            GET_BEST_BLOCK_HASH,
            NOT_FOUND_CODE,
            "Not found",
        );

        let client = get_test_client(&server);
        let result = client.call_method_raw(GET_BEST_BLOCK_HASH).unwrap();

        assert!(result.is_none());
        mock.assert();
    }

    #[test]
    fn test_is_transaction_finalized_function() {
        use crate::types::RollbackStatus;

        // Create a RuntimeTransaction for testing
        let rt_tx = RuntimeTransaction {
            version: 0,
            signatures: Vec::new(),
            message: ArchMessage::new(&[], None, Hash::from([0; 32])),
        };

        // Test all status variants
        let processed_tx = ProcessedTransaction {
            runtime_transaction: rt_tx.clone(),
            status: Status::Processed,
            bitcoin_txid: None,
            logs: Vec::new(),
            rollback_status: RollbackStatus::NotRolledback,
        };
        assert!(is_transaction_finalized(&processed_tx));

        let failed_tx = ProcessedTransaction {
            runtime_transaction: rt_tx.clone(),
            status: Status::Failed("error".to_string()),
            bitcoin_txid: None,
            logs: Vec::new(),
            rollback_status: RollbackStatus::NotRolledback,
        };
        assert!(is_transaction_finalized(&failed_tx));

        let queued_tx = ProcessedTransaction {
            runtime_transaction: rt_tx.clone(),
            status: Status::Queued,
            bitcoin_txid: None,
            logs: Vec::new(),
            rollback_status: RollbackStatus::NotRolledback,
        };
        assert!(!is_transaction_finalized(&queued_tx));
    }

    #[test]
    fn test_send_transaction() {
        let mut server = Server::new();

        // Create a minimal valid RuntimeTransaction for the test
        let tx = RuntimeTransaction {
            version: 0,
            signatures: Vec::new(),
            message: ArchMessage::new(&[], None, Hash::from([0; 32])),
        };

        let expected_tx_id = Hash::from([0; 32]);

        let mock = mock_rpc_response_with_params(
            &mut server,
            SEND_TRANSACTION,
            tx.clone(),
            json!(expected_tx_id.to_string()),
        );

        let client = get_test_client(&server);
        let result = client.send_transaction(tx).unwrap();

        assert_eq!(result, expected_tx_id);
        mock.assert();
    }

    // Additional test for get_program_accounts
    #[test]
    fn test_get_program_accounts() {
        let mut server = Server::new();
        let program_id = Pubkey::new_unique();
        let filters = None;

        // Create some program accounts for the response
        let account_info = AccountInfo {
            lamports: MIN_ACCOUNT_LAMPORTS,
            owner: program_id,
            data: vec![1, 2, 3, 4],
            utxo: "utxo123".to_string(),
            is_executable: false,
        };

        let program_account = ProgramAccount {
            pubkey: Pubkey::new_unique(),
            account: account_info,
        };

        let mock = mock_rpc_response_with_params(
            &mut server,
            GET_PROGRAM_ACCOUNTS,
            json!([program_id.serialize(), filters]),
            json!([program_account]),
        );

        let client = get_test_client(&server);
        let result = client.get_program_accounts(&program_id, filters).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].pubkey, program_account.pubkey);
        assert_eq!(result[0].account.data, program_account.account.data);
        mock.assert();
    }

    #[test]
    fn test_get_block_hash() {
        let mut server = Server::new();
        let block_height = 12345u64;
        let expected_hash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f";

        let mock = mock_rpc_response_with_params(
            &mut server,
            GET_BLOCK_HASH,
            block_height,
            json!(expected_hash),
        );

        let client = get_test_client(&server);
        let result = client.get_block_hash(block_height).unwrap();

        assert_eq!(result, expected_hash);
        mock.assert();
    }

    #[test]
    fn test_get_block() {
        let mut server = Server::new();
        let block_hash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f";

        // Create a sample block for the response
        let block = Block {
            transactions: vec![
                Hash::from_str("0000000000000000000000000000000000000000000000000000000000000001")
                    .unwrap(),
                Hash::from_str("0000000000000000000000000000000000000000000000000000000000000002")
                    .unwrap(),
            ],
            previous_block_hash: Hash::from_str(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
            timestamp: 1630000000,
            block_height: 100,
            bitcoin_block_height: 100,
        };

        let mock = mock_rpc_response_with_params(
            &mut server,
            GET_BLOCK,
            block_hash,
            serde_json::to_value(block.clone()).unwrap(),
        );

        let client = get_test_client(&server);
        let result = client.get_block_by_hash(block_hash).unwrap();

        assert!(result.is_some());
        let returned_block = result.unwrap();
        assert_eq!(returned_block.transactions, block.transactions);
        assert_eq!(
            returned_block.bitcoin_block_height,
            block.bitcoin_block_height
        );
        mock.assert();
    }

    #[test]
    fn test_get_processed_transaction() {
        let mut server = Server::new();
        let tx_id = Hash::from([0; 32]);

        use crate::types::RollbackStatus;

        // Create a sample processed transaction
        let rt_tx = RuntimeTransaction {
            version: 0,
            signatures: Vec::new(),
            message: ArchMessage::new(&[], None, Hash::from([0; 32])),
        };

        let processed_tx = ProcessedTransaction {
            runtime_transaction: rt_tx.clone(),
            status: Status::Processed,
            bitcoin_txid: None,
            logs: vec!["Log entry 1".to_string(), "Log entry 2".to_string()],
            rollback_status: RollbackStatus::NotRolledback,
        };

        let mock = mock_rpc_response_with_params(
            &mut server,
            GET_PROCESSED_TRANSACTION,
            tx_id.to_string(),
            serde_json::to_value(processed_tx.clone()).unwrap(),
        );

        let client = get_test_client(&server);
        let result = client.get_processed_transaction(&tx_id).unwrap();

        assert!(result.is_some());
        let returned_tx = result.unwrap();
        assert_eq!(returned_tx.status, processed_tx.status);
        assert_eq!(returned_tx.logs, processed_tx.logs);
        mock.assert();
    }

    #[test]
    fn test_send_transactions() {
        let mut server = Server::new();

        // Create multiple transactions
        let tx1 = RuntimeTransaction {
            version: 0,
            signatures: Vec::new(),
            message: ArchMessage::new(&[], None, Hash::from([0; 32])),
        };

        let tx2 = RuntimeTransaction {
            version: 1,
            signatures: Vec::new(),
            message: ArchMessage::new(&[], None, Hash::from([0; 32])),
        };

        let transactions = vec![tx1, tx2];
        let expected_tx_ids = vec![
            Hash::from([
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1,
            ]),
            Hash::from([
                2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                2, 2, 2, 2,
            ]),
        ];

        let mock = mock_rpc_response_with_params(
            &mut server,
            SEND_TRANSACTIONS,
            transactions.clone(),
            json!(expected_tx_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<String>>()),
        );

        let client = get_test_client(&server);
        let result = client.send_transactions(transactions).unwrap();

        assert_eq!(result, expected_tx_ids);
        mock.assert();
    }

    #[test]
    fn test_start_dkg() {
        let mut server = Server::new();
        let mock = mock_rpc_response(&mut server, START_DKG, json!(null));

        let client = get_test_client(&server);
        let result = client.start_dkg();

        assert!(result.is_ok());
        mock.assert();
    }

    #[test]
    fn test_get_network_pubkey() {
        let mut server = Server::new();
        let expected_key = "0000000000000000000000000000000000000000000000000000000000000000";
        let mock = mock_rpc_response(&mut server, "get_network_pubkey", json!(expected_key));

        let client = get_test_client(&server);
        let result = client.get_network_pubkey().unwrap();

        assert_eq!(result, expected_key);
        mock.assert();
    }

    #[test]
    fn test_call_method_basic() {
        let mut server = Server::new();

        // Test a basic string return type
        let mock = mock_rpc_response(&mut server, "test_method", json!("test_result"));

        let client = get_test_client(&server);
        let result: Option<String> = client.call_method("test_method").unwrap();

        assert_eq!(result, Some("test_result".to_string()));
        mock.assert();
    }

    #[test]
    fn test_call_method_complex_type() {
        let mut server = Server::new();

        // Test a more complex return type (using AccountInfo as an example)
        let account_info = AccountInfo {
            lamports: MIN_ACCOUNT_LAMPORTS,
            owner: Pubkey::new_unique(),
            data: vec![1, 2, 3, 4],
            utxo: "utxo123".to_string(),
            is_executable: false,
        };

        let mock = mock_rpc_response(
            &mut server,
            "get_account_info",
            serde_json::to_value(account_info.clone()).unwrap(),
        );

        let client = get_test_client(&server);
        let result: Option<AccountInfo> = client.call_method("get_account_info").unwrap();

        assert!(result.is_some());
        let returned_info = result.unwrap();
        assert_eq!(returned_info.owner, account_info.owner);
        assert_eq!(returned_info.data, account_info.data);
        mock.assert();
    }

    #[test]
    fn test_rpc_error_handling() {
        let mut server = Server::new();

        // Test handling of a non-404 error code
        let error_code = 500;
        let error_message = "Internal server error";

        let mock = mock_rpc_error(&mut server, "test_method", error_code, error_message);

        let client = get_test_client(&server);
        let result = client.call_method_raw("test_method");

        assert!(result.is_err());
        if let Err(ArchError::RpcRequestFailed(message)) = result {
            assert!(message.contains(&error_code.to_string()));
            assert!(message.contains(error_message));
        } else {
            panic!("Expected RpcRequestFailed error");
        }

        mock.assert();
    }

    #[test]
    fn test_get_multiple_accounts() {
        let mut server = Server::new();

        // Create test pubkeys
        let pubkey1 = Pubkey::new_unique();
        let pubkey2 = Pubkey::new_unique();
        let pubkeys = vec![pubkey1, pubkey2];

        // Create account info for responses
        let account_info1 = AccountInfo {
            lamports: MIN_ACCOUNT_LAMPORTS,
            owner: Pubkey::new_unique(),
            data: vec![1, 2, 3, 4],
            utxo: "utxo123".to_string(),
            is_executable: false,
        };

        let account_info2 = AccountInfo {
            lamports: MIN_ACCOUNT_LAMPORTS,
            owner: Pubkey::new_unique(),
            data: vec![5, 6, 7, 8],
            utxo: "utxo456".to_string(),
            is_executable: true,
        };

        // Updated to match actual struct definition
        let account_with_pubkey1 = AccountInfoWithPubkey {
            key: pubkey1,
            lamports: MIN_ACCOUNT_LAMPORTS,
            owner: account_info1.owner,
            data: account_info1.data.clone(),
            utxo: account_info1.utxo.clone(),
            is_executable: account_info1.is_executable,
        };

        // Updated to match actual struct definition
        let account_with_pubkey2 = AccountInfoWithPubkey {
            key: pubkey2,
            lamports: MIN_ACCOUNT_LAMPORTS,
            owner: account_info2.owner,
            data: account_info2.data.clone(),
            utxo: account_info2.utxo.clone(),
            is_executable: account_info2.is_executable,
        };

        let expected_accounts = vec![
            Some(account_with_pubkey1.clone()),
            Some(account_with_pubkey2.clone()),
        ];

        let mock = mock_rpc_response_with_params(
            &mut server,
            GET_MULTIPLE_ACCOUNTS,
            pubkeys.clone(),
            serde_json::to_value(expected_accounts.clone()).unwrap(),
        );

        let client = get_test_client(&server);
        let result = client.get_multiple_accounts(pubkeys).unwrap();

        assert_eq!(result.len(), 2);
        // Updated assertions to use the correct field names
        assert_eq!(result[0].as_ref().unwrap().key, pubkey1);
        assert_eq!(result[0].as_ref().unwrap().data, account_info1.data);
        assert_eq!(result[1].as_ref().unwrap().key, pubkey2);
        assert_eq!(
            result[1].as_ref().unwrap().is_executable,
            account_info2.is_executable
        );

        mock.assert();
    }

    #[test]
    fn test_get_full_block() {
        let mut server = Server::new();
        let block_hash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f";

        // Create a sample full block for the response
        let full_block = FullBlock {
            transactions: vec![], // Simplified for test purposes
            previous_block_hash: Hash::from_str(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
            timestamp: 1630000000,
            block_height: 100,
            bitcoin_block_height: 100,
        };

        // Mock response with the correct parameters (block_hash and BlockTransactionFilter::Full)
        let params = vec![
            serde_json::to_value(block_hash).unwrap(),
            serde_json::to_value(BlockTransactionFilter::Full).unwrap(),
        ];

        let mock = mock_rpc_response_with_params(
            &mut server,
            GET_BLOCK,
            params,
            serde_json::to_value(full_block.clone()).unwrap(),
        );

        let client = get_test_client(&server);
        let result = client.get_full_block_by_hash(block_hash).unwrap();

        assert!(result.is_some());
        let returned_block = result.unwrap();
        assert_eq!(returned_block.timestamp, full_block.timestamp);
        assert_eq!(
            returned_block.previous_block_hash,
            full_block.previous_block_hash
        );
        assert_eq!(
            returned_block.bitcoin_block_height,
            full_block.bitcoin_block_height
        );

        mock.assert();
    }

    #[test]
    fn test_get_block_by_height() {
        let mut server = Server::new();
        let block_height = 12345u64;

        // Create a sample block for the response
        let block = Block {
            transactions: vec![
                Hash::from_str("0000000000000000000000000000000000000000000000000000000000000001")
                    .unwrap(),
                Hash::from_str("0000000000000000000000000000000000000000000000000000000000000002")
                    .unwrap(),
            ],
            previous_block_hash: Hash::from_str(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
            timestamp: 1630000000,
            block_height: 100,
            bitcoin_block_height: 100,
        };

        let mock = mock_rpc_response_with_params(
            &mut server,
            GET_BLOCK_BY_HEIGHT,
            block_height,
            serde_json::to_value(block.clone()).unwrap(),
        );

        let client = get_test_client(&server);
        let result = client.get_block_by_height(block_height).unwrap();

        assert!(result.is_some());
        let returned_block = result.unwrap();
        assert_eq!(returned_block.transactions, block.transactions);
        assert_eq!(
            returned_block.bitcoin_block_height,
            block.bitcoin_block_height
        );
        mock.assert();
    }

    #[test]
    fn test_get_full_block_by_height() {
        let mut server = Server::new();
        let block_height = 12345u64;

        // Create a sample full block for the response
        let full_block = FullBlock {
            transactions: vec![], // Simplified for test purposes
            previous_block_hash: Hash::from_str(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
            timestamp: 1630000000,
            block_height,
            bitcoin_block_height: 100,
        };

        // Mock response with the correct parameters (block_height and BlockTransactionFilter::Full)
        let params = vec![
            serde_json::to_value(block_height).unwrap(),
            serde_json::to_value(BlockTransactionFilter::Full).unwrap(),
        ];

        let mock = mock_rpc_response_with_params(
            &mut server,
            GET_BLOCK_BY_HEIGHT,
            params,
            serde_json::to_value(full_block.clone()).unwrap(),
        );

        let client = get_test_client(&server);
        let result = client.get_full_block_by_height(block_height).unwrap();

        assert!(result.is_some());
        let returned_block = result.unwrap();
        assert_eq!(returned_block.timestamp, full_block.timestamp);
        assert_eq!(
            returned_block.previous_block_hash,
            full_block.previous_block_hash
        );
        assert_eq!(
            returned_block.bitcoin_block_height,
            full_block.bitcoin_block_height
        );

        mock.assert();
    }
}
