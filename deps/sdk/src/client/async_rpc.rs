use crate::arch_program::pubkey::Pubkey;
use crate::client::error::{ArchError, Result};
use crate::{
    sign_message_bip322, AccountInfoWithPubkey, BlockTransactionFilter, FullBlock, NOT_FOUND_CODE,
};
use bitcoin::key::Keypair;
use bitcoin::Network;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
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
const GET_PROCESSED_TRANSACTION: &str = "get_processed_transaction";
const GET_ACCOUNT_ADDRESS: &str = "get_account_address";
const GET_PROGRAM_ACCOUNTS: &str = "get_program_accounts";
const START_DKG: &str = "start_dkg";

/// AsyncArchRpcClient provides a simple interface for making asynchronous RPC calls to the Arch blockchain
#[derive(Clone)]
pub struct AsyncArchRpcClient {
    url: String,
    client: reqwest::Client,
}

impl AsyncArchRpcClient {
    /// Create a new AsyncArchRpcClient with the specified URL
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            client: reqwest::Client::new(),
        }
    }

    /// Make a raw RPC call with no parameters and parse the result
    /// Returns None if the item was not found (404)
    pub async fn call_method<R: DeserializeOwned>(&self, method: &str) -> Result<Option<R>> {
        match self.process_result(self.post(method).await?).await? {
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
    pub async fn call_method_with_params<T: Serialize + std::fmt::Debug, R: DeserializeOwned>(
        &self,
        method: &str,
        params: T,
    ) -> Result<Option<R>> {
        match self
            .process_result(self.post_data(method, params).await?)
            .await?
        {
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
    pub async fn call_method_raw(&self, method: &str) -> Result<Option<Value>> {
        self.process_result(self.post(method).await?).await
    }

    /// Get raw value from a method call with parameters
    /// Returns None if the item was not found (404)
    pub async fn call_method_with_params_raw<T: Serialize + std::fmt::Debug>(
        &self,
        method: &str,
        params: T,
    ) -> Result<Option<Value>> {
        self.process_result(self.post_data(method, params).await?)
            .await
    }

    /// Read account information for the specified public key
    pub async fn read_account_info(&self, pubkey: Pubkey) -> Result<AccountInfo> {
        match self
            .call_method_with_params(READ_ACCOUNT_INFO, pubkey)
            .await?
        {
            Some(info) => Ok(info),
            None => Err(ArchError::NotFound(format!(
                "Account not found for pubkey: {}",
                pubkey
            ))),
        }
    }

    /// Read account information for multiple public keys
    pub async fn get_multiple_accounts(
        &self,
        pubkeys: Vec<Pubkey>,
    ) -> Result<Vec<Option<AccountInfoWithPubkey>>> {
        match self
            .call_method_with_params(GET_MULTIPLE_ACCOUNTS, pubkeys.clone())
            .await?
        {
            Some(info) => Ok(info),
            None => Err(ArchError::NotFound(format!(
                "Accounts not found for pubkeys: {:?}",
                pubkeys
            ))),
        }
    }

    /// Get a processed transaction by ID
    pub async fn get_processed_transaction(
        &self,
        tx_id: &str,
    ) -> Result<Option<ProcessedTransaction>> {
        self.call_method_with_params(GET_PROCESSED_TRANSACTION, tx_id)
            .await
    }

    /// Waits for a transaction to be processed, polling until it reaches "Processed" or "Failed" status
    /// Will timeout after 60 seconds
    pub async fn wait_for_processed_transaction(
        &self,
        tx_id: &str,
    ) -> Result<ProcessedTransaction> {
        let mut wait_time = 1;

        // First try to get the transaction, retry if null
        let mut tx = match self.get_processed_transaction(tx_id).await {
            Ok(Some(tx)) => tx,
            Ok(None) => {
                // Transaction not found, start polling
                loop {
                    tokio::time::sleep(Duration::from_secs(wait_time)).await;
                    match self.get_processed_transaction(tx_id).await? {
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
            tokio::time::sleep(Duration::from_secs(wait_time)).await;
            match self.get_processed_transaction(tx_id).await? {
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

    /// Waits for multiple transactions to be processed
    /// Returns a vector of processed transactions in the same order as the input transaction IDs
    pub async fn wait_for_processed_transactions(
        &self,
        tx_ids: Vec<String>,
    ) -> Result<Vec<ProcessedTransaction>> {
        let mut processed_transactions: Vec<ProcessedTransaction> =
            Vec::with_capacity(tx_ids.len());

        for tx_id in tx_ids {
            match self.wait_for_processed_transaction(&tx_id).await {
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
    pub async fn get_best_block_hash(&self) -> Result<String> {
        match self.call_method_raw(GET_BEST_BLOCK_HASH).await? {
            Some(value) => value.as_str().map(|s| s.to_string()).ok_or_else(|| {
                ArchError::ParseError("Failed to get best block hash as string".to_string())
            }),
            None => Err(ArchError::NotFound("Best block hash not found".to_string())),
        }
    }

    /// Get the block hash for a given height
    pub async fn get_block_hash(&self, block_height: u64) -> Result<String> {
        match self
            .call_method_with_params_raw(GET_BLOCK_HASH, block_height)
            .await?
        {
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
    pub async fn get_block_count(&self) -> Result<u64> {
        match self.call_method(GET_BLOCK_COUNT).await? {
            Some(count) => Ok(count),
            None => Err(ArchError::NotFound("Block count not found".to_string())),
        }
    }

    /// Get block by hash with signatures only
    pub async fn get_block_by_hash(&self, block_hash: &str) -> Result<Option<Block>> {
        // For signatures only, we can just pass the block hash directly
        self.call_method_with_params(GET_BLOCK, block_hash).await
    }

    /// Get full block by hash with complete transaction details
    pub async fn get_full_block_by_hash(&self, block_hash: &str) -> Result<Option<FullBlock>> {
        // Create parameters array with block_hash and full filter
        let params = vec![
            serde_json::to_value(block_hash)?,
            serde_json::to_value(BlockTransactionFilter::Full)?,
        ];

        // Process the response - first get the raw value
        match self
            .process_result(self.post_data(GET_BLOCK, params).await?)
            .await?
        {
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
    pub async fn get_block_by_height(&self, block_height: u64) -> Result<Option<Block>> {
        // For signatures only, we can just pass the block hash directly
        self.call_method_with_params(GET_BLOCK_BY_HEIGHT, block_height)
            .await
    }

    /// Get full block by height with complete transaction details
    pub async fn get_full_block_by_height(&self, block_height: u64) -> Result<Option<FullBlock>> {
        // Create parameters array with block_height and full filter
        let params = vec![
            serde_json::to_value(block_height)?,
            serde_json::to_value(BlockTransactionFilter::Full)?,
        ];

        // Process the response - first get the raw value
        match self
            .process_result(self.post_data(GET_BLOCK_BY_HEIGHT, params).await?)
            .await?
        {
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
    pub async fn get_account_address(&self, pubkey: &Pubkey) -> Result<String> {
        match self
            .process_result(
                self.post_data(GET_ACCOUNT_ADDRESS, pubkey.serialize())
                    .await?,
            )
            .await?
        {
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
    pub async fn get_program_accounts(
        &self,
        program_id: &Pubkey,
        filters: Option<Vec<AccountFilter>>,
    ) -> Result<Vec<ProgramAccount>> {
        // Format params as [program_id, filters]
        let params = json!([program_id.serialize(), filters]);
        match self
            .call_method_with_params(GET_PROGRAM_ACCOUNTS, params)
            .await?
        {
            Some(accounts) => Ok(accounts),
            None => Err(ArchError::NotFound(format!(
                "Program accounts not found for program ID: {}",
                program_id
            ))),
        }
    }

    /// Start distributed key generation
    pub async fn start_dkg(&self) -> Result<()> {
        self.call_method_raw(START_DKG).await?;
        Ok(())
    }

    /// Send a single transaction
    pub async fn send_transaction(&self, transaction: RuntimeTransaction) -> Result<String> {
        match self
            .process_result(self.post_data(SEND_TRANSACTION, transaction).await?)
            .await?
        {
            Some(value) => value.as_str().map(|s| s.to_string()).ok_or_else(|| {
                ArchError::ParseError("Failed to get transaction ID as string".to_string())
            }),
            None => Err(ArchError::TransactionError(
                "Failed to send transaction".to_string(),
            )),
        }
    }

    /// Send multiple transactions
    pub async fn send_transactions(
        &self,
        transactions: Vec<RuntimeTransaction>,
    ) -> Result<Vec<String>> {
        match self
            .call_method_with_params(SEND_TRANSACTIONS, transactions)
            .await?
        {
            Some(tx_ids) => Ok(tx_ids),
            None => Err(ArchError::TransactionError(
                "Failed to send transactions".to_string(),
            )),
        }
    }

    /// Helper methods for RPC communication
    async fn process_result(&self, response: String) -> Result<Option<Value>> {
        let result = serde_json::from_str::<Value>(&response)
            .map_err(|e| ArchError::ParseError(format!("Failed to parse JSON: {}", e)))?;

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

    async fn post(&self, method: &str) -> Result<String> {
        match self
            .client
            .post(&self.url)
            .header("content-type", "application/json")
            .json(&json!({
                "jsonrpc": "2.0",
                "id": "curlycurl",
                "method": method,
            }))
            .send()
            .await
        {
            Ok(res) => match res.text().await {
                Ok(text) => Ok(text),
                Err(e) => Err(ArchError::NetworkError(format!(
                    "Failed to read response text: {}",
                    e
                ))),
            },
            Err(e) => Err(ArchError::NetworkError(format!("Request failed: {}", e))),
        }
    }

    async fn post_data<T: Serialize + std::fmt::Debug>(
        &self,
        method: &str,
        params: T,
    ) -> Result<String> {
        match self
            .client
            .post(&self.url)
            .header("content-type", "application/json")
            .json(&json!({
                "jsonrpc": "2.0",
                "id": "curlycurl",
                "method": method,
                "params": params,
            }))
            .send()
            .await
        {
            Ok(res) => match res.text().await {
                Ok(text) => Ok(text),
                Err(e) => Err(ArchError::NetworkError(format!(
                    "Failed to get response text: {}",
                    e
                ))),
            },
            Err(e) => Err(ArchError::NetworkError(format!("Request failed: {}", e))),
        }
    }

    /// Request an airdrop for a given key
    pub async fn request_airdrop(&self, pubkey: Pubkey) -> Result<ProcessedTransaction> {
        let result = self
            .process_result(self.post_data("request_airdrop", pubkey).await?)
            .await?
            .ok_or(ArchError::RpcRequestFailed(
                "request_airdrop failed".to_string(),
            ))?;

        // Handle the result parsing with proper error handling
        let txid_str = result.as_str().ok_or_else(|| {
            ArchError::ParseError("Failed to get transaction ID as string".to_string())
        })?;

        let processed_tx = self.wait_for_processed_transaction(txid_str).await?;

        Ok(processed_tx)
    }

    /// Create an account with lamports
    pub async fn create_and_fund_account_with_faucet(
        &self,
        keypair: &Keypair,
        bitcoin_network: Network,
    ) -> Result<()> {
        let pubkey = Pubkey::from_slice(&keypair.x_only_public_key().0.serialize());

        if self.read_account_info(pubkey).await.is_ok() {
            let _processed_tx = self.request_airdrop(pubkey).await?;
        } else {
            let result = self
                .process_result(self.post_data("create_account_with_faucet", pubkey).await?)
                .await?
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

            let result = self.send_transaction(runtime_tx).await?;

            let _processed_tx = self.wait_for_processed_transaction(&result).await?;
        }

        let account_info = self.read_account_info(pubkey).await?;

        // assert_eq!(account_info.owner, Pubkey::system_program());
        assert!(account_info.lamports >= 1_000_000_000);

        Ok(())
    }
}

/// Helper function to check if a transaction has reached a final status
fn is_transaction_finalized(tx: &ProcessedTransaction) -> bool {
    matches!(&tx.status, Status::Processed | Status::Failed(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arch_program::hash::Hash;
    use arch_program::{account::MIN_ACCOUNT_LAMPORTS, sanitized::ArchMessage};
    use mockito::Server;

    // Helper to create a test client with the mockito server
    async fn get_test_client(server: &Server) -> AsyncArchRpcClient {
        AsyncArchRpcClient::new(&server.url())
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

    #[tokio::test]
    async fn test_get_best_block_hash() {
        let mut server = Server::new_async().await;
        let mock = mock_rpc_response(&mut server, GET_BEST_BLOCK_HASH, json!("0123456789abcdef"));

        let client = get_test_client(&server).await;
        let result = client.get_best_block_hash().await.unwrap();

        assert_eq!(result, "0123456789abcdef");
        mock.assert();
    }

    #[tokio::test]
    async fn test_get_block_count() {
        let mut server = Server::new_async().await;
        let mock = mock_rpc_response(&mut server, GET_BLOCK_COUNT, json!(123456));

        let client = get_test_client(&server).await;
        let result = client.get_block_count().await.unwrap();

        assert_eq!(result, 123456);
        mock.assert();
    }

    #[tokio::test]
    async fn test_read_account_info() {
        let mut server = Server::new_async().await;
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

        let client = get_test_client(&server).await;
        let result = client.read_account_info(pubkey).await.unwrap();

        assert_eq!(result.owner, account_info.owner);
        assert_eq!(result.data, account_info.data);
        assert_eq!(result.utxo, account_info.utxo);
        assert_eq!(result.is_executable, account_info.is_executable);
        mock.assert();
    }

    #[tokio::test]
    async fn test_not_found_error() {
        let mut server = Server::new_async().await;
        let mock = mock_rpc_error(
            &mut server,
            GET_BEST_BLOCK_HASH,
            NOT_FOUND_CODE,
            "Not found",
        );

        let client = get_test_client(&server).await;
        let result = client.call_method_raw(GET_BEST_BLOCK_HASH).await.unwrap();

        assert!(result.is_none());
        mock.assert();
    }

    #[tokio::test]
    async fn test_send_transaction() {
        let mut server = Server::new_async().await;

        // Create a minimal valid RuntimeTransaction for the test
        let tx = RuntimeTransaction {
            version: 0,
            signatures: Vec::new(),
            message: ArchMessage::new(&[], None, Hash::from([0; 32])),
        };

        let mock = mock_rpc_response_with_params(
            &mut server,
            SEND_TRANSACTION,
            tx.clone(),
            json!("tx_id_12345"),
        );

        let client = get_test_client(&server).await;
        let result = client.send_transaction(tx).await.unwrap();

        assert_eq!(result, "tx_id_12345");
        mock.assert();
    }

    #[tokio::test]
    async fn test_get_program_accounts() {
        let mut server = Server::new_async().await;
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

        let client = get_test_client(&server).await;
        let result = client
            .get_program_accounts(&program_id, filters)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].pubkey, program_account.pubkey);
        assert_eq!(result[0].account.data, program_account.account.data);
        mock.assert();
    }
}
