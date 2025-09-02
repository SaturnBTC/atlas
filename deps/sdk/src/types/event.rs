use std::{collections::HashMap, fmt};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::Status;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum EventTopic {
    #[serde(rename = "block")]
    Block,
    #[serde(rename = "transaction")]
    Transaction,
    #[serde(rename = "account_update")]
    AccountUpdate,
    #[serde(rename = "rolledback_transactions")]
    RolledbackTransactions,
    #[serde(rename = "reapplied_transactions")]
    ReappliedTransactions,
    #[serde(rename = "dkg")]
    DKG,
}

impl fmt::Display for EventTopic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EventTopic::Block => write!(f, "block"),
            EventTopic::Transaction => write!(f, "transaction"),
            EventTopic::AccountUpdate => write!(f, "account_update"),
            EventTopic::RolledbackTransactions => write!(f, "rolledback_transactions"),
            EventTopic::ReappliedTransactions => write!(f, "reapplied_transactions"),
            EventTopic::DKG => write!(f, "dkg"),
        }
    }
}

/// The main Event enum that represents all possible events in the system
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "topic", content = "data")]
pub enum Event {
    /// A new block has been added to the blockchain
    #[serde(rename = "block")]
    Block(BlockEvent),
    /// A transaction was processed
    #[serde(rename = "transaction")]
    Transaction(TransactionEvent),
    /// An account was updated
    #[serde(rename = "account_update")]
    AccountUpdate(AccountUpdateEvent),
    /// A transaction was rolled back
    #[serde(rename = "rolledback_transactions")]
    RolledbackTransactions(RolledbackTransactionsEvent),
    /// A transaction was reapplied
    #[serde(rename = "reapplied_transactions")]
    ReappliedTransactions(ReappliedTransactionsEvent),
    /// A DKG event
    #[serde(rename = "dkg")]
    DKG(DKGEvent),
}

impl Event {
    /// Get the topic name for this event type
    pub fn topic(&self) -> EventTopic {
        match self {
            Event::Block(_) => EventTopic::Block,
            Event::Transaction(_) => EventTopic::Transaction,
            Event::AccountUpdate(_) => EventTopic::AccountUpdate,
            Event::RolledbackTransactions(_) => EventTopic::RolledbackTransactions,
            Event::ReappliedTransactions(_) => EventTopic::ReappliedTransactions,
            Event::DKG(_) => EventTopic::DKG,
        }
    }
}

/// Information about a new block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockEvent {
    /// The hash of the block
    pub hash: String,
    /// The timestamp when the block was created
    pub timestamp: String,
}

/// Information about a transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionEvent {
    /// The transaction hash/ID
    pub hash: String,
    /// The status of the transaction
    pub status: Status,
    /// The program IDs that were called in this transaction
    pub program_ids: Vec<String>,
    /// Block height
    pub block_height: u64,
}

/// Information about an account update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountUpdateEvent {
    /// The account public key
    pub account: String,
    /// The transaction that updated this account
    pub transaction_hash: String,
    /// Block height
    pub block_height: u64,
}

/// Transactions that were rolled back
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolledbackTransactionsEvent {
    /// Block height
    pub block_height: u64,
    /// The transaction hashes that were rolled back
    pub transaction_hashes: Vec<String>,
}

/// Transactions that were reapplied
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReappliedTransactionsEvent {
    /// Block height
    pub block_height: u64,
    /// The transaction hashes that were reapplied
    pub transaction_hashes: Vec<String>,
}

/// Information about a DKG event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DKGEvent {
    /// The status of the DKG
    pub status: String,
}

/// A filter specification for events
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct EventFilter {
    /// Key-value map of fields to filter on
    #[serde(flatten)]
    pub criteria: HashMap<String, Value>,
}

impl EventFilter {
    /// Create a new empty filter
    pub fn new() -> Self {
        EventFilter {
            criteria: HashMap::new(),
        }
    }

    /// Check if an event matches this filter
    pub fn matches(&self, event_data: &Value) -> bool {
        // If no filters, match everything
        if self.criteria.is_empty() {
            return true;
        }

        for (key, filter_value) in &self.criteria {
            match event_data.get(key) {
                Some(data_value) => {
                    if !Self::check_filter(data_value, filter_value) {
                        return false;
                    }
                }
                None => return false, // Missing field should not match
            }
        }
        true
    }

    fn check_filter(value: &Value, filter: &Value) -> bool {
        match filter {
            Value::Array(arr) => arr
                .iter()
                .any(|v| value.as_array().is_some_and(|arr| arr.contains(v))),
            _ => value == filter,
        }
    }

    /// Create from a JSON value
    pub fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => {
                let criteria = map.into_iter().collect();
                EventFilter { criteria }
            }
            _ => EventFilter::new(),
        }
    }
}
