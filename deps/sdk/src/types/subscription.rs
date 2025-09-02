use serde::{Deserialize, Serialize};

use super::{EventFilter, EventTopic};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "method", content = "params")]
pub enum WebSocketRequest {
    #[serde(rename = "subscribe")]
    Subscribe(SubscriptionRequest),
    #[serde(rename = "unsubscribe")]
    Unsubscribe(UnsubscribeRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscriptionRequest {
    pub topic: EventTopic,
    pub filter: EventFilter,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UnsubscribeRequest {
    pub topic: EventTopic,
    pub subscription_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SubscriptionStatus {
    Subscribed,
    Unsubscribed,
    Error,
}

/// Response to a subscription request
#[derive(Debug, Serialize, Deserialize)]
pub struct SubscriptionResponse {
    /// The result status
    pub status: SubscriptionStatus,
    /// The subscription ID (to use for unsubscribing)
    pub subscription_id: String,
    /// The topic that was subscribed to
    pub topic: EventTopic,
    /// The request ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Response to an unsubscribe request
#[derive(Debug, Serialize, Deserialize)]
pub struct UnsubscribeResponse {
    pub status: SubscriptionStatus,
    pub subscription_id: String,
    pub message: String,
}

/// Error
#[derive(Debug, Serialize, Deserialize)]
pub struct SubscriptionErrorResponse {
    pub status: SubscriptionStatus,
    pub error: String,
}
