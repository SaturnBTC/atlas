use crate::types::{
    Event, EventFilter, EventTopic, SubscriptionErrorResponse, SubscriptionResponse,
    SubscriptionStatus, UnsubscribeResponse, WebSocketRequest,
};
use crate::{SubscriptionRequest, UnsubscribeRequest};
use futures::future::{BoxFuture, FutureExt};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::collections::HashMap;
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_tungstenite::tungstenite::Bytes;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, WebSocketStream};
use tracing::{debug, error, info, warn};

/// Error types for the WebSocket client
#[derive(Debug, Clone, thiserror::Error)]
pub enum WebSocketError {
    /// Failed to connect to the server
    #[error("Failed to connect to the server: {0}")]
    ConnectionFailed(String),
    /// Failed to send a message
    #[error("Failed to send a message: {0}")]
    SendFailed(String),
    /// Failed to parse a response
    #[error("Failed to parse a response: {0}")]
    ParseError(String),
    /// Subscription failed
    #[error("Failed to subscribe: {0}")]
    SubscriptionFailed(String),
    /// Unsubscription failed
    #[error("Failed to unsubscribe: {0}")]
    UnsubscriptionFailed(String),
    /// Failed to read from the WebSocket
    #[error("Failed to read from the WebSocket: {0}")]
    ReadFailed(String),
    /// General error
    #[error("Other error: {0}")]
    Other(String),
}

/// Event handler that receives events from subscriptions
pub type EventCallback = Box<dyn Fn(Event) + Send + Sync + 'static>;

/// Connection status change handler
pub type ConnectionCallback = Box<dyn Fn(bool) + Send + Sync + 'static>;

/// Asynchronous event handler that can perform async operations
pub type AsyncEventCallback = Box<dyn Fn(Event) -> BoxFuture<'static, ()> + Send + Sync + 'static>;

/// A handler for a subscription
struct SubscriptionHandler {
    /// The topic that was subscribed to
    topic: EventTopic,
    /// The filter used for subscription
    filter: EventFilter,
    /// Whether this subscription is pending server confirmation
    pending: bool,
}

/// Represents all possible types of WebSocket messages that can be received
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum WebSocketMessage {
    /// An event from the server
    Event(Event),
    /// Response to a subscription request
    SubscriptionResponse(SubscriptionResponse),
    /// Response to an unsubscribe request
    UnsubscribeResponse(UnsubscribeResponse),
    /// Error response
    ErrorResponse(SubscriptionErrorResponse),
}

/// Backoff strategy for reconnection attempts
#[derive(Clone, Debug)]
pub enum BackoffStrategy {
    /// Constant delay between reconnection attempts
    Constant(Duration),

    /// Linear increase in delay (initial + attempt * step)
    Linear { initial: Duration, step: Duration },

    /// Exponential increase in delay with jitter
    /// delay = min(max_delay, initial * (factor ^ attempt) * (1 ± jitter))
    Exponential {
        initial: Duration,
        factor: f32,
        max_delay: Duration,
        jitter: f32, // Random factor to avoid thundering herd (0.0 - 1.0)
    },
}

impl BackoffStrategy {
    /// Create a default exponential backoff strategy
    pub fn default_exponential() -> Self {
        Self::Exponential {
            initial: Duration::from_secs(1),
            factor: 2.0,
            max_delay: Duration::from_secs(5),
            jitter: 0.1,
        }
    }

    /// Calculate the next delay based on attempt number
    pub fn next_delay(&self, attempt: usize) -> Duration {
        match self {
            Self::Constant(duration) => *duration,

            Self::Linear { initial, step } => *initial + (*step * attempt as u32),

            Self::Exponential {
                initial,
                factor,
                max_delay,
                jitter,
            } => {
                // Calculate base exponential delay
                let base_ms = initial.as_millis() as f32 * factor.powi(attempt as i32);

                // Apply jitter to avoid thundering herd
                let jitter_factor = 1.0 - jitter + rand::random::<f32>() * jitter * 2.0;
                let jittered_ms = base_ms * jitter_factor;

                // Ensure we don't exceed max delay
                let capped_ms = jittered_ms.min(max_delay.as_millis() as f32);

                Duration::from_millis(capped_ms as u64)
            }
        }
    }
}

/// A client for the WebSocket API
pub struct WebSocketClient {
    /// Channel for sending messages to the WebSocket
    sender: mpsc::Sender<Message>,
    /// Active subscriptions
    subscriptions: Arc<RwLock<HashMap<String, SubscriptionHandler>>>,
    /// Event callbacks by topic
    event_callbacks: Arc<RwLock<HashMap<EventTopic, Vec<EventCallback>>>>,
    /// Connection status callbacks
    connection_callbacks: Arc<RwLock<Vec<ConnectionCallback>>>,
    /// Connection status
    connected: Arc<Mutex<bool>>,
    /// Server URL
    server_url: Arc<String>,
    /// Auto-reconnect settings
    auto_reconnect: Arc<Mutex<bool>>,
    /// Backoff_strategy
    backoff_strategy: Arc<Mutex<BackoffStrategy>>,
    /// Maximum reconnect attempts (0 = infinite)
    max_reconnect_attempts: Arc<Mutex<usize>>,
    /// Whether the client should keep running
    running: Arc<Mutex<bool>>,
    /// Message processor cancellation channel
    cancel_tx: Option<mpsc::Sender<()>>,
    /// Async event callbacks by topic
    async_event_callbacks: Arc<RwLock<HashMap<EventTopic, Vec<AsyncEventCallback>>>>,
    /// Connection state change in progress
    state_change_lock: Arc<Mutex<()>>,
    /// Keep-alive interval if enabled
    keep_alive_interval: Arc<Mutex<Option<Duration>>>,
    /// Keep-alive task handle
    keep_alive_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl WebSocketClient {
    /// Create a new WebSocket client without connecting
    pub fn new(url: &str) -> Self {
        // Create shared state
        let subscriptions = Arc::new(RwLock::new(HashMap::new()));
        let event_callbacks = Arc::new(RwLock::new(HashMap::new()));
        let connection_callbacks = Arc::new(RwLock::new(Vec::new()));
        let connected = Arc::new(Mutex::new(false));
        let server_url = Arc::new(url.to_string());
        let auto_reconnect = Arc::new(Mutex::new(false));
        let backoff_strategy = Arc::new(Mutex::new(BackoffStrategy::default_exponential()));
        let max_reconnect_attempts = Arc::new(Mutex::new(0));
        let running = Arc::new(Mutex::new(true));
        let async_event_callbacks = Arc::new(RwLock::new(HashMap::new()));
        let state_change_lock = Arc::new(Mutex::new(()));
        let keep_alive_interval = Arc::new(Mutex::new(None));
        let keep_alive_handle = Arc::new(Mutex::new(None));

        // Create a dummy sender that will be replaced when connected
        let (sender, _) = mpsc::channel::<Message>(100);

        WebSocketClient {
            sender,
            subscriptions,
            event_callbacks,
            connection_callbacks,
            connected,
            server_url,
            auto_reconnect,
            backoff_strategy,
            max_reconnect_attempts,
            running,
            cancel_tx: None,
            async_event_callbacks,
            state_change_lock,
            keep_alive_interval,
            keep_alive_handle,
        }
    }

    /// Connect to the WebSocket server
    pub async fn connect(&mut self) -> Result<(), WebSocketError> {
        // Ensure we're not already in the process of connecting
        let _connection_lock = self.state_change_lock.lock().await;

        // If already connected, return early
        if *self.connected.lock().await {
            return Ok(());
        }

        // Cancel any existing processor task before starting a new one
        if let Some(cancel_tx) = self.cancel_tx.take() {
            // Send cancellation signal and wait a moment for it to be processed
            let _ = cancel_tx.send(()).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Create a channel for cancellation
        let (cancel_tx, cancel_rx) = mpsc::channel::<()>(1);
        self.cancel_tx = Some(cancel_tx);

        // Establish connection
        let connect_result = Self::establish_new_connection(
            &self.server_url,
            &self.connected,
            &self.connection_callbacks,
            &self.keep_alive_handle,
            &self.keep_alive_interval,
            &self.running,
        )
        .await;

        // Handle connection failure
        if let Err(e) = &connect_result {
            // Clear cancel_tx if connection fails
            self.cancel_tx = None;
            return Err(e.clone());
        }

        // Unwrap the successful connection result
        let (read, sender) = connect_result.unwrap();

        // Replace the dummy sender with the real one
        self.sender = sender.clone();

        // TODO: Store spawned task handle for cleanup
        let _task_handle = tokio::spawn(Self::message_processor(
            read,
            sender,
            self.subscriptions.clone(),
            self.event_callbacks.clone(),
            self.async_event_callbacks.clone(),
            self.connection_callbacks.clone(),
            self.connected.clone(),
            self.keep_alive_handle.clone(),
            self.keep_alive_interval.clone(),
            self.running.clone(),
            self.server_url.clone(),
            self.auto_reconnect.clone(),
            self.backoff_strategy.clone(),
            self.max_reconnect_attempts.clone(),
            cancel_rx,
        ));

        Ok(())
    }

    // Update the static connect method to use the new pattern
    pub async fn connect_static(url: &str) -> Result<Self, WebSocketError> {
        let mut client = Self::new(url);
        client.connect().await?;
        Ok(client)
    }

    /// Establish a new WebSocket connection
    /// Used by both initial connection and reconnection
    async fn establish_new_connection(
        server_url: &Arc<String>,
        connected: &Arc<Mutex<bool>>,
        connection_callbacks: &Arc<RwLock<Vec<ConnectionCallback>>>,
        keep_alive_handle: &Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
        keep_alive_interval: &Arc<Mutex<Option<Duration>>>,
        running: &Arc<Mutex<bool>>,
    ) -> Result<
        (
            futures::stream::SplitStream<
                WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
            >,
            mpsc::Sender<Message>,
        ),
        WebSocketError,
    > {
        // Connect to the server
        let ws_stream = Self::establish_connection(server_url).await?;

        // Split the WebSocket stream
        let (write, read) = ws_stream.split();

        // Set up the writer task
        let sender = Self::spawn_writer_task(write);

        // Keep connection alive with ping messages
        if let Some(interval) = *keep_alive_interval.lock().await {
            Self::restart_keep_alive(keep_alive_handle, &sender, running, connected, interval)
                .await?;
        }

        // Mark as connected
        Self::update_connection_status(connected, true, connection_callbacks).await;

        Ok((read, sender))
    }

    /// Attempt to reconnect to the server
    #[allow(clippy::too_many_arguments)]
    async fn attempt_reconnection(
        server_url: &Arc<String>,
        connected: &Arc<Mutex<bool>>,
        connection_callbacks: &Arc<RwLock<Vec<ConnectionCallback>>>,
        subscriptions: &Arc<RwLock<HashMap<String, SubscriptionHandler>>>,
        keep_alive_handle: &Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
        keep_alive_interval: &Arc<Mutex<Option<Duration>>>,
        running: &Arc<Mutex<bool>>,
        backoff_strategy: &Arc<Mutex<BackoffStrategy>>,
        max_reconnect_attempts: &Arc<Mutex<usize>>,
        reconnect_attempts: &mut usize,
        cancel_rx: &mut mpsc::Receiver<()>,
    ) -> Result<
        (
            futures::stream::SplitStream<
                WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
            >,
            mpsc::Sender<Message>,
        ),
        WebSocketError,
    > {
        loop {
            // Check max reconnect attempts
            let max_attempts = *max_reconnect_attempts.lock().await;
            if max_attempts > 0 && *reconnect_attempts >= max_attempts {
                error!("Maximum reconnection attempts reached ({})", max_attempts);
                return Err(WebSocketError::ConnectionFailed(
                    "Maximum reconnection attempts reached".to_string(),
                ));
            }

            // Get the next delay from the backoff strategy
            let delay = backoff_strategy
                .lock()
                .await
                .next_delay(*reconnect_attempts);
            tokio::time::sleep(delay).await;

            *reconnect_attempts += 1;
            info!(
                "Attempting to reconnect (attempt {}, delay: {:?})...",
                reconnect_attempts, delay
            );

            // Try to reconnect using our common connection logic
            match Self::establish_new_connection(
                server_url,
                connected,
                connection_callbacks,
                keep_alive_handle,
                keep_alive_interval,
                running,
            )
            .await
            {
                Ok((read, sender)) => {
                    // Re-subscribe to all topics
                    if let Err(e) = Self::resubscribe_all(&sender, subscriptions).await {
                        error!("Failed to re-subscribe: {}", e);
                    }

                    return Ok((read, sender));
                }
                Err(e) => {
                    error!("Reconnection failed: {}", e);
                    // Continue in the reconnection loop
                }
            }

            // Check for cancellation during reconnection attempts
            if let Ok(Some(())) =
                tokio::time::timeout(tokio::time::Duration::from_millis(10), cancel_rx.recv()).await
            {
                return Err(WebSocketError::Other(
                    "Cancelled during reconnection".to_string(),
                ));
            }
        }
    }

    /// Notify connection status changes
    async fn notify_connection_status(
        connected: bool,
        callbacks: &Arc<RwLock<Vec<ConnectionCallback>>>,
    ) {
        let callbacks_guard = callbacks.read().await;
        for callback in callbacks_guard.iter() {
            callback(connected);
        }
    }

    /// Handle subscription responses consistently
    async fn handle_subscription_response(
        response: SubscriptionResponse,
        subscriptions: &Arc<RwLock<HashMap<String, SubscriptionHandler>>>,
    ) {
        info!("Received subscription response: {:?}", response);

        // Try to find subscription by request_id first, then fall back to topic
        let mut subs = subscriptions.write().await;

        // Determine which key to look for
        let Some(lookup_key) = &response.request_id else {
            warn!("Received subscription response for unknown subscription");
            return;
        };

        if let Some(handler) = subs.remove(lookup_key) {
            if matches!(response.status, SubscriptionStatus::Subscribed) {
                // Store with subscription ID from server
                subs.insert(
                    response.subscription_id.clone(),
                    SubscriptionHandler {
                        topic: handler.topic,
                        filter: handler.filter,
                        pending: false,
                    },
                );
            }
        } else {
            warn!(
                "Received subscription response for unknown subscription: {:?}",
                response
            );
        }
    }

    /// Re-subscribe to all active subscriptions after reconnection
    async fn resubscribe_all(
        sender: &mpsc::Sender<Message>,
        subscriptions: &Arc<RwLock<HashMap<String, SubscriptionHandler>>>,
    ) -> Result<(), WebSocketError> {
        // Collect non-pending subscriptions and mark for replacement
        let resubscribe_list = {
            let mut subs = subscriptions.write().await;

            // Identify subscriptions that need to be resubscribed
            let to_resubscribe: Vec<_> = subs
                .iter()
                .map(|(id, handler)| (id.clone(), handler.topic.clone(), handler.filter.clone()))
                .collect();

            // Remove the old subscription entries from the map
            subs.clear();

            to_resubscribe
        };

        // Create new pending subscriptions
        for (_, topic, filter) in resubscribe_list {
            // Create a unique pending ID
            let pending_id = format!("pending-{}-{}", topic, uuid::Uuid::new_v4());

            // Add as pending subscription
            {
                let mut subs = subscriptions.write().await;
                subs.insert(
                    pending_id.clone(),
                    SubscriptionHandler {
                        topic: topic.clone(),
                        filter: filter.clone(),
                        pending: true,
                    },
                );
            }

            // Send subscription request
            let request = WebSocketRequest::Subscribe(SubscriptionRequest {
                topic,
                filter,
                request_id: Some(pending_id),
            });

            let message = serde_json::to_string(&request).map_err(|e| {
                WebSocketError::Other(format!("Failed to serialize request: {}", e))
            })?;

            sender
                .send(Message::Text(message.into()))
                .await
                .map_err(|e| WebSocketError::SendFailed(e.to_string()))?;
        }

        Ok(())
    }

    /// Process a single WebSocket message
    async fn process_message(
        message: Message,
        sender: &mpsc::Sender<Message>,
        subscriptions: &Arc<RwLock<HashMap<String, SubscriptionHandler>>>,
        event_callbacks: &Arc<RwLock<HashMap<EventTopic, Vec<EventCallback>>>>,
        async_event_callbacks: &Arc<RwLock<HashMap<EventTopic, Vec<AsyncEventCallback>>>>,
    ) -> bool {
        // Returns true if disconnected
        match message {
            Message::Text(text) => {
                // Handle text message
                match serde_json::from_str::<WebSocketMessage>(&text) {
                    Ok(WebSocketMessage::Event(event)) => {
                        Self::handle_event(event, event_callbacks, async_event_callbacks).await;
                    }
                    Ok(WebSocketMessage::SubscriptionResponse(response)) => {
                        Self::handle_subscription_response(response, subscriptions).await;
                    }
                    Ok(WebSocketMessage::UnsubscribeResponse(response)) => {
                        Self::handle_unsubscribe_response(response, subscriptions).await;
                    }
                    Ok(WebSocketMessage::ErrorResponse(error)) => {
                        warn!("Subscription error: {}", error.error);
                    }
                    Err(e) => {
                        error!("Failed to parse WebSocket message: {}", e);
                        debug!("Message content: {}", text);
                    }
                }
                false
            }
            Message::Binary(_) => {
                debug!("Received binary message");
                false
            }
            Message::Ping(data) => {
                // Handle ping - automatically respond with pong
                if let Err(e) = sender.send(Message::Pong(data)).await {
                    warn!("Failed to send pong: {}", e);
                }
                false
            }
            Message::Pong(_) => false, // Handle pong message (keep-alive response)
            Message::Frame(_) => false, // Handle raw frame
            Message::Close(_) => true, // Connection closed
        }
    }

    /// Handle an event message with support for both sync and async callbacks
    async fn handle_event(
        event: Event,
        event_callbacks: &Arc<RwLock<HashMap<EventTopic, Vec<EventCallback>>>>,
        async_event_callbacks: &Arc<RwLock<HashMap<EventTopic, Vec<AsyncEventCallback>>>>,
    ) {
        let topic = event.topic();

        // Process synchronous callbacks
        {
            let callbacks = event_callbacks.read().await;
            if let Some(handlers) = callbacks.get(&topic) {
                for handler in handlers {
                    // Catch panics from callback to prevent crashing the WebSocket loop
                    match panic::catch_unwind(AssertUnwindSafe(|| {
                        handler(event.clone());
                    })) {
                        Ok(_) => {}
                        Err(e) => {
                            // Log the panic but don't crash
                            let panic_msg = if let Some(s) = e.downcast_ref::<&str>() {
                                s
                            } else if let Some(s) = e.downcast_ref::<String>() {
                                s.as_str()
                            } else {
                                "Unknown panic"
                            };
                            error!("Event handler panicked: {}", panic_msg);
                        }
                    }
                }
            }
        }

        // Process asynchronous callbacks
        {
            let async_callbacks = async_event_callbacks.read().await;
            if let Some(handlers) = async_callbacks.get(&topic) {
                for handler in handlers {
                    // Create a clone of the handler by calling it to get a future
                    // This avoids borrowing issues by creating the future while we still have the lock
                    let event_clone = event.clone();
                    let future = match panic::catch_unwind(AssertUnwindSafe(|| {
                        handler(event_clone.clone())
                    })) {
                        Ok(future) => future,
                        Err(e) => {
                            // Log panic but don't crash
                            let panic_msg = if let Some(s) = e.downcast_ref::<&str>() {
                                s
                            } else if let Some(s) = e.downcast_ref::<String>() {
                                s.as_str()
                            } else {
                                "Unknown panic"
                            };
                            error!("Async event handler panicked during setup: {}", panic_msg);
                            continue;
                        }
                    };

                    // Spawn a task with the future we already created
                    tokio::spawn(async move {
                        match panic::catch_unwind(AssertUnwindSafe(|| async {
                            future.await;
                        })) {
                            Ok(f) => {
                                f.await;
                            }
                            Err(e) => {
                                // Log panic but don't crash
                                let panic_msg = if let Some(s) = e.downcast_ref::<&str>() {
                                    s
                                } else if let Some(s) = e.downcast_ref::<String>() {
                                    s.as_str()
                                } else {
                                    "Unknown panic"
                                };
                                error!(
                                    "Async event handler panicked during execution: {}",
                                    panic_msg
                                );
                            }
                        }
                    });
                }
            }
        }
    }

    /// Handle an unsubscribe response
    async fn handle_unsubscribe_response(
        response: UnsubscribeResponse,
        subscriptions: &Arc<RwLock<HashMap<String, SubscriptionHandler>>>,
    ) {
        if matches!(response.status, SubscriptionStatus::Unsubscribed) {
            subscriptions
                .write()
                .await
                .remove(&response.subscription_id);
        }
    }

    /// Attempt to establish a new WebSocket connection
    async fn establish_connection(
        server_url: &str,
    ) -> Result<WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>, WebSocketError> {
        match connect_async(server_url).await {
            Ok((ws_stream, _)) => Ok(ws_stream),
            Err(e) => Err(WebSocketError::ConnectionFailed(e.to_string())),
        }
    }

    /// Set up message writer task
    fn spawn_writer_task(
        write: futures::stream::SplitSink<
            WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
            Message,
        >,
    ) -> mpsc::Sender<Message> {
        // Create a channel for the writer
        let (sender, mut new_receiver) = mpsc::channel::<Message>(1000);

        // Set up the writer task
        tokio::spawn(async move {
            let mut writer = write;
            while let Some(msg) = new_receiver.recv().await {
                if let Err(e) = writer.send(msg).await {
                    error!("Failed to send message: {}", e);
                    break;
                }
            }
        });

        sender
    }

    /// Main message processor with reconnection handling
    #[allow(clippy::too_many_arguments)]
    async fn message_processor(
        initial_stream: futures::stream::SplitStream<
            WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
        >,
        initial_sender: mpsc::Sender<Message>,
        subscriptions: Arc<RwLock<HashMap<String, SubscriptionHandler>>>,
        event_callbacks: Arc<RwLock<HashMap<EventTopic, Vec<EventCallback>>>>,
        async_event_callbacks: Arc<RwLock<HashMap<EventTopic, Vec<AsyncEventCallback>>>>,
        connection_callbacks: Arc<RwLock<Vec<ConnectionCallback>>>,
        connected: Arc<Mutex<bool>>,
        keep_alive_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
        keep_alive_interval: Arc<Mutex<Option<Duration>>>,
        running: Arc<Mutex<bool>>,
        server_url: Arc<String>,
        auto_reconnect: Arc<Mutex<bool>>,
        backoff_strategy: Arc<Mutex<BackoffStrategy>>,
        max_reconnect_attempts: Arc<Mutex<usize>>,
        mut cancel_rx: mpsc::Receiver<()>,
    ) {
        let mut reconnect_attempts: usize = 0;
        let mut read = initial_stream;
        let mut sender = initial_sender;

        // Main loop - runs as long as the client should be running
        while *running.lock().await {
            let disconnected = Self::process_messages(
                &mut read,
                &sender,
                &subscriptions,
                &event_callbacks,
                &async_event_callbacks,
                &connection_callbacks,
                &connected,
                &mut cancel_rx,
            )
            .await;

            if disconnected {
                // Handle reconnection if needed
                if !*running.lock().await || !*auto_reconnect.lock().await {
                    return; // Exit if client is shutting down or auto-reconnect is disabled
                }

                // Try reconnection
                match Self::attempt_reconnection(
                    &server_url,
                    &connected,
                    &connection_callbacks,
                    &subscriptions,
                    &keep_alive_handle,
                    &keep_alive_interval,
                    &running,
                    &backoff_strategy,
                    &max_reconnect_attempts,
                    &mut reconnect_attempts,
                    &mut cancel_rx,
                )
                .await
                {
                    Ok((new_read, new_sender)) => {
                        // Update with new connection
                        read = new_read;
                        sender = new_sender;
                        reconnect_attempts = 0; // Reset counter after successful reconnection
                    }
                    Err(_) => return, // Exit on reconnection failure or cancellation
                }
            }
        }
    }

    /// Process messages until disconnection or cancellation
    #[allow(clippy::too_many_arguments)]
    async fn process_messages(
        read: &mut futures::stream::SplitStream<
            WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
        >,
        sender: &mpsc::Sender<Message>,
        subscriptions: &Arc<RwLock<HashMap<String, SubscriptionHandler>>>,
        event_callbacks: &Arc<RwLock<HashMap<EventTopic, Vec<EventCallback>>>>,
        async_event_callbacks: &Arc<RwLock<HashMap<EventTopic, Vec<AsyncEventCallback>>>>,
        connection_callbacks: &Arc<RwLock<Vec<ConnectionCallback>>>,
        connected: &Arc<Mutex<bool>>,
        cancel_rx: &mut mpsc::Receiver<()>,
    ) -> bool {
        loop {
            tokio::select! {
                // Check for cancellation
                _ = cancel_rx.recv() => {
                    return false; // Not disconnected, just cancelled
                }

                // Process WebSocket messages
                message = read.next() => {
                    match message {
                        Some(Ok(msg)) => {
                            if Self::process_message(msg, sender, subscriptions, event_callbacks, async_event_callbacks).await {
                                // Connection closed
                                Self::update_connection_status(connected, false, connection_callbacks).await;
                                return true; // Disconnected
                            }
                        }
                        Some(Err(e)) => {
                            // Connection error with specific error message
                            error!("WebSocket read error: {}", e);
                            Self::update_connection_status(connected, false, connection_callbacks).await;
                            return true; // Disconnected
                        }
                        None => {
                            // Stream ended
                            debug!("WebSocket stream ended");
                            Self::update_connection_status(connected, false, connection_callbacks).await;
                            return true; // Disconnected
                        }
                    }
                }
            }
        }
    }

    /// Subscribe to a topic with a filter
    pub async fn subscribe(
        &self,
        topic: EventTopic,
        filter: EventFilter,
    ) -> Result<(), WebSocketError> {
        // Create a unique pending ID using a UUID
        let pending_id = format!("pending-{}-{}", topic, uuid::Uuid::new_v4());

        // First, check if we already have a matching subscription
        let subs = self.subscriptions.read().await;
        for (_, handler) in subs.iter() {
            // If we have a non-pending subscription for this topic with the same filter
            if !handler.pending && handler.topic == topic && handler.filter == filter {
                return Ok(());
            }
        }
        drop(subs);

        // Add the subscription as pending
        let mut subs = self.subscriptions.write().await;
        subs.insert(
            pending_id.clone(), // Use the pending ID as the key
            SubscriptionHandler {
                topic: topic.clone(),
                filter: filter.clone(),
                pending: true,
            },
        );
        drop(subs);

        // Send the subscription request
        let request = WebSocketRequest::Subscribe(SubscriptionRequest {
            topic: topic.clone(),
            filter: filter.clone(),
            request_id: Some(pending_id.clone()),
        });

        let message = serde_json::to_string(&request)
            .map_err(|e| WebSocketError::Other(format!("Failed to serialize request: {}", e)))?;

        self.sender
            .send(Message::Text(message.into()))
            .await
            .map_err(|e| WebSocketError::SendFailed(e.to_string()))?;

        Ok(())
    }

    /// Register a synchronous callback for a specific event topic
    pub async fn on_event<F>(
        &self,
        topic: EventTopic,
        filter: Option<EventFilter>,
        callback: F,
    ) -> Result<(), WebSocketError>
    where
        F: Fn(Event) + Send + Sync + 'static,
    {
        // First, make sure we have a subscription for this topic
        let subs = self.subscriptions.read().await;
        let has_topic_subscription = subs.values().any(|s| s.topic == topic);
        drop(subs);

        if !has_topic_subscription {
            // Subscribe if needed
            self.subscribe(topic.clone(), filter.unwrap_or_default())
                .await?;
        }

        // Add the callback to the synchronous event callbacks collection
        let mut callbacks = self.event_callbacks.write().await;
        callbacks
            .entry(topic)
            .or_insert_with(Vec::new)
            .push(Box::new(callback));

        Ok(())
    }

    /// Register an async callback for a specific event topic
    pub async fn on_event_async<F, Fut>(
        &self,
        topic: EventTopic,
        filter: Option<EventFilter>,
        callback: F,
    ) -> Result<(), WebSocketError>
    where
        F: Fn(Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        // First, make sure we have a subscription for this topic
        let subs = self.subscriptions.read().await;
        let has_topic_subscription = subs.values().any(|s| s.topic == topic);
        drop(subs);

        if !has_topic_subscription {
            // Subscribe if needed
            self.subscribe(topic.clone(), filter.unwrap_or_default())
                .await?;
        }

        // Convert the callback to use BoxFuture
        let boxed_callback =
            move |event: Event| -> BoxFuture<'static, ()> { callback(event).boxed() };

        // Add the callback to the async event callbacks collection
        let mut callbacks = self.async_event_callbacks.write().await;
        callbacks
            .entry(topic)
            .or_insert_with(Vec::new)
            .push(Box::new(boxed_callback));

        Ok(())
    }

    /// Register a callback for connection status changes
    pub async fn on_connection_change<F>(&self, callback: F)
    where
        F: Fn(bool) + Send + Sync + 'static,
    {
        self.connection_callbacks
            .write()
            .await
            .push(Box::new(callback));
    }

    /// Unsubscribe from a topic
    pub async fn unsubscribe(&self, subscription_id: &str) -> Result<(), WebSocketError> {
        let topic = {
            // Check if we have this subscription
            let subs = self.subscriptions.read().await;
            match subs.get(subscription_id) {
                Some(sub) => sub.topic.clone(),
                None => {
                    return Err(WebSocketError::UnsubscriptionFailed(
                        "Subscription not found".to_string(),
                    ))
                }
            }
        };

        let request = WebSocketRequest::Unsubscribe(UnsubscribeRequest {
            topic,
            subscription_id: subscription_id.to_string(),
        });

        let request_json =
            serde_json::to_string(&request).map_err(|e| WebSocketError::Other(e.to_string()))?;

        // Send the request
        self.sender
            .send(Message::Text(request_json.into()))
            .await
            .map_err(|e| WebSocketError::SendFailed(e.to_string()))?;

        // Remove the subscription (server response will also remove it)
        self.subscriptions.write().await.remove(subscription_id);

        Ok(())
    }

    /// Unsubscribe from all subscriptions for a topic
    pub async fn unsubscribe_topic(&self, topic: &EventTopic) -> Result<(), WebSocketError> {
        let subscription_ids: Vec<String> = {
            let subs = self.subscriptions.read().await;
            subs.iter()
                .filter(|(_, handler)| handler.topic == *topic)
                .map(|(id, _)| id.clone())
                .collect()
        };

        let mut result = Ok(());
        for id in subscription_ids {
            if let Err(e) = self.unsubscribe(&id).await {
                result = Err(e);
            }
        }

        result
    }

    /// Remove all event callbacks for a topic (both sync and async)
    pub async fn remove_event_listeners(&self, topic: &EventTopic) {
        // Remove synchronous callbacks
        let mut callbacks = self.event_callbacks.write().await;
        callbacks.remove(topic);

        // Remove asynchronous callbacks
        let mut async_callbacks = self.async_event_callbacks.write().await;
        async_callbacks.remove(topic);
    }

    /// Configure auto-reconnect settings
    pub async fn set_auto_reconnect(
        &self,
        enabled: bool,
        interval: std::time::Duration,
        max_attempts: usize,
    ) {
        *self.auto_reconnect.lock().await = enabled;
        *self.backoff_strategy.lock().await = BackoffStrategy::Constant(interval);
        *self.max_reconnect_attempts.lock().await = max_attempts;
    }

    /// Check if the client is connected
    pub async fn is_connected(&self) -> bool {
        *self.connected.lock().await
    }

    /// Close the connection and clean up resources
    ///
    /// This method should be called explicitly before the client is dropped
    /// to ensure proper cleanup of resources and graceful connection termination.
    pub async fn close(&self) -> Result<(), WebSocketError> {
        // Disable auto-reconnect first
        *self.auto_reconnect.lock().await = false;

        // Signal the worker task to stop
        *self.running.lock().await = false;

        // Cancel the message processor
        if let Some(cancel_tx) = &self.cancel_tx {
            let _ = cancel_tx.send(()).await;
        }

        // Cancel any keep-alive task
        if let Some(handle) = self.keep_alive_handle.lock().await.take() {
            handle.abort();
        }

        // Send close frame
        let _ = self.sender.send(Message::Close(None)).await;

        // Update connection status
        Self::update_connection_status(&self.connected, false, &self.connection_callbacks).await;

        Ok(())
    }

    /// Configure reconnection settings
    pub async fn set_reconnect_options(
        &self,
        enabled: bool,
        strategy: BackoffStrategy,
        max_attempts: usize,
    ) {
        *self.auto_reconnect.lock().await = enabled;
        *self.backoff_strategy.lock().await = strategy;
        *self.max_reconnect_attempts.lock().await = max_attempts;
    }

    /// Update connection status safely
    async fn update_connection_status(
        connected: &Arc<Mutex<bool>>,
        new_state: bool,
        connection_callbacks: &Arc<RwLock<Vec<ConnectionCallback>>>,
    ) -> bool {
        // Update the mutex-protected state
        let mut connected_guard = connected.lock().await;
        let changed = *connected_guard != new_state;
        *connected_guard = new_state;
        drop(connected_guard);

        // Notify about the state change exactly once
        if changed {
            Self::notify_connection_status(new_state, connection_callbacks).await;
        }

        changed // State changed
    }

    /// Enable periodic ping messages to keep the connection alive
    pub async fn enable_keep_alive(&self, interval: Duration) -> Result<(), WebSocketError> {
        // Store the interval for reconnection handling
        *self.keep_alive_interval.lock().await = Some(interval);

        // Start the keep-alive task
        Self::restart_keep_alive(
            &self.keep_alive_handle,
            &self.sender,
            &self.running,
            &self.connected,
            interval,
        )
        .await
    }

    /// Restart the keep-alive task with a new sender
    async fn restart_keep_alive(
        keep_alive_handle: &Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
        sender: &mpsc::Sender<Message>,
        running: &Arc<Mutex<bool>>,
        connected: &Arc<Mutex<bool>>,
        interval: Duration,
    ) -> Result<(), WebSocketError> {
        // Cancel any existing keep-alive task
        if let Some(handle) = keep_alive_handle.lock().await.take() {
            handle.abort();
        }

        let sender = sender.clone();
        let running = running.clone();
        let connected = connected.clone();

        // Create a new keep-alive task
        let handle = tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);

            while *running.lock().await {
                interval_timer.tick().await;

                // Only send pings if we're actually connected
                if *connected.lock().await {
                    if let Err(e) = sender.send(Message::Ping(Bytes::from_static(&[]))).await {
                        error!("Failed to send ping: {}", e);
                        // Don't break here, as we might reconnect and update the sender
                    }
                }
            }
        });

        // Store the task handle
        *keep_alive_handle.lock().await = Some(handle);

        Ok(())
    }
}

impl Drop for WebSocketClient {
    fn drop(&mut self) {
        // Set running to false to stop background tasks
        if let Some(running) = Arc::get_mut(&mut self.running) {
            if let Ok(mut guard) = running.try_lock() {
                *guard = false;
            }
        }

        // Try to cancel tasks without spawning new ones
        if let Some(cancel_tx) = self.cancel_tx.take() {
            // Try a non-blocking send (which will work in some cases)
            let _ = cancel_tx.try_send(());
        }

        // Abort the keep-alive task if it exists
        if let Some(keep_alive_handle) = Arc::get_mut(&mut self.keep_alive_handle) {
            if let Ok(mut guard) = keep_alive_handle.try_lock() {
                if let Some(handle) = guard.take() {
                    handle.abort();
                }
            }
        }
    }
}
