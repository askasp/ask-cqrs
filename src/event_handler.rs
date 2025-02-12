use async_trait::async_trait;
use serde::de::DeserializeOwned;
use thiserror::Error;
use serde_json::Value;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct EventRow {
    pub id: String,
    pub stream_name: String,
    pub stream_id: String,
    pub event_data: Value,
    pub metadata: Value,
    pub stream_position: i64,
    pub global_position: i64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
#[error("{log_message}")]
pub struct EventHandlerError {
    pub log_message: String,
}

#[async_trait]
pub trait EventHandler: Send + Sync + 'static {
    /// The event type this handler processes
    type Events: DeserializeOwned + Send + Sync + 'static;
    /// Service dependencies

    fn name() -> &'static str;
    
    async fn handle_event(
        &self,
        event: Self::Events,
        event_row: EventRow,
    ) -> Result<(), EventHandlerError>;
}
