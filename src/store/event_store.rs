use std::time::Duration;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value as JsonValue;
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use sqlx::Error as SqlxError;
use serde_json::Error as SerdeJsonError;

use crate::{
    aggregate::Aggregate,
    command::DomainCommand,
    event_handler::{EventHandler, EventRow},
};

/// Error type for command execution that can wrap both domain errors and database errors
#[derive(Error, Debug)]
pub enum CommandError<E> {
    #[error("Domain error: {0}")]
    Domain(#[from] E),
    #[error("Database error: {0}")]
    Database(SqlxError),
    #[error("Serialization error: {0}")]
    Serialization(SerdeJsonError),
    #[error("Other error: {0}")]
    Other(anyhow::Error),
}

/// An event that has permanently failed processing and was moved to the dead letter queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterEvent {
    /// Unique ID of the dead letter event record
    pub id: String,
    /// Original event ID
    pub event_id: String,
    /// Stream name the event belongs to
    pub stream_name: String,
    /// Stream ID the event belongs to
    pub stream_id: String,
    /// Handler that failed to process the event
    pub handler_name: String,
    /// Last error message
    pub error_message: String,
    /// Number of retries attempted
    pub retry_count: i32,
    /// Original event data
    pub event_data: JsonValue,
    /// Position in the stream
    pub stream_position: i64,
    /// When the event was moved to dead letter queue
    pub dead_lettered_at: DateTime<Utc>,
}

/// Result type for command execution
#[derive(Debug, Clone)]
pub struct CommandResult {
    pub stream_id: String,
    pub global_position: i64,
    pub stream_position: i64,
    pub events: Vec<EventRow>,
}

/// Options for pagination
#[derive(Debug, Clone)]
pub struct PaginationOptions {
    pub page: i64,
    pub page_size: i64,
}

/// Result for paginated operations
#[derive(Debug, Clone)]
pub struct PaginatedResult<T> {
    pub items: Vec<T>,
    pub total_count: i64,
    pub page: i64,
    pub total_pages: i64,
}

/// Configuration for event processing
#[derive(Clone, Debug)]
pub struct EventProcessingConfig {
    /// Base delay for retries
    pub base_retry_delay: Duration,
    /// Maximum delay for retries
    pub max_retry_delay: Duration,
    /// Maximum number of retries before dead-lettering
    pub max_retries: i32,
    /// Number of events to process in a batch
    pub batch_size: i32,
    /// Interval for polling new events
    pub poll_interval: Duration,
    /// Whether to start processing from the beginning of all streams
    pub start_from_beginning: bool,
    /// Whether to start processing from the current position of all streams
    pub start_from_current: bool,
}

impl Default for EventProcessingConfig {
    fn default() -> Self {
        Self {
            base_retry_delay: Duration::from_secs(30),
            max_retry_delay: Duration::from_secs(24 * 60 * 60), // 1 day
            max_retries: 5,
            batch_size: 100,
            poll_interval: Duration::from_secs(5),
            start_from_beginning: false,
            start_from_current: false,
        }
    }
}

/// Core trait defining the Event Store interface
#[async_trait]
pub trait EventStore: Send + Sync + Clone {
    
    
    /// Execute a command and store resulting events
    async fn execute_command<A: Aggregate>(
        &self,
        command: A::Command,
        service: A::Service,
        metadata: JsonValue,
    ) -> Result<CommandResult, CommandError<A::DomainError>>
    where
        A::Command: DomainCommand;
    
    /// Build current state for an aggregate by replaying events
    async fn build_state<A: Aggregate>(
        &self,
        stream_id: &str,
    ) -> Result<(Option<A::State>, Option<i64>)>;
    
    /// Start an event handler with competing consumer support
    async fn start_event_handler<H: EventHandler + Send + Sync + Clone + 'static>(
        &self,
        handler: H,
        config: Option<EventProcessingConfig>,
    ) -> Result<()>;
    
    /// Get events for a stream starting from a position
    async fn get_events_for_stream(
        &self,
        stream_name: &str,
        stream_id: &str,
        from_position: i64,
        limit: i32,
    ) -> Result<Vec<EventRow>>;

    
}