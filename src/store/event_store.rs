use std::time::Duration;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value as JsonValue;
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};

use crate::{
    aggregate::Aggregate,
    command::DomainCommand,
    event_handler::{EventHandler, EventRow},
};

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
#[derive(Debug, Clone)]
pub struct EventProcessingConfig {
    /// How long a claim lasts before expiring
    pub claim_ttl: Duration,
    /// Maximum number of retry attempts
    pub max_retries: i32,
    /// Base delay for exponential backoff
    pub base_retry_delay: Duration,
    /// Max delay between retries
    pub max_retry_delay: Duration,
    /// Batch size for processing events
    pub batch_size: i32,
    /// Poll interval for checking new events
    pub poll_interval: Duration,
}

impl Default for EventProcessingConfig {
    fn default() -> Self {
        Self {
            claim_ttl: Duration::from_secs(60),
            max_retries: 10,
            base_retry_delay: Duration::from_secs(5),
            max_retry_delay: Duration::from_secs(3600),
            batch_size: 100,
            poll_interval: Duration::from_secs(5),
        }
    }
}

/// Core trait defining the Event Store interface
#[async_trait]
pub trait EventStore: Send + Sync + Clone {
    /// Initialize the event store
    async fn initialize(&self) -> Result<()>;
    
    /// Shutdown the event store and all background tasks
    async fn shutdown(&self);
    
    /// Execute a command and store resulting events
    async fn execute_command<A: Aggregate>(
        &self,
        command: A::Command,
        service: A::Service,
        metadata: JsonValue,
    ) -> Result<CommandResult>
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
    
    /// Get all events starting from a global position
    async fn get_all_events(
        &self,
        from_position: i64,
        limit: i32,
    ) -> Result<Vec<EventRow>>;
    
    /// Get events from the dead letter queue
    async fn get_dead_letter_events(&self, page: i64, page_size: i32) -> Result<PaginatedResult<DeadLetterEvent>>;
    
}