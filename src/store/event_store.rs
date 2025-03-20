use std::time::Duration;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::{
    aggregate::Aggregate,
    view::View,
    command::DomainCommand,
    event_handler::{EventHandler, EventRow},
};


/// Result type for command execution
#[derive(Debug, Clone)]
pub struct CommandResult {
    pub stream_id: String,
    pub global_position: i64,
}

/// Options for pagination
#[derive(Debug, Clone)]
pub struct PaginationOptions {
    pub page: i64,
    pub page_size: i64,
}

/// Result for paginated operations
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

    /// Start a view builder that processes events
    async fn start_view_builder<V>(
        &self,
        config: Option<EventProcessingConfig>,
    ) -> Result<()>
    where
        V: View + Default + Send + Sync + 'static;
    
    /// Get a view state for a partition
    async fn get_view_state<V: View>(&self, partition_key: &str) -> Result<Option<V>>;
    
    /// Query views by criteria in their state
    async fn query_views<V: View>(
        &self,
        condition: &str,
        params: Vec<JsonValue>,
        pagination: Option<PaginationOptions>,
    ) -> Result<PaginatedResult<V>>;
    
    /// Wait for a view to catch up to a specific position
    async fn wait_for_view<V: View>(&self, partition_key: &str, target_position: i64, timeout_ms: u64) -> Result<()>;
    
    /// Get a single view by user_id field
    async fn get_view_by_user_id<V: View>(&self, user_id: &str) -> Result<Option<V>>;
    
    /// Get all views for a given user_id
    async fn get_views_by_user_id<V: View>(&self, user_id: &str) -> Result<Vec<V>>;
    
    /// Get all views without filtering
    async fn get_all_views<V: View>(&self) -> Result<Vec<V>>;
}