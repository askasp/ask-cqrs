use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::{Result, anyhow};
use sqlx::postgres::PgPoolOptions;
use sqlx::{postgres::{PgPool, PgListener}, postgres::PgRow, Row, Executor, PgExecutor, FromRow};
use uuid::Uuid;
use tracing::{debug, error, info, info_span, instrument, trace, warn, Span};
use tokio::sync::broadcast;
use serde_json::{self, Value as JsonValue, json};
use chrono::{DateTime, Utc};
use futures::future::join_all;
use async_trait::async_trait;
use crate::view_event_handler::ViewEventHandler;
use crate::store::view_store::ViewStore;

use crate::{
    store::event_store::{
        EventStore, CommandResult, EventProcessingConfig, PaginationOptions, PaginatedResult,
    },
    aggregate::Aggregate,
    view::View,
    command::DomainCommand,
    event_handler::{EventHandler, EventRow, EventHandlerError},
};

use super::PostgresViewStore;

/// PostgreSQL-based event store implementation
#[derive(Clone)]
pub struct PostgresEventStore {
    pool: PgPool,
}

impl PostgresEventStore {
    /// Create a new PostgreSQL event store with the given connection string
    pub async fn new(connection_string: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(100)  // Increased from 30
            .acquire_timeout(Duration::from_secs(15))  // Add timeout
            .idle_timeout(Duration::from_secs(60))  // Clean up idle connections
            .connect(connection_string)
            .await
            .map_err(|e| anyhow!("Failed to create pool: {}", e))?;

        // Generate a unique node ID for this instance

        let store = Self { 
            pool,
        };

        // Initialize schema if needed
        //store.initialize().await?;

        Ok(store)
    }

    pub fn create_view_store(&self) -> PostgresViewStore {
        PostgresViewStore::new(self.pool.clone())
    }
    
    /// Initialize database schema if needed
    
    
    /// Calculate next retry time using exponential backoff
    fn calculate_next_retry_time(
        error_count: i32, 
        config: &EventProcessingConfig
    ) -> DateTime<Utc> {
        let delay = if error_count <= 0 {
            config.base_retry_delay
        } else {
            let backoff = std::cmp::min(
                config.max_retry_delay.as_secs(),
                config.base_retry_delay.as_secs() * 2u64.pow(error_count as u32)
            );
            Duration::from_secs(backoff)
        };
        
        // Utc::now() + chrono::Duration::from_std(delay).unwrap_or_else(|_| chrono::Duration::seconds(60))
        Utc::now() - chrono::Duration::hours(1)
    }

    /// Find streams that need processing for a handler, including their current positions
    #[instrument(skip(self, limit))]
    async fn find_streams_to_process(
        &self,
        handler_name: &str,
        limit: i32,
    ) -> Result<Vec<(String, String, i64, bool)>> {
        // Find streams with new events or ready for retry, and include current position
        let rows = sqlx::query(
            r#"
            SELECT 
                streams.stream_name, 
                streams.stream_id, 
                COALESCE(o.last_position, -1) as last_processed_position,
                o.next_retry_at IS NOT NULL as is_retry
            FROM (
                SELECT DISTINCT e.stream_name, e.stream_id
                FROM events e
                LEFT JOIN handler_stream_offsets o 
                  ON o.handler = $1 AND o.stream_id = e.stream_id AND o.stream_name = e.stream_name
                WHERE (o.last_position IS NULL OR e.stream_position > o.last_position)
                   AND (o.dead_lettered IS NULL OR o.dead_lettered = false)
                
                UNION
                
                SELECT h.stream_name, h.stream_id
                FROM handler_stream_offsets h
                WHERE h.handler = $1 
                  AND h.dead_lettered = false
                  AND h.next_retry_at IS NOT NULL 
                  AND h.next_retry_at <= now()
            ) AS streams
            LEFT JOIN handler_stream_offsets o 
              ON o.handler = $1 AND o.stream_id = streams.stream_id AND o.stream_name = streams.stream_name
            LIMIT $2
            "#
        )
        .bind(handler_name)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        
        let mut streams = Vec::with_capacity(rows.len());
        for row in rows {
            let stream_name: String = row.get("stream_name");
            let stream_id: String = row.get("stream_id");
            let last_position: i64 = row.get("last_processed_position");
            let is_retry: bool = row.get("is_retry");
            
            streams.push((stream_name, stream_id, last_position, is_retry));
        }
        
        debug!("Found {} streams to process for handler {}", streams.len(), handler_name);
        if !streams.is_empty() {
            for (name, id, pos, retry) in &streams {
                info!("Stream to process: {}/{} start_position={} is_retry={}", name, id, pos, retry);
            }
        }
        Ok(streams)
    }
    
    /// Process streams for a handler
    #[instrument(skip(self, handler, config))]
    async fn process_streams<H>(
        &self,
        handler: &H,
        config: &EventProcessingConfig,
        limit: i32,
    ) -> Result<usize>
    where
        H: EventHandler + Send + Sync + Clone,
    {
        // Find streams that need processing with their current positions
        let streams = self.find_streams_to_process(H::name(), limit).await?;
        
        if streams.is_empty() {
            return Ok(0);
        }
        
        // Process each stream concurrently
        let results = join_all(
            streams.into_iter().map(|(stream_name, stream_id, last_position, is_retry)| {
                let store_clone = self.clone();
                let handler_clone = handler.clone();
                let config_clone = config.clone();
                println!("Processing stream: {:?}", stream_name);
                
                async move {
                    store_clone.process_stream_with_known_position(
                        &handler_clone,
                        &stream_name,
                        &stream_id,
                        last_position,
                        is_retry,
                        &config_clone
                    ).await
                }
            })
        ).await;
        
        // Count successful processing
        let processed_count = results.into_iter()
            .filter(|r| r.is_ok())
            .count();
        
        // Raise to info level if processed streams > 0
        if processed_count > 0 {
            info!(handler = %H::name(), "Processed {} streams", processed_count);
        }
        
        Ok(processed_count)
    }
    
    /// Generates a unique advisory lock key for a stream/handler combination
    fn advisory_lock_key(&self, stream_name: &str, stream_id: &str, handler_name: &str) -> i64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        format!("{}:{}:{}", stream_name, stream_id, handler_name).hash(&mut hasher);
        hasher.finish() as i64
    }
    
    /// Try to acquire an advisory lock for a stream
    async fn try_acquire_lock(
        &self,
        stream_name: &str,
        stream_id: &str,
        handler_name: &str,
    ) -> Result<bool> {
        let lock_key = self.advisory_lock_key(stream_name, stream_id, handler_name);
        
        // Debug: Print when attempting to acquire lock
        println!("LOCK: Attempting to acquire lock with key {} for {}/{}, handler={}", 
                 lock_key, stream_name, stream_id, handler_name);
                 
        // Check if lock is already held by someone
        let holders_row = sqlx::query("SELECT count(*) as lock_count FROM pg_locks WHERE locktype = 'advisory' AND objid = $1")
            .bind(lock_key as i64)
            .fetch_one(&self.pool)
            .await?;
            
        let lock_count: i64 = holders_row.get("lock_count");
        println!("LOCK: Found {} existing holders for lock key {}", lock_count, lock_key);
        
        // Use session-level lock instead of transaction-level lock
        let row = sqlx::query("SELECT pg_try_advisory_lock($1)")
            .bind(lock_key)
            .fetch_one(&self.pool)
            .await?;
            
        let acquired = row.get::<bool, _>(0);
        
        // Debug: Print lock acquisition result
        println!("LOCK: Lock acquisition result for key {}: {}", lock_key, acquired);
        
        Ok(acquired)
    }
    
    /// Release an advisory lock for a stream
    async fn release_lock(
        &self,
        stream_name: &str,
        stream_id: &str,
        handler_name: &str,
    ) -> Result<()> {
        let lock_key = self.advisory_lock_key(stream_name, stream_id, handler_name);
        
        // Debug: Print when attempting to release lock
        println!("LOCK: Attempting to release lock with key {} for {}/{}, handler={}", 
                 lock_key, stream_name, stream_id, handler_name);
                 
        // Query to release lock
        let result = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(lock_key)
            .execute(&self.pool)
            .await?;
            
        // Debug: Print release result
        println!("LOCK: Lock release for key {}: executed with {} rows affected", 
                 lock_key, result.rows_affected());
                 
        // Verify the lock was released
        let holders_row = sqlx::query("SELECT count(*) as lock_count FROM pg_locks WHERE locktype = 'advisory' AND objid = $1")
            .bind(lock_key as i64)
            .fetch_one(&self.pool)
            .await?;
            
        let lock_count: i64 = holders_row.get("lock_count");
        println!("LOCK: After release, found {} existing holders for lock key {}", lock_count, lock_key);
        
        Ok(())
    }
    
    /// Process a single stream for a handler with advisory locking
    #[instrument(skip(self, handler, config), fields(
        stream_name = %stream_name, 
        stream_id = %stream_id, 
        last_position = last_position,
        is_retry = is_retry
    ))]
    async fn process_stream_with_known_position<H>(
        &self,
        handler: &H,
        stream_name: &str,
        stream_id: &str,
        last_position: i64,
        is_retry: bool,
        config: &EventProcessingConfig,
    ) -> Result<()>
    where
        H: EventHandler + Send + Sync,
    {
        let handler_name = H::name();
        
        if is_retry {
            debug!("Processing stream as a retry");
        }
        
        // Acquire a dedicated connection from the pool with timeout
        let mut conn = match tokio::time::timeout(
            Duration::from_secs(5), 
            self.pool.acquire()
        ).await {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => return Err(anyhow!("Failed to acquire connection: {}", e)),
            Err(_) => return Err(anyhow!("Timed out acquiring connection from pool")),
        };
        
        // Generate lock key
        let lock_key = self.advisory_lock_key(stream_name, stream_id, handler_name);
        
        // Disable verbose logging in production
        #[cfg(debug_assertions)]
        debug!("Acquiring lock {} on dedicated connection", lock_key);
        
        // Try to acquire the lock on this specific connection with timeout
        let acquired = match tokio::time::timeout(
            Duration::from_secs(3),
            sqlx::query("SELECT pg_try_advisory_lock($1)")
                .bind(lock_key)
                .fetch_one(&mut *conn)
        ).await {
            Ok(Ok(row)) => row.get::<bool, _>(0),
            Ok(Err(e)) => {
                error!("Error acquiring lock: {}", e);
                return Ok(());
            },
            Err(_) => {
                warn!("Lock acquisition timed out for {}/{}, handler={}", 
                    stream_name, stream_id, handler_name);
                return Ok(());
            }
        };
        
        if !acquired {
            debug!("Could not acquire lock {} on dedicated connection", lock_key);
            return Ok(());
        }
        
        #[cfg(debug_assertions)]
        debug!("Successfully acquired lock {} on dedicated connection", lock_key);
        
        // Process the stream
        let result = self.process_stream_with_lock(
            handler, 
            stream_name, 
            stream_id, 
            last_position,
            is_retry,
            config
        ).await;
        
        // Release the lock on the same connection
        #[cfg(debug_assertions)]
        debug!("Releasing lock {} on dedicated connection", lock_key);
        
        let release_result = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(lock_key)
            .execute(&mut *conn)
            .await;
        
        if let Err(e) = release_result {
            error!("Failed to release lock {}: {}", lock_key, e);
        }
        
        // Return the original result
        result
    }
    
    /// Process a stream once we have the lock and know the current position
    #[instrument(skip(self, handler, config), fields(
        stream_name = %stream_name, 
        stream_id = %stream_id, 
        last_position = last_position,
        is_retry = is_retry
    ))]
    async fn process_stream_with_lock<H>(
        &self,
        handler: &H,
        stream_name: &str,
        stream_id: &str,
        last_position: i64,
        is_retry: bool,
        config: &EventProcessingConfig,
    ) -> Result<()>
    where
        H: EventHandler + Send + Sync,
    {
        let handler_name = H::name();
        
        // Log retry status
        if is_retry {
            info!("Processing stream as a retry");
        }
        
        // Ensure offset record exists
        self.ensure_handler_offset_exists(handler_name, stream_name, stream_id, last_position).await?;
        
        // Get events to process - for retries, we need to get the specific event that failed
        let events = self.get_events_for_stream(
            stream_name,
            stream_id,
            last_position,
            config.batch_size
        ).await?;
        
        if events.is_empty() {
            debug!("No events to process");
            if is_retry {
                self.clear_retry_status(stream_name, stream_id, handler_name).await?;
            }

            return Ok(());
        }
        
        // Process each event
        let mut max_position = last_position;
        
        for event in events {
            let position = event.stream_position;
            
            // Process the event
            let result = match serde_json::from_value::<H::Events>(event.event_data.clone()) {
                Ok(typed_event) => {
                    handler.handle_event(typed_event, event.clone()).await
                        .map_err(|e| anyhow!("Handler error: {}", e.log_message))
                },
                Err(e) => {
                    Err(anyhow!("Deserialization error: {}", e))
                }
            };
            
            match result {
                Ok(_) => {
                    // Update max position
                    max_position = position;
                    debug!(position, "Successfully processed event");
                },
                Err(e) => {
                    // Record error and schedule retry
                    let error_msg = e.to_string();
                    error!(position, error = %error_msg, "Error processing event");
                    
                    self.record_processing_error(
                        stream_name,
                        stream_id,
                        handler_name,
                        &error_msg,
                        position,
                        &event.id,
                        config
                    ).await?;
                    
                    // Stop processing this stream
                    return Ok(());
                }
            }
        }
        
        // Update the last processed position
        if max_position > last_position {
            self.update_processed_position(
                stream_name,
                stream_id,
                handler_name,
                max_position,
                is_retry
            ).await?;
            
            info!(
                handler = %handler_name,
                prev_position = last_position,
                new_position = max_position, 
                "Stream processing completed"
            );
        }
        
        Ok(())
    }
    
    /// Update the last processed position
    async fn update_processed_position(
        &self,
        stream_name: &str,
        stream_id: &str,
        handler_name: &str,
        position: i64,
        was_retry: bool,
    ) -> Result<()> {
        let query = if was_retry {
            // If this was a retry, clear retry fields
            "UPDATE handler_stream_offsets
             SET last_position = $4,
                 retry_count = 0,
                 next_retry_at = NULL,
                 last_error = NULL,
                 last_updated_at = now()
             WHERE handler = $1 
               AND stream_id = $2 
               AND stream_name = $3"
        } else {
            // Regular update
            "UPDATE handler_stream_offsets
             SET last_position = $4,
                 last_updated_at = now()
             WHERE handler = $1 
               AND stream_id = $2 
               AND stream_name = $3"
        };
        
        sqlx::query(query)
            .bind(handler_name)
            .bind(stream_id)
            .bind(stream_name)
            .bind(position)
            .execute(&self.pool)
            .await?;
        
        Ok(())
    }
    
    /// Clear retry status when there are no events to process
    async fn clear_retry_status(
        &self,
        stream_name: &str,
        stream_id: &str,
        handler_name: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE handler_stream_offsets
             SET retry_count = 0,
                 next_retry_at = NULL,
                 last_error = NULL,
                 last_updated_at = now()
             WHERE handler = $1 
               AND stream_id = $2 
               AND stream_name = $3
               AND next_retry_at IS NOT NULL"
        )
        .bind(handler_name)
        .bind(stream_id)
        .bind(stream_name)
        .execute(&self.pool)
        .await?;
        
        Ok(())
    }
    
    /// Record error and schedule retry
    async fn record_processing_error(
        &self,
        stream_name: &str,
        stream_id: &str,
        handler_name: &str,
        error: &str,
        failed_position: i64,
        failed_event_id: &str,
        config: &EventProcessingConfig,
    ) -> Result<()> {
        // Get current retry info
        let row = sqlx::query(
            "SELECT retry_count 
             FROM handler_stream_offsets
             WHERE handler = $1 AND stream_id = $2 AND stream_name = $3"
        )
        .bind(handler_name)
        .bind(stream_id)
        .bind(stream_name)
        .fetch_one(&self.pool)
        .await?;
        
        let retry_count_temp: i32 = row.get("retry_count");
        let retry_count = retry_count_temp + 1;
        let max_retries_exceeded = retry_count > config.max_retries;
        
        if max_retries_exceeded {
            // Mark as dead-lettered
            sqlx::query(
                "UPDATE handler_stream_offsets
                 SET dead_lettered = true,
                     dead_lettered_position = $4,
                     dead_lettered_event_id = $5,
                     retry_count = $6,
                     last_error = $7,
                     next_retry_at = NULL,
                     last_updated_at = now()
                 WHERE handler = $1 
                   AND stream_id = $2 
                   AND stream_name = $3"
            )
            .bind(handler_name)
            .bind(stream_id)
            .bind(stream_name)
            .bind(failed_position)
            .bind(failed_event_id)
            .bind(retry_count)
            .bind(error)
            .execute(&self.pool)
            .await?;
            
            warn!(
                "Stream {}/{} dead-lettered for handler {} at position {}: {}",
                stream_name, stream_id, handler_name, failed_position, error
            );
        } else {
            // Schedule retry
            let next_retry_at = Self::calculate_next_retry_time(retry_count, config);
            
            sqlx::query(
                "UPDATE handler_stream_offsets
                 SET retry_count = $4,
                     next_retry_at = $5,
                     last_error = $6,
                     last_updated_at = now()
                 WHERE handler = $1 
                   AND stream_id = $2 
                   AND stream_name = $3"
            )
            .bind(handler_name)
            .bind(stream_id)
            .bind(stream_name)
            .bind(retry_count)
            .bind(next_retry_at)
            .bind(error)
            .execute(&self.pool)
            .await?;
            
            info!(
                "Scheduled retry #{} at {} for event at position {} for {}/{}, handler={}: {}",
                retry_count, next_retry_at, failed_position, stream_name, stream_id, handler_name, error
            );
        }
        
        Ok(())
    }

    /// Initialize a handler to start from the current position for streams that don't have offsets yet
    /// This creates handler_stream_offsets entries at the latest position only for streams without existing offsets
    pub async fn initialize_handler_at_current_position<H: EventHandler>(&self) -> Result<usize> {
        let handler_name = H::name();
        
        // Insert offsets at current position only for streams that don't have offsets yet
        let result = sqlx::query(
            r#"
            INSERT INTO handler_stream_offsets (handler, stream_name, stream_id, last_position, last_updated_at)
            SELECT 
                $1 as handler,
                e.stream_name,
                e.stream_id,
                MAX(e.stream_position) as last_position,
                NOW() as last_updated_at
            FROM events e
            WHERE NOT EXISTS (
                SELECT 1 FROM handler_stream_offsets 
                WHERE handler = $1 AND stream_name = e.stream_name AND stream_id = e.stream_id
            )
            GROUP BY e.stream_name, e.stream_id
            "#
        )
        .bind(handler_name)
        .execute(&self.pool)
        .await?;
        
        let count = result.rows_affected() as usize;
        info!("Initialized handler {} at current position for {} new streams", handler_name, count);
        
        Ok(count)
    }
    
    /// Initialize a handler to start from the beginning for streams that don't have offsets yet
    /// This removes any incomplete handler_stream_offsets entries
    pub async fn initialize_handler_at_beginning<H: EventHandler>(&self) -> Result<()> {
        let handler_name = H::name();
        
        // For start from beginning, we just need to make sure no offsets exist for streams
        // that haven't been processed yet. We can leave completed streams alone.
        let result = sqlx::query(
            "DELETE FROM handler_stream_offsets WHERE handler = $1"
        )
        .bind(handler_name)
        .execute(&self.pool)
        .await?;
        
        let count = result.rows_affected() as usize;
        info!("Reset handler {} to start from beginning (removed {} existing offsets)", handler_name, count);
        
        Ok(())
    }

    /// Ensure a handler offset record exists for this stream
    async fn ensure_handler_offset_exists(
        &self,
        handler_name: &str,
        stream_name: &str,
        stream_id: &str,
        last_position: i64,
    ) -> Result<()> {
        // This uses an upsert to create the record if it doesn't exist
        sqlx::query(
            r#"
            INSERT INTO handler_stream_offsets 
                (handler, stream_name, stream_id, last_position, last_updated_at)
            VALUES 
                ($1, $2, $3, $4, NOW())
            ON CONFLICT (handler, stream_name, stream_id) 
            DO NOTHING
            "#
        )
        .bind(handler_name)
        .bind(stream_name)
        .bind(stream_id)
        .bind(last_position)
        .execute(&self.pool)
        .await?;
        
        Ok(())
    }

    pub async fn start_view<V: View + Default + Send + Sync + Clone + 'static>(
        &self,
        view_store: PostgresViewStore,
        config: Option<EventProcessingConfig>,
    ) -> Result<()> {
        let handler = ViewEventHandler::<V>::new(view_store);
        let new_config = match config {
            Some(config) => config,
            None => {
                EventProcessingConfig { start_from_beginning: true, ..Default::default() }
            }
        };
        self.start_event_handler(handler, Some(new_config)).await
    }
    
    // Add this method to access the pool
    pub fn get_pool(&self) -> &PgPool {
        &self.pool
    }

    /// Check if a handler already has any offset records
    async fn handler_has_offsets(&self, handler_name: &str) -> Result<bool> {
        let row = sqlx::query(
            "SELECT EXISTS(SELECT 1 FROM handler_stream_offsets WHERE handler = $1) as has_offsets"
        )
        .bind(handler_name)
        .fetch_one(&self.pool)
        .await?;
        
        Ok(row.get::<bool, _>("has_offsets"))
    }
}

#[async_trait]
impl EventStore for PostgresEventStore {
    /// Get events for a specific stream after a position
    #[instrument(skip(self))]
    async fn get_events_for_stream(
        &self, 
        stream_name: &str, 
        stream_id: &str, 
        after_position: i64, 
        limit: i32
    ) -> Result<Vec<EventRow>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                id, stream_name, stream_id, event_data, metadata, stream_position, created_at
            FROM events 
            WHERE stream_name = $1 AND stream_id = $2 AND stream_position > $3
            ORDER BY stream_position
            LIMIT $4
            "#
        )
        .bind(stream_name)
        .bind(stream_id)
        .bind(after_position)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            events.push(EventRow {
                id: row.get("id"),
                stream_name: row.get("stream_name"),
                stream_id: row.get("stream_id"),
                event_data: row.get("event_data"),
                metadata: row.get("metadata"),
                stream_position: row.get("stream_position"),
                created_at: row.get("created_at"),
            });
        }

        debug!(count = events.len(), "Retrieved events for stream");
        Ok(events)
    }
    
    /// Start an event handler
    #[instrument(skip(self, handler, config))]
    async fn start_event_handler<H: EventHandler + Send + Sync + Clone + 'static>(
        &self,
        handler: H,
        config: Option<EventProcessingConfig>,
    ) -> Result<()> {
        let config = config.unwrap_or_default();
        
        let handler_name = H::name().to_string();
        info!("Starting handler for {}", handler_name);
        
        // Create a listener for new events
        let mut listener = match PgListener::connect_with(&self.pool).await {
            Ok(listener) => listener,
            Err(e) => {
                error!("Failed to create PgListener: {}", e);
                return Err(anyhow!("Failed to create listener: {}", e));
            }
        };
        
        match listener.listen("new_event").await {
            Ok(_) => {},
            Err(e) => {
                error!("Failed to listen to new_event channel: {}", e);
                return Err(anyhow!("Failed to listen: {}", e));
            }
        };
        
        let store = self.clone();
        
        // Start a single processing thread
        tokio::spawn(async move {
            let span = info_span!("event_processor", handler = %handler_name);
            let _guard = span.enter();
            
            info!("Starting event processor for handler: {}", handler_name);
            
            // Check if this handler already has any offsets
            let handler_has_offsets = match store.handler_has_offsets(&handler_name).await {
                Ok(has_offsets) => has_offsets,
                Err(e) => {
                    error!("Failed to check if handler {} has offsets: {}", handler_name, e);
                    false
                }
            };
            
            // Only initialize if the handler doesn't have offsets yet
            if !handler_has_offsets {
                // Set the initial processing strategy based on config
                if config.start_from_beginning {
                    match store.initialize_handler_at_beginning::<H>().await {
                        Ok(_) => info!("Handler configured to start from beginning"),
                        Err(e) => error!("Failed to initialize handler {} at beginning: {}", handler_name, e),
                    }
                } else if config.start_from_current {
                    match store.initialize_handler_at_current_position::<H>().await {
                        Ok(count) => info!("Handler {} initialized at current position for {} new streams", handler_name, count),
                        Err(e) => error!("Failed to initialize handler {} at current position: {}", handler_name, e),
                    }
                }
            } else {
                info!("Handler {} already has offsets, skipping initialization", handler_name);
            }
            
            // Use a semaphore to limit concurrent stream processing
            let semaphore = Arc::new(tokio::sync::Semaphore::new(20));
            
            loop {
                // Process a limited number of streams
                let permit = match semaphore.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => break, // Semaphore closed, exit loop
                };
                
                let process_span = info_span!("process_streams", handler = %handler_name);
                let _process_guard = process_span.enter();
                
                let store_clone = store.clone();
                let handler_clone = handler.clone();
                let config_clone = config.clone();
                
                // Process streams in a separate task to avoid blocking
                tokio::spawn(async move {
                    match store_clone.process_streams(&handler_clone, &config_clone, 10).await {
                        Ok(processed) => {
                            if processed > 0 {
                                debug!("Processed {} streams for handler: {}", processed, handler_name);
                            }
                        },
                        Err(e) => {
                            error!("Error processing streams for handler {}: {}", handler_name, e);
                        }
                    }
                    // Permit is dropped automatically when this task completes
                    drop(permit);
                });
                
                // Wait for notification or timeout
                tokio::select! {
                    notification = listener.recv() => {
                        match notification {
                            Ok(_) => debug!("Received notification of new event for handler: {}", handler_name),
                            Err(e) => error!("Error receiving notification: {}", e),
                        }
                    }
                    _ = tokio::time::sleep(config.poll_interval) => {
                        // Regular polling interval
                    }
                }
            }
        });
        
        Ok(())
    }
    async fn initialize(&self) -> Result<()> {
        // Read schema file
        let schema = include_str!("../../src/schema.sql");
        
        // Execute the schema setup - use query_multi_statement instead of query
        // sqlx::query_multi_statement(schema)
        //     .execute(&self.pool)
        //     .await?;
        
        info!("Database schema initialized");
        Ok(())
    }
    async fn execute_command<A: Aggregate>(
        &self,
        command: A::Command,
        service: A::Service,
        metadata: JsonValue,
    ) -> Result<CommandResult>
    where
        A::Command: DomainCommand,
        A::State: Send,
    {
        let stream_id = command.stream_id();
        info!("Executing command for stream: {}", stream_id);
        let stream_name = A::name();
        
        let (state, last_position) = self.build_state::<A>(&stream_id).await?;
        
        let events = A::execute(&state, &command, &stream_id, service)?;

        // For new streams, start at position 1 instead of 0
        let next_position = last_position.unwrap_or(0) + 1;

        let mut tx = self.pool.begin().await?;
        let mut final_stream_position = next_position;
        let mut created_events = Vec::with_capacity(events.len());

        for (idx, event) in events.into_iter().enumerate() {
            let event_json = serde_json::to_value(&event)?;
            let stream_position = next_position + idx as i64;
            final_stream_position = stream_position;
            let id = Uuid::new_v4();
            
            let event_span = info_span!(
                "store_event", 
                event_id = %id,
                event_type = %event_json["type"].as_str().unwrap_or("unknown"),
                stream_position = stream_position
            );
            let _event_guard = event_span.enter();

            let row = sqlx::query(
                r#"
                INSERT INTO events (id, stream_name, stream_id, event_data, metadata, stream_position)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id, created_at
                "#,
            )
            .bind(id.to_string())
            .bind(&stream_name)
            .bind(&stream_id)
            .bind(&event_json)
            .bind(&metadata)
            .bind(stream_position)
            .fetch_one(&mut *tx)
            .await?;

            // Create an EventRow for this event
            let created_event = EventRow {
                id: row.get("id"),
                stream_name: stream_name.to_string(),
                stream_id: stream_id.to_string(),
                event_data: event_json.clone(),
                metadata: metadata.clone(),
                stream_position,
                created_at: row.get("created_at"),
            };
            
            created_events.push(created_event);
        }
            
        // Commit the transaction - notification will happen automatically via database trigger
        tx.commit().await?;
        
        info!(
            events_count = created_events.len(),
            "Command executed successfully"
        );

        Ok(CommandResult {
            stream_id,
            global_position: 0, // This field is deprecated and will be removed
            stream_position: final_stream_position,
            events: created_events,
        })
    }
    
    
    async fn build_state<A: Aggregate>(
        &self,
        stream_id: &str,
    ) -> Result<(Option<A::State>, Option<i64>)> {
        let stream_name = A::name();
        
        debug!("Building aggregate state for {}/{}", stream_name, stream_id);
        
        let rows = sqlx::query(
            "SELECT event_data, stream_position 
             FROM events 
             WHERE stream_name = $1 AND stream_id = $2
             ORDER BY stream_position"
        )
        .bind(&stream_name)
        .bind(stream_id)
        .fetch_all(&self.pool)
        .await?;

        let mut state = None;
        let mut last_position = None;
        
        trace!(event_count = rows.len(), "Retrieved events for rebuilding state");

        for (idx, row) in rows.iter().enumerate() {
            let position: i64 = row.get("stream_position");
            Span::current().record("current_position", position);
            
            if let Ok(event) = serde_json::from_value(row.get("event_data")) {
                let event_span = info_span!("apply_event", position = position, event_index = idx);
                let _guard = event_span.enter();
                
                A::apply_event(&mut state, &event);
                last_position = Some(position);
                
                trace!("Applied event to state");
            }
        }
        
        debug!(
            last_position = last_position.unwrap_or(-1),
            "Finished building aggregate state"
        );

        Ok((state, last_position))
    }
}
