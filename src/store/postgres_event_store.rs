use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::{Result, anyhow};
use sqlx::{postgres::{PgPool, PgListener}, postgres::PgRow, Row, Executor, PgExecutor, FromRow};
use uuid::Uuid;
use tracing::{info, warn, error, instrument};
use tokio::sync::broadcast;
use serde_json::{self, Value as JsonValue, json};
use chrono::{DateTime, Utc};
use futures::future::join_all;
use tokio::time::sleep;
use std::collections::HashMap;
use async_trait::async_trait;

use crate::{
    store::event_store::{
        EventStore, CommandResult, EventProcessingConfig, PaginationOptions, PaginatedResult,
        DeadLetterEvent,
    },
    store::stream_claim::StreamClaim,
    aggregate::Aggregate,
    view::View,
    command::DomainCommand,
    event_handler::{EventHandler, EventRow, EventHandlerError},
};

/// PostgreSQL-based event store implementation with competing consumers
#[derive(Clone)]
pub struct PostgresEventStore {
    pool: PgPool,
    node_id: String,
    shutdown: broadcast::Sender<()>,
}

impl PostgresEventStore {
    /// Create a new PostgreSQL event store with the given connection string
    pub async fn new(connection_string: &str) -> Result<Self> {
        let pool = PgPool::connect(connection_string)
            .await
            .map_err(|e| anyhow!("Failed to create pool: {}", e))?;

        // Generate a unique node ID for this instance
        let node_id = Uuid::new_v4().to_string();
        info!("Creating PostgresEventStore with node_id: {}", node_id);

        let store = Self { 
            pool,
            node_id,
            shutdown: broadcast::channel(1).0,
        };

        // Initialize schema if needed
        store.initialize().await?;

        Ok(store)
    }

    /// Check if a table exists in the database
    async fn table_exists(&self, table_name: &str) -> Result<bool> {
        let row = sqlx::query(
            "SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = $1
            )"
        )
        .bind(table_name)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.get::<bool, _>(0))
    }

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
        
        Utc::now() + chrono::Duration::from_std(delay).unwrap_or_else(|_| chrono::Duration::seconds(60))
    }
    /// Attempt to claim a stream for processing
    async fn claim_stream(
        &self,
        stream_name: &str,
        stream_id: &str,
        handler_name: &str,
        config: &EventProcessingConfig,
    ) -> Result<Option<StreamClaim>> {
        let claim_ttl = config.claim_ttl;
        let now = Utc::now();
        let expiration = now + chrono::Duration::from_std(claim_ttl).unwrap();

        // First, try to create the claim record if it doesn't exist
        let id = Uuid::new_v4().to_string();
        sqlx::query(
            r#"
            INSERT INTO event_processing_claims 
            (id, stream_name, stream_id, handler_name, last_position, claimed_by, claim_expires_at)
            VALUES ($1, $2, $3, $4, -1, $5, $6)
            ON CONFLICT (stream_name, stream_id, handler_name) DO NOTHING
            "#
        )
        .bind(&id)
        .bind(stream_name)
        .bind(stream_id)
        .bind(handler_name)
        .bind(&self.node_id)
        .bind(expiration)
        .execute(&self.pool)
        .await?;

        // Now try to claim it if it's unclaimed or expired or ready for retry
        let claim_result = sqlx::query(
            r#"
            UPDATE event_processing_claims
            SET 
                claimed_by = $1, 
                claim_expires_at = $2,
                last_updated_at = now()
            WHERE 
                stream_name = $3 AND 
                stream_id = $4 AND 
                handler_name = $5 AND 
                (
                    claimed_by IS NULL OR 
                    claim_expires_at < now() OR 
                    (next_retry_at IS NOT NULL AND next_retry_at <= now())
                )
            RETURNING 
                id,
                stream_name,
                stream_id,
                handler_name,
                last_position,
                claimed_by,
                claim_expires_at,
                error_count,
                last_error,
                next_retry_at
            "#
        )
        .bind(&self.node_id)
        .bind(expiration)
        .bind(stream_name)
        .bind(stream_id)
        .bind(handler_name)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = claim_result {
            // Use FromRow implementation instead of manual conversion
            let stream_claim = StreamClaim::from_row(&row)?;
            Ok(Some(stream_claim))
        } else {
            Ok(None)
        }
    }

    /// Release a stream claim
    async fn release_stream(
        &self,
        stream_name: &str,
        stream_id: &str,
        handler_name: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE event_processing_claims
            SET 
                claimed_by = NULL, 
                claim_expires_at = NULL,
                last_updated_at = now()
            WHERE 
                stream_name = $1 AND 
                stream_id = $2 AND 
                handler_name = $3 AND
                claimed_by = $4
            "#
        )
        .bind(stream_name)
        .bind(stream_id)
        .bind(handler_name)
        .bind(&self.node_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Update a stream claim with successful processing
    async fn update_stream_claim_success(
        &self,
        stream_name: &str,
        stream_id: &str,
        handler_name: &str,
        last_position: i64,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE event_processing_claims
            SET 
                last_position = $1,
                error_count = 0,
                last_error = NULL,
                next_retry_at = NULL,
                last_updated_at = now()
            WHERE 
                stream_name = $2 AND 
                stream_id = $3 AND 
                handler_name = $4 AND
                claimed_by = $5
            "#
        )
        .bind(last_position)
        .bind(stream_name)
        .bind(stream_id)
        .bind(handler_name)
        .bind(&self.node_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Update a stream claim with error information
    async fn update_stream_claim_error(
        &self,
        stream_name: &str,
        stream_id: &str,
        handler_name: &str,
        error: &str,
        config: &EventProcessingConfig,
    ) -> Result<()> {
        // Get current error count
        let claim = sqlx::query_as::<_, StreamClaim>(
            r#"
            SELECT 
                id,
                stream_name,
                stream_id,
                handler_name,
                last_position,
                claimed_by,
                claim_expires_at,
                error_count,
                last_error,
                next_retry_at
            FROM event_processing_claims
            WHERE 
                stream_name = $1 AND 
                stream_id = $2 AND 
                handler_name = $3
            "#
        )
        .bind(stream_name)
        .bind(stream_id)
        .bind(handler_name)
        .fetch_one(&self.pool)
        .await?;

        let error_count = claim.error_count + 1;
        let next_retry_at = if error_count <= config.max_retries {
            Some(Self::calculate_next_retry_time(error_count, config))
        } else {
            // No more retries - move to dead letter queue
            info!(
                "Max retries exceeded for stream {}/{}, handler {}. Moving to dead letter queue.",
                stream_name, stream_id, handler_name
            );
            
            if let Err(e) = self.move_to_dead_letter_queue(
                stream_name,
                stream_id,
                handler_name,
                claim.last_position,
                error,
                error_count
            ).await {
                error!("Failed to move event to dead letter queue: {}", e);
            }
            
            // No more retries
            None
        };

        sqlx::query(
            r#"
            UPDATE event_processing_claims
            SET 
                error_count = $1,
                last_error = $2,
                next_retry_at = $3,
                claimed_by = NULL,
                claim_expires_at = NULL,
                last_updated_at = now()
            WHERE 
                stream_name = $4 AND 
                stream_id = $5 AND 
                handler_name = $6 AND
                claimed_by = $7
            "#
        )
        .bind(error_count)
        .bind(error)
        .bind(next_retry_at)
        .bind(stream_name)
        .bind(stream_id)
        .bind(handler_name)
        .bind(&self.node_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
    
    /// Move a failed event to the dead letter queue
    async fn move_to_dead_letter_queue(
        &self,
        stream_name: &str,
        stream_id: &str,
        handler_name: &str,
        position: i64,
        error: &str,
        retry_count: i32
    ) -> Result<()> {
        // Get the event at the specified position
        let event_row = sqlx::query(
            r#"
            SELECT 
                id, stream_name, stream_id, event_data, metadata, stream_position, global_position, created_at
            FROM events 
            WHERE 
                stream_name = $1 AND 
                stream_id = $2 AND 
                stream_position = $3
            "#
        )
        .bind(stream_name)
        .bind(stream_id)
        .bind(position)
        .fetch_optional(&self.pool)
        .await?;
        
        // If event exists, move it to the dead letter queue
        if let Some(row) = event_row {
            let id = Uuid::new_v4().to_string();
            
            sqlx::query(
                r#"
                INSERT INTO dead_letter_events
                (id, event_id, stream_name, stream_id, handler_name, error_message, 
                 retry_count, event_data, stream_position, dead_lettered_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
                "#
            )
            .bind(&id)
            .bind(row.get::<String, _>("id"))
            .bind(stream_name)
            .bind(stream_id)
            .bind(handler_name)
            .bind(error)
            .bind(retry_count)
            .bind(row.get::<JsonValue, _>("event_data"))
            .bind(position)
            .execute(&self.pool)
            .await?;
            
            info!(
                "Event moved to dead letter queue: id={}, stream={}/{}, position={}, handler={}",
                id, stream_name, stream_id, position, handler_name
            );
        } else {
            warn!(
                "Could not find event to move to dead letter queue: stream={}/{}, position={}",
                stream_name, stream_id, position
            );
        }
        
        Ok(())
    }

    /// Process a stream's events for a handler
    async fn process_stream_events<H>(
        &self,
        handler: &H,
        stream_name: &str,
        stream_id: &str,
        last_position: i64,
        config: &EventProcessingConfig,
    ) -> Result<i64>
    where
        H: EventHandler + Send + Sync,
    {
        let rows = sqlx::query(
            r#"
            SELECT 
                id, stream_name, stream_id, event_data, metadata, stream_position, global_position, created_at
            FROM events 
            WHERE 
                stream_name = $1 AND 
                stream_id = $2 AND 
                stream_position > $3
            ORDER BY stream_position
            LIMIT $4
            "#
        )
        .bind(stream_name)
        .bind(stream_id)
        .bind(last_position)
        .bind(config.batch_size)
        .fetch_all(&self.pool)
        .await?;

        let mut final_position = last_position;
        for row in rows {
            if let Ok(event) = serde_json::from_value(row.get("event_data")) {
                let event_row = EventRow {
                    id: row.get("id"),
                    stream_name: row.get("stream_name"),
                    stream_id: row.get("stream_id"),
                    event_data: row.get("event_data"),
                    metadata: row.get("metadata"),
                    stream_position: row.get("stream_position"),
                    global_position: row.get("global_position"),
                    created_at: row.get("created_at"),
                };
                
                handler.handle_event(event, event_row.clone()).await
                    .map_err(|e| anyhow!("Handler error: {}", e))?;
                
                final_position = row.get("stream_position");
                
                // Update claim after each event for durability
                self.update_stream_claim_success(
                    stream_name,
                    stream_id,
                    H::name(),
                    final_position
                ).await?;
            }
        }

        Ok(final_position)
    }

    /// Get all active streams for a given handler
    async fn get_active_streams_for_handler(
        &self,
        handler_name: &str,
    ) -> Result<Vec<(String, String)>> {
        // First, check for any streams with unprocessed events
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT e.stream_name, e.stream_id
            FROM events e
            LEFT JOIN event_processing_claims c ON 
                e.stream_name = c.stream_name AND 
                e.stream_id = c.stream_id AND 
                c.handler_name = $1
            WHERE 
                c.id IS NULL OR 
                e.stream_position > c.last_position
            LIMIT 100
            "#
        )
        .bind(handler_name)
        .fetch_all(&self.pool)
        .await?;

        let mut streams = Vec::new();
        for row in rows {
            streams.push((
                row.get::<String, _>("stream_name"),
                row.get::<String, _>("stream_id")
            ));
        }

        // Also check for streams with failed processing ready for retry
        let retry_rows = sqlx::query(
            r#"
            SELECT stream_name, stream_id
            FROM event_processing_claims
            WHERE 
                handler_name = $1 AND
                next_retry_at IS NOT NULL AND
                next_retry_at <= now() AND
                (claimed_by IS NULL OR claim_expires_at < now())
            LIMIT 100
            "#
        )
        .bind(handler_name)
        .fetch_all(&self.pool)
        .await?;

        for row in retry_rows {
            let stream_name = row.get::<String, _>("stream_name");
            let stream_id = row.get::<String, _>("stream_id");
            
            // Only add if not already in the list
            if !streams.contains(&(stream_name.clone(), stream_id.clone())) {
                streams.push((stream_name, stream_id));
            }
        }

        Ok(streams)
    }

    /// Process view events for a specific partition
    async fn process_view_events<V>(
        &self,
        view: &V,
        partition_key: &str, 
        last_position: i64,
        config: &EventProcessingConfig,
    ) -> Result<i64>
    where
        V: View + Send + Sync,
    {
        let rows = sqlx::query(
            r#"
            SELECT 
                id, stream_name, stream_id, event_data, metadata, stream_position, global_position, created_at
            FROM events 
            WHERE global_position > $1
            ORDER BY global_position
            LIMIT $2
            "#
        )
        .bind(last_position)
        .bind(config.batch_size)
        .fetch_all(&self.pool)
        .await?;

        let mut final_position = last_position;
        for row in rows {
            if let Ok(event) = serde_json::from_value(row.get("event_data")) {
                let event_row = EventRow {
                    id: row.get("id"),
                    stream_name: row.get("stream_name"),
                    stream_id: row.get("stream_id"),
                    event_data: row.get("event_data"),
                    metadata: row.get("metadata"),
                    stream_position: row.get("stream_position"),
                    global_position: row.get("global_position"),
                    created_at: row.get("created_at"),
                };

                if let Some(pk) = V::get_partition_key(&event, &event_row) {
                    if pk == partition_key {
                        // Get or initialize view state
                        let mut view_state = match self.get_view_state::<V>(partition_key).await? {
                            Some(state) => state,
                            None => match V::initialize(&event, &event_row) {
                                Some(view) => view,
                                None => {
                                    error!("Failed to initialize view for {}", partition_key);
                                    return Ok(last_position);
                                }
                            },
                        };
                        
                        // Apply the event
                        view_state.apply_event(&event, &event_row);
                        
                        // Save updated view state
                        self.save_view_state::<V>(partition_key, &view_state, event_row.global_position).await?;
                        
                        final_position = event_row.global_position;
                    }
                }
            }
        }

        Ok(final_position)
    }

    /// Save a view state to the database
    async fn save_view_state<V: View>(
        &self,
        partition_key: &str,
        view: &V,
        position: i64,
    ) -> Result<()> {
        let state_json = serde_json::to_value(view)?;
        let id = Uuid::new_v4();

        sqlx::query(
            r#"
            INSERT INTO view_snapshots (id, view_name, partition_key, state, last_event_position)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (view_name, partition_key) 
            DO UPDATE SET state = $4, last_event_position = $5
            "#
        )
        .bind(id)
        .bind(V::name())
        .bind(partition_key)
        .bind(&state_json)
        .bind(position)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get all partition keys for a view
    async fn get_view_partitions<V: View>(&self) -> Result<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT partition_key
            FROM view_snapshots
            WHERE view_name = $1
            "#
        )
        .bind(V::name())
        .fetch_all(&self.pool)
        .await?;

        let partitions = rows.into_iter()
            .map(|row| row.get::<String, _>("partition_key"))
            .collect();

        Ok(partitions)
    }

    /// Start processing for view partitions
    async fn start_view_partition_processors<V>(
        &self,
        config: EventProcessingConfig,
    ) -> Result<()>
    where
        V: View + Default + Send + Sync + 'static,
    {
        let view_name = V::name();
        let view = V::default();

        // Get the latest global position to start from
        let starting_position = match sqlx::query("SELECT COALESCE(MAX(global_position), -1) as pos FROM events")
            .fetch_one(&self.pool)
            .await
        {
            Ok(row) => row.get::<i64, _>("pos"),
            Err(e) => {
                error!("Failed to get latest event position: {}", e);
                -1
            }
        };

        info!("Starting view builder for {} from position {}", view_name, starting_position);
        
        // Start listener for new events
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen("new_event").await?;
        
        // Spawn background task
        let store = self.clone();
        let mut shutdown = self.shutdown.subscribe();
        
        // Process all events first (this will initialize all partitions)
        let store_clone = store.clone();
        let config_clone = config.clone();
        let view_clone = view.clone();
        
        tokio::spawn(async move {
            // Initial processing of all events
            if let Err(e) = store_clone.process_all_view_events::<V>(&view_clone, -1, &config_clone).await {
                error!("Error in initial view processing: {}", e);
            }
            
            let poll_interval = Duration::from_secs(5);
            
            loop {
                tokio::select! {
                    // Wait for new event notification
                    Ok(_) = listener.recv() => {
                        // When new event arrives, process all events
                        if let Err(e) = store.process_all_view_events::<V>(&view, -1, &config).await {
                            error!("Error processing view events: {}", e);
                        }
                    }
                    
                    // Periodic poll for updates
                    _ = sleep(poll_interval) => {
                        // Check for any new events periodically
                        if let Err(e) = store.process_all_view_events::<V>(&view, -1, &config).await {
                            error!("Error processing view events: {}", e);
                        }
                    }
                    
                    // Shutdown signal
                    _ = shutdown.recv() => break,
                }
            }
        });

        Ok(())
    }
    
    /// Process all events for a view regardless of partition
    async fn process_all_view_events<V>(
        &self,
        view: &V,
        force_position: i64,
        config: &EventProcessingConfig,
    ) -> Result<i64>
    where
        V: View + Send + Sync,
    {
        // When force_position is provided, use that, otherwise get the highest global position from any
        // existing view snapshots to avoid reprocessing
        let last_position = if force_position >= 0 {
            force_position
        } else {
            // Get max position from view snapshots
            let max_pos_query = sqlx::query(
                r#"
                SELECT COALESCE(MAX(last_event_position), -1) as max_pos
                FROM view_snapshots
                WHERE view_name = $1
                "#
            )
            .bind(V::name())
            .fetch_one(&self.pool)
            .await?;
            
            max_pos_query.get::<i64, _>("max_pos")
        };

        let rows = sqlx::query(
            r#"
            SELECT 
                id, stream_name, stream_id, event_data, metadata, stream_position, global_position, created_at
            FROM events 
            WHERE global_position > $1
            ORDER BY global_position
            LIMIT $2
            "#
        )
        .bind(last_position)
        .bind(config.batch_size)
        .fetch_all(&self.pool)
        .await?;

        if rows.is_empty() {
            return Ok(last_position);
        }

        let mut final_position = last_position;
        let mut updated_partitions = std::collections::HashSet::new();
        
        for row in rows {
            if let Ok(event) = serde_json::from_value::<V::Event>(row.get("event_data")) {
                let event_row = EventRow {
                    id: row.get("id"),
                    stream_name: row.get("stream_name"),
                    stream_id: row.get("stream_id"),
                    event_data: row.get("event_data"),
                    metadata: row.get("metadata"),
                    stream_position: row.get("stream_position"),
                    global_position: row.get("global_position"),
                    created_at: row.get("created_at"),
                };

                if let Some(pk) = V::get_partition_key(&event, &event_row) {
                    // Get the latest position for this partition
                    let partition_position = match sqlx::query(
                        r#"
                        SELECT last_event_position
                        FROM view_snapshots
                        WHERE view_name = $1 AND partition_key = $2
                        "#
                    )
                    .bind(V::name())
                    .bind(&pk)
                    .fetch_optional(&self.pool)
                    .await? {
                        Some(row) => row.get::<i64, _>("last_event_position"),
                        None => -1
                    };
                    
                    // Only process if this event is newer than what the partition has seen
                    if event_row.global_position > partition_position {
                        // Get or initialize view state
                        let mut view_state = match self.get_view_state::<V>(&pk).await? {
                            Some(state) => state,
                            None => match V::initialize(&event, &event_row) {
                                Some(new_view) => new_view,
                                None => {
                                    error!("Failed to initialize view for {}", pk);
                                    continue; // Skip this event and try the next one
                                }
                            },
                        };
                        
                        // Apply the event
                        view_state.apply_event(&event, &event_row);
                        
                        // Save updated view state
                        self.save_view_state::<V>(&pk, &view_state, event_row.global_position).await?;
                        
                        updated_partitions.insert(pk);
                    }
                }
                
                final_position = event_row.global_position;
            }
        }

        if !updated_partitions.is_empty() {
            info!("Updated {} partitions for view {}", updated_partitions.len(), V::name());
        }

        Ok(final_position)
    }

    /// Get the last processed position for a view
    async fn get_view_last_position<V: View>(&self, partition_key: &str) -> Result<Option<i64>> {
        let row = sqlx::query(
            r#"
            SELECT last_event_position
            FROM view_snapshots
            WHERE view_name = $1 AND partition_key = $2
            "#
        )
        .bind(V::name())
        .bind(partition_key)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => Ok(Some(row.get::<i64, _>("last_event_position"))),
            None => Ok(None),
        }
    }

    /// Start monitoring for new streams that need processing
    async fn start_handler_monitor<H>(
        &self,
        handler: H,
        config: EventProcessingConfig,
    ) -> Result<()>
    where
        H: EventHandler + Send + Sync + Clone + 'static,
    {
        let handler_name = H::name().to_string();
        info!("Starting handler monitor for {}", handler_name);
        
        // Create a listener for new events
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen("new_event").await?;
        
        let store = self.clone();
        let mut shutdown = self.shutdown.subscribe();
        
        tokio::spawn(async move {
            // Immediately check for any existing streams
            let _ = store.check_streams_for_handler(&handler, &config).await;
            
            let poll_interval = Duration::from_secs(5);
            
            loop {
                tokio::select! {
                    // Handle new event notifications
                    Ok(_) = listener.recv() => {
                        if let Err(e) = store.check_streams_for_handler(&handler, &config).await {
                            error!("Error checking streams for handler {}: {}", handler_name, e);
                        }
                    }
                    
                    // Periodic poll to find new streams or retry failed ones
                    _ = sleep(poll_interval) => {
                        if let Err(e) = store.check_streams_for_handler(&handler, &config).await {
                            error!("Error checking streams for handler {}: {}", handler_name, e);
                        }
                    }
                    
                    // Shutdown signal
                    _ = shutdown.recv() => break,
                }
            }
            
            info!("Handler monitor for {} shutting down", handler_name);
        });
        
        Ok(())
    }
    
    /// Check for streams that need processing for a handler
    async fn check_streams_for_handler<H>(
        &self,
        handler: &H,
        config: &EventProcessingConfig,
    ) -> Result<()>
    where
        H: EventHandler + Send + Sync + Clone,
    {
        // Find streams with events needing processing
        let streams = self.get_active_streams_for_handler(H::name()).await?;
        
        // Process each stream
        for (stream_name, stream_id) in streams {
            let handler_clone = handler.clone();
            let config_clone = config.clone();
            let store = self.clone();
            
            // Using an inline task to concurrently process streams
            // while the main monitor continues checking for more
            tokio::spawn(async move {
                if let Ok(Some(claim)) = store.claim_stream(
                    &stream_name,
                    &stream_id,
                    H::name(),
                    &config_clone
                ).await {
                    // Process the stream's events
                    match store.process_stream_events(
                        &handler_clone,
                        &stream_name,
                        &stream_id,
                        claim.last_position,
                        &config_clone
                    ).await {
                        Ok(position) => {
                            if let Err(e) = store.update_stream_claim_success(
                                &stream_name,
                                &stream_id,
                                H::name(),
                                position
                            ).await {
                                error!("Error updating claim success: {}", e);
                            }
                        },
                        Err(e) => {
                            error!("Error processing stream events: {}", e);
                            if let Err(e) = store.update_stream_claim_error(
                                &stream_name,
                                &stream_id,
                                H::name(),
                                &e.to_string(),
                                &config_clone
                            ).await {
                                error!("Error updating claim error: {}", e);
                            }
                        }
                    }
                    
                    // Release the claim when done
                    if let Err(e) = store.release_stream(
                        &stream_name,
                        &stream_id,
                        H::name()
                    ).await {
                        error!("Error releasing stream claim: {}", e);
                    }
                }
            });
        }
        
        Ok(())
    }
}

#[async_trait]
impl EventStore for PostgresEventStore {
    async fn initialize(&self) -> Result<()> {
        // Check if the core tables exist
        let events_exist = self.table_exists("events").await?;
        let claims_exist = self.table_exists("event_processing_claims").await?;
        
        if !events_exist || !claims_exist {
            info!("Initializing event store schema");
            
            // Use schema from SQL file
            let schema = include_str!("../schema.sql");
            let mut tx = self.pool.begin().await?;
            tx.execute(schema)
                .await
                .map_err(|e| anyhow!("Failed to initialize schema: {}", e))?;
            tx.commit().await?;
        }
        
        Ok(())
    }
    
    async fn shutdown(&self) {
        let _ = self.shutdown.send(());
    }
    
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
                id, stream_name, stream_id, event_data, metadata, stream_position, global_position, created_at
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
                global_position: row.get("global_position"),
                created_at: row.get("created_at"),
            });
        }

        Ok(events)
    }
    
    async fn get_all_events(&self, after_position: i64, limit: i32) -> Result<Vec<EventRow>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                id, stream_name, stream_id, event_data, metadata, stream_position, global_position, created_at
            FROM events 
            WHERE global_position > $1
            ORDER BY global_position
            LIMIT $2
            "#
        )
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
                global_position: row.get("global_position"),
                created_at: row.get("created_at"),
            });
        }

        Ok(events)
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

        let stream_name = A::name();
        let next_position = last_position.unwrap_or(-1) + 1;

        let mut tx = self.pool.begin().await?;
        let mut final_global_position = 0;

        for (idx, event) in events.into_iter().enumerate() {
            let event_json = serde_json::to_value(&event)?;
            let stream_position = next_position + idx as i64;
            let id = Uuid::new_v4();

            let row = sqlx::query(
                r#"
                INSERT INTO events (id, stream_name, stream_id, event_data, metadata, stream_position)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING global_position
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

            final_global_position = row.get("global_position");
        }

        // Notify listeners of new events
        sqlx::query("SELECT pg_notify('new_event', $1)")
            .bind(final_global_position.to_string())
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        Ok(CommandResult {
            stream_id,
            global_position: final_global_position,
        })
    }
    
    #[instrument(skip(self))]
    async fn build_state<A: Aggregate>(
        &self,
        stream_id: &str,
    ) -> Result<(Option<A::State>, Option<i64>)> {
        let stream_name = A::name();
        
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

        for row in rows {
            if let Ok(event) = serde_json::from_value(row.get("event_data")) {
                A::apply_event(&mut state, &event);
                last_position = Some(row.get("stream_position"));
            }
        }

        Ok((state, last_position))
    }
    
    async fn start_event_handler<H: EventHandler + Send + Sync + Clone + 'static>(
        &self,
        handler: H,
        config: Option<EventProcessingConfig>,
    ) -> Result<()> {
        let config = config.unwrap_or_default();
        self.start_handler_monitor(handler, config).await
    }
    
    async fn start_view_builder<V>(
        &self,
        config: Option<EventProcessingConfig>,
    ) -> Result<()>
    where
        V: View + Default + Send + Sync + 'static,
    {
        let config = config.unwrap_or_default();
        self.start_view_partition_processors::<V>(config).await
    }
    
    async fn get_view_state<V: View>(&self, partition_key: &str) -> Result<Option<V>> {
        let row = sqlx::query(
            "SELECT state 
             FROM view_snapshots 
             WHERE view_name = $1 AND partition_key = $2"
        )
        .bind(V::name())
        .bind(partition_key)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => Ok(Some(serde_json::from_value(row.get("state"))?)),
            None => Ok(None)
        }
    }
    
    async fn query_views<V: View>(
        &self,
        condition: &str,
        params: Vec<JsonValue>,
        pagination: Option<PaginationOptions>,
    ) -> Result<PaginatedResult<V>> {
        // First get total count
        let view_name = V::name();
        let count_query = format!(
            "SELECT COUNT(*) as count 
             FROM view_snapshots 
             WHERE view_name = $1 
             AND {}", 
            condition
        );

        let mut count_builder = sqlx::query(&count_query).bind(view_name.clone());
        for param in &params {
            count_builder = count_builder.bind(param);
        }

        let count_row = count_builder.fetch_one(&self.pool).await?;
        let total_count: i64 = count_row.get("count");

        if total_count == 0 {
            return Ok(PaginatedResult {
                items: vec![],
                total_count: 0,
                page: 0,
                total_pages: 0,
            });
        }

        // Add pagination if provided
        let (query, page, page_size) = if let Some(ref pagination) = pagination {
            (
                format!(
                    "SELECT partition_key, state 
                     FROM view_snapshots 
                     WHERE view_name = $1 
                     AND {}
                     ORDER BY partition_key 
                     LIMIT ${} OFFSET ${}", 
                    condition,
                    params.len() + 2,
                    params.len() + 3
                ),
                pagination.page,
                pagination.page_size,
            )
        } else {
            (
                format!(
                    "SELECT partition_key, state 
                     FROM view_snapshots 
                     WHERE view_name = $1 
                     AND {}
                     ORDER BY partition_key",
                    condition
                ),
                0,
                total_count,
            )
        };

        let mut query_builder = sqlx::query(&query).bind(view_name);
        for param in &params {
            query_builder = query_builder.bind(param);
        }

        // Add pagination parameters if provided
        if pagination.is_some() {
            query_builder = query_builder
                .bind(page_size)
                .bind(page * page_size);
        }

        let rows = query_builder.fetch_all(&self.pool).await?;

        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let view: V = serde_json::from_value(row.get("state"))?;
            items.push(view);
        }

        Ok(PaginatedResult {
            items,
            total_count,
            page,
            total_pages: (total_count as f64 / page_size as f64).ceil() as i64,
        })
    }
    
    async fn wait_for_view<V: View + Default>(&self, partition_key: &str, target_position: i64, timeout_ms: u64) -> Result<()> {
        let start = Instant::now();
        let timeout = Duration::from_millis(timeout_ms);
        let view = V::default();
        let config = EventProcessingConfig::default();
        
        // Try to process events immediately to ensure the view is created and up to date
        self.process_all_view_events::<V>(&view, -1, &config).await?;

        loop {
            let row = sqlx::query(
                "SELECT last_event_position 
                 FROM view_snapshots 
                 WHERE view_name = $1 AND partition_key = $2"
            )
            .bind(V::name())
            .bind(partition_key)
            .fetch_optional(&self.pool)
            .await?;

            if let Some(row) = row {
                let position: i64 = row.get("last_event_position");
                if position >= target_position {
                    return Ok(());
                }
            }

            // Try processing again in case we missed something - force position -1 to check all events
            self.process_all_view_events::<V>(&view, -1, &config).await?;

            if start.elapsed() > timeout {
                return Err(anyhow!("Timeout waiting for view to catch up"));
            }

            sleep(Duration::from_millis(50)).await;
        }
    }
    
    async fn get_view_by_user_id<V: View>(&self, user_id: &str) -> Result<Option<V>> {
        let res = self.query_views::<V>(
            "state->>'user_id' = ($2#>>'{}')::text",
            vec![json!(user_id)],
            None,
        ).await?;
        Ok(res.items.first().cloned())
    }
    
    async fn get_views_by_user_id<V: View>(&self, user_id: &str) -> Result<Vec<V>> {
        let res = self.query_views::<V>(
            "state->>'user_id' = ($2#>>'{}')::text",
            vec![json!(user_id)],
            None,
        ).await?;
        Ok(res.items)
    }
    
    async fn get_all_views<V: View>(&self) -> Result<Vec<V>> {
        let res = self.query_views::<V>("true", vec![], None).await?;
        Ok(res.items)
    }
    
    async fn get_dead_letter_events(&self, page: i64, page_size: i32) -> Result<PaginatedResult<DeadLetterEvent>> {
        // Get total count first
        let count_row = sqlx::query("SELECT COUNT(*) FROM dead_letter_events")
            .fetch_one(&self.pool)
            .await?;
            
        let total_count: i64 = count_row.get(0);
        
        if total_count == 0 {
            return Ok(PaginatedResult {
                items: Vec::new(),
                total_count: 0,
                page,
                total_pages: 0,
            });
        }
        
        // Calculate pagination
        let offset = page * page_size as i64;
        let total_pages = (total_count as f64 / page_size as f64).ceil() as i64;
        
        // Fetch paginated results
        let rows = sqlx::query(
            r#"
            SELECT 
                id, event_id, stream_name, stream_id, handler_name,
                error_message, retry_count, event_data, stream_position, dead_lettered_at
            FROM dead_letter_events
            ORDER BY dead_lettered_at DESC
            LIMIT $1 OFFSET $2
            "#
        )
        .bind(page_size)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        
        // Convert rows to DeadLetterEvent objects
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(DeadLetterEvent {
                id: row.get("id"),
                event_id: row.get("event_id"),
                stream_name: row.get("stream_name"),
                stream_id: row.get("stream_id"),
                handler_name: row.get("handler_name"),
                error_message: row.get("error_message"),
                retry_count: row.get("retry_count"),
                event_data: row.get("event_data"),
                stream_position: row.get("stream_position"),
                dead_lettered_at: row.get("dead_lettered_at"),
            });
        }
        
        Ok(PaginatedResult {
            items,
            total_count,
            page,
            total_pages,
        })
    }
    
    async fn replay_dead_letter_event(&self, dead_letter_id: &str) -> Result<()> {
        // Start a transaction
        let mut tx = self.pool.begin().await?;
        
        // Get the dead letter event
        let dead_letter = sqlx::query(
            r#"
            SELECT 
                id, event_id, stream_name, stream_id, handler_name,
                error_message, retry_count, event_data, stream_position
            FROM dead_letter_events
            WHERE id = $1
            "#
        )
        .bind(dead_letter_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("Dead letter event not found: {}", dead_letter_id))?;
        
        // Verify the original event still exists
        let original_event = sqlx::query(
            r#"
            SELECT id FROM events
            WHERE stream_name = $1 AND stream_id = $2 AND stream_position = $3
            "#
        )
        .bind(dead_letter.get::<String, _>("stream_name"))
        .bind(dead_letter.get::<String, _>("stream_id"))
        .bind(dead_letter.get::<i64, _>("stream_position"))
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("Original event no longer exists"))?;
        
        // Reset the processing claim for this stream/handler
        sqlx::query(
            r#"
            UPDATE event_processing_claims
            SET 
                last_position = $1 - 1, -- Set to before the failed event position
                error_count = 0,
                last_error = NULL,
                next_retry_at = NULL,
                claimed_by = NULL,
                claim_expires_at = NULL,
                last_updated_at = now()
            WHERE 
                stream_name = $2 AND 
                stream_id = $3 AND 
                handler_name = $4
            "#
        )
        .bind(dead_letter.get::<i64, _>("stream_position"))
        .bind(dead_letter.get::<String, _>("stream_name"))
        .bind(dead_letter.get::<String, _>("stream_id"))
        .bind(dead_letter.get::<String, _>("handler_name"))
        .execute(&mut *tx)
        .await?;
        
        // Delete the dead letter event
        sqlx::query("DELETE FROM dead_letter_events WHERE id = $1")
            .bind(dead_letter_id)
            .execute(&mut *tx)
            .await?;
            
        // Commit the transaction
        tx.commit().await?;
        
        info!("Replayed dead letter event: {}", dead_letter_id);
        
        // Notify of a new event to trigger processing
        sqlx::query("SELECT pg_notify('new_event', '0')")
            .execute(&self.pool)
            .await?;
        
        Ok(())
    }
    
    async fn delete_dead_letter_event(&self, dead_letter_id: &str) -> Result<()> {
        // Check if the dead letter event exists
        let exists = sqlx::query("SELECT 1 FROM dead_letter_events WHERE id = $1")
            .bind(dead_letter_id)
            .fetch_optional(&self.pool)
            .await?
            .is_some();
            
        if !exists {
            return Err(anyhow!("Dead letter event not found: {}", dead_letter_id));
        }
        
        // Delete the dead letter event
        sqlx::query("DELETE FROM dead_letter_events WHERE id = $1")
            .bind(dead_letter_id)
            .execute(&self.pool)
            .await?;
            
        info!("Deleted dead letter event: {}", dead_letter_id);
        
        Ok(())
    }
} 