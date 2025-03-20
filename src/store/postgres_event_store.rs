use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::{Result, anyhow};
use sqlx::{postgres::{PgPool, PgListener}, postgres::PgRow, Row, Executor, PgExecutor, FromRow};
use uuid::Uuid;
use tracing::{info, warn, error, debug, instrument};
use tokio::sync::broadcast;
use serde_json::{self, Value as JsonValue, json};
use chrono::{DateTime, Utc};
use futures::future::join_all;
use tokio::time::sleep;
use std::collections::HashMap;
use async_trait::async_trait;
use tokio::task::JoinHandle;
use serde::de::DeserializeOwned;

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
    view_event_handler::ViewEventHandler,
    store::view_store::ViewStore,
    store::postgres_view_store::PostgresViewStore,
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
        tracing::debug!("Attempting to claim stream {}/{} for handler {}", 
                      stream_name, stream_id, handler_name);
        
        let claim_ttl = config.claim_ttl;
        let now = Utc::now();
        let expiration = now + chrono::Duration::from_std(claim_ttl).unwrap();

        // First, try to create the claim record if it doesn't exist
        let id = Uuid::new_v4().to_string();
        
        // Check if a claim already exists before trying to create one
        let existing_claim = sqlx::query(
            "SELECT id, claimed_by, claim_expires_at FROM event_processing_claims 
             WHERE stream_name = $1 AND stream_id = $2 AND handler_name = $3"
        )
        .bind(stream_name)
        .bind(stream_id)
        .bind(handler_name)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = &existing_claim {
            let claimed_by: Option<String> = row.get("claimed_by");
            let expires_at: Option<DateTime<Utc>> = row.get("claim_expires_at");
            
            tracing::debug!(
                "Found existing claim for {}/{}, handler={}: claimed_by={:?}, expires_at={:?}",
                stream_name, stream_id, handler_name, claimed_by, expires_at
            );
        } else {
            tracing::debug!(
                "No existing claim found for {}/{}, handler={}. Will create new claim.",
                stream_name, stream_id, handler_name
            );
        }
        
        tracing::debug!("Creating claim record for {}/{}, handler={}", 
                      stream_name, stream_id, handler_name);
        
        let insert_result = sqlx::query(
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
        
        tracing::debug!("Insert result: {} rows affected", insert_result.rows_affected());

        // Now try to claim it if it's unclaimed or expired or ready for retry
        tracing::debug!("Attempting to update claim for {}/{}, handler={}", 
                       stream_name, stream_id, handler_name);
        
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
            tracing::debug!("Successfully claimed stream {}/{} for handler {}", 
                           stream_name, stream_id, handler_name);
            Ok(Some(stream_claim))
        } else {
            // Check why the claim failed
            let claim_status = sqlx::query(
                r#"
                SELECT 
                    claimed_by, 
                    claim_expires_at, 
                    next_retry_at,
                    last_updated_at
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
            .fetch_optional(&self.pool)
            .await?;
            
            if let Some(row) = claim_status {
                let claimed_by: Option<String> = row.get("claimed_by");
                let expires_at: Option<DateTime<Utc>> = row.get("claim_expires_at");
                let retry_at: Option<DateTime<Utc>> = row.get("next_retry_at");
                let updated_at: DateTime<Utc> = row.get("last_updated_at");
                
                tracing::debug!(
                    "Claim failed for {}/{}, handler={}: claimed_by={:?}, expires_at={:?}, retry_at={:?}, updated_at={:?}",
                    stream_name, stream_id, handler_name, claimed_by, expires_at, retry_at, updated_at
                );
                
                if let Some(claimed) = claimed_by {
                    if claimed == self.node_id {
                        tracing::debug!("Stream already claimed by this node: {}", self.node_id);
                    } else {
                        tracing::debug!("Stream claimed by another node: {}", claimed);
                    }
                }
                
                if let Some(expires) = expires_at {
                    if expires > now {
                        tracing::debug!("Claim not expired yet. Expires in {} seconds", 
                                     (expires - now).num_seconds());
                    } else {
                        tracing::debug!("Claim is expired, but update failed");
                    }
                }
            } else {
                tracing::debug!("No claim record found during status check, but create/update failed");
            }
            
            tracing::debug!("Failed to claim stream {}/{} for handler {}", 
                           stream_name, stream_id, handler_name);
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
        tracing::debug!(
            "Processing events for handler '{}' stream '{}/{}' from position {}",
            H::name(), stream_name, stream_id, last_position
        );
        
        // Get events for this stream that need processing
        let events = self.get_events_for_stream(
            stream_name,
            stream_id,
            last_position,
            config.batch_size,
        ).await?;
        
        tracing::debug!("Found {} events to process for handler '{}'", events.len(), H::name());
        
        if events.is_empty() {
            return Ok(last_position);
        }
        
        let mut max_position = last_position;
        
        // Process each event in order
        for event in events {
            let position = event.stream_position;
            
            tracing::debug!(
                "Processing event: stream={}/{}, position={}, global_position={}, handler={}",
                event.stream_name, event.stream_id, event.stream_position, 
                event.global_position, H::name()
            );
            
            // Try to deserialize and handle the event
            match serde_json::from_value::<H::Events>(event.event_data.clone()) {
                Ok(typed_event) => {
                    if let Err(e) = handler.handle_event(typed_event, event.clone()).await {
                        return Err(anyhow!(e.log_message));
                    }
                    // Update the max position we've processed successfully
                    max_position = position;
                },
                Err(e) => {
                    tracing::error!(
                        "Error deserializing event: stream={}/{}, position={}, error={}", 
                        stream_name, stream_id, position, e
                    );
                }
            }
        }
        
        tracing::debug!(
            "Processed events for handler '{}' stream '{}/{}' up to position {}",
            H::name(), stream_name, stream_id, max_position
        );
        
        Ok(max_position)
    }

    /// Get all active streams for a given handler
    async fn get_active_streams_for_handler(
        &self,
        handler_name: &str,
    ) -> Result<Vec<(String, String)>> {
        tracing::info!("Getting active streams for handler: {}", handler_name);
        
        // Check existing claims for this handler
        let existing_claims = sqlx::query(
            r#"
            SELECT stream_name, stream_id, last_position, error_count, next_retry_at
            FROM event_processing_claims
            WHERE handler_name = $1
            "#
        )
        .bind(handler_name)
        .fetch_all(&self.pool)
        .await?;
        
        tracing::info!("Found {} existing claims for handler {}", existing_claims.len(), handler_name);
        for row in &existing_claims {
            let stream_name: String = row.get("stream_name");
            let stream_id: String = row.get("stream_id"); 
            let last_position: i64 = row.get("last_position");
            let error_count: i32 = row.get("error_count");
            tracing::info!("Claim: {}/{}, last_position={}, error_count={}", 
                          stream_name, stream_id, last_position, error_count);
        }
        
        // Check for any events in the database
        let all_events = sqlx::query(
            r#"
            SELECT COUNT(*) as count FROM events
            "#
        )
        .fetch_one(&self.pool)
        .await?;
        
        let event_count: i64 = all_events.get("count");
        tracing::info!("Found {} total events in the database", event_count);
        
        // Get a sample of events
        let sample_events = sqlx::query(
            r#"
            SELECT stream_name, stream_id, stream_position, global_position
            FROM events
            ORDER BY global_position
            LIMIT 5
            "#
        )
        .fetch_all(&self.pool)
        .await?;
        
        for row in &sample_events {
            let stream_name: String = row.get("stream_name");
            let stream_id: String = row.get("stream_id");
            let stream_position: i64 = row.get("stream_position");
            let global_position: i64 = row.get("global_position");
            tracing::info!("Event: {}/{}, stream_pos={}, global_pos={}", 
                          stream_name, stream_id, stream_position, global_position);
        }
        
        // First, find all streams that have events after the last processed position
        let streams_with_new_events = sqlx::query(
            r#"
            WITH last_positions AS (
                SELECT 
                    stream_name, 
                    stream_id, 
                    MAX(last_position) as position
                FROM 
                    event_processing_claims
                WHERE 
                    handler_name = $1
                GROUP BY 
                    stream_name, stream_id
            )
            SELECT DISTINCT 
                e.stream_name, 
                e.stream_id
            FROM 
                events e
            LEFT JOIN 
                last_positions lp
            ON 
                e.stream_name = lp.stream_name AND
                e.stream_id = lp.stream_id
            WHERE 
                (lp.position IS NULL OR e.stream_position > lp.position)
            ORDER BY 
                e.stream_name, e.stream_id
            "#
        )
        .bind(handler_name)
        .fetch_all(&self.pool)
        .await?;
        
        let mut result = Vec::with_capacity(streams_with_new_events.len());
        
        // Track how many streams we found
        let found_streams = streams_with_new_events.len();
        
        for row in streams_with_new_events {
            let stream_name: String = row.get("stream_name");
            let stream_id: String = row.get("stream_id");
            tracing::info!("Active stream found: {}/{} for handler {}", 
                          stream_name, stream_id, handler_name);
            result.push((stream_name, stream_id));
        }
        
        tracing::info!("Found {} active streams for handler: {}", found_streams, handler_name);
        Ok(result)
    }

       
    /// Start a view as an event handler
    pub async fn start_view<V: View + Default + Send + Sync + 'static>(
        &self,
        view_store: PostgresViewStore,
        config: Option<EventProcessingConfig>,
    ) -> Result<()> {
        let handler = ViewEventHandler::<V>::new(view_store);
        self.start_event_handler(handler, config).await
    }
    
    /// Helper method to create a ViewStore that shares the same database connection
    pub fn create_view_store(&self) -> PostgresViewStore {
        PostgresViewStore::new(self.pool.clone())
    }

    /// Find streams that need processing and claim them
    async fn find_and_claim_streams(
        &self,
        handler_name: &str,
        config: &EventProcessingConfig,
    ) -> Result<usize> {
        // Find streams with events needing processing
        let streams = self.get_active_streams_for_handler(handler_name).await?;
        
        let mut claimed_count = 0;
        
        // Try to claim each stream
        for (stream_name, stream_id) in streams {
            if let Ok(Some(_)) = self.claim_stream(
                &stream_name,
                &stream_id,
                handler_name,
                config
            ).await {
                claimed_count += 1;
                debug!("Claimed stream {}/{} for handler {}", stream_name, stream_id, handler_name);
            }
        }
        
        Ok(claimed_count)
    }
    
    /// Process streams that have already been claimed by this node
    async fn process_claimed_streams<H: EventHandler + Send + Sync + Clone>(
        &self,
        handler: &H,
        config: &EventProcessingConfig,
    ) -> Result<usize> {
        // Find streams claimed by this node for this handler
        let claimed_streams = sqlx::query(
            r#"
            SELECT stream_name, stream_id, last_position
            FROM event_processing_claims
            WHERE handler_name = $1 AND claimed_by = $2
            "#
        )
        .bind(H::name())
        .bind(&self.node_id)
        .fetch_all(&self.pool)
        .await?;
        
        let mut processed_count = 0;
        
        // Process each claimed stream
        for row in claimed_streams {
            let stream_name: String = row.get("stream_name");
            let stream_id: String = row.get("stream_id");
            let last_position: i64 = row.get("last_position");
            
            debug!("Processing claimed stream {}/{} from position {}", 
                   stream_name, stream_id, last_position);
            
            // Process the stream's events
            match self.process_stream_events(
                handler,
                &stream_name,
                &stream_id,
                last_position,
                config
            ).await {
                Ok(position) => {
                    debug!("Successfully processed stream {}/{} up to position {}",
                           stream_name, stream_id, position);
                    
                    if let Err(e) = self.update_stream_claim_success(
                        &stream_name,
                        &stream_id,
                        H::name(),
                        position
                    ).await {
                        error!("Error updating claim success: {}", e);
                    }
                    processed_count += 1;
                },
                Err(e) => {
                    error!("Error processing stream events: {}", e);
                    if let Err(e) = self.update_stream_claim_error(
                        &stream_name,
                        &stream_id,
                        H::name(),
                        &e.to_string(),
                        config
                    ).await {
                        error!("Error updating claim error: {}", e);
                    }
                }
            }
            
            // Release the claim when done
            if let Err(e) = self.release_stream(
                &stream_name,
                &stream_id,
                H::name()
            ).await {
                error!("Error releasing stream claim: {}", e);
            }
        }
        
        Ok(processed_count)
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
        
        let handler_name = H::name().to_string();
        info!("Starting handler monitor for {}", handler_name);
        
        // Create a listener for new events
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen("new_event").await?;
        
        let store_claim = self.clone();
        let store_process = self.clone();
        let mut shutdown_claim = self.shutdown.subscribe();
        let mut shutdown_process = self.shutdown.subscribe();
        let config_claim = config.clone();
        let config_process = config.clone();
        let handler_clone = handler.clone();
        let handler_name_claim = handler_name.clone();
        let handler_name_process = handler_name.clone();
        
        // THREAD 1: Continuously look for streams to claim
        tokio::spawn(async move {
            info!("Starting claim thread for handler: {}", handler_name_claim);
            loop {
                // Break on shutdown signal
                if shutdown_claim.try_recv().is_ok() {
                    info!("Shutting down claim thread for handler: {}", handler_name_claim);
                    break;
                }
                
                match store_claim.find_and_claim_streams(&handler_name_claim, &config_claim).await {
                    Ok(claimed) => {
                        if claimed > 0 {
                            debug!("Claimed {} streams for handler: {}", claimed, handler_name_claim);
                        }
                    },
                    Err(e) => {
                        error!("Error claiming streams for handler {}: {}", handler_name_claim, e);
                    }
                }
                
                // Wait for notification or timeout
                tokio::select! {
                    _ = listener.recv() => {
                        debug!("Received notification of new event for handler: {}", handler_name_claim);
                    }
                    _ = tokio::time::sleep(config_claim.poll_interval) => {
                        // Regular polling interval
                    }
                }
            }
        });
        
        // THREAD 2: Process streams that are already claimed by this node
        tokio::spawn(async move {
            info!("Starting process thread for handler: {}", handler_name_process);
            loop {
                // Break on shutdown signal
                if shutdown_process.try_recv().is_ok() {
                    info!("Shutting down process thread for handler: {}", handler_name_process);
                    break;
                }
                
                match store_process.process_claimed_streams(&handler_clone, &config_process).await {
                    Ok(processed) => {
                        if processed > 0 {
                            debug!("Processed {} claimed streams for handler: {}", processed, handler_name_process);
                        }
                    },
                    Err(e) => {
                        error!("Error processing claimed streams for handler {}: {}", handler_name_process, e);
                    }
                }
                
                // Sleep briefly to avoid tight loop
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
        
        Ok(())
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