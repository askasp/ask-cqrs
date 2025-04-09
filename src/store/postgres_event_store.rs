use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::{self, Value as JsonValue};
use sqlx::postgres::PgPoolOptions;
use sqlx::{
    postgres::{PgListener, PgPool},
    Row,
};
use tokio::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, info_span, instrument, trace, warn, Span};
use uuid::Uuid;
use std::collections::HashMap;
use sqlx::postgres::{ PgConnectOptions};
use sqlx::{ConnectOptions, Executor};

use crate::store::{StreamClaim, StreamClaimer};
use crate::view_event_handler::ViewEventHandler;
use crate::{
    aggregate::Aggregate,
    command::DomainCommand,
    event_handler::{EventHandler, EventHandlerError, EventRow},
    store::event_store::{
        CommandResult, EventProcessingConfig, EventStore, PaginatedResult, PaginationOptions,
    },
    view::View,
};

use super::event_processor::EventProcessor;
use super::PostgresViewStore;
const SCHEMA: &str = include_str!("../../src/schema.sql");

/// PostgreSQL-based event store implementation
#[derive(Clone)]
pub struct PostgresEventStore {
    pub pool: PgPool,
    pub node_id: String,
}

impl PostgresEventStore {
    /// Create a new PostgreSQL event store with the given connection string
    pub async fn new(connection_string: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(50) // Still reasonable for a 2-core system
            .acquire_timeout(Duration::from_secs(15))
            .idle_timeout(Duration::from_secs(60))
            .connect(connection_string)
            .await?;


        let node_id = Uuid::new_v4().to_string();
        Ok(Self { pool, node_id })
    }
    pub fn get_pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn create_view_store(&self) -> PostgresViewStore {
        PostgresViewStore::new(self.pool.clone())
    }
    pub async fn start_view<V: View + Default>(&self, view_store: PostgresViewStore, config: Option<EventProcessingConfig>) -> Result<()> {
        let view_handler = ViewEventHandler::<V>::new(view_store);
        let mut config = config.unwrap_or_default();
        config.start_from_beginning = true;
        self.start_event_handler(view_handler, Some(config)).await?;
        Ok(())
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
        limit: i32,
    ) -> Result<Vec<EventRow>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                id, stream_name, stream_id, event_data, metadata, stream_position, created_at
            FROM events 
            WHERE stream_name = $1 AND stream_id = $2 AND stream_position > $3
            ORDER BY stream_position
            LIMIT $4
            "#,
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
        let handler = handler.clone();
        let node_id = self.node_id.clone();

        let store = Arc::new(self.clone());

        // Start a single processing thread
        tokio::spawn(async move {
            let span = info_span!("event_processor", handler = %handler_name);
            let _guard = span.enter();

            info!("Starting event processor for handler: {}", handler_name);

            // Create an event processor with a dedicated connection
            let  processor = match EventProcessor::new(&store).await {
                Ok(processor) => processor,
                Err(e) => {
                    error!("Failed to create event processor: {}", e);
                    return;
                }
            };
            let _ = processor.initialize_offsets(&handler_name, &config).await;

           
            let (sender, mut receiver) = mpsc::channel::<StreamClaim>(100); // Capacity of 100
            let stream_claimer = StreamClaimer::new(&store, handler_name.clone()).await.unwrap();
            let arc_stream_claimer = Arc::new(stream_claimer);
            let arc_stream_claimer_clone = arc_stream_claimer.clone();
            info!("Starting stream claimer for handler: {}", handler_name);
            tokio::spawn(async move {
                info!("Starting stream claimer for handler in thread");
                arc_stream_claimer_clone.start_claiming(sender, &node_id).await;
            });
            
            // Create a processor that can be cloned
            let processor = Arc::new(processor);
            
            while let Some(stream_claim) = receiver.recv().await {
                // Clone necessary data for the task
                let handler_name_clone = handler_name.clone();
                let config_clone = config.clone();
                let processor_clone = Arc::clone(&processor);
                let handler_clone = handler.clone();

                let stream_claimer_clone = arc_stream_claimer.clone();
                
                // Spawn a task to process this specific stream
                tokio::spawn(async move {
                    let _ = processor_clone.process_stream::<H>(
                        handler_clone,
                        &stream_claim.stream_name,
                        &stream_claim.stream_id,
                        stream_claim.last_position,
                        &config_clone,
                        &handler_name_clone
                    ).await;
                    let _ = stream_claimer_clone.release_claim(&stream_claim.stream_name, &stream_claim.stream_id).await;
                });
            }
        });
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
             ORDER BY stream_position",
        )
        .bind(&stream_name)
        .bind(stream_id)
        .fetch_all(&self.pool)
        .await?;

        let mut state = None;
        let mut last_position = None;

        trace!(
            event_count = rows.len(),
            "Retrieved events for rebuilding state"
        );

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

pub async fn initialize_database(connection_string: &str) -> Result<()> {
        // Build the connection string for the new database
        
        // Now connect to the new database and initialize schema
    warn!("Initializing database");
    let db_pool = PgPool::connect(connection_string).await?;

    db_pool.execute(SCHEMA).await.unwrap();
    warn!("Database initialized");

    Ok(())
}
