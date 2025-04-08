use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::{self, json, Value as JsonValue};
use sqlx::postgres::PgPoolOptions;
use sqlx::{
    postgres::{PgListener, PgPool},
     PgExecutor, Row,
};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, info_span, instrument, trace, warn, Span};
use uuid::Uuid;
use std::collections::HashMap;
use futures::future::join_all;

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

/// PostgreSQL-based event store implementation
#[derive(Clone)]
pub struct PostgresEventStore {
    pub pool: PgPool,
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

        Ok(Self { pool })
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
            Ok(_) => {
                debug!("Successfully subscribed to 'new_event' notification channel");
            }
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

            // Create an event processor with a dedicated connection
            let mut processor = match EventProcessor::new(&store).await {
                Ok(processor) => processor,
                Err(e) => {
                    error!("Failed to create event processor: {}", e);
                    return;
                }
            };

            // Check if this handler already has any offsets
            let handler_has_offsets = match processor.handler_has_offsets(&handler_name).await {
                Ok(has_offsets) => has_offsets,
                Err(e) => {
                    error!(
                        "Failed to check if handler {} has offsets: {}",
                        handler_name, e
                    );
                    false
                }
            };

            // Only initialize if the handler doesn't have offsets yet
            if !handler_has_offsets {
                // Set the initial processing strategy based on config
                if config.start_from_beginning {
                    match processor.initialize_handler_at_beginning::<H>().await {
                        Ok(_) => info!("Handler configured to start from beginning"),
                        Err(e) => error!(
                            "Failed to initialize handler {} at beginning: {}",
                            handler_name, e
                        ),
                    }
                } else if config.start_from_current {
                    match processor
                        .initialize_handler_at_current_position::<H>()
                        .await
                    {
                        Ok(count) => info!(
                            "Handler {} initialized at current position for {} new streams",
                            handler_name, count
                        ),
                        Err(e) => error!(
                            "Failed to initialize handler {} at current position: {}",
                            handler_name, e
                        ),
                    }
                }
            } else {
                info!(
                    "Handler {} already has offsets, skipping initialization",
                    handler_name
                );
            }

            // Generate handler lock key
            let handler_lock_key = {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                format!("handler:{}", handler_name).hash(&mut hasher);
                hasher.finish() as i64
            };

            let max_concurrent_streams = 10;

            loop {
                // Try to acquire handler lock
                let acquired = match sqlx::query("SELECT pg_try_advisory_lock($1)")
                    .bind(handler_lock_key)
                    .fetch_one(&mut *processor.conn)
                    .await
                {
                    Ok(row) => row.get::<bool, _>(0),
                    Err(e) => {
                        error!("Error acquiring handler lock: {}", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                if !acquired {
                    info!(
                        "Handler {} is being processed by another node",
                        handler_name
                    );
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }

                info!("Acquired lock for handler {}", handler_name);

                // Process events in order by stream
                let events: Vec<EventRow> = processor.find_events_to_process(&handler_name, max_concurrent_streams).await.unwrap();
                println!("Found {} events to process", events.len());

                if events.is_empty() {
                    // Release lock and wait for notifications as before
                    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                        .bind(handler_lock_key)
                        .execute(&mut *processor.conn)
                        .await;

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
                    continue;
                }

                // Group events by stream
                let mut events_by_stream: HashMap<(String, String), Vec<EventRow>> = HashMap::new();
                for event in events {
                    events_by_stream
                        .entry((event.stream_name.clone(), event.stream_id.clone()))
                        .or_default()
                        .push(event);
                }

                // Process each stream's events in order
                for ((_stream_name, _stream_id), stream_events) in events_by_stream {
                    // Process events in this stream sequentially until error
                    println!("Processing stream: {}/{}", _stream_name, _stream_id);
                    
                    for event in stream_events {
                        match serde_json::from_value(event.event_data.clone()) {
                            Err(_) => {
                                // Event type not handled by this handler, mark as processed
                                if let Err(e) = processor.set_handler_offset(&handler_name, &event).await {
                                    error!("Failed to update handler offset: {}", e);
                                    break; // Stop processing this stream
                                }
                            }
                            Ok(parsed_event) => {
                                match handler.handle_event(parsed_event, event.clone()).await {
                                    Ok(_) => {
                                        if let Err(e) = processor.set_handler_offset(&handler_name, &event).await {
                                            error!("Failed to update handler offset: {}", e);
                                            break; // Stop processing this stream
                                        }
                                    }
                                    Err(e) => {
                                        if let Err(err) = processor.set_processing_error(&handler_name, &event, &e.log_message, &config).await {
                                            error!("Failed to set processing error: {}", err);
                                        }
                                        break; // Stop processing this stream after a failure
                                    }
                                }
                            }
                        }
                    }
                }

                // Release handler lock before next iteration
                let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                    .bind(handler_lock_key)
                    .execute(&mut *processor.conn)
                    .await;
            }
        });

        Ok(())
    }
    async fn initialize(&self) -> Result<()> {
        // Read schema file
        let schema = include_str!("../../src/schema.sql");

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
