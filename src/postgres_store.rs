use std::sync::Arc;
use anyhow::Result;
use sqlx::{postgres::{PgPool, PgListener}, Row};
use uuid::Uuid;
use tracing::instrument;
use tokio::sync::broadcast;
use serde_json::{self, Value as JsonValue};
use std::collections::HashMap;
use crate::event_handler::EventRow;

use crate::{
    aggregate::Aggregate,
    view::{View, StreamView, GlobalView},
    command::DomainCommand,
    event_handler::{EventHandler, EventHandlerError},
};

/// PostgreSQL-based event store implementation
#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
    shutdown: broadcast::Sender<()>,
}

impl PostgresStore {
    /// Create a new PostgreSQL store with the given connection string
    pub async fn new(connection_string: &str) -> Result<Self> {
        let pool = PgPool::connect(connection_string)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create pool: {}", e))?;

        let (shutdown, _) = broadcast::channel(1);

        Ok(Self { 
            pool,
            shutdown,
        })
    }

    /// Shutdown all background tasks
    pub async fn shutdown(&self) {
        let _ = self.shutdown.send(());
    }

    /// Build the current state for an aggregate by replaying all events
    #[instrument(skip(self))]
    pub async fn build_state<A: Aggregate>(
        &self,
        stream_id: &str,
    ) -> Result<(Option<A::State>, Option<i64>), anyhow::Error> {
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

        tracing::info!("Got state {:?}", state);
        tracing::warn!("Got position {:?}", last_position);

        Ok((state, last_position))
    }

    /// Execute a command and store the resulting events
    #[instrument(skip(self, command, service, metadata))]
    pub async fn execute_command<A: Aggregate>(
        &self,
        command: A::Command,
        service: A::Service,
        metadata: JsonValue,
    ) -> Result<String, anyhow::Error> 
    where
        A::Command: DomainCommand,
    {
        let stream_id = command.stream_id();
        tracing::info!("Executing command {:?}", stream_id);
        
        let (state, last_position) = self.build_state::<A>(&stream_id).await?;
        let events = A::execute(&state, &command, &stream_id, service)?;

        let stream_name = A::name();
        let next_position = last_position.unwrap_or(-1) + 1;

        let mut tx = self.pool.begin().await?;

        for (idx, event) in events.into_iter().enumerate() {
            let event_json = serde_json::to_value(&event)?;
            let stream_position = next_position + idx as i64;
            let id = Uuid::new_v4();

            sqlx::query(
                "INSERT INTO events (id, stream_name, stream_id, event_data, metadata, stream_position)
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(id)
            .bind(&stream_name)
            .bind(&stream_id)
            .bind(&event_json)
            .bind(&metadata)
            .bind(stream_position)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(stream_id)
    }

    /// Start an event handler that processes events
    pub async fn start_event_handler<H: EventHandler>(
        &self,
        handler: H,
    ) -> Result<(), anyhow::Error> {
        tracing::info!("Starting event handler for {}", H::name());

        // Get or create subscription record
        let subscription_id = Uuid::new_v4();
        let handler_name = H::name().to_string();

        sqlx::query(
            "INSERT INTO persistent_subscriptions (id, name, consumer_group, last_processed_position)
             VALUES ($1, $2, 'default', 0)
             ON CONFLICT (name, consumer_group) DO NOTHING"
        )
        .bind(subscription_id)
        .bind(&handler_name)
        .execute(&self.pool)
        .await?;

        // Get last processed position
        let row = sqlx::query(
            "SELECT last_processed_position 
             FROM persistent_subscriptions 
             WHERE name = $1 AND consumer_group = 'default'"
        )
        .bind(&handler_name)
        .fetch_one(&self.pool)
        .await?;
        
        let mut last_position: i64 = row.get("last_processed_position");

        // Process any events that occurred while we were down
        let rows = sqlx::query(
            "SELECT id, stream_name, stream_id, event_data, metadata, stream_position, global_position, created_at
             FROM events 
             WHERE global_position > $1 
             ORDER BY global_position"
        )
        .bind(last_position)
        .fetch_all(&self.pool)
        .await?;

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
                
                handler.handle_event(event, event_row).await
                    .map_err(|e| anyhow::anyhow!("Failed to handle event: {}", e.log_message))?;
                
                sqlx::query(
                    "UPDATE persistent_subscriptions 
                     SET last_processed_position = $1 
                     WHERE name = $2 AND consumer_group = 'default'"
                )
                .bind(row.get::<i64, _>("global_position"))
                .bind(&handler_name)
                .execute(&self.pool)
                .await?;
            }
            last_position = row.get("global_position");
        }

        // Create a listener for new events
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen("new_event").await?;

        // Start background task to process new events
        let pool = self.pool.clone();
        let handler = Arc::new(handler);
        let mut shutdown = self.shutdown.subscribe();
        let handler_name = handler_name.clone();

        // Spawn a task to handle notifications
        let connection_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Ok(notification) = listener.recv() => {
                        if let Ok(global_position) = notification.payload().parse::<i64>() {
                            if global_position > last_position {
                                // Fetch and process new events
                                let rows = match sqlx::query(
                                    "SELECT id, stream_name, stream_id, event_data, metadata, stream_position, global_position, created_at
                                     FROM events 
                                     WHERE global_position > $1 
                                     ORDER BY global_position"
                                )
                                .bind(last_position)
                                .fetch_all(&pool)
                                .await
                                {
                                    Ok(rows) => rows,
                                    Err(e) => {
                                        eprintln!("Error querying events: {}", e);
                                        continue;
                                    }
                                };

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

                                        match handler.handle_event(event, event_row).await {
                                            Ok(_) => {
                                                if let Err(e) = sqlx::query(
                                                    "UPDATE persistent_subscriptions 
                                                     SET last_processed_position = $1 
                                                     WHERE name = $2 AND consumer_group = 'default'"
                                                )
                                                .bind(row.get::<i64, _>("global_position"))
                                                .bind(&handler_name)
                                                .execute(&pool)
                                                .await {
                                                    eprintln!("Error updating subscription position: {}", e);
                                                }
                                            }
                                            Err(e) => {
                                                eprintln!("Error handling event: {}", e.log_message);
                                            }
                                        }
                                    }
                                    last_position = row.get("global_position");
                                }
                            }
                        }
                    }
                    _ = shutdown.recv() => break,
                }
            }
        });

        Ok(())
    }

    /// Get a view's state from snapshot or rebuild from events
    pub async fn get_view_state<V: StreamView>(&self, stream_id: &str) -> Result<Option<V>> {
        // Try to get from snapshot first
        if V::snapshot_frequency().is_some() {
            if let Some(row) = sqlx::query(
                "SELECT state, last_event_position 
                 FROM view_snapshots 
                 WHERE view_name = $1 AND stream_id = $2"
            )
            .bind(V::name())
            .bind(stream_id)
            .fetch_optional(&self.pool)
            .await? {
                let mut view: V = serde_json::from_value(row.get("state"))?;
                
                // Get any events after the snapshot
                let rows = sqlx::query(
                    "SELECT id, stream_name, stream_id, event_data, metadata, stream_position, global_position, created_at
                     FROM events 
                     WHERE stream_id = $1 
                     AND global_position > $2
                     ORDER BY global_position"
                )
                .bind(stream_id)
                .bind(row.get::<i64, _>("last_event_position"))
                .fetch_all(&self.pool)
                .await?;

                for row in rows {
                    let event: V::Event = serde_json::from_value(row.get("event_data"))?;
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
                    view.apply_event(&event, &event_row);
                }
                
                return Ok(Some(view));
            }
        }
        
        // No snapshot or snapshot disabled - rebuild from events
        let mut view = None;
        let rows = sqlx::query(
            "SELECT id, stream_name, stream_id, event_data, metadata, stream_position, global_position, created_at
             FROM events 
             WHERE stream_id = $1 
             ORDER BY global_position"
        )
        .bind(stream_id)
        .fetch_all(&self.pool)
        .await?;

        for row in rows {
            let event: V::Event = serde_json::from_value(row.get("event_data"))?;
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
            match view {
                None => {
                    view = V::initialize(&event, &event_row);
                }
                Some(ref mut v) => {
                    v.apply_event(&event, &event_row);
                }
            }
        }

        Ok(view)
    }

    /// Save a snapshot of the current state
    pub async fn save_view_snapshot<V: StreamView>(&self, stream_id: &str, view: &V) -> Result<()> {
        if V::snapshot_frequency().is_none() {
            return Ok(());
        }

        let state_json = serde_json::to_value(view)?;
        
        // Get the latest event position
        let row = sqlx::query(
            "SELECT MAX(global_position) as pos 
             FROM events 
             WHERE stream_id = $1"
        )
        .bind(stream_id)
        .fetch_one(&self.pool)
        .await?;
        
        let last_position: i64 = row.get("pos");
        let id = Uuid::new_v4();

        sqlx::query(
            "INSERT INTO view_snapshots (id, view_name, stream_id, state, last_event_position)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (view_name, stream_id) 
             DO UPDATE SET state = $4, last_event_position = $5"
        )
        .bind(id)
        .bind(V::name())
        .bind(stream_id)
        .bind(&state_json)
        .bind(last_position)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get multiple views' states in a single query
    pub async fn get_view_states<V: StreamView>(&self, stream_ids: &[String]) -> Result<HashMap<String, V>> {
        if stream_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut views = HashMap::new();
        
        // Try to get from snapshots first
        if V::snapshot_frequency().is_some() {
            let snapshots = sqlx::query(
                "SELECT stream_id, state, last_event_position 
                 FROM view_snapshots 
                 WHERE view_name = $1 AND stream_id = ANY($2)"
            )
            .bind(V::name())
            .bind(stream_ids)
            .fetch_all(&self.pool)
            .await?;

            // Initialize views from snapshots
            for row in snapshots {
                let view: V = serde_json::from_value(row.get("state"))?;
                views.insert(row.get("stream_id"), view);
            }

            // Get all events after the snapshots
            if !views.is_empty() {
                let rows = sqlx::query(
                    "SELECT id, stream_name, stream_id, event_data, metadata, stream_position, global_position, created_at
                     FROM events 
                     WHERE stream_id = ANY($1) 
                     AND global_position > (
                         SELECT COALESCE(MAX(last_event_position), -1)
                         FROM view_snapshots 
                         WHERE view_name = $2 
                         AND stream_id = ANY($1)
                     )
                     ORDER BY global_position"
                )
                .bind(stream_ids)
                .bind(V::name())
                .fetch_all(&self.pool)
                .await?;

                for row in rows {
                    let event: V::Event = serde_json::from_value(row.get("event_data"))?;
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
                    let stream_id: String = row.get("stream_id");
                    if let Some(view) = views.get_mut(&stream_id) {
                        view.apply_event(&event, &event_row);
                    }
                }
            }
        }

        // For any stream_ids not found in snapshots, rebuild from events
        let missing_ids: Vec<_> = stream_ids.iter()
            .filter(|id| !views.contains_key(*id))
            .map(|s| s.to_string())
            .collect();

        if !missing_ids.is_empty() {
            let rows = sqlx::query(
                "SELECT id, stream_name, stream_id, event_data, metadata, stream_position, global_position, created_at
                 FROM events 
                 WHERE stream_id = ANY($1) 
                 ORDER BY global_position"
            )
            .bind(&missing_ids)
            .fetch_all(&self.pool)
            .await?;

            for row in rows {
                let event: V::Event = serde_json::from_value(row.get("event_data"))?;
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
                let stream_id: String = row.get("stream_id");
                match views.get_mut(&stream_id) {
                    Some(view) => {
                        view.apply_event(&event, &event_row);
                    }
                    None => {
                        if let Some(view) = V::initialize(&event, &event_row) {
                            views.insert(stream_id, view);
                        }
                    }
                }
            }
        }

        Ok(views)
    }

    /// Save a snapshot of the current global state
    pub async fn save_global_snapshot<V: GlobalView>(&self, state: &V::State) -> Result<()> {
        if V::snapshot_frequency().is_none() {
            return Ok(());
        }

        let state_json = serde_json::to_value(state)?;
        
        // Get the latest event position
        let row = sqlx::query(
            "SELECT MAX(global_position) as pos 
             FROM events"
        )
        .fetch_one(&self.pool)
        .await?;
        
        let last_position: i64 = row.get("pos");
        let id = Uuid::new_v4();

        sqlx::query(
            "INSERT INTO view_snapshots (id, view_name, stream_id, state, last_event_position)
             VALUES ($1, $2, 'all_streams', $3, $4)
             ON CONFLICT (view_name, stream_id) 
             DO UPDATE SET state = $3, last_event_position = $4"
        )
        .bind(id)
        .bind(V::name())
        .bind(&state_json)
        .bind(last_position)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get a global view's state from snapshot or rebuild from events
    pub async fn get_global_state<V: GlobalView + Default>(&self) -> Result<V::State> {
        // Try to get from snapshot first
        if V::snapshot_frequency().is_some() {
            if let Some(row) = sqlx::query(
                "SELECT state, last_event_position 
                 FROM view_snapshots 
                 WHERE view_name = $1 AND stream_id = 'all_streams'"
            )
            .bind(V::name())
            .fetch_optional(&self.pool)
            .await? {
                let state: V::State = serde_json::from_value(row.get("state"))?;
                
                // Get any events after the snapshot
                let rows = sqlx::query(
                    "SELECT id, stream_name, stream_id, event_data, metadata, stream_position, global_position, created_at
                     FROM events 
                     WHERE global_position > $1
                     ORDER BY global_position"
                )
                .bind(row.get::<i64, _>("last_event_position"))
                .fetch_all(&self.pool)
                .await?;

                let mut current_state = state;
                let view = V::default();
                for row in rows {
                    let event: V::Event = serde_json::from_value(row.get("event_data"))?;
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
                    view.update_state(&mut current_state, &event, &event_row);
                }
                
                return Ok(current_state);
            }
        }
        
        // No snapshot or snapshot disabled - rebuild from events
        let mut state = V::initialize_state();
        let rows = sqlx::query(
            "SELECT id, stream_name, stream_id, event_data, metadata, stream_position, global_position, created_at
             FROM events 
             ORDER BY global_position"
        )
        .fetch_all(&self.pool)
        .await?;

        let view = V::default();
        for row in rows {
            let event: V::Event = serde_json::from_value(row.get("event_data"))?;
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
            view.update_state(&mut state, &event, &event_row);
        }

        Ok(state)
    }

    /// Get paginated views by querying events table for stream IDs first
    pub async fn get_paginated_views<V: StreamView>(
        &self,
        pagination: PaginationOptions,
    ) -> Result<PaginatedResult<(String, V)>> {
        let stream_name = V::stream_name();

        // First get total count of unique stream IDs
        let count_row = sqlx::query(
            "SELECT COUNT(DISTINCT stream_id) as count 
             FROM events 
             WHERE stream_name = $1"
        )
        .bind(&stream_name)
        .fetch_one(&self.pool)
        .await?;

        let total_count: i64 = count_row.get("count");

        if total_count == 0 {
            return Ok(PaginatedResult {
                items: vec![],
                total_count: 0,
                page: pagination.page,
                total_pages: 0,
            });
        }

        // Get paginated stream IDs
        let stream_ids = sqlx::query(
            "SELECT DISTINCT stream_id 
             FROM events 
             WHERE stream_name = $1
             ORDER BY stream_id
             LIMIT $2 OFFSET $3"
        )
        .bind(&stream_name)
        .bind(pagination.page_size)
        .bind(pagination.page * pagination.page_size)
        .fetch_all(&self.pool)
        .await?;

        // Build views for each stream ID
        let mut items = Vec::with_capacity(stream_ids.len());
        for row in stream_ids {
            let stream_id: String = row.get("stream_id");
            if let Some(view) = self.get_view_state::<V>(&stream_id).await? {
                items.push((stream_id, view));
            }
        }

        Ok(PaginatedResult {
            items,
            total_count,
            page: pagination.page,
            total_pages: (total_count as f64 / pagination.page_size as f64).ceil() as i64,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PaginationOptions {
    pub page: i64,
    pub page_size: i64,
}

pub struct PaginatedResult<T> {
    pub items: Vec<T>,
    pub total_count: i64,
    pub page: i64,
    pub total_pages: i64,
}
