use std::sync::Arc;
use anyhow::Result;
use sqlx::{postgres::{PgPool, PgListener}, Row, Executor};
use uuid::Uuid;
use tracing::instrument;
use tokio::sync::broadcast;
use serde_json::{self, Value as JsonValue};
use std::collections::HashMap;
use crate::event_handler::EventRow;
use async_trait::async_trait;

use crate::{
    aggregate::Aggregate,
    view::{View },
    command::DomainCommand,
    event_handler::{EventHandler, EventHandlerError},
};

/// Result type for command execution
#[derive(Debug, Clone)]
pub struct CommandResult {
    pub stream_id: String,
    pub global_position: i64,
}

/// PostgreSQL-based event store implementation
#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
    shutdown: broadcast::Sender<()>,
}

impl PostgresStore {
    /// Check if the event store schema exists
    async fn check_schema_exists(pool: &PgPool) -> Result<bool> {
        let row = sqlx::query(
            "SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'events'
            )"
        )
        .fetch_one(pool)
        .await?;

        Ok(row.get::<bool, _>(0))
    }

    /// Create a new PostgreSQL store with the given connection string
    pub async fn new(connection_string: &str) -> Result<Self> {
        let pool = PgPool::connect(connection_string)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create pool: {}", e))?;

        // Only initialize schema if this is a new database
        if !Self::check_schema_exists(&pool).await? {
            tracing::info!("Initializing new event store schema");
            let schema = include_str!("schema.sql");
            let mut tx = pool.begin().await?;
            tx.execute(schema)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to initialize schema: {}", e))?;
            tx.commit().await?;
        }

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
    ) -> Result<CommandResult, anyhow::Error> 
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
            .bind(stream_name)
            .bind(&stream_id)
            .bind(event_json)
            .bind(metadata.clone())
            .bind(stream_position)
            .fetch_one(&mut *tx)
            .await?;

            final_global_position = row.get("global_position");
        }

        tx.commit().await?;

        Ok(CommandResult {
            stream_id,
            global_position: final_global_position,
        })
    }

    /// Wait for a view to catch up to a specific global position
    pub async fn wait_for_view<V: View>(&self, partition_key: &str, target_position: i64, timeout_ms: u64) -> Result<(), anyhow::Error> {
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_millis(timeout_ms);

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

            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!("Timeout waiting for view to catch up"));
            }

            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Get or create a subscription, with configurable initial position
    async fn get_or_create_subscription(
        &self,
        name: &str,
        start_from_beginning: bool,
    ) -> Result<(Uuid, i64)> {
        // Get current event position if needed
        let initial_position = if !start_from_beginning {
            let row = sqlx::query(
                "SELECT COALESCE(MAX(global_position), -1) as position FROM events"
            )
            .fetch_one(&self.pool)
            .await?;
            row.get::<i64, _>("position")
        } else {
            -1 // Start from beginning
        };

        // Create subscription if it doesn't exist
        let subscription_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO persistent_subscriptions (id, name, consumer_group, last_processed_position)
             VALUES ($1, $2, 'default', $3)
             ON CONFLICT (name, consumer_group) DO NOTHING"
        )
        .bind(subscription_id)
        .bind(name)
        .bind(initial_position)
        .execute(&self.pool)
        .await?;

        // Get subscription's last position
        let row = sqlx::query(
            "SELECT id, last_processed_position::BIGINT as last_processed_position 
             FROM persistent_subscriptions 
             WHERE name = $1 AND consumer_group = 'default'"
        )
        .bind(name)
        .fetch_one(&self.pool)
        .await?;

        Ok((
            row.get("id"),
            row.get("last_processed_position")
        ))
    }

    /// Process a batch of events through a handler
    async fn process_events<H>(
        &self,
        handler: &H,
        last_position: i64,
        stream_filter: Option<&[&str]>,
    ) -> Result<i64> 
    where 
        H: EventHandler + Send + Sync,
    {
        let mut query = String::from(
            "SELECT id, stream_name, stream_id, event_data, metadata, stream_position, global_position, created_at
             FROM events 
             WHERE global_position > $1"
        );
        
        if let Some(streams) = stream_filter {
            query.push_str(" AND stream_name = ANY($2)");
        }
        query.push_str(" ORDER BY global_position");

        let mut query_builder = sqlx::query(&query).bind(last_position);
        if let Some(streams) = stream_filter {
            query_builder = query_builder.bind(streams);
        }

        let rows = query_builder.fetch_all(&self.pool).await?;
        
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
                
                handler.handle_event(event, event_row).await?;
                final_position = row.get("global_position");
            }
        }

        Ok(final_position)
    }

    /// Start a subscription that processes events
    async fn start_subscription<H>(
        &self,
        handler: H,
        name: &str,
        stream_filter: Option<&[&str]>,
    ) -> Result<()>
    where
        H: EventHandler + Send + Sync + 'static,
    {
        tracing::info!("Starting subscription for {}", name);

        // Get or create subscription starting from current position for event handlers
        let (subscription_id, mut last_position) = self.get_or_create_subscription(
            name,
            false // Start from current position for event handlers
        ).await?;

        // Create a listener for new events BEFORE processing existing ones
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen("new_event").await?;

        // Process existing events
        last_position = self.process_events(
            &handler,
            last_position,
            stream_filter
        ).await?;

        // Update subscription position after catching up
        self.update_subscription_position(name, last_position).await?;

        // Start background task to process new events
        let pool = self.pool.clone();
        let handler = Arc::new(handler);
        let mut shutdown = self.shutdown.subscribe();
        let name = name.to_string();
        let stream_filter = stream_filter.map(|s| s.iter().map(|&s| s.to_string()).collect::<Vec<_>>());

        tokio::spawn(async move {
            let mut last_position = last_position; // Make mutable copy
            loop {
                tokio::select! {
                    Ok(notification) = listener.recv() => {
                        if let Ok(global_position) = notification.payload().parse::<i64>() {
                            if global_position > last_position {
                                let mut query = String::from(
                                    "SELECT id, stream_name, stream_id, event_data, metadata, stream_position, global_position, created_at
                                     FROM events 
                                     WHERE global_position > $1"
                                );
                                
                                if stream_filter.is_some() {
                                    query.push_str(" AND stream_name = ANY($2)");
                                }
                                query.push_str(" ORDER BY global_position");

                                let mut query_builder = sqlx::query(&query).bind(last_position);
                                if let Some(ref streams) = stream_filter {
                                    query_builder = query_builder.bind(streams);
                                }

                                match query_builder.fetch_all(&pool).await {
                                    Ok(rows) => {
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

                                                if let Err(e) = handler.handle_event(event, event_row).await {
                                                    eprintln!("Error handling event: {}", e);
                                                }
                                            }
                                            last_position = row.get("global_position");

                                            // Update subscription position after each event
                                            if let Err(e) = sqlx::query(
                                                "UPDATE persistent_subscriptions 
                                                 SET last_processed_position = $1 
                                                 WHERE name = $2 AND consumer_group = 'default'"
                                            )
                                            .bind(last_position)
                                            .bind(name.as_str())
                                            .execute(&pool)
                                            .await {
                                                eprintln!("Error updating subscription position: {}", e);
                                            }
                                        }
                                    }
                                    Err(e) => eprintln!("Error querying events: {}", e),
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

    /// Process a batch of view events
    async fn process_view_events<V>(
        &self,
        view: &V,
        name: &str,
        last_position: i64,
    ) -> Result<i64>
    where
        V: View + Send + Sync,
    {
        let query = format!(
            "SELECT id, stream_name, stream_id, event_data, metadata, stream_position, global_position, created_at
             FROM events 
             WHERE global_position > $1
             ORDER BY global_position"
        );

        let rows = sqlx::query(&query)
            .bind(last_position)
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

                if let Some(partition_key) = V::get_partition_key(&event, &event_row) {
                    self.process_view_event::<V>(&event, &event_row, name, &partition_key).await?;
                }
            }
            final_position = row.get("global_position");
        }

        Ok(final_position)
    }

    /// Process a single view event
    async fn process_view_event<V>(
        &self,
        event: &V::Event,
        event_row: &EventRow,
        name: &str,
        partition_key: &str,
    ) -> Result<()>
    where
        V: View + Send + Sync,
    {
        // Load or initialize view for this partition
        let row = sqlx::query(
            "SELECT state, last_event_position 
             FROM view_snapshots 
             WHERE view_name = $1 AND partition_key = $2"
        )
        .bind(name)
        .bind(partition_key)
        .fetch_optional(&self.pool)
        .await?;

        // Skip if we've already processed this event
        if let Some(ref row) = row {
            let last_event_position: i64 = row.get("last_event_position");
            if last_event_position >= event_row.global_position {
                return Ok(());
            }
        }

        // Process the event
        let mut view = match row {
            Some(row) => serde_json::from_value(row.get("state"))?,
            None => V::initialize(event, event_row).expect("Failed to initialize view"),
        };
        
        view.apply_event(event, event_row);

        // Save view state
        let state_json = serde_json::to_value(&view)?;
        let id = Uuid::new_v4();

        sqlx::query(
            "INSERT INTO view_snapshots (id, view_name, partition_key, state, last_event_position)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (view_name, partition_key) 
             DO UPDATE SET state = $4, last_event_position = $5"
        )
        .bind(id)
        .bind(name)
        .bind(partition_key)
        .bind(&state_json)
        .bind(event_row.global_position)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Start a view subscription that processes events and maintains view state
    async fn start_view_subscription<V>(
        &self,
        view: V,
        name: &str,
    ) -> Result<()>
    where
        V: View + Send + Sync + 'static,
    {
        tracing::info!("Starting view subscription for {}", name);

        // Get or create subscription starting from beginning for views
        let (subscription_id, mut last_position) = self.get_or_create_subscription(
            name,
            true // Start from beginning for views
        ).await?;

        // Create a listener for new events BEFORE processing existing ones
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen("new_event").await?;

        // Process existing events
        last_position = self.process_view_events(&view, name, last_position).await?;

        // Update subscription position after catching up
        self.update_subscription_position(name, last_position).await?;

        // Start background task to process new events
        let pool = self.pool.clone();
        let mut shutdown = self.shutdown.subscribe();
        let name = name.to_string();
        let view = Arc::new(view);

        tokio::spawn(async move {
            let mut last_position = last_position;
            loop {
                tokio::select! {
                    Ok(notification) = listener.recv() => {
                        if let Ok(global_position) = notification.payload().parse::<i64>() {
                            if global_position > last_position {
                                let query = format!(
                                    "SELECT id, stream_name, stream_id, event_data, metadata, stream_position, global_position, created_at
                                     FROM events 
                                     WHERE global_position > $1
                                     ORDER BY global_position"
                                );

                                match sqlx::query(&query)
                                    .bind(last_position)
                                    .fetch_all(&pool)
                                    .await 
                                {
                                    Ok(rows) => {
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

                                                if let Some(partition_key) = V::get_partition_key(&event, &event_row) {
                                                    // Load or initialize view for this partition
                                                    match sqlx::query(
                                                        "SELECT state, last_event_position 
                                                         FROM view_snapshots 
                                                         WHERE view_name = $1 AND partition_key = $2"
                                                    )
                                                    .bind(name.as_str())
                                                    .bind(&partition_key)
                                                    .fetch_optional(&pool)
                                                    .await {
                                                        Ok(row) => {
                                                            // Skip if we've already processed this event
                                                            if let Some(ref row) = row {
                                                                let last_event_position: i64 = row.get("last_event_position");
                                                                if last_event_position >= event_row.global_position {
                                                                    continue;
                                                                }
                                                            }

                                                            // Process the event
                                                            let view = match row {
                                                                Some(row) => match serde_json::from_value(row.get("state")) {
                                                                    Ok(view) => view,
                                                                    Err(e) => {
                                                                        eprintln!("Error deserializing view state: {}", e);
                                                                        continue;
                                                                    }
                                                                },
                                                                None => V::initialize(&event, &event_row).expect("Failed to initialize view"),
                                                            };
                                                            
                                                            let mut view = view;
                                                            view.apply_event(&event, &event_row);

                                                            // Save view state
                                                            match serde_json::to_value(&view) {
                                                                Ok(state_json) => {
                                                                    let id = Uuid::new_v4();
                                                                    if let Err(e) = sqlx::query(
                                                                        "INSERT INTO view_snapshots (id, view_name, partition_key, state, last_event_position)
                                                                         VALUES ($1, $2, $3, $4, $5)
                                                                         ON CONFLICT (view_name, partition_key) 
                                                                         DO UPDATE SET state = $4, last_event_position = $5"
                                                                    )
                                                                    .bind(id)
                                                                    .bind(name.as_str())
                                                                    .bind(&partition_key)
                                                                    .bind(&state_json)
                                                                    .bind(event_row.global_position)
                                                                    .execute(&pool)
                                                                    .await {
                                                                        eprintln!("Error saving view state: {}", e);
                                                                    }
                                                                }
                                                                Err(e) => eprintln!("Error serializing view state: {}", e),
                                                            }
                                                        }
                                                        Err(e) => eprintln!("Error loading view state: {}", e),
                                                    }
                                                }
                                            }
                                            last_position = row.get("global_position");

                                            // Update subscription position after each event
                                            if let Err(e) = sqlx::query(
                                                "UPDATE persistent_subscriptions 
                                                 SET last_processed_position = $1 
                                                 WHERE name = $2 AND consumer_group = 'default'"
                                            )
                                            .bind(last_position)
                                            .bind(name.as_str())
                                            .execute(&pool)
                                            .await {
                                                eprintln!("Error updating subscription position: {}", e);
                                            }
                                        }
                                    }
                                    Err(e) => eprintln!("Error querying events: {}", e),
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

    /// Start an event handler that processes events
    pub async fn start_event_handler<H: EventHandler + Send + Sync + 'static>(
        &self,
        handler: H,
    ) -> Result<()> {
        // Event handlers start from current position to avoid reprocessing old events
        self.start_subscription(
            handler,
            H::name(),
            None, // No stream filter
        ).await
    }

    /// Start a view builder that processes events
    pub async fn start_view_builder<V>(&self) -> Result<()>
    where
        V: View + Default + Send + Sync + 'static,
    {
        // Views start from beginning to build complete state
        self.start_view_subscription(
            V::default(),
            V::name().as_str(),
        ).await
    }

    /// Get a view's state from snapshot
    pub async fn get_view_state<V: View>(&self, partition_key: &str) -> Result<Option<V>> {
        let row = sqlx::query(
            "SELECT state 
             FROM view_snapshots 
             WHERE view_name = $1 AND partition_key = $2"
        )
        .bind(V::name().as_str())
        .bind(partition_key)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => Ok(Some(serde_json::from_value(row.get("state"))?)),
            None => Ok(None)
        }
    }

     /// Query views by criteria in their state
    pub async fn query_views<V: View>(
        &self,
        condition: &str,
        params: Vec<serde_json::Value>,
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

        let mut count_builder = sqlx::query(&count_query).bind(view_name.as_str());
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

        let mut query_builder = sqlx::query(&query).bind(view_name.as_str());
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

    /// Update subscription position
    async fn update_subscription_position(
        &self,
        name: &str,
        position: i64,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE persistent_subscriptions 
             SET last_processed_position = $1 
             WHERE name = $2 AND consumer_group = 'default'"
        )
        .bind(position)
        .bind(name)
        .execute(&self.pool)
        .await?;

        Ok(())
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
