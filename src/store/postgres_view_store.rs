use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{self, json, Value as JsonValue};
use sqlx::{postgres::PgPool, Row};
use std::collections::HashMap;
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use crate::{
    event_handler::EventRow,
    store::event_store::{PaginatedResult, PaginationOptions},
    store::view_store::ViewStore,
    view::View,
    view_event_handler::ViewEventHandler,
};

/// Type to track stream positions in a type-safe way
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StreamPositions {
    positions: HashMap<String, i64>,
}

impl StreamPositions {
    /// Create a new empty StreamPositions
    pub fn new() -> Self {
        Self {
            positions: HashMap::new(),
        }
    }

    /// Get position for a stream
    pub fn get_position(&self, stream_name: &str, stream_id: &str) -> Option<i64> {
        let key = format!("{}:{}", stream_name, stream_id);
        self.positions.get(&key).copied()
    }

    /// Set position for a stream
    pub fn set_position(&mut self, stream_name: &str, stream_id: &str, position: i64) {
        let key = format!("{}:{}", stream_name, stream_id);
        self.positions.insert(key, position);
    }

    /// Convert from raw JSONB Value
    pub fn from_json(json: JsonValue) -> Result<Self> {
        // If it's already in our struct format, deserialize directly
        if let Ok(positions) = serde_json::from_value::<Self>(json.clone()) {
            return Ok(positions);
        }

        // Otherwise, handle the legacy format (a plain JSON object)
        let mut result = Self::new();

        if let Some(obj) = json.as_object() {
            for (key, value) in obj {
                if let Some(pos_str) = value.as_str() {
                    if let Ok(pos) = pos_str.parse::<i64>() {
                        result.positions.insert(key.clone(), pos);
                    }
                }
            }
        }

        Ok(result)
    }
}

/// PostgreSQL-based view store implementation
#[derive(Clone)]
pub struct PostgresViewStore {
    pool: PgPool,
}

impl PostgresViewStore {
    /// Create a new PostgreSQL view store with the given connection pool
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Check if a table exists in the database
    async fn table_exists(&self, table_name: &str) -> Result<bool> {
        let row = sqlx::query(
            "SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = $1
            )",
        )
        .bind(table_name)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.get::<bool, _>(0))
    }

    /// Reset a view by deleting all its snapshots and handler offsets
    /// This will cause the view to be rebuilt from scratch when restarted
    #[instrument(skip(self))]
    pub async fn reset_view<V: View>(&self) -> Result<()> {
        let view_name = V::name();
        info!("Resetting view: {}", view_name);

        // Start a transaction to ensure both operations succeed or fail together
        let mut tx = self.pool.begin().await?;

        // Delete all view snapshots for this view
        let deleted_snapshots = sqlx::query("DELETE FROM view_snapshots WHERE view_name = $1")
            .bind(view_name)
            .execute(&mut *tx)
            .await?;

        // Delete all handler offsets for this view
        let deleted_offsets = sqlx::query("DELETE FROM handler_stream_offsets WHERE handler = $1")
            .bind(view_name)
            .execute(&mut *tx)
            .await?;

        // Commit the transaction
        tx.commit().await?;

        info!(
            "Reset view {}: deleted {} snapshots and {} handler offsets",
            view_name,
            deleted_snapshots.rows_affected(),
            deleted_offsets.rows_affected()
        );

        Ok(())
    }

    /// Create an event handler for a view
    /// This is primarily used for testing
    pub fn create_event_handler<V: View + Default + 'static>(&self) -> ViewEventHandler<V> {
        ViewEventHandler::new(self.clone())
    }

    /// Get all partition keys for a specific view
    #[instrument(skip(self))]
    pub async fn get_all_partitions<V: View>(&self) -> Result<Vec<String>> {
        let view_name = V::name();

        let rows = sqlx::query("SELECT partition_key FROM view_snapshots WHERE view_name = $1")
            .bind(view_name)
            .fetch_all(&self.pool)
            .await?;

        let partition_keys = rows
            .into_iter()
            .map(|row| row.get("partition_key"))
            .collect();

        Ok(partition_keys)
    }

    /// Get partition keys for a view based on a query condition on the state
    ///
    /// This allows filtering partitions based on properties in their state
    /// For example, to get all partitions where the state has a specific user_id:
    #[instrument(skip(self, params))]
    pub async fn get_partitions_by_query<V: View>(
        &self,
        condition: &str,
        params: Vec<JsonValue>,
    ) -> Result<Vec<String>> {
        let view_name = V::name();

        let query = format!(
            "SELECT partition_key 
             FROM view_snapshots 
             WHERE view_name = $1 
             AND {}",
            condition
        );

        let mut query_builder = sqlx::query(&query).bind(view_name);
        for param in &params {
            query_builder = query_builder.bind(param);
        }

        let rows = query_builder.fetch_all(&self.pool).await?;

        let partition_keys = rows
            .into_iter()
            .map(|row| row.get("partition_key"))
            .collect();

        Ok(partition_keys)
    }
}

#[async_trait]
impl ViewStore for PostgresViewStore {
    #[instrument(skip(self))]
    async fn initialize(&self) -> Result<()> {
        // Check if the view_snapshots table exists
        let view_snapshots_exist = self.table_exists("view_snapshots").await?;

        if !view_snapshots_exist {
            return Err(anyhow!("The view_snapshots table doesn't exist. Please run initialization for the event store first."));
        }

        info!("View store initialized successfully");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn get_view_state<V: View>(&self, partition_key: &str) -> Result<Option<V>> {
        let row = sqlx::query(
            "SELECT state
             FROM view_snapshots 
             WHERE view_name = $1 AND partition_key = $2",
        )
        .bind(V::name())
        .bind(partition_key)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => {
                debug!("Retrieved view state");
                Ok(Some(serde_json::from_value(row.get("state"))?))
            }
            None => {
                debug!("View state not found");
                Ok(None)
            }
        }
    }

    #[instrument(skip(self, event_row), fields(
        stream_name = %event_row.stream_name,
        stream_id = %event_row.stream_id,
        stream_position = event_row.stream_position
    ), level = "debug")]
    async fn is_event_processed<V: View>(
        &self,
        partition_key: &str,
        event_row: &EventRow,
    ) -> Result<bool> {
        // Check view_snapshots.processed_stream_positions to see if this specific view
        // has processed this event for this partition
        let row = sqlx::query(
            "SELECT processed_stream_positions
             FROM view_snapshots 
             WHERE view_name = $1 
             AND partition_key = $2",
        )
        .bind(V::name())
        .bind(partition_key)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            let positions_json: JsonValue = row.get("processed_stream_positions");
            let positions = StreamPositions::from_json(positions_json)?;

            if let Some(position) =
                positions.get_position(&event_row.stream_name, &event_row.stream_id)
            {
                if event_row.stream_position <= position {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    #[instrument(skip(self, view), fields(
        stream_name = %event_row.stream_name,
        stream_id = %event_row.stream_id,
        stream_position = event_row.stream_position
    ), level = "debug")]
    async fn save_view_state<V: View>(
        &self,
        partition_key: &str,
        view: &V,
        event_row: &EventRow,
    ) -> Result<()> {
        let state_json = serde_json::to_value(view)?;
        let id = Uuid::new_v4().to_string();

        // Just save the view state without tracking positions
        let query = r#"
            INSERT INTO view_snapshots 
            (id, view_name, partition_key, state)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (view_name, partition_key) 
            DO UPDATE SET state = $4"#;

        sqlx::query(query)
            .bind(&id)
            .bind(V::name())
            .bind(partition_key)
            .bind(&state_json)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    #[instrument(skip(self, params, pagination))]
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

        let mut count_builder = sqlx::query(&count_query).bind(view_name);
        for param in &params {
            count_builder = count_builder.bind(param);
        }

        let count_row = count_builder.fetch_one(&self.pool).await?;
        let total_count: i64 = count_row.get("count");

        if total_count == 0 {
            debug!("No views found matching condition");
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
            query_builder = query_builder.bind(page_size).bind(page * page_size);
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

    #[instrument(skip(self), level = "debug")]
    async fn get_view_by_user_id<V: View>(&self, user_id: &str) -> Result<Option<V>> {
        let res = self
            .query_views::<V>(
                "state->>'user_id' = ($2#>>'{}')::text",
                vec![json!(user_id)],
                None,
            )
            .await?;

        if !res.items.is_empty() {
            debug!("Found view for user");
            Ok(res.items.first().cloned())
        } else {
            debug!("No view found for user");
            Ok(None)
        }
    }

    #[instrument(skip(self), level = "debug")]
    async fn get_views_by_user_id<V: View>(&self, user_id: &str) -> Result<Vec<V>> {
        let res = self
            .query_views::<V>(
                "state->>'user_id' = ($2#>>'{}')::text",
                vec![json!(user_id)],
                None,
            )
            .await?;

        debug!(count = res.items.len(), "Retrieved views for user");
        Ok(res.items)
    }

    #[instrument(skip(self), level = "debug")]
    async fn get_all_views<V: View>(&self) -> Result<Vec<V>> {
        let res = self.query_views::<V>("true", vec![], None).await?;

        debug!(count = res.items.len(), "Retrieved all views");
        Ok(res.items)
    }

    /// Get the state of a view for a specific stream
    #[instrument(skip(self), level = "debug")]
    async fn get_view_state_by_stream<V: View + Default>(
        &self,
        stream_name: &str,
        stream_id: &str,
        partition_key: &str,
    ) -> Result<Option<V>> {
        // First get the view state
        let view = self.get_view_state::<V>(partition_key).await?;

        // Then check if it belongs to the specified stream
        if let Some(view) = view {
            debug!("Retrieved view state by stream");
            Ok(Some(view))
        } else {
            debug!("View state not found for stream");
            Ok(None)
        }
    }

    /// Save a view state with a stream position
    #[instrument(skip(self, state))]
    async fn save_view_state_with_position<V: View + Default>(
        &self,
        stream_name: &str,
        stream_id: &str,
        partition_key: &str,
        state: &V,
        position: i64,
    ) -> Result<()> {
        let state_json = serde_json::to_value(state)?;
        let id = Uuid::new_v4().to_string();

        // Check if there's an existing record to get current positions
        let existing_row = sqlx::query(
            "SELECT processed_stream_positions 
             FROM view_snapshots 
             WHERE view_name = $1 AND partition_key = $2",
        )
        .bind(V::name())
        .bind(partition_key)
        .fetch_optional(&self.pool)
        .await?;

        // Initialize our positions, either from existing data or as new
        let mut positions = if let Some(row) = existing_row {
            let positions_json: JsonValue = row.get("processed_stream_positions");
            let positions = StreamPositions::from_json(positions_json)?;
            debug!(
                stream_name = %stream_name,
                stream_id = %stream_id,
                "Loaded existing positions: {:?}", positions
            );
            positions
        } else {
            debug!("No existing positions found, creating new");
            StreamPositions::new()
        };

        // Update with the new position
        positions.set_position(stream_name, stream_id, position);

        // Serialize positions
        let positions_json = serde_json::to_value(&positions)?;

        // Do the upsert with our prepared positions
        let query = r#"
            INSERT INTO view_snapshots 
            (id, view_name, partition_key, state, processed_stream_positions)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (view_name, partition_key) 
            DO UPDATE SET 
                state = $4, 
                processed_stream_positions = $5
            "#;

        sqlx::query(query)
            .bind(&id)
            .bind(V::name())
            .bind(partition_key)
            .bind(&state_json)
            .bind(&positions_json)
            .execute(&self.pool)
            .await?;

        debug!("View state saved successfully with position");
        Ok(())
    }

    /// Get the position of a view
    #[instrument(skip(self))]
    async fn get_view_state_position<V: View + Default>(
        &self,
        partition_key: &str,
        stream_name: &str,
        stream_id: &str,
    ) -> Result<Option<i64>> {
        // Get the processed_stream_positions for this view
        let row = sqlx::query(
            "SELECT processed_stream_positions
             FROM view_snapshots 
             WHERE view_name = $1 AND partition_key = $2",
        )
        .bind(V::name())
        .bind(partition_key)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            let positions_json: JsonValue = row.get("processed_stream_positions");
            let positions = StreamPositions::from_json(positions_json)?;

            if let Some(position) = positions.get_position(stream_name, stream_id) {
                debug!(position, "Found position for stream");
                return Ok(Some(position));
            }
        }

        debug!("No position found for stream");
        Ok(None)
    }

    /// Wait for a view to catch up to a specific event
    #[instrument(skip(self), level = "debug")]
    async fn wait_for_view<V: View + Default>(
        &self,
        event: &EventRow,
        timeout_ms: u64,
    ) -> Result<()> {
        let stream_name = &event.stream_name;
        let stream_id = &event.stream_id;
        let position = event.stream_position;
        let handler_name = V::name();

        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_millis(timeout_ms);

        debug!(
            "Waiting for view {} to catch up to event in stream {}/{} at position {}",
            handler_name, stream_name, stream_id, position
        );

        loop {
            if start.elapsed() > timeout {
                return Err(anyhow!(
                    "Timeout waiting for view {} to catch up to position {} for stream {}/{}",
                    handler_name,
                    position,
                    stream_name,
                    stream_id
                ));
            }

            // Check handler_stream_offsets table for the current position
            let row = sqlx::query(
                "SELECT last_position 
                 FROM handler_stream_offsets 
                 WHERE handler = $1 
                   AND stream_name = $2 
                   AND stream_id = $3",
            )
            .bind(&handler_name)
            .bind(stream_name)
            .bind(stream_id)
            .fetch_optional(&self.pool)
            .await?;

            match row {
                Some(row) => {
                    let current_position: i64 = row.get("last_position");
                    debug!(
                        "Waiting for view to catch up: current_position={}, target_position={}",
                        current_position, position
                    );

                    if current_position >= position {
                        debug!(
                            "View {} caught up to position {} for stream {}/{}",
                            handler_name, position, stream_name, stream_id
                        );
                        return Ok(());
                    }
                }
                None => {
                    debug!("No handler offset found yet, waiting...");
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    /// Wait for a view to catch up to all events in all streams
    /// This will check if there are any unprocessed events for the view
    #[instrument(skip(self))]
    async fn wait_for_view_to_catch_up<V: View + Default>(&self, timeout_ms: u64) -> Result<()> {
        let handler_name = V::name();
        info!(
            "Waiting for view {} to catch up to all events",
            handler_name
        );

        // Find the latest event in the database
        let latest_event = sqlx::query(
            "SELECT id, stream_name, stream_id, event_data, metadata, stream_position, created_at
             FROM events
             ORDER BY stream_position DESC
             LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = latest_event {
            let event = EventRow {
                id: row.get("id"),
                stream_name: row.get("stream_name"),
                stream_id: row.get("stream_id"),
                event_data: row.get("event_data"),
                metadata: row.get("metadata"),
                stream_position: row.get("stream_position"),
                created_at: row.get("created_at"),
            };

            info!(
                "Found latest event at position {} in stream {}/{}",
                event.stream_position, event.stream_name, event.stream_id
            );

            // Wait for the view to catch up to this event
            self.wait_for_view::<V>(&event, timeout_ms).await
        } else {
            info!("No events found in the database, view is already caught up");
            Ok(())
        }
    }
}
