use std::time::{Duration, Instant};
use anyhow::{Result, anyhow};
use sqlx::{postgres::PgPool, Row};
use tracing::{info, error, debug};
use serde_json::{self, Value as JsonValue, json};
use uuid::Uuid;
use tokio::time::sleep;
use async_trait::async_trait;

use crate::{
    store::view_store::ViewStore,
    store::event_store::{PaginationOptions, PaginatedResult},
    view::View,
    event_handler::EventRow,
};

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
            )"
        )
        .bind(table_name)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.get::<bool, _>(0))
    }
    
    /// Generate a stream key for JSONB lookup
    fn get_stream_key(stream_name: &str, stream_id: &str) -> String {
        format!("{}:{}", stream_name, stream_id)
    }
}

#[async_trait]
impl ViewStore for PostgresViewStore {
    async fn initialize(&self) -> Result<()> {
        // Check if the view_snapshots table exists
        let view_snapshots_exist = self.table_exists("view_snapshots").await?;
        
        if !view_snapshots_exist {
            return Err(anyhow!("The view_snapshots table doesn't exist. Please run initialization for the event store first."));
        }
        
        Ok(())
    }
    
    async fn get_view_state<V: View>(&self, partition_key: &str) -> Result<Option<V>> {
        let row = sqlx::query(
            "SELECT state, last_processed_global_position, processed_stream_positions
             FROM view_snapshots 
             WHERE view_name = $1 AND partition_key = $2"
        )
        .bind(V::name())
        .bind(partition_key)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => {
                let position: i64 = row.get("last_processed_global_position");
                debug!("Retrieved view {} partition {} at global position {}", 
                       V::name(), partition_key, position);
                Ok(Some(serde_json::from_value(row.get("state"))?))
            },
            None => {
                debug!("View {} partition {} not found", V::name(), partition_key);
                Ok(None)
            }
        }
    }
    
    async fn is_event_processed<V: View>(
        &self,
        partition_key: &str,
        event_row: &EventRow,
    ) -> Result<bool> {
        // Create a unique key for this stream
        let stream_key = Self::get_stream_key(&event_row.stream_name, &event_row.stream_id);
        
        // Check if we have already processed this event for this stream
        let row = sqlx::query(
            "SELECT processed_stream_positions->>$3 as stream_position
             FROM view_snapshots 
             WHERE view_name = $1 
             AND partition_key = $2"
        )
        .bind(V::name())
        .bind(partition_key)
        .bind(&stream_key)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            if let Some(position_str) = row.get::<Option<String>, _>("stream_position") {
                if let Ok(last_position) = position_str.parse::<i64>() {
                    // If the event's position is less than or equal to what we've already processed, skip it
                    if event_row.stream_position <= last_position {
                        debug!("Skipping already processed event for {}/{}: event pos={}, last processed={}",
                              event_row.stream_name, event_row.stream_id, event_row.stream_position, last_position);
                        return Ok(true);
                    }
                }
            }
        }
        
        Ok(false)
    }
    
    async fn save_view_state<V: View>(
        &self,
        partition_key: &str,
        view: &V,
        event_row: &EventRow,
    ) -> Result<()> {
        let state_json = serde_json::to_value(view)?;
        let id = Uuid::new_v4().to_string();
        let stream_key = Self::get_stream_key(&event_row.stream_name, &event_row.stream_id);
        
        // We need to update the processed_stream_positions object with the new stream position
        // Using PostgreSQL's jsonb_set function for this
        sqlx::query(
            r#"
            INSERT INTO view_snapshots 
            (id, view_name, partition_key, state, last_processed_global_position, processed_stream_positions)
            VALUES ($1, $2, $3, $4, $5, jsonb_build_object($6, $7::text))
            ON CONFLICT (view_name, partition_key) 
            DO UPDATE SET 
                state = $4, 
                last_processed_global_position = $5,
                processed_stream_positions = 
                    view_snapshots.processed_stream_positions || jsonb_build_object($6, $7::text)
            "#
        )
        .bind(&id)
        .bind(V::name())
        .bind(partition_key)
        .bind(&state_json)
        .bind(event_row.global_position)
        .bind(&stream_key)
        .bind(event_row.stream_position.to_string())
        .execute(&self.pool)
        .await?;

        Ok(())
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
    
    async fn wait_for_view<V: View + Default>(&self, partition_key: &str, target_position: i64, timeout_ms: u64) -> Result<()> {
        let start = Instant::now();
        let timeout = Duration::from_millis(timeout_ms);
        let view_name = V::name();
        
        info!("Waiting for view {view_name} partition {partition_key} to reach position {target_position}");
        
        loop {
            // Check if the view has processed up to our target position
            let row = sqlx::query(
                "SELECT last_processed_global_position 
                 FROM view_snapshots 
                 WHERE view_name = $1 AND partition_key = $2"
            )
            .bind(view_name.clone())
            .bind(partition_key)
            .fetch_optional(&self.pool)
            .await?;

            // Return success if target position has been reached
            if let Some(row) = row {
                let last_position: i64 = row.get("last_processed_global_position");
                if last_position >= target_position {
                    info!("View {view_name} partition {partition_key} reached target position {target_position}");
                    return Ok(());
                }
            } else {
                debug!("View {view_name} partition {partition_key} not found yet");
            }

            // Check for timeout
            if start.elapsed() > timeout {
                // Check if target event exists for better error diagnosis
                let event_exists = sqlx::query("SELECT 1 FROM events WHERE global_position = $1")
                    .bind(target_position)
                    .fetch_optional(&self.pool)
                    .await?
                    .is_some();
                
                error!("Timeout waiting for view {view_name} partition {partition_key} to reach position {target_position}. Target event exists: {event_exists}");
                return Err(anyhow!("Timeout waiting for view to catch up"));
            }

            sleep(Duration::from_millis(50)).await;
        }
    }
} 