use std::time::Duration;
use anyhow::Result ;
use sqlx::{postgres::{PgPool, PgListener}, postgres::PgRow, Row, Executor, PgExecutor};
use tracing::{debug, info, instrument, warn};
use chrono::{DateTime, Utc};

use crate::{
    event_handler::{EventHandler, EventRow, EventHandlerError},
};

use super::PostgresEventStore;

/// Helper struct for processing events with a dedicated connection
pub struct EventProcessor {
    pub conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
}

impl EventProcessor {
    /// Create a new event processor with a dedicated connection
    pub async fn new(store: &PostgresEventStore) -> Result<Self> {
        let conn = store.pool.acquire().await?;
        Ok(Self {  conn })
    }

   
    #[instrument(skip(self))]
    pub async fn get_events_for_stream(
        &mut self, 
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
        .fetch_all(&mut *self.conn)
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
    
   
    /// Calculate the next retry time based on the error count and config
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
    
    /// Check if a handler already has any offset records
    pub async fn handler_has_offsets(&mut self, handler_name: &str) -> Result<bool> {
        let row = sqlx::query(
            "SELECT EXISTS(SELECT 1 FROM handler_stream_offsets WHERE handler = $1) as has_offsets"
        )
        .bind(handler_name)
        .fetch_one(&mut *self.conn)
        .await?;
        
        Ok(row.get::<bool, _>("has_offsets"))
    }
    
    /// Initialize a handler to start from the current position for streams that don't have offsets yet
    pub async fn initialize_handler_at_current_position<H: EventHandler>(&mut self) -> Result<usize> {
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
        .execute(&mut *self.conn)
        .await?;
        
        let count = result.rows_affected() as usize;
        info!("Initialized handler {} at current position for {} new streams", handler_name, count);
        
        Ok(count)
    }
    
    /// Initialize a handler to start from the beginning for streams that don't have offsets yet
    pub async fn initialize_handler_at_beginning<H: EventHandler>(&mut self) -> Result<()> {
        let handler_name = H::name();
        
        // For start from beginning, we just need to make sure no offsets exist for streams
        // that haven't been processed yet. We can leave completed streams alone.
        let result = sqlx::query(
            "DELETE FROM handler_stream_offsets WHERE handler = $1"
        )
        .bind(handler_name)
        .execute(&mut *self.conn)
        .await?;
        
        let count = result.rows_affected() as usize;
        info!("Reset handler {} to start from beginning (removed {} existing offsets)", handler_name, count);
        
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn find_events_to_process(
        &mut self,
        handler_name: &str,
        batch_size: i32,
    ) -> Result<Vec<EventRow>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                e.id,
                e.stream_name,
                e.stream_id,
                e.event_data,
                e.metadata,
                e.stream_position,
                e.created_at,
                o.next_retry_at IS NOT NULL as is_retry,
                COALESCE(o.retry_count, 0) as retry_count,
                COALESCE(o.dead_lettered, false) as dead_lettered
            FROM events e
            LEFT JOIN handler_stream_offsets o 
                ON o.handler = $1 
                AND o.stream_id = e.stream_id 
                AND o.stream_name = e.stream_name
            WHERE 
                (
                    (o.last_position IS NULL OR e.stream_position > o.last_position)
                    OR (o.next_retry_at IS NOT NULL AND o.next_retry_at <= now() AND e.stream_position > o.last_position)
                )
                AND (o.dead_lettered IS NULL OR o.dead_lettered = false)
            ORDER BY e.stream_name, e.stream_id, e.stream_position
            LIMIT $2
            "#,
        )
        .bind(handler_name)
        .bind(batch_size)
        .fetch_all(&mut *self.conn)
        .await?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let retry_count: i32 = row.get("retry_count");
            let is_dead_lettered: bool = row.get("dead_lettered");
            println!("Found event to process - Position: {}, Retry count: {}, Is dead lettered: {}", 
                row.get::<i64, _>("stream_position"), retry_count, is_dead_lettered);
            
            events.push(
                EventRow {
                    id: row.get("id"),
                    stream_name: row.get("stream_name"),
                    stream_id: row.get("stream_id"),
                    event_data: row.get("event_data"),
                    metadata: row.get("metadata"),
                    stream_position: row.get("stream_position"),
                    created_at: row.get("created_at"),
                },
            );
        }

        debug!(
            "Found {} events to process for handler {}",
            events.len(),
            handler_name
        );
        Ok(events)
    }

    /// Update or create handler offset for an event
    #[instrument(skip(self))]
    pub async fn set_handler_offset(
        &mut self,
        handler_name: &str,
        event: &EventRow,
    ) -> Result<(), EventHandlerError> {
        let query = r#"
        INSERT INTO handler_stream_offsets 
            (handler, stream_name, stream_id, last_position, retry_count, next_retry_at, last_error, last_updated_at)
        VALUES 
            ($1, $2, $3, $4, 0, NULL, NULL, NOW())
        ON CONFLICT (handler, stream_name, stream_id) 
        DO UPDATE SET 
            last_position = $4,
            retry_count = 0,
            next_retry_at = NULL,
            last_error = NULL,
            last_updated_at = NOW()
        "#;

        sqlx::query(query)
            .bind(handler_name)
            .bind(&event.stream_name)
            .bind(&event.stream_id)
            .bind(event.stream_position)
            .execute(&mut *self.conn)
            .await.map_err(|e| EventHandlerError { log_message: format!("Error setting handler offset: {}", e) })?;

        debug!(
            "Updated offset for handler {} on {}/{} to position {}",
            handler_name, event.stream_name, event.stream_id, event.stream_position
        );

        Ok(())
    }

    /// Record error and schedule retry for a failed event
    #[instrument(skip(self, config))]
    pub async fn set_processing_error(
        &mut self,
        handler_name: &str,
        event: &EventRow,
        error: &str,
        config: &EventProcessingConfig,
    ) -> Result<(), EventHandlerError> {
        // Get current retry info
        println!("Setting processing error for handler {} on {}/{}", handler_name, event.stream_name, event.stream_id);
        let row = sqlx::query(
            "SELECT retry_count, dead_lettered 
             FROM handler_stream_offsets
             WHERE handler = $1 AND stream_id = $2 AND stream_name = $3"
        )
        .bind(handler_name)
        .bind(&event.stream_id)
        .bind(&event.stream_name)
        .fetch_one(&mut *self.conn)
        .await.map_err(|e| EventHandlerError { log_message: format!("Error getting retry count: {}", e) })?;
        
        let current_retry_count: i32 = row.get("retry_count");
        let is_dead_lettered: bool = row.get("dead_lettered");
        println!("Current retry count: {}, Is dead lettered: {}", current_retry_count, is_dead_lettered);
        let new_retry_count = current_retry_count + 1;
        println!("New retry count: {}", new_retry_count);
        
        if new_retry_count > config.max_retries {
            // Mark as dead-lettered
            println!("Marking as dead-lettered - retry count {} exceeds max retries {}", new_retry_count, config.max_retries);
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
                   AND stream_name = $3
                 RETURNING retry_count, dead_lettered"
            )
            .bind(handler_name)
            .bind(&event.stream_id)
            .bind(&event.stream_name)
            .bind(event.stream_position)
            .bind(&event.id)
            .bind(new_retry_count)
            .bind(error)
            .fetch_one(&mut *self.conn)
            .await.map_err(|e| EventHandlerError { log_message: format!("Error setting processing error: {}", e) })?;
            
            warn!(
                "Stream {}/{} dead-lettered for handler {} at position {}: {}",
                event.stream_name, event.stream_id, handler_name, event.stream_position, error
            );
            
            return Ok(());
        }
        
        // Schedule retry
        let next_retry_at = Self::calculate_next_retry_time(new_retry_count, config);
        println!("Next retry at: {}", next_retry_at);
        println!("Retry count: {}", new_retry_count);
        println!("about to update handler_stream_offsets with retry_count: {}", new_retry_count);
        let updated_row = sqlx::query(
            "UPDATE handler_stream_offsets
             SET retry_count = $4,
                 next_retry_at = $5,
                 last_error = $6,
                 last_updated_at = now()
             WHERE handler = $1 
               AND stream_id = $2 
               AND stream_name = $3
             RETURNING retry_count, next_retry_at"
        )
        .bind(handler_name)
        .bind(&event.stream_id)
        .bind(&event.stream_name)
        .bind(new_retry_count)
        .bind(next_retry_at)
        .bind(error)
        .fetch_one(&mut *self.conn)
        .await.map_err(|e| EventHandlerError { log_message: format!("Error setting processing error: {}", e) })?;
        
        let updated_retry_count: i32 = updated_row.get("retry_count");
        let updated_next_retry: Option<DateTime<Utc>> = updated_row.get("next_retry_at");
        println!("Updated retry count: {}, Next retry at: {:?}", updated_retry_count, updated_next_retry);
        
        info!(
            "Scheduled retry #{} at {} for event at position {} for {}/{}, handler={}: {}",
            new_retry_count, next_retry_at, event.stream_position, event.stream_name, 
            event.stream_id, handler_name, error
        );
        
        Ok(())
    }
}

