use crate::event_handler::{EventHandler, EventHandlerError, EventRow};
use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::Row;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use super::{EventProcessingConfig, PostgresEventStore};

/// Helper struct for processing events with a dedicated connection
pub struct EventProcessor {
    pool: sqlx::PgPool,
}

impl EventProcessor {
    /// Create a new event processor
    pub async fn new(store: &PostgresEventStore) -> Result<Self> {
        Ok(Self {
            pool: store.pool.clone(),
        })
    }

    /// Process events from a specific stream
    pub async fn process_stream<H: EventHandler>(
        &self,
        handler: H,
        stream_name: &str,
        stream_id: &str,
        last_position: i64,
        config: &EventProcessingConfig,
        handler_name: &str,
    ) -> Result<()> {
        let current_position = last_position;
        // Get events to process
        let events = self
            .get_events_for_stream(stream_name, stream_id, current_position, config.batch_size)
            .await?;

        if events.is_empty() {
            return Ok(());
        }

        // Process each event
        for event in events {
            match serde_json::from_value(event.event_data.clone()) {
                Err(_) => {
                    // Event type not handled by this handler, mark as processed
                    if let Err(e) = self.set_handler_offset(&handler_name, &event).await {
                        error!("Failed to update handler offset: {}", e);
                        break; // Stop processing this stream
                    }
                }
                Ok(parsed_event) => {
                    match handler.handle_event(parsed_event, event.clone()).await {
                        Ok(_) => {
                            if let Err(e) = self.set_handler_offset(&handler_name, &event).await {
                                error!("Failed to update handler offset: {}", e);
                                break; // Stop processing this stream
                            }
                        }
                        Err(e) => {
                            if let Err(err) = self
                                .set_processing_error(
                                    &handler_name,
                                    &event,
                                    &e.log_message,
                                    &config,
                                )
                                .await
                            {
                                error!("Failed to set processing error: {}", err);
                            }
                            break; // Stop processing this stream after a failure
                        }
                    }
                }
            }
        }

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn get_events_for_stream(
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

    /// Calculate the next retry time based on the error count and config
    fn calculate_next_retry_time(
        error_count: i32,
        config: &EventProcessingConfig,
    ) -> DateTime<Utc> {
        let delay = if error_count <= 0 {
            config.base_retry_delay
        } else {
            let backoff = std::cmp::min(
                config.max_retry_delay.as_secs(),
                config.base_retry_delay.as_secs() * 2u64.pow(error_count as u32),
            );
            Duration::from_secs(backoff)
        };

        Utc::now()
            + chrono::Duration::from_std(delay).unwrap_or_else(|_| chrono::Duration::seconds(60))
    }
    pub async fn initialize_offsets(
        &self,
        handler_name: &str,
        config: &EventProcessingConfig,
    ) -> Result<()> {
        // Check if this handler already has any offsets
        let handler_has_offsets = match self.handler_has_offsets(&handler_name).await {
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
                match self.initialize_handler_at_beginning(handler_name).await {
                    Ok(_) => info!("Handler configured to start from beginning"),
                    Err(e) => error!(
                        "Failed to initialize handler {} at beginning: {}",
                        handler_name, e
                    ),
                }
            } else if config.start_from_current {
                match self
                    .initialize_handler_at_current_position(handler_name)
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
        Ok(())
    }

    /// Check if a handler already has any offset records
    pub async fn handler_has_offsets(&self, handler_name: &str) -> Result<bool> {
        let row = sqlx::query(
            "SELECT EXISTS(SELECT 1 FROM handler_stream_offsets WHERE handler = $1) as has_offsets",
        )
        .bind(handler_name)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.get::<bool, _>("has_offsets"))
    }

    /// Initialize a handler to start from the current position for streams that don't have offsets yet
    pub async fn initialize_handler_at_current_position(
        &self,
        handler_name: &str,
    ) -> Result<usize> {
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
        info!(
            "Initialized handler {} at current position for {} new streams",
            handler_name, count
        );

        Ok(count)
    }

    /// Initialize a handler to start from the beginning for streams that don't have offsets yet
    pub async fn initialize_handler_at_beginning(&self, handler_name: &str) -> Result<()> {
        // For start from beginning, we just need to make sure no offsets exist for streams
        // that haven't been processed yet. We can leave completed streams alone.
        let result = sqlx::query("DELETE FROM handler_stream_offsets WHERE handler = $1")
            .bind(handler_name)
            .execute(&self.pool)
            .await?;

        let count = result.rows_affected() as usize;
        info!(
            "Reset handler {} to start from beginning (removed {} existing offsets)",
            handler_name, count
        );

        Ok(())
    }

    /// Update or create handler offset for an event
    #[instrument(skip(self))]
    pub async fn set_handler_offset(
        &self,
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
            .execute(&self.pool)
            .await
            .map_err(|e| EventHandlerError {
                log_message: format!("Error setting handler offset: {}", e),
            })?;

        debug!(
            "Updated offset for handler {} on {}/{} to position {}",
            handler_name, event.stream_name, event.stream_id, event.stream_position
        );

        Ok(())
    }

    /// Record error and schedule retry for a failed event
    #[instrument(skip_all)]
    pub async fn set_processing_error(
        &self,
        handler_name: &str,
        event: &EventRow,
        error: &str,
        config: &EventProcessingConfig,
    ) -> Result<(), EventHandlerError> {
        // Get current retry info
        info!(
            "Setting processing error for handler {} on {}/{}",
            handler_name, event.stream_name, event.stream_id
        );
        let row = sqlx::query(
            "SELECT retry_count, dead_lettered 
             FROM handler_stream_offsets
             WHERE handler = $1 AND stream_id = $2 AND stream_name = $3",
        )
        .bind(handler_name)
        .bind(&event.stream_id)
        .bind(&event.stream_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| EventHandlerError {
            log_message: format!("Error getting retry count: {}", e),
        })?;

        let current_retry_count: i32 = row.get("retry_count");
        let is_dead_lettered: bool = row.get("dead_lettered");
        info!(
            "Current retry count: {}, Is dead lettered: {}",
            current_retry_count, is_dead_lettered
        );
        let new_retry_count = current_retry_count + 1;
        info!("New retry count: {}", new_retry_count);

        if new_retry_count > config.max_retries {
            // Mark as dead-lettered
            warn!(
                "Marking as dead-lettered - retry count {} exceeds max retries {}",
                new_retry_count, config.max_retries
            );
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
                 RETURNING retry_count, dead_lettered",
            )
            .bind(handler_name)
            .bind(&event.stream_id)
            .bind(&event.stream_name)
            .bind(event.stream_position)
            .bind(&event.id)
            .bind(new_retry_count)
            .bind(error)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| EventHandlerError {
                log_message: format!("Error setting processing error: {}", e),
            })?;

            warn!(
                "Stream {}/{} dead-lettered for handler {} at position {}: {}",
                event.stream_name, event.stream_id, handler_name, event.stream_position, error
            );

            return Ok(());
        }

        // Schedule retry
        let next_retry_at = Self::calculate_next_retry_time(new_retry_count, config);
        debug!("Next retry at: {}", next_retry_at);
        debug!("Retry count: {}", new_retry_count);
        debug!(
            "Updating handler_stream_offsets with retry_count: {}",
            new_retry_count
        );
        let updated_row = sqlx::query(
            "UPDATE handler_stream_offsets
             SET retry_count = $4,
                 next_retry_at = $5,
                 last_error = $6,
                 last_updated_at = now()
             WHERE handler = $1 
               AND stream_id = $2 
               AND stream_name = $3
             RETURNING retry_count, next_retry_at",
        )
        .bind(handler_name)
        .bind(&event.stream_id)
        .bind(&event.stream_name)
        .bind(new_retry_count)
        .bind(next_retry_at)
        .bind(error)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| EventHandlerError {
            log_message: format!("Error setting processing error: {}", e),
        })?;

        let updated_retry_count: i32 = updated_row.get("retry_count");
        let updated_next_retry: Option<DateTime<Utc>> = updated_row.get("next_retry_at");
        debug!(
            "Updated retry count: {}, Next retry at: {:?}",
            updated_retry_count, updated_next_retry
        );

        info!(
            "Scheduled retry #{} at {} for event at position {} for {}/{}, handler={}: {}",
            new_retry_count,
            next_retry_at,
            event.stream_position,
            event.stream_name,
            event.stream_id,
            handler_name,
            error
        );

        Ok(())
    }

    pub async fn claim_streams(
        &mut self,
        handler_name: &str,
        batch_size: i32,
    ) -> Result<Vec<(String, String)>> {
        let claim_id = Uuid::new_v4().to_string();
        let claim_duration = Duration::from_secs(30); // Adjust as needed
        let claim_until = Utc::now() + chrono::Duration::from_std(claim_duration).unwrap();

        let rows = sqlx::query(
            r#"
            SELECT e.stream_name, e.stream_id
            FROM events e
            LEFT JOIN handler_stream_offsets o 
                ON o.handler = $1 
                AND o.stream_id = e.stream_id 
                AND o.stream_name = e.stream_name
            WHERE 
                (o.last_position IS NULL OR e.stream_position > o.last_position)
                AND (o.claimed_by IS NULL OR o.claimed_until < NOW())
            GROUP BY e.stream_name, e.stream_id
            LIMIT $2
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(handler_name)
        .bind(batch_size)
        .fetch_all(&self.pool)
        .await?;

        let mut claimed_streams = Vec::new();
        for row in rows {
            let stream_name: String = row.get("stream_name");
            let stream_id: String = row.get("stream_id");

            // Attempt to claim the stream
            let result = sqlx::query(
                r#"
                INSERT INTO handler_stream_offsets (handler, stream_name, stream_id, claimed_by, claimed_until)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (handler, stream_name, stream_id)
                DO UPDATE SET claimed_by = $4, claimed_until = $5
                WHERE handler_stream_offsets.claimed_by IS NULL OR handler_stream_offsets.claimed_until < NOW()
                RETURNING stream_name, stream_id
                "#,
            )
            .bind(handler_name)
            .bind(&stream_name)
            .bind(&stream_id)
            .bind(&claim_id)
            .bind(claim_until)
            .fetch_optional(&self.pool)
            .await?;

            if result.is_some() {
                // Successfully claimed, add to claimed_streams
                claimed_streams.push((stream_name, stream_id));
            }
        }

        Ok(claimed_streams)
    }
}
