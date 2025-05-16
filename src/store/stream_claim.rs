use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::Row;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use tracing::debug;
use tracing::{error, info};

use super::PostgresEventStore;

/// Represents the state of a claim for stream processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamClaim {
    pub stream_name: String,
    pub stream_id: String,
    pub handler_name: String,
    pub last_position: i64,
    pub claimed_by: Option<String>,
    pub claim_expires_at: Option<DateTime<Utc>>,
    pub error_count: i32,
    pub last_error: Option<String>,
    pub next_retry_at: Option<DateTime<Utc>>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for StreamClaim {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(StreamClaim {
            stream_name: row.try_get("stream_name")?,
            stream_id: row.try_get("stream_id")?,
            handler_name: row.try_get("handler_name")?,
            last_position: row.try_get("last_position")?,
            claimed_by: row.try_get("claimed_by")?,
            claim_expires_at: row.try_get("claim_expires_at")?,
            error_count: row.try_get("error_count")?,
            last_error: row.try_get("last_error")?,
            next_retry_at: row.try_get("next_retry_at")?,
        })
    }
}

pub struct StreamClaimer {
    pool: sqlx::PgPool,
    handler_name: String,
}

impl StreamClaimer {
    pub async fn new(store: &PostgresEventStore, handler_name: String) -> Result<Self> {
        Ok(Self {
            pool: store.pool.clone(),
            handler_name,
        })
    }

    pub async fn start_claiming(&self, stream_sender: mpsc::Sender<StreamClaim>, node_id: &str) {
        let mut interval = time::interval(Duration::from_secs(2)); // Adjust as needed

        // Create a listener for PostgreSQL notifications
        let mut listener = sqlx::postgres::PgListener::connect_with(&self.pool)
            .await
            .unwrap();
        listener.listen("new_event").await.unwrap();

        info!("Listening for new event notifications");

        loop {
            // Try to claim streams without waiting first
            match self.claim_streams(10, node_id).await {
                Ok(streams) => {
                    debug!("Claimed {} streams", streams.len());
                    if streams.is_empty() {
                        // Wait for either the interval or a notification
                        tokio::select! {
                            _ = interval.tick() => {
                                debug!("No streams to claim, interval tick");
                            },
                           _ = listener.recv() => {
                                continue;
                            }
                        }
                    } else {
                        for stream in streams {
                            if stream_sender.send(stream).await.is_err() {
                                return; // Channel closed, exit the loop
                            }
                        }
                        // Continue immediately to check for more streams
                    }
                }
                Err(e) => {
                    error!("Error claiming streams: {}", e);
                    interval.tick().await; // Wait before retrying
                }
            }
        }
    }

    pub async fn release_claim(&self, stream_name: &str, stream_id: &str) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE handler_stream_offsets
            SET claimed_by = NULL,
                claimed_until = NULL
            WHERE handler = $1
                AND stream_name = $2
                AND stream_id = $3
                AND claimed_by IS NOT NULL
            RETURNING 1
            "#,
        )
        .bind(&self.handler_name)
        .bind(stream_name)
        .bind(stream_id)
        .fetch_optional(&self.pool)
        .await?;

        debug!("Released claim for stream: {} {}", stream_name, stream_id);

        Ok(result.is_some())
    }

    async fn claim_streams(&self, batch_size: i32, node_id: &str) -> Result<Vec<StreamClaim>> {
        let claim_duration = Duration::from_secs(30); // Adjust as needed
        let claim_until = Utc::now() + chrono::Duration::from_std(claim_duration).unwrap();
        debug!("Claiming streams until: {}", claim_until);

        let rows = match sqlx::query(
            r#"
            SELECT e.stream_name, e.stream_id, COALESCE(o.last_position, -1) as last_position
            FROM events e
            LEFT JOIN handler_stream_offsets o 
                ON o.handler = $1 
                AND o.stream_id = e.stream_id 
                AND o.stream_name = e.stream_name
            WHERE 
                (o.last_position IS NULL OR e.stream_position > o.last_position)
                AND (o.claimed_by IS NULL OR o.claimed_until < NOW())
                AND e.stream_position > COALESCE(o.last_position, -1)
            LIMIT $2
            "#,
        )
        .bind(&self.handler_name)
        .bind(batch_size)
        .fetch_all(&self.pool)
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                error!("Error fetching streams: {}", e);
                return Err(e.into());
            }
        };

        debug!("Found {} streams to claim", rows.len());

        let mut claimed_streams = Vec::new();
        for row in rows {
            let stream_name: String = row.get("stream_name");
            let stream_id: String = row.get("stream_id");
            let last_position: i64 = row.get("last_position");

            // Attempt to claim the stream
            let result = sqlx::query(
                r#"
                INSERT INTO handler_stream_offsets (handler, stream_name, stream_id, claimed_by, claimed_until, last_position)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (handler, stream_name, stream_id)
                DO UPDATE SET claimed_by = $4, claimed_until = $5
                WHERE handler_stream_offsets.claimed_by IS NULL OR handler_stream_offsets.claimed_until < NOW()
                RETURNING stream_name, stream_id, last_position
                "#,
            )
            .bind(&self.handler_name)
            .bind(&stream_name)
            .bind(&stream_id)
            .bind(node_id)
            .bind(claim_until)
            .bind(last_position)
            .fetch_optional(&self.pool)
            .await?;

            if let Some(row) = result {
                // Successfully claimed, add to claimed_streams
                info!("Claimed stream: {} {}", stream_name, stream_id);
                claimed_streams.push(StreamClaim {
                    stream_name,
                    stream_id,
                    handler_name: self.handler_name.clone(),
                    last_position,
                    claimed_by: Some(node_id.to_string()),
                    claim_expires_at: Some(claim_until),
                    error_count: 0,
                    last_error: None,
                    next_retry_at: None,
                });
            }
        }

        Ok(claimed_streams)
    }
}
