use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Row;

/// Represents the state of a claim for stream processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamClaim {
    pub id: String,
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
            id: row.try_get("id")?,
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
