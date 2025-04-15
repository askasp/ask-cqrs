use std::time::Duration;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value as JsonValue;
use chrono::{DateTime, Utc};

use crate::{
    view::View,
    store::event_store::{PaginationOptions, PaginatedResult},
    event_handler::EventRow,
};

/// Core trait defining the View Store interface
#[async_trait]
pub trait ViewStore: Send + Sync + Clone {
    /// Initialize the view store
    async fn initialize(&self) -> Result<()>;
    
    /// Get a view state for a partition
    async fn get_view_state<V: View>(&self, partition_key: &str) -> Result<Option<V>>;
    
    /// Save a view state for a partition
    async fn save_view_state<V: View>(
        &self,
        partition_key: &str,
        view: &V,
        event_row: &EventRow,
    ) -> Result<()>;
    
    /// Check if an event has already been processed by this view partition
    async fn is_event_processed<V: View>(
        &self,
        partition_key: &str,
        event_row: &EventRow,
    ) -> Result<bool>;
    
    /// Query views by criteria in their state
    async fn query_views<V: View>(
        &self,
        condition: &str,
        params: Vec<JsonValue>,
        pagination: Option<PaginationOptions>,
    ) -> Result<PaginatedResult<V>>;
    
    /// Wait for a view to catch up to a specific event
    async fn wait_for_view<V: View + Default>(
        &self,
        event: &EventRow,
        timeout_ms: u64,
    ) -> Result<()> {
        // Default implementation that always succeeds
        Ok(())
    }
    
    /// Get a single view by user_id field
    async fn get_view_by_user_id<V: View>(&self, user_id: &str) -> Result<Option<V>> {
        let res = self.query_views::<V>(
            "state->>'user_id' = ($2#>>'{}')::text",
            vec![JsonValue::String(user_id.to_string())],
            None,
        ).await?;
        
        if !res.items.is_empty() {
            Ok(res.items.first().cloned())
        } else {
            Ok(None)
        }
    }
    
    /// Get all views for a given user_id
    async fn get_views_by_user_id<V: View>(&self, user_id: &str) -> Result<Vec<V>> {
        let res = self.query_views::<V>(
            "state->>'user_id' = ($2#>>'{}')::text",
            vec![JsonValue::String(user_id.to_string())],
            None,
        ).await?;
        
        Ok(res.items)
    }
    
    /// Get all views without filtering
    async fn get_all_views<V: View>(&self) -> Result<Vec<V>> {
        let res = self.query_views::<V>("true", vec![], None).await?;
        Ok(res.items)
    }

    /// Get the state of a view for a specific stream
    async fn get_view_state_by_stream<V: View + Default>(
        &self,
        _stream_name: &str,
        _stream_id: &str, 
        partition_key: &str
    ) -> Result<Option<V>> {
        // Default implementation that just delegates to get_view_state
        self.get_view_state::<V>(partition_key).await
    }

    /// Save a view state with a stream position
    async fn save_view_state_with_position<V: View + Default>(
        &self,
        stream_name: &str,
        stream_id: &str,
        partition_key: &str,
        state: &V,
        position: i64,
    ) -> Result<()> {
        // Default implementation that just delegates to save_view_state
        self.save_view_state::<V>(
            partition_key, 
            state, 
            &EventRow {
                id: uuid::Uuid::new_v4().to_string(),
                stream_name: stream_name.to_string(),
                stream_id: stream_id.to_string(),
                event_data: JsonValue::Null,
                metadata: JsonValue::Null,
                stream_position: position,
                created_at: chrono::Utc::now(),
            }
        ).await
    }

    /// Get the position of a view
    async fn get_view_state_position<V: View + Default>(
        &self,
        _partition_key: &str,
        _stream_name: &str,
        _stream_id: &str,
    ) -> Result<Option<i64>> {
        // Default implementation that always returns None
        Ok(None)
    }

    /// Wait for a view to catch up to all events in all streams
    /// This will check if there are any unprocessed events for the view
    async fn wait_for_view_to_catch_up<V: View + Default>(
        &self,
        _timeout_ms: u64,
    ) -> Result<()> {
        // Default implementation that always succeeds
        Ok(())
    }
} 