use anyhow::anyhow;
use async_trait::async_trait;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::{
    event_handler::{EventHandler, EventHandlerError, EventRow},
    store::{postgres_view_store::PostgresViewStore, view_store::ViewStore},
    view::{PartitionUpdateStrategy, View},
};

/// Adapter that implements EventHandler for Views
#[derive(Clone)]
pub struct ViewEventHandler<V: View> {
    view_store: PostgresViewStore,
    _phantom: PhantomData<V>,
}

impl<V: View + Default> ViewEventHandler<V> {
    pub fn new(view_store: PostgresViewStore) -> Self {
        Self {
            view_store,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<V: View + Default + 'static> EventHandler for ViewEventHandler<V> {
    type Events = V::Event;

    fn name() -> &'static str {
        V::name()
    }

    async fn handle_event(
        &self,
        event: Self::Events,
        event_row: EventRow,
    ) -> Result<(), EventHandlerError> {
        // Add debug logging
        tracing::debug!(
            "ViewEventHandler::handle_event - View: {}, stream: {}, id: {}, pos: {}",
            V::name(),
            event_row.stream_name,
            event_row.stream_id,
            event_row.stream_position
        );

        // Determine which partitions should be updated based on the strategy
        let update_strategy = V::determine_affected_partitions(&event, &event_row);

        let partition_keys = match update_strategy {
            PartitionUpdateStrategy::All => {
                tracing::debug!(
                    "ViewEventHandler - Updating all partitions for view {}",
                    V::name()
                );

                // Get all partitions and process the event for each one
                self.view_store
                    .get_all_partitions::<V>()
                    .await
                    .map_err(|e| EventHandlerError {
                        log_message: format!("Failed to fetch partitions: {}", e),
                    })?
            }

            PartitionUpdateStrategy::Query { condition } => {
                tracing::debug!(
                    "ViewEventHandler - Updating partitions matching: {}",
                    condition
                );

                // Get matching partitions and process the event for each one
                self.view_store
                    .get_partitions_by_query::<V>(&condition, vec![])
                    .await
                    .map_err(|e| EventHandlerError {
                        log_message: format!("Failed to fetch partitions by query: {}", e),
                    })?
            }

            PartitionUpdateStrategy::SpecificPartition { key } => {
                tracing::debug!("ViewEventHandler - Processing for partition: {}", key);
                vec![key]
            }

            PartitionUpdateStrategy::None => {
                tracing::debug!("ViewEventHandler - Skipping event for view {}", V::name());
                return Ok(());
            }
        };

        // Process the event for all identified partitions
        for key in partition_keys {
            if let Err(e) = self
                .process_event_for_partition(&event, &event_row, &key)
                .await
            {
                return Err(e);
            }
        }

        Ok(())
    }
}

impl<V: View + Default + 'static> ViewEventHandler<V> {
    // Helper method to process an event for a specific partition
    async fn process_event_for_partition(
        &self,
        event: &V::Event,
        event_row: &EventRow,
        partition_key: &str,
    ) -> Result<(), EventHandlerError> {
        // Check if event has already been processed
        let already_processed = match self
            .view_store
            .is_event_processed::<V>(partition_key, event_row)
            .await
        {
            Ok(processed) => processed,
            Err(e) => {
                return Err(EventHandlerError {
                    log_message: format!("Failed to check if event was processed: {}", e),
                })
            }
        };

        if already_processed {
            // Skip processing this event
            tracing::debug!(
                "ViewEventHandler - Event already processed for view {} partition {}",
                V::name(),
                partition_key
            );
            return Ok(());
        }

        tracing::debug!(
            "ViewEventHandler - Event not yet processed, getting current state or initializing"
        );

        // Get current state or initialize
        let mut view = match self.view_store.get_view_state::<V>(partition_key).await {
            Ok(Some(state)) => {
                tracing::debug!(
                    "ViewEventHandler - Found existing view state for partition {}",
                    partition_key
                );
                state
            }
            Ok(None) => {
                // Initialize new view
                tracing::debug!("ViewEventHandler - No existing view state, initializing new view");
                match V::initialize(event, event_row) {
                    Some(new_view) => new_view,
                    None => {
                        tracing::debug!(
                            "ViewEventHandler - View initialization returned None, skipping"
                        );
                        return Ok(()); // Skip if can't initialize
                    }
                }
            }
            Err(e) => {
                return Err(EventHandlerError {
                    log_message: format!("Failed to get view state: {}", e),
                })
            }
        };

        // Apply the event and save
        tracing::debug!(
            "ViewEventHandler - Applying event to view {} partition {}",
            V::name(),
            partition_key
        );
        view.apply_event(event, event_row);

        tracing::debug!(
            "ViewEventHandler - Saving updated view state for {} partition {}",
            V::name(),
            partition_key
        );
        if let Err(e) = self
            .view_store
            .save_view_state_with_position::<V>(
                &event_row.stream_name,
                &event_row.stream_id,
                partition_key,
                &view,
                event_row.stream_position,
            )
            .await
        {
            return Err(EventHandlerError {
                log_message: format!("Failed to save view state: {}", e),
            });
        }

        tracing::debug!(
            "ViewEventHandler - Successfully processed event for view {} partition {}",
            V::name(),
            partition_key
        );
        Ok(())
    }
}
