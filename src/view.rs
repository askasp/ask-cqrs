use crate::event_handler::EventRow;
use serde::{de::DeserializeOwned, Serialize};

/// Trait for implementing a read model/view
///
/// Views provide a way to create read-optimized projections of event data.
/// Each view can subscribe to events from multiple aggregates and build a specialized
/// representation of the data.
///
/// Events are guaranteed to be processed:
/// - Exactly once per view partition (no duplicate processing)
/// - In correct order within a single aggregate's stream (stream_position order)
/// - Consistently across partitions for the same view
///
/// This is achieved by tracking which specific events have been processed
/// by each view partition in a transaction with the view state update.
pub trait View: Clone + Send + Sync + 'static + DeserializeOwned + Serialize {
    /// The event type this view processes
    type Event: Clone + Send + Sync + 'static + DeserializeOwned + Serialize + std::fmt::Debug;

    /// Name of the view, used for logging and debugging
    /// Should return a static string to allow compatibility with EventHandler trait
    fn name() -> &'static str;

    /// Get the partition key for this event
    /// This determines how the view state is split across rows
    /// Return None to skip this event
    fn get_partition_key(event: &Self::Event, event_row: &EventRow) -> Option<String> {
        Some("aggregate".to_string()) // Default to single partition
    }
    /// Determine which view partitions should be updated for this event
    ///
    /// This allows for more flexible view update strategies:
    /// - Update a specific partition (default behavior using get_partition_key)
    /// - Update all partitions matching a query condition
    /// - Update all partitions of this view type
    ///
    /// Default implementation uses get_partition_key to update a single partition
    fn determine_affected_partitions(
        event: &Self::Event,
        event_row: &EventRow,
    ) -> PartitionUpdateStrategy {
        if let Some(key) = Self::get_partition_key(event, event_row) {
            PartitionUpdateStrategy::SpecificPartition { key }
        } else {
            PartitionUpdateStrategy::None
        }
    }

    /// Initialize a new view when first relevant event is received
    fn initialize(event: &Self::Event, event_row: &EventRow) -> Option<Self>;

    /// Update view with an event
    /// Each event will be applied exactly once per partition
    fn apply_event(&mut self, event: &Self::Event, event_row: &EventRow);

    /// Query the view state with the given criteria (optional)
    fn query(state: &Self, criteria: &str) -> Vec<String> {
        vec![] // Default implementation returns empty list
    }
    
    /// Legacy method for backward compatibility
    /// Use determine_affected_partitions instead
    #[deprecated(since = "0.2.0", note = "Use determine_affected_partitions instead")]
    fn update_all(_event: &Self::Event) -> bool {
        false // Default implementation doesn't update all partitions
    }
}
/// Specifies which view partitions should be updated for an event
pub enum PartitionUpdateStrategy {
    /// Update a specific partition by key
    SpecificPartition { key: String },
    /// Update all partitions matching a query condition
    Query { condition: String },
    /// Update all partitions of this view type
    All,
    /// Skip updating any partitions
    None,
}
