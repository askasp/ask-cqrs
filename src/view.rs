use serde::{de::DeserializeOwned, Serialize};
use crate::event_handler::EventRow;

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

    /// Initialize a new view when first relevant event is received
    fn initialize(event: &Self::Event, event_row: &EventRow) -> Option<Self>;
    
    /// Update view with an event
    /// Each event will be applied exactly once per partition
    fn apply_event(&mut self, event: &Self::Event, event_row: &EventRow);

    /// Query the view state with the given criteria (optional)
    fn query(state: &Self, criteria: &str) -> Vec<String> {
        vec![] // Default implementation returns empty list
    }
}
