use serde::{de::DeserializeOwned, Serialize};
use crate::event_handler::EventRow;

/// Trait for implementing a read model/view
pub trait View: Clone + Send + Sync + 'static + DeserializeOwned + Serialize {
    /// The event type this view processes
    type Event: Clone + Send + Sync + 'static + DeserializeOwned + Serialize + std::fmt::Debug;

    /// Name of the view, used for logging and debugging
    fn name() -> String;

    /// Get the partition key for this event
    /// This determines how the view state is split across rows
    /// Return None to skip this event
    fn get_partition_key(event: &Self::Event, event_row: &EventRow) -> Option<String> {
        Some("aggregate".to_string()) // Default to single partition
    }

    /// Initialize a new view when first relevant event is received
    fn initialize(event: &Self::Event, event_row: &EventRow) -> Option<Self>;
    
    /// Update view with an event
    fn apply_event(&mut self, event: &Self::Event, event_row: &EventRow);

    /// Query the view state with the given criteria (optional)
    fn query(state: &Self, criteria: &str) -> Vec<String> {
        vec![] // Default implementation returns empty list
    }
}
