use serde::{de::DeserializeOwned, Serialize};
use crate::event_handler::EventRow;

/// Trait for implementing a read model/view
pub trait View: Clone + Send + Sync + 'static + DeserializeOwned + Serialize {
    /// The event type this view processes
    type Event: Clone + Send + Sync + 'static + DeserializeOwned + Serialize + std::fmt::Debug;

    /// Name of the view, used for logging and debugging
    fn name() -> String;
    
    /// Optional snapshot configuration - how many events before taking a snapshot
    fn snapshot_frequency() -> Option<u32> {
        None // By default, no snapshots
    }

    /// Optional event filtering - which event types to process
    fn event_types() -> Vec<String> {
        vec![] // Empty means all events
    }
}

/// Trait for views that maintain state for individual streams
pub trait StreamView: View {
    /// Extract the entity ID from an event, if this event is relevant
    fn entity_id_from_event(event: &Self::Event) -> Option<String>;

    fn stream_name() -> &'static str;
    
    /// Initialize a new view when first relevant event is received
    fn initialize(event: &Self::Event, event_row: &EventRow) -> Option<Self>;
    
    /// Update view with an event
    fn apply_event(&mut self, event: &Self::Event, event_row: &EventRow);
}

/// Trait for views that maintain state across all streams
pub trait GlobalView: View {
    /// The global state type
    type State: Clone + Send + Sync + 'static + DeserializeOwned + Serialize + Default;

    /// Query the state with the given criteria
    fn query(&self, state: &Self::State, criteria: &str) -> Vec<String>;
    
    /// Initialize a new state
    fn initialize_state() -> Self::State {
        Self::State::default()
    }
    
    /// Update state with an event
    fn update_state(&self, state: &mut Self::State, event: &Self::Event, event_row: &EventRow);
}
