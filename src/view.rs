use std::sync::Arc;
use dashmap::DashMap;
use serde::{de::DeserializeOwned, Serialize};
use tokio_postgres::Client;
use anyhow::Result;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio_postgres::Row;
use uuid::Uuid;
use deadpool_postgres::Pool;
use std::sync::RwLock;

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

/// Trait for views that maintain state for individual entities
pub trait StateView: View {
    /// Extract the entity ID from an event, if this event is relevant
    fn entity_id_from_event(event: &Self::Event) -> Option<String>;
    
    /// Initialize a new view when first relevant event is received
    fn initialize(event: &Self::Event) -> Option<Self>;
    
    /// Update view with an event
    fn apply_event(&mut self, event: &Self::Event);
}

/// Trait for views that maintain an index across multiple streams
pub trait IndexView: View {
    /// The index state type
    type Index: Clone + Send + Sync + 'static + DeserializeOwned + Serialize + Default;

    /// Query the index with the given criteria
    fn query(&self, index: &Self::Index, criteria: &str) -> Vec<String>;
    
    /// Initialize a new index
    fn initialize_index() -> Self::Index {
        Self::Index::default()
    }
    
    /// Update index with an event
    fn update_index(&self, index: &mut Self::Index, event: &Self::Event);
}
