use std::sync::Arc;
use dashmap::DashMap;
use serde::{de::DeserializeOwned, Serialize};
use eventstore::RecordedEvent;
use uuid::Uuid;

/// Trait for implementing a read model/view
pub trait View: Send + Sync + 'static {
    /// The state type for a single entity in the view
    type State: Clone + Send + Sync + 'static + DeserializeOwned + Serialize;
    /// The event type this view processes
    type Event: Clone + Send + Sync + 'static + DeserializeOwned + Serialize + std::fmt::Debug;

    /// Name of the view, used for logging and debugging
    fn name() -> &'static str;
    
    /// Extract the view entity ID from an event, if this event is relevant
    fn entity_id_from_event(event: &Self::Event) -> Option<String>;
    
    /// Initialize a new state when first relevant event is received
    fn initialize_state(event: &Self::Event, stream_id: &str, recorded_event: &RecordedEvent) -> Option<Self::State>;
    
    /// Update existing state with an event
    fn update_state(state: &mut Self::State, event: &Self::Event, stream_id: &str, recorded_event: &RecordedEvent);
}

/// A thread-safe view store that can be shared across the application
pub struct ViewStore<V: View> {
    states: Arc<DashMap<String, V::State>>,
    handled_events: Arc<DashMap<Uuid, ()>>,
}

impl<V: View> ViewStore<V> {
    pub fn new() -> Self {
        Self {
            states: Arc::new(DashMap::new()),
            handled_events: Arc::new(DashMap::new()),
        }
    }

    /// Get a single entity by ID
    pub fn get(&self, id: &str) -> Option<V::State> {
        self.states.get(id).map(|v| v.clone())
    }

    /// Get all entities that match a predicate
    pub fn find<F>(&self, predicate: F) -> Vec<V::State> 
    where 
        F: Fn(&V::State) -> bool 
    {
        self.states
            .iter()
            .filter(|r| predicate(r.value()))
            .map(|r| r.value().clone())
            .collect()
    }

    /// Handle an incoming event
    pub fn handle_event(
        &self,
        event: &V::Event,
        stream_id: &str,
        recorded_event: &RecordedEvent,
    ) {
        tracing::info!("Handling event: {:?}", event);
        
        // Check if we've already handled this event
        if self.handled_events.contains_key(&recorded_event.id) {
            tracing::debug!(
                "Ignoring already handled event: {}",
                recorded_event.id
            );
            return;
        }

        if let Some(entity_id) = V::entity_id_from_event(event) {
            tracing::info!("Entity ID: {:?}", entity_id);

            match self.states.get_mut(&entity_id) {
                Some(mut entry) => {
                    tracing::info!("Updating state");
                    V::update_state(&mut entry, event, stream_id, recorded_event);
                }
                None => {
                    if let Some(new_state) = V::initialize_state(event, stream_id, recorded_event) {
                        tracing::info!("Inserting new state");
                        self.states.insert(entity_id.clone(), new_state);
                    }
                }
            }
            
            // Mark this event as handled
            self.handled_events.insert(recorded_event.id, ());
        }
    }
}
