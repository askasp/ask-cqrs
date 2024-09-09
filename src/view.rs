use std::sync::Arc;

use axum::async_trait;
use darkbird::{document::Document, Storage};
use serde::{de::DeserializeOwned, Serialize};
use tokio::stream;

use crate::event::DomainEvent;

#[async_trait]
pub trait View {
    type State: Document + Serialize + DeserializeOwned + Clone + Send + Sync + 'static;
    fn apply_event(
        state: &mut Self::State,
        event: &Box<dyn DomainEvent>,
        stream_id: &str,
        raw_event: &eventstore::RecordedEvent,
    );

    // Only handle events that returns some here
    fn view_id_from_event(event: &Box<dyn DomainEvent>) -> Option<String>;
    fn create_from_first_event(
        event: &Box<dyn DomainEvent>,
        stream_id: &str,
        raw_event: &eventstore::RecordedEvent,
    ) -> Self::State;

    fn name() -> &'static str;
}
