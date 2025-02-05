use std::fmt::Debug;

use async_trait::async_trait;
use eventstore::{
    Client, PersistentSubscriptionToAllOptions, RecordedEvent, ResolvedEvent, SubscriptionFilter,
};
use serde::de::DeserializeOwned;
use tracing::{event, instrument};

use crate::event::DomainEvent;

pub struct EventHandlerError {
    pub log_message: String,
    pub nack_action: eventstore::NakAction,
}

#[async_trait]
pub trait EventHandler: Send + Sync + 'static {
    type Service: Send + Sync + Clone = ();
    fn name() -> &'static str;
    async fn handle_event(
        event: &Box<dyn DomainEvent>,
        raw_event: &RecordedEvent,
        service: Self::Service,
    ) -> Result<(), EventHandlerError>;
}
