use async_trait::async_trait;
use eventstore::RecordedEvent;
use serde::de::DeserializeOwned;

pub struct EventHandlerError {
    pub log_message: String,
    pub nack_action: eventstore::NakAction,
}

#[async_trait]
pub trait EventHandler: Send + Sync + 'static {
    /// The event type this handler processes
    type Events: Send + Sync + 'static + DeserializeOwned;
    /// Service dependencies
    type Service: Send + Sync + Clone;

    fn name() -> &'static str;
    
    async fn handle_event(
        &self,
        event: Self::Events,
        raw_event: &RecordedEvent,
        service: Self::Service,
    ) -> Result<(), EventHandlerError>;
}
