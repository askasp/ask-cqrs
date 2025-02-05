use async_trait::async_trait;
use serde::de::DeserializeOwned;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{log_message}")]
pub struct EventHandlerError {
    pub log_message: String,
}

#[async_trait]
pub trait EventHandler: Send + Sync + 'static {
    /// The event type this handler processes
    type Events: DeserializeOwned + Send + Sync + 'static;
    /// Service dependencies
    type Service: Send + Sync + 'static;

    fn name() -> &'static str;
    
    async fn handle_event(
        &self,
        event: Self::Events,
    ) -> Result<(), EventHandlerError>;
}
