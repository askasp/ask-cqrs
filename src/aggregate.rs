use core::fmt;
use std::error::Error;
use std::sync::Arc;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

use std::fmt::Debug;

use crate::command::DomainCommand;

#[async_trait]
pub trait Aggregate {
    type Event: DeserializeOwned + Serialize + Send + Sync;
    type Command: DomainCommand + Send + Sync;
    type DomainError:  Error + Send + Sync + 'static;
    type State: Debug + Send + Sync;
    type Service: Clone + Send + Sync;

    fn name() -> &'static str;
    
    fn apply_event(state: &mut Option<Self::State>, event: &Self::Event);
    
    fn execute(
        state: &Option<Self::State>,
        command: &Self::Command,
        stream_id: &str,
        service: Self::Service,
    ) -> Result<Vec<Self::Event>, Self::DomainError>;
}
