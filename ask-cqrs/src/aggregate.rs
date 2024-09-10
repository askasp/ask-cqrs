use core::fmt;
use std::error::Error;
use std::sync::Arc;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{ser::StdError, Serialize};

use std::fmt::Debug;

use crate::command::DomainCommand;
use crate::event::DomainEvent;

#[async_trait]
pub trait Aggregate {
    type Event: DeserializeOwned + Serialize + DomainEvent; // Ensure Event implements Deserialize
    type Command: DomainCommand;
    type DomainError: StdError + Send + Sync + 'static; // Ensure Error implements std::error::Error and necessary conversions
    type State: Debug;

    type AuthUser =();
    type Service = ();

    fn apply_event(state: &mut Option<Self::State>, event: &Self::Event);
    fn execute(
        state: &Option<Self::State>,
        command: &Self::Command,
        stream_id: &str,
        auth_user: Self::AuthUser,
        service: Self::Service,
        
        
    ) -> Result<Vec<Self::Event>, Self::DomainError>;

    fn name() -> &'static str;
}
