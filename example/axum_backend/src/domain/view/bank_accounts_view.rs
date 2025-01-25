use std::collections::HashMap;

use ask_cqrs::{event::DomainEvent, view::View};
use darkbird::document::{Document, FullText, Indexer, MaterializedView, Range, RangeField, Tags};
use eventstore::RecordedEvent;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::domain::bank_stream::event::BankAccountEvent;


#[derive(Debug, Clone, Serialize, Deserialize,ToSchema)]
pub struct BankAccountState {
    pub user_id: String,
    pub account_id: String,
    pub balance: i32,
}

impl FullText for BankAccountState {
    fn get_content(&self) -> Option<String> {
        None
    }
}
impl MaterializedView for BankAccountState {
    fn filter(&self) -> Option<String> {
        None
    }
}
impl Range for BankAccountState {
    fn get_fields(&self) -> Vec<RangeField> {
        vec![]
    }
}

impl Indexer for BankAccountState {
    fn extract(&self) -> Vec<String> {
        vec![]
    }
}

impl Tags for BankAccountState {
    fn get_tags(&self) -> Vec<String> {
        vec![self.user_id.clone()]
    }
}

impl Document for BankAccountState {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BankAccountView {}

#[async_trait::async_trait]
impl View for BankAccountView {
    type State = BankAccountState;

    fn name() -> &'static str {
        "BankAccountView"
    }

    fn view_id_from_event(event: &Box<dyn DomainEvent>) -> Option<String> {
        // Downcast the event to the specific type and match the variant
        if let Some(bank_event) = event.as_any().downcast_ref::<BankAccountEvent>() {
            match bank_event {
                BankAccountEvent::AccountOpened { account_id, .. }
                | BankAccountEvent::OwnerChanged { account_id, .. }
                | BankAccountEvent::FundsDeposited { account_id, .. }
                | BankAccountEvent::FundsWithdrawn { account_id, .. }
                | BankAccountEvent::WithdrawalDisputed { account_id, .. } => {
                    Some(account_id.clone())
                }
                _ => None,
            }
        } else {
            None
        }
    }

    fn apply_event(
        state: &mut Self::State,
        event: &Box<dyn DomainEvent>,
        stream_id: &str,
        raw_event: &RecordedEvent,
    ) {
        // Downcast the event to the specific type and match the variant
        if let Some(bank_event) = event.as_any().downcast_ref::<BankAccountEvent>() {
            match bank_event {
                BankAccountEvent::FundsDeposited { amount, .. } => state.balance += amount,
                BankAccountEvent::FundsWithdrawn { amount, .. } => state.balance -= amount,
                BankAccountEvent::OwnerChanged { user_id, .. } => state.user_id = user_id.clone(),
                _ => (),
            }
        } else {
            panic!("Unexpected event type for apply_event");
        }
    }

    fn create_from_first_event(
        event: &Box<dyn DomainEvent>,
        stream_id: &str,
        raw_event: &RecordedEvent,
    ) -> Self::State {
        if let Some(bank_event) = event.as_any().downcast_ref::<BankAccountEvent>() {
            match bank_event {
                BankAccountEvent::AccountOpened {
                    user_id,
                    balance,
                    account_id,
                } => BankAccountState {
                    user_id: user_id.clone(),
                    account_id: account_id.clone(),
                    balance: *balance,
                },
                _ => panic!("Cannot create from non-creation event"),
            }
        } else {
            panic!("Unexpected event type for create_from_first_event");
        }
    }
}
