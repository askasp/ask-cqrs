use ask_cqrs::view::View;
use eventstore::RecordedEvent;
use serde::{Deserialize, Serialize};

use super::bank_account::BankAccountEvent;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BankAccountViewState {
    pub user_id: String,
    pub balance: u64,
}

pub struct BankAccountView;

impl View for BankAccountView {
    type State = BankAccountViewState;
    type Event = BankAccountEvent;

    fn name() -> &'static str {
        "bank_account_view"
    }

    fn entity_id_from_event(event: &Self::Event) -> Option<String> {
        Some(event.account_id())
    }

    fn initialize_state(event: &Self::Event, _stream_id: &str, _recorded_event: &RecordedEvent) -> Option<Self::State> {
        match event {
            BankAccountEvent::AccountOpened { user_id, .. } => Some(BankAccountViewState {
                user_id: user_id.clone(),
                balance: 0,
            }),
            _ => None,
        }
    }

    fn update_state(state: &mut Self::State, event: &Self::Event, _stream_id: &str, _recorded_event: &RecordedEvent) {
        match event {
            BankAccountEvent::FundsDeposited { amount, .. } => {
                state.balance += amount;
            }
            BankAccountEvent::FundsWithdrawn { amount, .. } => {
                state.balance -= amount;
            }
            _ => {}
        }
    }
} 