use ask_cqrs::view::{StreamView, View, GlobalView};
use ask_cqrs::aggregate::Aggregate;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use super::bank_account::{BankAccountAggregate, BankAccountEvent};
use ask_cqrs::event_handler::EventRow;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BankAccountView {
    pub user_id: String,
    pub balance: u64,
}

impl View for BankAccountView {
    type Event = BankAccountEvent;

    fn name() -> String {
        "bank_account_view".to_string()
    }
}

impl StreamView for BankAccountView {
    fn entity_id_from_event(event: &Self::Event) -> Option<String> {
        Some(event.account_id())
    }

    fn stream_name() -> &'static str {
        BankAccountAggregate::name()
    }

    fn initialize(event: &Self::Event, _event_row: &EventRow) -> Option<Self> {
        match event {
            BankAccountEvent::AccountOpened { user_id, .. } => Some(BankAccountView {
                user_id: user_id.clone(),
                balance: 0,
            }),
            _ => None,
        }
    }

    fn apply_event(&mut self, event: &Self::Event, _event_row: &EventRow) {
        match event {
            BankAccountEvent::FundsDeposited { amount, .. } => {
                self.balance += amount;
            }
            BankAccountEvent::FundsWithdrawn { amount, .. } => {
                self.balance -= amount;
            }
            _ => {}
        }
    }
}

// Index to find accounts by user ID
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UserAccountsIndex {
    accounts_by_user: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UserAccountsIndexView;

impl View for UserAccountsIndexView {
    type Event = BankAccountEvent;

    fn name() -> String {
        "user_accounts_index".to_string()
    }
}

impl GlobalView for UserAccountsIndexView {
    type State = UserAccountsIndex;

    fn query(&self, state: &Self::State, criteria: &str) -> Vec<String> {
        state.accounts_by_user.get(criteria)
            .cloned()
            .unwrap_or_default()
    }

    fn update_state(&self, state: &mut Self::State, event: &Self::Event, _event_row: &EventRow) {
        match event {
            BankAccountEvent::AccountOpened { user_id, account_id } => {
                state.accounts_by_user
                    .entry(user_id.clone())
                    .or_default()
                    .push(account_id.clone());
            }
            _ => {}
        }
    }
} 