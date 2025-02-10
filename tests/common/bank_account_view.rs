use ask_cqrs::view::View;
use ask_cqrs::aggregate::Aggregate;
use serde::{Deserialize, Serialize};
use super::bank_account::{BankAccountAggregate, BankAccountEvent};
use ask_cqrs::event_handler::EventRow;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BankAccountView {
    pub account_id: String,
    pub user_id: String,
    pub balance: u64,
}

impl Default for BankAccountView {
    fn default() -> Self {
        Self {
            account_id: String::new(),
            user_id: String::new(),
            balance: 0,
        }
    }
}

impl View for BankAccountView {
    type Event = BankAccountEvent;

    fn name() -> String {
        "bank_account_view".to_string()
    }


    fn get_partition_key(event: &Self::Event, _event_row: &EventRow) -> Option<String> {
        Some(event.account_id()) // Each account gets its own row
    }

    fn initialize(event: &Self::Event, _event_row: &EventRow) -> Option<Self> {
        match event {
            BankAccountEvent::AccountOpened { user_id, account_id, .. } => Some(BankAccountView {
                account_id: account_id.clone(),
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