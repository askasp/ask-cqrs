use ask_cqrs::view::View;
use serde::{Deserialize, Serialize};
use crate::common::bank_account::BankAccountAggregate;
use ask_cqrs::aggregate::Aggregate;

use super::bank_account::BankAccountEvent;
use ask_cqrs::event_handler::EventRow;

// Bank liquidity tracking across all accounts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BankLiquidityView {
    pub total_balance: u64,
    pub total_accounts: u64,
}

impl Default for BankLiquidityView {
    fn default() -> Self {
        Self {
            total_balance: 0,
            total_accounts: 0,
        }
    }
}

impl View for BankLiquidityView {
    type Event = BankAccountEvent;

    fn name() -> String {
        "bank_liquidity_view".to_string()
    }

    fn stream_names() -> Vec<&'static str> {
        vec![BankAccountAggregate::name()]
    }

    fn get_partition_key(_event: &Self::Event, _event_row: &EventRow) -> Option<String> {
        Some("aggregate".to_string()) // Single global view tracking aggregate metrics
    }

    fn initialize(_event: &Self::Event, _event_row: &EventRow) -> Option<Self> {
        Some(Self::default())
    }

    fn apply_event(&mut self, event: &Self::Event, _event_row: &EventRow) {
        match event {
            BankAccountEvent::AccountOpened { .. } => {
                self.total_accounts += 1;
            }
            BankAccountEvent::FundsDeposited { amount, .. } => {
                self.total_balance += amount;
            }
            BankAccountEvent::FundsWithdrawn { amount, .. } => {
                self.total_balance -= amount;
            }
            _ => {}
        }
    }
} 