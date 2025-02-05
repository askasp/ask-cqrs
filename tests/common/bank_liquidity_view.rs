use ask_cqrs::view::{View, GlobalView};
use serde::{Deserialize, Serialize};
use super::bank_account::BankAccountEvent;
use ask_cqrs::event_handler::EventRow;

// Bank liquidity tracking across all accounts
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BankLiquidityState {
    pub total_balance: u64,
    pub total_accounts: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BankLiquidityView;

impl View for BankLiquidityView {
    type Event = BankAccountEvent;

    fn name() -> String {
        "bank_liquidity_view".to_string()
    }

    fn snapshot_frequency() -> Option<u32> {
        Some(100) // Take snapshot every 100 events
    }
}

impl GlobalView for BankLiquidityView {
    type State = BankLiquidityState;

    fn query(&self, state: &Self::State, _criteria: &str) -> Vec<String> {
        // This view doesn't really need query functionality
        // but we could return accounts with balance > criteria if needed
        Vec::new()
    }

    fn update_state(&self, state: &mut Self::State, event: &Self::Event, _event_row: &EventRow) {
        match event {
            BankAccountEvent::AccountOpened { .. } => {
                state.total_accounts += 1;
            }
            BankAccountEvent::FundsDeposited { amount, .. } => {
                state.total_balance += amount;
            }
            BankAccountEvent::FundsWithdrawn { amount, .. } => {
                state.total_balance -= amount;
            }
            _ => {}
        }
    }
} 