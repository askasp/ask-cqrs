use super::bank_account::BankAccountEvent;
use ask_cqrs::view::View;
use ask_cqrs::{event_handler::EventRow, view::PartitionUpdateStrategy};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BankNotificationView {
    pub account_id: String,
    pub user_id: String,
    pub notifications: Vec<String>,
    pub global_notifications: Vec<String>,
}

impl View for BankNotificationView {
    type Event = BankAccountEvent;

    fn name() -> &'static str {
        "bank_notification_view"
    }

    fn determine_affected_partitions(
        event: &Self::Event,
        _event_row: &EventRow,
    ) -> ask_cqrs::view::PartitionUpdateStrategy {
        // Account suspension is a global event that should update all views
        match event {
            BankAccountEvent::AccountSuspended { .. } => PartitionUpdateStrategy::All,
            _ => {
                // For other events, update the specific account partition
                match event {
                    BankAccountEvent::AccountOpened { account_id, .. } => {
                        PartitionUpdateStrategy::SpecificPartition {
                            key: account_id.clone(),
                        }
                    }
                    BankAccountEvent::FundsDeposited { account_id, .. } => {
                        PartitionUpdateStrategy::SpecificPartition {
                            key: account_id.clone(),
                        }
                    }
                    BankAccountEvent::FundsWithdrawn { account_id, .. } => {
                        PartitionUpdateStrategy::SpecificPartition {
                            key: account_id.clone(),
                        }
                    }
                    _ => PartitionUpdateStrategy::None, // Should never reach here
                }
            }
        }
    }

    fn initialize(event: &Self::Event, _event_row: &EventRow) -> Option<Self> {
        match event {
            BankAccountEvent::AccountOpened {
                user_id,
                account_id,
                ..
            } => Some(Self {
                account_id: account_id.clone(),
                user_id: user_id.clone(),
                notifications: vec![format!("Account opened on {}", chrono::Utc::now())],
                global_notifications: vec![],
            }),
            _ => None,
        }
    }

    fn apply_event(&mut self, event: &Self::Event, _event_row: &EventRow) {
        match event {
            BankAccountEvent::FundsDeposited { amount, .. } => {
                self.notifications.push(format!(
                    "Deposit of ${} received on {}",
                    amount,
                    chrono::Utc::now()
                ));
            }
            BankAccountEvent::FundsWithdrawn { amount, .. } => {
                self.notifications.push(format!(
                    "Withdrawal of ${} processed on {}",
                    amount,
                    chrono::Utc::now()
                ));
            }
            BankAccountEvent::AccountSuspended { .. } => {
                // This is a global notification that will be applied to all accounts
                self.global_notifications.push(format!(
                    "IMPORTANT: Account suspended on {}",
                    chrono::Utc::now()
                ));
            }
            _ => {}
        }
    }
}
