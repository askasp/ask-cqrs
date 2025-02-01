use ask_cqrs::{event_handler::{EventHandler, EventHandlerError}, postgres_store::PostgresStore};
use async_trait::async_trait;
use tokio_postgres::Row;
use std::sync::Arc;

use super::bank_account::{BankAccountCommand, BankAccountEvent, BankAccountAggregate};

pub struct FraudDetectionHandler {
    store: PostgresStore,
}

impl FraudDetectionHandler {
    pub fn new(store: PostgresStore) -> Self {
        Self { store }
    }

    const SUSPICIOUS_AMOUNT: u64 = 2000;
}

#[async_trait]
impl EventHandler for FraudDetectionHandler {
    type Events = BankAccountEvent;
    type Service = ();

    fn name() -> &'static str {
        "fraud_detection_handler"
    }

    async fn handle_event(
        &self,
        event: Self::Events,
        _row: &Row,
    ) -> Result<(), EventHandlerError> {
        if let BankAccountEvent::FundsWithdrawn { amount, account_id } = event {
            if amount >= Self::SUSPICIOUS_AMOUNT {
                // Suspend the account
                let suspend_command = BankAccountCommand::suspend_account(account_id);
                self.store.execute_command::<BankAccountAggregate>(
                    suspend_command,
                    (),
                )
                .await
                .map_err(|e| EventHandlerError {
                    log_message: format!("Failed to suspend account: {}", e),
                })?;
            }
        }
        Ok(())
    }
} 