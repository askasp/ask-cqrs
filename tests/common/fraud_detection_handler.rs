use super::bank_account::{BankAccountCommand, BankAccountEvent, BankAccountAggregate};
use ask_cqrs::{event_handler::{EventHandler, EventHandlerError}, postgres_store::PostgresStore};
use async_trait::async_trait;
use anyhow::Result;

pub struct FraudDetectionHandler {
    store: PostgresStore,
}

impl FraudDetectionHandler {
    pub fn new(store: PostgresStore) -> Self {
        Self { store }
    }

    const SUSPICIOUS_AMOUNT: u64 = 2000;
}

#[async_trait::async_trait]
impl EventHandler for FraudDetectionHandler {
    type Events = BankAccountEvent;
    type Service = ();

    fn name() -> &'static str {
        "fraud_detection_handler"
    }

    async fn handle_event(&self, event: Self::Events) -> Result<(), EventHandlerError> {
        if let BankAccountEvent::FundsWithdrawn { amount, account_id } = event {
            if amount >= Self::SUSPICIOUS_AMOUNT {
                // Suspend the account
                self.store.execute_command::<BankAccountAggregate>(
                    BankAccountCommand::SuspendAccount { 
                        account_id: account_id.clone() 
                    },
                    (),
                ).await.map_err(|e| EventHandlerError {
                    log_message: format!("Failed to suspend account: {}", e),
                })?;
            }
        }

        Ok(())
    }
} 