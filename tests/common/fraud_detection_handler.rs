use super::bank_account::{BankAccountCommand, BankAccountEvent, BankAccountAggregate};
use ask_cqrs::{event_handler::{EventHandler, EventHandlerError, EventRow}, store::postgres_event_store::PostgresEventStore, store::event_store::EventStore};
use async_trait::async_trait;
use anyhow::Result;
use serde_json::json;
use tracing::{info, error};

#[derive(Clone)]
pub struct FraudDetectionHandler {
    store: PostgresEventStore,
}

impl FraudDetectionHandler {
    pub fn new(store: PostgresEventStore) -> Self {
        Self { store }
    }

    const SUSPICIOUS_AMOUNT: u64 = 2000;
}

#[async_trait::async_trait]
impl EventHandler for FraudDetectionHandler {
    type Events = BankAccountEvent;

    fn name() -> &'static str {
        "fraud_detection_handler"
    }

    async fn handle_event(&self, event: Self::Events, event_row: EventRow) -> Result<(), EventHandlerError> {
        info!("Fraud detection handling event: {:?}, stream_position: {}", 
            &event, event_row.stream_position);
            
        if let BankAccountEvent::FundsWithdrawn { amount, account_id } = event {
            info!("Detected withdrawal of {} for account {}", amount, account_id);
            
            if amount >= Self::SUSPICIOUS_AMOUNT {
                info!("SUSPICIOUS WITHDRAWAL DETECTED! Amount {} >= threshold {}", 
                    amount, Self::SUSPICIOUS_AMOUNT);
                
                // Suspend the account
                info!("Attempting to suspend account {}", account_id);
                match self.store.execute_command::<BankAccountAggregate>(
                    BankAccountCommand::SuspendAccount { 
                        account_id: account_id.clone() 
                    },
                    (),
                    json!({
                        "reason": "suspicious_withdrawal",
                        "triggered_by_event": event_row.id,
                        "amount": amount
                    }) 
                ).await {
                    Ok(result) => {
                        info!("Successfully suspended account! Result: {:?}", result);
                    },
                    Err(e) => {
                        error!("Failed to suspend account: {}", e);
                        return Err(EventHandlerError {
                            log_message: format!("Failed to suspend account: {}", e),
                        });
                    }
                }
            } else {
                info!("Withdrawal amount {} is below suspicious threshold {}", 
                    amount, Self::SUSPICIOUS_AMOUNT);
            }
        } else {
            info!("Event is not a withdrawal, ignoring");
        }

        Ok(())
    }
} 