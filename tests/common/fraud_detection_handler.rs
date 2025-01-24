use ask_cqrs::{event_handler::EventHandler, execute_command, view::ViewStore};
use async_trait::async_trait;
use eventstore::{Client, RecordedEvent};
use std::sync::Arc;

use super::bank_account::{BankAccountCommand, BankAccountEvent, BankAccountAggregate};
use super::bank_account_view::BankAccountView;

pub struct FraudDetectionHandler {
    client: Arc<Client>,
}

impl FraudDetectionHandler {
    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
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
        raw_event: &RecordedEvent,
        _service: Self::Service,
    ) -> Result<(), ask_cqrs::event_handler::EventHandlerError> {
        if let BankAccountEvent::FundsWithdrawn { amount, account_id } = event {
            if amount >= Self::SUSPICIOUS_AMOUNT {
                // Suspend the account
                let suspend_command = BankAccountCommand::suspend_account(account_id);
                execute_command::<BankAccountAggregate>(
                    self.client.clone(),
                    suspend_command,
                    (),
                )
                .await
                .map_err(|e| ask_cqrs::event_handler::EventHandlerError {
                    log_message: format!("Failed to suspend account: {}", e),
                    nack_action: eventstore::NakAction::Park,
                })?;
            }
        }
        Ok(())
    }
} 