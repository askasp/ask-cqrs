use std::sync::Arc;

use ask_cqrs::{
    event::DomainEvent,
    event_handler::{EventHandler, EventHandlerError},
    execute_command,
};
use axum::async_trait;
use darkbird::Storage;
use eventstore::{Client, RecordedEvent, ResolvedEvent};
use tracing::{event, instrument};

use crate::common::bank_aggregate::{BankAccountAggregate, BankAccountCommand};

use super::{
    bank_account_view::{BankAccountState, BankAccountView},
    bank_events::BankAccountEvent,
};

#[derive(Clone)]
pub struct FraudDetectionHandler {
    client: Arc<Client>,
}

impl FraudDetectionHandler {
    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
    }
}

#[derive(Clone)]
pub struct FraudDetectionService {
    pub bank_view: Arc<Storage<String, BankAccountState>>,
    pub reqwest_client: Arc<reqwest::Client>,
    pub es_client: Arc<Client>,
}

#[async_trait]
impl EventHandler for FraudDetectionHandler {
    type Service = FraudDetectionService;
    fn name() -> &'static str {
        "FraudDetectionHandler"
    }

    async fn handle_event(
        event: &Box<dyn DomainEvent>,
        raw_event: &RecordedEvent,
        service: Self::Service,
    ) -> Result<(), EventHandlerError> {
        // let parsed_event = orig_event.as_json::<Self::Event>();
        // #region handle-event

        if let Some(bank_event) = event.as_any().downcast_ref::<BankAccountEvent>() {
            match bank_event {
                BankAccountEvent::FundsWithdrawn { amount, account_id } => {
                    if *amount > 1000 {
                        if service
                            .reqwest_client
                            .get("https://nrk.no")
                            .send()
                            .await
                            .unwrap()
                            .status()
                            .is_success()
                        {
                            event!(
                                tracing::Level::ERROR,
                                "Fraud detected and we have internet, something is wrong!"
                            );
                            execute_command::<BankAccountAggregate>(
                                service.es_client.clone(),
                                BankAccountCommand::DisputeWithdrawal {
                                    withdrawal_id: raw_event.id.into(),
                                    account_id: account_id.clone(),
                                },
                                &account_id,
                                (),
                                (),
                            )
                            .await
                            .unwrap();

                            println!("Fraud detected, something is wrong!");
                        } else {
                            println!("No fraud detected, everything is fine!");
                            return Ok(());
                        }
                        return Ok(());
                    } else {
                        println!("No fraud detected, everything is fine!");
                        return Ok(());
                    }
                }
                _ => return Ok(()),
            }
        } else {
            return Ok(());
        }
    }
    // #endregion handle-event
}
