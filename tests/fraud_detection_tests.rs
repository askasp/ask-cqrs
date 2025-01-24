use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::instrument;
use uuid::Uuid;

mod common;
mod test_utils;

use ask_cqrs::{command::DomainCommand, execute_command, start_event_handler};
use common::bank_account::{BankAccountAggregate, BankAccountCommand, BankAccountError};
use common::fraud_detection_handler::FraudDetectionHandler;
use test_utils::{create_es_client, initialize_logger};

#[tokio::test]
#[instrument]
async fn test_fraud_detection_handler() -> Result<(), anyhow::Error> {
    initialize_logger();
    let client = create_es_client();
    
    // Start the fraud detection handler
    let handler = FraudDetectionHandler::new(client.clone());
    start_event_handler(handler, client.clone(), ()).await?;

    // Generate a unique user ID for this test
    let user_id = Uuid::new_v4().to_string();

    // Open account
    let open_command = BankAccountCommand::open_account(user_id);
    let account_id = execute_command::<BankAccountAggregate>(
        client.clone(),
        open_command,
        (),
    )
    .await?;

    // Deposit 5000 to ensure we have enough funds
    let deposit_command = BankAccountCommand::deposit_funds(5000, account_id.clone());
    execute_command::<BankAccountAggregate>(
        client.clone(),
        deposit_command,
        (),
    )
    .await?;

    // First withdrawal of 2000 should succeed
    let withdraw_command = BankAccountCommand::withdraw_funds(2000, account_id.clone());
    execute_command::<BankAccountAggregate>(
        client.clone(),
        withdraw_command,
        (),
    )
    .await?;

    // Wait for the fraud detection handler to process the event
    sleep(Duration::from_millis(100)).await;

    // Second withdrawal should fail because account is suspended
    let withdraw_command = BankAccountCommand::withdraw_funds(100, account_id.clone());
    let result = execute_command::<BankAccountAggregate>(
        client.clone(),
        withdraw_command,
        (),
    )
    .await;

    assert!(matches!(
        result.unwrap_err().downcast::<BankAccountError>().unwrap(),
        BankAccountError::AccountSuspended
    ));

    Ok(())
}

#[tokio::test]
#[instrument]
async fn test_fraud_detection_handler_small_withdrawals() -> Result<(), anyhow::Error> {
    initialize_logger();
    let client = create_es_client();
    
    // Start the fraud detection handler
    let handler = FraudDetectionHandler::new(client.clone());
    start_event_handler(handler, client.clone(), ()).await?;

    // Generate a unique user ID for this test
    let user_id = Uuid::new_v4().to_string();

    // Open account
    let open_command = BankAccountCommand::open_account(user_id);
    let account_id = execute_command::<BankAccountAggregate>(
        client.clone(),
        open_command,
        (),
    )
    .await?;

    // Deposit 3000 to ensure we have enough funds
    let deposit_command = BankAccountCommand::deposit_funds(3000, account_id.clone());
    execute_command::<BankAccountAggregate>(
        client.clone(),
        deposit_command,
        (),
    )
    .await?;

    // Multiple small withdrawals should succeed
    for _ in 0..3 {
        let withdraw_command = BankAccountCommand::withdraw_funds(500, account_id.clone());
        execute_command::<BankAccountAggregate>(
            client.clone(),
            withdraw_command,
            (),
        )
        .await?;

        // Wait a bit between withdrawals
        sleep(Duration::from_millis(50)).await;
    }

    Ok(())
} 