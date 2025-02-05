use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::instrument;
use uuid::Uuid;

mod common;
mod test_utils;

use common::bank_account::{BankAccountAggregate, BankAccountCommand, BankAccountError};
use common::fraud_detection_handler::FraudDetectionHandler;
use test_utils::{initialize_logger, create_test_store};

#[tokio::test]
#[instrument]
async fn test_fraud_detection_handler() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = create_test_store().await?;
    
    // Start the fraud detection handler
    let handler = FraudDetectionHandler::new(store.clone());
    store.start_event_handler(handler).await?;

    // Generate a unique user ID for this test
    let user_id = Uuid::new_v4().to_string();

    // Open account
    let account_id = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id,
            account_id: None,
        },
        (),
    )
    .await?;

    // Deposit 5000 to ensure we have enough funds
    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 5000,
            account_id: account_id.clone(),
        },
        (),
    )
    .await?;

    // First withdrawal of 2000 should succeed
    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::WithdrawFunds { 
            amount: 2000,
            account_id: account_id.clone(),
        },
        (),
    )
    .await?;

    // Wait for the fraud detection handler to process the event
    sleep(Duration::from_millis(2000)).await;

    // Second withdrawal should fail because account is suspended
    let result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::WithdrawFunds { 
            amount: 100,
            account_id: account_id.clone(),
        },
        (),
    )
    .await;

    assert!(matches!(
        result.unwrap_err().downcast::<BankAccountError>().unwrap(),
        BankAccountError::AccountSuspended
    ));

    // Cleanup
    store.shutdown().await;
    sleep(Duration::from_millis(100)).await;

    Ok(())
}

#[instrument]
async fn test_fraud_detection_handler_small_withdrawals() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = create_test_store().await?;
    
    // Start the fraud detection handler
    let handler = FraudDetectionHandler::new(store.clone());
    store.start_event_handler(handler).await?;

    // Generate a unique user ID for this test
    let user_id = Uuid::new_v4().to_string();

    // Open account
    let account_id = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id,
            account_id: None,
        },
        (),
    )
    .await?;

    // Deposit 3000 to ensure we have enough funds
    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 3000,
            account_id: account_id.clone(),
        },
        (),
    )
    .await?;

    // Multiple small withdrawals should succeed
    for _ in 0..3 {
        store.execute_command::<BankAccountAggregate>(
            BankAccountCommand::WithdrawFunds { 
                amount: 500,
                account_id: account_id.clone(),
            },
            (),
        )
        .await?;

        // Wait a bit between withdrawals
        sleep(Duration::from_millis(50)).await;
    }

    // Cleanup
    store.shutdown().await;
    sleep(Duration::from_millis(100)).await;

    Ok(())
} 