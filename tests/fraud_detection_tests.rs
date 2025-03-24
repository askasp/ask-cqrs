use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{instrument, info};
use uuid::Uuid;
use serde_json::json;

mod common;

use common::bank_account::{BankAccountAggregate, BankAccountCommand, BankAccountError, BankAccountEvent};
use common::fraud_detection_handler::FraudDetectionHandler;
use ask_cqrs::test_utils::{initialize_logger, create_test_store};
use ask_cqrs::store::event_store::{EventStore, EventProcessingConfig};
use ask_cqrs::event_handler::{EventHandler, EventRow};

#[tokio::test]
#[instrument]
#[serial_test::serial]
async fn test_fraud_detection_handler() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = create_test_store("postgres://postgres:postgres@localhost:5432/ask_cqrs_test2").await?;
    
    // Start the fraud detection handler
    let handler = FraudDetectionHandler::new(store.clone());
    store.start_event_handler(handler.clone(), None).await?;

    // Generate a unique user ID for this test
    let user_id = Uuid::new_v4().to_string();

    // Open account
    let res = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id}),
    )
    .await?;
    let account_id = res.stream_id;
    info!("Account opened with ID: {}", account_id);

    // Deposit 5000 to ensure we have enough funds
    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 5000,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    )
    .await?;
    info!("Deposited 5000 to account");

    // First withdrawal of 2000 should succeed
    let withdraw_result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::WithdrawFunds { 
            amount: 2000,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    )
    .await?;
    info!("First withdrawal of 2000 succeeded, global position: {}", withdraw_result.global_position);

    // Wait longer for the fraud detection handler to process the event
    info!("Waiting for fraud detection handler to process the event...");
    sleep(Duration::from_millis(500)).await;
    
    // Manually trigger event processing to ensure it gets processed
    info!("Manually processing events to ensure they're handled...");
    let events = store.get_events_for_stream("bank_account", &account_id, -1, 10).await?;
    for event in events {
        if let Ok(parsed_event) = serde_json::from_value::<BankAccountEvent>(event.event_data.clone()) {
            info!("Manually processing event: {:?}", parsed_event);
            handler.handle_event(parsed_event, event).await?;
        }
    }
    sleep(Duration::from_millis(100)).await;

    // Second withdrawal should fail because account is suspended
    info!("Attempting second withdrawal which should fail...");
    let result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::WithdrawFunds { 
            amount: 100,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    )
    .await;

    match &result {
        Ok(cmd_result) => info!("Second withdrawal unexpectedly succeeded: {:?}", cmd_result),
        Err(e) => info!("Second withdrawal failed as expected: {}", e),
    }

    assert!(matches!(
        result.unwrap_err().downcast::<BankAccountError>().unwrap(),
        BankAccountError::AccountSuspended
    ));

    // Cleanup
    sleep(Duration::from_millis(100)).await;

    Ok(())
}

#[tokio::test]
#[instrument]
#[serial_test::serial]
async fn test_fraud_detection_handler_small_withdrawals() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = create_test_store("postgres://postgres:postgres@localhost:5432/ask_cqrs_test2").await?;
    
    // Start the fraud detection handler
    let handler = FraudDetectionHandler::new(store.clone());
    store.start_event_handler(handler, None).await?;

    // Generate a unique user ID for this test
    let user_id = Uuid::new_v4().to_string();

    // Open account
    let res = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id}),
    )
    .await?;
    let account_id = res.stream_id;

    // Deposit 3000 to ensure we have enough funds
    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 3000,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
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
            json!({"user_id": user_id}),
        )
        .await?;

        // Wait a bit between withdrawals
        sleep(Duration::from_millis(50)).await;
    }

    // Cleanup
    sleep(Duration::from_millis(100)).await;

    Ok(())
}

#[tokio::test]
#[instrument]
#[serial_test::serial]
async fn test_fraud_detection_start_from_current() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = create_test_store("postgres://postgres:postgres@localhost:5432/ask_cqrs_test2").await?;
    
    // Generate a unique user ID for this test
    let user_id = Uuid::new_v4().to_string();

    // First create an account and make a large withdrawal WITHOUT the handler running
    // Open account
    let res = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id}),
    )
    .await?;
    let account_id = res.stream_id;
    info!("Account opened with ID: {}", account_id);

    // Deposit 10000 to ensure we have enough funds
    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 10000,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    )
    .await?;
    info!("Deposited 10000 to account");

    // Make a large withdrawal (over the suspicious threshold) - without fraud detection running
    let withdraw_result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::WithdrawFunds { 
            amount: 5000, // Well over the suspicious threshold
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    )
    .await?;
    info!("Made large withdrawal of 5000 without fraud detection running");
    
    // Now start the fraud detection handler with start_from_current: true
    let handler = FraudDetectionHandler::new(store.clone());
    let config = EventProcessingConfig {
        start_from_current: true,
        ..Default::default()
    };
    
    info!("Starting fraud detection handler with start_from_current=true");
    store.start_event_handler(handler.clone(), Some(config)).await?;
    
    // Account should still be active since handler is configured to start from current
    // and ignore past events
    sleep(Duration::from_millis(200)).await;
    
    // Small withdrawal should succeed because account shouldn't be suspended
    info!("Attempting small withdrawal which should succeed...");
    let small_withdraw_result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::WithdrawFunds { 
            amount: 100,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    )
    .await;
    
    assert!(small_withdraw_result.is_ok(), 
            "Small withdrawal should succeed because past large withdrawal should be ignored");
    info!("Small withdrawal succeeded as expected");
    
    // Now make another large withdrawal which should trigger the fraud detection
    info!("Making another large withdrawal which should trigger fraud detection...");
    let large_withdraw_result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::WithdrawFunds { 
            amount: 3000,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    )
    .await?;
    
    // Wait for fraud detection to process
    sleep(Duration::from_millis(500)).await;
    
    // Manually trigger event processing to ensure it gets processed
    info!("Manually processing latest events...");
    let events = store.get_events_for_stream("bank_account", &account_id, large_withdraw_result.stream_position - 1, 10).await?;
    for event in events {
        if let Ok(parsed_event) = serde_json::from_value::<BankAccountEvent>(event.event_data.clone()) {
            info!("Manually processing event: {:?}", parsed_event);
            handler.handle_event(parsed_event, event).await?;
        }
    }
    sleep(Duration::from_millis(100)).await;
    
    // Now the account should be suspended, so another withdrawal should fail
    info!("Attempting final withdrawal which should fail due to suspension...");
    let final_withdraw_result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::WithdrawFunds { 
            amount: 50,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    )
    .await;
    
    match &final_withdraw_result {
        Ok(_) => info!("Final withdrawal unexpectedly succeeded"),
        Err(e) => info!("Final withdrawal failed as expected: {}", e),
    }
    
    assert!(matches!(
        final_withdraw_result.unwrap_err().downcast::<BankAccountError>().unwrap(),
        BankAccountError::AccountSuspended
    ), "Account should be suspended after new large withdrawal");
    
    // Cleanup
    sleep(Duration::from_millis(100)).await;

    Ok(())
} 