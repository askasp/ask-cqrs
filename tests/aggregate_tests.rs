use tracing::instrument;
use uuid::Uuid;
use serde_json::json;

mod common;

use common::bank_account::{BankAccountAggregate, BankAccountCommand, BankAccountError};
use ask_cqrs::test_utils::{create_test_database, create_test_database_with_schema, create_test_store, initialize_logger};
use ask_cqrs::store::event_store::{EventStore, CommandError};

#[tokio::test]
#[instrument]
#[serial_test::serial]
async fn test_bank_account_aggregate() -> Result<(), anyhow::Error> {
    initialize_logger();
    // let store = create_test_store("postgres://postgres:postgres@localhost:5432/ask_cqrs_test2").await?;gccAg
    let store = create_test_database("postgres://postgres:postgres@localhost:5432/", "ask_cqrs_test3").await?;
    
    // Test opening an account
    let user_id = Uuid::new_v4().to_string();
    let res = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id}),
    )
    .await?;

    // Test depositing funds
    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 100,
            account_id: res.stream_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    )
    .await?;

    // Test withdrawing funds successfully
    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::WithdrawFunds { 
            amount: 50,
            account_id: res.stream_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    )
    .await?;

    // Test insufficient funds
    let result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::WithdrawFunds { 
            amount: 1000,
            account_id: res.stream_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    )
    .await;
    
    assert!(matches!(
        result.unwrap_err(),
        CommandError::Domain(BankAccountError::InsufficientFunds)
    ));

    Ok(())
}

#[tokio::test]
#[instrument]
#[serial_test::serial]
async fn test_bank_account_duplicate_open() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = create_test_database("postgres://postgres:postgres@localhost:5432/", "ask_cqrs_test3").await?;
    
    // First open command
    let account_id = Uuid::new_v4().to_string();
    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: "user1".to_string(),
            account_id: Some(account_id.clone()),
        },
        (),
        json!({"user_id": "user1"}),
    )
    .await?;

    // Second open command with same account ID
    let result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: "user1".to_string(),
            account_id: Some(account_id),
        },
        (),
        json!({"user_id": "user1"}),
    )
    .await;

    assert!(matches!(
        result.unwrap_err(),
        CommandError::Domain(BankAccountError::AccountAlreadyExists)
    ));

    Ok(())
}

#[tokio::test]
#[instrument]
#[serial_test::serial]
async fn test_bank_account_nonexistent() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = create_test_database("postgres://postgres:postgres@localhost:5432/", "ask_cqrs_test2").await?;
    
    // Try to deposit to nonexistent account
    let account_id = Uuid::new_v4().to_string();
    let result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::WithdrawFunds { 
            amount: 100,
            account_id,
        },
        (),
        json!({"user_id": "unknown"}),
    )
    .await;
    
    assert!(matches!(
        result.unwrap_err(),
        CommandError::Domain(BankAccountError::AccountNotFound)
    ));

    Ok(())
} 