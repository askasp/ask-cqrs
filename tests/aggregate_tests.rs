use tracing::instrument;
use uuid::Uuid;

mod common;
mod test_utils;

use common::bank_account::{BankAccountAggregate, BankAccountCommand, BankAccountError};
use test_utils::{initialize_logger, create_test_store};

#[tokio::test]
#[instrument]
async fn test_bank_account_aggregate() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = create_test_store().await?;
    
    // Test opening an account
    let user_id = Uuid::new_v4().to_string();
    let open_command = BankAccountCommand::open_account(user_id.clone());
    
    let stream_id = store.execute_command::<BankAccountAggregate>(
        open_command,
        (),
    )
    .await?;

    // Test depositing funds
    let deposit_command = BankAccountCommand::deposit_funds(100, stream_id.clone());
    
    store.execute_command::<BankAccountAggregate>(
        deposit_command,
        (),
    )
    .await?;

    // Test withdrawing funds successfully
    let withdraw_command = BankAccountCommand::withdraw_funds(50, stream_id.clone());
    
    store.execute_command::<BankAccountAggregate>(
        withdraw_command,
        (),
    )
    .await?;

    // Test insufficient funds
    let withdraw_too_much = BankAccountCommand::withdraw_funds(1000, stream_id.clone());
    
    let result = store.execute_command::<BankAccountAggregate>(
        withdraw_too_much,
        (),
    )
    .await;
    
    assert!(matches!(
        result.unwrap_err().downcast::<BankAccountError>().unwrap(),
        BankAccountError::InsufficientFunds
    ));

    Ok(())
}

#[tokio::test]
#[instrument]
async fn test_bank_account_duplicate_open() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = create_test_store().await?;
    
    // First open command
    let account_id = Uuid::new_v4().to_string();
    let open_command = BankAccountCommand::open_account("user1".to_string())
        .with_account_id(account_id.clone());

    store.execute_command::<BankAccountAggregate>(
        open_command,
        (),
    )
    .await?;

    // Second open command with same account ID
    let duplicate_open = BankAccountCommand::open_account("user1".to_string())
        .with_account_id(account_id);

    let result = store.execute_command::<BankAccountAggregate>(
        duplicate_open,
        (),
    )
    .await;

    assert!(matches!(
        result.unwrap_err().downcast::<BankAccountError>().unwrap(),
        BankAccountError::AccountAlreadyExists
    ));

    Ok(())
}

#[tokio::test]
#[instrument]
async fn test_bank_account_nonexistent() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = create_test_store().await?;
    
    // Try to deposit to nonexistent account
    let account_id = Uuid::new_v4().to_string();
    let command = BankAccountCommand::withdraw_funds(100, account_id);
    
    let result = store.execute_command::<BankAccountAggregate>(
        command,
        (),
    )
    .await;
    
    assert!(matches!(
        result.unwrap_err().downcast::<BankAccountError>().unwrap(),
        BankAccountError::AccountNotFound
    ));

    Ok(())
} 