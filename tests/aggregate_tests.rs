use std::sync::Arc;
use tracing::instrument;
use uuid::Uuid;

mod common;
mod test_utils;

use ask_cqrs::view::ViewStore;
use common::bank_account::{BankAccountAggregate, BankAccountCommand, BankAccountError};
use common::bank_account_view::BankAccountView;
use test_utils::{create_es_client, initialize_logger};
use ask_cqrs::{command::DomainCommand, execute_command};

#[tokio::test]
#[instrument]
async fn test_bank_account_aggregate() -> Result<(), anyhow::Error> {
    initialize_logger();
    let client = create_es_client();
    
    // Test opening an account
    let user_id = Uuid::new_v4().to_string();
    let open_command = BankAccountCommand::open_account(user_id.clone());
    
    let stream_id = ask_cqrs::execute_command::<BankAccountAggregate>(
        client.clone(),
        open_command,
        (),
    )
    .await?;

    // Test depositing funds
    let deposit_command = BankAccountCommand::deposit_funds(100, stream_id.clone());
    
    ask_cqrs::execute_command::<BankAccountAggregate>(
        client.clone(),
        deposit_command,
        (),
    )
    .await?;

    // Test withdrawing funds successfully
    let withdraw_command = BankAccountCommand::withdraw_funds(50, stream_id.clone());
    
    ask_cqrs::execute_command::<BankAccountAggregate>(
        client.clone(),
        withdraw_command,
        (),
    )
    .await?;

    // Test insufficient funds
    let withdraw_too_much = BankAccountCommand::withdraw_funds(1000, stream_id.clone());
    
    let result = ask_cqrs::execute_command::<BankAccountAggregate>(
        client.clone(),
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
    let client = create_es_client();
    
    // First open command
    let account_id = Uuid::new_v4().to_string();
    let open_command = BankAccountCommand::open_account("user1".to_string())
        .with_account_id(account_id.clone());

    execute_command::<BankAccountAggregate>(
        client.clone(),
        open_command,
        (),
    )
    .await?;

    // Second open command with same account ID
    let duplicate_open = BankAccountCommand::open_account("user1".to_string())
        .with_account_id(account_id);

    let result = execute_command::<BankAccountAggregate>(
        client,
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
    let client = create_es_client();
    
    // Try to deposit to nonexistent account
    let account_id = Uuid::new_v4().to_string();
    let command = BankAccountCommand::withdraw_funds(100, account_id);
    
    let result = execute_command::<BankAccountAggregate>(
        client,
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