use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::instrument;
use uuid::Uuid;

mod common;
mod test_utils;

use ask_cqrs::{command::DomainCommand, execute_command, execute_command_sync, start_view_builder, view::ViewStore};
use common::bank_account::{BankAccountAggregate, BankAccountCommand};
use common::bank_account_view::BankAccountView;
use test_utils::{create_es_client, initialize_logger};
use eventstore::Client;

#[tokio::test]
#[instrument]
async fn test_bank_account_view_async() -> Result<(), anyhow::Error> {
    initialize_logger();
    let client = create_es_client();
    
    // Start the view builder
    let view_store = start_view_builder::<BankAccountView>(client.clone()).await?;

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

    // Wait for view to catch up
    sleep(Duration::from_millis(100)).await;

    // Deposit funds
    let deposit_command = BankAccountCommand::deposit_funds(100, account_id.clone());
    execute_command::<BankAccountAggregate>(
        client.clone(),
        deposit_command,
        (),
    )
    .await?;

    // Wait for view to catch up
    sleep(Duration::from_millis(100)).await;

    // Get state from view
    let state = view_store.get(&account_id).expect("Account should exist in view");
    assert_eq!(state.balance, 100);

    Ok(())
}

#[tokio::test]
#[instrument]
async fn test_bank_account_view_sync() -> Result<(), anyhow::Error> {
    initialize_logger();
    let client = create_es_client();
    
    // Start the view builder
    let view_store = start_view_builder::<BankAccountView>(client.clone()).await?;

    // Generate a unique user ID for this test
    let user_id = Uuid::new_v4().to_string();

    // Open account
    let open_command = BankAccountCommand::open_account(user_id);
    let account_id = execute_command_sync::<BankAccountAggregate>(
        client.clone(),
        open_command,
        (),
        vec![view_store.clone()],
    )
    .await?;

    // Deposit funds
    let deposit_command = BankAccountCommand::deposit_funds(100, account_id.clone());
    execute_command_sync::<BankAccountAggregate>(
        client.clone(),
        deposit_command,
        (),
        vec![view_store.clone()],
    )
    .await?;

    // Get state from view
    let state = view_store.get(&account_id).expect("Account should exist in view");
    assert_eq!(state.balance, 100);

    Ok(())
}

#[tokio::test]
#[instrument]
async fn test_bank_account_view_find() -> Result<(), anyhow::Error> {
    initialize_logger();
    let client = create_es_client();
    
    // Start the view builder
    let view_store = start_view_builder::<BankAccountView>(client.clone()).await?;

    // Generate a unique user ID for this test
    let user_id = Uuid::new_v4().to_string();

    // Open first account
    let open_command1 = BankAccountCommand::open_account(user_id.clone());
    let account_id1 = execute_command_sync::<BankAccountAggregate>(
        client.clone(),
        open_command1,
        (),
        vec![view_store.clone()],
    )
    .await?;

    // Open second account
    let open_command2 = BankAccountCommand::open_account(user_id.clone());
    let account_id2 = execute_command_sync::<BankAccountAggregate>(
        client.clone(),
        open_command2,
        (),
        vec![view_store.clone()],
    )
    .await?;

    // Deposit different amounts to distinguish the accounts
    let deposit_command1 = BankAccountCommand::deposit_funds(100, account_id1.clone());
    execute_command_sync::<BankAccountAggregate>(
        client.clone(),
        deposit_command1,
        (),
        vec![view_store.clone()],
    )
    .await?;

    let deposit_command2 = BankAccountCommand::deposit_funds(200, account_id2.clone());
    execute_command_sync::<BankAccountAggregate>(
        client.clone(),
        deposit_command2,
        (),
        vec![view_store.clone()],
    )
    .await?;

    // Find all accounts for this user
    let accounts = view_store.find(|state| state.user_id == user_id);
    assert_eq!(accounts.len(), 2);

    // Verify we have both accounts with correct balances
    let account1 = accounts.iter().find(|state| state.balance == 100).expect("Should find account with 100 balance");
    let account2 = accounts.iter().find(|state| state.balance == 200).expect("Should find account with 200 balance");
    
    assert_eq!(account1.user_id, user_id);
    assert_eq!(account2.user_id, user_id);

    Ok(())
} 