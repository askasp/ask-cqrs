use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::instrument;
use uuid::Uuid;

mod common;
mod test_utils;

use ask_cqrs::view::IndexView;
use common::bank_account::{BankAccountAggregate, BankAccountCommand};
use common::bank_account_view::{BankAccountView, UserAccountsIndexView};
use test_utils::{initialize_logger, create_test_store};

#[tokio::test]
#[instrument]
async fn test_bank_account_view_async() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = create_test_store().await?;
    
    // Generate a unique user ID for this test
    let user_id = Uuid::new_v4().to_string();

    // Open account
    let open_command = BankAccountCommand::open_account(user_id.clone());
    let account_id = store.execute_command::<BankAccountAggregate>(
        open_command,
        (),
    )
    .await?;

    // Deposit funds
    let deposit_command = BankAccountCommand::deposit_funds(100, account_id.clone());
    store.execute_command::<BankAccountAggregate>(
        deposit_command,
        (),
    )
    .await?;

    // Get view state
    let view = store.get_view_state::<BankAccountView>(&account_id).await?
        .expect("Account should exist in view");
    
    assert_eq!(view.balance, 100);
    assert_eq!(view.user_id, user_id);

    Ok(())
}

#[tokio::test]
#[instrument]
async fn test_bank_account_view_multiple() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = create_test_store().await?;
    
    // Generate a unique user ID for this test
    let user_id = Uuid::new_v4().to_string();

    // Open first account
    let open_command1 = BankAccountCommand::open_account(user_id.clone());
    let account_id1 = store.execute_command::<BankAccountAggregate>(
        open_command1,
        (),
    )
    .await?;

    // Open second account
    let open_command2 = BankAccountCommand::open_account(user_id.clone());
    let account_id2 = store.execute_command::<BankAccountAggregate>(
        open_command2,
        (),
    )
    .await?;

    // Deposit different amounts
    let deposit_command1 = BankAccountCommand::deposit_funds(100, account_id1.clone());
    store.execute_command::<BankAccountAggregate>(
        deposit_command1,
        (),
    )
    .await?;

    let deposit_command2 = BankAccountCommand::deposit_funds(200, account_id2.clone());
    store.execute_command::<BankAccountAggregate>(
        deposit_command2,
        (),
    )
    .await?;

    // Get and verify first account
    let view1 = store.get_view_state::<BankAccountView>(&account_id1).await?
        .expect("First account should exist in view");
    assert_eq!(view1.balance, 100);
    assert_eq!(view1.user_id, user_id);

    // Get and verify second account
    let view2 = store.get_view_state::<BankAccountView>(&account_id2).await?
        .expect("Second account should exist in view");
    assert_eq!(view2.balance, 200);
    assert_eq!(view2.user_id, user_id);

    Ok(())
}

#[tokio::test]
#[instrument]
async fn test_user_accounts_index() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = create_test_store().await?;
    
    // Generate two unique user IDs for this test
    let user_id1 = Uuid::new_v4().to_string();
    let user_id2 = Uuid::new_v4().to_string();

    // Create two accounts for first user
    let open_command1 = BankAccountCommand::open_account(user_id1.clone());
    let account_id1 = store.execute_command::<BankAccountAggregate>(
        open_command1,
        (),
    )
    .await?;

    let open_command2 = BankAccountCommand::open_account(user_id1.clone());
    let account_id2 = store.execute_command::<BankAccountAggregate>(
        open_command2,
        (),
    )
    .await?;

    // Create one account for second user
    let open_command3 = BankAccountCommand::open_account(user_id2.clone());
    let account_id3 = store.execute_command::<BankAccountAggregate>(
        open_command3,
        (),
    )
    .await?;

    // Get the index state
    let index_view = UserAccountsIndexView;
    let index = store.get_index_state::<UserAccountsIndexView>().await?;

    // Query accounts for first user
    let user1_accounts = index_view.query(&index, &user_id1);
    assert_eq!(user1_accounts.len(), 2);
    assert!(user1_accounts.contains(&account_id1));
    assert!(user1_accounts.contains(&account_id2));

    // Query accounts for second user
    let user2_accounts = index_view.query(&index, &user_id2);
    assert_eq!(user2_accounts.len(), 1);
    assert!(user2_accounts.contains(&account_id3));

    // Query accounts for non-existent user
    let non_existent_user = Uuid::new_v4().to_string();
    let no_accounts = index_view.query(&index, &non_existent_user);
    assert!(no_accounts.is_empty());

    Ok(())
}

#[tokio::test]
#[instrument]
async fn test_batch_view_loading() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = create_test_store().await?;
    
    // Generate a unique user ID for this test
    let user_id = Uuid::new_v4().to_string();

    // Create multiple accounts
    let mut account_ids = Vec::new();
    let amounts = [100, 200, 300];

    for amount in amounts {
        // Open account
        let open_command = BankAccountCommand::open_account(user_id.clone());
        let account_id = store.execute_command::<BankAccountAggregate>(
            open_command,
            (),
        )
        .await?;

        // Deposit funds
        let deposit_command = BankAccountCommand::deposit_funds(amount, account_id.clone());
        store.execute_command::<BankAccountAggregate>(
            deposit_command,
            (),
        )
        .await?;

        account_ids.push(account_id);
    }

    // Load all views in a single query
    let views = store.get_view_states::<BankAccountView>(&account_ids).await?;

    // Verify all accounts were loaded with correct balances
    assert_eq!(views.len(), 3);
    for (i, account_id) in account_ids.iter().enumerate() {
        let view = views.get(account_id).expect("Account should exist in views");
        assert_eq!(view.balance, amounts[i] as u64);
        assert_eq!(view.user_id, user_id);
    }

    Ok(())
} 