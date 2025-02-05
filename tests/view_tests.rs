use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::instrument;
use uuid::Uuid;
use serde_json::json;

mod common;
mod test_utils;

use ask_cqrs::view::GlobalView;
use common::bank_account::{BankAccountAggregate, BankAccountCommand};
use common::bank_account_view::{BankAccountView, UserAccountsIndexView};
use common::bank_liquidity_view::BankLiquidityView;
use test_utils::{initialize_logger, create_test_store};

#[tokio::test]
#[instrument]
async fn test_bank_account_view_async() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = create_test_store().await?;
    
    // Generate a unique user ID for this test
    let user_id = Uuid::new_v4().to_string();

    // Open account
    let account_id = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id}),
    ).await?;

    // Deposit funds
    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 100,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    ).await?;

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
    let account_id1 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id}),
    ).await?;

    // Open second account
    let account_id2 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id}),
    ).await?;

    // Deposit different amounts
    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 100,
            account_id: account_id1.clone(),
        },
        (),
        json!({"user_id": user_id}),
    ).await?;

    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 200,
            account_id: account_id2.clone(),
        },
        (),
        json!({"user_id": user_id}),
    ).await?;

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
    let account_id1 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id1.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id1}),
    ).await?;

    let account_id2 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id1.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id1}),
    ).await?;

    // Create one account for second user
    let account_id3 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id2.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id2}),
    ).await?;

    // Get the index state
    let index_view = UserAccountsIndexView;
    let index = store.get_global_state::<UserAccountsIndexView>().await?;

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
        let account_id = store.execute_command::<BankAccountAggregate>(
            BankAccountCommand::OpenAccount { 
                user_id: user_id.clone(),
                account_id: None,
            },
            (),
            json!({"user_id": user_id}),
        ).await?;

        // Deposit funds
        store.execute_command::<BankAccountAggregate>(
            BankAccountCommand::DepositFunds { 
                amount,
                account_id: account_id.clone(),
            },
            (),
            json!({"user_id": user_id}),
        ).await?;

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

#[tokio::test]
async fn test_bank_liquidity_view() -> Result<(), anyhow::Error> {
    let store = create_test_store().await?;
    
    // Create a few accounts and perform transactions
    let account_id1 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: "user1".to_string(),
            account_id: None,
        },
        (),
        json!({"user_id": "user1"}),
    ).await?;

    let account_id2 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: "user2".to_string(),
            account_id: None,
        },
        (),
        json!({"user_id": "user2"}),
    ).await?;

    // Deposit funds
    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 1000,
            account_id: account_id1.clone(),
        },
        (),
        json!({"user_id": "user1"}),
    ).await?;

    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 500,
            account_id: account_id2.clone(),
        },
        (),
        json!({"user_id": "user2"}),
    ).await?;

    // Withdraw some funds
    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::WithdrawFunds { 
            amount: 300,
            account_id: account_id1.clone(),
        },
        (),
        json!({"user_id": "user1"}),
    ).await?;

    // Get the liquidity view
    let liquidity = store.get_global_state::<BankLiquidityView>().await?;

    // Check the results - we expect at least 2 accounts since we created them in this test
    assert!(liquidity.total_accounts >= 2, "Expected at least 2 accounts, got {}", liquidity.total_accounts);
    assert!(liquidity.total_balance >= 1200, "Expected at least 1200 balance, got {}", liquidity.total_balance); // 1000 + 500 - 300 = 1200

    // Test snapshot functionality
    store.save_global_snapshot::<BankLiquidityView>(&liquidity).await?;

    // Make another transaction
    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 800,
            account_id: account_id2.clone(),
        },
        (),
        json!({"user_id": "user2"}),
    ).await?;

    // Get the view again, should load from snapshot and apply new events
    let liquidity = store.get_global_state::<BankLiquidityView>().await?;
    assert!(liquidity.total_balance >= 2000, "Expected at least 2000 balance, got {}", liquidity.total_balance); // 1200 + 800 = 2000

    Ok(())
} 