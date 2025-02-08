use std::sync::Arc;
use tracing::instrument;
use uuid::Uuid;
use serde_json::json;
use serial_test::serial;

mod common;
mod test_utils;

use ask_cqrs::postgres_store::PaginationOptions;
use common::bank_account::{BankAccountAggregate, BankAccountCommand};
use common::bank_account_view::BankAccountView;
use common::bank_liquidity_view::BankLiquidityView;
use test_utils::{initialize_logger, create_test_store};

const VIEW_TIMEOUT_MS: u64 = 5000;

async fn initialize_view_builders(store: &Arc<ask_cqrs::postgres_store::PostgresStore>) -> Result<(), anyhow::Error> {
    store.start_view_builder::<BankAccountView>().await?;
    store.start_view_builder::<BankLiquidityView>().await?;
    Ok(())
}

#[tokio::test]
#[instrument]
#[serial]
async fn test_bank_account_view_async() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = Arc::new(create_test_store().await?);
    
    // Start view builder
    store.start_view_builder::<BankAccountView>().await?;
    
    // Generate a unique user ID for this test
    let user_id = Uuid::new_v4().to_string();

    // Open account
    let result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id}),
    ).await?;

    // Wait for view to catch up
    store.wait_for_view::<BankAccountView>(&result.stream_id, result.global_position, VIEW_TIMEOUT_MS).await?;

    // Deposit funds
    let result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 100,
            account_id: result.stream_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    ).await?;

    // Wait for view to catch up
    store.wait_for_view::<BankAccountView>(&result.stream_id, result.global_position, VIEW_TIMEOUT_MS).await?;

    // Get view state - should be available now
    let view = store.get_view_state::<BankAccountView>(&result.stream_id).await?
        .expect("Account should exist in view");
    
    assert_eq!(view.balance, 100);
    assert_eq!(view.user_id, user_id);

    Ok(())
}

#[tokio::test]
#[instrument]
#[serial]
async fn test_bank_liquidity_view() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = Arc::new(create_test_store().await?);
    
    // Start view builder
    store.start_view_builder::<BankLiquidityView>().await?;
    
    tracing::info!("Starting liquidity view test...");
    
    // Create a few accounts and perform transactions
    let result1 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: "user1".to_string(),
            account_id: None,
        },
        (),
        json!({"user_id": "user1"}),
    ).await?;

    store.wait_for_view::<BankLiquidityView>("aggregate", result1.global_position, VIEW_TIMEOUT_MS).await?;
    let view = store.get_view_state::<BankLiquidityView>("aggregate").await?.unwrap();
    assert_eq!(view.total_accounts, 1);
    tracing::info!("After first account: {:?}", view);

    let result2 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: "user2".to_string(),
            account_id: None,
        },
        (),
        json!({"user_id": "user2"}),
    ).await?;

    store.wait_for_view::<BankLiquidityView>("aggregate", result2.global_position, VIEW_TIMEOUT_MS).await?;
    let view = store.get_view_state::<BankLiquidityView>("aggregate").await?.unwrap();
    assert_eq!(view.total_accounts, 2);
    tracing::info!("After second account: {:?}", view);

    // Deposit funds
    let result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 1000,
            account_id: result1.stream_id.clone(),
        },
        (),
        json!({"user_id": "user1"}),
    ).await?;

    store.wait_for_view::<BankLiquidityView>("aggregate", result.global_position, VIEW_TIMEOUT_MS).await?;
    let view = store.get_view_state::<BankLiquidityView>("aggregate").await?.unwrap();
    assert_eq!(view.total_balance, 1000);
    tracing::info!("After first deposit: {:?}", view);

    let result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 500,
            account_id: result2.stream_id.clone(),
        },
        (),
        json!({"user_id": "user2"}),
    ).await?;

    store.wait_for_view::<BankLiquidityView>("aggregate", result.global_position, VIEW_TIMEOUT_MS).await?;
    let view = store.get_view_state::<BankLiquidityView>("aggregate").await?.unwrap();
    assert_eq!(view.total_balance, 1500);
    tracing::info!("After second deposit: {:?}", view);

    // Withdraw some funds
    let result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::WithdrawFunds { 
            amount: 300,
            account_id: result1.stream_id.clone(),
        },
        (),
        json!({"user_id": "user1"}),
    ).await?;

    store.wait_for_view::<BankLiquidityView>("aggregate", result.global_position, VIEW_TIMEOUT_MS).await?;
    let view = store.get_view_state::<BankLiquidityView>("aggregate").await?.unwrap();
    assert_eq!(view.total_balance, 1200);
    tracing::info!("After withdrawal: {:?}", view);

    // Final assertions
    let liquidity = store.get_view_state::<BankLiquidityView>("aggregate").await?
        .expect("Liquidity view should exist");

    tracing::info!("Final liquidity view state: {:?}", liquidity);
    assert_eq!(liquidity.total_accounts, 2, "Expected 2 accounts");
    assert_eq!(liquidity.total_balance, 1200, "Expected 1200 balance"); // 1000 + 500 - 300 = 1200

    Ok(())
}

#[tokio::test]
#[instrument]
#[serial]
async fn test_view_query_pagination() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = Arc::new(create_test_store().await?);
    
    // Start view builder
    store.start_view_builder::<BankAccountView>().await?;
    
    // Generate unique user IDs
    let user_id1 = Uuid::new_v4().to_string();
    let user_id2 = Uuid::new_v4().to_string();

    // Create accounts
    let result1 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id1.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id1}),
    ).await?;

    store.wait_for_view::<BankAccountView>(&result1.stream_id, result1.global_position, VIEW_TIMEOUT_MS).await?;

    let result2 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id1.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id1}),
    ).await?;

    store.wait_for_view::<BankAccountView>(&result2.stream_id, result2.global_position, VIEW_TIMEOUT_MS).await?;

    // Test pagination
    let page1 = store.query_views::<BankAccountView>(
        "state->>'user_id' = ($2#>>'{}')::text",
        vec![json!(user_id1)],
        Some(PaginationOptions { page: 0, page_size: 1 })
    ).await?;
    assert_eq!(page1.total_count, 2);
    assert_eq!(page1.items.len(), 1);
    assert_eq!(page1.total_pages, 2);

    let page2 = store.query_views::<BankAccountView>(
        "state->>'user_id' = ($2#>>'{}')::text",
        vec![json!(user_id1)],
        Some(PaginationOptions { page: 1, page_size: 1 })
    ).await?;
    assert_eq!(page2.total_count, 2);
    assert_eq!(page2.items.len(), 1);
    assert_eq!(page2.total_pages, 2);

    // Verify we found both accounts across pages
    let all_accounts: Vec<String> = page1.items.into_iter()
        .chain(page2.items.into_iter())
        .map(|view| view.account_id)
        .collect();
    assert_eq!(all_accounts.len(), 2);
    assert!(all_accounts.contains(&result1.stream_id));
    assert!(all_accounts.contains(&result2.stream_id));

    Ok(())
} 