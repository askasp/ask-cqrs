use std::sync::Arc;
use ask_cqrs::store::EventProcessingConfig;
use ask_cqrs::test_utils::create_test_store;
use tracing::instrument;
use uuid::Uuid;
use serde_json::json;
use serial_test::serial;
use chrono::Utc;

mod common;

use ask_cqrs::store::{EventStore, ViewStore, postgres_event_store::PostgresEventStore, event_store::PaginationOptions};
use ask_cqrs::event_handler::{EventHandler, EventRow};
use common::bank_account::{BankAccountAggregate, BankAccountCommand, BankAccountEvent};
use common::bank_account_view::BankAccountView;
use common::bank_liquidity_view::BankLiquidityView;
use common::bank_notification_view::BankNotificationView;
use ask_cqrs::test_utils::{initialize_logger};

const VIEW_TIMEOUT_MS: u64 = 5000;
#[tokio::test]
#[instrument]
#[serial]
async fn test_bank_account_view_async() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = Arc::new(create_test_store("postgres://postgres:postgres@localhost:5432/ask_cqrs_test2").await?);
    
    // Create view store
    let view_store = store.create_view_store();
    
    // Start view builder
    store.start_view::<BankAccountView>(view_store.clone(),None ).await?;
    
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

    // Get the last event created
    let last_event = result.events.last().expect("At least one event should be created");
    
    // Wait for view to catch up using the exact event
    view_store.wait_for_view::<BankAccountView>(last_event, VIEW_TIMEOUT_MS).await?;

    // Deposit funds
    let result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 100,
            account_id: result.stream_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    ).await?;

    // Get the last event created
    let last_event = result.events.last().expect("At least one event should be created");
    
    // Wait for view to catch up using the exact event
    view_store.wait_for_view::<BankAccountView>(last_event, VIEW_TIMEOUT_MS).await?;

    // Get view state - should be available now
    let view = view_store.get_view_state::<BankAccountView>(&result.stream_id).await?
        .expect("Account should exist in view");
    
    assert_eq!(view.balance, 100);
    assert_eq!(view.user_id, user_id);

    Ok(())
}

#[tokio::test]
#[instrument]
#[serial]
async fn test_duplicate_event_processing() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = Arc::new(create_test_store("postgres://postgres:postgres@localhost:5432/ask_cqrs_test2").await?);
    
    // Create view store
    let view_store = store.create_view_store();
    
    // Start notification view
    store.start_view::<BankNotificationView>(view_store.clone(), None).await?;
    
    // Create an account
    let user_id = Uuid::new_v4().to_string();
    let result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id}),
    ).await?;
    let account_id = result.stream_id.clone();
    let last_event = result.events.last().expect("At least one event should be created");
    view_store.wait_for_view::<BankNotificationView>(last_event, VIEW_TIMEOUT_MS).await?;
    
    // Get the initial view state
    let view_before = view_store.get_view_state::<BankNotificationView>(&account_id).await?
        .expect("View should exist");
    
    tracing::info!("Initial view state: {:?}", view_before);
    assert_eq!(view_before.notifications.len(), 1, "Should have 1 notification");
    
    // Now manually process the same event again by directly calling the view handler
    // This simulates a duplicate event being processed
    let event_handler = view_store.create_event_handler::<BankNotificationView>();
    event_handler.handle_event(
        BankAccountEvent::AccountOpened { 
            user_id: user_id.clone(), 
            account_id: account_id.clone() 
        },
        last_event.clone()
    ).await?;
    
    // Get the view state after processing the duplicate event
    let view_after = view_store.get_view_state::<BankNotificationView>(&account_id).await?
        .expect("View should exist");
    
    tracing::info!("View state after duplicate event: {:?}", view_after);
    
    // Verify that the duplicate event didn't add another notification
    assert_eq!(view_after.notifications.len(), 1, 
        "Should still have only 1 notification after processing duplicate event");
    
    // Verify that the processed_stream_positions was properly updated
    // We can check this indirectly by confirming the event is considered processed
    let is_processed = view_store.is_event_processed::<BankNotificationView>(&account_id, last_event).await?;
    assert!(is_processed, "Event should be marked as processed");
    
    // Now make a real change to verify normal processing still works
    let deposit_result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 100,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    ).await?;
    let deposit_event = deposit_result.events.last().expect("At least one event should be created");
    view_store.wait_for_view::<BankNotificationView>(deposit_event, VIEW_TIMEOUT_MS).await?;
    
    // Get the view state after the deposit
    let view_after_deposit = view_store.get_view_state::<BankNotificationView>(&account_id).await?
        .expect("View should exist");
    
    tracing::info!("View state after deposit: {:?}", view_after_deposit);
    assert_eq!(view_after_deposit.notifications.len(), 2, 
        "Should have 2 notifications after deposit");
    
    Ok(())
}

#[tokio::test]
#[instrument]
#[serial]
async fn test_bank_liquidity_view() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = Arc::new(create_test_store("postgres://postgres:postgres@localhost:5432/ask_cqrs_test2").await?);
    
    // Create view store
    let view_store = store.create_view_store();
    
    // Start view builder
    store.start_view::<BankLiquidityView>(view_store.clone(), None).await?;
    
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

    let last_event1 = result1.events.last().expect("At least one event should be created");
    tracing::info!("Created first account: {}", result1.stream_id);
    view_store.wait_for_view::<BankLiquidityView>(last_event1, VIEW_TIMEOUT_MS).await?;
    let view = view_store.get_view_state::<BankLiquidityView>("aggregate").await?.unwrap();
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
    let last_event2 = result2.events.last().expect("At least one event should be created");

    tracing::info!("Created second account: {}", result2.stream_id);
    
    
    view_store.wait_for_view::<BankLiquidityView>(last_event2, VIEW_TIMEOUT_MS).await?;
    let view = view_store.get_view_state::<BankLiquidityView>("aggregate").await?.unwrap();
    tracing::info!("After second account, total_accounts = {}, expected 2", view.total_accounts);
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
    let last_event = result.events.last().expect("At least one event should be created");

    view_store.wait_for_view::<BankLiquidityView>(last_event, VIEW_TIMEOUT_MS).await?;
    let view = view_store.get_view_state::<BankLiquidityView>("aggregate").await?.unwrap();
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
    let last_event = result.events.last().expect("At least one event should be created");

    view_store.wait_for_view::<BankLiquidityView>(last_event, VIEW_TIMEOUT_MS).await?;
    let view = view_store.get_view_state::<BankLiquidityView>("aggregate").await?.unwrap();
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
    let last_event = result.events.last().expect("At least one event should be created");

    view_store.wait_for_view::<BankLiquidityView>(last_event, VIEW_TIMEOUT_MS).await?;
    let view = view_store.get_view_state::<BankLiquidityView>("aggregate").await?.unwrap();
    assert_eq!(view.total_balance, 1200);
    tracing::info!("After withdrawal: {:?}", view);

    // Final assertions
    let liquidity = view_store.get_view_state::<BankLiquidityView>("aggregate").await?
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
    let store = Arc::new(create_test_store("postgres://postgres:postgres@localhost:5432/ask_cqrs_test2").await?);
    
    // Create view store
    let view_store = store.create_view_store();
    
    // Start view builder
    store.start_view::<BankAccountView>(view_store.clone(), None).await?;
    
    // Generate unique user IDs
    let user_id1 = Uuid::new_v4().to_string();

       // Create accounts
    let result1 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id1.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id1}),
    ).await?;

    let last_event1 = result1.events.last().expect("At least one event should be created");
    view_store.wait_for_view::<BankAccountView>(last_event1, VIEW_TIMEOUT_MS).await?;

    let result2 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id1.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id1}),
    ).await?;

    let last_event2 = result2.events.last().expect("At least one event should be created");
    view_store.wait_for_view::<BankAccountView>(last_event2, VIEW_TIMEOUT_MS).await?;

    // Test pagination
    let page1 = view_store.query_views::<BankAccountView>(
        "state->>'user_id' = ($2#>>'{}')::text",
        vec![json!(user_id1)],
        Some(PaginationOptions { page: 0, page_size: 1 })
    ).await?;
    assert_eq!(page1.total_count, 2);
    assert_eq!(page1.items.len(), 1);
    assert_eq!(page1.total_pages, 2);

    let page2 = view_store.query_views::<BankAccountView>(
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

#[tokio::test]
#[instrument]
#[serial]
async fn test_view_start_from_beginning() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = Arc::new(create_test_store("postgres://postgres:postgres@localhost:5432/ask_cqrs_test2").await?);
    
    // Create view store
    let view_store = store.create_view_store();
    
    // Generate unique user ID
    let user_id = Uuid::new_v4().to_string();

    // First create some events without the view running
    let result1 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id}),
    ).await?;
    let account_id = result1.stream_id.clone();
    
    // Add some deposits
    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 500,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    ).await?;
    
    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 300,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    ).await?;
    
    // Now start the view with start_from_beginning: true
    let config = EventProcessingConfig {
        start_from_beginning: true,
        ..Default::default()
    };
    
    tracing::info!("Starting view with start_from_beginning=true");
    store.start_view::<BankAccountView>(view_store.clone(), Some(config)).await?;
    
    // Get the last created event to wait for
    let events = store.get_events_for_stream("bank_account", &account_id, -1, 10).await?;
    let last_event = events.last().expect("At least one event should exist");
    
    // Wait for view to catch up
    view_store.wait_for_view::<BankAccountView>(last_event, VIEW_TIMEOUT_MS).await?;

    // Get view state - should have processed all events from the beginning
    let view = view_store.get_view_state::<BankAccountView>(&account_id).await?
        .expect("Account should exist in view");
    
    tracing::info!("View state after processing: {:?}", view);
    assert_eq!(view.balance, 800, "Balance should reflect all deposits (500+300)");
    assert_eq!(view.user_id, user_id);

    Ok(())
}

#[tokio::test]
#[instrument]
#[serial]
async fn test_view_reset() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = Arc::new(create_test_store("postgres://postgres:postgres@localhost:5432/ask_cqrs_test2").await?);
    
    // Create view store
    let view_store = store.create_view_store();
    
    // Generate unique user ID
    let user_id = Uuid::new_v4().to_string();

    // First create some events without the view running
    let result1 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id}),
    ).await?;
    let account_id = result1.stream_id.clone();
    
    // Add some deposits
    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 500,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    ).await?;
    
    // Now start the view
    tracing::info!("Starting view");
    store.start_view::<BankAccountView>(view_store.clone(), None).await?;
    
    // Get the last created event to wait for
    let events = store.get_events_for_stream("bank_account", &account_id, -1, 10).await?;
    let last_event = events.last().expect("At least one event should exist");
    
    // Wait for view to catch up
    view_store.wait_for_view::<BankAccountView>(last_event, VIEW_TIMEOUT_MS).await?;

    // Get view state - should have processed all events
    let view = view_store.get_view_state::<BankAccountView>(&account_id).await?
        .expect("View should exist");
    
    tracing::info!("View state before reset: {:?}", view);
    assert_eq!(view.balance, 500, "Expected balance to be 500");
    
    // Reset the view
    tracing::info!("Resetting view");
    store.reset_view::<BankAccountView>().await?;
    
    // Verify the view was reset
    let view_after_reset = view_store.get_view_state::<BankAccountView>(&account_id).await?;
    assert!(view_after_reset.is_none(), "View should not exist after reset");
    
    // Restart the view with start_from_beginning=true
    tracing::info!("Restarting view with start_from_beginning=true");
    let config = EventProcessingConfig {
        start_from_beginning: true,
        ..Default::default()
    };
    store.start_view::<BankAccountView>(view_store.clone(), Some(config)).await?;
    
    // Wait for view to catch up again
    view_store.wait_for_view::<BankAccountView>(last_event, VIEW_TIMEOUT_MS).await?;
    
    // Get view state again - should have processed all events again
    let view_after_restart = view_store.get_view_state::<BankAccountView>(&account_id).await?
        .expect("View should exist after restart");
    
    tracing::info!("View state after restart: {:?}", view_after_restart);
    assert_eq!(view_after_restart.balance, 500, "Expected balance to be 500 after restart");
    
    Ok(())
}

#[tokio::test]
#[instrument]
#[serial]
async fn test_notification_view_with_update_all() -> Result<(), anyhow::Error> {
    initialize_logger();
    let store = Arc::new(create_test_store("postgres://postgres:postgres@localhost:5432/ask_cqrs_test2").await?);
    
    // Create view store
    let view_store = store.create_view_store();
    
    // Start notification view
    store.start_view::<BankNotificationView>(view_store.clone(), None).await?;
    
    // Create two accounts
    let user1_id = Uuid::new_v4().to_string();
    let user2_id = Uuid::new_v4().to_string();
    
    // Create first account
    let result1 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user1_id.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user1_id}),
    ).await?;
    let account1_id = result1.stream_id.clone();
    let last_event1 = result1.events.last().expect("At least one event should be created");
    view_store.wait_for_view::<BankNotificationView>(last_event1, VIEW_TIMEOUT_MS).await?;
    
    // Create second account
    let result2 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user2_id.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user2_id}),
    ).await?;
    let account2_id = result2.stream_id.clone();
    let last_event2 = result2.events.last().expect("At least one event should be created");
    view_store.wait_for_view::<BankNotificationView>(last_event2, VIEW_TIMEOUT_MS).await?;
    
    // Make some transactions on first account
    let deposit_result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 500,
            account_id: account1_id.clone(),
        },
        (),
        json!({"user_id": user1_id}),
    ).await?;
    let last_deposit_event = deposit_result.events.last().expect("At least one event should be created");
    view_store.wait_for_view::<BankNotificationView>(last_deposit_event, VIEW_TIMEOUT_MS).await?;
    
    // Check notifications for first account
    let view1 = view_store.get_view_state::<BankNotificationView>(&account1_id).await?
        .expect("View should exist for account 1");
    
    tracing::info!("Account 1 notifications: {:?}", view1.notifications);
    assert_eq!(view1.notifications.len(), 2, "Should have 2 notifications (open + deposit)");
    assert!(view1.notifications[1].contains("Deposit of $500"), "Should have deposit notification");
    assert_eq!(view1.global_notifications.len(), 0, "Should have no global notifications yet");
    
    // Check notifications for second account
    let view2 = view_store.get_view_state::<BankNotificationView>(&account2_id).await?
        .expect("View should exist for account 2");
    
    tracing::info!("Account 2 notifications: {:?}", view2.notifications);
    assert_eq!(view2.notifications.len(), 1, "Should have 1 notification (open)");
    assert_eq!(view2.global_notifications.len(), 0, "Should have no global notifications yet");
    
    // Now suspend the first account - this should trigger update_all
    let suspend_result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::SuspendAccount { 
            account_id: account1_id.clone(),
        },
        (),
        json!({"user_id": user1_id}),
    ).await?;
    let last_suspend_event = suspend_result.events.last().expect("At least one event should be created");
    view_store.wait_for_view::<BankNotificationView>(last_suspend_event, VIEW_TIMEOUT_MS).await?;
    
    // Check that both accounts received the global notification
    let view1_after = view_store.get_view_state::<BankNotificationView>(&account1_id).await?
        .expect("View should exist for account 1");
    
    tracing::info!("Account 1 notifications after suspend: {:?}", view1_after);
    assert_eq!(view1_after.global_notifications.len(), 1, "Should have 1 global notification");
    assert!(view1_after.global_notifications[0].contains("Account suspended"), 
            "Should have suspension notification");
    
    let view2_after = view_store.get_view_state::<BankNotificationView>(&account2_id).await?
        .expect("View should exist for account 2");
    
    tracing::info!("Account 2 notifications after suspend: {:?}", view2_after);
    assert_eq!(view2_after.global_notifications.len(), 1, "Should have 1 global notification");
    assert!(view2_after.global_notifications[0].contains("Account suspended"), 
            "Should have suspension notification");
    
    // Verify account 2 still only has its original notification plus the global one
    assert_eq!(view2_after.notifications.len(), 1, "Account 2 should still have only 1 notification");
    
    Ok(())
}
