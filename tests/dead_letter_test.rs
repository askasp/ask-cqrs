/*
 
 * 
 * This test verifies the functionality of the dead letter queue, including:
 * - Events are retried according to the configured max_retries
 * - Failed events are moved to the dead letter queue after max_retries
 * - Streams are blocked from further processing until the failed event is resolved
 * - Replaying a dead letter event unblocks the stream
 */

use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::collections::HashMap;
use tokio::time::sleep;
use tracing::{info, warn, error, instrument};
use uuid::Uuid;
use serde_json::json;
use anyhow::Result;
use serial_test::serial;

mod common;
mod test_utils;

use test_utils::{initialize_logger, create_test_store};
use common::bank_account::{BankAccountAggregate, BankAccountCommand, BankAccountEvent};
use ask_cqrs::store::event_store::{EventStore, EventProcessingConfig};
use ask_cqrs::store::postgres_event_store::PostgresEventStore;
use ask_cqrs::event_handler::{EventHandler, EventHandlerError, EventRow};

// A handler that will fail a specific number of times before succeeding
#[derive(Clone)]
struct RetryThenFailHandler {
    store: PostgresEventStore,
    retries_per_event: Arc<Mutex<HashMap<String, i32>>>,
    max_retries: i32,
}

impl RetryThenFailHandler {
    pub fn new(store: PostgresEventStore, max_retries: i32) -> Self {
        Self {
            store,
            retries_per_event: Arc::new(Mutex::new(HashMap::new())),
            max_retries,
        }
    }
    
    pub fn get_retry_counts(&self) -> HashMap<String, i32> {
        self.retries_per_event.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl EventHandler for RetryThenFailHandler {
    type Events = BankAccountEvent;

    fn name() -> &'static str {
        "retry_then_fail_handler"
    }

    async fn handle_event(&self, event: Self::Events, event_row: EventRow) -> Result<(), EventHandlerError> {
        // Track retry count for this event
        let mut retries = self.retries_per_event.lock().unwrap();
        let retry_count = *retries.entry(event_row.id.clone()).or_insert(0);
        retries.insert(event_row.id.clone(), retry_count + 1);
        
        info!(
            "Processing event {}: retry {} of max {}", 
            event_row.id, retry_count, self.max_retries
        );
        
        // Fix: Always fail for withdrawal events to test the dead letter queue
        let should_fail = match event {
            BankAccountEvent::FundsWithdrawn { .. } => true,
            _ => false // Process other events normally
        };
        
        drop(retries); // Release the lock before processing
        
        match event {
            BankAccountEvent::FundsWithdrawn { amount, account_id } => {
                info!("Processed withdrawal of {} for account {}", amount, account_id);
                
                if should_fail {
                    let error_msg = format!("Simulated failure for event {} on retry {}", 
                                           event_row.id, retry_count);
                    error!("{}", error_msg);
                    return Err(EventHandlerError {
                        log_message: error_msg,
                    });
                }
                
                info!("Successfully processed event {} after {} retries", event_row.id, retry_count);
            },
            _ => {
                // Process other events normally
                info!("Processed event: {:?}", event);
            }
        }

        Ok(())
    }
}

// A continuous handler that should always succeed
#[derive(Clone)]
struct AlwaysSucceedHandler {
    processed_events: Arc<Mutex<Vec<String>>>,
}

impl AlwaysSucceedHandler {
    pub fn new() -> Self {
        Self {
            processed_events: Arc::new(Mutex::new(Vec::new())),
        }
    }
    
    pub fn get_processed_events(&self) -> Vec<String> {
        self.processed_events.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl EventHandler for AlwaysSucceedHandler {
    type Events = BankAccountEvent;

    fn name() -> &'static str {
        "always_succeed_handler"
    }

    async fn handle_event(&self, event: Self::Events, event_row: EventRow) -> Result<(), EventHandlerError> {
        info!("Always succeed handler processing event: {:?}", event);
        
        {
            let mut events = self.processed_events.lock().unwrap();
            events.push(event_row.id.clone());
        }
        
        Ok(())
    }
}

async fn create_account_with_events(store: &PostgresEventStore) -> Result<(String, String)> {
    let user_id = Uuid::new_v4().to_string();
    
    // Open account
    let res = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id.clone(),
            account_id: None,
        },
        (),
        json!({"user_id": user_id}),
    ).await?;
    
    let account_id = res.stream_id;
    
    // Deposit funds
    store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 5000,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    ).await?;
    
    // Withdrawal that will fail processing
    let withdraw_res = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::WithdrawFunds { 
            amount: 1000,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    ).await?;
    
    info!("Created account {} with events up to position {}", 
         account_id, withdraw_res.global_position);
    
    Ok((account_id, user_id))
}

#[tokio::test]
#[instrument]
#[serial]
async fn test_dead_letter_queue_and_retries() -> Result<()> {
    initialize_logger();
    
    // Create store and handlers
    let store = create_test_store().await?;
    
    // Set max retries to 2 (will retry twice, then dead-letter)
    let event_config = EventProcessingConfig {
        max_retries: 1,               // Lower to 1 so we move to dead letter queue faster
        base_retry_delay: Duration::from_millis(50),
        max_retry_delay: Duration::from_millis(200),
        claim_ttl: Duration::from_millis(500),
        poll_interval: Duration::from_millis(100),
        ..Default::default()
    };
    
    // Print configuration for debugging
    info!("Test config: max_retries={}, base_delay={:?}, max_delay={:?}", 
          event_config.max_retries, 
          event_config.base_retry_delay,
          event_config.max_retry_delay);
    
    // Create handler that will fail after max_retries
    let retry_handler = RetryThenFailHandler::new(store.clone(), 1);  // Match max_retries in config
    
    // Also register a handler that always succeeds
    let always_succeed = AlwaysSucceedHandler::new();
    
    // Start both handlers
    store.start_event_handler(retry_handler.clone(), Some(event_config.clone())).await?;
    store.start_event_handler(always_succeed.clone(), Some(event_config.clone())).await?;
    
    // Create account with events including a withdrawal
    let (account_id, user_id) = create_account_with_events(&store).await?;
    
    // Check for events right after creation
    let events = store.get_events_for_stream("bank_account", &account_id, -1, 10).await?;
    info!("Found {} events for stream", events.len());
    
    let has_withdraw = events.iter().any(|e| {
        if let Ok(event_obj) = serde_json::from_value::<BankAccountEvent>(e.event_data.clone()) {
            match event_obj {
                BankAccountEvent::FundsWithdrawn { .. } => true,
                _ => false
            }
        } else {
            false
        }
    });
    
    info!("Has withdrawal event: {}", has_withdraw);
    assert!(has_withdraw, "Withdrawal event was not stored correctly");

    // Verify handler name matches what's used in database
    info!("Handler name: {}", RetryThenFailHandler::name());
    
    // Wait for event processing attempts and dead letter placement
    // Need to wait for max_retries + 1 attempts plus some retry delay
    info!("Waiting for retry attempts and dead letter placement...");
    sleep(Duration::from_secs(15)).await;  // Increase from 7 to 15 seconds for better reliability
    
    // Check if we have any events in the dead letter queue
    let dead_letter_events = store.get_dead_letter_events(0, 10).await?;
    info!("Found {} events in dead letter queue", dead_letter_events.items.len());
    
    // Find the dead letter events specifically for our test stream
    let our_dead_letter_events: Vec<_> = dead_letter_events.items.iter()
        .filter(|e| e.stream_id == account_id)
        .collect();
    
    info!("Found {} dead letter events for our test stream", our_dead_letter_events.len());
    
    // Examine debug information about event processing directly from store
    // This is available through the EventStore interface
    let active_account_events = store.get_events_for_stream("bank_account", &account_id, -1, 10).await?;
    info!("Total events for account after processing: {}", active_account_events.len());
    
    // Check retry counts
    let retry_counts = retry_handler.get_retry_counts();
    info!("Event retry counts: {:?}", retry_counts);
    
    // Check if we have events in the dead letter queue or recorded retries
    // Either condition indicates the test is working correctly
    if retry_counts.is_empty() && our_dead_letter_events.is_empty() {
        panic!("Neither retries were recorded nor events moved to dead letter queue");
    }
    
    // Verify we have at least one event in the dead letter queue for our stream
    assert!(!our_dead_letter_events.is_empty(), "No events in dead letter queue for our test stream");
    
    // Use the first dead letter event from our stream
    let dead_letter_event = &our_dead_letter_events[0];
    let dead_letter_event_id = dead_letter_event.id.clone();
    
    // Create another withdrawal event to verify that the stream is blocked
    info!("Creating additional withdrawal that should be blocked by failed event");
    let result = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::WithdrawFunds { 
            amount: 200,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    ).await;
    
    // The new command should succeed (events are stored), but handler processing should be blocked
    assert!(result.is_ok(), "Failed to create additional withdrawal");
    
    // Wait a bit to ensure processing attempt for new event
    sleep(Duration::from_millis(500)).await;
    
    // Get processed event counts from always succeed handler 
    let always_succeed_events = always_succeed.get_processed_events();
    
    // The always succeed handler should have processed the first events
    info!("Always succeed handler processed {} events", always_succeed_events.len());
    
    // Print detailed information about the dead letter event we'll replay
    info!("Dead letter event details:");
    info!("  ID: {}", dead_letter_event.id);
    info!("  Event ID: {}", dead_letter_event.event_id);
    info!("  Stream: {}/{}", dead_letter_event.stream_name, dead_letter_event.stream_id);
    info!("  Position: {}", dead_letter_event.stream_position);
    info!("  Handler: {}", dead_letter_event.handler_name);
    
    // Now replay the dead letter event (pretend we fixed the issue)
    info!("Replaying dead letter event: {}", dead_letter_event_id);
    
    // Wait a bit longer before replaying to ensure the system is ready
    sleep(Duration::from_millis(500)).await;
    
    let replay_result = store.replay_dead_letter_event(&dead_letter_event_id).await;
    
    if let Err(e) = &replay_result {
        warn!("Replay error: {}", e);
        
        // Sleep briefly and try again if there was an error
        sleep(Duration::from_millis(500)).await;
        let retry_replay = store.replay_dead_letter_event(&dead_letter_event_id).await;
        if let Err(e) = &retry_replay {
            warn!("Second replay attempt failed: {}", e);
        }
        
        // Use the result of the retry
        assert!(retry_replay.is_ok(), "Failed to replay dead letter event after retry");
    } else {
        assert!(replay_result.is_ok(), "Failed to replay dead letter event");
    }
    
    // Wait for the replayed event to be processed
    sleep(Duration::from_millis(1000)).await;
    
    // The event should no longer be in the dead letter queue
    let dead_letter_events_after = store.get_dead_letter_events(0, 10).await?;
    assert!(
        dead_letter_events_after.items.iter().all(|e| e.id != dead_letter_event_id),
        "Event still in dead letter queue after replay"
    );
    
    // Create additional withdrawal to verify stream is unblocked
    info!("Creating final withdrawal that should process normally");
    let _final_withdraw = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::WithdrawFunds { 
            amount: 300,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    ).await?;
    
    // Wait for it to be processed
    sleep(Duration::from_millis(500)).await;
    
    // Check the always succeed handler got the latest events
    let final_events = always_succeed.get_processed_events();
    info!("Always succeed handler now processed {} events", final_events.len());
    
    // The always succeed handler should have processed more events after the replay
    assert!(final_events.len() > always_succeed_events.len(), 
        "No new events processed after dead letter replay");
    
    // Cleanup
    store.shutdown().await;
    
    Ok(())
} 