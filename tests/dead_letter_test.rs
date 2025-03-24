/*
 * This test verifies the retry handling functionality by following a simple sequence:
 * 1. First event succeeds
 * 2. Second event fails first, then succeeds on retry
 * 3. Third event fails repeatedly and gets marked as dead-lettered
 * 4. Fourth event is never processed (would require manual intervention)
 */

use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::collections::HashMap;
use ask_cqrs::aggregate::Aggregate;
use tokio::time::sleep;
use tracing::{info, error, instrument};
use uuid::Uuid;
use serde_json::json;
use anyhow::Result;
use serial_test::serial;
use sqlx::Row;

mod common;
mod test_utils;

use test_utils::{initialize_logger, create_test_store};
use common::bank_account::{BankAccountAggregate, BankAccountCommand, BankAccountEvent};
use ask_cqrs::store::event_store::{EventStore, EventProcessingConfig};
use ask_cqrs::store::postgres_event_store::PostgresEventStore;
use ask_cqrs::event_handler::{EventHandler, EventHandlerError, EventRow};

// A handler that fails specific events according to our test sequence
#[derive(Clone)]
struct SequencedFailureHandler {
    // Track which events have been seen and how many times
    event_attempts: Arc<Mutex<HashMap<String, i32>>>,
    // Track which events have been processed successfully
    processed_events: Arc<Mutex<Vec<String>>>,
}

impl SequencedFailureHandler {
    pub fn new() -> Self {
        Self {
            event_attempts: Arc::new(Mutex::new(HashMap::new())),
            processed_events: Arc::new(Mutex::new(Vec::new())),
        }
    }
    
    pub fn get_processed_events(&self) -> Vec<String> {
        self.processed_events.lock().unwrap().clone()
    }
    
    pub fn get_attempt_counts(&self) -> HashMap<String, i32> {
        self.event_attempts.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl EventHandler for SequencedFailureHandler {
    type Events = BankAccountEvent;

    fn name() -> &'static str {
        "sequenced_failure_handler"
    }

    async fn handle_event(&self, event: Self::Events, event_row: EventRow) -> Result<(), EventHandlerError> {
        error!("Handling event: {:?}", event);
        println!("Handling event: {:?}", event);
        // Track attempt count for this event
        let mut attempts = self.event_attempts.lock().unwrap();
        let attempt_count = *attempts.entry(event_row.id.clone()).or_insert(0);
        attempts.insert(event_row.id.clone(), attempt_count + 1);
      
        // Get the event position in the stream to determine behavior
        let position = event_row.stream_position;
        
        info!(
            "Processing event {} (stream position {}): attempt {}", 
            event_row.id, position, attempt_count + 1
        );
        
        match position {
            1 => {
                // First event (position 0) - Always succeeds
                info!("First event succeeded: {:?}", event);
                let mut processed = self.processed_events.lock().unwrap();
                processed.push(event_row.id.clone());
                Ok(())
            },
            2 => {
                // Second event (position 1) - Fails on first attempt, succeeds on retry
                if attempt_count == 0 {
                    // First attempt - fail
                    error!("Second event failing on first attempt");
                    Err(EventHandlerError {
                        log_message: "Simulated failure for second event - first attempt".to_string(),
                    })
                } else {
                    // Retry - succeed
                    info!("Second event succeeded on retry");
                    let mut processed = self.processed_events.lock().unwrap();
                    processed.push(event_row.id.clone());
                    Ok(())
                }
            },
            3 => {
                // Third event (position 2) - Always fails, should go to dead letter after max retries
                error!("Third event failing, attempt {}", attempt_count + 1);
                Err(EventHandlerError {
                    log_message: format!("Simulated failure for third event - attempt {}", attempt_count + 1),
                })
            },
            _ => {
                // Fourth and later events - Always succeed
                info!("Event at position {} succeeded: {:?}", position, event);
                let mut processed = self.processed_events.lock().unwrap();
                processed.push(event_row.id.clone());
                Ok(())
            }
        }
    }
}

// Helper function to check dead-lettered status from the database
async fn check_dead_lettered_events(store: &PostgresEventStore, stream_id: &str, handler_name: &str) -> Result<Vec<(String, i64)>> {
    let pool = store.get_pool();
    let rows = sqlx::query(
        "SELECT stream_id, dead_lettered_position 
         FROM handler_stream_offsets 
         WHERE handler = $1 AND stream_id = $2 AND dead_lettered = true"
    )
    .bind(handler_name)
    .bind(stream_id)
    .fetch_all(pool)
    .await?;
    
    let mut results = Vec::new();
    for row in rows {
        results.push((
            row.get::<String, _>("stream_id"),
            row.get::<i64, _>("dead_lettered_position")
        ));
    }
    
    Ok(results)
}

#[tokio::test]
#[instrument]
#[serial]
async fn test_retry_handling_simple_sequence() -> Result<()> {
    initialize_logger();
    
    // Create store with test configuration
    let store = create_test_store().await?;
    
    // Set max retries to 1 for quicker testing
    let event_config = EventProcessingConfig {
        max_retries: 1,  // After one retry, events go to dead letter
        base_retry_delay: Duration::from_millis(200),
        max_retry_delay: Duration::from_millis(500),
        poll_interval: Duration::from_millis(500),
        start_from_beginning: true,
        ..Default::default()
    };
    
    // Create and start our handler
    let handler = SequencedFailureHandler::new();
    store.start_event_handler(handler.clone(), Some(event_config.clone())).await?;
    
    // Create a unique ID for our test stream
    let account_id = Uuid::new_v4().to_string();
    let user_id = Uuid::new_v4().to_string();
    
    // Step 1: Create the first event (should succeed)
    info!("Creating first event (should succeed)");
    let res1 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::OpenAccount { 
            user_id: user_id.clone(),
            account_id: Some(account_id.clone()),
        },
        (),
        json!({"user_id": user_id}),
    ).await?;
    
    // Give the handler a moment to process the first event
    sleep(Duration::from_millis(500)).await;
    
    // Assert first event was processed successfully
    let processed_events = handler.get_processed_events();
    let events = store.get_events_for_stream(BankAccountAggregate::name(), &account_id, -1, 10).await?;
    let first_event_id = events.iter()
        .find(|e| e.stream_position == 1)
        .map(|e| e.id.clone())
        .expect("Couldn't find first event");
    
    assert!(processed_events.contains(&first_event_id), 
            "First event should have been processed successfully");
    info!("✅ First event processed successfully");
    
    // Step 2: Create the second event (should fail first, then succeed on retry)
    info!("Creating second event (should fail first, then succeed on retry)");
    let res2 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 1000,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    ).await?;
    
    // Wait for retry and processing of the second event
    sleep(Duration::from_secs(1)).await;
    
    // Assert second event failed first, then succeeded on retry
    let processed_events = handler.get_processed_events();
    let attempt_counts = handler.get_attempt_counts();
    let events = store.get_events_for_stream(BankAccountAggregate::name(), &account_id, -1, 10).await?;
    let second_event_id = events.iter()
        .find(|e| e.stream_position == 2)
        .map(|e| e.id.clone())
        .expect("Couldn't find second event");
    
    println!("Processed events: {:?}", processed_events);
    println!("Attempt counts: {:?}", attempt_counts);
    println!("Second event id: {:?}", second_event_id);
    assert!(processed_events.contains(&second_event_id), 
            "Second event should have been processed successfully after retry");
    assert!(attempt_counts.get(&second_event_id).map_or(false, |&count| count > 1),
            "Second event should have been attempted more than once");
    info!("✅ Second event failed first, then succeeded on retry");
    
    // Step 3: Create the third event (should fail and go to dead letter)
    info!("Creating third event (should fail and go to dead letter)");
    let res3 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::DepositFunds { 
            amount: 500,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    ).await?;
    
    // Step 4: Create the fourth event (should not be processed since it's after a dead-lettered event)
    info!("Creating fourth event (should not be processed)");
    let res4 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::WithdrawFunds { 
            amount: 200,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    ).await?;
    
    // Wait for the third event to be retried and marked as dead-lettered
    sleep(Duration::from_secs(1)).await;
    
    // Assert third event failed and is marked as dead-lettered
    let dead_lettered = check_dead_lettered_events(&store, &account_id, SequencedFailureHandler::name()).await?;
    
    assert!(!dead_lettered.is_empty(), 
            "Expected third event to be marked as dead-lettered");
    assert_eq!(dead_lettered[0].1, 3, 
            "Expected third event (position 3) to be marked as dead-lettered");
    info!("✅ Third event failed and was marked as dead-lettered");
    
    // Assert fourth event was not processed (since it's after the dead-lettered event)
    let processed_events = handler.get_processed_events();
    let events = store.get_events_for_stream(BankAccountAggregate::name(), &account_id, -1, 10).await?;
    let fourth_event_id = events.iter()
        .find(|e| e.stream_position == 4)
        .map(|e| e.id.clone())
        .expect("Couldn't find fourth event");
    
    println!("Processed events: {:?}", processed_events);
    println!("Fourth event id: {:?}", fourth_event_id); 
    assert!(!processed_events.contains(&fourth_event_id), 
            "Fourth event should NOT have been processed since it's after a dead-lettered event");
    info!("✅ Fourth event not processed (as expected)");
    
    // Verification complete
    info!("✅ All verifications passed");
    
    Ok(())
} 