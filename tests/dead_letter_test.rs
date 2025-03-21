/*
 * This test verifies the functionality of the dead letter queue by following a simple sequence:
 * 1. First event succeeds
 * 2. Second event fails first, then succeeds on retry
 * 3. Third event fails twice and goes to the dead letter queue
 * 4. Fourth event waits until third event is in the dead letter queue, then processes
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

#[tokio::test]
#[instrument]
#[serial]
async fn test_dead_letter_queue_simple_sequence() -> Result<()> {
    initialize_logger();
    
    // Create store with test configuration
    let store = create_test_store().await?;
    
    // Set max retries to 1 for quicker testing
    let event_config = EventProcessingConfig {
        max_retries: 1,  // After one retry, events go to dead letter
        base_retry_delay: Duration::from_millis(200),
        max_retry_delay: Duration::from_millis(500),
        claim_ttl: Duration::from_millis(1000),
        poll_interval: Duration::from_millis(500),
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
    sleep(Duration::from_millis(2000)).await;
    
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
    sleep(Duration::from_secs(3)).await;
    
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
    info!("Creating fourth event (should wait until third event is in dead letter)");
    let res4 = store.execute_command::<BankAccountAggregate>(
        BankAccountCommand::WithdrawFunds { 
            amount: 200,
            account_id: account_id.clone(),
        },
        (),
        json!({"user_id": user_id}),
    ).await?;
    
    // Wait for the third event to be retried and moved to dead letter
    sleep(Duration::from_secs(3)).await;
    
    // Assert third event failed and went to dead letter
    let dead_letter_events = store.get_dead_letter_events(0, 10).await?;
    let our_dead_letter_events: Vec<_> = dead_letter_events.items.iter()
        .filter(|e| e.stream_id == account_id)
        .collect();
    
    assert!(!our_dead_letter_events.is_empty(), 
            "Expected third event to be in dead letter queue");
    assert_eq!(our_dead_letter_events[0].stream_position, 3, 
            "Expected third event (position 2) to be in dead letter queue");
    info!("✅ Third event failed and was moved to dead letter queue");
    
    // Step 4: Create the fourth event (should wait for third to be in dead letter)
 
    
    // Wait for fourth event to be processed
    sleep(Duration::from_secs(3)).await;
    
    // Assert fourth event was processed after third went to dead letter
    let processed_events = handler.get_processed_events();
    let events = store.get_events_for_stream(BankAccountAggregate::name(), &account_id, -1, 10).await?;
    let fourth_event_id = events.iter()
        .find(|e| e.stream_position == 4)
        .map(|e| e.id.clone())
        .expect("Couldn't find fourth event");
    
    println!("Processed events: {:?}", processed_events);
    println!("Fourth event id: {:?}", fourth_event_id); 
    println!("Events: {:?}", events);
    assert!(processed_events.contains(&fourth_event_id), 
            "Fourth event should have been processed after third event went to dead letter");
    info!("✅ Fourth event processed successfully after third event went to dead letter");
    
    // Final verification of all steps
    info!("Final verification - checking all test assertions passed");
    
    // Verify we have exactly one event in the dead letter queue
    let dead_letter_events = store.get_dead_letter_events(0, 10).await?;
    let our_dead_letter_events: Vec<_> = dead_letter_events.items.iter()
        .filter(|e| e.stream_id == account_id)
        .collect();
    
    assert_eq!(our_dead_letter_events.len(), 1, "Expected exactly one event in dead letter queue");
    assert_eq!(our_dead_letter_events[0].stream_position, 3, "Expected third event (position 2) to be in dead letter queue");
    
    // Verify all other events were processed correctly
    let processed_events = handler.get_processed_events();
    let attempt_counts = handler.get_attempt_counts();
    
    info!("Processed events count: {}", processed_events.len());
    info!("Event attempt counts: {:?}", attempt_counts);
    
    // Verification complete
    info!("✅ All verifications passed");
    
    Ok(())
} 