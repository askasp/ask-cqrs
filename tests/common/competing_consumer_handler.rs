use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use tokio::time::sleep;
use std::time::Duration;
use uuid::Uuid;
use tracing::{info, error, warn};
use anyhow::Result;
use serde::{Serialize, Deserialize};
use serde_json::Value;

use super::bank_account::{BankAccountEvent, BankAccountCommand, BankAccountAggregate};
use ask_cqrs::{
    event_handler::{EventHandler, EventHandlerError, EventRow},
    store::event_store::EventStore,
    store::postgres_event_store::PostgresEventStore
};

// Track processed events for verification
#[derive(Debug, Clone, Default)]
pub struct ProcessedEvents {
    // Map from stream_id to Vec of (position, event_id, node_id)
    pub events: HashMap<String, Vec<(i64, String, String)>>,
    pub failed_events: Vec<String>,
    pub total_processed: usize,
}

#[derive(Clone)]
pub struct CompetingConsumerHandler {
    store: PostgresEventStore,
    node_id: String,
    processed_events: Arc<Mutex<ProcessedEvents>>,
    fail_on_event_ids: Arc<Mutex<Vec<String>>>,
    processing_delay_ms: u64,
}

impl CompetingConsumerHandler {
    pub fn new(
        store: PostgresEventStore, 
        node_id: String,
        processed_events: Arc<Mutex<ProcessedEvents>>,
        processing_delay_ms: u64,
    ) -> Self {
        Self { 
            store, 
            node_id, 
            processed_events,
            fail_on_event_ids: Arc::new(Mutex::new(Vec::new())),
            processing_delay_ms,
        }
    }
    
    pub fn configure_failures(&self, event_ids: Vec<String>) {
        let mut failures = self.fail_on_event_ids.lock().unwrap();
        failures.clear();
        failures.extend(event_ids);
    }
    
    pub fn get_processed_events(&self) -> ProcessedEvents {
        self.processed_events.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl EventHandler for CompetingConsumerHandler {
    type Events = BankAccountEvent;

    fn name() -> &'static str {
        "competing_consumer_handler"
    }

    async fn handle_event(&self, event: Self::Events, event_row: EventRow) -> Result<(), EventHandlerError> {
        // Add artificial delay to simulate processing time and increase chance of concurrency
        sleep(Duration::from_millis(self.processing_delay_ms)).await;
        
        info!(
            "Node {} processing event: {:?}, stream: {}, position: {}", 
            self.node_id, &event, event_row.stream_id, event_row.stream_position
        );
        
        // Check if this event should fail
        {
            let failures = self.fail_on_event_ids.lock().unwrap();
            if failures.contains(&event_row.id) {
                let err_msg = format!("Simulated failure for event {} by node {}", event_row.id, self.node_id);
                error!("{}", err_msg);
                
                // Track the failure
                let mut processed = self.processed_events.lock().unwrap();
                processed.failed_events.push(event_row.id.clone());
                
                return Err(EventHandlerError {
                    log_message: err_msg,
                });
            }
        }
        
        // Record that we processed this event
        {
            let mut processed = self.processed_events.lock().unwrap();
            processed.total_processed += 1;
            
            let stream_events = processed.events
                .entry(event_row.stream_id.clone())
                .or_insert_with(Vec::new);
                
            stream_events.push((
                event_row.stream_position,
                event_row.id.clone(),
                self.node_id.clone()
            ));
        }

        // Simulate some real processing
        match event {
            BankAccountEvent::FundsWithdrawn { amount, .. } if amount > 1000 => {
                info!("Large withdrawal detected by node {}: {}", self.node_id, amount);
            }
            _ => {}
        }

        Ok(())
    }
} 