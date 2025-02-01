use std::sync::Arc;
use anyhow::Result;
use deadpool_postgres::{Config, Pool, Runtime, ManagerConfig};
use tokio_postgres::{NoTls, types::ToSql};
use uuid::Uuid;
use tracing::instrument;
use tokio::sync::{mpsc, broadcast};
use tokio::time::interval;
use std::time::Duration;
use serde_json::{self, Value as JsonValue};
use std::collections::HashMap;

use crate::{
    aggregate::Aggregate,
    view::{View,  StateView, IndexView},
    command::DomainCommand,
    event_handler::{EventHandler, EventHandlerError},
};

/// PostgreSQL-based event store implementation
#[derive(Clone)]
pub struct PostgresStore {
    pool: Pool,
    shutdown: broadcast::Sender<()>,
}

impl PostgresStore {
    /// Create a new PostgreSQL store with the given connection string
    pub async fn new(connection_string: &str) -> Result<Self> {
        // Parse the connection string using tokio-postgres
        let pg_config = connection_string.parse::<tokio_postgres::Config>()
            .map_err(|e| anyhow::anyhow!("Failed to parse connection string: {}", e))?;
        
        // Create deadpool config from tokio-postgres config
        let mut cfg = Config::new();
        if let Some(host) = pg_config.get_hosts().first() {
            match host {
                tokio_postgres::config::Host::Tcp(h) => cfg.host = Some(h.to_string()),
                _ => return Err(anyhow::anyhow!("Only TCP hosts are supported")),
            }
        }
        cfg.port = pg_config.get_ports().first().copied();
        cfg.user = pg_config.get_user().map(|u| u.to_string());
        if let Some(pass) = pg_config.get_password() {
            cfg.password = Some(String::from_utf8_lossy(pass).into_owned());
        }
        cfg.dbname = pg_config.get_dbname().map(|d| d.to_string());
        cfg.manager = Some(ManagerConfig { 
            recycling_method: deadpool_postgres::RecyclingMethod::Fast,
        });

        // Create a connection pool
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| anyhow::anyhow!("Failed to create pool: {}", e))?;

        let (shutdown, _) = broadcast::channel(1);

        Ok(Self { pool, shutdown })
    }

    /// Shutdown all background tasks
    pub async fn shutdown(&self) {
        let _ = self.shutdown.send(());
    }

    /// Get a client from the pool with retry logic
    async fn get_client(&self) -> Result<deadpool_postgres::Client> {
        let mut attempts = 0;
        let max_attempts = 3;
        let retry_delay = Duration::from_millis(100);
        
        loop {
            match self.pool.get().await {
                Ok(client) => {
                    // Test the connection with a simple query
                    if let Err(_) = client.simple_query("").await {
                        // Connection is broken, drop it and retry
                        attempts += 1;
                        if attempts >= max_attempts {
                            return Err(anyhow::anyhow!("Failed to get valid client after {} attempts", max_attempts));
                        }
                        tokio::time::sleep(retry_delay).await;
                        continue;
                    }
                    return Ok(client);
                },
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_attempts {
                        return Err(anyhow::anyhow!("Failed to get client after {} attempts: {}", max_attempts, e));
                    }
                    tokio::time::sleep(retry_delay).await;
                }
            }
        }
    }

    /// Build the current state for an aggregate by replaying all events
    #[instrument(skip(self))]
    pub async fn build_state<A: Aggregate>(
        &self,
        stream_id: &str,
    ) -> Result<(Option<A::State>, Option<i64>), anyhow::Error> {
        let stream_name = format!("{}-{}-{}", "ask_cqrs", A::name(), stream_id);
        
        // Get all events for this stream
        let client = self.get_client().await?;
        let rows = client
            .query(
                "SELECT event_data, stream_position FROM events 
                 WHERE stream_name = $1 
                 ORDER BY stream_position",
                &[&stream_name],
            )
            .await?;

        let mut state = None;
        let mut last_position = None;

        for row in rows {
            let event_data: JsonValue = row.get("event_data");
            if let Ok(event) = serde_json::from_value::<A::Event>(event_data) {
                A::apply_event(&mut state, &event);
                last_position = Some(row.get::<_, i64>("stream_position"));
            }
        }

        tracing::info!("Got state {:?}", state);
        tracing::warn!("Got position {:?}", last_position);

        Ok((state, last_position))
    }

    /// Execute a command and store the resulting events
    #[instrument(skip(self, command, service))]
    pub async fn execute_command<A: Aggregate>(
        &self,
        command: A::Command,
        service: A::Service,
    ) -> Result<String, anyhow::Error> 
    where
        A::Command: DomainCommand,
    {
        let stream_id = command.stream_id();
        tracing::info!("Executing command {:?}", stream_id);
        
        let (state, last_position) = self.build_state::<A>(&stream_id).await?;
        let events = A::execute(&state, &command, &stream_id, service)?;

        let stream_name = format!("{}-{}-{}", "ask_cqrs", A::name(), stream_id);
        let next_position = last_position.unwrap_or(-1) + 1;

        // Get a client and start a transaction
        let mut client = self.get_client().await?;
        let tx = client.transaction().await?;

        for (idx, event) in events.into_iter().enumerate() {
            let event_json = serde_json::to_value(&event)?;
            let stream_position = next_position + idx as i64;
            let id = Uuid::new_v4().to_string();

            tx.execute(
                "INSERT INTO events (id, stream_name, stream_id, event_data, stream_position) 
                 VALUES ($1, $2, $3, $4::jsonb, $5)",
                &[
                    &id,
                    &stream_name,
                    &stream_id,
                    &event_json,
                    &stream_position,
                ],
            ).await?;
        }

        tx.commit().await?;
        Ok(stream_id)
    }


    /// Start an event handler that processes events
    pub async fn start_event_handler<H: EventHandler>(
        &self,
        handler: H,
    ) -> Result<(), anyhow::Error> {
        tracing::info!("Starting event handler for {}", H::name());

        let client = self.get_client().await?;
        
        // Get or create subscription record
        let subscription_id = Uuid::new_v4();
        client.execute(
            "INSERT INTO persistent_subscriptions (id, name, consumer_group, last_processed_position)
             VALUES ($1, $2, 'default', 0)
             ON CONFLICT (name, consumer_group) DO NOTHING",
            &[&subscription_id, &H::name()],
        ).await?;

        // Get last processed position
        let row = client
            .query_one(
                "SELECT last_processed_position FROM persistent_subscriptions 
                 WHERE name = $1 AND consumer_group = 'default'",
                &[&H::name()],
            )
            .await?;
        
        let mut last_position: i64 = row.get("last_processed_position");

        // Process events from last position
        loop {
            let client = self.get_client().await?;
            let rows = client
                .query(
                    "SELECT event_data, global_position FROM events 
                     WHERE global_position > $1 
                     ORDER BY global_position 
                     LIMIT 100",
                    &[&last_position],
                )
                .await?;

            if rows.is_empty() {
                break;
            }

            for row in rows {
                let event_data: JsonValue = row.get("event_data");
                let global_position: i64 = row.get("global_position");
                if let Ok(event) = serde_json::from_value::<H::Events>(event_data) {
                    handler.handle_event(event, &row).await
                        .map_err(|e| anyhow::anyhow!("Failed to handle event: {}", e.log_message))?;
                    
                    // Update subscription position after successful processing
                    client.execute(
                        "UPDATE persistent_subscriptions 
                         SET last_processed_position = $1 
                         WHERE name = $2 AND consumer_group = 'default'",
                        &[&global_position, &H::name()],
                    ).await?;
                }
                last_position = global_position;
            }
        }

        // Start background task to process new events
        let pool = self.pool.clone();
        let handler = Arc::new(handler);
        let mut shutdown = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Ok(client) = pool.get().await {
                            // Fetch and process new events
                            let rows = match client
                                .query(
                                    "SELECT event_data, global_position FROM events 
                                     WHERE global_position > $1 
                                     ORDER BY global_position 
                                     LIMIT 100",
                                    &[&last_position],
                                )
                                .await
                            {
                                Ok(rows) => rows,
                                Err(e) => {
                                    eprintln!("Error querying events: {}", e);
                                    continue;
                                }
                            };

                            for row in rows {
                                let event_data: JsonValue = row.get("event_data");
                                let global_position: i64 = row.get("global_position");
                                if let Ok(event) = serde_json::from_value::<H::Events>(event_data) {
                                    match handler.handle_event(event, &row).await {
                                        Ok(_) => {
                                            // Update subscription position after successful processing
                                            if let Err(e) = client.execute(
                                                "UPDATE persistent_subscriptions 
                                                 SET last_processed_position = $1 
                                                 WHERE name = $2 AND consumer_group = 'default'",
                                                &[&global_position, &H::name()],
                                            ).await {
                                                eprintln!("Error updating subscription position: {}", e);
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!("Error handling event: {}", e.log_message);
                                        }
                                    }
                                }
                                last_position = global_position;
                            }
                        }
                    }
                    _ = shutdown.recv() => break,
                }
            }
        });

        Ok(())
    }

    /// Get a view's state from snapshot or rebuild from events
    pub async fn get_view_state<V: StateView>(&self, stream_id: &str) -> Result<Option<V>> {
        let client = self.get_client().await?;
        
        // Try to get from snapshot first
        if V::snapshot_frequency().is_some() {
            if let Some(row) = client
                .query_opt(
                    "SELECT state, last_event_position FROM view_snapshots 
                     WHERE view_name = $1 AND stream_id = $2",
                    &[&V::name(), &stream_id],
                )
                .await?
            {
                let mut view: V = serde_json::from_value(row.get("state"))?;
                let last_position: i64 = row.get("last_event_position");
                
                // Get any events after the snapshot
                let rows = client
                    .query(
                        "SELECT event_data FROM events 
                         WHERE stream_id = $1 
                         AND global_position > $2
                         ORDER BY global_position",
                        &[&stream_id, &last_position],
                    )
                    .await?;

                for row in rows {
                    let event: V::Event = serde_json::from_value(row.get("event_data"))?;
                    view.apply_event(&event);
                }
                
                return Ok(Some(view));
            }
        }
        
        // No snapshot or snapshot disabled - rebuild from events
        let mut view = None;
        let rows = client
            .query(
                "SELECT event_data FROM events 
                 WHERE stream_id = $1 
                 ORDER BY global_position",
                &[&stream_id],
            )
            .await?;

        for row in rows {
            let event: V::Event = serde_json::from_value(row.get("event_data"))?;
            match view {
                None => {
                    view = V::initialize(&event);
                }
                Some(ref mut v) => {
                    v.apply_event(&event);
                }
            }
        }

        Ok(view)
    }

    /// Save a snapshot of the current state
    pub async fn save_view_snapshot<V: StateView>(&self, stream_id: &str, view: &V) -> Result<()> {
        if V::snapshot_frequency().is_none() {
            return Ok(());
        }

        let client = self.get_client().await?;
        let state_json = serde_json::to_value(view)?;
        
        // Get the latest event position
        let row = client
            .query_one(
                "SELECT MAX(global_position) as pos FROM events WHERE stream_id = $1",
                &[&stream_id],
            )
            .await?;
        
        let last_position: i64 = row.get("pos");
        let id = Uuid::new_v4().to_string();

        client
            .execute(
                "INSERT INTO view_snapshots (id, view_name, stream_id, state, last_event_position)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (view_name, stream_id) 
                 DO UPDATE SET state = $4, last_event_position = $5",
                &[&id, &V::name(), &stream_id, &state_json, &last_position],
            )
            .await?;

        Ok(())
    }

    /// Get an index view's state from snapshot or rebuild from events
    pub async fn get_index_state<V: IndexView + Default>(&self) -> Result<V::Index> {
        let client = self.get_client().await?;
        
        // Try to get from snapshot first
        if V::snapshot_frequency().is_some() {
            if let Some(row) = client
                .query_opt(
                    "SELECT state, last_event_position FROM view_snapshots 
                     WHERE view_name = $1 AND stream_id = 'index'",
                    &[&V::name()],
                )
                .await?
            {
                let state: V::Index = serde_json::from_value(row.get("state"))?;
                let last_position: i64 = row.get("last_event_position");
                
                // Get any events after the snapshot
                let rows = client
                    .query(
                        "SELECT event_data FROM events 
                         WHERE global_position > $1
                         ORDER BY global_position",
                        &[&last_position],
                    )
                    .await?;

                let mut current_state = state;
                let view = V::default();
                for row in rows {
                    let event: V::Event = serde_json::from_value(row.get("event_data"))?;
                    view.update_index(&mut current_state, &event);
                }
                
                return Ok(current_state);
            }
        }
        
        // No snapshot or snapshot disabled - rebuild from events
        let mut state = V::initialize_index();
        let rows = client
            .query(
                "SELECT event_data FROM events ORDER BY global_position",
                &[],
            )
            .await?;

        let view = V::default();
        for row in rows {
            let event: V::Event = serde_json::from_value(row.get("event_data"))?;
            view.update_index(&mut state, &event);
        }

        Ok(state)
    }

    /// Save a snapshot of the current index state
    pub async fn save_index_snapshot<V: IndexView>(&self, state: &V::Index) -> Result<()> {
        if V::snapshot_frequency().is_none() {
            return Ok(());
        }

        let client = self.get_client().await?;
        let state_json = serde_json::to_value(state)?;
        
        // Get the latest event position
        let row = client
            .query_one(
                "SELECT MAX(global_position) as pos FROM events",
                &[],
            )
            .await?;
        
        let last_position: i64 = row.get("pos");
        let id = Uuid::new_v4().to_string();

        client
            .execute(
                "INSERT INTO view_snapshots (id, view_name, stream_id, state, last_event_position)
                 VALUES ($1, $2, 'index', $3, $4)
                 ON CONFLICT (view_name, stream_id) 
                 DO UPDATE SET state = $3, last_event_position = $4",
                &[&id, &V::name(), &state_json, &last_position],
            )
            .await?;

        Ok(())
    }

    /// Get multiple views' states in a single query
    pub async fn get_view_states<V: StateView>(&self, stream_ids: &[String]) -> Result<HashMap<String, V>> {
        if stream_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let client = self.get_client().await?;
        let mut views = HashMap::new();
        
        // Try to get from snapshots first
        if V::snapshot_frequency().is_some() {
            let snapshots = client
                .query(
                    "SELECT stream_id, state, last_event_position FROM view_snapshots 
                     WHERE view_name = $1 AND stream_id = ANY($2)",
                    &[&V::name(), &stream_ids],
                )
                .await?;

            // Initialize views from snapshots
            for row in snapshots {
                let stream_id: String = row.get("stream_id");
                let view: V = serde_json::from_value(row.get("state"))?;
                views.insert(stream_id, view);
            }

            // Get all events after the snapshots
            if !views.is_empty() {
                let rows = client
                    .query(
                        "SELECT stream_id, event_data FROM events 
                         WHERE stream_id = ANY($1) 
                         AND global_position > (
                             SELECT COALESCE(MAX(last_event_position), -1)
                             FROM view_snapshots 
                             WHERE view_name = $2 
                             AND stream_id = ANY($1)
                         )
                         ORDER BY global_position",
                        &[&stream_ids, &V::name()],
                    )
                    .await?;

                for row in rows {
                    let stream_id: String = row.get("stream_id");
                    let event: V::Event = serde_json::from_value(row.get("event_data"))?;
                    if let Some(view) = views.get_mut(&stream_id) {
                        view.apply_event(&event);
                    }
                }
            }
        }

        // For any stream_ids not found in snapshots, rebuild from events
        let missing_ids: Vec<_> = stream_ids.iter()
            .filter(|id| !views.contains_key(*id))
            .collect();

        if !missing_ids.is_empty() {
            let rows = client
                .query(
                    "SELECT stream_id, event_data FROM events 
                     WHERE stream_id = ANY($1) 
                     ORDER BY global_position",
                    &[&missing_ids],
                )
                .await?;

            // Group events by stream_id
            for row in rows {
                let stream_id: String = row.get("stream_id");
                let event: V::Event = serde_json::from_value(row.get("event_data"))?;
                
                match views.get_mut(&stream_id) {
                    Some(view) => {
                        view.apply_event(&event);
                    }
                    None => {
                        if let Some(view) = V::initialize(&event) {
                            views.insert(stream_id, view);
                        }
                    }
                }
            }
        }

        Ok(views)
    }
} 