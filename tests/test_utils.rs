use sqlx::postgres::PgPool;
use anyhow::Result;
use tracing_subscriber;

pub async fn create_test_store() -> Result<ask_cqrs::store::postgres_event_store::PostgresEventStore> {
    // First connect to the database
    let pool = PgPool::connect("postgres://postgres:postgres@localhost:5432/ask_cqrs_test2").await?;
    
    // Clear event processing claims
    sqlx::query("DELETE FROM event_processing_claims")
        .execute(&pool)
        .await?;

    // Wait longer to ensure all handlers are stopped and connections are closed
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    // Terminate all existing connections to ensure clean slate
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'ask_cqrs_test2' AND pid <> pg_backend_pid()")
        .execute(&pool)
        .await?;

    // Wait for connections to be terminated
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Truncate all tables including dead_letter_events
    sqlx::query(
        "TRUNCATE TABLE events, view_snapshots, event_processing_claims, dead_letter_events RESTART IDENTITY CASCADE"
    )
    .execute(&pool)
    .await?;

    // Create and return the store with a fresh connection - using the same database name
    let store = ask_cqrs::store::postgres_event_store::PostgresEventStore::new("postgres://postgres:postgres@localhost:5432/ask_cqrs_test2").await?;
    
    // Wait longer to ensure store is fully initialized
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    Ok(store)
}

pub fn initialize_logger() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,ask_cqrs::view_event_handler=debug,ask_cqrs::store::postgres_view_store=debug")
        .try_init();
} 