use sqlx::postgres::PgPool;
use anyhow::Result;
use tracing_subscriber;

pub async fn create_test_store() -> Result<ask_cqrs::postgres_store::PostgresStore> {
    // First connect to the database
    let pool = PgPool::connect("postgres://postgres:postgres@localhost:5432/ask_cqrs_test").await?;
    
    // Stop all view builders and event handlers by removing their subscriptions
    sqlx::query("DELETE FROM persistent_subscriptions")
        .execute(&pool)
        .await?;

    // Wait longer to ensure all handlers are stopped and connections are closed
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    // Terminate all existing connections to ensure clean slate
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'ask_cqrs_test' AND pid <> pg_backend_pid()")
        .execute(&pool)
        .await?;

    // Wait for connections to be terminated
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Truncate all tables
    sqlx::query(
        "TRUNCATE TABLE events, view_snapshots, persistent_subscriptions RESTART IDENTITY CASCADE"
    )
    .execute(&pool)
    .await?;

    // Create and return the store with a fresh connection
    let store = ask_cqrs::postgres_store::PostgresStore::new("postgres://postgres:postgres@localhost:5432/ask_cqrs_test").await?;
    
    // Wait longer to ensure store is fully initialized
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    Ok(store)
}

pub fn initialize_logger() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();
} 