use sqlx::postgres::PgPool;
use anyhow::Result;
use tracing_subscriber;

pub async fn create_test_store() -> Result<ask_cqrs::postgres_store::PostgresStore> {
    // First connect to the database
    let pool = PgPool::connect("postgres://postgres:postgres@localhost:5432/ask_cqrs_test").await?;
    
    // Truncate all tables
    sqlx::query(
        "TRUNCATE TABLE events, view_snapshots, persistent_subscriptions RESTART IDENTITY CASCADE"
    )
    .execute(&pool)
    .await?;

    // Create and return the store
    Ok(ask_cqrs::postgres_store::PostgresStore::new("postgres://postgres:postgres@localhost:5432/ask_cqrs_test").await?)
}

pub fn initialize_logger() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();
} 