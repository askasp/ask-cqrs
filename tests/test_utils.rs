use sqlx::postgres::PgPool;
use anyhow::Result;
use tracing_subscriber;

pub async fn create_test_store() -> Result<ask_cqrs::postgres_store::PostgresStore> {
      Ok(ask_cqrs::postgres_store::PostgresStore::new("postgres://postgres:postgres@localhost:5432/ask_cqrs_test").await?)
}

pub fn initialize_logger() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();
} 