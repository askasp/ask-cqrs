use anyhow::Result;
use ask_cqrs::postgres_store::PostgresStore;
use once_cell::sync::OnceCell;
use tokio_postgres::NoTls;
use deadpool_postgres::{Config, Pool, Runtime};
use tracing_subscriber;

static DB_POOL: OnceCell<Pool> = OnceCell::new();

pub fn initialize_logger() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive("ask_cqrs=debug".parse().unwrap())
                    .add_directive("warn".parse().unwrap()),
            )
            .init();
    });
}

fn get_pool() -> &'static Pool {
    DB_POOL.get_or_init(|| {
        let mut cfg = Config::new();
        cfg.host = Some("localhost".to_string());
        cfg.port = Some(5432);
        cfg.user = Some("postgres".to_string());
        cfg.password = Some("postgres".to_string());
        cfg.dbname = Some("ask_cqrs_test".to_string());
        
        cfg.create_pool(Some(Runtime::Tokio1), NoTls)
            .expect("Failed to create pool")
    })
}

pub async fn create_test_store() -> Result<PostgresStore> {
    // Just create a store using the shared pool's connection string
    PostgresStore::new("postgres://postgres:postgres@localhost:5432/ask_cqrs_test").await
} 