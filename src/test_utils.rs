use anyhow::{Context, Result};
use sqlx::postgres::{PgPool, PgConnectOptions};
use sqlx::ConnectOptions;
use url::Url;
use std::str::FromStr;
use tracing_subscriber;

use crate::store::PostgresEventStore;

/// Creates a clean event store using the provided connection string
pub async fn create_test_store(connection_string: &str) -> Result<PostgresEventStore> {
    // First connect to the database
    let pool = PgPool::connect(connection_string).await?;
    
    // Clear event processing claims
    sqlx::query("DELETE FROM handler_stream_offsets")
        .execute(&pool)
        .await?;

    // Wait longer to ensure all handlers are stopped and connections are closed
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    // Extract database name from connection string for targeted connection termination
    let db_name = extract_db_name_from_connection_string(connection_string)?;
    
    // Terminate all existing connections to ensure clean slate
    sqlx::query(&format!("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}' AND pid <> pg_backend_pid()", db_name))
        .execute(&pool)
        .await?;

    // Wait for connections to be terminated
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Truncate all tables
    truncate_tables(&pool).await?;

    // Create and return the store with a fresh connection
    let store = PostgresEventStore::new(connection_string).await?;
    
    // Wait longer to ensure store is fully initialized
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    Ok(store)
}

/// Truncates all tables in the database used for testing
pub async fn truncate_tables(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "TRUNCATE TABLE events, view_snapshots, handler_stream_offsets"
    )
    .execute(pool)
    .await?;
    
    Ok(())
}

// Embed the schema.sql file as a string constant at compile time
const EMBEDDED_SCHEMA: &str = include_str!("../src/schema.sql");

/// Creates a test database with the given name and initializes it with the embedded schema
pub async fn create_test_database(admin_connection_string: &str, test_db_name: &str) -> Result<String> {
    // Connect to postgres to create the database
    let options = PgConnectOptions::from_str(admin_connection_string)?
        .disable_statement_logging();
    
    let pool = PgPool::connect_with(options).await?;
    
    // First try to drop the database if it exists
    sqlx::query(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", test_db_name))
        .execute(&pool)
        .await?;
    
    // Create the database
    sqlx::query(&format!("CREATE DATABASE {}", test_db_name))
        .execute(&pool)
        .await?;
    
    // Build the connection string for the new database
    let url = Url::parse(admin_connection_string)?;
    let mut components = url.to_string();
    
    // Replace the last part of the connection string with our test database name
    if let Some(pos) = components.rfind('/') {
        components.truncate(pos + 1);
        components.push_str(test_db_name);
    } else {
        components.push_str("/");
        components.push_str(test_db_name);
    }
    
    // Now connect to the new database and initialize schema
    let db_pool = PgPool::connect(&components).await?;
    
    // Execute the embedded schema SQL to initialize the database
    sqlx::query(EMBEDDED_SCHEMA)
        .execute(&db_pool)
        .await
        .context("Failed to initialize database schema")?;
    
    Ok(components)
}

/// Creates a test database with a custom schema file instead of the embedded one
pub async fn create_test_database_with_schema(admin_connection_string: &str, test_db_name: &str, schema_path: &str) -> Result<String> {
    // Connect to postgres to create the database
    let options = PgConnectOptions::from_str(admin_connection_string)?
        .disable_statement_logging();
    
    let pool = PgPool::connect_with(options).await?;
    
    // First try to drop the database if it exists
    sqlx::query(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", test_db_name))
        .execute(&pool)
        .await?;
    
    // Create the database
    sqlx::query(&format!("CREATE DATABASE {}", test_db_name))
        .execute(&pool)
        .await?;
    
    // Build the connection string for the new database
    let url = Url::parse(admin_connection_string)?;
    let mut components = url.to_string();
    
    // Replace the last part of the connection string with our test database name
    if let Some(pos) = components.rfind('/') {
        components.truncate(pos + 1);
        components.push_str(test_db_name);
    } else {
        components.push_str("/");
        components.push_str(test_db_name);
    }
    
    // Now connect to the new database and initialize schema
    let db_pool = PgPool::connect(&components).await?;
    
    // Read schema.sql file
    let schema_sql = std::fs::read_to_string(schema_path)
        .context(format!("Failed to read schema file at {}", schema_path))?;
    
    // Execute schema SQL to initialize the database
    sqlx::query(&schema_sql)
        .execute(&db_pool)
        .await
        .context("Failed to initialize database schema")?;
    
    Ok(components)
}

/// Drops a test database
pub async fn drop_test_database(admin_connection_string: &str, test_db_name: &str) -> Result<()> {
    let options = PgConnectOptions::from_str(admin_connection_string)?
        .disable_statement_logging();
    
    let pool = PgPool::connect_with(options).await?;
    
    // Terminate all connections to the database
    sqlx::query(&format!("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}' AND pid <> pg_backend_pid()", test_db_name))
        .execute(&pool)
        .await?;
    
    // Wait for connections to terminate
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    // Drop the database
    sqlx::query(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", test_db_name))
        .execute(&pool)
        .await?;
    
    Ok(())
}

/// Extracts the database name from a Postgres connection string
fn extract_db_name_from_connection_string(connection_string: &str) -> Result<String> {
    let url = Url::parse(connection_string)
        .context("Failed to parse connection string as URL")?;
    
    let path = url.path();
    if path.is_empty() || path == "/" {
        return Err(anyhow::anyhow!("No database name found in connection string"));
    }
    
    // Remove leading slash if present
    let db_name = path.trim_start_matches('/');
    Ok(db_name.to_string())
}

pub fn initialize_logger() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,ask_cqrs::view_event_handler=debug,ask_cqrs::store::postgres_view_store=debug")
        .try_init();
} 