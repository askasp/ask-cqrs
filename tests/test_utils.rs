use std::sync::Arc;
use eventstore::Client;

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

pub fn create_es_client() -> Arc<Client> {
    let settings = "esdb://127.0.0.1:2113?tls=false&keepAliveTimeout=10000&keepAliveInterval=10000"
        .parse()
        .unwrap();
    Arc::new(Client::new(settings).unwrap())
} 