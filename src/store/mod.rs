// Define the modules
pub mod event_store;
pub mod view_store;
pub mod postgres_event_store;
pub mod postgres_view_store;
pub mod stream_claim;
pub mod event_processor;

// Re-export the public items
pub use event_store::*;
pub use view_store::*;
pub use postgres_event_store::*;
pub use postgres_view_store::*;
pub use stream_claim::*;