use std::sync::Arc;
use dashmap::DashMap;
use serde::Serialize;

use aggregate::Aggregate;
use event_handler::EventHandler;
use eventstore::{
    Client, PersistentSubscriptionToAllOptions, SubscribeToAllOptions, SubscriptionFilter,
};
use tracing::{event as tracing_event, instrument};
use uuid::serde::braced::serialize;
use view::{View, ViewStore};

pub mod aggregate;
pub mod command;
pub mod event_handler;
pub mod view;

pub use ask_cqrs_macros::commandhandler;

use command::DomainCommand;


#[instrument(skip(client))]
async fn build_state<A: Aggregate>(
    client: Arc<Client>,
    stream_id: &str,
) -> Result<(Option<A::State>, Option<u64>), anyhow::Error> {
    let stream_name = format!("{}-{}-{}", "ask_cqrs", A::name(), stream_id);
    let stream = client.read_stream(stream_name, &Default::default()).await;

    let mut revision = None;
    let state = match stream {
        Err(eventstore::Error::ResourceNotFound) => {
            tracing::debug!("Stream not found, initializing empty state");
            None
        }
        Ok(mut stream) => {
            let mut state = None;
            while let Ok(Some(event)) = stream.next().await {
                let parsed_event = event.get_original_event().as_json::<A::Event>()?;
                A::apply_event(&mut state, &parsed_event);
                revision = Some(event.get_original_event().revision);
            }
            state
        }
        Err(e) => return Err(e.into()),
    };
    tracing::info!("Got state {:?}", state);
    tracing::warn!("Got revision {:?}", revision);

    Ok((state, revision))
}

#[instrument(skip(client,command,service,views))]
pub async fn execute_command<A: Aggregate>(
    client: Arc<Client>,
    command: A::Command,
    service: A::Service,
    views: Option<Vec<Arc<ViewStore<impl View<Event = A::Event>>>>>,
) -> Result<String, anyhow::Error> {
    let stream_id = command.stream_id();
    tracing::info!("Executing command {:?}", stream_id);
    let (state, revision) = build_state::<A>(client.clone(), &stream_id).await?;
    let events = A::execute(&state, &command, &stream_id, service)?;

    let stream_name = format!("{}-{}-{}", "ask_cqrs", A::name(), stream_id);
    for event in events {
        let serialized_event = serde_json::to_value(&event)?;
        let event_type = serialized_event.get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Event missing 'type' field"))?;

        let event_data = eventstore::EventData::json(event_type, &event)?;
        let options = eventstore::AppendToStreamOptions::default()
            .expected_revision(match revision {
                None => eventstore::ExpectedRevision::NoStream,
                Some(x) => eventstore::ExpectedRevision::Exact(x),
            });

        let write_result = client
            .append_to_stream(stream_name.clone(), &options, event_data)
            .await?;
        // If views are provided, update them immediately
        if let Some(ref views) = views {
            // Read back the event we just wrote to get its ID
            let position = if write_result.next_expected_version > 0 {
                eventstore::StreamPosition::Position(write_result.next_expected_version - 1)
            } else {
                eventstore::StreamPosition::Start
            };
            
            let read_options = eventstore::ReadStreamOptions::default()
                .position(position);
                
            let mut stream = client.read_stream(stream_name.clone(), &read_options).await?;
            if let Ok(Some(recorded_event)) = stream.next().await {
                for view_store in views {
                    view_store.handle_event(&event, &stream_id, recorded_event.get_original_event());
                }
            }
        }
    }

    Ok(stream_id)
}

pub async fn start_view_builder<V: View>(
    client: Arc<Client>,
) -> Result<Arc<ViewStore<V>>, anyhow::Error> {
    let view_store = Arc::new(ViewStore::new());
    let store_clone = view_store.clone();
    
    tracing::info!("Starting view builder for {}", V::name());

    tokio::spawn(async move {
        let filter = SubscriptionFilter::on_stream_name().add_prefix("ask_cqrs");
        let options = SubscribeToAllOptions::default().filter(filter);
        let mut stream = client.subscribe_to_all(&options).await;

        while let Ok(event) = stream.next().await {
            tracing::info!("Got event");
            let orig_event = event.get_original_event();
            if let Ok(parsed_event) = orig_event.as_json::<V::Event>() {
                tracing::info!("Parsed event");
                if let Some(entity_id) = V::entity_id_from_event(&parsed_event) {
                    tracing::info!("Entity ID: {:?}", entity_id);
                    store_clone.handle_event(&parsed_event, &entity_id, &orig_event);
                }
            }
            else {
                tracing::error!("Failed to parse event: {:?}", orig_event);
            }
        }
    });

    Ok(view_store)
}

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub async fn start_event_handler<H: EventHandler>(
    handler: H,
    client: Arc<Client>,
    service: H::Service,
) -> Result<(), anyhow::Error> {
    tokio::spawn(async move {
        create_persistent_subscription_to_all(&client, H::name()).await?;
        let mut sub = client
            .subscribe_to_persistent_subscription_to_all(H::name(), &Default::default())
            .await?;

        while let Ok(event) = sub.next().await {
            let orig_event = event.get_original_event();
            
            if let Ok(parsed_event) = orig_event.as_json::<H::Events>() {
                match handler.handle_event(parsed_event, orig_event, service.clone()).await {
                    Ok(_) => {
                        sub.ack(event).await?;
                    }
                    Err(e) => {
                        sub.nack(event, e.nack_action, e.log_message).await?;
                    }
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    });

    Ok(())
}

async fn create_persistent_subscription_to_all(client: &Client, name: &str) -> eventstore::Result<()> {
    // #region create-persistent-subscription-to-all
    let options = PersistentSubscriptionToAllOptions::default()
        .filter(SubscriptionFilter::on_stream_name().add_prefix("ask_cqrs"));

    return match client
        .create_persistent_subscription_to_all(name, &options)
        .await
    {
        Err(eventstore::Error::ResourceAlreadyExists) => eventstore::Result::Ok(()),
        Err(e) => Err(e),
        _x => eventstore::Result::Ok(()),
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
