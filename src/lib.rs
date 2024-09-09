#![feature(associated_type_defaults)]
use std::sync::Arc;

use aggregate::Aggregate;
use darkbird::{Options, Storage, StorageType};
use event_handler::EventHandler;
use eventstore::{
    Client, PersistentSubscriptionToAllOptions, SubscribeToAllOptions, SubscriptionFilter,
};
use tracing::{event as tracing_event, instrument};
use uuid::serde::braced::serialize;
use view::View;

pub mod aggregate;
pub mod command;
pub mod event;
pub mod event_handler;
pub mod read_model;
pub mod view;
use command::DomainCommand;
use event::DomainEvent;

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

pub async fn execute_command<A: Aggregate>(
    client: Arc<Client>,
    command: A::Command,
    stream_id: &str,
    service: A::Service,
) -> Result<(), anyhow::Error> {
    tracing::info!("Executing command {:?}", stream_id);
    let (state, revision) = build_state::<A>(client.clone(), &stream_id).await?;
    tracing::info!("done ulding state for command {:?}", stream_id);
    let events = A::execute(&state, &command, stream_id, service)?;

    let count = events.len();
    tracing::info!("done reating events{:?} count is {:?} ", stream_id, count);

    let stream_name = format!("{}-{}-{}", "ask_cqrs", A::name(), stream_id);
    for event in events {
        tracing::info!("in loop ");
        let dyn_event = &event as &dyn DomainEvent;

        let serialized_event = serde_json::to_value(&event)?;

        let event_type = serialized_event.get("type").unwrap().as_str().unwrap();

        let event_data = eventstore::EventData::json(event_type, dyn_event).unwrap();
        let options =
            eventstore::AppendToStreamOptions::default().expected_revision(match revision {
                None => eventstore::ExpectedRevision::NoStream,
                Some(x) => eventstore::ExpectedRevision::Exact(x),
            });

        tracing::info!("appending to stream {:?}", stream_name);
        client
            .clone()
            .append_to_stream(stream_name.clone(), &options, event_data)
            .await?;

        tracing::info!("appenidng to the loop done ");
    }

    tracing::info!("Done Executing command {:?}", command.stream_id());

    Ok(())
}

pub async fn start_view_builder<V: View>(
    client: Arc<Client>,
) -> Result<Arc<Storage<String, V::State>>, anyhow::Error> {
    let path = ".";

    let storage_name = V::name();
    let total_page_size = 1000;

    let ops = Options::new(
        path,
        storage_name,
        total_page_size,
        StorageType::RamCopies,
        true,
    );
    let s = Arc::new(Storage::<String, V::State>::open(ops).await.unwrap());
    let s1 = s.clone();
    tracing::info!("Starting view builder for {}", V::name());

    tokio::spawn(async move {
        let filter = SubscriptionFilter::on_stream_name().add_prefix("ask_cqrs");
        let options = SubscribeToAllOptions::default().filter(filter);
        let mut stream = client.clone().subscribe_to_all(&options).await;

        tracing::info!("in tokio task for {}", V::name());

        while let Ok(event) = stream.next().await {
            let orig_event = event.get_original_event();
            tracing::info!("Got event in view {:?}", orig_event.event_type);
            let stream_id = orig_event.stream_id.clone().replace("ask_cqrs-", "");

            let parsed_event = orig_event.as_json::<Box<dyn DomainEvent>>();
            match parsed_event {
                Ok(f) => {
                    tracing::info!("parsing event");
                    let view_id = V::view_id_from_event(&f);

                    match view_id {
                        None => {
                            tracing::info!("No view id found, not processing event");
                            continue;
                        }
                        Some(view_id) => {
                            tracing::info!("Got view id {:?}", view_id);

                            let old_state = s1.lookup(&view_id);
                            let new_state = match old_state {
                                Some(state) => {
                                    let mut new_state = state.clone();
                                    V::apply_event(&mut new_state, &f, &stream_id, orig_event);
                                    new_state
                                }
                                None => {
                                    let new_state =
                                        V::create_from_first_event(&f, &stream_id, orig_event);
                                    new_state
                                }
                            };
                            // Step 2: Insert the new state back into the storage
                            if let Err(e) = s1.insert(view_id, new_state).await {
                                tracing::error!("Failed to insert new state: {:?}", e);
                            }

                            tracing::info!("Done parsing and inserting event");
                        }
                    }
                }
                Err(e) => tracing::error!("Failed to parse event: {:?}", e),
            }
            tracing::info!("waiting for next event in tokio loop");
        }
    });

    tracing::info!("Returning from {}", V::name());
    Ok(s)
}

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub async fn start_event_handler<E: EventHandler>(
    client: Arc<Client>,
    service: E::Service,
) -> Result<(), anyhow::Error> {
    tokio::spawn(async move {
        tracing_event!(
            tracing::Level::INFO,
            "Starting event handler for {}",
            E::name()
        );
        create_persistent_subscription_to_all(&client, E::name())
            .await
            .unwrap();

        let mut sub = client
            .subscribe_to_persistent_subscription_to_all(E::name(), &Default::default())
            .await
            .unwrap();
        tracing_event!(
            tracing::Level::INFO,
            "Subscribed to persistent subscription for {}",
            E::name()
        );

        loop {
            let event = sub.next().await.unwrap();
            let orig_event = event.get_original_event();
            // let parsed_event = orig_event.as_json::<Box<dyn DomainEvent>>().unwrap();
            let parsed_event = orig_event.as_json::<Box<dyn DomainEvent>>();
            match parsed_event {
                Ok(f) => {
                    tracing::info!("parsing event");
                match E::handle_event(&f, orig_event, service.clone()).await {
                Ok(_) => {
                    tracing_event!(
                        tracing::Level::INFO,
                        "Handled event {}",
                        orig_event.event_type
                    );

                    sub.ack(event).await.unwrap();
                }

                Err(e) => {
                    tracing_event!(
                        tracing::Level::ERROR,
                        "Failed to handle event: got error {:?}",
                        e.log_message
                    );
                    sub.nack(event, e.nack_action, e.log_message).await.unwrap();
                }
            };
        }, 
        Err(e) => tracing::error!("Failed to parse event: {:?}", e),
    }
}
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
