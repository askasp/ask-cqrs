use core::fmt;
use std::error::Error;
use std::sync::Arc;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{ser::StdError, Serialize};

use std::fmt::Debug;

use crate::command::DomainCommand;
use crate::event::DomainEvent;


#[async_trait]
pub trait Aggregate {
    type Event: DeserializeOwned + Serialize + DomainEvent; // Ensure Event implements Deserialize
    type Command: DomainCommand;
    type DomainError: StdError + Send + Sync + 'static; // Ensure Error implements std::error::Error and necessary conversions
    type State: Debug;
    
    type Service = ();

    fn apply_event(state: &mut Option<Self::State>, event: &Self::Event);
    fn execute(
        state: &Option<Self::State>,
        command: &Self::Command,
        stream_id: &str,
        service: Self::Service,
    ) -> Result<Vec<Self::Event>, Self::DomainError>;

    fn name() -> &'static str;
    fn stream_revision(&self) -> u64;
}

#[derive(Clone, Debug)]
struct StateWithRevision<T> {
    state: T,
    revision: eventstore::ExpectedRevision,
}
#[derive(Debug, Clone)]
pub enum EsErrorOrDomainError<T> {
    EsError(eventstore::Error),
    DomainError(T),
}
impl<T> fmt::Display for EsErrorOrDomainError<T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EsErrorOrDomainError::EsError(e) => write!(f, "EventStore error: {}", e),
            EsErrorOrDomainError::DomainError(e) => write!(f, "Domain error: {}", e),
        }
    }
}

impl<T> Error for EsErrorOrDomainError<T>
where
    T: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            EsErrorOrDomainError::EsError(e) => Some(e),
            EsErrorOrDomainError::DomainError(e) => Some(e),
        }
    }
}

// pub struct AggregateWrapper<T: Aggregate> {
//     state_map: DashMap<String, StateWithRevision<T::State>>,
//     db_client: Arc<Client>,
// }

// impl<T: Aggregate> AggregateWrapper<T> {
//     pub fn new(db_client: Arc<Client>) -> Self {
//         AggregateWrapper {
//             state_map: DashMap::new(),
//             db_client,
//         }
//     }
//     async fn get_state(&self, id: &str) -> Option<StateWithRevision<T::State>> {
//         self.state_map.get(id).map(|entry| entry.clone())
//     }

//     pub async fn dispatch_event(
//         &self,
//         stream_id: &str,
//         command: &T::Command,
//     ) -> Result<(), EsErrorOrDomainError<T::DomainError>> {
//         let stream_id_full = format!("ask_cqrs-{}-{}", T::name(), stream_id);
//         let mut state_with_rev = self.get_state(&stream_id_full).await;
//         let mut state = state_with_rev.clone().map(|s| s.state);

//         if state.is_none() {
//             self.populate_state(&stream_id_full).await;
//             state_with_rev = self.get_state(&stream_id_full).await;
//             state = state_with_rev.clone().map(|s| s.state);
//         }

//         let event =
//             T::execute(&state, command).map_err(|e| EsErrorOrDomainError::DomainError(e))?;

//         let expected_revision = state_with_rev
//             .clone()
//             .map(|s| s.revision)
//             .unwrap_or(eventstore::ExpectedRevision::NoStream);

//         let res = self
//             .commit_event(&stream_id_full, &event, expected_revision)
//             .await;

//         match res {
//             Ok(new_rev) => {
//                 let new_state_with_rev = StateWithRevision {
//                     state: T::apply_events(state.clone(), &event),
//                     revision: new_rev,
//                 };

//                 self.state_map
//                     .insert(stream_id_full.to_string(), new_state_with_rev);
//             }
//             Err(eventstore::Error::WrongExpectedVersion {
//                 expected: e,
//                 current: c,
//             }) => {
//                 event!(
//                     Level::ERROR,
//                     "Wrong expected version: expected {}, current {}",
//                     e,
//                     c
//                 );
//                 self.populate_state(&stream_id_full).await;
//                 return res
//                     .map(|_| ())
//                     .map_err(|e| EsErrorOrDomainError::EsError(e));
//             }
//             Err(e) => return Err(EsErrorOrDomainError::EsError(e)),
//         }
//         Ok(())
//     }

//     async fn populate_state(&self, stream_id: &str) -> Result<(), anyhow::Error> {
//         let options = ReadStreamOptions::default();
//         let mut stream = self
//             .db_client
//             .clone()
//             .read_stream(stream_id, &options)
//             .await?;
//         let mut state_with_revision = self.get_state(stream_id).await;
//         let mut state = state_with_revision.clone().map(|s| s.state);

//         // let mut updated_state: T::State;

//         let mut position: u64 = 0;
//         while let Some(event) = stream.next().await? {
//             position = event.get_original_event().revision;
//             let parsed_event = event.get_original_event().as_json::<T::Event>()?;
//             state = Some(T::apply_event(state.clone(), &parsed_event));
//         }

//         if let Some(s) = state {
//             let state_with_rev = StateWithRevision {
//                 state: s,
//                 revision: eventstore::ExpectedRevision::Exact(position),
//             };
//             self.state_map.insert(stream_id.to_string(), state_with_rev);
//         }

//         Ok(())
//     }

//     async fn commit_event(
//         &self,
//         stream_id: &str,
//         events: &Vec<T::Event>,
//         expected_revision: eventstore::ExpectedRevision,
//     ) -> Result<(eventstore::ExpectedRevision), eventstore::Error> {
//         let options = AppendToStreamOptions::default().expected_revision(expected_revision);

//         let es_events: Vec<EventData> = events
//             .into_iter()
//             .map(|event| {
//                 EventData::json(event.event_type(), &event)
//                     .unwrap()
//                     .id(Uuid::new_v4())
//             })
//             .collect();

//         let res = self
//             .db_client
//             .clone()
//             .append_to_stream(stream_id, &options, es_events)
//             .await?;

//         return Ok(eventstore::ExpectedRevision::Exact(
//             res.next_expected_version,
//         ));
//     }
// }
