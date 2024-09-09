use axum::http::HeaderMap;
use dashmap::DashMap;
use eventstore::{Client, RecordedEvent, SubscribeToAllOptions, SubscriptionFilter};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::instrument;

pub struct MetaData {
    headers: Option<HeaderMap>,
    authenticated_user_sub: Option<String>,
}

// #[async_trait::async_trait]
// pub trait View {
//     type Event: DeserializeOwned + Send + Sync + 'static;
//     type State: Send + Sync + Clone + 'static;

//     fn apply_event(
//         state: Self::State,
//         event: &Self::Event,
//         raw_event: &RecordedEvent,
//     ) -> Self::State;

//     // fn get_id(&self) -> &str;
//     fn get_indices(state: Self::State) -> HashMap<String, String>;
//     fn create_from_event(view_id: &str, event: &Self::Event) -> Result<Self::State, anyhow::Error>;
// }

// Can i just jamp the indices righ into the hashmap? Still, everytime i update the view i need to update all all the linked indices...
// its more clean to have it in a
// indices cant change run time, so it might be just as easy to just concat the indice-name_index-value and use that as the key.
// but then i need to have a copy of the value, but that is fine, thought it might trouble the size of the readmodel.
// but each index might have a lot of values, so it might be better to have a hashmap for each index, like i already have
// but does i need to use the index name as well? It might be easier to just concat here..

// i can also have a lookup_index that

// pub struct ReadModelWrapper<T: View + 'static> {
//     views: DashMap<String, T::State>,
//     // indices: DashMap<String, DashMap<String, Vec<String>>>,
//     views_by_index: DashMap<String, Vec<String>>,
//     db_client: Arc<Client>,
// }

// impl<T: View> ReadModelWrapper<T> {
//     pub fn init(db_client: Arc<Client>) -> Arc<Self> {
//         let rm = Arc::new(ReadModelWrapper {
//             views: DashMap::new(),
//             views_by_index: DashMap::new(),
//             db_client,
//         });
//         let rm_clone = Arc::clone(&rm);
//         tokio::spawn(async move {
//             rm_clone.start_dispatcher().await;
//         });
//         rm
//     }

//     #[instrument(skip_all)]
//     pub async fn start_dispatcher(&self) {
//         let filter = SubscriptionFilter::on_stream_name().add_prefix("ask_cqrs");
//         let options = SubscribeToAllOptions::default().filter(filter);
//         let mut stream = self.db_client.clone().subscribe_to_all(&options).await;

//         while let Ok(event) = stream.next().await {
//             let orig_event = event.get_original_event();
//             let stream_id = orig_event.stream_id.clone().split("-").collect::<Vec<_>>()[2..]
//                 .join("-")
//                 .to_string();
//             // let stream_id = orig_event.stream_id.clone().replace("ask_cqrs-", "").l

//             let parsed_event = orig_event.as_json::<T::Event>();
//             match parsed_event {
//                 Ok(f) => {
//                     let res = self.update_view(&stream_id, f, orig_event);
//                     tracing::debug!("Updated view: {:?}", res);
//                 }
//                 Err(e) => tracing::error!("Failed to parse event: {:?}", e),
//             }
//         }
//     }

//     pub fn get_views(&self) -> &DashMap<String, T::State> {
//         &self.views
//     }
//     pub fn get_view(&self, id: &str) -> Option<T::State> {
//         self.views.get(id).map(|v| v.clone())
//     }
//     pub fn get_view_by_index(&self, index_name: &str, index_value: &str) -> Vec<T::State> {
//         let key = format!("{}-{}", index_name, index_value);
//         self.views_by_index
//             .get(&key)
//             .and_then(|v| Some(v.iter().map(|id| self.get_view(id).unwrap()).collect()))
//             .unwrap_or(Vec::new())
//     }
//     pub fn update_view(
//         &self,
//         view_id: &str,
//         event: T::Event,
//         orig_event: &RecordedEvent,
//     ) -> Result<(), anyhow::Error> {
//         tracing::debug!("Updating view: {}", view_id);

//         let old_indices_data = self.get_indices_ids(view_id);
//         let is_new = self.update_view_state(view_id, event, orig_event)?;
//         let new_indices_data = self.get_indices_ids(view_id);

//         let (removed_indices, added_indices) =
//             self.calculate_index_diff(old_indices_data, new_indices_data);

//         self.update_indices(view_id, removed_indices, added_indices);

//         Ok(())
//     }
//     pub fn get_indices_ids(&self, view_id: &str) -> Vec<String> {
//         self.views
//             .get(view_id)
//             .and_then(|v| Some(T::get_indices(v.clone())))
//             .unwrap_or(HashMap::new())
//             .into_iter()
//             .map(|(key, value)| format!("{}-{}", key, value))
//             .collect()
//     }

//     pub fn calculate_index_diff(
//         &self,
//         old_indices: Vec<String>,
//         new_indices: Vec<String>,
//     ) -> (Vec<String>, Vec<String>) {
//         // Calculate removed indices: items in old_indices but not in new_indices
//         let removed_indices: Vec<String> = old_indices
//             .iter()
//             .filter(|item| !new_indices.contains(item))
//             .cloned()
//             .collect();

//         // Calculate added indices: items in new_indices but not in old_indices
//         let added_indices: Vec<String> = new_indices
//             .iter()
//             .filter(|item| !old_indices.contains(item))
//             .cloned()
//             .collect();

//         (removed_indices, added_indices)
//     }
//     pub fn update_view_state(
//         &self,
//         view_id: &str,
//         event: T::Event,
//         orig_event: &RecordedEvent,
//     ) -> Result<(), anyhow::Error> {
//         let new_view = match self.views.get(view_id) {
//             Some(view) => T::apply_event(view.clone(), &event, orig_event),
//             None => {
//                 tracing::debug!("Creating new view: {}", view_id);
//                 T::create_from_event(view_id, &event)?
//             }
//         };
//         self.views.insert(view_id.to_string(), new_view);
//         Ok(())
//     }
//     #[instrument(skip_all)]
//     pub fn update_indices(
//         &self,
//         view_id: &str,
//         removed_indices: Vec<String>,
//         added_indices: Vec<String>,
//     ) {
//         tracing::debug!("Updating indices for view: {}", view_id);

//         let view_id_str = view_id.to_string();
//         for index in removed_indices {
//             self.views_by_index.entry(index).and_modify(|ids| {
//                 ids.retain(|id| id != &view_id_str);
//             });
//         }
//         for index in added_indices {
//             self.views_by_index
//                 .entry(index)
//                 .and_modify(|ids| {
//                     if !ids.contains(&view_id_str) {
//                         ids.push(view_id_str.clone());
//                     }
//                 })
//                 .or_insert_with(|| vec![view_id_str.clone()]);
//         }
//     }
// }

// #[derive(Debug, Default)]
// pub struct ReadModel<V> {
//     views: DashMap<String, Arc<V>>,
//     indices: DashMap<String, DashMap<String, Vec<String>>>,
// }

// impl<V> ReadModel<V>
// where
//     V: View,
// {
//     pub fn new() -> Self {
//         Self {
//             views: DashMap::new(),
//             indices: DashMap::new(),
//         }
//     }

//     pub fn get_views(&self) -> &DashMap<String, Arc<V>> {
//         &self.views
//     }

//     pub fn get_view(&self, id: &str) -> Option<Arc<V>> {
//         self.views.get(id).map(|v| Arc::clone(&v))
//     }

//     // Dont need to say the index name here.
//     // then we can just have a get call
//     pub fn get_view_by_index(&self, index_name: &str, index_value: &str) -> Vec<Arc<V>> {
//         if let Some(index_map) = self.indices.get(index_name) {
//             if let Some(account_ids) = index_map.get(index_value) {
//                 return account_ids
//                     .iter()
//                     .filter_map(|id| self.get_view(id))
//                     .collect();
//             }
//         }
//         Vec::new()
//     }
//     pub fn update_view_state(
//         &self,
//         view_id: &str,
//         event: V::Event,
//         orig_event: &RecordedEvent,
//     ) -> Result<bool, anyhow::Error> {
//         let mut is_new = false;
//         if let Some(mut view) = self.views.get_mut(view_id) {
//             tracing::debug!("Found existing view: {}", view_id);
//             let mut view_mut = Arc::make_mut(&mut view);
//             view_mut.apply_event(&event, orig_event);
//         } else {
//             let new_view = Arc::new(V::create_from_event(view_id, &event)?);
//             tracing::debug!("Creating new view: {}", view_id);
//             self.views.insert(view_id.to_string(), new_view);
//             // then just insert it here on with the index as the id.

//             is_new = true;
//         }
//         Ok(is_new)
//     }
//     pub fn update_view(
//         &self,
//         view_id: &str,
//         event: V::Event,
//         orig_event: &RecordedEvent,
//     ) -> Result<(), anyhow::Error> {
//         tracing::debug!("Updating view: {}", view_id);

//         let old_indices_data = self.get_old_indices(view_id);
//         let is_new = self.update_view_state(view_id, event, orig_event)?;
//         let new_indices_data = self.get_new_indices(view_id);

//         let (removed_indices, added_indices) =
//             self.calculate_index_diff(&old_indices_data, &new_indices_data);

//         self.update_indices(view_id, removed_indices, added_indices, is_new);

//         Ok(())
//     }

//     pub fn get_old_indices(&self, view_id: &str) -> HashMap<String, String> {
//         self.views
//             .get(view_id)
//             .map(|view| view.get_indices())
//             .unwrap_or_default()
//     }

//     pub fn get_new_indices(&self, view_id: &str) -> HashMap<String, String> {
//         self.views
//             .get(view_id)
//             .map(|view| view.get_indices())
//             .unwrap_or_default()
//     }

//     pub fn calculate_index_diff(
//         &self,
//         old_indices: &HashMap<String, String>,
//         new_indices: &HashMap<String, String>,
//     ) -> (Vec<(String, String)>, Vec<(String, String)>) {
//         let removed_indices: Vec<(String, String)> = old_indices
//             .iter()
//             .filter(|(k, v)| new_indices.get(*k) != Some(v))
//             .map(|(k, v)| (k.clone(), v.clone()))
//             .collect();
//         let added_indices: Vec<(String, String)> = new_indices
//             .iter()
//             .filter(|(k, v)| !old_indices.contains_key(*k))
//             .map(|(k, v)| (k.clone(), v.clone()))
//             .collect();

//         (removed_indices, added_indices)
//     }

//     #[instrument(skip_all)]
//     pub fn update_indices(
//         &self,
//         view_id: &str,
//         removed_indices: Vec<(String, String)>,
//         added_indices: Vec<(String, String)>,
//     ) {
//         tracing::debug!("Updating indices for view: {}", view_id);

//         let view_id_str = view_id.to_string();

//         // Remove from old indices
//         for (index_name, index_value) in removed_indices {
//             if let Some(index_map) = self.indices.get(&index_name) {
//                 if let Some(mut ids) = index_map.get_mut(&index_value) {
//                     ids.retain(|id| id != &view_id_str);
//                 }
//             }
//         }

//         // Add to new indices
//         for (index_name, index_value) in added_indices {
//             let index_map = self.indices.entry(index_name).or_insert_with(DashMap::new);
//             let mut ids = index_map.entry(index_value).or_insert_with(Vec::new);
//             if !ids.contains(&view_id_str) {
//                 ids.push(view_id_str.clone());
//             }
//         }
//     }

//     #[instrument(skip_all)]
//     pub async fn start_dispatcher(self: Arc<Self>, client: Arc<Client>)
//     where
//         V: View,
//     {
//         let filter = SubscriptionFilter::on_stream_name().add_prefix("ask_cqrs");
//         let options = SubscribeToAllOptions::default().filter(filter);

//         let mut stream = client.subscribe_to_all(&options).await;

//         while let Ok(event) = stream.next().await {
//             let orig_event = event.get_original_event();
//             let stream_id = orig_event.stream_id.clone().split("-").collect::<Vec<_>>()[2..]
//                 .join("-")
//                 .to_string();
//             // let stream_id = orig_event.stream_id.clone().replace("ask_cqrs-", "").l

//             let parsed_event = orig_event.as_json::<V::Event>();
//             match parsed_event {
//                 Ok(f) => {
//                     let res = self.update_view(&stream_id, f, orig_event);
//                     tracing::debug!("Updated view: {:?}", res);
//                 }
//                 Err(e) => tracing::error!("Failed to parse event: {:?}", e),
//             }
//         }
//     }
// }
