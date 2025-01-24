use utoipa::ToSchema;

#[typetag::serde(tag = "type")]
pub trait DomainCommand: Send + Sync {
    fn stream_id(&self) -> String;
}
