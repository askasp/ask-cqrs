
#[typetag::serde(tag = "type")]
pub trait DomainCommand {
    fn stream_id(&self) -> String;
}