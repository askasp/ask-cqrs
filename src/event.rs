use std::any::Any;

#[typetag::serde(tag = "event")]
pub trait DomainEvent: Send + Sync + Any + AToAny{
    fn stream_id(&self) -> String;

    //     *self
    // }
}
pub trait AToAny: 'static {
    fn as_any(&self) -> &dyn Any;
}

impl<T: 'static> AToAny for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

// impl<T: Any> DomainEvent for T {
// fn as_any(&self) -> &Any {
// self
// }
// }
// impl dyn DomainEvent {
//     pub fn as_any(&self) -> Box<dyn Any> {
//         Box::new(self) as Box<dyn Any>
//     }
// }

// Implement Any for all DomainEvents to enable downcasting
