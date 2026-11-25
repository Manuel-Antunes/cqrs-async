use cqrs::{event::EventEnvelope, processing::{EventHandler, HandlerError, ProcessingContext}};

#[derive(Clone, cqrs::DomainEvent)]
enum PostEvent {
    Created,
}

#[cqrs::projection(name = "post", events = PostEvent, namespaces("counts"))]
struct PostProjection;

#[cqrs::event_handler(namespace = "notificatons")]
impl EventHandler for PostProjection {
    async fn handle(&self, _: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        Ok(())
    }
}

fn main() {}
