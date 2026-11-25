use cqrs::{event::EventEnvelope, processing::{EventHandler, HandlerError, ProcessingContext}};

#[derive(Clone, cqrs::DomainEvent)]
enum PostEvent {
    Created,
}

struct Counts;

#[cqrs::event_handler]
impl EventHandler for Counts {
    async fn handle(&self, _: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        Ok(())
    }

    async fn updated(&self, _: &EventEnvelope<PostEvent>, _: &()) {}
}

fn main() {}
