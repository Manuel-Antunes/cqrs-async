// The macro refuses the impl, so the imports it would use are not used.
#![allow(unused_imports)]
use cqrs::{event::EventEnvelope, processing::{EventHandler, HandlerError, ProcessingContext}};

#[derive(Clone, cqrs::DomainEvent)]
enum PostEvent {
    Created,
}

struct Counts;

#[cqrs::event_handler(batch = 8)]
impl EventHandler for Counts {
    async fn handle(&self, _: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        Ok(())
    }
}

fn main() {}
