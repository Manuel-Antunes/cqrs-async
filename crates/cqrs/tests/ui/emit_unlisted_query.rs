use cqrs::{
    event::EventEnvelope,
    processing::{EventHandler, HandlerError, ProcessingContext},
    query::QueryUpdateEmitter,
};

#[derive(Clone, cqrs::DomainEvent)]
enum PostEvent {
    Created,
}

#[cqrs::query(output = u64, error = cqrs::store::StoreError)]
#[derive(Clone)]
struct Count;

#[cqrs::query(output = u64, error = cqrs::store::StoreError)]
#[derive(Clone)]
struct Max;

#[cqrs::query(output = u64, error = cqrs::store::StoreError)]
#[derive(Clone)]
struct Total;

#[cqrs::projection(name = "post", events = PostEvent, namespaces("counts"), updates(Count, Max))]
struct PostProjection;

#[cqrs::event_handler(namespace = "counts")]
impl EventHandler for PostProjection {
    async fn handle(&self, _: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        Ok(())
    }

    async fn updated(&self, _: &EventEnvelope<PostEvent>, updates: &QueryUpdateEmitter<Self>) {
        updates.emit(|_: &Total| true, 1);
    }
}

fn main() {}
