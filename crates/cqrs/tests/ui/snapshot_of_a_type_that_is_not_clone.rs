use cqrs::{processing::Events, store::InMemoryEventStore};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum Ticked {
    Once,
}

// Declares snapshots, but cannot be cloned: the in-memory store cannot keep them.
#[cqrs::aggregate(event = Ticked, snapshot(after_events = 2))]
struct Clock {
    id: u32,
}

impl Clock {
    fn apply(&mut self, _: &Ticked) {}
}

fn main() {
    let events = Events::<Ticked, _>::new(InMemoryEventStore::new());
    let _ = events.repository::<Clock>();
}
