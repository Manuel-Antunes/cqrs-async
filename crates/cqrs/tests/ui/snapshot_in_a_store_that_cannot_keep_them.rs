use cqrs::processing::Events;

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum Ticked {
    Once,
}

#[cqrs::aggregate(event = Ticked, snapshot(after_events = 2))]
#[derive(Clone)]
struct Clock {
    id: u32,
}

impl Clock {
    fn apply(&mut self, _: &Ticked) {}
}

/// An event store that knows nothing about snapshots.
#[derive(Clone)]
struct Elsewhere;

fn main() {
    let events = Events::<Ticked, _>::new(Elsewhere);
    let _ = events.repository::<Clock>();
}
