#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum Ticked {
    Once,
}

#[cqrs::aggregate(event = Ticked, snapshot(after_events = 10, after_sourcing = "5h"))]
struct Clock {
    id: u32,
}

impl Clock {
    fn apply(&mut self, _: &Ticked) {}
}

fn main() {}
