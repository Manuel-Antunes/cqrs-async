#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum Ticked {
    Once,
}

#[cqrs::aggregate(event = Ticked, snapshot(every = 10))]
struct Clock {
    id: u32,
}

impl Clock {
    fn apply(&mut self, _: &Ticked) {}
}

fn main() {}
