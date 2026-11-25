#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum Ticked {
    Once,
}

#[cqrs::aggregate(event = Ticked, snapshot())]
struct Clock {
    id: u32,
}

impl Clock {
    fn apply(&mut self, _: &Ticked) {}
}

fn main() {}
