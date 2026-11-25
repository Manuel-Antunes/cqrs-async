#[derive(Clone, cqrs::DomainEvent)]
enum PostEvent {
    Published,
}

#[derive(Clone, cqrs::DomainEvent)]
#[event(fan_in)]
enum FeedEvent {
    Post(PostEvent),
    Shown { at: u64 },
}

fn main() {}
