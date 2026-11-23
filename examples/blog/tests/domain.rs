//! Domain tests: no store, no async. Given past events, when a decision
//! runs, then expect the events it appended or the error.

use std::marker::PhantomData;

use blog::domain::{Post, PostError, PostEvent, PostId};
use cqrs::{
    Aggregate, EventSourced, InMemoryEventStore,
    testing::{AggregateFixture, block_on},
};

const ID: PostId = PostId(7);

fn created() -> PostEvent {
    PostEvent::Created {
        id: ID,
        title: "Hello".into(),
    }
}

#[test]
fn creating_a_post_appends_created() {
    AggregateFixture::<Post>::new(ID)
        .when(|post, events| post.create("  Hello ", events))
        .expect_events([created()])
        .expect_state(|post| assert_eq!(post.title(), "Hello"));
}

#[test]
fn a_post_needs_a_title() {
    AggregateFixture::<Post>::new(ID)
        .when(|post, events| post.create("   ", events))
        .expect_error(PostError::EmptyTitle)
        .expect_no_events();
}

#[test]
fn publishing_appends_published() {
    AggregateFixture::<Post>::new(ID)
        .given([created()])
        .when(|post, events| post.publish(events))
        .expect_events([PostEvent::Published])
        .expect_state(|post| assert!(post.is_published()));
}

#[test]
fn a_post_is_published_once() {
    AggregateFixture::<Post>::new(ID)
        .given([created(), PostEvent::Published])
        .when(|post, events| post.publish(events))
        .expect_error(PostError::AlreadyPublished)
        .expect_no_events();
}

#[test]
fn a_published_post_is_frozen() {
    AggregateFixture::<Post>::new(ID)
        .given([created(), PostEvent::Published])
        .when(|post, events| post.retitle("Other", events))
        .expect_error(PostError::PublishedPostsAreFrozen);
}

#[test]
fn a_decision_only_appends_the_state_changes_when_applied() {
    let mut post = Post::initial(&ID);
    post.evolve(&created());

    let mut events = Vec::new(); // a `Vec` is an appender
    post.publish(&mut events).unwrap();
    assert_eq!(events, [PostEvent::Published]);
    assert!(!post.is_published(), "deciding changes nothing by itself");

    post.evolve(&events[0]);
    assert!(post.is_published());
}

#[test]
fn the_aggregate_is_plain_state() {
    let mut post = Post::initial(&ID);
    post.evolve(&created());
    let json = serde_json::to_string(&post).unwrap();
    assert_eq!(json, r#"{"id":7,"title":"Hello","published":false}"#);
    assert_eq!(serde_json::from_str::<Post>(&json).unwrap(), post);
}

// A generic aggregate whose decisions do not depend on the framework: they
// take `impl Extend` from std. The repository's `Vec` fits it as is.
#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent, cqrs::TagResolver)]
enum CounterEvent {
    Incremented,
}

struct Counter<Unit>
where
    Unit: Send + Sync + 'static,
{
    id: u32,
    value: u64,
    unit: PhantomData<fn() -> Unit>,
}

impl<Unit: Send + Sync + 'static> Counter<Unit> {
    /// No `cqrs` type in the signature.
    fn increment(&self, limit: u64, events: &mut impl Extend<CounterEvent>) -> Result<(), String> {
        if self.value >= limit {
            return Err(format!("limit {limit} reached"));
        }
        events.extend([CounterEvent::Incremented]);
        Ok(())
    }
}

impl<Unit: Send + Sync + 'static> Aggregate for Counter<Unit> {
    const TYPE: &'static str = "counter";
    type Id = u32;
    type Event = CounterEvent;

    fn initial(id: &u32) -> Self {
        Self {
            id: *id,
            value: 0,
            unit: PhantomData,
        }
    }

    fn id(&self) -> &u32 {
        &self.id
    }

    fn evolve(&mut self, CounterEvent::Incremented: &CounterEvent) {
        self.value += 1;
    }
}

#[derive(Debug)]
struct Error(String);

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self(message)
    }
}

impl From<cqrs::StoreError> for Error {
    fn from(error: cqrs::StoreError) -> Self {
        Self(error.to_string())
    }
}

impl From<cqrs::NotFound<u32>> for Error {
    fn from(error: cqrs::NotFound<u32>) -> Self {
        Self(error.to_string())
    }
}

#[test]
fn framework_free_decisions_work_with_the_repository() {
    struct Clicks;
    let counters = EventSourced::<Counter<Clicks>, _>::new(InMemoryEventStore::new());
    block_on(async {
        let created: Result<(), Error> = counters.create(&1, |c, events| c.increment(2, events)).await;
        created.unwrap();
        let updated: Result<(), Error> = counters.update(&1, |c, events| c.increment(2, events)).await;
        updated.unwrap();
        let refused: Result<(), Error> = counters.update(&1, |c, events| c.increment(2, events)).await;
        assert_eq!(refused.unwrap_err().0, "limit 2 reached");
        assert_eq!(counters.load(&1).await.unwrap().unwrap().value, 2);
    });
}

#[test]
fn event_types_come_from_the_derive() {
    use cqrs::DomainEvent;
    assert_eq!(created().event_type(), "post.created");
    assert_eq!(CounterEvent::Incremented.event_type(), "Incremented");
}
