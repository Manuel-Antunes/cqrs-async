//! Domain tests: no store, no async. Given past events, when a decision
//! runs, then expect the events it appended or the error.

use std::{marker::PhantomData, sync::Arc};

use blog::domain::{Post, PostError, PostEvent, PostId};
use cqrs::{
    Aggregate, AggregateLoader, EventPublisher, InMemoryEventStore, InlinePublisher, Loaded, Pending,
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

#[test]
fn framework_free_decisions_work_with_the_repository() {
    struct Clicks;
    let store = Arc::new(InMemoryEventStore::new());
    let loader = AggregateLoader::<Counter<Clicks>, _>::new(Arc::clone(&store));
    let publisher = InlinePublisher::new(store);
    // Load (or start), decide with a plain `Vec`, commit.
    let click = || async {
        let counter = match loader.load(&1).await.unwrap() {
            Some(counter) => counter,
            None => Loaded::new(Counter::initial(&1)),
        };
        let mut events = Vec::new();
        counter.increment(2, &mut events)?;
        publisher.commit(counter, Pending::new(events)).await.unwrap();
        Ok::<_, String>(())
    };
    block_on(async {
        click().await.unwrap();
        click().await.unwrap();
        assert_eq!(click().await.unwrap_err(), "limit 2 reached");
        assert_eq!(loader.load(&1).await.unwrap().unwrap().value, 2);
    });
}

#[test]
fn event_types_come_from_the_derive() {
    use cqrs::DomainEvent;
    assert_eq!(created().event_type(), "post.created");
    assert_eq!(CounterEvent::Incremented.event_type(), "Incremented");
}
