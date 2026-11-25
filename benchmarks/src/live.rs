//! The event-processing API the load scenarios use, in one place: when that
//! API changes, only this module does.

use std::sync::Arc;

use blog::{
    application::{
        commands::PostCommands,
        queries::{PostQueries, PostSubscriptions},
    },
    domain::{Post, PostEvent},
};
use cqrs::{
    aggregate::{AggregateLoader, Loaded},
    command::{EventPublisher, Pending},
    event::EventEnvelope,
    processing::{EventHandler, Events, HandlerError, ProcessingContext, ProcessorHandle},
    store::{EventFeed, InMemoryEventStore, StoreError},
};
use futures_util::Stream;

/// The shared store.
pub type Store = InMemoryEventStore<PostEvent>;

/// A store, its events service, and a repository that broadcasts after
/// commit.
pub struct Live {
    /// The store.
    pub store: Store,
    /// Broadcast, subscriptions, processors.
    events: Events<PostEvent, Store>,
    /// Rebuilds posts.
    pub posts: AggregateLoader<Post, Store>,
}

impl Live {
    /// Fresh, empty.
    pub fn new() -> Self {
        let store: Store = InMemoryEventStore::new();
        let events = Events::new(store.clone());
        let posts = AggregateLoader::new(store.clone());
        Self { store, events, posts }
    }

    /// Commits `events` to `post` through the events service: stored, then
    /// broadcast.
    ///
    /// # Errors
    ///
    /// The store's.
    pub async fn commit(
        &self,
        post: impl Into<Loaded<Post>>,
        events: Vec<PostEvent>,
    ) -> Result<Option<u64>, StoreError> {
        self.events.commit(post.into(), Pending::new(events)).await
    }

    /// A catch-up subscription from `after`.
    pub fn subscribe(
        &self,
        after: u64,
    ) -> impl Stream<Item = Result<Arc<EventEnvelope<PostEvent>>, StoreError>> + Send + use<> {
        self.events.subscribe(after)
    }

    /// Spawns a tracking processor feeding `handler`.
    pub fn track<H: EventHandler<PostEvent> + 'static>(&self, name: &str, handler: H) -> ProcessorHandle {
        self.events.track(name, handler)
    }

    /// The events service, e.g. to configure a processor.
    pub fn events(&self) -> &Events<PostEvent, Store> {
        &self.events
    }

    /// Position of the last committed event.
    pub async fn last_position(&self) -> u64 {
        self.events.last_position().await.unwrap()
    }
}

impl Default for Live {
    fn default() -> Self {
        Self::new()
    }
}

/// An event handler that does nothing: the processor's own overhead.
pub struct Noop;

impl EventHandler<PostEvent> for Noop {
    async fn handle(&self, _: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// An event handler that burns CPU for each event, like a projection that
/// serializes or computes: what segments parallelize.
pub struct Busy(pub u32);

impl EventHandler<PostEvent> for Busy {
    async fn handle(
        &self,
        event: &EventEnvelope<PostEvent>,
        _: &mut ProcessingContext<'_>,
    ) -> Result<(), HandlerError> {
        let mut x = event.position;
        for _ in 0..self.0 {
            x = std::hint::black_box(x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1));
        }
        std::hint::black_box(x);
        Ok(())
    }
}

/// Starts the whole blog application (asynchronous wiring).
pub fn start_blog() -> blog::Running<
    impl PostCommands,
    impl PostQueries + PostSubscriptions,
    blog::infrastructure::InMemoryPostSummaryRepository,
    blog::PostEvents,
> {
    blog::start_in_memory()
}

/// Waits until the blog's processors handled everything committed so far.
pub async fn settle<C, Q, R>(running: &blog::Running<C, Q, R, blog::PostEvents>) {
    running.settled().await;
}
