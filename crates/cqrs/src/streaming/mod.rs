//! Live event processing on Tokio (feature `tokio`, on by default).
//!
//! - [`Events`]: the event service of one event type. It broadcasts what an
//!   [`EventSourced`] commits, implements [`EventFeed`] (catch-up subscriptions)
//!   and spawns [`TrackingProcessor`]s. Wiring an application is one
//!   `Events::new(store)` plus `events.event_sourced()` and `events.track(...)`.
//! - [`subscribe`]: the catch-up subscription itself, a `Stream` of every
//!   event after a position, never missing one.
//! - [`TrackingProcessor`]: one event handler over the log split into
//!   segments by aggregate (Axon's pooled streaming processor): claims shared
//!   between instances, retries, and a sequenced dead-letter queue.
//! - [`LiveEvents`]: the in-memory live feed underneath, plain Tokio
//!   `broadcast` (a latency hint; the store stays the source of truth).
//!
//! Every subscriber is the same type (a receiver of committed envelopes), so
//! a dynamic number of them needs no `dyn`.

mod live_queries;
mod processor;
mod subscription;
mod updates;

use std::sync::Arc;

use futures_core::Stream;
use tokio::sync::broadcast;

pub use live_queries::{GlobalProgress, LiveQueries, LocalProgress, ProjectionProgress};
pub use processor::{ProcessorHandle, ProcessorStatus, ResetError, TrackingOptions, TrackingProcessor};
pub use subscription::{SubscriptionOptions, subscribe};
pub use updates::{CatchUp, Projected, QueryUpdateEmitter, SubscriptionQueries, UpdatesOf};

use crate::{
    Aggregate, DomainEvent, EventEnvelope, EventFeed, EventHandler, EventProcessor, EventSourced, EventStore,
    HandlerError, InMemoryTokenStore, ProcessingFailure, StoreError, TransactionalTokenStore,
};

/// The live feed: Tokio's `broadcast` of committed envelopes, as is.
///
/// A hint, not a guarantee: a receiver that falls behind loses the oldest
/// events, and an event whose save was cancelled between the append and the
/// processing is never sent. [`subscribe`] covers both by going back to the
/// store. Events travel as `Arc`: one allocation per event, shared by every
/// receiver.
pub type LiveEvents<E> = broadcast::Sender<Arc<EventEnvelope<E>>>;

/// The live feed in a processing group: sends what was
/// committed. `E: Clone` because the event is lent; it is copied once, not
/// once per receiver.
impl<E: DomainEvent + Clone, Tx: Send> EventHandler<E, Tx> for LiveEvents<E> {
    async fn handle(
        &self,
        event: &EventEnvelope<E>,
        _: &mut crate::ProcessingContext<'_, Tx>,
    ) -> Result<(), HandlerError> {
        // Nobody listening is not an error: the event is in the store anyway.
        let _ = self.send(Arc::new(event.clone()));
        Ok(())
    }
}

/// Events of type `E` over the store `S`: everything asynchronous about
/// them, in one value.
///
/// ```text
/// let events = Events::new(Arc::new(InMemoryEventStore::new()));
/// let posts = events.event_sourced::<Post>();       // broadcasts its commits
/// let handle = events.track("post-view", post_view); // tracking processor
/// let stream = events.subscribe(0);                  // EventFeed
/// ```
///
/// The blog example wires its whole asynchronous side this way.
///
/// Cloning is cheap and shares everything (the store, the live feed and the
/// tokens): give a clone to each part of the application.
pub struct Events<E, S, C = Arc<InMemoryTokenStore>> {
    /// The source of truth, shared with the repositories (`Arc` or a pool).
    store: S,
    /// The live feed the repositories publish to after each commit.
    live: LiveEvents<E>,
    /// Where tracking processors keep their positions and claims.
    tokens: C,
    /// How subscriptions read.
    options: SubscriptionOptions,
}

impl<E, S> Events<E, S> {
    /// Events over `store`, with in-memory tokens and a live buffer of 1024
    /// events per subscriber.
    pub fn new(store: S) -> Self {
        Self {
            store,
            live: broadcast::channel(1024).0,
            tokens: Arc::default(),
            options: SubscriptionOptions::default(),
        }
    }
}

impl<E, S, C> Events<E, S, C> {
    /// Keeps tracking processors' tokens in `tokens` instead (e.g. a SQL
    /// token store shared by every instance).
    pub fn with_tokens<C2>(self, tokens: C2) -> Events<E, S, C2> {
        Events {
            store: self.store,
            live: self.live,
            tokens,
            options: self.options,
        }
    }

    /// Replaces how subscriptions read (page size, polling).
    #[must_use]
    pub fn with_options(mut self, options: SubscriptionOptions) -> Self {
        self.options = options;
        self
    }

    /// The store.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// The live feed.
    pub fn live(&self) -> &LiveEvents<E> {
        &self.live
    }

    /// The token store the processors share.
    pub fn tokens(&self) -> &C {
        &self.tokens
    }

    /// The progress of processor `name` across all its instances, for live
    /// queries when several instances share its segments.
    pub fn global_progress(&self, name: &str) -> GlobalProgress<C>
    where
        C: Clone,
    {
        GlobalProgress::new(self.tokens.clone(), name)
    }
}

impl<E, S, C> Events<E, S, C>
where
    E: DomainEvent + Clone,
    S: EventStore<E> + Clone + 'static,
    C: TransactionalTokenStore + Clone + 'static,
{
    /// Event sourcing for `A` over the same store, broadcasting each commit
    /// to this feed: what the application's repositories load through, and
    /// its handlers commit through.
    pub fn event_sourced<A>(&self) -> EventSourced<A, S, Self>
    where
        A: Aggregate<Event = E>,
    {
        EventSourced::with_processor(self.store.clone(), self.clone())
    }

    /// A [`TrackingProcessor`] named `name` (its token key) feeding
    /// `handler` (or a tuple of them: a processing group) in the token
    /// store's units of work, to configure (segments, dead letters) and
    /// `spawn`.
    pub fn processor<H>(&self, name: &str, handler: H) -> TrackingProcessor<E, S, H, C>
    where
        H: EventHandler<E, C::Transaction> + 'static,
    {
        TrackingProcessor::new(name, self.store.clone(), &self.live, handler, self.tokens.clone()).with_options(
            TrackingOptions {
                subscription: self.options,
                ..TrackingOptions::default()
            },
        )
    }

    /// Starts a processor with the default settings (one segment). Must run
    /// inside a Tokio runtime.
    pub fn track<H>(&self, name: &str, handler: H) -> ProcessorHandle
    where
        H: EventHandler<E, C::Transaction> + 'static,
    {
        self.processor(name, handler).spawn()
    }

    /// The processor of `projection`, named after its namespace
    /// ([`Projection::NAMESPACE`](crate::Projection::NAMESPACE)).
    pub fn projection<P>(&self, projection: P) -> TrackingProcessor<E, S, P, C>
    where
        P: crate::Projection + EventHandler<E, C::Transaction> + 'static,
    {
        self.processor(P::NAMESPACE, projection)
    }
}

impl<E, S: Clone, C: Clone> Clone for Events<E, S, C> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            live: self.live.clone(),
            tokens: self.tokens.clone(),
            options: self.options,
        }
    }
}

/// As the processing group of an [`EventSourced`]: broadcasts what was committed.
impl<E, S, C> EventProcessor<E> for Events<E, S, C>
where
    E: DomainEvent + Clone,
    S: Send + Sync,
    C: Send + Sync,
{
    async fn process(&self, events: &[EventEnvelope<E>]) -> Result<(), ProcessingFailure> {
        for event in events {
            let _ = self.live.send(Arc::new(event.clone())); // no receiver is fine
        }
        Ok(())
    }
}

/// The events service publishes what command handlers commit: appended to
/// the store, then broadcast to the processors and subscriptions.
impl<E, S, C> crate::EventPublisher<E> for Events<E, S, C>
where
    E: DomainEvent + Clone,
    S: EventStore<E>,
    C: Send + Sync,
{
    fn commit<A: Aggregate<Event = E>>(
        &self,
        entity: crate::Loaded<A>,
        pending: crate::Pending<E>,
    ) -> impl std::future::Future<Output = Result<Option<u64>, StoreError>> + Send {
        crate::event_sourced::publish(&self.store, self, &crate::LogFailures, entity, pending)
    }
}

/// As the [`EventFeed`] port: catch-up subscriptions over the store, followed
/// live.
impl<E, S, C> EventFeed<E> for Events<E, S, C>
where
    E: DomainEvent,
    S: EventStore<E> + Clone + 'static,
    C: Send + Sync,
{
    fn subscribe(
        &self,
        after: u64,
    ) -> impl Stream<Item = Result<Arc<EventEnvelope<E>>, StoreError>> + Send + use<E, S, C> {
        subscribe(self.store.clone(), &self.live, after, self.options)
    }

    async fn last_position(&self) -> Result<u64, StoreError> {
        self.store.last_position().await
    }
}
