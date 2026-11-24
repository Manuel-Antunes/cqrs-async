//! Live event processing on Tokio (feature `tokio`, on by default).
//!
//! - [`Events`]: the event service of one event type. It broadcasts what an
//!   command handlers commit (it is their
//!   [`EventPublisher`](crate::EventPublisher)), implements [`EventFeed`]
//!   (catch-up subscriptions) and spawns [`TrackingProcessor`]s. Wiring an
//!   application is one `Events::new(store)` plus `events.track(...)`.
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

mod processor;
mod progress;
mod projection;
mod subscription;
mod updates;

use std::sync::Arc;

use futures_core::Stream;
use tokio::sync::broadcast;

pub use processor::{ProcessorHandle, ProcessorStatus, ResetError, TrackingOptions, TrackingProcessor};
pub use progress::{GlobalProgress, ProcessorProgress, ProjectionProgress};
pub use projection::{ProjectionHandle, ProjectionProcessors};
pub use subscription::{SubscriptionOptions, subscribe};
pub use updates::{
    CatchUp, EmitUpdates, Projected, QueryUpdateEmitter, SubscriptionQueries, UpdatesOf, emit_updates, head_of,
};

use crate::{
    Aggregate, AppendEvents, DomainEvent, EventEnvelope, EventFeed, EventHandler, EventProcessor, HandlerError,
    InMemoryTokenStore, ProcessingFailure, ReadLog, StoreError, TokenStore, TransactionManager,
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
/// let events = Events::new(InMemoryEventStore::new());
/// let commands = PostGateway::new(posts, events.clone()); // handlers commit through it
/// let handle = events.track("post-view", post_view); // tracking processor
/// let stream = events.subscribe(0);                  // EventFeed
/// ```
///
/// The blog example wires its whole asynchronous side this way.
///
/// Cloning is cheap and shares everything (the store, the live feed, the
/// tokens and the transaction manager): give a clone to each part of the
/// application.
///
/// Its processors run their units of work in transactions from `M` (`()`:
/// in memory), which the tokens `C` must join: a SQL token store goes with
/// that database's transactions, checked when a processor is made.
pub struct Events<E, S, C = InMemoryTokenStore, M = ()> {
    /// The source of truth, shared with the repositories (a handle).
    store: S,
    /// The live feed the repositories publish to after each commit.
    live: LiveEvents<E>,
    /// Where tracking processors keep their positions and claims.
    tokens: C,
    /// Opens and commits the processors' units of work.
    transactions: M,
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
            tokens: InMemoryTokenStore::default(),
            transactions: (),
            options: SubscriptionOptions::default(),
        }
    }
}

impl<E, S, C, M> Events<E, S, C, M> {
    /// Keeps tracking processors' tokens in `tokens` instead (e.g. a SQL
    /// token store shared by every instance). Its unit of work must be the
    /// one of [`with_transactions`](Self::with_transactions).
    pub fn with_tokens<C2>(self, tokens: C2) -> Events<E, S, C2, M> {
        Events {
            store: self.store,
            live: self.live,
            tokens,
            transactions: self.transactions,
            options: self.options,
        }
    }

    /// Runs the processors' units of work in transactions from
    /// `transactions` (e.g. the database the read models and the tokens
    /// live in).
    pub fn with_transactions<M2>(self, transactions: M2) -> Events<E, S, C, M2> {
        Events {
            store: self.store,
            live: self.live,
            tokens: self.tokens,
            transactions,
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

    /// What opens and commits the processors' units of work.
    pub fn transactions(&self) -> &M {
        &self.transactions
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

impl<E, S, C, M> Events<E, S, C, M>
where
    E: DomainEvent + Clone,
    S: ReadLog<E> + Clone + 'static,
    C: TokenStore<Tx = M::Tx> + Clone + 'static,
    M: TransactionManager + Clone + 'static,
{
    /// A [`TrackingProcessor`] named `name` (its token key) feeding
    /// `handler` (or a tuple of them: a processing group) in the
    /// processors' units of work, to configure (segments, dead letters) and
    /// `spawn`.
    pub fn processor<H>(&self, name: &str, handler: H) -> TrackingProcessor<E, S, H, C, M>
    where
        H: EventHandler<E, M::Tx> + 'static,
    {
        TrackingProcessor::new(
            name,
            self.store.clone(),
            &self.live,
            handler,
            self.tokens.clone(),
            self.transactions.clone(),
        )
        .with_options(TrackingOptions {
            subscription: self.options,
            ..TrackingOptions::default()
        })
    }

    /// Starts a processor with the default settings (one segment). Must run
    /// inside a Tokio runtime.
    pub fn track<H>(&self, name: &str, handler: H) -> ProcessorHandle
    where
        H: EventHandler<E, M::Tx> + 'static,
    {
        self.processor(name, handler).spawn()
    }

    /// The processors of `projection`, one per namespace, named
    /// `"{projection}.{namespace}"`, to configure and `spawn`.
    pub fn projection<P>(&self, projection: P) -> ProjectionProcessors<E, S, P, C, M>
    where
        P: crate::Route<M::Tx, Event = E> + 'static,
    {
        ProjectionProcessors::new(projection, |name, handler| self.processor(&name, handler))
    }
}

impl<E, S: Clone, C: Clone, M: Clone> Clone for Events<E, S, C, M> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            live: self.live.clone(),
            tokens: self.tokens.clone(),
            transactions: self.transactions.clone(),
            options: self.options,
        }
    }
}

/// As the processing group of an [`InlinePublisher`](crate::InlinePublisher): broadcasts what was committed.
impl<E, S, C, M> EventProcessor<E> for Events<E, S, C, M>
where
    E: DomainEvent + Clone,
    S: Send + Sync,
    C: Send + Sync,
    M: Send + Sync,
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
impl<E, S, C, M> crate::EventPublisher<E> for Events<E, S, C, M>
where
    E: DomainEvent + Clone,
    S: AppendEvents<E>,
    C: Send + Sync,
    M: Send + Sync,
{
    fn commit<A: Aggregate<Event = E>>(
        &self,
        entity: crate::Loaded<A>,
        pending: crate::Pending<E>,
    ) -> impl std::future::Future<Output = Result<Option<u64>, StoreError>> + Send {
        crate::inline::publish(&self.store, self, &crate::LogFailures, entity, pending)
    }
}

/// Commands without a transaction: the store appends on its own, in one
/// statement, then the commit is broadcast.
impl<E, S, C, M> crate::UnitOfWork<E, crate::Autocommit> for Events<E, S, C, M>
where
    E: DomainEvent,
    S: AppendEvents<E>,
    C: Send + Sync,
    M: Send + Sync,
{
    fn begin(&self) -> impl std::future::Future<Output = Result<(), StoreError>> + Send {
        std::future::ready(Ok(()))
    }

    fn append(
        &self,
        tx: &mut (),
        write: crate::Write<E>,
    ) -> impl std::future::Future<Output = Result<Vec<EventEnvelope<E>>, StoreError>> + Send {
        crate::inline::append_write(&self.store, tx, write)
    }

    fn commit(&self, (): ()) -> impl std::future::Future<Output = Result<(), StoreError>> + Send {
        std::future::ready(Ok(()))
    }

    async fn announce(&self, events: Vec<EventEnvelope<E>>) {
        self.broadcast(events);
    }
}

/// Commands in a transaction of the events' transaction manager: the
/// handler and its repositories write in it, the events are appended in it
/// last, it commits, then the commit is broadcast.
impl<E, S, C, M> crate::UnitOfWork<E, crate::InTransaction<M::Tx>> for Events<E, S, C, M>
where
    E: DomainEvent,
    S: AppendEvents<E, M::Tx>,
    C: Send + Sync,
    M: TransactionManager,
{
    const ROLLS_BACK: bool = M::ROLLS_BACK;

    fn begin(&self) -> impl std::future::Future<Output = Result<M::Tx, StoreError>> + Send {
        self.transactions.begin()
    }

    fn append(
        &self,
        tx: &mut M::Tx,
        write: crate::Write<E>,
    ) -> impl std::future::Future<Output = Result<Vec<EventEnvelope<E>>, StoreError>> + Send {
        crate::inline::append_write(&self.store, tx, write)
    }

    fn commit(&self, tx: M::Tx) -> impl std::future::Future<Output = Result<(), StoreError>> + Send {
        self.transactions.commit(tx)
    }

    async fn announce(&self, events: Vec<EventEnvelope<E>>) {
        self.broadcast(events);
    }
}

impl<E, S, C, M> Events<E, S, C, M> {
    /// Hands committed `events` to the live feed (no receiver is fine).
    fn broadcast(&self, events: Vec<EventEnvelope<E>>) {
        for event in events {
            let _ = self.live.send(Arc::new(event));
        }
    }
}

/// As the [`EventFeed`] port: catch-up subscriptions over the store, followed
/// live.
impl<E, S, C, M> EventFeed<E> for Events<E, S, C, M>
where
    E: DomainEvent,
    S: ReadLog<E> + Clone + 'static,
    C: Send + Sync,
    M: Send + Sync,
{
    fn subscribe(
        &self,
        after: u64,
    ) -> impl Stream<Item = Result<Arc<EventEnvelope<E>>, StoreError>> + Send + use<E, S, C, M> {
        subscribe(self.store.clone(), &self.live, after, self.options)
    }

    async fn last_position(&self) -> Result<u64, StoreError> {
        self.store.last_position().await
    }
}
