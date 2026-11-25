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
//!   segments by aggregate: claims shared
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

use std::sync::{Arc, Mutex};

use futures_core::Stream;
use tokio::sync::{broadcast, watch};

pub use processor::{ProcessorHandle, ProcessorStatus, ResetError, TrackingOptions, TrackingProcessor};
pub use progress::{GlobalProgress, ProcessorProgress, ProjectionProgress};
pub use projection::{ProjectionHandle, ProjectionProcessors};
pub use subscription::{SubscriptionOptions, subscribe};
pub use updates::{
    CatchUp, EmitUpdates, Projected, QueryUpdateChannel, QueryUpdateEmitter, SubscriptionQueries, UpdatesOf,
    emit_updates, head_of,
};

use crate::{
    Aggregate, AggregateLoader, AppendEvents, DomainEvent, EventEnvelope, EventFeed, EventHandler, EventProcessor,
    FanIn, HandlerError, InMemoryEventStore, InMemoryLogs, InMemoryTokenStore, KeepSnapshots, ProcessingFailure,
    ReadLog, Snapshotting, StoreError, TokenStore, TransactionManager, sync::Locked,
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
/// let commands = PostCommandGateway::new(posts, events.clone()); // its unit of work
/// let handle = events.track("post-view", post_view);            // a tracking processor
/// let running = events.projection(post_projection).spawn();     // a processor per namespace
/// let stream = events.subscribe(0);                             // the log, as a Stream
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
    /// Bumped when another process committed to the log
    /// ([`committed_elsewhere`](Self::committed_elsewhere)).
    elsewhere: Arc<watch::Sender<u64>>,
    /// The wake signals of the events reading this log among others
    /// ([`follow`](Self::follow)): bumped at each commit.
    followers: Arc<Mutex<Vec<Arc<watch::Sender<u64>>>>>,
    /// Where tracking processors keep their positions and claims.
    tokens: C,
    /// Opens and commits the processors' units of work.
    transactions: M,
    /// How subscriptions read.
    options: SubscriptionOptions,
}

impl<E, S, C, M> ::std::fmt::Debug for Events<E, S, C, M> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("Events").finish_non_exhaustive()
    }
}

impl<E, S> Events<E, S> {
    /// Events over `store`, with in-memory tokens and a live buffer of 1024
    /// events per subscriber.
    pub fn new(store: S) -> Self {
        Self {
            store,
            live: broadcast::channel(1024).0,
            elsewhere: Arc::new(watch::channel(0).0),
            followers: Arc::default(),
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
            elsewhere: self.elsewhere,
            followers: self.followers,
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
            elsewhere: self.elsewhere,
            followers: self.followers,
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

    /// Says that another process committed to the log: every subscription
    /// and processor of these events reads the store now, instead of at its
    /// next poll ([`SubscriptionOptions::poll_interval`]). Their live feed
    /// only carries this process's commits; whatever relays the others'
    /// (`LISTEN`/`NOTIFY` in `cqrs-postgres`) calls this.
    pub fn committed_elsewhere(&self) {
        wake(&self.elsewhere);
        self.wake_followers();
    }

    /// Wakes these events' subscriptions and processors whenever `source`
    /// commits (or is told another process did): what the events of a
    /// fan-in do with each log they read, whose commits their own live
    /// feed does not carry. `Events::fan_in` and the adapters' fan-ins call
    /// it; without it, the source's commits arrive at the next poll.
    pub fn follow<E2, S2, C2, M2>(&self, source: &Events<E2, S2, C2, M2>) {
        source.followers.locked().push(Arc::clone(&self.elsewhere));
    }

    /// Wakes the events following these ([`follow`](Self::follow)).
    fn wake_followers(&self) {
        for follower in self.followers.locked().iter() {
            wake(follower);
        }
    }

    /// The repository of the aggregates `A` of these events, for command
    /// handlers: it reads from the engine's store, keeps the snapshots that
    /// store can keep for `A` as `A` declares (`snapshot(..)` of
    /// `#[cqrs::aggregate]`; none if it declares none), and takes part in the
    /// engine's units of work: its transaction is the transaction manager's
    /// (`()` in memory).
    ///
    /// The store must be able to keep snapshots of an aggregate that
    /// declares them (`Clone` in memory, `Serialize + DeserializeOwned` on
    /// PostgreSQL), or this does not compile. Where the transaction is not
    /// wanted (a handler of units without one over a transactional engine),
    /// `.in_transaction::<()>()` names it.
    ///
    /// Snapshots are an optimization: each call hands out the snapshots of its
    /// own, and where the store keeps them in memory two repositories of one
    /// aggregate do not share them.
    pub fn repository<A>(&self) -> AggregateLoader<A, S, <S as KeepSnapshots<A, A::Mode>>::Snapshots, M::Tx>
    where
        A: Snapshotting<Event = E>,
        S: Clone + KeepSnapshots<A, A::Mode>,
        M: TransactionManager,
    {
        crate::loader::declared_by_the_aggregate(&self.store)
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
        .waking_on(self.elsewhere.subscribe())
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
            elsewhere: Arc::clone(&self.elsewhere),
            followers: Arc::clone(&self.followers),
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
        self.wake_followers();
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
        self.wake_followers();
    }
}

/// Bumps a wake signal.
fn wake(signal: &watch::Sender<u64>) {
    signal.send_modify(|count| *count = count.wrapping_add(1));
}

impl<U, L> Events<U, FanIn<U, L>> {
    /// The events of several in-memory logs read as one, in their global
    /// order, as `U` (an enum with a variant per log's event type,
    /// `#[event(fan_in)]`): what a projection over them runs on.
    ///
    /// `sources` is a tuple of the logs' events (2 to 8), whose stores share
    /// their numbering ([`InMemoryEventStore::beside`]); their commits wake
    /// these events' readers ([`follow`](Self::follow)). Nothing is appended
    /// through these events: commands keep going to each log's own.
    ///
    /// ```
    /// # use cqrs::{processing::Events, store::{FanIn, InMemoryEventStore}};
    /// # #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)] enum PostEvent { Published }
    /// # #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)] enum UserEvent { Renamed }
    /// # #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)] #[event(fan_in)]
    /// # enum FeedEvent { Post(PostEvent), User(UserEvent) }
    /// let posts = Events::new(InMemoryEventStore::<PostEvent>::new());
    /// let users = Events::new(posts.store().beside::<UserEvent>());
    /// let feed: Events<FeedEvent, _> = Events::fan_in((&posts, &users));
    /// // feed.projection(FeedProjection { .. }).spawn();
    /// # let _ = feed;
    /// ```
    ///
    /// # Panics
    ///
    /// If the stores do not share their numbering (see [`FanIn::new`]).
    pub fn fan_in<T>(sources: T) -> Self
    where
        T: FanInSources<Logs = L>,
        L: InMemoryLogs<U>,
    {
        let events = Self::new(FanIn::new(sources.logs()));
        sources.followed_by(&events);
        events
    }
}

/// The events of the logs a fan-in reads: a tuple of 2 to 8 references to
/// [`Events`] over [`InMemoryEventStore`]s, for [`Events::fan_in`].
pub trait FanInSources: fan_in_sealed::Sealed {
    /// The stores, as [`FanIn`] reads them.
    type Logs;

    /// A clone of each store.
    #[doc(hidden)]
    fn logs(&self) -> Self::Logs;

    /// Has `events` follow each source.
    #[doc(hidden)]
    fn followed_by<U, S, C, M>(&self, events: &Events<U, S, C, M>);
}

/// The seal of [`FanInSources`].
mod fan_in_sealed {
    /// Implemented for tuples of references to `Events`.
    pub trait Sealed {}
}

/// Implements [`FanInSources`] for a tuple of references to events over
/// in-memory stores.
macro_rules! fan_in_sources {
    ($(($event:ident, $tokens:ident, $transactions:ident)),+) => {
        impl<$($event, $tokens, $transactions),+> fan_in_sealed::Sealed
            for ($(&Events<$event, InMemoryEventStore<$event>, $tokens, $transactions>,)+) {}

        #[allow(non_snake_case)]
        impl<$($event, $tokens, $transactions),+> FanInSources
            for ($(&Events<$event, InMemoryEventStore<$event>, $tokens, $transactions>,)+)
        {
            type Logs = ($(InMemoryEventStore<$event>,)+);

            fn logs(&self) -> Self::Logs {
                let ($($event,)+) = self;
                ($($event.store().clone(),)+)
            }

            fn followed_by<Union, Store, Tokens, Transactions>(&self, events: &Events<Union, Store, Tokens, Transactions>) {
                let ($($event,)+) = self;
                $(events.follow($event);)+
            }
        }
    };
}

fan_in_sources!((A, CA, MA), (B, CB, MB));
fan_in_sources!((A, CA, MA), (B, CB, MB), (C, CC, MC));
fan_in_sources!((A, CA, MA), (B, CB, MB), (C, CC, MC), (D, CD, MD));
fan_in_sources!((A, CA, MA), (B, CB, MB), (C, CC, MC), (D, CD, MD), (E, CE, ME));
fan_in_sources!(
    (A, CA, MA),
    (B, CB, MB),
    (C, CC, MC),
    (D, CD, MD),
    (E, CE, ME),
    (F, CF, MF)
);
fan_in_sources!(
    (A, CA, MA),
    (B, CB, MB),
    (C, CC, MC),
    (D, CD, MD),
    (E, CE, ME),
    (F, CF, MF),
    (G, CG, MG)
);
fan_in_sources!(
    (A, CA, MA),
    (B, CB, MB),
    (C, CC, MC),
    (D, CD, MD),
    (E, CE, ME),
    (F, CF, MF),
    (G, CG, MG),
    (H, CH, MH)
);

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
        subscription::subscribe_waking(
            self.store.clone(),
            &self.live,
            self.elsewhere.subscribe(),
            after,
            self.options,
        )
    }

    async fn last_position(&self) -> Result<u64, StoreError> {
        self.store.last_position().await
    }
}
