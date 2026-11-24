//! Command handlers that decide by appending events, and commit them
//! through the publisher they were built with (Axon 5's `EventAppender`,
//! NestJS's `aggregate.commit()`).
//!
//! One struct per use case, holding the services it needs: usually the
//! application's own repository and the [`EventPublisher`] its commits go
//! through. `#[command_handler]` reads `type Command` and `type Event` from
//! the signature of `handle`:
//!
//! ```
//! use std::future::Future;
//! use cqrs::{Appender, AggregateLoader, CommandGateway, CommandHandler, EventAppender, EventPublisher, InlinePublisher, InMemoryEventStore, Loaded, New, StoreError, testing::block_on};
//!
//! #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
//! enum CounterEvent { Added(u64) }
//!
//! #[cqrs::aggregate(event = CounterEvent)]
//! struct Counter { id: u32, total: u64 }
//!
//! impl Counter {
//!     fn apply(&mut self, CounterEvent::Added(n): &CounterEvent) { self.total += n; }
//! }
//!
//! #[cqrs::command(output = u64, error = StoreError)]
//! struct Add { id: u32, n: u64 }
//!
//! /// The application's port: how handlers find counters.
//! trait Counters: Send + Sync {
//!     fn find(&self, id: u32) -> impl Future<Output = Result<Option<Loaded<Counter>>, StoreError>> + Send;
//! }
//!
//! /// Its adapter, rebuilding counters from the store.
//! struct StoredCounters(AggregateLoader<Counter, InMemoryEventStore<CounterEvent>>);
//!
//! impl Counters for StoredCounters {
//!     async fn find(&self, id: u32) -> Result<Option<Loaded<Counter>>, StoreError> {
//!         self.0.load(&id).await
//!     }
//! }
//!
//! /// The repository and the publisher: both are the handler's, given once.
//! struct AddHandler<R, P> { counters: R, events: P }
//!
//! #[cqrs::command_handler]
//! impl<R: Counters, P: EventPublisher<CounterEvent>> CommandHandler for AddHandler<R, P> {
//!     async fn handle(&self, command: &Add, events: &mut Appender<CounterEvent>) -> Result<u64, StoreError> {
//!         // A counter starts at zero: adding to a missing one creates it.
//!         let counter = self.counters.find(command.id).await?.unwrap_or_else(|| New(command.id).into());
//!         events.append(CounterEvent::Added(command.n));
//!         let total = counter.total + command.n;
//!         events.commit(counter, &self.events).await?;
//!         Ok(total)
//!     }
//! }
//!
//! #[derive(cqrs::Gateway)]
//! #[gateway(commands)]
//! struct Commands<R, P> { add: AddHandler<R, P> }
//!
//! let store = InMemoryEventStore::new();
//! let commands = Commands {
//!     add: AddHandler {
//!         counters: StoredCounters(AggregateLoader::new(store.clone())),
//!         events: InlinePublisher::new(store),
//!     },
//! };
//! assert_eq!(block_on(commands.send(Add { id: 1, n: 2 }))?, 2);
//! assert_eq!(block_on(commands.send(Add { id: 1, n: 3 }))?, 5);
//! # Ok::<(), StoreError>(())
//! ```
//!
//! A handler commits to a publisher of its own event type; any other value
//! is refused where the handler is written:
//!
//! ```compile_fail,E0277
//! # use cqrs::{Appender, CommandHandler, New, StoreError};
//! # #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)] enum Ticked { Once }
//! # #[cqrs::aggregate(event = Ticked)] struct Clock { id: u32 }
//! # impl Clock { fn apply(&mut self, _: &Ticked) {} }
//! # #[cqrs::command(error = StoreError)] struct Tick;
//! struct NotAPublisher;
//! struct TickHandler;
//! #[cqrs::command_handler]
//! impl CommandHandler for TickHandler {
//!     async fn handle(&self, _: &Tick, events: &mut Appender<Ticked>) -> Result<(), StoreError> {
//!         events.commit(New::<Clock>(1), &NotAPublisher).await
//!     }
//! }
//! ```
//!
//! The command is taken by reference, as the handler may run again:
//!
//! ```compile_fail
//! # use cqrs::{Appender, CommandHandler};
//! # #[cqrs::command(error = ())] struct Ping;
//! struct PingHandler;
//! #[cqrs::command_handler]
//! impl CommandHandler for PingHandler {
//!     async fn handle(&self, command: Ping, events: &mut Appender<()>) -> Result<(), ()> { Ok(()) }
//! }
//! ```
//!
//! For each command the gateway runs the handler with a fresh [`Appender`]
//! (carrying the command's metadata): the handler finds what it needs, lets
//! the domain append, and commits explicitly. If a commit loses a race (a
//! stream moved on, or a [DCB condition](crate::AppendCondition) failed),
//! [`dispatch_command`] runs the handler again with a new appender, so it
//! decides on fresh state.

use std::future::Future;

use crate::{
    Aggregate, AppendCondition, Command, DomainEvent, ErrorOf, EventCriteria, EventEnvelope, ExpectedVersion, Loaded,
    Metadata, OutputOf, SourceEvents, StoreError,
};

/// The context of one run of a command handler, handed in by the gateway
/// (Axon 5's `EventAppender` with its processing context): the events the
/// domain decided on, until [`commit`](Self::commit) takes them; the
/// metadata the command was sent with, which every committed event carries
/// (Axon's correlation data); and the [`AppendCondition`] of what it read
/// with [`source`](Self::source) (Axon 5's DCB). The publisher is the
/// handler's, passed to `commit`.
///
/// It is an [`EventAppender`](crate::EventAppender), so domain methods take
/// it as `&mut impl EventAppender<E>`.
#[derive(Debug)]
pub struct Appender<E> {
    /// Appended and not committed yet.
    events: Vec<E>,
    /// Sent with the command; stored with every event committed.
    metadata: Metadata,
    /// What the run read by criteria, checked by every commit.
    condition: Option<AppendCondition>,
    /// Some commit of this run succeeded: re-running would commit it twice.
    saved: bool,
    /// Some commit of this run lost a race (a stream moved on, or the
    /// condition failed).
    lost_race: bool,
}

/// What one commit stores: the events taken from the [`Appender`], its
/// metadata and its condition.
#[derive(Debug)]
pub struct Pending<E> {
    /// The events, in the order they were appended.
    pub events: Vec<E>,
    /// Stored with every event.
    pub metadata: Metadata,
    /// Checked atomically with the append, if any.
    pub condition: Option<AppendCondition>,
}

impl<E> Pending<E> {
    /// `events` without metadata nor condition: what a test commits through
    /// a publisher.
    pub fn new(events: Vec<E>) -> Self {
        Self {
            events,
            metadata: Metadata::new(),
            condition: None,
        }
    }
}

impl<E> Appender<E> {
    /// An empty appender without metadata (what a test passes).
    pub fn new() -> Self {
        Self::with_metadata(Metadata::new())
    }

    /// An empty appender whose events carry `metadata`: what a gateway hands
    /// each run of a handler.
    pub fn with_metadata(metadata: Metadata) -> Self {
        Self {
            events: Vec::new(),
            metadata,
            condition: None,
            saved: false,
            lost_race: false,
        }
    }

    /// The metadata the command was sent with.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// The events appended and not committed yet.
    pub fn pending(&self) -> &[E] {
        &self.events
    }

    /// What the commits of this run are conditioned on, if it read anything
    /// by criteria.
    pub fn condition(&self) -> Option<&AppendCondition> {
        self.condition.as_ref()
    }

    /// Reads every event matching `criteria` from `store` (any stream) and
    /// makes the following commits of this run conditional on nothing
    /// matching being appended meanwhile. Several reads combine: the commits
    /// check all of them.
    ///
    /// # Errors
    ///
    /// The store's.
    pub async fn source<S: SourceEvents<E>>(
        &mut self,
        store: &S,
        criteria: EventCriteria,
    ) -> Result<Vec<EventEnvelope<E>>, StoreError>
    where
        E: DomainEvent,
    {
        let (events, head) = store.source(&criteria).await?;
        self.depend_on(AppendCondition { criteria, after: head });
        Ok(events)
    }

    /// Makes the following commits of this run also check `condition`, e.g.
    /// the one of an aggregate read but not committed.
    pub fn depend_on(&mut self, condition: AppendCondition) {
        self.condition = AppendCondition::both(self.condition.take(), Some(condition));
    }

    /// Commits the pending events to `entity`'s stream through `publisher`
    /// (NestJS's `aggregate.commit()`), with the command's metadata and the
    /// conditions of what this run read. `entity` is a [`Loaded`] aggregate,
    /// whose stream must still be at the version it was loaded at, or a
    /// [`New`](crate::New) one, whose stream must not exist.
    ///
    /// # Errors
    ///
    /// [`StoreError::Conflict`] if the entity changed since it was loaded (or
    /// exists already, for a new one), [`StoreError::ConditionFailed`] if
    /// what the run read changed: the gateway then runs the handler again,
    /// if nothing was committed before in this run.
    pub async fn commit<A, P>(&mut self, entity: impl Into<Loaded<A>>, publisher: &P) -> Result<(), StoreError>
    where
        E: DomainEvent,
        A: Aggregate<Event = E>,
        P: EventPublisher<E>,
    {
        let pending = self.take();
        let outcome = publisher.commit(entity.into(), pending).await;
        self.record(outcome)
    }

    /// Commits the pending events like [`commit`](Self::commit), but in
    /// `tx`, a transaction the caller owns: the application's own writes in
    /// it (an aggregate's state, a row of its own) and the events commit
    /// together, or roll back together. The library borrows `tx` and can
    /// neither commit it nor keep it.
    ///
    /// ```text
    /// let mut tx = pool.begin().await?;                                    // the transaction is yours
    /// let committed = appender.commit_in(&mut tx, post, &self.events).await?;
    /// save_state(&mut tx, committed.entity()).await?;                      // your repository, your ORM
    /// tx.commit().await?;                                                  // the commit is yours
    /// committed.announce();                                                // processors, subscriptions
    /// ```
    ///
    /// Nothing is announced before the caller's commit, which the publisher
    /// cannot see: the returned [`Committed`] carries the events, and
    /// [`announce`](Committed::announce) hands them to processors and
    /// subscriptions. Forgetting it only costs latency (they also poll the
    /// log); dropping it is right when the transaction rolled back.
    ///
    /// # Errors
    ///
    /// As [`commit`](Self::commit). A lost race fails the statement, which
    /// aborts a PostgreSQL transaction: drop it, and the gateway runs the
    /// handler again, which opens a new one.
    pub async fn commit_in<'p, A, P, Tx>(
        &mut self,
        tx: &mut Tx,
        entity: impl Into<Loaded<A>>,
        publisher: &'p P,
    ) -> Result<Committed<'p, A, P>, StoreError>
    where
        E: DomainEvent,
        A: Aggregate<Event = E>,
        P: PublishIn<E, Tx>,
        Tx: Send,
    {
        let entity = entity.into();
        let pending = self.take();
        let events = match publisher.append_in(tx, &entity, pending).await {
            Ok(events) => events,
            Err(error) => return Err(self.lost(error)),
        };
        self.saved(events.last().map(|event| event.position));
        Ok(Committed {
            entity: entity.after(&events),
            events,
            publisher,
        })
    }

    /// The pending events, with a copy of the metadata and the condition.
    fn take(&mut self) -> Pending<E> {
        Pending {
            events: std::mem::take(&mut self.events),
            metadata: self.metadata.clone(),
            condition: self.condition.clone(),
        }
    }

    /// Records a commit's outcome, so the gateway knows whether a re-run is
    /// safe, and moves the condition past the events just committed.
    fn record(&mut self, outcome: Result<Option<u64>, StoreError>) -> Result<(), StoreError> {
        match outcome {
            Ok(last) => {
                self.saved(last);
                Ok(())
            }
            Err(error) => Err(self.lost(error)),
        }
    }

    /// A commit succeeded, its last event at `last`.
    fn saved(&mut self, last: Option<u64>) {
        self.saved = true;
        // Our own events match what we read: later commits check only what
        // others append after them.
        if let (Some(condition), Some(last)) = (&mut self.condition, last) {
            condition.after = last;
        }
    }

    /// A commit failed with `error`, returned; a lost race makes a re-run
    /// safe if nothing was committed before.
    fn lost(&mut self, error: StoreError) -> StoreError {
        if matches!(
            error,
            StoreError::ConditionFailed
                | StoreError::Conflict {
                    expected: ExpectedVersion::Exact(_),
                    ..
                }
        ) {
            self.lost_race = true;
        }
        error
    }

    /// A commit lost a race and nothing was committed before: running the
    /// handler again on fresh state is safe.
    fn retryable(&self) -> bool {
        self.lost_race && !self.saved
    }
}

impl<E> Default for Appender<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> Extend<E> for Appender<E> {
    fn extend<I: IntoIterator<Item = E>>(&mut self, events: I) {
        self.events.extend(events);
    }
}

/// Persists and publishes what a command decided (NestJS's
/// `EventPublisher`): appends the pending events to the entity's stream,
/// expecting the version it was loaded at, with the command's metadata and
/// conditions, then hands them to whatever processes events. A handler holds
/// one and commits through its [`Appender`]: `events.commit(post, &self.events)`.
///
/// One publisher serves every aggregate of an event type.
/// [`Events`](crate::Events) is one (it broadcasts the commit), and so is
/// an [`InlinePublisher`](crate::InlinePublisher) (it feeds its processing group
/// inline); a test double implements it to record what was committed.
pub trait EventPublisher<E: DomainEvent>: Send + Sync {
    /// Commits `pending` to `entity`'s stream; returns the position of the
    /// last event committed (`None` if there was nothing to commit).
    ///
    /// # Errors
    ///
    /// [`StoreError::Conflict`] or [`StoreError::ConditionFailed`] if the
    /// decision is stale; the store's error otherwise.
    fn commit<A: Aggregate<Event = E>>(
        &self,
        entity: Loaded<A>,
        pending: Pending<E>,
    ) -> impl Future<Output = Result<Option<u64>, StoreError>> + Send;
}

/// A borrowed publisher is a publisher (what each run's appender holds).
// Forwards every method, so an override of a provided one is never lost.
#[deny(clippy::missing_trait_methods)]
impl<E: DomainEvent, T: EventPublisher<E>> EventPublisher<E> for &T {
    fn commit<A: Aggregate<Event = E>>(
        &self,
        entity: Loaded<A>,
        pending: Pending<E>,
    ) -> impl Future<Output = Result<Option<u64>, StoreError>> + Send {
        (**self).commit(entity, pending)
    }
}

/// A shared publisher is a publisher.
// Forwards every method, so an override of a provided one is never lost.
#[deny(clippy::missing_trait_methods)]
impl<E: DomainEvent, T: EventPublisher<E>> EventPublisher<E> for std::sync::Arc<T> {
    fn commit<A: Aggregate<Event = E>>(
        &self,
        entity: Loaded<A>,
        pending: Pending<E>,
    ) -> impl Future<Output = Result<Option<u64>, StoreError>> + Send {
        (**self).commit(entity, pending)
    }
}

/// Appends in a transaction its caller owns, for
/// [`Appender::commit_in`]: the [`EventPublisher`] counterpart for
/// applications that write their own state with the events.
///
/// [`Events`](crate::Events) is one, for every transaction its store can
/// append in ([`AppendEvents<E, Tx>`](crate::AppendEvents)).
/// [`InlinePublisher`](crate::InlinePublisher) is not: its handlers run
/// right after the commit, which it cannot see, and a forgotten
/// [`announce`](Committed::announce) would silently skip them.
pub trait PublishIn<E: DomainEvent, Tx: Send>: Announce<E> {
    /// Appends `pending` to `entity`'s stream in `tx`, expecting the
    /// version it was loaded at (or the condition it was built from);
    /// returns the envelopes, invisible to others until `tx` commits.
    ///
    /// # Errors
    ///
    /// As [`EventPublisher::commit`].
    fn append_in<A: Aggregate<Event = E>>(
        &self,
        tx: &mut Tx,
        entity: &Loaded<A>,
        pending: Pending<E>,
    ) -> impl Future<Output = Result<Vec<EventEnvelope<E>>, StoreError>> + Send;
}

/// Hands events committed in a caller's transaction to whatever follows
/// them (processors, subscriptions): the after-commit half of
/// [`PublishIn`], whatever the transaction was.
pub trait Announce<E>: Send + Sync {
    /// `events` were committed: tell those following the log.
    fn announce(&self, events: Vec<EventEnvelope<E>>);
}

/// A shared publisher announces too.
impl<E, T: Announce<E>> Announce<E> for std::sync::Arc<T> {
    fn announce(&self, events: Vec<EventEnvelope<E>>) {
        (**self).announce(events);
    }
}

/// A shared publisher appends in transactions too.
// Forwards every method, so an override of a provided one is never lost.
#[deny(clippy::missing_trait_methods)]
impl<E: DomainEvent, Tx: Send, T: PublishIn<E, Tx>> PublishIn<E, Tx> for std::sync::Arc<T> {
    fn append_in<A: Aggregate<Event = E>>(
        &self,
        tx: &mut Tx,
        entity: &Loaded<A>,
        pending: Pending<E>,
    ) -> impl Future<Output = Result<Vec<EventEnvelope<E>>, StoreError>> + Send {
        (**self).append_in(tx, entity, pending)
    }
}

/// What [`Appender::commit_in`] appended in the caller's transaction: the
/// entity as it is after the events (for a repository keeping its state),
/// and the events, to [`announce`](Self::announce) once the caller
/// committed. Drop it if the transaction rolled back.
#[must_use = "announce() it once your transaction committed (or drop it if it rolled back)"]
pub struct Committed<'p, A: Aggregate, P> {
    /// The entity after the events, at its new version.
    entity: Loaded<A>,
    /// What was appended.
    events: Vec<EventEnvelope<A::Event>>,
    /// Who announces them.
    publisher: &'p P,
}

impl<A: Aggregate, P> Committed<'_, A, P> {
    /// The entity after the events ([`Aggregate::evolve`]), at the version
    /// they took it to: what a repository keeping state saves in the same
    /// transaction, with that version.
    pub fn entity(&self) -> &Loaded<A> {
        &self.entity
    }

    /// The events appended, in order.
    pub fn events(&self) -> &[EventEnvelope<A::Event>] {
        &self.events
    }

    /// The position of the last event appended (`None` if there was
    /// nothing to append).
    pub fn last_position(&self) -> Option<u64> {
        self.events.last().map(|event| event.position)
    }

    /// Tells processors and subscriptions about the events, once the
    /// transaction they were appended in committed; returns the entity.
    pub fn announce(self) -> Loaded<A>
    where
        P: Announce<A::Event>,
    {
        self.publisher.announce(self.events);
        self.entity
    }
}

impl<A: Aggregate, P> std::fmt::Debug for Committed<'_, A, P>
where
    A::Event: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Committed")
            .field("version", &self.entity.version())
            .field("events", &self.events)
            .finish_non_exhaustive()
    }
}

/// Handles one command: finds what it needs (through the application's
/// repositories), lets the domain append events, and commits them through
/// its appender and its own publisher: `events.commit(entity, &self.events)`.
pub trait CommandHandler: Send + Sync {
    /// The command handled.
    type Command: Command + Sync;
    /// The events it decides on.
    type Event: DomainEvent;

    /// Decides and commits. Events still pending when it returns `Ok` are a
    /// bug (a forgotten `commit`): debug builds panic.
    ///
    /// It may run again with a fresh appender if a commit lost a race (see
    /// [`conflict_retries`](Self::conflict_retries)): besides committing,
    /// it must have no side effects.
    fn handle(
        &self,
        command: &Self::Command,
        events: &mut Appender<Self::Event>,
    ) -> impl Future<Output = Result<OutputOf<Self::Command>, ErrorOf<Self::Command>>> + Send;

    /// How many times the handler re-runs after losing a race (0: never).
    fn conflict_retries(&self) -> u32 {
        8
    }
}

/// Runs `handler` on `command`: a fresh appender per run, carrying the
/// command's `metadata`, and a re-run when a commit lost a race. What a
/// command gateway does for each command, and how a handler is called
/// directly.
///
/// # Errors
///
/// What the handler returns (after the retries, for a lost race).
pub async fn dispatch_command<H>(
    handler: &H,
    command: H::Command,
    metadata: Metadata,
) -> Result<OutputOf<H::Command>, ErrorOf<H::Command>>
where
    H: CommandHandler,
{
    let mut retries = handler.conflict_retries();
    loop {
        let mut events = Appender::with_metadata(metadata.clone());
        let result = handler.handle(&command, &mut events).await;
        if result.is_err() && events.retryable() && retries > 0 {
            retries -= 1;
            continue;
        }
        debug_assert!(
            result.is_err() || events.pending().is_empty(),
            "{} returned Ok with events it never committed",
            std::any::type_name::<H>(),
        );
        return result;
    }
}
