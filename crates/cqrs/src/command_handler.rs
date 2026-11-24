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
//! #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent, cqrs::TagResolver)]
//! enum CounterEvent { Added(u64) }
//!
//! #[derive(cqrs::AggregateRoot)]
//! #[aggregate(event = CounterEvent)]
//! struct Counter { id: u32, total: u64 }
//!
//! impl Counter {
//!     fn apply(&mut self, CounterEvent::Added(n): &CounterEvent) { self.total += n; }
//! }
//!
//! #[derive(cqrs::Command)]
//! #[command(output = u64, error = StoreError)]
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
//! assert_eq!(block_on(commands.send(Add { id: 1, n: 2 })), Ok(2));
//! assert_eq!(block_on(commands.send(Add { id: 1, n: 3 })), Ok(5));
//! ```
//!
//! A handler commits to a publisher of its own event type; any other value
//! is refused where the handler is written:
//!
//! ```compile_fail,E0277
//! # use cqrs::{Appender, CommandHandler, New, StoreError};
//! # #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent, cqrs::TagResolver)] enum Ticked { Once }
//! # #[derive(cqrs::AggregateRoot)] #[aggregate(event = Ticked)] struct Clock { id: u32 }
//! # impl Clock { fn apply(&mut self, _: &Ticked) {} }
//! # #[derive(cqrs::Command)] #[command(error = StoreError)] struct Tick;
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
//! # #[derive(cqrs::Command)] #[command(error = ())] struct Ping;
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
    Aggregate, AppendCondition, Command, DomainEvent, ErrorOf, EventCriteria, EventEnvelope, EventStore,
    ExpectedVersion, Loaded, Metadata, OutputOf, StoreError,
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
    pub async fn source<S: EventStore<E>>(
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
                self.saved = true;
                // Our own events match what we read: later commits check
                // only what others append after them.
                if let (Some(condition), Some(last)) = (&mut self.condition, last) {
                    condition.after = last;
                }
                Ok(())
            }
            Err(error) => {
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
                Err(error)
            }
        }
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
