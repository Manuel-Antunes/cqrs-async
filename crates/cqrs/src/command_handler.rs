//! Command handlers that decide by appending events, and commit them
//! through their context (Axon 5's `EventAppender`, NestJS's
//! `aggregate.commit()`).
//!
//! One struct per use case, holding only the services it needs (usually the
//! application's own repository). The publisher is not one of them: the
//! gateway is given it ([`WithPublisher`]) and hands it to each run through
//! the [`Appender`]. `#[command_handler]` reads `type Command` and
//! `type Event` from the signature of `handle`:
//!
//! ```
//! use std::{future::Future, sync::Arc};
//! use cqrs::{Appender, CommandGateway, CommandHandler, AggregateLoader, EventAppender, InlinePublisher, InMemoryEventStore, Loaded, StoreError, WithPublisher, testing::block_on};
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
//! struct StoredCounters(AggregateLoader<Counter, Arc<InMemoryEventStore<CounterEvent>>>);
//!
//! impl Counters for StoredCounters {
//!     async fn find(&self, id: u32) -> Result<Option<Loaded<Counter>>, StoreError> {
//!         self.0.load(&id).await
//!     }
//! }
//!
//! struct AddHandler<R> { counters: R }
//!
//! #[cqrs::command_handler]
//! impl<R: Counters> CommandHandler for AddHandler<R> {
//!     async fn handle(&self, command: &Add, events: &mut Appender<CounterEvent>) -> Result<u64, StoreError> {
//!         let counter = match self.counters.find(command.id).await? {
//!             Some(counter) => counter,
//!             None => Loaded::new(cqrs::Aggregate::initial(&command.id)),
//!         };
//!         events.append(CounterEvent::Added(command.n));
//!         let total = counter.total + command.n;
//!         events.commit(counter).await?;
//!         Ok(total)
//!     }
//! }
//!
//! #[derive(cqrs::Gateway)]
//! #[gateway(commands)]
//! struct Commands<R> { add: AddHandler<R> }
//!
//! let store = Arc::new(InMemoryEventStore::new());
//! let commands = Commands { add: AddHandler { counters: StoredCounters(AggregateLoader::new(Arc::clone(&store))) } }
//!     .with_publisher(InlinePublisher::new(store));
//! assert_eq!(block_on(commands.send(Add { id: 1, n: 2 })), Ok(2));
//! assert_eq!(block_on(commands.send(Add { id: 1, n: 3 })), Ok(5));
//! ```
//!
//! A gateway of commands sends nothing until it has a publisher:
//!
//! ```compile_fail,E0599
//! # use cqrs::{Appender, CommandGateway, CommandHandler, StoreError};
//! # #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent, cqrs::TagResolver)] enum Ticked { Once }
//! # #[derive(cqrs::Command)] #[command(error = StoreError)] struct Tick;
//! # struct TickHandler;
//! # #[cqrs::command_handler]
//! # impl CommandHandler for TickHandler {
//! #     async fn handle(&self, _: &Tick, _: &mut Appender<Ticked>) -> Result<(), StoreError> { Ok(()) }
//! # }
//! #[derive(cqrs::Gateway)]
//! #[gateway(commands)]
//! struct Commands { tick: TickHandler }
//!
//! let _ = Commands { tick: TickHandler }.send(Tick);
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
//! For each command the gateway hands the handler a fresh [`Appender`]
//! (with the command's metadata and the publisher): the handler finds what
//! it needs, lets the domain append, and commits explicitly. If a commit loses a race (a stream
//! moved on, or a [DCB condition](crate::AppendCondition) failed), the
//! gateway runs the handler again with a new appender, so it decides on
//! fresh state.

use std::future::Future;

use crate::{
    Aggregate, AppendCondition, Command, DomainEvent, ErrorOf, EventCriteria, EventEnvelope, EventStore,
    ExpectedVersion, Handler, Loaded, Message, Metadata, OutputOf, StoreError,
};

/// The context of one run of a command handler, handed in by the gateway
/// (Axon 5's `EventAppender` with its processing context): the events the
/// domain decided on, until [`commit`](Self::commit) takes them; the
/// metadata the command was sent with, which every committed event carries
/// (Axon's correlation data); the [`AppendCondition`] of what it read with
/// [`source`](Self::source) (Axon 5's DCB); and the publisher `P` its
/// commits go through, which the gateway was given
/// ([`WithPublisher::with_publisher`]).
///
/// Without a publisher (`P = ()`, what [`new`](Self::new) builds, e.g. in a
/// test of the domain) it still collects events, but `commit` does not
/// compile.
///
/// It is an [`EventAppender`](crate::EventAppender), so domain methods take
/// it as `&mut impl EventAppender<E>`.
#[derive(Debug)]
pub struct Appender<E, P = ()> {
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
    /// Where the commits go.
    publisher: P,
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
    /// An empty appender without metadata nor publisher (what a test
    /// passes).
    pub fn new() -> Self {
        Self::with_metadata(Metadata::new())
    }

    /// An empty appender whose events carry `metadata`, without publisher.
    pub fn with_metadata(metadata: Metadata) -> Self {
        Self::publishing(metadata, ())
    }
}

impl<E, P> Appender<E, P> {
    /// An empty appender whose events carry `metadata`, committing through
    /// `publisher`: what a gateway hands each run of a handler.
    pub fn publishing(metadata: Metadata, publisher: P) -> Self {
        Self {
            events: Vec::new(),
            metadata,
            condition: None,
            saved: false,
            lost_race: false,
            publisher,
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

    /// Commits the pending events to `entity`'s stream through the
    /// publisher (NestJS's `aggregate.commit()`): expecting the version it
    /// was loaded at, with the command's metadata and the conditions of what
    /// this run read.
    ///
    /// # Errors
    ///
    /// [`StoreError::Conflict`] if the entity changed since it was loaded,
    /// [`StoreError::ConditionFailed`] if what the run read changed: the
    /// gateway then runs the handler again, if nothing was committed before
    /// in this run.
    pub async fn commit<A>(&mut self, entity: Loaded<A>) -> Result<(), StoreError>
    where
        E: DomainEvent,
        A: Aggregate<Event = E>,
        P: EventPublisher<E>,
    {
        let pending = self.take();
        let outcome = self.publisher.commit(entity, pending).await;
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

impl<E, P> Extend<E> for Appender<E, P> {
    fn extend<I: IntoIterator<Item = E>>(&mut self, events: I) {
        self.events.extend(events);
    }
}

/// Persists and publishes what a command decided (NestJS's
/// `EventPublisher`): appends the pending events to the entity's stream,
/// expecting the version it was loaded at, with the command's metadata and
/// conditions, then hands them to whatever processes events. Handlers reach
/// it through their [`Appender`]: `events.commit(post)`.
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
/// its appender: `events.commit(entity)`.
///
/// The publisher is not the handler's: the gateway it is in hands it to
/// each run, so the handler is generic over it (`P`). `#[command_handler]`
/// writes that part: in the impl, `events: &mut Appender<Event>` is enough.
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
    fn handle<P: EventPublisher<Self::Event>>(
        &self,
        command: &Self::Command,
        events: &mut Appender<Self::Event, P>,
    ) -> impl Future<Output = Result<OutputOf<Self::Command>, ErrorOf<Self::Command>>> + Send;

    /// How many times the handler re-runs after losing a race (0: never).
    fn conflict_retries(&self) -> u32 {
        8
    }
}

/// Runs `handler` on `command`: a fresh appender per run, carrying the
/// command's `metadata` and committing through `publisher`, and a re-run
/// when a commit lost a race. What a command gateway does for each command.
///
/// # Errors
///
/// What the handler returns (after the retries, for a lost race).
pub async fn dispatch_command<H, X>(
    handler: &H,
    command: H::Command,
    metadata: Metadata,
    publisher: &X,
) -> Result<OutputOf<H::Command>, ErrorOf<H::Command>>
where
    H: CommandHandler,
    X: EventPublisher<H::Event>,
{
    let mut retries = handler.conflict_retries();
    loop {
        let mut events = Appender::publishing(metadata.clone(), publisher);
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

/// Routes message `M` to its command handler, handing it `publisher`: what
/// `#[derive(Gateway)]` implements for a gateway of commands
/// (`#[gateway(commands)]`). Such a gateway sends nothing until it has a
/// publisher: [`with_publisher`](WithPublisher::with_publisher) gives it
/// one, and the result is a [`Dispatch`](crate::Dispatch).
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no command handler for `{M}`",
    label = "nothing in this gateway handles `{M}`",
    note = "add a field whose type implements `CommandHandler<Command = {M}>`, in a `#[gateway(commands)]` struct"
)]
pub trait DispatchIn<M: Message, X>: Send + Sync {
    /// Hands `message` and `metadata` to the handler, committing through
    /// `publisher`.
    fn dispatch_in(
        &self,
        message: M,
        metadata: Metadata,
        publisher: &X,
    ) -> impl Future<Output = Result<M::Output, M::Error>> + Send;
}

/// An enum of commands, routed variant by variant through a gateway of
/// commands with a publisher. Implemented by `#[derive(Commands)]`.
pub trait DispatchInVia<G: ?Sized, X>: Message {
    /// Hands the variant's command, `metadata` and `publisher` to `gateway`,
    /// wrapping its output in the enum's output.
    fn dispatch_in_via(
        self,
        gateway: &G,
        metadata: Metadata,
        publisher: &X,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;
}

/// A gateway of commands (or one command handler) with the publisher its
/// handlers commit through. Built by
/// [`with_publisher`](WithPublisher::with_publisher).
pub struct Published<G, X> {
    /// The gateway or handler.
    inner: G,
    /// What its handlers commit through.
    publisher: X,
}

impl<G, X> Published<G, X> {
    /// `inner`, its handlers committing through `publisher`.
    pub fn new(inner: G, publisher: X) -> Self {
        Self { inner, publisher }
    }

    /// The gateway or handler.
    pub fn inner(&self) -> &G {
        &self.inner
    }

    /// The publisher.
    pub fn publisher(&self) -> &X {
        &self.publisher
    }
}

/// A gateway of commands with its publisher sends them.
impl<M: Message, G: DispatchIn<M, X>, X: Send + Sync> crate::Dispatch<M> for Published<G, X> {
    fn dispatch_with(
        &self,
        message: M,
        metadata: Metadata,
    ) -> impl Future<Output = Result<M::Output, M::Error>> + Send {
        self.inner.dispatch_in(message, metadata, &self.publisher)
    }
}

/// One command handler with its publisher is a handler: registered on a
/// [`Bus`](crate::Bus), or called directly.
impl<H: CommandHandler, X: EventPublisher<H::Event>> Handler for Published<H, X> {
    type Message = H::Command;

    fn handle(
        &self,
        command: H::Command,
    ) -> impl Future<Output = Result<OutputOf<H::Command>, ErrorOf<H::Command>>> + Send {
        dispatch_command(&self.inner, command, Metadata::new(), &self.publisher)
    }

    fn handle_with(
        &self,
        command: H::Command,
        metadata: Metadata,
    ) -> impl Future<Output = Result<OutputOf<H::Command>, ErrorOf<H::Command>>> + Send {
        dispatch_command(&self.inner, command, metadata, &self.publisher)
    }
}

/// Gives a gateway of commands (or a command handler) the publisher its
/// handlers commit through: constructors take only the services handlers
/// need, and the publisher comes once, here (what a
/// [`Cqrs`](crate::Cqrs) unit does with its own).
pub trait WithPublisher: Sized {
    /// `self`, committing through `publisher`.
    fn with_publisher<X>(self, publisher: X) -> Published<Self, X> {
        Published::new(self, publisher)
    }
}

impl<T> WithPublisher for T {}
