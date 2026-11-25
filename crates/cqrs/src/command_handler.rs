//! Command handlers and their unit of work (Axon 5's command handling with
//! its `ProcessingContext`): the handler decides, records what it decided,
//! and the gateway commits.
//!
//! The gateway is the handler: one struct holding the services its commands
//! need (usually the application's own repository), with one
//! `CommandHandler<C>` impl per command. `#[command_handler]` reads the
//! command, the event type and the unit of work from the signature of
//! `handle`; `#[command_gateway]` routes the commands it lists to those impls,
//! each in a unit of work of its `#[gateway(unit)]` field:
//!
//! ```
//! use std::future::Future;
//! use cqrs::{aggregate::{AggregateLoader, EventAppender, Loaded, New}, command::{CommandContext, CommandGateway, CommandHandler}, processing::InlinePublisher, store::{InMemoryEventStore, StoreError}, testing::block_on};
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
//! /// The gateway, and the unit of work its commands run in.
//! #[cqrs::command_gateway(handles(Add))]
//! struct CounterCommands<R, P> {
//!     counters: R,
//!     #[gateway(unit)]
//!     events: P,
//! }
//!
//! #[cqrs::command_handler]
//! impl<R: Counters, P> CommandHandler<Add> for CounterCommands<R, P> {
//!     async fn handle(&self, command: &Add, ctx: &mut CommandContext<CounterEvent>) -> Result<u64, StoreError> {
//!         // A counter starts at zero: adding to a missing one creates it.
//!         let counter = self.counters.find(command.id).await?.unwrap_or_else(|| New(command.id).into());
//!         ctx.append(CounterEvent::Added(command.n));
//!         let counter = ctx.record(counter); // the counter after its events
//!         Ok(counter.total)
//!     }
//! }
//!
//! let store = InMemoryEventStore::new();
//! let commands = CounterCommands {
//!     counters: StoredCounters(AggregateLoader::new(store.clone())),
//!     events: InlinePublisher::new(store),
//! };
//! assert_eq!(block_on(commands.send(Add { id: 1, n: 2 }))?, 2);
//! assert_eq!(block_on(commands.send(Add { id: 1, n: 3 }))?, 5);
//! # Ok::<(), StoreError>(())
//! ```
//!
//! # The unit of work
//!
//! For each command the gateway hands the handler a fresh [`CommandContext`]
//! (carrying the command's metadata) and, if the handler asks for one, the
//! transaction of the unit of work:
//!
//! - **Without a transaction** (`handle(&self, command, events)`, the mode
//!   [`Autocommit`]): what the handler recorded is appended in one statement
//!   of the store, once the handler returned `Ok`. One entity per command.
//! - **With one** (`handle(&self, command, events, tx: &mut Tx)`, the mode
//!   [`InTransaction<Tx>`]): the unit opens the transaction of its
//!   [`TransactionManager`](crate::TransactionManager), the handler and its
//!   repositories write in it (an entity's state, rows of their own), the
//!   recorded events are appended in it last, and the unit commits.
//!   Everything commits together, or nothing does.
//!
//! The handler never commits, and never announces: once committed, the unit
//! hands the events to the processors. If the append lost a race (a stream
//! moved on, or a [DCB condition](crate::AppendCondition) failed), the unit
//! is rolled back and the handler runs again on fresh state. So, besides
//! what it writes in the transaction, a handler must have no side effects:
//! one runs in a projection, after the commit.
//!
//! The command is taken by reference, as the handler may run again:
//!
//! ```compile_fail
//! # use cqrs::command::{CommandContext, CommandHandler};
//! # #[cqrs::command(error = ())] struct Ping;
//! struct PingHandler;
//! #[cqrs::command_handler]
//! impl CommandHandler<Ping> for PingHandler {
//!     async fn handle(&self, command: Ping, events: &mut CommandContext<()>) -> Result<(), ()> { Ok(()) }
//! }
//! ```
//!
//! and an entity is recorded with events of its own type only:
//!
//! ```compile_fail,E0271
//! # use cqrs::{aggregate::New, command::{CommandContext, CommandHandler}, store::StoreError};
//! # #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)] enum Ticked { Once }
//! # #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)] enum Rang { Once }
//! # #[cqrs::aggregate(event = Rang)] struct Bell { id: u32 }
//! # impl Bell { fn apply(&mut self, _: &Rang) {} }
//! # #[cqrs::command(error = StoreError)] struct Tick;
//! struct TickHandler;
//! #[cqrs::command_handler]
//! impl CommandHandler<Tick> for TickHandler {
//!     async fn handle(&self, _: &Tick, events: &mut CommandContext<Ticked>) -> Result<(), StoreError> {
//!         events.record(New::<Bell>(1)); // a bell rings, it does not tick
//!         Ok(())
//!     }
//! }
//! ```
//!
//! A gateway routes what it lists in `handles(...)`, and only that: a
//! command listed without its handler impl does not compile, where the
//! gateway is declared,
//!
//! ```compile_fail,E0277
//! # use cqrs::{command::{CommandContext, CommandHandler}, store::StoreError};
//! # #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)] enum Ticked { Once }
//! # #[cqrs::command(error = StoreError)] struct Tick;
//! # #[cqrs::command(error = StoreError)] struct Ring;
//! #[cqrs::command_gateway(handles(Tick, Ring))]
//! struct Clock<P> {
//!     #[gateway(unit)]
//!     events: P,
//! }
//!
//! #[cqrs::command_handler]
//! impl<P> CommandHandler<Tick> for Clock<P> {
//!     async fn handle(&self, _: &Tick, _: &mut CommandContext<Ticked>) -> Result<(), StoreError> { Ok(()) }
//! }
//! // No `CommandHandler<Ring>`.
//! ```
//!
//! and a command gateway needs the unit of work its commands run in:
//!
//! ```compile_fail
//! # #[cqrs::command(error = cqrs::store::StoreError)] struct Tick;
//! #[cqrs::command_gateway(handles(Tick))]
//! struct Clock; // no `#[gateway(unit)]` field
//! ```

use std::{future::Future, marker::PhantomData};

use crate::{
    Aggregate, AppendCondition, Command, DomainEvent, ErrorOf, EventCriteria, EventEnvelope, ExpectedVersion, Loaded,
    Metadata, OutputOf, SourceEvents, StoreError, StreamId,
};

/// The context of one run of a command handler, handed in by the gateway
/// (Axon 5's `EventAppender` with its processing context): the events the
/// domain decides on, until [`record`](Self::record) assigns them to an
/// entity; the metadata the command was sent with, which every event
/// carries (Axon's correlation data); and the [`AppendCondition`] of what
/// the run read with [`source`](Self::source) (Axon 5's DCB). The unit of
/// work appends what was recorded once the handler returned.
///
/// It is an [`EventAppender`](crate::EventAppender), so domain methods take
/// it as `&mut impl EventAppender<E>`.
#[derive(Debug)]
pub struct CommandContext<E> {
    /// Appended and not recorded yet.
    events: Vec<E>,
    /// Sent with the command; stored with every event.
    metadata: Metadata,
    /// What the run read by criteria, checked by every write.
    condition: Option<AppendCondition>,
    /// What the run decided, entity by entity, in order.
    writes: Vec<Write<E>>,
}

/// What one commit stores, for [`EventPublisher::commit`]: events, their
/// metadata and a condition.
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

/// The events recorded for one entity by a command handler: what its unit
/// of work appends.
#[derive(Debug)]
pub struct Write<E> {
    /// The entity's stream.
    pub stream: StreamId,
    /// What the stream must be at.
    pub expected: ExpectedVersion,
    /// What must still hold (what the run, or the entity, was read from).
    pub condition: Option<AppendCondition>,
    /// The events, in order.
    pub events: Vec<E>,
    /// Stored with every event.
    pub metadata: Metadata,
}

impl<E> CommandContext<E> {
    /// An empty context without metadata (what a test passes).
    pub fn new() -> Self {
        Self::with_metadata(Metadata::new())
    }

    /// An empty context whose events carry `metadata`: what a gateway hands
    /// each run of a handler.
    pub fn with_metadata(metadata: Metadata) -> Self {
        Self {
            events: Vec::new(),
            metadata,
            condition: None,
            writes: Vec::new(),
        }
    }

    /// Appends `event` to the decision, until [`record`](Self::record)
    /// assigns it to an entity. The same as
    /// [`EventAppender::append`](crate::EventAppender::append), without
    /// importing the trait (domain methods take `&mut impl EventAppender<E>`).
    pub fn append(&mut self, event: E) {
        self.events.push(event);
    }

    /// The metadata the command was sent with.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// The events appended and not recorded yet.
    pub fn pending(&self) -> &[E] {
        &self.events
    }

    /// What was recorded so far, entity by entity.
    pub fn writes(&self) -> &[Write<E>] {
        &self.writes
    }

    /// What the writes of this run are conditioned on, if it read anything
    /// by criteria.
    pub fn condition(&self) -> Option<&AppendCondition> {
        self.condition.as_ref()
    }

    /// Reads every event matching `criteria` from `store` (any stream) and
    /// makes this run's writes conditional on nothing matching being
    /// appended meanwhile. Several reads combine: the writes check all of
    /// them.
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

    /// Makes this run's writes also check `condition`, e.g. the one of an
    /// aggregate read but not written.
    pub fn depend_on(&mut self, condition: AppendCondition) {
        self.condition = AppendCondition::both(self.condition.take(), Some(condition));
    }

    /// Records the events appended since the last call as `entity`'s
    /// (Axon 5's `EventAppender` with its entity): they are appended to its
    /// stream by the unit of work, expecting the version it was loaded at
    /// (or, for a [`New`](crate::New) one, that it does not exist), with the
    /// command's metadata and the conditions of what the run read.
    ///
    /// Pure: nothing is written yet. Returns the entity as it is after the
    /// events ([`Aggregate::evolve`]), at the version they take it to: what
    /// a repository that keeps state saves, in the unit's transaction.
    pub fn record<A>(&mut self, entity: impl Into<Loaded<A>>) -> Loaded<A>
    where
        E: DomainEvent,
        A: Aggregate<Event = E>,
    {
        let mut entity = entity.into();
        let events = std::mem::take(&mut self.events);
        if events.is_empty() {
            return entity;
        }
        let (stream, expected) = entity.target();
        let condition = AppendCondition::both(entity.condition().cloned(), self.condition.clone());
        entity.apply(&events);
        self.writes.push(Write {
            stream,
            expected,
            condition,
            events,
            metadata: self.metadata.clone(),
        });
        entity
    }

    /// The writes of this run, leaving it empty: what a unit of work
    /// appends ([`append_writes`]), when one is run by hand.
    pub fn take_writes(&mut self) -> Vec<Write<E>> {
        std::mem::take(&mut self.writes)
    }
}

impl<E> Default for CommandContext<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> Extend<E> for CommandContext<E> {
    fn extend<I: IntoIterator<Item = E>>(&mut self, events: I) {
        self.events.extend(events);
    }
}

/// Appends and publishes events outside a command's unit of work (an
/// import, a test seeding a store): appends `pending` to the entity's
/// stream, expecting the version it was loaded at, then hands the events to
/// whatever processes them. Command handlers do not use it: they record, and
/// their unit of work ([`UnitOfWork`]) appends.
///
/// [`Events`](crate::Events) is one (it broadcasts the commit), and so is
/// an [`InlinePublisher`](crate::InlinePublisher) (it feeds its processing
/// group inline).
pub trait EventPublisher<E: DomainEvent>: Send + Sync {
    /// Commits `pending` to `entity`'s stream; returns the position of the
    /// last event committed (`None` if there was nothing to commit).
    ///
    /// # Errors
    ///
    /// [`StoreError::Conflict`] or [`StoreError::ConditionFailed`] if the
    /// entity changed; the store's error otherwise.
    fn commit<A: Aggregate<Event = E>>(
        &self,
        entity: Loaded<A>,
        pending: Pending<E>,
    ) -> impl Future<Output = Result<Option<u64>, StoreError>> + Send;
}

/// A borrowed publisher is a publisher.
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

/// How a command's unit of work runs: [`Autocommit`] or
/// [`InTransaction<Tx>`], chosen by the handler's signature.
pub trait UnitMode: Send {
    /// What the handler borrows (`()` without a transaction).
    type Tx: Send;
    /// Whether the store appends on its own, one statement per command.
    const AUTOCOMMIT: bool;
}

/// No transaction: the handler takes none, and what it recorded is appended
/// by the store on its own, in one statement. The fastest path, for one
/// entity per command (an event-sourced repository).
#[derive(Clone, Copy, Debug)]
pub struct Autocommit;

impl UnitMode for Autocommit {
    type Tx = ();
    const AUTOCOMMIT: bool = true;
}

/// A transaction of type `Tx`, opened and committed by the unit of work,
/// borrowed by the handler and its repositories (`tx: &mut Tx`): their
/// writes and the events commit together.
pub struct InTransaction<Tx>(PhantomData<fn() -> Tx>);

impl<Tx> ::std::fmt::Debug for InTransaction<Tx> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("InTransaction").finish_non_exhaustive()
    }
}

impl<Tx: Send> UnitMode for InTransaction<Tx> {
    type Tx = Tx;
    const AUTOCOMMIT: bool = false;
}

/// Runs commands' units of work for events `E` in mode `U`: what a command
/// gateway holds (`#[gateway(unit)]`). [`Events`](crate::Events) runs both
/// modes (in a transaction, its
/// [`TransactionManager`](crate::TransactionManager)'s);
/// [`InlinePublisher`](crate::InlinePublisher) runs them in memory, feeding
/// its handlers after each.
pub trait UnitOfWork<E: DomainEvent, U: UnitMode>: Send + Sync {
    /// Whether dropping the transaction undoes the appends made in it. In
    /// memory it does not: after a lost race, the handler of a unit that
    /// already appended something is not run again (the conflict reaches
    /// the caller), so nothing is appended twice.
    const ROLLS_BACK: bool = true;

    /// Opens the unit's transaction (`()` in [`Autocommit`]).
    fn begin(&self) -> impl Future<Output = Result<U::Tx, StoreError>> + Send;

    /// Appends `write` in `tx`; returns the events' envelopes.
    fn append(
        &self,
        tx: &mut U::Tx,
        write: Write<E>,
    ) -> impl Future<Output = Result<Vec<EventEnvelope<E>>, StoreError>> + Send;

    /// Commits `tx` (nothing to do in [`Autocommit`]).
    fn commit(&self, tx: U::Tx) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Hands `events`, committed, to whatever follows them (processors,
    /// subscriptions, inline handlers).
    fn announce(&self, events: Vec<EventEnvelope<E>>) -> impl Future<Output = ()> + Send;
}

/// A shared unit of work is one.
// Forwards every method, so an override of a provided one is never lost.
#[deny(clippy::missing_trait_methods)]
impl<E: DomainEvent, U: UnitMode, T: UnitOfWork<E, U>> UnitOfWork<E, U> for std::sync::Arc<T> {
    const ROLLS_BACK: bool = T::ROLLS_BACK;

    fn begin(&self) -> impl Future<Output = Result<U::Tx, StoreError>> + Send {
        (**self).begin()
    }

    fn append(
        &self,
        tx: &mut U::Tx,
        write: Write<E>,
    ) -> impl Future<Output = Result<Vec<EventEnvelope<E>>, StoreError>> + Send {
        (**self).append(tx, write)
    }

    fn commit(&self, tx: U::Tx) -> impl Future<Output = Result<(), StoreError>> + Send {
        (**self).commit(tx)
    }

    fn announce(&self, events: Vec<EventEnvelope<E>>) -> impl Future<Output = ()> + Send {
        (**self).announce(events)
    }
}

/// Handles command `C`: finds what it needs (through the application's
/// repositories), lets the domain append events, and records them for the
/// entity they change (`events.record(entity)`). Its unit of work commits.
///
/// The command is a generic parameter: one struct (the gateway,
/// `#[cqrs::command_gateway]`) handles several commands, one impl each,
/// sharing its dependencies.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not handle `{C}`",
    note = "implement `CommandHandler<{C}>` for it, usually with `#[cqrs::command_handler]`"
)]
pub trait CommandHandler<C: Command + Sync>: Send + Sync {
    /// The events it decides on.
    type Event: DomainEvent;
    /// Its unit of work: [`Autocommit`], or [`InTransaction<Tx>`] when
    /// `handle` takes a `tx: &mut Tx` (filled in by `#[command_handler]`).
    type Unit: UnitMode;

    /// Decides, and records what it decided. Events appended but never
    /// recorded when it returns `Ok` are a bug (a forgotten `record`):
    /// debug builds panic.
    ///
    /// It may run again, with a fresh context and transaction, if the
    /// append lost a race (see [`conflict_retries`](Self::conflict_retries)):
    /// besides writing in `tx`, it must have no side effects.
    fn handle(
        &self,
        command: &C,
        events: &mut CommandContext<Self::Event>,
        tx: &mut <Self::Unit as UnitMode>::Tx,
    ) -> impl Future<Output = Result<OutputOf<C>, ErrorOf<C>>> + Send;

    /// How many times the handler re-runs after losing a race (0: never).
    fn conflict_retries(&self) -> u32 {
        8
    }
}

/// Whether `error` is a lost race: running the decision again on fresh
/// state may succeed. Creating what exists already is not one.
fn lost_race(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::ConditionFailed
            | StoreError::Conflict {
                expected: ExpectedVersion::Exact(_),
                ..
            }
    )
}

/// Runs `handler` on `command` in a unit of work of `unit`: opens it, runs
/// the handler with a fresh context (carrying the command's `metadata`)
/// and the unit's transaction, appends what it recorded, commits, then
/// announces. An error rolls the unit back; a lost race also runs the
/// handler again. What a command gateway does for each command, and how a
/// handler is called directly.
///
/// # Errors
///
/// What the handler returns, or the store's error (converted), after the
/// retries for a lost race. In [`Autocommit`], recording more than one
/// entity is an error: their appends would not be atomic.
pub async fn dispatch_command<C, H, U>(
    handler: &H,
    command: C,
    metadata: Metadata,
    unit: &U,
) -> Result<OutputOf<C>, ErrorOf<C>>
where
    C: Command + Sync,
    H: CommandHandler<C>,
    U: UnitOfWork<H::Event, H::Unit>,
    ErrorOf<C>: From<StoreError>,
{
    let mut retries = handler.conflict_retries();
    loop {
        let mut events = CommandContext::with_metadata(metadata.clone());
        let mut tx = unit.begin().await?;
        let output = handler.handle(&command, &mut events, &mut tx).await?;
        debug_assert!(
            events.pending().is_empty(),
            "{} returned Ok with events it never recorded",
            std::any::type_name::<H>(),
        );
        let writes = events.take_writes();
        if <H::Unit as UnitMode>::AUTOCOMMIT && writes.len() > 1 {
            return Err(StoreError::backend(format!(
                "{} recorded {} entities without a transaction: their appends would not be atomic; \
                 take the unit's transaction in `handle` (`tx: &mut Tx`)",
                std::any::type_name::<H>(),
                writes.len(),
            ))
            .into());
        }
        // A unit that cannot roll back keeps a write that went through
        // before a lost race: running the handler again would append it twice.
        let rerunnable = U::ROLLS_BACK || writes.len() <= 1;
        let appended = match append_writes(unit, &mut tx, writes).await {
            Ok(appended) => appended,
            // Dropping `tx` rolls back what the handler wrote in it.
            Err(error) if lost_race(&error) && rerunnable && retries > 0 => {
                retries -= 1;
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        unit.commit(tx).await?;
        unit.announce(appended).await;
        return Ok(output);
    }
}

/// Appends `writes` through `unit`, in `tx`, in order: what a unit of work
/// does with what a handler recorded. A write's condition skips the events
/// the writes before it appended: the run's own events match what it read,
/// so a later write checks only what others appended.
///
/// # Errors
///
/// The first write's that failed; what was appended before it is in `tx`,
/// which the caller then drops (rolling it back, in a database).
pub async fn append_writes<E, U, W>(
    unit: &W,
    tx: &mut U::Tx,
    writes: Vec<Write<E>>,
) -> Result<Vec<EventEnvelope<E>>, StoreError>
where
    E: DomainEvent,
    U: UnitMode,
    W: UnitOfWork<E, U>,
{
    let mut appended: Vec<EventEnvelope<E>> = Vec::new();
    for mut write in writes {
        if let (Some(condition), Some(last)) = (&mut write.condition, appended.last()) {
            condition.after = condition.after.max(last.position);
        }
        appended.extend(unit.append(tx, write).await?);
    }
    Ok(appended)
}
