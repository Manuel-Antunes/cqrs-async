//! Loading aggregates: [`AggregateLoader`] rebuilds them from their events
//! (from a snapshot, if the application keeps any), and [`Loaded`] is what it
//! returns: the aggregate with the version it was read at. [`New`] is the
//! other target of a commit: an aggregate whose stream does not exist yet.
//!
//! It is the only way to rebuild an aggregate from its events, and the
//! engine hands it out (`events.repository::<A>()`), with its store, the
//! snapshots it can keep and its transaction: there is no repository to
//! write for an event-sourced aggregate. In a command handler it is a
//! [`Repository`] (`find` and `save`); an application whose port adds
//! methods (a next id) implements that port for it. A loader put together by
//! hand (`AggregateLoader::new(store)`) is for tests, tools, and reading a
//! log without its engine.

use std::{fmt, marker::PhantomData, ops::Deref, time::Instant};

use crate::{
    Aggregate, AppendCondition, CommandContext, EventCriteria, EventEnvelope, ExpectedVersion, KeepSnapshots,
    LoadStream, Repository, SnapshotPolicy, SnapshotStore, Snapshotting, SourceEvents, StoreError, StreamId,
};
#[derive(Debug)]

/// An aggregate together with the version it was loaded at.
///
/// The version lives here, not in the aggregate: the domain never sees
/// persistence concerns. It derefs to `&A` only: the only way to change the
/// aggregate is to save events, which [`Aggregate::evolve`] applies.
pub struct Loaded<A> {
    /// The aggregate, reachable through `Deref`.
    aggregate: A,
    /// The stream's last sequence when loaded (`None`: not stored yet). It is
    /// the expected version of the next save.
    version: Option<u64>,
    /// For an aggregate built from [`Aggregate::criteria`]: what it was
    /// built from, checked by its saves instead of the version.
    condition: Option<AppendCondition>,
}

impl<A: Aggregate> Loaded<A> {
    /// `id` built from the events matching its criteria (`None` if there
    /// were none), consistent through `condition`.
    pub(crate) fn sourced(id: &A::Id, events: &[EventEnvelope<A::Event>], condition: AppendCondition) -> Option<Self> {
        if events.is_empty() {
            return None;
        }
        let mut aggregate = A::initial(id);
        for event in events {
            aggregate.evolve(&event.payload);
        }
        Some(Self {
            aggregate,
            version: None,
            condition: Some(condition),
        })
    }

    /// `aggregate` as stored at `version`, its stream's last sequence: what a
    /// repository that keeps the aggregate's state (rather than rebuilding
    /// it from events) hands a commit, so the append still expects the
    /// stream at that version (optimistic concurrency). Version 0 is a
    /// stream with no events: the commit then expects it not to exist.
    pub fn at(aggregate: A, version: u64) -> Self {
        Self {
            aggregate,
            version: (version > 0).then_some(version),
            condition: None,
        }
    }

    /// What a commit of this aggregate's events needs: its stream, what it
    /// expects of it (built from criteria, its condition guards it, not a
    /// stream version, as other streams hold its events too), and that
    /// condition.
    pub(crate) fn into_commit(self) -> (StreamId, ExpectedVersion, Option<AppendCondition>) {
        let (stream, expected) = self.target();
        (stream, expected, self.condition)
    }

    /// The stream a commit appends to and what it expects of it (see
    /// [`into_commit`](Self::into_commit)).
    pub(crate) fn target(&self) -> (StreamId, ExpectedVersion) {
        let expected = match (&self.condition, self.version) {
            (Some(_), _) => ExpectedVersion::Any,
            (None, None) => ExpectedVersion::NoStream,
            (None, Some(version)) => ExpectedVersion::Exact(version),
        };
        (StreamId::of::<A>(self.aggregate.id()), expected)
    }

    /// The condition its commits are guarded by, if it was built from
    /// criteria.
    pub(crate) fn condition(&self) -> Option<&AppendCondition> {
        self.condition.as_ref()
    }

    /// This aggregate once `events` (recorded for its stream, not appended
    /// yet) are applied, at the version they take it to. One built from
    /// criteria keeps no version: its condition guards it.
    pub(crate) fn apply(&mut self, events: &[A::Event]) {
        for event in events {
            self.aggregate.evolve(event);
        }
        if self.condition.is_none() {
            self.version = Some(self.version.unwrap_or(0) + events.len() as u64);
        }
    }

    /// The stream version this aggregate was loaded at (`None` when new).
    pub fn version(&self) -> Option<u64> {
        self.version
    }

    /// Unwraps the aggregate, dropping the version: it can no longer be
    /// saved.
    pub fn into_inner(self) -> A {
        self.aggregate
    }
}

/// The aggregate `id`, not stored yet: what a creation commits to.
///
/// A creation is decided without an instance, by an associated function of
/// the aggregate, and recorded
/// for `New`, which expects the stream not to exist:
///
/// ```
/// # use cqrs::{aggregate::{EventAppender, New}, command::{CommandContext, Autocommit, append_writes}, processing::InlinePublisher, store::{InMemoryEventStore, StoreError}, testing::block_on};
/// # #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
/// # enum DoorEvent { Installed { id: u32 } }
/// # #[cqrs::aggregate(event = DoorEvent)]
/// # struct Door { id: u32 }
/// # impl Door { fn apply(&mut self, _: &DoorEvent) {} }
/// impl Door {
///     /// Decided on nothing: there is no door yet.
///     fn install(id: u32, events: &mut impl EventAppender<DoorEvent>) {
///         events.append(DoorEvent::Installed { id });
///     }
/// }
///
/// # block_on(async {
/// let publisher = InlinePublisher::new(InMemoryEventStore::new());
/// // What a command's unit of work does, by hand: commit, then append.
/// let mut events = CommandContext::new();
/// Door::install(1, &mut events);
/// events.commit(New::<Door>(1));
/// append_writes::<_, Autocommit, _>(&publisher, &mut (), events.take_writes()).await?;
///
/// // Once more: the stream exists now.
/// Door::install(1, &mut events);
/// events.commit(New::<Door>(1));
/// let again = append_writes::<_, Autocommit, _>(&publisher, &mut (), events.take_writes()).await;
/// assert!(matches!(again, Err(StoreError::Conflict { .. })));
/// # Ok::<(), StoreError>(())
/// # }).unwrap();
/// ```
///
/// A decision that creates the aggregate if it is missing (a counter that
/// starts at zero) runs on its state before any event, which `New` converts
/// into: `repository.find(&id, ctx.tx()).await?.unwrap_or_else(|| New(id).into())`. The
/// commit then expects the stream not to exist when it was not found.
pub struct New<A: Aggregate>(pub A::Id);

impl<A: Aggregate> From<New<A>> for Loaded<A> {
    /// The aggregate in its state before any event ([`Aggregate::initial`]),
    /// expecting its stream not to exist.
    fn from(New(id): New<A>) -> Self {
        Self {
            aggregate: A::initial(&id),
            version: None,
            condition: None,
        }
    }
}

impl<A: Aggregate> Clone for New<A> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<A: Aggregate> fmt::Debug for New<A>
where
    A::Id: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("New").field(&self.0).finish()
    }
}

impl<A> Deref for Loaded<A> {
    type Target = A;

    fn deref(&self) -> &A {
        &self.aggregate
    }
}

/// Rebuilds the aggregates `A` stored in `S`: from the newest snapshot if
/// there is one, then the events after it. Statically typed over the store
/// `S` and the snapshot store `N` (`()`: no snapshots).
///
/// ```text
/// let posts = events.repository::<Post>();   // from the engine
/// let post = posts.load(&id).await?;          // in a command handler: `posts.find(&id, ctx.tx())`
/// ```
///
/// With [`with_snapshots`](Self::with_snapshots), a load that reaches the
/// [`SnapshotPolicy`] (it had to replay enough events, or took long enough)
/// saves a snapshot, so the next one replays only what came after it;
/// commits do not snapshot (the publisher serves every aggregate of an event
/// type, the loader is the one place that knows one, and a snapshot taken at
/// commit would be of events a rolled back unit never stored).
///
/// `Tx` is the transaction of the units of work whose handlers use it as their
/// [`Repository`](crate::Repository): it reads outside the transaction and
/// writes nothing in it, so any transaction does (`()`: none, which
/// [`new`](Self::new) gives; [`in_transaction`](Self::in_transaction) names
/// another).
pub struct AggregateLoader<A, S, N = (), Tx = ()> {
    /// Where the event streams live.
    store: S,
    /// Saved states, so a load replays only the tail (`()`: none).
    snapshots: N,
    /// When a load keeps a snapshot (`None`: never).
    policy: Option<SnapshotPolicy>,
    /// Ties the loader to one aggregate type without storing one.
    aggregate: PhantomData<fn() -> A>,
    /// Ties the loader to the transaction of the units it takes part in,
    /// which it only passes along.
    transaction: PhantomData<fn() -> Tx>,
}

impl<A, S, N, Tx> ::std::fmt::Debug for AggregateLoader<A, S, N, Tx> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("AggregateLoader").finish_non_exhaustive()
    }
}

impl<A, S> AggregateLoader<A, S> {
    /// A loader over `store`, without snapshots, for units of work without a
    /// transaction.
    pub fn new(store: S) -> Self {
        Self {
            store,
            snapshots: (),
            policy: None,
            aggregate: PhantomData,
            transaction: PhantomData,
        }
    }
}

/// The loader of the aggregate `A` an engine hands out: over a handle of its
/// event store `store`, with the snapshots that store keeps for `A` and the
/// policy `A` declares.
pub(crate) fn declared_by_the_aggregate<A, S, Tx>(
    store: &S,
) -> AggregateLoader<A, S, <S as KeepSnapshots<A, A::Mode>>::Snapshots, Tx>
where
    A: Snapshotting,
    S: Clone + KeepSnapshots<A, A::Mode>,
{
    AggregateLoader {
        store: store.clone(),
        snapshots: store.snapshots(),
        policy: A::POLICY,
        aggregate: PhantomData,
        transaction: PhantomData,
    }
}

impl<A, S, N, Tx> AggregateLoader<A, S, N, Tx> {
    /// The same loader for the units of work whose transaction is `Tx2`: it
    /// reads outside it, so it takes part in them as it does in any other.
    pub fn in_transaction<Tx2>(self) -> AggregateLoader<A, S, N, Tx2> {
        AggregateLoader {
            store: self.store,
            snapshots: self.snapshots,
            policy: self.policy,
            aggregate: PhantomData,
            transaction: PhantomData,
        }
    }

    /// Keeps a snapshot in `snapshots` whenever a load reaches `policy` (a
    /// [`SnapshotPolicy`], or a number of events: `10` keeps one whenever a
    /// load replayed 10 events or more, so loading replays fewer than 10
    /// events after the newest one). Worth it for long-lived aggregates; the
    /// stream stays the source of truth.
    ///
    /// This is the loader put together by hand. An aggregate declares its
    /// own policy (`#[cqrs::aggregate(.., snapshot(..))]`), and the engine's
    /// `repository::<A>()` builds the loader from it.
    ///
    /// # Panics
    ///
    /// If `policy` is zero events.
    pub fn with_snapshots<N2>(self, snapshots: N2, policy: impl Into<SnapshotPolicy>) -> AggregateLoader<A, S, N2, Tx> {
        AggregateLoader {
            store: self.store,
            snapshots,
            policy: Some(policy.into()),
            aggregate: PhantomData,
            transaction: PhantomData,
        }
    }

    /// The underlying event store, e.g. to read every event in order.
    pub fn store(&self) -> &S {
        &self.store
    }
}

impl<A, S, N, Tx> AggregateLoader<A, S, N, Tx>
where
    A: Aggregate,
    S: LoadStream<A::Event> + SourceEvents<A::Event>,
    N: SnapshotStore<A>,
{
    /// Rebuilds the aggregate `id`, `None` if it has no events. Replay calls
    /// [`Aggregate::evolve`] directly, so it never records pending events.
    ///
    /// An aggregate built from [`criteria`](Aggregate::criteria) is read by
    /// them instead, and its commits are conditioned on what it read.
    ///
    /// # Errors
    ///
    /// The store's.
    pub async fn load(&self, id: &A::Id) -> Result<Option<Loaded<A>>, StoreError> {
        match A::criteria(id) {
            Some(criteria) => Ok(self.source_by(id, criteria).await?.0),
            None => self.load_stream(id).await,
        }
    }

    /// [`load`](Self::load) within a run of a command handler: for an
    /// aggregate built from [`criteria`](Aggregate::criteria), the writes of
    /// the run then fail if anything it was built from changes, **even if it
    /// did not exist** (absence is a fact a decision can rely on too), and
    /// even if the run never saves it. What a decision only reads, or
    /// creates if missing, is loaded this way; for an aggregate that is its
    /// stream, it is `load`.
    ///
    /// # Errors
    ///
    /// The store's.
    pub async fn load_in<Tx2>(
        &self,
        id: &A::Id,
        ctx: &mut CommandContext<A::Event, Tx2>,
    ) -> Result<Option<Loaded<A>>, StoreError> {
        let Some(criteria) = A::criteria(id) else {
            return self.load_stream(id).await;
        };
        let (loaded, condition) = self.source_by(id, criteria).await?;
        ctx.depend_on(condition);
        Ok(loaded)
    }

    /// Reads every event matching `criteria`, from any stream of the store,
    /// and makes the following commits of `events` conditional on them (see
    /// [`CommandContext::source`]).
    ///
    /// # Errors
    ///
    /// The store's.
    pub async fn source(
        &self,
        criteria: EventCriteria,
        events: &mut CommandContext<A::Event, Tx>,
    ) -> Result<Vec<EventEnvelope<A::Event>>, StoreError> {
        events.source(&self.store, criteria).await
    }

    /// The aggregate built from the events matching `criteria` (`None` if
    /// there were none), and the condition its commits are guarded by.
    async fn source_by(
        &self,
        id: &A::Id,
        criteria: EventCriteria,
    ) -> Result<(Option<Loaded<A>>, AppendCondition), StoreError> {
        let (events, head) = self.store.source(&criteria).await?;
        let condition = AppendCondition { criteria, after: head };
        Ok((Loaded::sourced(id, &events, condition.clone()), condition))
    }

    /// The aggregate of the stream `id`: from its newest snapshot, if any,
    /// then the events after it.
    async fn load_stream(&self, id: &A::Id) -> Result<Option<Loaded<A>>, StoreError> {
        // The clock is read only for a policy that has a time threshold.
        let started = self
            .policy
            .as_ref()
            .is_some_and(SnapshotPolicy::times_sourcing)
            .then(Instant::now);
        // A failing snapshot store only costs a full replay, but one that
        // keeps failing costs it on every load: say so.
        let snapshot = self.snapshots.load(id).await.unwrap_or_else(|error| {
            tracing::warn!(aggregate = A::TYPE, %error, "cannot read the snapshot, replaying the whole stream");
            None
        });
        let (mut aggregate, mut version) = snapshot.unwrap_or_else(|| (A::initial(id), 0));
        let replayed = self
            .store
            .replay(&StreamId::of::<A>(id), version, |sequence, event| {
                aggregate.evolve(&event);
                version = sequence;
            })
            .await?;
        if version == 0 && replayed == 0 {
            return Ok(None);
        }
        let sourcing = started.map(|started| started.elapsed()).unwrap_or_default();
        // Nothing replayed, nothing new to keep, however long the load took.
        if replayed > 0 && self.policy.is_some_and(|policy| policy.reached(replayed, sourcing)) {
            // A failed snapshot only means a longer replay next time.
            if let Err(error) = self.snapshots.save(&aggregate, version).await {
                tracing::warn!(aggregate = A::TYPE, version, %error, "cannot keep the snapshot");
            }
        }
        Ok(Some(Loaded {
            aggregate,
            version: Some(version),
            condition: None,
        }))
    }
}

/// The loader is a [`Repository`] of `A` for units of work of any transaction
/// `Tx`: it reads outside it and writes nothing in it, as the events of an
/// aggregate are its state.
///
/// `find` is [`load`](AggregateLoader::load): the loader reads outside the
/// transaction it is lent (optimistic concurrency guards the append) and
/// persists nothing in it, the events being the state. An aggregate built from
/// [`criteria`](Aggregate::criteria) that a decision reads without saving
/// it, or that may not exist yet, is read with
/// [`load_in`](AggregateLoader::load_in) instead, which takes the context.
impl<A, S, N, Tx> Repository<A> for AggregateLoader<A, S, N, Tx>
where
    A: Aggregate,
    S: LoadStream<A::Event> + SourceEvents<A::Event>,
    N: SnapshotStore<A>,
    Tx: Send,
{
    type Tx = Tx;

    fn find(&self, id: &A::Id, _: &mut Tx) -> impl Future<Output = Result<Option<Loaded<A>>, StoreError>> + Send {
        self.load(id)
    }
}

/// The aggregate a handler looked for has no events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotFound<Id>(pub Id);

impl<Id: fmt::Display> fmt::Display for NotFound<Id> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} not found", self.0)
    }
}

impl<Id: fmt::Debug + fmt::Display> std::error::Error for NotFound<Id> {}
