//! Loading aggregates: [`AggregateLoader`] rebuilds them from their events
//! (from a snapshot, if the application keeps any), and [`Loaded`] is what it
//! returns: the aggregate with the version it was read at.
//!
//! It is the only way to load. Repositories belong to the application (a port
//! with the finders it needs), and their adapters hold an `AggregateLoader`;
//! command handlers commit what they decide through the
//! [`EventPublisher`](crate::EventPublisher) port.

use std::{fmt, marker::PhantomData, ops::Deref};

use crate::{
    Aggregate, AppendCondition, Appender, EventCriteria, EventEnvelope, EventStore, ExpectedVersion, SnapshotStore,
    StoreError, StreamId,
};

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
    /// A brand-new aggregate whose stream must not exist yet.
    pub fn new(aggregate: A) -> Self {
        Self {
            aggregate,
            version: None,
            condition: None,
        }
    }

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

    /// What a commit of this aggregate's events needs: its stream, what it
    /// expects of it (built from criteria, its condition guards it, not a
    /// stream version, as other streams hold its events too), and that
    /// condition.
    pub(crate) fn into_commit(self) -> (StreamId, ExpectedVersion, Option<AppendCondition>) {
        let expected = match (&self.condition, self.version) {
            (Some(_), _) => ExpectedVersion::Any,
            (None, None) => ExpectedVersion::NoStream,
            (None, Some(version)) => ExpectedVersion::Exact(version),
        };
        (StreamId::of::<A>(self.aggregate.id()), expected, self.condition)
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
/// let posts = AggregateLoader::<Post, _>::new(store);                  // in your repository's adapter
/// let post = posts.load(&id).await?;
/// ```
///
/// With [`with_snapshots`](Self::with_snapshots), a load that had to replay
/// at least `every` events saves a snapshot, so the next one replays only
/// what came after it; commits do not snapshot (the publisher serves every
/// aggregate of an event type, the loader is the one place that knows one).
pub struct AggregateLoader<A, S, N = ()> {
    /// Where the event streams live.
    store: S,
    /// Saved states, so a load replays only the tail (`()`: none).
    snapshots: N,
    /// Take a snapshot after replaying this many events (0: never).
    snapshot_every: u64,
    /// Ties the loader to one aggregate type without storing one.
    aggregate: PhantomData<fn() -> A>,
}

impl<A, S> AggregateLoader<A, S> {
    /// A loader over `store`, without snapshots.
    pub fn new(store: S) -> Self {
        Self {
            store,
            snapshots: (),
            snapshot_every: 0,
            aggregate: PhantomData,
        }
    }
}

impl<A, S, N> AggregateLoader<A, S, N> {
    /// Keeps a snapshot in `snapshots` whenever a load replayed `every`
    /// events or more, so loading replays fewer than `every` events after
    /// the newest one. Worth it for long-lived aggregates; the stream stays
    /// the source of truth.
    ///
    /// # Panics
    ///
    /// If `every` is zero.
    pub fn with_snapshots<N2>(self, snapshots: N2, every: u64) -> AggregateLoader<A, S, N2> {
        assert!(every > 0, "snapshot every 0 events");
        AggregateLoader {
            store: self.store,
            snapshots,
            snapshot_every: every,
            aggregate: PhantomData,
        }
    }

    /// The underlying event store, e.g. to read every event in order.
    pub fn store(&self) -> &S {
        &self.store
    }
}

impl<A, S, N> AggregateLoader<A, S, N>
where
    A: Aggregate,
    S: EventStore<A::Event>,
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

    /// [`load`](Self::load) within a command: for an aggregate built from
    /// [`criteria`](Aggregate::criteria), the commits of `events` then fail
    /// if anything it was built from changes, **even if it did not exist**
    /// (absence is a fact a decision can rely on too). The aggregates a
    /// decision reads without committing are loaded this way.
    ///
    /// # Errors
    ///
    /// The store's.
    pub async fn load_in(&self, id: &A::Id, events: &mut Appender<A::Event>) -> Result<Option<Loaded<A>>, StoreError> {
        let Some(criteria) = A::criteria(id) else {
            return self.load_stream(id).await;
        };
        let (loaded, condition) = self.source_by(id, criteria).await?;
        events.depend_on(condition);
        Ok(loaded)
    }

    /// Reads every event matching `criteria`, from any stream of the store,
    /// and makes the following commits of `events` conditional on them (see
    /// [`Appender::source`]).
    ///
    /// # Errors
    ///
    /// The store's.
    pub async fn source(
        &self,
        criteria: EventCriteria,
        events: &mut Appender<A::Event>,
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
        // A failing snapshot store only costs a full replay.
        let snapshot = self.snapshots.load(id).await.ok().flatten();
        let (mut aggregate, mut version) = snapshot.unwrap_or_else(|| (A::initial(id), 0));
        let tail = self.store.load(&StreamId::of::<A>(id), version).await?;
        if version == 0 && tail.is_empty() {
            return Ok(None);
        }
        for event in &tail {
            aggregate.evolve(&event.payload);
            version = event.sequence;
        }
        if self.snapshot_every > 0 && tail.len() as u64 >= self.snapshot_every {
            // A failed snapshot only means a longer replay next time.
            let _ = self.snapshots.save(&aggregate, version).await;
        }
        Ok(Some(Loaded {
            aggregate,
            version: Some(version),
            condition: None,
        }))
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
