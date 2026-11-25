//! Snapshots: the [`SnapshotStore`] port, when to take one
//! ([`SnapshotPolicy`], declared by the aggregate through [`Snapshotting`])
//! and what an event store can keep them of ([`KeepSnapshots`]). An
//! optimization, never the source of truth: the event stream always is.

use crate::sync::Locked;
use std::{
    collections::HashMap,
    future::{Future, ready},
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{Aggregate, InMemoryEventStore, StoreError, StreamId};

/// When a load keeps a snapshot of the aggregate it rebuilt: after it
/// replayed a number of events, after it took a given time, or whichever
/// comes first. What `#[cqrs::aggregate(.., snapshot(..))]` declares, and what
/// [`AggregateLoader::with_snapshots`](crate::AggregateLoader::with_snapshots)
/// takes (a plain integer is "after that many events").
///
/// ```
/// # use std::time::Duration;
/// # use cqrs::aggregate::SnapshotPolicy;
/// // After 100 events replayed, or a load of 5 seconds or more.
/// const POLICY: SnapshotPolicy = SnapshotPolicy::after_events(100).or_after_sourcing(Duration::from_secs(5));
/// assert!(POLICY.reached(100, Duration::ZERO));
/// assert!(POLICY.reached(3, Duration::from_secs(6)));
/// assert!(!POLICY.reached(99, Duration::from_secs(4)));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotPolicy {
    /// Keep one after replaying this many events (at least 1).
    after_events: Option<u64>,
    /// Keep one after a load that took this long.
    after_sourcing: Option<Duration>,
}

impl SnapshotPolicy {
    /// A snapshot after a load replayed `events` events or more.
    ///
    /// # Panics
    ///
    /// If `events` is zero (in a `const`, that is a compile error).
    pub const fn after_events(events: u64) -> Self {
        assert!(events > 0, "snapshot after 0 events");
        Self {
            after_events: Some(events),
            after_sourcing: None,
        }
    }

    /// A snapshot after a load took `time` or more to rebuild the aggregate
    /// (reading the snapshot there was, and replaying what came after it).
    pub const fn after_sourcing(time: Duration) -> Self {
        Self {
            after_events: None,
            after_sourcing: Some(time),
        }
    }

    /// This policy, or a snapshot after a load replayed `events` events or
    /// more, whichever comes first.
    ///
    /// # Panics
    ///
    /// If `events` is zero (in a `const`, that is a compile error).
    #[must_use]
    pub const fn or_after_events(mut self, events: u64) -> Self {
        assert!(events > 0, "snapshot after 0 events");
        self.after_events = Some(events);
        self
    }

    /// This policy, or a snapshot after a load took `time` or more,
    /// whichever comes first.
    #[must_use]
    pub const fn or_after_sourcing(mut self, time: Duration) -> Self {
        self.after_sourcing = Some(time);
        self
    }

    /// Whether a load that replayed `replayed` events in `sourcing` keeps a
    /// snapshot: either threshold that is set was met.
    pub fn reached(&self, replayed: u64, sourcing: Duration) -> bool {
        self.after_events.is_some_and(|events| replayed >= events)
            || self.after_sourcing.is_some_and(|time| sourcing >= time)
    }

    /// Whether loads must be timed: only a policy with a time threshold
    /// needs the clock.
    pub const fn times_sourcing(&self) -> bool {
        self.after_sourcing.is_some()
    }
}

/// A plain number of events: [`SnapshotPolicy::after_events`].
impl From<u64> for SnapshotPolicy {
    fn from(events: u64) -> Self {
        Self::after_events(events)
    }
}

/// [`Snapshotting::Mode`] of an aggregate that keeps snapshots: its event
/// store must be able to keep them ([`KeepSnapshots`]).
#[derive(Clone, Copy, Debug)]
pub struct Snapshots;

/// [`Snapshotting::Mode`] of an aggregate that keeps none: any event store
/// serves it.
#[derive(Clone, Copy, Debug)]
pub struct NoSnapshots;

/// How an aggregate is snapshotted. Written by `#[cqrs::aggregate]`:
/// `snapshot(after_events = 100, after_sourcing = "5s")` declares the
/// [`SnapshotPolicy`] (and [`Snapshots`] as the mode), without it the
/// aggregate says it keeps none ([`NoSnapshots`]).
///
/// What an engine's `repository::<A>()` reads to build the aggregate's
/// loader. An aggregate implemented by hand (`impl Aggregate`) implements
/// it by hand too, to be handed out by an engine; one that keeps no
/// snapshots says so:
///
/// ```
/// use cqrs::aggregate::{Aggregate, NoSnapshots, SnapshotPolicy, Snapshotting};
/// # #[derive(cqrs::DomainEvent)] enum DoorEvent { Installed }
/// struct Door { id: u32 }
///
/// impl Aggregate for Door {
///     const TYPE: &'static str = "door";
///     type Id = u32;
///     type Event = DoorEvent;
///     fn initial(id: &u32) -> Self { Door { id: *id } }
///     fn id(&self) -> &u32 { &self.id }
///     fn evolve(&mut self, _: &DoorEvent) {}
/// }
///
/// impl Snapshotting for Door {
///     type Mode = NoSnapshots;
///     const POLICY: Option<SnapshotPolicy> = None;
/// }
/// ```
pub trait Snapshotting: Aggregate {
    /// [`Snapshots`] if the aggregate keeps snapshots, [`NoSnapshots`] if
    /// not: what lets the event store be checked at compile time for it
    /// ([`KeepSnapshots`]).
    type Mode;

    /// When a load keeps a snapshot (`None`: never).
    const POLICY: Option<SnapshotPolicy>;
}

/// What an event store can keep snapshots of: the store the engine hands the
/// repository of an aggregate (`Events::repository`) is chosen through it,
/// from the aggregate's [`Snapshotting::Mode`].
///
/// Every store serves an aggregate that keeps none (`()`: no snapshots).
/// For one that does, the store must know how: the in-memory one keeps
/// clones (`Clone`), PostgreSQL keeps JSON (`Serialize + DeserializeOwned`).
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot keep snapshots of `{A}`",
    note = "an aggregate declared with `snapshot(..)` needs an event store that can keep it: `Clone` in memory, `Serialize + DeserializeOwned` on PostgreSQL",
    note = "or drop `snapshot(..)` from `{A}`"
)]
pub trait KeepSnapshots<A: Aggregate, Mode> {
    /// The snapshot store this event store keeps snapshots of `A` in.
    type Snapshots: SnapshotStore<A>;

    /// The snapshot store for `A`.
    fn snapshots(&self) -> Self::Snapshots;
}

/// An aggregate that keeps no snapshots is served by any event store.
impl<A: Aggregate, S> KeepSnapshots<A, NoSnapshots> for S {
    type Snapshots = ();

    fn snapshots(&self) -> Self::Snapshots {}
}

/// The in-memory store keeps snapshots of the aggregates that are `Clone`.
///
/// Each call hands out a fresh [`InMemorySnapshots`]: snapshots are an
/// optimization, so two repositories of one aggregate do not share them (each
/// takes its own, and loses them with the process, as the events are lost).
impl<A: Aggregate + Clone> KeepSnapshots<A, Snapshots> for InMemoryEventStore<A::Event> {
    type Snapshots = InMemorySnapshots<A>;

    fn snapshots(&self) -> Self::Snapshots {
        InMemorySnapshots::default()
    }
}

/// Saved aggregate states, so loading replays only the events after them.
///
/// Implemented for `()`: no snapshots, every load replays the whole stream.
pub trait SnapshotStore<A: Aggregate>: Send + Sync {
    /// The most recent snapshot of `id` and the stream version it was taken
    /// at, if any.
    fn load(&self, id: &A::Id) -> impl Future<Output = Result<Option<(A, u64)>, StoreError>> + Send;

    /// Saves `aggregate` as it is at stream `version`. Keeping only the
    /// newest snapshot per aggregate is enough.
    fn save(&self, aggregate: &A, version: u64) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// No snapshots.
impl<A: Aggregate> SnapshotStore<A> for () {
    fn load(&self, _: &A::Id) -> impl Future<Output = Result<Option<(A, u64)>, StoreError>> + Send {
        ready(Ok(None))
    }

    fn save(&self, _: &A, _: u64) -> impl Future<Output = Result<(), StoreError>> + Send {
        ready(Ok(()))
    }
}

/// Snapshots kept in memory, as clones of the aggregate. A handle: clones
/// share the same snapshots.
pub struct InMemorySnapshots<A> {
    /// The newest snapshot per stream, with its version.
    snapshots: Arc<Mutex<HashMap<StreamId, (A, u64)>>>,
}

impl<A> ::std::fmt::Debug for InMemorySnapshots<A> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("InMemorySnapshots").finish_non_exhaustive()
    }
}

impl<A> Default for InMemorySnapshots<A> {
    fn default() -> Self {
        Self {
            snapshots: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<A> Clone for InMemorySnapshots<A> {
    fn clone(&self) -> Self {
        Self {
            snapshots: Arc::clone(&self.snapshots),
        }
    }
}

impl<A: Aggregate + Clone> SnapshotStore<A> for InMemorySnapshots<A> {
    fn load(&self, id: &A::Id) -> impl Future<Output = Result<Option<(A, u64)>, StoreError>> + Send {
        let snapshots = self.snapshots.locked();
        ready(Ok(snapshots.get(&StreamId::of::<A>(id)).cloned()))
    }

    fn save(&self, aggregate: &A, version: u64) -> impl Future<Output = Result<(), StoreError>> + Send {
        let mut snapshots = self.snapshots.locked();
        let entry = snapshots.entry(StreamId::of::<A>(aggregate.id()));
        // Concurrent writers may save out of order: keep the newest.
        entry
            .and_modify(|(stored, at)| {
                if version > *at {
                    (*stored, *at) = (aggregate.clone(), version);
                }
            })
            .or_insert_with(|| (aggregate.clone(), version));
        ready(Ok(()))
    }
}
