//! Snapshots: a load from a snapshot ends in the same state as a full
//! replay, and a broken snapshot store only costs a full replay.

use std::future::{Future, ready};

use cqrs::{
    Aggregate, EventAppender, EventSourced, InMemoryEventStore, InMemorySnapshots, NotFound, SnapshotStore, StoreError,
    testing::block_on,
};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent, cqrs::TagResolver)]
struct Added(u64);

#[derive(Clone, Debug, PartialEq)]
struct Sum {
    id: u32,
    total: u64,
}

impl Aggregate for Sum {
    const TYPE: &'static str = "sum";
    type Id = u32;
    type Event = Added;

    fn initial(id: &u32) -> Self {
        Self { id: *id, total: 0 }
    }

    fn id(&self) -> &u32 {
        &self.id
    }

    fn evolve(&mut self, Added(n): &Added) {
        self.total += n;
    }
}

impl Sum {
    /// A decision: `&self` plus an appender.
    fn add(&self, n: u64, events: &mut impl EventAppender<Added>) -> Result<(), Error> {
        events.append(Added(n));
        Ok(())
    }
}

/// What the tests' decisions and saves can fail with.
#[derive(Debug, PartialEq)]
enum Error {
    Store(StoreError),
    NotFound,
}

impl From<StoreError> for Error {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<NotFound<u32>> for Error {
    fn from(_: NotFound<u32>) -> Self {
        Self::NotFound
    }
}

/// Saves `n` commands of one event each, as a real application would.
fn add_n<P, N>(sums: &EventSourced<Sum, InMemoryEventStore<Added>, P, N>, n: u64)
where
    P: cqrs::EventProcessor<Added>,
    N: SnapshotStore<Sum>,
{
    for i in 1..=n {
        let saved: Result<(), Error> = if i == 1 {
            block_on(sums.create(&1, |sum, events| sum.add(i, events)))
        } else {
            block_on(sums.update(&1, |sum, events| sum.add(i, events)))
        };
        saved.unwrap();
    }
}

#[test]
fn loading_from_a_snapshot_equals_a_full_replay() {
    let sums = EventSourced::new(InMemoryEventStore::new()).with_snapshots(InMemorySnapshots::default(), 10);
    add_n(&sums, 25);

    let loaded = block_on(sums.load(&1)).unwrap().unwrap();
    assert_eq!(loaded.total, (1..=25).sum::<u64>());
    assert_eq!(loaded.version(), Some(25));

    // Same events, no snapshots: same state.
    let plain = EventSourced::<Sum, _>::new(InMemoryEventStore::new());
    add_n(&plain, 25);
    assert_eq!(*block_on(plain.load(&1)).unwrap().unwrap(), *loaded);
}

#[test]
fn a_load_starts_from_the_snapshot_and_replays_only_the_tail() {
    let snapshots = InMemorySnapshots::default();
    snapshots_of(&snapshots, 25); // snapshot at 20, events 21..=25 after it
    // Mark the snapshot: only a load that starts from it can see the mark.
    let (mut marked, at) = block_on(snapshots.load(&1)).unwrap().unwrap();
    marked.total = 1_000_000;
    block_on(snapshots.save(&marked, at + 1)).unwrap(); // newer, so it replaces
    let sums = EventSourced::new(InMemoryEventStore::new()).with_snapshots(Shared(&snapshots), 10);
    add_n(&sums, 0);
    // Different store, same snapshot: proves the stream is read after the snapshot only.
    let loaded = block_on(sums.load(&1)).unwrap().unwrap();
    assert_eq!(loaded.total, 1_000_000);
    assert_eq!(loaded.version(), Some(21));
}

#[test]
fn snapshots_are_taken_every_n_events_and_keep_the_newest() {
    let snapshots = InMemorySnapshots::default();
    snapshots_of(&snapshots, 25);
    let (sum, version) = block_on(snapshots.load(&1)).unwrap().unwrap();
    assert_eq!(version, 20);
    assert_eq!(sum.total, (1..=20).sum::<u64>());
}

fn snapshots_of(snapshots: &InMemorySnapshots<Sum>, n: u64) {
    let sums = EventSourced::new(InMemoryEventStore::new()).with_snapshots(Shared(snapshots), 10);
    add_n(&sums, n);
}

/// Lends a snapshot store to a repository for the test.
struct Shared<'a>(&'a InMemorySnapshots<Sum>);

impl SnapshotStore<Sum> for Shared<'_> {
    fn load(&self, id: &u32) -> impl Future<Output = Result<Option<(Sum, u64)>, StoreError>> + Send {
        self.0.load(id)
    }

    fn save(&self, sum: &Sum, version: u64) -> impl Future<Output = Result<(), StoreError>> + Send {
        self.0.save(sum, version)
    }
}

/// A snapshot store that always fails.
struct Broken;

impl SnapshotStore<Sum> for Broken {
    fn load(&self, _: &u32) -> impl Future<Output = Result<Option<(Sum, u64)>, StoreError>> + Send {
        ready(Err(StoreError::Backend("snapshot store down".into())))
    }

    fn save(&self, _: &Sum, _: u64) -> impl Future<Output = Result<(), StoreError>> + Send {
        ready(Err(StoreError::Backend("snapshot store down".into())))
    }
}

#[test]
fn a_broken_snapshot_store_only_costs_a_full_replay() {
    let sums = EventSourced::new(InMemoryEventStore::new()).with_snapshots(Broken, 2);
    add_n(&sums, 5); // saves keep working
    let loaded = block_on(sums.load(&1)).unwrap().unwrap();
    assert_eq!(loaded.total, 15);
    assert_eq!(loaded.version(), Some(5));
}

#[test]
fn optimistic_concurrency_still_holds_with_snapshots() {
    let sums = EventSourced::new(InMemoryEventStore::new()).with_snapshots(InMemorySnapshots::default(), 1);
    add_n(&sums, 3);
    let first = block_on(sums.load(&1)).unwrap().unwrap();
    let second = block_on(sums.load(&1)).unwrap().unwrap();
    block_on(sums.append(first, vec![Added(10)])).unwrap();
    assert!(matches!(
        block_on(sums.append(second, vec![Added(20)])),
        Err(StoreError::Conflict { actual: Some(4), .. })
    ));
}
