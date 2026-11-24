//! Snapshots: a load from a snapshot ends in the same state as a full
//! replay, a load that replayed enough keeps one, and a broken snapshot
//! store only costs a full replay.

use std::{
    future::{Future, ready},
    sync::Arc,
};

use cqrs::{
    Aggregate, AggregateLoader, EventPublisher, InMemoryEventStore, InMemorySnapshots, InlinePublisher, Loaded,
    Pending, SnapshotStore, StoreError, testing::block_on,
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

type Store = Arc<InMemoryEventStore<Added>>;

/// What the recommended path is made of: a publisher that command handlers
/// commit through, and a loader in the repository's adapter. The test body
/// plays the handler: load, decide, commit.
struct App<N = ()> {
    loader: AggregateLoader<Sum, Store, N>,
    publisher: InlinePublisher<Added, Store>,
}

fn app() -> App {
    let store = Store::default();
    App {
        loader: AggregateLoader::new(Arc::clone(&store)),
        publisher: InlinePublisher::new(store),
    }
}

impl<N: SnapshotStore<Sum>> App<N> {
    /// One command of one event: adds `n`, loading what was there.
    fn add(&self, n: u64) {
        let sum = block_on(self.loader.load(&1))
            .unwrap()
            .unwrap_or_else(|| cqrs::New(1).into());
        block_on(self.publisher.commit(sum, Pending::new(vec![Added(n)]))).unwrap();
    }

    fn add_n(&self, n: u64) {
        (1..=n).for_each(|i| self.add(i));
    }

    fn load(&self) -> Loaded<Sum> {
        block_on(self.loader.load(&1)).unwrap().unwrap()
    }
}

impl App {
    fn with_snapshots<N>(self, snapshots: N, every: u64) -> App<N> {
        App {
            loader: self.loader.with_snapshots(snapshots, every),
            publisher: self.publisher,
        }
    }
}

#[test]
fn loading_from_a_snapshot_equals_a_full_replay() {
    let snapshots = Arc::new(InMemorySnapshots::default());
    let with = app().with_snapshots(Shared(Arc::clone(&snapshots)), 10);
    with.add_n(25);

    // Each command loaded before committing, so the 21st load replayed 10
    // events and kept a snapshot at version 20; these loads start from it.
    assert_eq!(
        block_on(snapshots.load(&1)).unwrap().map(|(_, version)| version),
        Some(20)
    );
    let first = with.load();
    let from_snapshot = with.load();
    assert_eq!(from_snapshot.total, (1..=25).sum::<u64>());
    assert_eq!(from_snapshot.version(), Some(25));
    assert_eq!(*first, *from_snapshot);

    // Same events, no snapshots: same state.
    let plain = app();
    plain.add_n(25);
    assert_eq!(*plain.load(), *from_snapshot);
}

#[test]
fn a_load_starts_from_the_snapshot_and_replays_only_the_tail() {
    let snapshots = Arc::new(InMemorySnapshots::default());
    let app = app().with_snapshots(Shared(Arc::clone(&snapshots)), 10);
    app.add_n(25);
    // A newer snapshot (version 21; the one taken while adding is at 20) that
    // no replay could have produced: only a load that starts from it sees the
    // mark.
    block_on(snapshots.save(
        &Sum {
            id: 1,
            total: 1_000_000,
        },
        21,
    ))
    .unwrap();

    let loaded = app.load();
    assert_eq!(loaded.total, 1_000_000 + (22..=25).sum::<u64>());
    assert_eq!(loaded.version(), Some(25));
}

#[test]
fn a_snapshot_is_kept_when_a_load_replayed_enough_and_only_then() {
    let snapshots = Arc::new(InMemorySnapshots::default());
    let app = app().with_snapshots(Shared(Arc::clone(&snapshots)), 10);
    app.add_n(9);
    app.load();
    assert!(
        block_on(snapshots.load(&1)).unwrap().is_none(),
        "9 events replayed: not enough"
    );

    app.add(10); // the 10th event: the next load replays 10
    app.load();
    let (sum, version) = block_on(snapshots.load(&1)).unwrap().unwrap();
    assert_eq!((sum.total, version), ((1..=10).sum::<u64>(), 10));

    app.add(11);
    app.load(); // replays 1 event after the snapshot
    assert_eq!(
        block_on(snapshots.load(&1)).unwrap().unwrap().1,
        10,
        "the snapshot was not replaced"
    );
}

/// Lends a snapshot store to a loader for the test.
struct Shared(Arc<InMemorySnapshots<Sum>>);

impl SnapshotStore<Sum> for Shared {
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
    let app = app().with_snapshots(Broken, 2);
    app.add_n(5); // loads and commits keep working
    let loaded = app.load();
    assert_eq!(loaded.total, 15);
    assert_eq!(loaded.version(), Some(5));
}

#[test]
fn optimistic_concurrency_still_holds_with_snapshots() {
    let app = app().with_snapshots(InMemorySnapshots::default(), 1);
    app.add_n(3);
    let (first, second) = (app.load(), app.load());
    block_on(app.publisher.commit(first, Pending::new(vec![Added(10)]))).unwrap();
    assert!(matches!(
        block_on(app.publisher.commit(second, Pending::new(vec![Added(20)]))),
        Err(StoreError::Conflict { actual: Some(4), .. })
    ));
}

/// The recommended path: command handlers commit through the publisher, the
/// repository adapter loads with the loader. With snapshots every 2 events, a
/// snapshot exists after the third command (its load replayed 2 events).
/// Reproduced in Phase 1 (item 1.4) against `cqrs::load`, which never read or
/// kept one.
#[test]
fn commits_through_the_publisher_leave_snapshots_for_the_repository_to_load_from() {
    let snapshots = Arc::new(InMemorySnapshots::default());
    let app = app().with_snapshots(Shared(Arc::clone(&snapshots)), 2);
    app.add_n(3);

    let (snapshot, version) = block_on(snapshots.load(&1)).unwrap().expect("a snapshot was taken");
    assert_eq!(
        (snapshot.total, version),
        (3, 2),
        "taken when the third command's load replayed 2 events"
    );
    assert_eq!(app.load().total, 6);
}
