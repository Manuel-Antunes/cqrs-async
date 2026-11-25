//! Snapshots: a load from a snapshot ends in the same state as a full
//! replay, a load that replayed enough keeps one, and a broken snapshot
//! store only costs a full replay.

use std::{
    future::{Future, ready},
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use cqrs::{
    aggregate::{
        Aggregate, AggregateLoader, InMemorySnapshots, Loaded, NoSnapshots, SnapshotPolicy, SnapshotStore, Snapshots,
        Snapshotting,
    },
    command::{EventPublisher, Pending},
    processing::InlinePublisher,
    store::{InMemoryEventStore, StoreError},
    testing::block_on,
};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
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

type Store = InMemoryEventStore<Added>;

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
        loader: AggregateLoader::new(store.clone()),
        publisher: InlinePublisher::new(store),
    }
}

impl<N: SnapshotStore<Sum>> App<N> {
    /// One command of one event: adds `n`, loading what was there.
    fn add(&self, n: u64) {
        let sum = block_on(self.loader.load(&1))
            .unwrap()
            .unwrap_or_else(|| cqrs::aggregate::New(1).into());
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
    fn with_snapshots<N>(self, snapshots: N, policy: impl Into<SnapshotPolicy>) -> App<N> {
        App {
            loader: self.loader.with_snapshots(snapshots, policy),
            publisher: self.publisher,
        }
    }
}

#[test]
fn loading_from_a_snapshot_equals_a_full_replay() {
    let snapshots = InMemorySnapshots::default();
    let with = app().with_snapshots(Shared(snapshots.clone()), 10);
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
    let snapshots = InMemorySnapshots::default();
    let app = app().with_snapshots(Shared(snapshots.clone()), 10);
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
    let snapshots = InMemorySnapshots::default();
    let app = app().with_snapshots(Shared(snapshots.clone()), 10);
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
struct Shared(InMemorySnapshots<Sum>);

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
    let snapshots = InMemorySnapshots::default();
    let app = app().with_snapshots(Shared(snapshots.clone()), 2);
    app.add_n(3);

    let (snapshot, version) = block_on(snapshots.load(&1)).unwrap().expect("a snapshot was taken");
    assert_eq!(
        (snapshot.total, version),
        (3, 2),
        "taken when the third command's load replayed 2 events"
    );
    assert_eq!(app.load().total, 6);
}

#[test]
fn a_load_that_took_long_enough_keeps_a_snapshot() {
    let snapshots = InMemorySnapshots::default();
    // Any time at all is long enough: every load that replayed something keeps one.
    let app = app().with_snapshots(
        Shared(snapshots.clone()),
        SnapshotPolicy::after_sourcing(Duration::ZERO),
    );
    app.add(1);
    assert!(
        block_on(snapshots.load(&1)).unwrap().is_none(),
        "nothing existed: nothing was replayed"
    );

    app.add(2); // its load replayed one event
    let (sum, version) = block_on(snapshots.load(&1)).unwrap().unwrap();
    assert_eq!((sum.total, version), (1, 1));

    app.add(3);
    assert_eq!(block_on(snapshots.load(&1)).unwrap().unwrap().1, 2);
    assert_eq!(app.load().total, 6);
}

/// Counts the snapshots it is asked to keep.
struct Counting(InMemorySnapshots<Sum>, Arc<AtomicUsize>);

impl SnapshotStore<Sum> for Counting {
    fn load(&self, id: &u32) -> impl Future<Output = Result<Option<(Sum, u64)>, StoreError>> + Send {
        self.0.load(id)
    }

    fn save(&self, sum: &Sum, version: u64) -> impl Future<Output = Result<(), StoreError>> + Send {
        self.1.fetch_add(1, Ordering::Relaxed);
        self.0.save(sum, version)
    }
}

#[test]
fn a_load_that_replayed_nothing_keeps_no_snapshot_however_long_it_took() {
    let saves = Arc::new(AtomicUsize::new(0));
    let app = app().with_snapshots(
        Counting(InMemorySnapshots::default(), Arc::clone(&saves)),
        SnapshotPolicy::after_sourcing(Duration::ZERO),
    );
    app.add(1);
    app.add(2); // replayed event 1: kept a snapshot
    assert_eq!(saves.load(Ordering::Relaxed), 1);
    app.load(); // starts from the snapshot at 1, replays event 2: another
    assert_eq!(saves.load(Ordering::Relaxed), 2);
    app.load(); // starts from the snapshot at 2: nothing to replay, nothing to keep
    assert_eq!(saves.load(Ordering::Relaxed), 2);
}

#[test]
fn either_threshold_keeps_a_snapshot() {
    // Events alone: 10 are not reached by a load of 3 events.
    let by_events = InMemorySnapshots::default();
    let app = app().with_snapshots(Shared(by_events.clone()), SnapshotPolicy::after_events(10));
    app.add_n(4);
    assert!(block_on(by_events.load(&1)).unwrap().is_none());

    // Both set: the time threshold (zero) is reached long before the events.
    let both = InMemorySnapshots::default();
    let app = self::app().with_snapshots(
        Shared(both.clone()),
        SnapshotPolicy::after_events(10).or_after_sourcing(Duration::ZERO),
    );
    app.add_n(3);
    assert_eq!(block_on(both.load(&1)).unwrap().unwrap().1, 2);

    // Both set: the events threshold is reached though the time one is not.
    let by_count = InMemorySnapshots::default();
    let app = self::app().with_snapshots(
        Shared(by_count.clone()),
        SnapshotPolicy::after_events(3).or_after_sourcing(Duration::from_secs(3600)),
    );
    app.add_n(4);
    assert_eq!(block_on(by_count.load(&1)).unwrap().unwrap().1, 3);
}

#[test]
fn a_policy_says_what_it_is_made_of() {
    let events = SnapshotPolicy::after_events(5);
    assert!(events.reached(5, Duration::ZERO));
    assert!(!events.reached(4, Duration::from_secs(3600)));
    assert!(
        !events.times_sourcing(),
        "loads are not timed for a policy without a time threshold"
    );

    let time = SnapshotPolicy::after_sourcing(Duration::from_secs(5));
    assert!(time.times_sourcing());
    assert!(time.reached(0, Duration::from_secs(5)));
    assert!(!time.reached(1_000, Duration::from_secs(4)));

    let both = events.or_after_sourcing(Duration::from_secs(5));
    assert!(both.times_sourcing());
    assert!(both.reached(5, Duration::ZERO) && both.reached(1, Duration::from_secs(6)));
    assert!(!both.reached(4, Duration::from_secs(4)));

    assert_eq!(SnapshotPolicy::from(7), SnapshotPolicy::after_events(7));
    assert_eq!(
        SnapshotPolicy::after_sourcing(Duration::from_secs(1)).or_after_events(2),
        SnapshotPolicy::after_events(2).or_after_sourcing(Duration::from_secs(1)),
    );
}

#[test]
#[should_panic(expected = "after 0 events")]
fn a_policy_after_zero_events_is_refused() {
    let _ = SnapshotPolicy::after_events(0);
}

#[test]
#[should_panic(expected = "after 0 events")]
fn a_loader_snapshotting_every_zero_events_is_refused() {
    let _ = AggregateLoader::<Sum, _>::new(Store::default()).with_snapshots(InMemorySnapshots::<Sum>::default(), 0);
}

// ---------------------------------------------------------------------------
// The policy belongs to the aggregate, the engine hands out the repository.
// ---------------------------------------------------------------------------

/// How many events `Counter` applied, replayed or recorded: what a load that
/// started from a snapshot saves.
static APPLIED: AtomicUsize = AtomicUsize::new(0);

/// Held by the tests that count, so they do not count each other's events.
static COUNTING: Mutex<()> = Mutex::new(());

/// Declares its own policy: a snapshot after a load that replayed 2 events.
#[cqrs::aggregate(name = "counter", event = Added, snapshot(after_events = 2))]
#[derive(Clone, Debug, PartialEq)]
struct Counter {
    id: u32,
    total: u64,
}

impl Counter {
    fn apply(&mut self, Added(n): &Added) {
        APPLIED.fetch_add(1, Ordering::Relaxed);
        self.total += n;
    }
}

/// Neither `Clone` nor serializable, and it asks for no snapshots.
#[cqrs::aggregate(name = "plain", event = Added)]
struct Plain {
    id: u32,
    total: u64,
}

impl Plain {
    fn apply(&mut self, Added(n): &Added) {
        self.total += n;
    }
}

fn policy_of<A: Snapshotting>() -> Option<SnapshotPolicy> {
    A::POLICY
}

fn keeps_snapshots<A: Snapshotting<Mode = Snapshots>>() {}

fn keeps_none<A: Snapshotting<Mode = NoSnapshots>>() {}

#[test]
fn the_macro_writes_the_policy_the_aggregate_declares() {
    #[cqrs::aggregate(event = Added, snapshot(after_sourcing = "250ms"))]
    struct Timed {
        id: u32,
    }
    impl Timed {
        fn apply(&mut self, _: &Added) {}
    }

    #[cqrs::aggregate(event = Added, snapshot(after_events = 100, after_sourcing = "2m"))]
    struct Both {
        id: u32,
    }
    impl Both {
        fn apply(&mut self, _: &Added) {}
    }

    // The generics and the where clause of the struct go to the impl too.
    #[cqrs::aggregate(event = Added, snapshot(after_events = 7))]
    struct Tagged<T: Default + Send + Sync + 'static>
    where
        T: Clone,
    {
        id: u32,
        tag: T,
    }
    impl<T: Default + Send + Sync + 'static + Clone> Tagged<T> {
        fn apply(&mut self, _: &Added) {
            let _ = &self.tag;
        }
    }

    assert_eq!(policy_of::<Counter>(), Some(SnapshotPolicy::after_events(2)));
    assert_eq!(
        policy_of::<Timed>(),
        Some(SnapshotPolicy::after_sourcing(Duration::from_millis(250)))
    );
    assert_eq!(
        policy_of::<Both>(),
        Some(SnapshotPolicy::after_events(100).or_after_sourcing(Duration::from_secs(120)))
    );
    assert_eq!(policy_of::<Tagged<String>>(), Some(SnapshotPolicy::after_events(7)));
    assert_eq!(policy_of::<Plain>(), None);

    keeps_snapshots::<Counter>();
    keeps_snapshots::<Tagged<u8>>();
    keeps_none::<Plain>();
}

/// What a command's handler does, by hand: find (or start), decide, commit.
fn add_to_counter(
    repository: &AggregateLoader<Counter, Store, InMemorySnapshots<Counter>>,
    publisher: &impl EventPublisher<Added>,
    n: u64,
) {
    let counter = block_on(repository.load(&1))
        .unwrap()
        .unwrap_or_else(|| cqrs::aggregate::New(1).into());
    block_on(publisher.commit(counter, Pending::new(vec![Added(n)]))).unwrap();
}

/// A load that starts from the snapshot applies only the events after it.
fn applied_by_a_load(repository: &AggregateLoader<Counter, Store, InMemorySnapshots<Counter>>) -> (u64, usize) {
    APPLIED.store(0, Ordering::Relaxed);
    let counter = block_on(repository.load(&1)).unwrap().unwrap();
    (counter.total, APPLIED.load(Ordering::Relaxed))
}

#[test]
fn the_publishers_repository_snapshots_as_the_aggregate_declares() {
    let _counting = COUNTING.lock().unwrap_or_else(PoisonError::into_inner);
    let store = Store::default();
    let publisher = InlinePublisher::new(store);
    let repository = publisher.repository::<Counter>();
    for n in 1..=3 {
        add_to_counter(&repository, &publisher, n);
    }
    // The third command's load replayed 2 events and kept a snapshot at 2:
    // loading again applies only the third event.
    assert_eq!(applied_by_a_load(&repository), (6, 1));

    // Its own repository, without a policy of its own: the aggregate that
    // keeps no snapshots is served all the same, from any store.
    let _: AggregateLoader<Plain, Store, (), ()> = publisher.repository::<Plain>();
}

#[cfg(feature = "tokio")]
#[test]
fn the_engines_repository_snapshots_as_the_aggregate_declares() {
    use cqrs::processing::Events;

    let _counting = COUNTING.lock().unwrap_or_else(PoisonError::into_inner);
    let events = Events::new(Store::default());
    let repository: AggregateLoader<Counter, Store, InMemorySnapshots<Counter>, ()> = events.repository::<Counter>();
    for n in 1..=3 {
        add_to_counter(&repository, &events, n);
    }
    assert_eq!(applied_by_a_load(&repository), (6, 1));

    let _: AggregateLoader<Plain, Store, (), ()> = events.repository::<Plain>();
}

#[cfg(feature = "tokio")]
#[test]
fn the_engines_repository_takes_the_transaction_of_the_engine() {
    use cqrs::processing::Events;

    struct Transaction;
    struct Transactions;
    impl cqrs::processing::TransactionManager for Transactions {
        type Tx = Transaction;

        fn begin(&self) -> impl Future<Output = Result<Transaction, StoreError>> + Send {
            ready(Ok(Transaction))
        }

        fn commit(&self, _: Transaction) -> impl Future<Output = Result<(), StoreError>> + Send {
            ready(Ok(()))
        }
    }

    let events = Events::new(Store::default()).with_transactions(Transactions);
    let _: AggregateLoader<Counter, Store, InMemorySnapshots<Counter>, Transaction> = events.repository::<Counter>();
    // The path that reads outside any transaction is asked for.
    let _: AggregateLoader<Counter, Store, InMemorySnapshots<Counter>, ()> =
        events.repository::<Counter>().in_transaction::<()>();
}
