//! The loader and the publisher: what a repository adapter and a command
//! handler's commit do between them. Rules of the domain are tested with
//! `AggregateFixture`; re-running a decision that lost a race belongs to the
//! gateway (see `stateful.rs`).

use cqrs::{
    Aggregate, AggregateLoader, EventAppender, EventPublisher, InMemoryEventStore, InlinePublisher, Loaded, Pending,
    StoreError,
    testing::{AggregateFixture, block_on},
};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent, cqrs::TagResolver)]
struct Added(u64);

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
    /// Refuses to go past 1000.
    fn add(&self, n: u64, events: &mut impl EventAppender<Added>) -> Result<(), Refused> {
        if self.total + n > 1000 {
            return Err(Refused);
        }
        events.append(Added(n));
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
struct Refused;

type Store = InMemoryEventStore<Added>;

struct Sums {
    loader: AggregateLoader<Sum, Store>,
    publisher: InlinePublisher<Added, Store>,
}

fn sums() -> Sums {
    let store: Store = Store::default();
    Sums {
        loader: AggregateLoader::new(Store::clone(&store)),
        publisher: InlinePublisher::new(store),
    }
}

impl Sums {
    fn find(&self, id: u32) -> Option<Loaded<Sum>> {
        block_on(self.loader.load(&id)).unwrap()
    }

    /// Decides `n` on `loaded` and commits it.
    fn add(&self, loaded: Loaded<Sum>, n: u64) -> Result<Option<u64>, StoreError> {
        let mut events = Vec::new();
        loaded.add(n, &mut events).unwrap();
        block_on(self.publisher.commit(loaded, Pending::new(events)))
    }

    fn with_total(total: u64) -> Self {
        let sums = sums();
        sums.add(cqrs::New::<Sum>(1).into(), total).unwrap();
        sums
    }

    fn total(&self) -> u64 {
        self.find(1).unwrap().total
    }
}

#[test]
fn a_commit_stores_what_the_decision_appended_and_the_next_load_sees_it() {
    let sums = Sums::with_total(10);
    let loaded = sums.find(1).unwrap();
    assert_eq!(loaded.version(), Some(1));
    assert_eq!(sums.add(loaded, 5).unwrap(), Some(2));
    assert_eq!(sums.total(), 15);
    assert_eq!(sums.find(1).unwrap().version(), Some(2));
}

#[test]
fn a_refused_decision_appends_nothing() {
    AggregateFixture::<Sum>::new(1)
        .given([Added(990)])
        .when(|sum, events| sum.add(50, events))
        .expect_error(Refused)
        .expect_no_events();
}

#[test]
fn an_aggregate_loaded_before_another_commit_is_refused_with_the_actual_version() {
    let sums = Sums::with_total(10);
    let (first, second) = (sums.find(1).unwrap(), sums.find(1).unwrap());
    sums.add(first, 100).unwrap();
    let refused = sums.add(second, 1);
    assert!(
        matches!(refused, Err(StoreError::Conflict { actual: Some(2), .. })),
        "{refused:?}"
    );
    assert_eq!(sums.total(), 110, "only the first commit");
}

#[test]
fn committing_a_new_aggregate_twice_is_a_conflict() {
    let sums = Sums::with_total(10);
    let refused = sums.add(cqrs::New::<Sum>(1).into(), 1);
    assert!(matches!(refused, Err(StoreError::Conflict { .. })), "{refused:?}");
    assert_eq!(sums.total(), 10);
}

#[test]
fn a_decision_that_appends_nothing_commits_nothing() {
    let sums = Sums::with_total(10);
    let loaded = sums.find(1).unwrap();
    assert_eq!(
        block_on(sums.publisher.commit(loaded, Pending::new(Vec::new()))).unwrap(),
        None
    );
    assert_eq!(sums.find(1).unwrap().version(), Some(1));
}

#[test]
fn an_aggregate_without_events_is_not_found() {
    assert!(sums().find(7).is_none());
}
