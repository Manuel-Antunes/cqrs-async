//! `create` / `update`: a decision is run on the entity and its appended
//! events committed with optimistic concurrency, re-run when it loses a race.

use std::cell::Cell;

use cqrs::{Aggregate, EventAppender, EventSourced, InMemoryEventStore, NotFound, StoreError, testing::block_on};

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

#[derive(Debug, PartialEq)]
enum Error {
    Refused,
    NotFound(u32),
    Store(StoreError),
}

impl From<Refused> for Error {
    fn from(_: Refused) -> Self {
        Self::Refused
    }
}

impl From<NotFound<u32>> for Error {
    fn from(NotFound(id): NotFound<u32>) -> Self {
        Self::NotFound(id)
    }
}

impl From<StoreError> for Error {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

type Sums = EventSourced<Sum, InMemoryEventStore<Added>>;

fn sums_with(total: u64) -> Sums {
    let sums = Sums::new(InMemoryEventStore::new());
    let created: Result<(), Error> = block_on(sums.create(&1, |sum, events| sum.add(total, events)));
    created.unwrap();
    sums
}

fn total(sums: &Sums) -> u64 {
    block_on(sums.load(&1)).unwrap().unwrap().total
}

#[test]
fn update_commits_what_the_decision_appended() {
    let sums = sums_with(10);
    let result: Result<(), Error> = block_on(sums.update(&1, |sum, events| sum.add(5, events)));
    assert_eq!(result, Ok(()));
    assert_eq!(total(&sums), 15);
}

#[test]
fn a_refused_decision_saves_nothing_and_is_not_retried() {
    let sums = sums_with(990);
    let runs = Cell::new(0);
    let result: Result<(), Error> = block_on(sums.update(&1, |sum, events| {
        runs.set(runs.get() + 1);
        sum.add(50, events)
    }));
    assert_eq!(result, Err(Error::Refused));
    assert_eq!(runs.get(), 1);
    assert_eq!(total(&sums), 990);
}

/// The decision's first run saves a competing change, as another writer
/// would between this load and this save.
fn race_once<'a>(sums: &'a Sums, raced: &'a Cell<bool>) -> impl Fn(&Sum, &mut Vec<Added>) -> Result<(), Refused> + 'a {
    move |sum, events| {
        if !raced.replace(true) {
            let other = block_on(sums.load(&1)).unwrap().unwrap();
            block_on(sums.append(other, vec![Added(100)])).unwrap();
        }
        sum.add(1, events)
    }
}

#[test]
fn a_lost_race_is_retried_on_the_fresh_state() {
    let sums = sums_with(10);
    let raced = Cell::new(false);
    let result: Result<(), Error> = block_on(sums.update(&1, race_once(&sums, &raced)));
    assert_eq!(result, Ok(()));
    // Both survive: the competing +100 and this +1, decided again on 110.
    assert_eq!(total(&sums), 111);
}

#[test]
fn retries_run_out_with_a_conflict() {
    let sums = sums_with(10).with_conflict_retries(0);
    let raced = Cell::new(false);
    let result: Result<(), Error> = block_on(sums.update(&1, race_once(&sums, &raced)));
    assert!(matches!(result, Err(Error::Store(StoreError::Conflict { .. }))));
    assert_eq!(total(&sums), 110); // only the competing change
}

#[test]
fn updating_a_missing_entity_is_not_found() {
    let sums = Sums::new(InMemoryEventStore::new());
    let result: Result<(), Error> = block_on(sums.update(&7, |sum, events| sum.add(1, events)));
    assert_eq!(result, Err(Error::NotFound(7)));
}

#[test]
fn creating_twice_is_a_conflict() {
    let sums = sums_with(10);
    let result: Result<(), Error> = block_on(sums.create(&1, |sum, events| sum.add(1, events)));
    assert!(matches!(result, Err(Error::Store(StoreError::Conflict { .. }))));
    assert_eq!(total(&sums), 10);
}

#[test]
fn a_decision_that_appends_nothing_commits_nothing() {
    let sums = sums_with(10);
    let result: Result<&str, Error> = block_on(sums.update(&1, |_, _| Ok::<_, Refused>("nothing to do")));
    assert_eq!(result, Ok("nothing to do"));
    assert_eq!(block_on(sums.load(&1)).unwrap().unwrap().version(), Some(1));
}
