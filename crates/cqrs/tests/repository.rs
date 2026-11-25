//! The loader and the publisher: what a repository adapter and a command
//! handler's commit do between them. Rules of the domain are tested with
//! `AggregateFixture`; re-running a decision that lost a race belongs to the
//! gateway (see `stateful.rs`).

use std::sync::Arc;

use cqrs::{
    aggregate::{Aggregate, AggregateLoader, EventAppender, Loaded, New, Repository},
    command::{CommandContext, EventPublisher, Pending},
    event::Metadata,
    processing::InlinePublisher,
    store::{InMemoryEventStore, StoreError},
    testing::{AggregateFixture, block_on},
};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
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
        sums.add(cqrs::aggregate::New::<Sum>(1).into(), total).unwrap();
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
    let refused = sums.add(cqrs::aggregate::New::<Sum>(1).into(), 1);
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

/// A repository that keeps the state of the sums itself: what `save`
/// writes goes to the unit's transaction (here, a list of statements).
struct Kept;

impl Repository<Sum> for Kept {
    type Tx = Vec<String>;

    async fn find(&self, id: &u32, tx: &mut Vec<String>) -> Result<Option<Loaded<Sum>>, StoreError> {
        tx.push(format!("select {id}"));
        Ok(Some(Loaded::at(Sum { id: *id, total: 7 }, 3)))
    }

    async fn save(&self, sum: &Loaded<Sum>, tx: &mut Vec<String>) -> Result<(), StoreError> {
        tx.push(format!(
            "upsert {} total {} version {:?}",
            sum.id,
            sum.total,
            sum.version()
        ));
        Ok(())
    }
}

#[test]
fn committing_then_saving_keeps_the_entity_after_its_events_in_the_transaction() {
    let mut ctx = CommandContext::in_transaction(Metadata::new(), Vec::new());
    let sum = block_on(Kept.find(&1, ctx.tx())).unwrap().unwrap();
    sum.add(5, &mut ctx).unwrap();
    let saved = ctx.commit(sum);
    block_on(Kept.save(&saved, ctx.tx())).unwrap();
    assert_eq!(
        (saved.total, saved.version()),
        (12, Some(4)),
        "the entity after its events"
    );
    assert_eq!(ctx.tx(), &["select 1", "upsert 1 total 12 version Some(4)"]);
    assert!(ctx.pending().is_empty());
    let (writes, tx) = ctx.into_parts();
    assert_eq!(tx.len(), 2);
    assert_eq!(writes.len(), 1, "the events are appended by the unit of work, once");
    assert_eq!(writes[0].events, [Added(5)]);
}

#[test]
fn a_shared_repository_forwards_find_and_save() {
    let kept = Arc::new(Kept);
    let mut ctx = CommandContext::in_transaction(Metadata::new(), Vec::new());
    let sum = block_on(kept.find(&1, ctx.tx())).unwrap().unwrap();
    sum.add(1, &mut ctx).unwrap();
    let sum = ctx.commit(sum);
    block_on(kept.save(&sum, ctx.tx())).unwrap();
    assert_eq!(ctx.tx(), &["select 1", "upsert 1 total 8 version Some(4)"]);
}

#[test]
fn a_loader_is_a_repository_for_any_transaction_and_saves_nothing() {
    let store: Store = Store::default();
    let sums = AggregateLoader::<Sum, _>::new(Store::clone(&store)).in_transaction::<Vec<String>>();
    let mut ctx = CommandContext::in_transaction(Metadata::new(), Vec::new());
    assert!(block_on(sums.find(&1, ctx.tx())).unwrap().is_none());
    Sum::initial(&1).add(4, &mut ctx).unwrap();
    let saved = ctx.commit(New::<Sum>(1));
    block_on(sums.save(&saved, ctx.tx())).unwrap();
    assert_eq!((saved.total, saved.version()), (4, Some(1)));
    assert!(
        ctx.tx().is_empty(),
        "its events are its state: nothing written in the transaction"
    );
    assert_eq!(ctx.writes().len(), 1);
}
