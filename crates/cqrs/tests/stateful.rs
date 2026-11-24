//! Command handlers (Axon 5 style): the gateway hands in an appender with
//! its publisher, the handler loads, decides and commits through it.

use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use cqrs::{
    Aggregate, AggregateLoader, Appender, CommandGateway, CommandHandler, EventAppender, EventPublisher,
    InMemoryEventStore, InlinePublisher, Loaded, NotFound, Pending, StoreError, testing::block_on,
};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum CounterEvent {
    Opened,
    Added(u64),
}

struct Counter {
    id: u32,
    total: u64,
}

impl Aggregate for Counter {
    const TYPE: &'static str = "counter";
    type Id = u32;
    type Event = CounterEvent;

    fn initial(id: &u32) -> Self {
        Self { id: *id, total: 0 }
    }

    fn id(&self) -> &u32 {
        &self.id
    }

    fn evolve(&mut self, event: &CounterEvent) {
        if let CounterEvent::Added(n) = event {
            self.total += n;
        }
    }
}

impl Counter {
    fn add(&self, n: u64, events: &mut impl EventAppender<CounterEvent>) -> Result<(), Error> {
        if self.total + n > 100 {
            return Err(Error::TooMuch);
        }
        events.append(CounterEvent::Added(n));
        Ok(())
    }
}

#[derive(Debug)]
enum Error {
    TooMuch,
    NotFound(u32),
    Store(StoreError),
}

impl From<NotFound<u32>> for Error {
    fn from(NotFound(id): NotFound<u32>) -> Self {
        Self::NotFound(id)
    }
}

/// Equal kinds; store errors by their message (they keep their cause, which
/// is not comparable).
impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Store(a), Self::Store(b)) => a.to_string() == b.to_string(),
            (Self::NotFound(a), Self::NotFound(b)) => a == b,
            (Self::TooMuch, Self::TooMuch) => true,
            _ => false,
        }
    }
}

impl From<StoreError> for Error {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

#[cqrs::command(output = u32, error = Error)]
struct Open(u32);

#[cqrs::command(output = u64, error = Error)]
struct Add {
    id: u32,
    n: u64,
}

type Store = InMemoryEventStore<CounterEvent>;

/// What the application has over the store: a loader for its repository and
/// the publisher its handlers commit through.
struct Engine {
    loader: AggregateLoader<Counter, Store>,
    publisher: InlinePublisher<CounterEvent, Store>,
}

impl Engine {
    async fn load(&self, id: &u32) -> Result<Option<Loaded<Counter>>, StoreError> {
        self.loader.load(id).await
    }

    fn store(&self) -> &Store {
        self.loader.store()
    }

    /// Another writer's commit, straight through the publisher.
    async fn append(&self, counter: Loaded<Counter>, events: Vec<CounterEvent>) -> Result<(), StoreError> {
        self.publisher.commit(counter, Pending::new(events)).await.map(|_| ())
    }
}

/// The engine runs the commands' units of work too, through its publisher:
/// in memory, one entity per command without a transaction, several with
/// the (empty) transaction of memory.
impl<U: cqrs::UnitMode> cqrs::UnitOfWork<CounterEvent, U> for Engine
where
    InlinePublisher<CounterEvent, Store>: cqrs::UnitOfWork<CounterEvent, U>,
{
    const ROLLS_BACK: bool = <InlinePublisher<CounterEvent, Store> as cqrs::UnitOfWork<CounterEvent, U>>::ROLLS_BACK;

    fn begin(&self) -> impl std::future::Future<Output = Result<U::Tx, StoreError>> + Send {
        self.publisher.begin()
    }

    fn append(
        &self,
        tx: &mut U::Tx,
        write: cqrs::Write<CounterEvent>,
    ) -> impl std::future::Future<Output = Result<Vec<cqrs::EventEnvelope<CounterEvent>>, StoreError>> + Send {
        self.publisher.append(tx, write)
    }

    fn commit(&self, tx: U::Tx) -> impl std::future::Future<Output = Result<(), StoreError>> + Send {
        cqrs::UnitOfWork::<CounterEvent, U>::commit(&self.publisher, tx)
    }

    fn announce(&self, events: Vec<cqrs::EventEnvelope<CounterEvent>>) -> impl std::future::Future<Output = ()> + Send {
        cqrs::UnitOfWork::<CounterEvent, U>::announce(&self.publisher, events)
    }
}

impl EventPublisher<CounterEvent> for Engine {
    fn commit<A: Aggregate<Event = CounterEvent>>(
        &self,
        entity: Loaded<A>,
        pending: Pending<CounterEvent>,
    ) -> impl std::future::Future<Output = Result<Option<u64>, StoreError>> + Send {
        self.publisher.commit(entity, pending)
    }
}

type Counters = Arc<Engine>;

/// Both commands, on one gateway. Adding counts its runs; on its first run
/// it can sneak a competing change in, as another writer would between the
/// load and the save.
#[cqrs::command_gateway(handles(Open, Add))]
struct Gateway {
    #[gateway(unit)]
    counters: Counters,
    runs: AtomicU32,
    racing: bool,
    retries: u32,
    forget_save: bool,
}

impl Gateway {
    fn new(counters: &Counters) -> Self {
        Self {
            counters: Arc::clone(counters),
            runs: AtomicU32::new(0),
            racing: false,
            retries: 8,
            forget_save: false,
        }
    }
}

/// Opening needs nothing, as a creation decides on nothing. Written by
/// hand, without `#[command_handler]`: the unit of work is named, `handle`
/// takes its transaction (none here, `()`), and the gateway is told it
/// handles `Open` (`Handles`, which the attribute writes otherwise).
impl CommandHandler<Open> for Gateway {
    type Event = CounterEvent;
    type Unit = cqrs::Autocommit;

    async fn handle(&self, Open(id): &Open, events: &mut Appender<CounterEvent>, (): &mut ()) -> Result<u32, Error> {
        events.append(CounterEvent::Opened);
        events.record(cqrs::New::<Counter>(*id));
        Ok(*id)
    }
}

impl cqrs::Handles<Open> for Gateway {}

#[cqrs::command_handler]
impl CommandHandler<Add> for Gateway {
    fn conflict_retries(&self) -> u32 {
        self.retries
    }

    async fn handle(&self, command: &Add, events: &mut Appender<CounterEvent>) -> Result<u64, Error> {
        let counter = self.counters.load(&command.id).await?.ok_or(NotFound(command.id))?;
        if self.runs.fetch_add(1, Ordering::Relaxed) == 0 && self.racing {
            let other = self.counters.load(&command.id).await?.unwrap();
            self.counters.append(other, vec![CounterEvent::Added(10)]).await?;
        }
        counter.add(command.n, events)?;
        if self.forget_save {
            return Ok(counter.total + command.n);
        }
        Ok(events.record(counter).total)
    }
}

fn counters() -> Counters {
    let store: Store = InMemoryEventStore::new();
    Arc::new(Engine {
        loader: AggregateLoader::new(store.clone()),
        publisher: InlinePublisher::new(store),
    })
}

fn total(counters: &Counters, id: u32) -> u64 {
    block_on(counters.load(&id)).unwrap().unwrap().total
}

#[test]
fn a_command_commits_what_its_handler_appended() {
    let counters = counters();
    let gateway = Gateway::new(&counters);
    assert_eq!(block_on(gateway.send(Open(1))), Ok(1));
    assert_eq!(block_on(gateway.send(Add { id: 1, n: 5 })), Ok(5));
    assert_eq!(total(&counters, 1), 5);
}

#[test]
fn an_existing_entity_is_required_unless_the_command_creates_it() {
    let counters = counters();
    let gateway = Gateway::new(&counters);
    assert_eq!(block_on(gateway.send(Add { id: 7, n: 1 })), Err(Error::NotFound(7)));
    block_on(gateway.send(Open(7))).unwrap();
    // Creating twice: a conflict, not retried (re-running would conflict again).
    assert!(matches!(
        block_on(gateway.send(Open(7))),
        Err(Error::Store(StoreError::Conflict { .. }))
    ));
}

#[test]
fn a_refused_decision_saves_nothing() {
    let counters = counters();
    let gateway = Gateway::new(&counters);
    block_on(gateway.send(Open(1))).unwrap();
    assert_eq!(block_on(gateway.send(Add { id: 1, n: 500 })), Err(Error::TooMuch));
    assert_eq!(block_on(counters.load(&1)).unwrap().unwrap().version(), Some(1));
}

#[test]
fn a_lost_race_runs_the_handler_again_on_the_fresh_entity() {
    let counters = counters();
    let racing = Gateway {
        racing: true,
        ..Gateway::new(&counters)
    };
    let gateway = racing;
    block_on(gateway.send(Open(1))).unwrap();

    // First run decides on total 0, loses to the competing +10; the rerun
    // decides on 10 and returns 15.
    assert_eq!(block_on(gateway.send(Add { id: 1, n: 5 })), Ok(15));
    assert_eq!(gateway.runs.load(Ordering::Relaxed), 2);
    assert_eq!(total(&counters, 1), 15);
}

#[test]
fn retries_run_out_with_a_conflict() {
    let counters = counters();
    let racing = Gateway {
        racing: true,
        retries: 0,
        ..Gateway::new(&counters)
    };
    let gateway = racing;
    block_on(gateway.send(Open(1))).unwrap();
    assert!(matches!(
        block_on(gateway.send(Add { id: 1, n: 5 })),
        Err(Error::Store(StoreError::Conflict { .. }))
    ));
    assert_eq!(total(&counters, 1), 10, "only the competing change");
}

#[test]
fn dispatching_is_send() {
    fn assert_send<T: Send>(value: T) -> T {
        value
    }
    let counters = counters();
    let gateway = Gateway::new(&counters);
    // Compiles only if the whole chain is `Send`: ready for `tokio::spawn`.
    assert_eq!(block_on(assert_send(gateway.send(Open(3)))), Ok(3));
}

/// Records the counter twice in one command: once fresh, once stale.
struct Twice(Counters);

impl CommandHandler<Add> for Twice {
    type Event = CounterEvent;
    type Unit = cqrs::InTransaction<()>;

    async fn handle(&self, command: &Add, events: &mut Appender<CounterEvent>, (): &mut ()) -> Result<u64, Error> {
        let stale = self.0.load(&command.id).await?.unwrap();
        let fresh = self.0.load(&command.id).await?.unwrap();
        events.append(CounterEvent::Added(1));
        events.record(fresh);
        events.append(CounterEvent::Added(1));
        events.record(stale);
        Ok(0)
    }
}

#[test]
fn a_unit_that_cannot_roll_back_is_not_run_again_after_a_partial_write() {
    // In memory nothing rolls back: the first write stays when the second
    // loses the race, so re-running would append it twice. The conflict
    // reaches the caller instead.
    let counters = counters();
    block_on(Gateway::new(&counters).send(Open(1))).unwrap();
    let twice = Twice(Arc::clone(&counters));
    assert!(matches!(
        block_on(cqrs::dispatch_command(
            &twice,
            Add { id: 1, n: 1 },
            cqrs::Metadata::new(),
            &counters
        )),
        Err(Error::Store(StoreError::Conflict { .. }))
    ));
    assert_eq!(total(&counters, 1), 1, "the first write, once");
}

#[test]
fn several_entities_without_a_transaction_are_refused() {
    struct TwiceWithoutTransaction(Counters);
    impl CommandHandler<Add> for TwiceWithoutTransaction {
        type Event = CounterEvent;
        type Unit = cqrs::Autocommit;
        async fn handle(&self, command: &Add, events: &mut Appender<CounterEvent>, (): &mut ()) -> Result<u64, Error> {
            Twice(Arc::clone(&self.0)).handle(command, events, &mut ()).await
        }
    }
    let counters = counters();
    block_on(Gateway::new(&counters).send(Open(1))).unwrap();
    let handler = TwiceWithoutTransaction(Arc::clone(&counters));
    assert!(matches!(
        block_on(cqrs::dispatch_command(
            &handler,
            Add { id: 1, n: 1 },
            cqrs::Metadata::new(),
            &counters
        )),
        Err(Error::Store(StoreError::Backend(_)))
    ));
    assert_eq!(total(&counters, 1), 0, "nothing appended");
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "never recorded")]
fn forgetting_to_save_panics_in_debug_builds() {
    let counters = counters();
    let forgetful = Gateway {
        forget_save: true,
        ..Gateway::new(&counters)
    };
    let gateway = forgetful;
    block_on(gateway.send(Open(1))).unwrap();
    let _ = block_on(gateway.send(Add { id: 1, n: 5 }));
}

#[test]
fn the_metadata_a_command_is_sent_with_is_stored_with_its_events() {
    let counters = counters();
    let correlation = cqrs::Metadata::from([("correlation_id".to_owned(), "req-42".to_owned())]);

    let gateway = Gateway::new(&counters);
    block_on(gateway.send_with(Open(1), correlation.clone())).unwrap();
    // Called directly too, and kept across a re-run after a lost race.
    let racing = Gateway {
        racing: true,
        ..Gateway::new(&counters)
    };
    block_on(cqrs::dispatch_command(
        &racing,
        Add { id: 1, n: 5 },
        correlation.clone(),
        &counters,
    ))
    .unwrap();

    let stored = counters.store().all();
    let tagged: Vec<_> = stored
        .iter()
        .map(|event| event.metadata.get("correlation_id").map(String::as_str))
        .collect();
    // Opened, the competing +10 (sent without metadata), then the +5.
    assert_eq!(tagged, [Some("req-42"), None, Some("req-42")]);
}
