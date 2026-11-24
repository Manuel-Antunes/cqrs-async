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

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent, cqrs::TagResolver)]
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

#[derive(cqrs::Command)]
#[command(output = u32, error = Error)]
struct Open(u32);

#[derive(cqrs::Command)]
#[command(output = u64, error = Error)]
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

struct OpenHandler(Counters);

impl CommandHandler for OpenHandler {
    type Command = Open;
    type Event = CounterEvent;

    async fn handle(&self, Open(id): &Open, events: &mut Appender<CounterEvent>) -> Result<u32, Error> {
        events.append(CounterEvent::Opened);
        events.commit(cqrs::New::<Counter>(*id), &self.0).await?;
        Ok(*id)
    }
}

/// Adds, and counts its runs. On its first run it can sneak a competing
/// change in, as another writer would between the load and the save.
struct AddHandler {
    counters: Counters,
    runs: AtomicU32,
    racing: bool,
    retries: u32,
    forget_save: bool,
}

impl AddHandler {
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

impl CommandHandler for AddHandler {
    type Command = Add;
    type Event = CounterEvent;

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
        let total = counter.total + command.n;
        if !self.forget_save {
            events.commit(counter, &self.counters).await?;
        }
        Ok(total)
    }
}

/// Both commands, routed by a derived gateway.
#[derive(cqrs::Gateway)]
#[gateway(commands)]
struct Gateway {
    open: OpenHandler,
    add: AddHandler,
}

fn counters() -> Counters {
    let store: Store = InMemoryEventStore::new();
    Arc::new(Engine {
        loader: AggregateLoader::new(store.clone()),
        publisher: InlinePublisher::new(store),
    })
}

/// The gateway, committing through the counters' engine.
fn gateway(counters: &Counters, add: AddHandler) -> Gateway {
    Gateway {
        open: OpenHandler(Arc::clone(counters)),
        add,
    }
}

fn total(counters: &Counters, id: u32) -> u64 {
    block_on(counters.load(&id)).unwrap().unwrap().total
}

#[test]
fn a_command_commits_what_its_handler_appended() {
    let counters = counters();
    let gateway = gateway(&counters, AddHandler::new(&counters));
    assert_eq!(block_on(gateway.send(Open(1))), Ok(1));
    assert_eq!(block_on(gateway.send(Add { id: 1, n: 5 })), Ok(5));
    assert_eq!(total(&counters, 1), 5);
}

#[test]
fn an_existing_entity_is_required_unless_the_command_creates_it() {
    let counters = counters();
    let gateway = gateway(&counters, AddHandler::new(&counters));
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
    let gateway = gateway(&counters, AddHandler::new(&counters));
    block_on(gateway.send(Open(1))).unwrap();
    assert_eq!(block_on(gateway.send(Add { id: 1, n: 500 })), Err(Error::TooMuch));
    assert_eq!(block_on(counters.load(&1)).unwrap().unwrap().version(), Some(1));
}

#[test]
fn a_lost_race_runs_the_handler_again_on_the_fresh_entity() {
    let counters = counters();
    let racing = AddHandler {
        racing: true,
        ..AddHandler::new(&counters)
    };
    let gateway = gateway(&counters, racing);
    block_on(gateway.send(Open(1))).unwrap();

    // First run decides on total 0, loses to the competing +10; the rerun
    // decides on 10 and returns 15.
    assert_eq!(block_on(gateway.send(Add { id: 1, n: 5 })), Ok(15));
    assert_eq!(gateway.add.runs.load(Ordering::Relaxed), 2);
    assert_eq!(total(&counters, 1), 15);
}

#[test]
fn retries_run_out_with_a_conflict() {
    let counters = counters();
    let racing = AddHandler {
        racing: true,
        retries: 0,
        ..AddHandler::new(&counters)
    };
    let gateway = gateway(&counters, racing);
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
    let gateway = gateway(&counters, AddHandler::new(&counters));
    // Compiles only if the whole chain is `Send`: ready for `tokio::spawn`.
    assert_eq!(block_on(assert_send(gateway.send(Open(3)))), Ok(3));
}

#[test]
fn a_save_after_another_save_is_not_retried() {
    // Two saves in one run: if the second loses the race, re-running would
    // save the first one twice, so the conflict reaches the caller.
    struct Twice(Counters);
    impl CommandHandler for Twice {
        type Command = Add;
        type Event = CounterEvent;
        async fn handle(&self, command: &Add, events: &mut Appender<CounterEvent>) -> Result<u64, Error> {
            let stale = self.0.load(&command.id).await?.unwrap();
            let fresh = self.0.load(&command.id).await?.unwrap();
            events.append(CounterEvent::Added(1));
            events.commit(fresh, &self.0).await?;
            events.append(CounterEvent::Added(1));
            events.commit(stale, &self.0).await?;
            Ok(0)
        }
    }
    let counters = counters();
    block_on(gateway(&counters, AddHandler::new(&counters)).send(Open(1))).unwrap();
    let twice = Twice(Arc::clone(&counters));
    assert!(matches!(
        block_on(cqrs::dispatch_command(
            &twice,
            Add { id: 1, n: 1 },
            cqrs::Metadata::new()
        )),
        Err(Error::Store(StoreError::Conflict { .. }))
    ));
    assert_eq!(total(&counters, 1), 1, "the first save, once");
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "never committed")]
fn forgetting_to_save_panics_in_debug_builds() {
    let counters = counters();
    let forgetful = AddHandler {
        forget_save: true,
        ..AddHandler::new(&counters)
    };
    let gateway = gateway(&counters, forgetful);
    block_on(gateway.send(Open(1))).unwrap();
    let _ = block_on(gateway.send(Add { id: 1, n: 5 }));
}

#[test]
fn the_metadata_a_command_is_sent_with_is_stored_with_its_events() {
    let counters = counters();
    let correlation = cqrs::Metadata::from([("correlation_id".to_owned(), "req-42".to_owned())]);

    let gateway = gateway(&counters, AddHandler::new(&counters));
    block_on(gateway.send_with(Open(1), correlation.clone())).unwrap();
    // Called directly too, and kept across a re-run after a lost race.
    let racing = AddHandler {
        racing: true,
        ..AddHandler::new(&counters)
    };
    block_on(cqrs::dispatch_command(
        &racing,
        Add { id: 1, n: 5 },
        correlation.clone(),
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
