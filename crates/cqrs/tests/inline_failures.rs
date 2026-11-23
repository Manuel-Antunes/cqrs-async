//! A command sent through a gateway whose inline handler fails: the event is
//! committed anyway, and the failure is not lost. By default it is logged;
//! `on_failure` replaces that.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use cqrs::{
    Aggregate, Appender, CommandGateway, CommandHandler, EventAppender, EventEnvelope, EventHandler, EventSourced,
    EventStore, HandlerError, InMemoryEventStore, Loaded, ProcessingContext, ProcessingFailure, StoreError,
    WithPublisher, testing::block_on,
};
use tracing::{
    Event, Level, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent, cqrs::TagResolver)]
struct Ticked(u64);

struct Clock {
    id: u32,
}

impl Aggregate for Clock {
    const TYPE: &'static str = "clock";
    type Id = u32;
    type Event = Ticked;

    fn initial(id: &u32) -> Self {
        Self { id: *id }
    }

    fn id(&self) -> &u32 {
        &self.id
    }

    fn evolve(&mut self, _: &Ticked) {}
}

#[derive(cqrs::Command)]
#[command(output = (), error = StoreError)]
struct Tick {
    id: u32,
}

struct TickHandler;

#[cqrs::command_handler]
impl CommandHandler for TickHandler {
    async fn handle(&self, command: &Tick, events: &mut Appender<Ticked>) -> Result<(), StoreError> {
        let clock = Loaded::new(Clock::initial(&command.id));
        events.append(Ticked(1));
        events.commit(clock).await
    }
}

#[derive(cqrs::Gateway)]
#[gateway(commands)]
struct Commands {
    tick: TickHandler,
}

/// An inline handler that can be switched off.
struct Notifier {
    down: AtomicBool,
}

impl EventHandler<Ticked> for Notifier {
    async fn handle(&self, _: &EventEnvelope<Ticked>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        if self.down.load(Ordering::Relaxed) {
            return Err(HandlerError::msg("the notifier is down"));
        }
        Ok(())
    }
}

fn notifier(down: bool) -> Arc<Notifier> {
    Arc::new(Notifier {
        down: AtomicBool::new(down),
    })
}

/// Collects the events a `tracing` subscriber sees: (level, message).
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<(Level, String)>>>);

struct Message(String);

impl Visit for Message {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

impl Subscriber for Captured {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut message = Message(String::new());
        event.record(&mut message);
        self.0.lock().unwrap().push((*event.metadata().level(), message.0));
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

#[test]
fn a_failing_inline_handler_is_logged_by_default_and_the_event_is_committed() {
    let store = Arc::new(InMemoryEventStore::new());
    let commands = Commands { tick: TickHandler }.with_publisher(EventSourced::<Clock, _, _>::with_processor(
        Arc::clone(&store),
        (notifier(true),),
    ));

    let captured = Captured::default();
    tracing::subscriber::with_default(captured.clone(), || {
        block_on(commands.send(Tick { id: 1 })).unwrap();
    });

    assert_eq!(block_on(store.last_position()).unwrap(), 1, "committed anyway");
    let logged = captured.0.lock().unwrap().clone();
    assert!(
        logged
            .iter()
            .any(|(level, message)| *level == Level::ERROR && message.contains("inline")),
        "the failure was logged at error level, got {logged:?}"
    );
}

#[test]
fn on_failure_hands_the_failure_to_the_application() {
    let store = Arc::new(InMemoryEventStore::new());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let events = EventSourced::<Clock, _, _>::with_processor(Arc::clone(&store), (notifier(true),))
        .on_failure(move |failure: &ProcessingFailure| sink.lock().unwrap().push(failure.failures.len()));
    let commands = Commands { tick: TickHandler }.with_publisher(events);

    block_on(commands.send(Tick { id: 1 })).unwrap();

    assert_eq!(*seen.lock().unwrap(), [1]);
    assert_eq!(block_on(store.last_position()).unwrap(), 1);
}

#[test]
fn a_healthy_handler_reports_nothing() {
    let store = Arc::new(InMemoryEventStore::new());
    let seen = Arc::new(Mutex::new(0));
    let sink = Arc::clone(&seen);
    let events = EventSourced::<Clock, _, _>::with_processor(Arc::clone(&store), (notifier(false),))
        .on_failure(move |_: &ProcessingFailure| *sink.lock().unwrap() += 1);
    let commands = Commands { tick: TickHandler }.with_publisher(events);

    block_on(commands.send(Tick { id: 1 })).unwrap();

    assert_eq!(*seen.lock().unwrap(), 0);
}
