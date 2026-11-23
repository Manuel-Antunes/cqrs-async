//! Subscriptions and tracking processors over the in-memory store (feature
//! `tokio`).

#![cfg(feature = "tokio")]
//!
//! Tests run with Tokio's paused clock: timers fire as soon as every task is
//! idle, so polling and backoff cost no real time.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use cqrs::{
    Aggregate, EventEnvelope, EventHandler, EventSourced, EventStore, ExpectedVersion, HandlerError,
    InMemoryEventStore, InMemoryTokenStore, LiveEvents, Metadata, ProcessingContext, StreamId, SubscriptionOptions,
    TrackingProcessor, subscribe,
};
use futures_util::StreamExt;
use tokio::sync::broadcast;

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent, cqrs::TagResolver)]
struct Ticked(u32);

struct Clock {
    id: u32,
    ticks: u32,
}

impl Aggregate for Clock {
    const TYPE: &'static str = "clock";
    type Id = u32;
    type Event = Ticked;

    fn initial(id: &u32) -> Self {
        Self { id: *id, ticks: 0 }
    }

    fn id(&self) -> &u32 {
        &self.id
    }

    fn evolve(&mut self, _: &Ticked) {
        self.ticks += 1;
    }
}

type Store = Arc<InMemoryEventStore<Ticked>>;

struct World {
    store: Store,
    live: LiveEvents<Ticked>,
    clocks: EventSourced<Clock, Store, (LiveEvents<Ticked>,)>,
}

impl World {
    fn new(capacity: usize) -> Self {
        let store = Arc::new(InMemoryEventStore::new());
        let live = broadcast::channel(capacity).0;
        let clocks = EventSourced::with_processor(Arc::clone(&store), (live.clone(),));
        Self { store, live, clocks }
    }

    /// Commits `n` ticks on clock 1 through the repository: stored, then
    /// broadcast.
    async fn tick(&self, n: u32) {
        let clock = match self.clocks.load(&1).await.unwrap() {
            Some(clock) => clock,
            None => cqrs::Loaded::new(Clock::initial(&1)),
        };
        let ticks: Vec<_> = (1..=n).map(|i| Ticked(clock.ticks + i)).collect();
        self.clocks.append(clock, ticks).await.unwrap();
    }

    /// Commits a tick straight to the store: what a save cancelled between
    /// its append and its processing leaves behind (never broadcast).
    async fn tick_without_broadcast(&self) {
        let stream = StreamId::of::<Clock>(&1);
        let ticks = self.store.load(&stream, 0).await.unwrap().len() as u32;
        self.store
            .append(
                &stream,
                match ticks {
                    0 => ExpectedVersion::NoStream,
                    n => ExpectedVersion::Exact(u64::from(n)),
                },
                vec![Ticked(ticks + 1)],
                &Metadata::new(),
            )
            .await
            .unwrap();
    }

    fn subscribe(&self, after: u64) -> impl futures_core::Stream<Item = u64> + use<> {
        subscribe(Arc::clone(&self.store), &self.live, after, options()).map(|event| event.unwrap().position)
    }
}

fn options() -> SubscriptionOptions {
    SubscriptionOptions {
        batch_size: 4,
        poll_interval: Duration::from_millis(100),
    }
}

async fn take(stream: impl futures_core::Stream<Item = u64>, n: usize) -> Vec<u64> {
    let stream = std::pin::pin!(stream.take(n));
    tokio::time::timeout(Duration::from_secs(60), stream.collect())
        .await
        .expect("the subscription stalled")
}

#[tokio::test(start_paused = true)]
async fn catches_up_from_the_store_then_follows_live_events() {
    let world = World::new(16);
    world.tick(10).await; // history: more than two pages
    let events = world.subscribe(0);
    world.tick(3).await; // live
    assert_eq!(take(events, 13).await, (1..=13).collect::<Vec<_>>());
}

#[tokio::test(start_paused = true)]
async fn starts_after_the_given_position() {
    let world = World::new(16);
    world.tick(5).await;
    assert_eq!(take(world.subscribe(3), 2).await, [4, 5]);
}

#[tokio::test(start_paused = true)]
async fn a_lost_broadcast_is_recovered_from_the_gap() {
    let world = World::new(16);
    let events = world.subscribe(0);
    world.tick(1).await;
    world.tick_without_broadcast().await; // position 2, never broadcast
    world.tick(1).await; // position 3 reveals the gap
    assert_eq!(take(events, 3).await, [1, 2, 3]);
}

#[tokio::test(start_paused = true)]
async fn a_lost_last_broadcast_is_recovered_by_polling() {
    let world = World::new(16);
    let events = world.subscribe(0);
    world.tick_without_broadcast().await; // nothing after it to show a gap
    assert_eq!(take(events, 1).await, [1]);
}

#[tokio::test(start_paused = true)]
async fn a_slow_subscriber_loses_nothing() {
    let world = World::new(2); // the live buffer holds only 2 events
    let events = world.subscribe(0);
    for _ in 0..10 {
        world.tick(1).await; // the subscriber is not reading meanwhile
    }
    assert_eq!(take(events, 10).await, (1..=10).collect::<Vec<_>>());
}

/// Records what it handles; fails its first `failures` calls.
#[derive(Default)]
struct Recorder {
    handled: Mutex<Vec<u64>>,
    failures: AtomicU32,
}

impl Recorder {
    fn failing(times: u32) -> Self {
        Self {
            failures: AtomicU32::new(times),
            ..Self::default()
        }
    }

    fn handled(&self) -> Vec<u64> {
        self.handled.lock().unwrap().clone()
    }
}

impl EventHandler<Ticked> for Recorder {
    async fn handle(&self, event: &EventEnvelope<Ticked>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        if self.failures.load(Ordering::Relaxed) > 0 {
            self.failures.fetch_sub(1, Ordering::Relaxed);
            return Err(HandlerError::msg("database down"));
        }
        self.handled.lock().unwrap().push(event.position);
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn a_tracking_processor_retries_in_order_until_it_succeeds() {
    let world = World::new(16);
    let recorder = Arc::new(Recorder::failing(3));
    let checkpoints = Arc::new(InMemoryTokenStore::default());
    let processor = TrackingProcessor::new(
        "recorder",
        Arc::clone(&world.store),
        &world.live,
        Arc::clone(&recorder),
        Arc::clone(&checkpoints),
    )
    .spawn();

    world.tick(3).await;
    processor.caught_up(3).await;

    assert_eq!(recorder.handled(), [1, 2, 3]); // in order, none skipped
    assert_eq!(processor.status().position, 3);
    assert_eq!(processor.status().failing, None);
    assert_eq!(checkpoints.positions("recorder"), [3]);
    processor.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_restarted_processor_resumes_from_its_checkpoint() {
    let world = World::new(16);
    let checkpoints = Arc::new(InMemoryTokenStore::default());
    let first = Arc::new(Recorder::default());
    let processor = TrackingProcessor::new(
        "recorder",
        Arc::clone(&world.store),
        &world.live,
        Arc::clone(&first),
        Arc::clone(&checkpoints),
    )
    .spawn();
    world.tick(2).await;
    processor.caught_up(2).await;
    processor.stop().await;

    world.tick(2).await; // committed while it was down

    let second = Arc::new(Recorder::default());
    let processor = TrackingProcessor::new(
        "recorder",
        Arc::clone(&world.store),
        &world.live,
        Arc::clone(&second),
        Arc::clone(&checkpoints),
    )
    .spawn();
    processor.caught_up(4).await;

    assert_eq!(first.handled(), [1, 2]);
    assert_eq!(second.handled(), [3, 4]); // nothing handled twice
    processor.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_failing_processor_reports_it_and_does_not_block_others() {
    let world = World::new(16);
    let stuck = Arc::new(Recorder::failing(u32::MAX));
    let healthy = Arc::new(Recorder::default());
    let checkpoints = Arc::new(InMemoryTokenStore::default());
    let stuck_processor = TrackingProcessor::new(
        "stuck",
        Arc::clone(&world.store),
        &world.live,
        Arc::clone(&stuck),
        Arc::clone(&checkpoints),
    )
    .spawn();
    let healthy_processor = TrackingProcessor::new(
        "healthy",
        Arc::clone(&world.store),
        &world.live,
        Arc::clone(&healthy),
        Arc::clone(&checkpoints),
    )
    .spawn();

    world.tick(2).await;
    healthy_processor.caught_up(2).await;
    tokio::time::sleep(Duration::from_secs(30)).await; // many retries

    assert_eq!(healthy.handled(), [1, 2]);
    assert!(stuck.handled().is_empty());
    let status = stuck_processor.status();
    assert_eq!(status.position, 0);
    assert_eq!(status.failing.as_deref(), Some("database down"));

    stuck_processor.stop().await; // stops even while retrying
    healthy_processor.stop().await;
}
