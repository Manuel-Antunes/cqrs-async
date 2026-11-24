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
    Aggregate, AggregateLoader, AppendEvents, EventEnvelope, EventHandler, EventPublisher, ExpectedVersion,
    HandlerError, InMemoryEventStore, InMemoryTokenStore, InlinePublisher, LiveEvents, LoadStream, Metadata, Pending,
    ProcessingContext, StreamId, SubscriptionOptions, TokenStore, TrackingProcessor, subscribe,
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

type Store = InMemoryEventStore<Ticked>;

struct World {
    store: Store,
    live: LiveEvents<Ticked>,
    clocks: AggregateLoader<Clock, Store>,
    publisher: InlinePublisher<Ticked, Store, (LiveEvents<Ticked>,)>,
}

impl World {
    fn new(capacity: usize) -> Self {
        let store = InMemoryEventStore::new();
        let live = broadcast::channel(capacity).0;
        let clocks = AggregateLoader::new(store.clone());
        let publisher = InlinePublisher::new(store.clone()).with_processor((live.clone(),));
        Self {
            store,
            live,
            clocks,
            publisher,
        }
    }

    /// Commits `n` ticks on clock 1 through the repository: stored, then
    /// broadcast.
    async fn tick(&self, n: u32) {
        let clock = match self.clocks.load(&1).await.unwrap() {
            Some(clock) => clock,
            None => cqrs::New(1).into(),
        };
        let ticks: Vec<_> = (1..=n).map(|i| Ticked(clock.ticks + i)).collect();
        self.publisher.commit(clock, Pending::new(ticks)).await.unwrap();
    }

    /// Commits a tick straight to the store: what a save cancelled between
    /// its append and its processing leaves behind (never broadcast).
    async fn tick_without_broadcast(&self) {
        let stream = StreamId::of::<Clock>(&1);
        let ticks = self.store.load(&stream, 0).await.unwrap().len() as u32;
        self.store
            .append(
                &mut (),
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
        subscribe(self.store.clone(), &self.live, after, options()).map(|event| event.unwrap().position)
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
    let checkpoints = InMemoryTokenStore::default();
    let processor = TrackingProcessor::new(
        "recorder",
        world.store.clone(),
        &world.live,
        Arc::clone(&recorder),
        checkpoints.clone(),
        (),
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
    let checkpoints = InMemoryTokenStore::default();
    let first = Arc::new(Recorder::default());
    let processor = TrackingProcessor::new(
        "recorder",
        world.store.clone(),
        &world.live,
        Arc::clone(&first),
        checkpoints.clone(),
        (),
    )
    .spawn();
    world.tick(2).await;
    processor.caught_up(2).await;
    processor.stop().await;

    world.tick(2).await; // committed while it was down

    let second = Arc::new(Recorder::default());
    let processor = TrackingProcessor::new(
        "recorder",
        world.store.clone(),
        &world.live,
        Arc::clone(&second),
        checkpoints.clone(),
        (),
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
    let checkpoints = InMemoryTokenStore::default();
    let stuck_processor = TrackingProcessor::new(
        "stuck",
        world.store.clone(),
        &world.live,
        Arc::clone(&stuck),
        checkpoints.clone(),
        (),
    )
    .spawn();
    let healthy_processor = TrackingProcessor::new(
        "healthy",
        world.store.clone(),
        &world.live,
        Arc::clone(&healthy),
        checkpoints.clone(),
        (),
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

/// `run` is a future the application runs where it wants (here a
/// `JoinSet`), observed through `progress`, ended by its shutdown signal:
/// the segments are released for the next instance.
#[tokio::test(start_paused = true)]
async fn a_processor_runs_as_a_future_until_its_shutdown() {
    let world = World::new(16);
    let tokens = InMemoryTokenStore::default();
    let recorder = Arc::new(Recorder::default());
    let processor = TrackingProcessor::new(
        "recorder",
        world.store.clone(),
        &world.live,
        Arc::clone(&recorder),
        tokens.clone(),
        (),
    );
    let mut progress = processor.progress();
    let (shutdown, signal) = tokio::sync::oneshot::channel::<()>();
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(processor.run(async move {
        let _ = signal.await;
    }));

    world.tick(2).await;
    progress.wait_for(|status| status.covers(2)).await.unwrap();
    shutdown.send(()).unwrap();
    tasks.join_next().await.unwrap().unwrap();

    assert_eq!(recorder.handled(), [1, 2]);
    assert_eq!(tokens.positions("recorder"), [2]);
    // Released: another instance claims the segment at once.
    assert!(
        tokens.claim("recorder", 0, "next", 0, 1).await.unwrap().is_some(),
        "the segment was released"
    );
}
