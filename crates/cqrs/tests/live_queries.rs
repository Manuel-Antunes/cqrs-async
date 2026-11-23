//! Subscription queries: the current result, then each change, only after
//! the projection handled it; filtered, deduplicated and coalesced.

#![cfg(feature = "tokio")]

use std::{
    collections::HashMap,
    pin::pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use cqrs::{
    Aggregate, EventEnvelope, EventHandler, EventPublisher, Events, Handler, HandlerError, InMemoryEventStore,
    LiveQueries, ProcessingContext, QuerySubscriptions, RefreshedBy, StoreError,
};
use futures_util::{Stream, StreamExt};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent, cqrs::TagResolver)]
enum ClockEvent {
    Ticked,
    Renamed(String),
}

struct Clock {
    id: u32,
}

impl Aggregate for Clock {
    const TYPE: &'static str = "clock";
    type Id = u32;
    type Event = ClockEvent;

    fn initial(id: &u32) -> Self {
        Self { id: *id }
    }

    fn id(&self) -> &u32 {
        &self.id
    }

    fn evolve(&mut self, _: &ClockEvent) {}
}

/// Read model: ticks per clock. Renames do not change it.
#[derive(Default)]
struct Ticks(Mutex<HashMap<String, u64>>);

impl EventHandler<ClockEvent> for Ticks {
    async fn handle(
        &self,
        event: &EventEnvelope<ClockEvent>,
        _: &mut ProcessingContext<'_>,
    ) -> Result<(), HandlerError> {
        if event.payload == ClockEvent::Ticked {
            *self.0.lock().unwrap().entry(event.stream.id().to_owned()).or_default() += 1;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, cqrs::Query)]
#[query(output = u64, error = StoreError)]
struct TicksOf(u32);

/// Re-run only for events of this clock.
impl RefreshedBy<ClockEvent> for TicksOf {
    fn refreshed_by(&self, event: &EventEnvelope<ClockEvent>) -> bool {
        event.stream.id() == self.0.to_string()
    }
}

struct TicksOfHandler(Arc<Ticks>);

impl Handler for TicksOfHandler {
    type Message = TicksOf;

    async fn handle(&self, TicksOf(id): TicksOf) -> Result<u64, StoreError> {
        Ok(self.0.0.lock().unwrap().get(&id.to_string()).copied().unwrap_or(0))
    }
}

#[derive(cqrs::Gateway)]
struct ClockQueries {
    ticks: TicksOfHandler,
}

type Store = Arc<InMemoryEventStore<ClockEvent>>;

struct World {
    clocks: cqrs::AggregateLoader<Clock, Store>,
    events: Events<ClockEvent, Store>,
    queries: LiveQueries<ClockQueries, Events<ClockEvent, Store>, ClockEvent>,
    projection: cqrs::ProcessorHandle,
}

impl World {
    fn new() -> Self {
        let events = Events::new(Arc::new(InMemoryEventStore::new()));
        let ticks = Arc::new(Ticks::default());
        let projection = events.track("ticks", Arc::clone(&ticks));
        let queries = LiveQueries::new(
            ClockQueries {
                ticks: TicksOfHandler(ticks),
            },
            events.clone(),
            &projection,
        );
        Self {
            clocks: cqrs::AggregateLoader::new(Arc::clone(events.store())),
            events,
            queries,
            projection,
        }
    }

    async fn record(&self, id: u32, event: ClockEvent) {
        let clock = match self.clocks.load(&id).await.unwrap() {
            Some(clock) => clock,
            None => cqrs::Loaded::new(Clock::initial(&id)),
        };
        self.events
            .commit(clock, cqrs::Pending::new(vec![event]))
            .await
            .unwrap();
    }
}

async fn next<T>(stream: &mut (impl Stream<Item = Result<T, StoreError>> + Unpin)) -> T {
    tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("no update")
        .expect("stream ended")
        .unwrap()
}

/// Nothing arrives within a moment (the paused clock makes it instant).
async fn quiet<T: std::fmt::Debug>(stream: &mut (impl Stream<Item = Result<T, StoreError>> + Unpin)) {
    if let Ok(item) = tokio::time::timeout(Duration::from_secs(3), stream.next()).await {
        panic!("unexpected update {item:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn starts_with_the_current_result_then_follows_changes() {
    let world = World::new();
    world.record(1, ClockEvent::Ticked).await;
    world.record(1, ClockEvent::Ticked).await;

    let mut ticks = pin!(world.queries.subscribe(TicksOf(1)));
    assert_eq!(next(&mut ticks).await, 2); // current state, after projection

    world.record(1, ClockEvent::Ticked).await;
    assert_eq!(next(&mut ticks).await, 3);
    world.projection.stop().await;
}

#[tokio::test(start_paused = true)]
async fn unrelated_and_unchanging_events_emit_nothing() {
    let world = World::new();
    let mut ticks = pin!(world.queries.subscribe(TicksOf(1)));
    assert_eq!(next(&mut ticks).await, 0);

    world.record(2, ClockEvent::Ticked).await; // another clock: filtered out
    world.record(1, ClockEvent::Renamed("a".into())).await; // re-run, same result: deduplicated
    quiet(&mut ticks).await;

    world.record(1, ClockEvent::Ticked).await;
    assert_eq!(next(&mut ticks).await, 1);
    world.projection.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_burst_ends_on_the_final_result_and_never_goes_back() {
    let world = World::new();
    let mut ticks = pin!(world.queries.subscribe(TicksOf(1)));
    assert_eq!(next(&mut ticks).await, 0);

    for _ in 0..50 {
        world.record(1, ClockEvent::Ticked).await;
    }
    let mut seen = vec![];
    while seen.last() != Some(&50) {
        seen.push(next(&mut ticks).await);
    }
    assert!(seen.windows(2).all(|w| w[0] < w[1]), "monotonic: {seen:?}");
    assert!(seen.len() <= 50, "coalesced, at most one per event: {}", seen.len());
    world.projection.stop().await;
}

#[tokio::test(start_paused = true)]
async fn each_subscriber_gets_its_own_query_and_pipeline() {
    let world = World::new();
    let one = world.queries.subscribe(TicksOf(1));
    // RxJS-style operators are plain `StreamExt` combinators on the result.
    let mut doubled_two = pin!(world.queries.subscribe(TicksOf(2)).map(|r| r.map(|n| n * 2)));
    let mut one = pin!(one.filter(|r| std::future::ready(matches!(r, Ok(n) if n % 2 == 0))));

    assert_eq!(next(&mut one).await, 0);
    assert_eq!(next(&mut doubled_two).await, 0);
    world.record(2, ClockEvent::Ticked).await;
    world.record(1, ClockEvent::Ticked).await; // odd: filtered by this subscriber
    world.record(1, ClockEvent::Ticked).await;
    assert_eq!(next(&mut doubled_two).await, 2);
    assert_eq!(next(&mut one).await, 2);
    world.projection.stop().await;
}

#[tokio::test(start_paused = true)]
async fn plain_queries_still_work_through_the_same_gateway() {
    use cqrs::QueryGateway;
    let world = World::new();
    world.record(1, ClockEvent::Ticked).await;
    world.projection.caught_up(1).await;
    assert_eq!(world.queries.query(TicksOf(1)).await, Ok(1));
    world.projection.stop().await;
}

#[tokio::test(start_paused = true)]
async fn global_progress_waits_for_every_segment_of_every_instance() {
    use cqrs::{GlobalProgress, InMemoryTokenStore, ProjectionProgress, TokenStore};
    let tokens = Arc::new(InMemoryTokenStore::default());
    tokens.initialize("p", 2).await.unwrap();
    tokens.claim("p", 0, "a", 0, u64::MAX).await.unwrap();
    tokens.claim("p", 1, "b", 0, u64::MAX).await.unwrap();
    let progress = GlobalProgress::new(Arc::clone(&tokens), "p");

    tokens.store("p", 0, "a", &cqrs::Cursor::at(5)).await.unwrap(); // only one instance got there
    let waiting = tokio::spawn({
        let progress = progress.clone();
        async move { progress.reached(5).await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!waiting.is_finished(), "segment 1 is still behind");

    tokens.store("p", 1, "b", &cqrs::Cursor::at(7)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .expect("released")
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn live_queries_follow_a_projection_split_across_instances() {
    use cqrs::{QueryGateway, StreamId, TrackingOptions, segment_of};
    let events: Events<ClockEvent, Store> = Events::new(Arc::new(InMemoryEventStore::new()));
    let ticks = Arc::new(Ticks::default());
    let options = TrackingOptions {
        segments: 2,
        max_segments: 1,
        ..TrackingOptions::default()
    };
    // Two instances of the projection, one segment each.
    let a = events
        .processor("ticks", Arc::clone(&ticks))
        .with_options(options)
        .spawn();
    let b = events
        .processor("ticks", Arc::clone(&ticks))
        .with_options(options)
        .spawn();
    let queries = LiveQueries::with_progress(
        ClockQueries {
            ticks: TicksOfHandler(Arc::clone(&ticks)),
        },
        events.clone(),
        events.global_progress("ticks"),
    );
    let clocks = cqrs::AggregateLoader::<Clock, _>::new(Arc::clone(events.store()));
    let in_segment = |segment| {
        (1..)
            .find(|id| segment_of(&StreamId::of::<Clock>(id), 2) == segment)
            .unwrap()
    };
    let (first, second) = (in_segment(0), in_segment(1));

    let mut on_first = pin!(queries.subscribe(TicksOf(first)));
    let mut on_second = pin!(queries.subscribe(TicksOf(second)));
    assert_eq!(next(&mut on_first).await, 0);
    assert_eq!(next(&mut on_second).await, 0);
    for id in [first, second, second] {
        let clock = match clocks.load(&id).await.unwrap() {
            Some(clock) => clock,
            None => cqrs::Loaded::new(Clock::initial(&id)),
        };
        events
            .commit(clock, cqrs::Pending::new(vec![ClockEvent::Ticked]))
            .await
            .unwrap();
    }
    // Each update shows the projected state, whichever instance projected it.
    assert_eq!(next(&mut on_first).await, 1);
    let mut seen = vec![next(&mut on_second).await];
    while seen.last() != Some(&2) {
        seen.push(next(&mut on_second).await);
    }
    assert_eq!(queries.query(TicksOf(second)).await, Ok(2));
    a.stop().await;
    b.stop().await;
}
