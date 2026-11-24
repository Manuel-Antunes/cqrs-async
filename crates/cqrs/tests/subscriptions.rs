//! Subscription queries with emitters: the current result once the
//! projection caught up, then what the emitter pushes once each event is
//! projected, on every instance, whichever projected it; deduplicated.

#![cfg(feature = "tokio")]

use std::{
    collections::HashMap,
    pin::pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use cqrs::{
    Aggregate, EmitUpdates, EventEnvelope, EventHandler, EventPublisher, Events, Handler, HandlerError,
    InMemoryEventStore, ProcessingContext, ProcessorProgress, QuerySubscriptions, QueryUpdateEmitter, StoreError,
    SubscriptionQueries, emit_updates, head_of,
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

impl Ticks {
    fn of(&self, id: &str) -> u64 {
        self.0.lock().unwrap().get(id).copied().unwrap_or(0)
    }
}

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

struct TicksOfHandler(Arc<Ticks>);

impl Handler for TicksOfHandler {
    type Message = TicksOf;

    async fn handle(&self, TicksOf(id): TicksOf) -> Result<u64, StoreError> {
        Ok(self.0.of(&id.to_string()))
    }
}

#[derive(cqrs::Gateway)]
struct ClockQueries {
    ticks: TicksOfHandler,
}

/// Where the queries' new results go.
#[derive(Default, cqrs::QueryUpdates)]
struct ClockUpdates {
    ticks: QueryUpdateEmitter<TicksOf>,
}

/// Once an event is projected: the new count of its clock, to that clock's
/// subscribers.
struct EmitTicks {
    ticks: Arc<Ticks>,
    updates: Arc<ClockUpdates>,
}

impl EmitUpdates<ClockEvent> for EmitTicks {
    async fn emit(&self, event: &EventEnvelope<ClockEvent>) {
        let id = event.stream.id();
        let count = self.ticks.of(id);
        self.updates.ticks.emit(|query| query.0.to_string() == id, count);
    }
}

type Store = InMemoryEventStore<ClockEvent>;

/// One instance: its projection, its emitter task, its subscriptions.
struct Instance {
    queries: SubscriptionQueries<
        ClockQueries,
        ClockUpdates,
        cqrs::Projected<Events<ClockEvent, Store>, ProcessorProgress, ClockEvent>,
    >,
    projection: cqrs::ProcessorHandle,
    emitting: tokio::task::JoinHandle<()>,
}

impl Instance {
    /// Its emitter starts at the head, before it serves subscriptions.
    async fn new(events: &Events<ClockEvent, Store>, ticks: &Arc<Ticks>, options: cqrs::TrackingOptions) -> Self {
        let projection = events
            .processor("ticks", Arc::clone(ticks))
            .with_options(options)
            .spawn();
        let progress = ProcessorProgress::of(&projection);
        let updates = Arc::new(ClockUpdates::default());
        let emitter = EmitTicks {
            ticks: Arc::clone(ticks),
            updates: Arc::clone(&updates),
        };
        let emitting = tokio::spawn(emit_updates(
            head_of(events).await,
            events.clone(),
            progress.clone(),
            emitter,
            std::future::pending(),
        ));
        let queries = SubscriptionQueries::new(
            ClockQueries {
                ticks: TicksOfHandler(Arc::clone(ticks)),
            },
            updates,
        )
        .after_projection(events.clone(), progress);
        Self {
            queries,
            projection,
            emitting,
        }
    }

    async fn stop(self) {
        self.emitting.abort();
        self.projection.stop().await;
    }
}

struct World {
    clocks: cqrs::AggregateLoader<Clock, Store>,
    events: Events<ClockEvent, Store>,
    instance: Instance,
}

impl World {
    async fn new() -> Self {
        let events = Events::new(InMemoryEventStore::new());
        let ticks = Arc::new(Ticks::default());
        let instance = Instance::new(&events, &ticks, cqrs::TrackingOptions::default()).await;
        Self {
            clocks: cqrs::AggregateLoader::new(events.store().clone()),
            events,
            instance,
        }
    }

    async fn record(&self, id: u32, event: ClockEvent) {
        record(&self.clocks, &self.events, id, event).await;
    }
}

async fn record(
    clocks: &cqrs::AggregateLoader<Clock, Store>,
    events: &Events<ClockEvent, Store>,
    id: u32,
    event: ClockEvent,
) {
    let clock = match clocks.load(&id).await.unwrap() {
        Some(clock) => clock,
        None => cqrs::New(id).into(),
    };
    events.commit(clock, cqrs::Pending::new(vec![event])).await.unwrap();
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
    let world = World::new().await;
    world.record(1, ClockEvent::Ticked).await;
    world.record(1, ClockEvent::Ticked).await;

    let mut ticks = pin!(world.instance.queries.subscribe(TicksOf(1)));
    assert_eq!(next(&mut ticks).await, 2); // current state, after projection

    world.record(1, ClockEvent::Ticked).await;
    assert_eq!(next(&mut ticks).await, 3);
    world.instance.stop().await;
}

#[tokio::test(start_paused = true)]
async fn unrelated_and_unchanging_events_emit_nothing() {
    let world = World::new().await;
    let mut ticks = pin!(world.instance.queries.subscribe(TicksOf(1)));
    assert_eq!(next(&mut ticks).await, 0);

    world.record(2, ClockEvent::Ticked).await; // another clock: filtered out
    world.record(1, ClockEvent::Renamed("a".into())).await; // same result: deduplicated
    quiet(&mut ticks).await;

    world.record(1, ClockEvent::Ticked).await;
    assert_eq!(next(&mut ticks).await, 1);
    world.instance.stop().await;
}

#[tokio::test(start_paused = true)]
async fn each_subscriber_gets_its_own_query_and_pipeline() {
    let world = World::new().await;
    let one = world.instance.queries.subscribe(TicksOf(1));
    // RxJS-style operators are plain `StreamExt` combinators on the result.
    let mut doubled_two = pin!(world.instance.queries.subscribe(TicksOf(2)).map(|r| r.map(|n| n * 2)));
    let mut one = pin!(one.filter(|r| std::future::ready(matches!(r, Ok(n) if n % 2 == 0))));

    assert_eq!(next(&mut one).await, 0);
    assert_eq!(next(&mut doubled_two).await, 0);
    world.record(2, ClockEvent::Ticked).await;
    world.record(1, ClockEvent::Ticked).await; // odd: filtered by this subscriber
    world.record(1, ClockEvent::Ticked).await;
    assert_eq!(next(&mut doubled_two).await, 2);
    assert_eq!(next(&mut one).await, 2);
    world.instance.stop().await;
}

#[tokio::test(start_paused = true)]
async fn plain_queries_still_work_through_the_same_gateway() {
    use cqrs::QueryGateway;
    let world = World::new().await;
    world.record(1, ClockEvent::Ticked).await;
    world.instance.projection.caught_up(1).await;
    assert_eq!(world.instance.queries.query(TicksOf(1)).await.unwrap(), 1);
    world.instance.stop().await;
}

#[tokio::test(start_paused = true)]
async fn global_progress_waits_for_every_segment_of_every_instance() {
    use cqrs::{GlobalProgress, InMemoryTokenStore, ProjectionProgress, TokenStore, TrackingToken};
    // Moves `segment`'s token to `position`, as its owner's processor would.
    async fn store(tokens: &InMemoryTokenStore, segment: u32, owner: &str, position: u64) {
        let cursor = cqrs::Cursor::at(position);
        let token = TrackingToken {
            processor: "p",
            segment,
            owner,
            replay: false,
            cursor: &cursor,
        };
        assert!(tokens.store(&mut (), token).await.unwrap(), "owned");
    }
    let tokens = InMemoryTokenStore::default();
    tokens.initialize("p", 2).await.unwrap();
    tokens.claim("p", 0, "a", 0, u64::MAX).await.unwrap();
    tokens.claim("p", 1, "b", 0, u64::MAX).await.unwrap();
    let progress = GlobalProgress::new(tokens.clone(), "p");

    store(&tokens, 0, "a", 5).await; // only one instance got there
    let waiting = tokio::spawn({
        let progress = progress.clone();
        async move { progress.reached(5).await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!waiting.is_finished(), "segment 1 is still behind");

    store(&tokens, 1, "b", 7).await;
    tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .expect("released")
        .unwrap();
}

/// Two instances share the projection, one segment each, and each serves
/// its own subscribers: a subscriber gets the change whichever instance
/// projected it (the other one sees it in the shared tokens).
#[tokio::test(start_paused = true)]
async fn subscribers_of_every_instance_get_changes_projected_by_another() {
    use cqrs::{StreamId, TrackingOptions, segment_of};
    let events: Events<ClockEvent, Store> = Events::new(InMemoryEventStore::new());
    let ticks = Arc::new(Ticks::default());
    let options = TrackingOptions {
        segments: 2,
        max_segments: 1,
        ..TrackingOptions::default()
    };
    let a = Instance::new(&events, &ticks, options).await;
    let b = Instance::new(&events, &ticks, options).await;
    tokio::time::sleep(Duration::from_secs(2)).await; // each claimed its segment
    let (a_owns, b_owns) = (a.projection.status().segments, b.projection.status().segments);
    assert_eq!((a_owns.len(), b_owns.len()), (1, 1), "{a_owns:?} {b_owns:?}");
    // A clock of the segment `a` projects, watched from `b`, and the other
    // way round.
    let clock_of = |segment: u32| {
        (1..)
            .find(|id| segment_of(&StreamId::of::<Clock>(id), 2) == segment)
            .unwrap()
    };
    let (from_a, from_b) = (clock_of(a_owns[0]), clock_of(b_owns[0]));

    let mut watched_on_b = pin!(b.queries.subscribe(TicksOf(from_a)));
    let mut watched_on_a = pin!(a.queries.subscribe(TicksOf(from_b)));
    assert_eq!(next(&mut watched_on_b).await, 0);
    assert_eq!(next(&mut watched_on_a).await, 0);

    let clocks = cqrs::AggregateLoader::<Clock, _>::new(events.store().clone());
    record(&clocks, &events, from_a, ClockEvent::Ticked).await;
    record(&clocks, &events, from_b, ClockEvent::Ticked).await;
    assert_eq!(next(&mut watched_on_b).await, 1, "projected by a, emitted on b");
    assert_eq!(next(&mut watched_on_a).await, 1, "projected by b, emitted on a");
    a.stop().await;
    b.stop().await;
}
