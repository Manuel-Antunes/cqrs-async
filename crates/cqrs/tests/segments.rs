//! Pooled streaming (Axon 5): segments by aggregate, claims shared between
//! instances, takeover after a crash, and the sequenced dead-letter queue.

#![cfg(feature = "tokio")]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use cqrs::{
    Aggregate, AggregateLoader, DeadLetter, DeadLetterQueue, EventEnvelope, EventHandler, EventPublisher, Events,
    HandlerError, InMemoryDeadLetters, InMemoryEventStore, InMemoryTokenStore, Loaded, Pending, ProcessingContext,
    StoreError, StreamId, TrackingOptions, TransactionalDeadLetters, retry_dead_letters, segment_of,
};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent, cqrs::TagResolver)]
struct Ticked(u64);

struct Clock {
    id: u32,
    ticks: u64,
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
    events: Events<Ticked, Store, Arc<InMemoryTokenStore>>,
    clocks: AggregateLoader<Clock, Store>,
    tokens: Arc<InMemoryTokenStore>,
}

impl World {
    fn new() -> Self {
        let tokens = Arc::new(InMemoryTokenStore::default());
        let store = Arc::new(InMemoryEventStore::new());
        let events = Events::new(Arc::clone(&store)).with_tokens(Arc::clone(&tokens));
        Self {
            clocks: AggregateLoader::new(store),
            events,
            tokens,
        }
    }

    /// One more tick on clock `id`; returns the event's position.
    async fn tick(&self, id: u32) -> u64 {
        let clock = match self.clocks.load(&id).await.unwrap() {
            Some(clock) => clock,
            None => Loaded::new(Clock::initial(&id)),
        };
        let next = clock.ticks + 1;
        let committed = self
            .events
            .commit(clock, Pending::new(vec![Ticked(next)]))
            .await
            .unwrap();
        committed.expect("one event was committed")
    }

    fn options(segments: u32, max_segments: u32) -> TrackingOptions {
        TrackingOptions {
            segments,
            max_segments,
            claim_timeout: Duration::from_secs(5),
            claim_interval: Duration::from_millis(500),
            ..TrackingOptions::default()
        }
    }
}

fn stream(id: u32) -> String {
    StreamId::of::<Clock>(&id).to_string()
}

/// Records (stream, sequence) of what it handles; can be told to fail on
/// one stream.
#[derive(Default)]
struct Recorder {
    seen: Mutex<Vec<(String, u64)>>,
    poison: Mutex<Option<String>>,
}

impl Recorder {
    fn poisoned(stream: String) -> Self {
        Self {
            poison: Mutex::new(Some(stream)),
            ..Self::default()
        }
    }

    fn heal(&self) {
        *self.poison.lock().unwrap() = None;
    }

    fn seen(&self) -> Vec<(String, u64)> {
        self.seen.lock().unwrap().clone()
    }

    fn streams(&self) -> BTreeSet<String> {
        self.seen().into_iter().map(|(stream, _)| stream).collect()
    }
}

impl EventHandler<Ticked> for Recorder {
    async fn handle(&self, event: &EventEnvelope<Ticked>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        let stream = event.stream.to_string();
        if self.poison.lock().unwrap().as_ref() == Some(&stream) {
            return Err(HandlerError::msg("cannot handle this one"));
        }
        self.seen.lock().unwrap().push((stream, event.sequence));
        Ok(())
    }
}

/// Every stream's sequences, in the order they were handled.
fn by_stream(seen: &[(String, u64)]) -> BTreeMap<String, Vec<u64>> {
    let mut streams: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for (stream, sequence) in seen {
        streams.entry(stream.clone()).or_default().push(*sequence);
    }
    streams
}

/// Clock ids whose streams fall in `segment` (of `segments`).
fn ids_in(segment: u32, segments: u32) -> impl Iterator<Item = u32> {
    (1..).filter(move |id| segment_of(&StreamId::of::<Clock>(id), segments) == segment)
}

#[tokio::test(start_paused = true)]
async fn segments_keep_each_aggregate_in_order_and_handle_everything_once() {
    let world = World::new();
    let recorder = Arc::new(Recorder::default());
    let processor = world
        .events
        .processor("recorder", Arc::clone(&recorder))
        .with_options(World::options(4, u32::MAX))
        .spawn();

    let mut last = 0;
    for _round in 0..5 {
        for id in 1..=20 {
            last = world.tick(id).await; // interleaved across 20 aggregates
        }
    }
    processor.caught_up(last).await;

    assert_eq!(processor.status().segments, [0, 1, 2, 3]);
    let streams = by_stream(&recorder.seen());
    assert_eq!(streams.len(), 20);
    for (stream, sequences) in streams {
        assert_eq!(sequences, [1, 2, 3, 4, 5], "{stream}: in order, each once");
    }
    processor.stop().await;
}

#[tokio::test(start_paused = true)]
async fn two_instances_share_the_segments_without_handling_an_event_twice() {
    let world = World::new();
    let (first, second) = (Arc::new(Recorder::default()), Arc::new(Recorder::default()));
    let a = world
        .events
        .processor("shared", Arc::clone(&first))
        .with_options(World::options(4, 2))
        .spawn();
    let b = world
        .events
        .processor("shared", Arc::clone(&second))
        .with_options(World::options(4, 2))
        .spawn();

    let mut last = 0;
    for id in 1..=40 {
        last = world.tick(id).await;
    }
    a.caught_up(last).await;
    b.caught_up(last).await;

    assert_eq!(a.status().segments.len(), 2);
    assert_eq!(b.status().segments.len(), 2);
    let (mine, theirs) = (first.streams(), second.streams());
    assert!(!mine.is_empty() && !theirs.is_empty(), "both did work");
    assert!(mine.is_disjoint(&theirs), "no aggregate handled by both");
    assert_eq!(mine.len() + theirs.len(), 40, "every aggregate handled");
    a.stop().await;
    b.stop().await;
}

#[tokio::test(start_paused = true)]
async fn the_segments_of_a_crashed_instance_are_taken_over() {
    let world = World::new();
    let (first, second) = (Arc::new(Recorder::default()), Arc::new(Recorder::default()));
    let a = world
        .events
        .processor("failover", Arc::clone(&first))
        .with_options(World::options(2, u32::MAX))
        .spawn();
    let mut last = 0;
    for id in 1..=10 {
        last = world.tick(id).await;
    }
    a.caught_up(last).await;
    a.abort().await; // a crash: its claims are not released

    let b = world
        .events
        .processor("failover", Arc::clone(&second))
        .with_options(World::options(2, u32::MAX))
        .spawn();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(b.status().segments.is_empty(), "claims still held by the dead instance");

    let mut later = Vec::new();
    for id in 11..=20 {
        later.push(world.tick(id).await);
    }
    tokio::time::sleep(Duration::from_secs(6)).await; // past the claim timeout
    b.caught_up(*later.last().unwrap()).await;

    assert_eq!(b.status().segments, [0, 1]);
    let handled_later: BTreeSet<_> = second.streams();
    for id in 11..=20 {
        assert!(handled_later.contains(&stream(id)), "clock {id} handled after takeover");
    }
    // Resumed from the stored positions: what the dead instance finished is
    // not handled again (at most the events it had not stored yet).
    assert!(first.seen().len() + second.seen().len() >= 20);
    assert!(
        world
            .tokens
            .positions("failover")
            .iter()
            .all(|position| *position >= 10)
    );
    b.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_poison_event_only_holds_back_its_own_segment() {
    let world = World::new();
    let poison_id = ids_in(0, 4).next().unwrap();
    let healthy: Vec<u32> = (1..=3).flat_map(|segment| ids_in(segment, 4).take(3)).collect();
    let recorder = Arc::new(Recorder::poisoned(stream(poison_id)));
    let processor = world
        .events
        .processor("isolated", Arc::clone(&recorder))
        .with_options(World::options(4, u32::MAX))
        .spawn();

    world.tick(poison_id).await;
    for &id in &healthy {
        world.tick(id).await;
    }
    tokio::time::sleep(Duration::from_secs(30)).await; // many retries

    let handled = recorder.streams();
    for &id in &healthy {
        assert!(
            handled.contains(&stream(id)),
            "clock {id} in a healthy segment was handled"
        );
    }
    assert!(!handled.contains(&stream(poison_id)));
    assert_eq!(processor.status().failing.as_deref(), Some("cannot handle this one"));
    processor.stop().await;
}

#[tokio::test(start_paused = true)]
async fn dead_letters_park_the_aggregate_in_order_and_let_the_rest_flow() {
    let world = World::new();
    let (poison, other) = (1, 2);
    let recorder = Arc::new(Recorder::poisoned(stream(poison)));
    let queue = Arc::new(InMemoryDeadLetters::default());
    let processor = world
        .events
        .processor("parking", Arc::clone(&recorder))
        .with_options(World::options(1, u32::MAX))
        .with_dead_letters(Arc::clone(&queue), 2)
        .spawn();

    world.tick(poison).await; // 1: fails, parked
    world.tick(other).await; // 2
    world.tick(poison).await; // 3: parked behind 1, never tried
    let last = world.tick(other).await; // 4
    processor.caught_up(last).await; // the segment moved past the poison

    assert_eq!(recorder.seen(), [(stream(other), 1), (stream(other), 2)]);
    let parked: Vec<_> = queue
        .letters("parking")
        .iter()
        .map(|letter| (letter.event.sequence, letter.cause.clone()))
        .collect();
    assert_eq!(
        parked,
        [
            (1, "cannot handle this one".to_owned()),
            (2, "an earlier event of this aggregate is parked".to_owned()),
        ]
    );

    // Still broken: the retry stops at the first event, keeping the order.
    let outcome = retry_dead_letters(&*queue, "parking", &*world.tokens, &*recorder)
        .await
        .unwrap();
    assert_eq!((outcome.handled, outcome.still_parked), (0, 1));

    // Fixed: the parked events are handled in order and the queue empties.
    // Through the processor's own configuration this time.
    recorder.heal();
    let retrying = world
        .events
        .processor("parking", Arc::clone(&recorder))
        .with_dead_letters(Arc::clone(&queue), 2);
    let outcome = retrying.retry_dead_letters().await.unwrap();
    assert_eq!((outcome.handled, outcome.still_parked), (2, 0));
    assert!(queue.letters("parking").is_empty());

    // New events of that aggregate flow normally again.
    let next = world.tick(poison).await;
    processor.caught_up(next).await;
    assert_eq!(by_stream(&recorder.seen())[&stream(poison)], [1, 2, 3]);
    processor.stop().await;
}

/// Fails the first event of one stream, handles everything else.
struct FailsFirstOf {
    stream: String,
    seen: Mutex<Vec<(String, u64)>>,
}

impl EventHandler<Ticked> for FailsFirstOf {
    async fn handle(&self, event: &EventEnvelope<Ticked>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        let stream = event.stream.to_string();
        if stream == self.stream && event.sequence == 1 {
            return Err(HandlerError::msg("cannot handle the first one"));
        }
        self.seen.lock().unwrap().push((stream, event.sequence));
        Ok(())
    }
}

/// A queue whose `contains` fails a few times once something was parked,
/// as a database hiccup would.
#[derive(Default)]
struct FlakyQueue {
    inner: InMemoryDeadLetters<Ticked>,
    failures_left: Mutex<u32>,
    failures_served: AtomicU32,
}

impl TransactionalDeadLetters<Ticked, ()> for FlakyQueue {
    async fn enqueue_in(&self, tx: &mut (), processor: &str, letter: DeadLetter<Ticked>) -> Result<(), StoreError> {
        let first = self.inner.letters(processor).is_empty();
        self.inner.enqueue_in(tx, processor, letter).await?;
        if first {
            *self.failures_left.lock().unwrap() = 3; // the next lookups fail
        }
        Ok(())
    }
}

impl DeadLetterQueue<Ticked> for FlakyQueue {
    async fn contains(&self, processor: &str, sequence: &str) -> Result<bool, StoreError> {
        let failing = {
            // A plain lock, not a compare-and-swap loop: `AtomicU32::try_update`
            // is newer than the crate's MSRV and `fetch_update` is deprecated.
            let mut left = self.failures_left.lock().unwrap();
            let failing = *left > 0;
            *left = left.saturating_sub(1);
            failing
        };
        if failing {
            self.failures_served.fetch_add(1, Ordering::SeqCst);
            return Err(StoreError::Backend("the queue is unreachable".to_owned()));
        }
        self.inner.contains(processor, sequence).await
    }

    async fn sequences(&self, processor: &str) -> Result<Vec<String>, StoreError> {
        self.inner.sequences(processor).await
    }

    async fn first(&self, processor: &str, sequence: &str) -> Result<Option<DeadLetter<Ticked>>, StoreError> {
        self.inner.first(processor, sequence).await
    }

    async fn remove_first(&self, processor: &str, sequence: &str) -> Result<(), StoreError> {
        self.inner.remove_first(processor, sequence).await
    }
}

/// A failing lookup in the queue is not "not parked": the event waits and
/// asks again, so it never overtakes the parked events of its aggregate.
#[tokio::test(start_paused = true)]
async fn a_queue_that_fails_a_lookup_never_lets_an_event_overtake_the_parked_ones() {
    let world = World::new();
    let (poison, other) = (1, 2);
    let handler = Arc::new(FailsFirstOf {
        stream: stream(poison),
        seen: Mutex::default(),
    });
    let queue = Arc::new(FlakyQueue::default());
    let processor = world
        .events
        .processor("flaky", Arc::clone(&handler))
        .with_options(World::options(1, u32::MAX))
        .with_dead_letters(Arc::clone(&queue), 2)
        .spawn();

    world.tick(poison).await; // 1: fails twice, parked; the next lookups fail
    world.tick(poison).await; // 2: its lookup fails, then says: parked behind 1
    let last = world.tick(other).await;
    processor.caught_up(last).await;

    assert_eq!(
        queue.failures_served.load(Ordering::SeqCst),
        3,
        "the lookup was retried until it answered"
    );
    assert_eq!(
        *handler.seen.lock().unwrap(),
        [(stream(other), 1)],
        "the second event of the parked aggregate was never handled"
    );
    let parked: Vec<_> = queue
        .inner
        .letters("flaky")
        .iter()
        .map(|letter| (letter.event.sequence, letter.cause.clone()))
        .collect();
    assert_eq!(
        parked,
        [
            (1, "cannot handle the first one".to_owned()),
            (2, "an earlier event of this aggregate is parked".to_owned()),
        ]
    );
    processor.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_stuck_segment_is_detached_so_the_shared_reader_never_waits_for_it() {
    let world = World::new();
    let stuck = ids_in(0, 4).next().unwrap();
    let healthy: Vec<u32> = (1..=3).flat_map(|segment| ids_in(segment, 4).take(5)).collect();
    let recorder = Arc::new(Recorder::poisoned(stream(stuck)));
    let options = TrackingOptions {
        segment_buffer: 2, // fills after two events of the stuck aggregate
        segment_stall_timeout: Duration::from_secs(1),
        ..World::options(4, u32::MAX)
    };
    let processor = world
        .events
        .processor("detach", Arc::clone(&recorder))
        .with_options(options)
        .spawn();

    // Interleave: many events for the stuck aggregate between healthy ones.
    for round in 0..10 {
        for _ in 0..5 {
            world.tick(stuck).await;
        }
        for &id in &healthy {
            if round < 2 {
                world.tick(id).await;
            }
        }
    }
    tokio::time::sleep(Duration::from_secs(30)).await;

    let seen = by_stream(&recorder.seen());
    for &id in &healthy {
        assert_eq!(seen.get(&stream(id)).map(Vec::len), Some(2), "clock {id} fully handled");
    }
    assert!(!seen.contains_key(&stream(stuck)));
    processor.stop().await;
}
