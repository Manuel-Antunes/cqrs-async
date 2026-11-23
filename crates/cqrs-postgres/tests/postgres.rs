//! Against a real PostgreSQL: set `CQRS_POSTGRES_URL` (skipped otherwise).
//!
//! ```text
//! docker run -d --name cqrs-pg -e POSTGRES_PASSWORD=cqrs -e POSTGRES_USER=cqrs \
//!     -e POSTGRES_DB=cqrs -p 55432:5432 postgres:17-alpine
//! CQRS_POSTGRES_URL=postgres://cqrs:cqrs@127.0.0.1:55432/cqrs cargo test -p cqrs-postgres
//! ```
//!
//! Each test runs in its own schema, so they run in parallel.

use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use cqrs::SnapshotStore;
use cqrs::{
    Aggregate, Cursor, EventEnvelope, EventFeed, EventHandler, EventStore, Events, ExpectedVersion, Gap, HandlerError,
    Metadata, ProcessingContext, StoreError, StreamId, SubscriptionOptions, TokenStore, TrackingOptions,
};
use cqrs_postgres::{PgTransaction, PostgresEventStore, PostgresSnapshotStore, PostgresTokenStore, migrate, upcast};
use serde::{Deserialize, Serialize};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, cqrs::DomainEvent, cqrs::TagResolver)]
enum ClockEvent {
    #[event(name = "clock.ticked")]
    Ticked { n: u64 },
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

fn stream(id: u32) -> StreamId {
    StreamId::of::<Clock>(&id)
}

/// A pool on a fresh schema with the tables created, or `None` without a
/// database to test against.
async fn database() -> Option<PgPool> {
    let url = std::env::var("CQRS_POSTGRES_URL").ok()?;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let schema = format!("test_{}_{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed));
    let admin = PgPool::connect(&url).await.expect("connecting");
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("creating the schema");
    let options: PgConnectOptions = url.parse().expect("a postgres url");
    let pool = PgPoolOptions::new()
        .max_connections(32)
        .connect_with(options.options([("search_path", schema.as_str())]))
        .await
        .expect("connecting to the schema");
    migrate(&pool).await.expect("migrating");
    Some(pool)
}

macro_rules! database_or_skip {
    () => {
        match database().await {
            Some(pool) => pool,
            None => {
                eprintln!("CQRS_POSTGRES_URL not set: skipped");
                return;
            }
        }
    };
}

fn ticks(from: u64, count: u64) -> Vec<ClockEvent> {
    (from..from + count).map(|n| ClockEvent::Ticked { n }).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn events_round_trip_with_their_envelope() {
    let pool = database_or_skip!();
    let store = PostgresEventStore::<ClockEvent>::new(pool, "clocks");
    let metadata = Metadata::from([("correlation".to_owned(), "req-1".to_owned())]);

    let committed = store
        .append(&stream(1), ExpectedVersion::NoStream, ticks(1, 2), &metadata)
        .await
        .unwrap();
    assert_eq!(
        committed.iter().map(|e| (e.position, e.sequence)).collect::<Vec<_>>(),
        [(1, 1), (2, 2)]
    );

    let loaded = store.load(&stream(1), 0).await.unwrap();
    assert_eq!(loaded.len(), 2);
    let first = &loaded[0];
    assert_eq!(first.stream, stream(1));
    assert_eq!(first.payload, ClockEvent::Ticked { n: 1 });
    assert_eq!(first.event_type, "clock.ticked");
    assert_eq!(first.metadata, metadata);
    assert_eq!(first.recorded_at, committed[0].recorded_at);
    assert_eq!(
        store.load(&stream(1), 1).await.unwrap().len(),
        1,
        "only after sequence 1"
    );
    assert_eq!(store.last_position().await.unwrap(), 2);
    assert_eq!(store.read_all(1, 10).await.unwrap()[0].position, 2);
}

/// The same event in a newer shape: `Ticked` gained `by` in version 2.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, cqrs::DomainEvent, cqrs::TagResolver)]
enum ClockEventV2 {
    #[event(name = "clock.ticked", version = 2)]
    Ticked { n: u64, by: String },
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_events_are_upcast_when_read() {
    let pool = database_or_skip!();
    let none = Metadata::new();
    PostgresEventStore::<ClockEvent>::new(pool.clone(), "clocks")
        .append(&stream(1), ExpectedVersion::NoStream, ticks(1, 1), &none)
        .await
        .unwrap();

    // Without an upcaster the old row no longer decodes, and says why.
    let plain = PostgresEventStore::<ClockEventV2>::new(pool.clone(), "clocks");
    let error = plain.load(&stream(1), 0).await.err().unwrap();
    assert!(
        matches!(&error, StoreError::Backend(message) if message.contains("clock.ticked v1")),
        "{error}"
    );

    let store = plain.with_upcasters(upcast("clock.ticked", 1, |payload| {
        payload["Ticked"]["by"] = serde_json::json!("unknown");
    }));
    store
        .append(
            &stream(1),
            ExpectedVersion::Exact(1),
            vec![ClockEventV2::Ticked { n: 2, by: "me".into() }],
            &none,
        )
        .await
        .unwrap();
    let read: Vec<_> = store
        .read_all(0, 10)
        .await
        .unwrap()
        .into_iter()
        .map(|e| (e.event_version, e.payload))
        .collect();
    assert_eq!(
        read,
        [
            (
                2,
                ClockEventV2::Ticked {
                    n: 1,
                    by: "unknown".into()
                }
            ),
            (2, ClockEventV2::Ticked { n: 2, by: "me".into() }),
        ]
    );
    let stored: i32 = sqlx::query_scalar("SELECT event_version FROM cqrs_events WHERE position = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(stored, 1, "the stored row is never rewritten");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn optimistic_concurrency_is_enforced() {
    let pool = database_or_skip!();
    let store = PostgresEventStore::<ClockEvent>::new(pool, "clocks");
    let none = Metadata::new();
    store
        .append(&stream(1), ExpectedVersion::NoStream, ticks(1, 1), &none)
        .await
        .unwrap();

    let twice = store
        .append(&stream(1), ExpectedVersion::NoStream, ticks(2, 1), &none)
        .await;
    assert!(matches!(twice, Err(StoreError::Conflict { actual: Some(1), .. })));
    let stale = store
        .append(&stream(1), ExpectedVersion::Exact(0), ticks(2, 1), &none)
        .await;
    assert!(matches!(stale, Err(StoreError::Conflict { .. })));
    store
        .append(&stream(1), ExpectedVersion::Exact(1), ticks(2, 1), &none)
        .await
        .unwrap();
    assert_eq!(
        store.last_position().await.unwrap(),
        2,
        "refused appends leave no trace"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writers_append_in_parallel_without_losing_anything() {
    let pool = database_or_skip!();
    let store = PostgresEventStore::<ClockEvent>::new(pool, "clocks");
    let writers: Vec<_> = (1..=16)
        .map(|id| {
            let store = store.clone();
            tokio::spawn(async move {
                let mut version = None;
                for round in 0..10 {
                    let expected = version.map_or(ExpectedVersion::NoStream, ExpectedVersion::Exact);
                    let events = ticks(round * 2, 2); // two events per append
                    let committed = store
                        .append(&stream(id), expected, events, &Metadata::new())
                        .await
                        .unwrap();
                    version = committed.last().map(|e| e.sequence);
                }
            })
        })
        .collect();
    for writer in writers {
        writer.await.unwrap();
    }

    let all = store.read_all(0, 1_000).await.unwrap();
    assert_eq!(all.len(), 320);
    assert!(all.windows(2).all(|pair| pair[0].position < pair[1].position));
    // Each stream's events are in sequence order in the log.
    for id in 1..=16 {
        let sequences: Vec<u64> = all
            .iter()
            .filter(|e| e.stream == stream(id))
            .map(|e| e.sequence)
            .collect();
        assert_eq!(sequences, (1..=20).collect::<Vec<_>>());
    }
    assert_eq!(store.last_position().await.unwrap(), all.last().unwrap().position);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn racing_writers_on_one_stream_have_exactly_one_winner_per_version() {
    let pool = database_or_skip!();
    let store = PostgresEventStore::<ClockEvent>::new(pool, "clocks");
    store
        .append(&stream(1), ExpectedVersion::NoStream, ticks(0, 1), &Metadata::new())
        .await
        .unwrap();
    let racers: Vec<_> = (0..12)
        .map(|n| {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .append(
                        &stream(1),
                        ExpectedVersion::Exact(1),
                        ticks(100 + n, 1),
                        &Metadata::new(),
                    )
                    .await
            })
        })
        .collect();
    let mut wins = 0;
    for racer in racers {
        match racer.await.unwrap() {
            Ok(_) => wins += 1,
            Err(StoreError::Conflict { .. }) => {}
            Err(error) => panic!("{error}"),
        }
    }
    assert_eq!(wins, 1);
    assert_eq!(store.load(&stream(1), 0).await.unwrap().len(), 2);
    // Refused appends (by the check or by the unique backstop) moved nothing.
    assert_eq!(store.last_position().await.unwrap(), 2);
    let next = store
        .append(&stream(2), ExpectedVersion::NoStream, ticks(0, 1), &Metadata::new())
        .await
        .unwrap();
    assert_eq!(next[0].position, 3, "no gap left by the losers");
}

/// Appends commit out of order and racing ones leave gaps (positions taken
/// by appends that lost): a subscription reading while they write still
/// gets every event exactly once, each stream in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_subscription_reading_while_writers_race_misses_nothing() {
    let pool = database_or_skip!();
    let store = PostgresEventStore::<ClockEvent>::new(pool, "clocks");
    let events = Events::new(store.clone()).with_options(SubscriptionOptions {
        batch_size: 7,
        poll_interval: Duration::from_millis(20),
    });
    let mut subscription = Box::pin(events.subscribe(0));

    // 8 writers racing on stream 1 (retrying on conflicts), 8 on their own.
    let writers: Vec<_> = (0..16_u32)
        .map(|writer| {
            let store = store.clone();
            let id = if writer < 8 { 1 } else { writer };
            tokio::spawn(async move {
                let mut appended = 0;
                while appended < 10 {
                    let version = store.load(&stream(id), 0).await.unwrap().last().map(|e| e.sequence);
                    let expected = version.map_or(ExpectedVersion::NoStream, ExpectedVersion::Exact);
                    match store.append(&stream(id), expected, ticks(0, 1), &Metadata::new()).await {
                        Ok(_) => appended += 1,
                        Err(StoreError::Conflict { .. }) => {}
                        Err(error) => panic!("{error}"),
                    }
                }
            })
        })
        .collect();

    let mut seen = Vec::new();
    while seen.len() < 160 {
        let next = tokio::time::timeout(
            Duration::from_secs(20),
            futures_util::StreamExt::next(&mut subscription),
        );
        seen.push(next.await.expect("every event arrives").unwrap().unwrap());
    }
    for writer in writers {
        writer.await.unwrap();
    }
    let mut positions: Vec<u64> = seen.iter().map(|e| e.position).collect();
    positions.sort_unstable();
    positions.dedup();
    assert_eq!(positions.len(), 160, "each event once");
    for id in [1, 8, 9, 15] {
        let sequences: Vec<u64> = seen
            .iter()
            .filter(|e| e.stream == stream(id))
            .map(|e| e.sequence)
            .collect();
        let expected = if id == 1 { 80 } else { 10 };
        assert_eq!(sequences, (1..=expected).collect::<Vec<_>>(), "stream {id} in order");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn token_claims_are_exclusive_fenced_and_expire() {
    let pool = database_or_skip!();
    let tokens = PostgresTokenStore::new(pool);
    assert_eq!(tokens.initialize("p", 2).await.unwrap(), 2);
    assert_eq!(tokens.initialize("p", 8).await.unwrap(), 2, "fixed by the first");

    // Caller clocks only give the lease length; expiry is the database's.
    let lease = |ms: u64| (0, ms);
    let (now, until) = lease(300);
    assert_eq!(
        tokens.claim("p", 0, "a", now, until).await.unwrap(),
        Some(Cursor::at(0))
    );
    assert_eq!(tokens.claim("p", 0, "b", now, until).await.unwrap(), None, "held by a");
    // A cursor is stored with its gaps.
    let seven = Cursor::new(
        7,
        [Gap {
            position: 5,
            horizon: 99,
        }],
    );
    assert!(tokens.store("p", 0, "a", &seven).await.unwrap());
    assert!(
        !tokens.store("p", 0, "b", &Cursor::at(9)).await.unwrap(),
        "b does not own it"
    );

    tokio::time::sleep(Duration::from_millis(500)).await; // a's lease expires
    assert_eq!(
        tokens.claim("p", 0, "b", now, until).await.unwrap(),
        Some(seven.clone()),
        "taken over at 7"
    );
    assert!(
        !tokens.store("p", 0, "a", &Cursor::at(8)).await.unwrap(),
        "a is fenced off"
    );

    tokens.release("p", 0, "b").await.unwrap();
    assert_eq!(tokens.claim("p", 0, "a", now, until).await.unwrap(), Some(seven));
}

/// Records which streams it handled.
#[derive(Default)]
struct Recorder(Mutex<Vec<(String, u64)>>);

impl Recorder {
    fn streams(&self) -> BTreeSet<String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|(stream, _)| stream.clone())
            .collect()
    }
}

/// Writes nothing to the database: generic over the unit of work.
impl<Tx: Send> EventHandler<ClockEvent, Tx> for Recorder {
    async fn handle(
        &self,
        event: &EventEnvelope<ClockEvent>,
        _: &mut ProcessingContext<'_, Tx>,
    ) -> Result<(), HandlerError> {
        self.0.lock().unwrap().push((event.stream.to_string(), event.sequence));
        Ok(())
    }
}

fn options(segments: u32, max_segments: u32) -> TrackingOptions {
    TrackingOptions {
        segments,
        max_segments,
        claim_timeout: Duration::from_secs(1),
        claim_interval: Duration::from_millis(100),
        ..TrackingOptions::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn instances_share_segments_through_postgres() {
    let pool = database_or_skip!();
    let events = Events::new(PostgresEventStore::<ClockEvent>::new(pool.clone(), "clocks"))
        .with_tokens(PostgresTokenStore::new(pool));
    let clocks = events.event_sourced::<Clock>();
    let (first, second) = (Arc::new(Recorder::default()), Arc::new(Recorder::default()));
    let a = events
        .processor("shared", Arc::clone(&first))
        .with_options(options(4, 2))
        .spawn();
    let b = events
        .processor("shared", Arc::clone(&second))
        .with_options(options(4, 2))
        .spawn();

    let mut last = 0;
    for id in 1..=40 {
        let new = cqrs::Loaded::new(Clock::initial(&id));
        last = clocks.append(new, ticks(1, 1)).await.unwrap().events[0].position;
    }
    tokio::time::timeout(Duration::from_secs(20), async {
        a.caught_up(last).await;
        b.caught_up(last).await;
    })
    .await
    .expect("both instances caught up");

    let (mine, theirs) = (first.streams(), second.streams());
    assert!(!mine.is_empty() && !theirs.is_empty(), "both did work");
    assert!(mine.is_disjoint(&theirs), "no aggregate handled twice");
    assert_eq!(mine.len() + theirs.len(), 40);
    a.stop().await;
    b.stop().await;
}

/// A second event enum, for the second aggregate of the same database.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, cqrs::DomainEvent, cqrs::TagResolver)]
enum BellEvent {
    #[event(name = "bell.rung")]
    Rung { times: u32 },
}

/// Records the bells it handled.
#[derive(Default)]
struct BellRecorder(Mutex<Vec<u32>>);

impl<Tx: Send> EventHandler<BellEvent, Tx> for BellRecorder {
    async fn handle(
        &self,
        event: &EventEnvelope<BellEvent>,
        _: &mut ProcessingContext<'_, Tx>,
    ) -> Result<(), HandlerError> {
        let BellEvent::Rung { times } = event.payload;
        self.0.lock().unwrap().push(times);
        Ok(())
    }
}

/// The guide asks for one event enum per aggregate, so two enums end up in
/// one database: each store must read only its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_event_enums_in_one_database_read_only_their_own_events() {
    let pool = database_or_skip!();
    let clocks = PostgresEventStore::<ClockEvent>::new(pool.clone(), "clocks");
    let bells = PostgresEventStore::<BellEvent>::new(pool, "bells");
    let none = Metadata::new();

    clocks
        .append(&stream(1), ExpectedVersion::NoStream, ticks(1, 2), &none)
        .await
        .unwrap();
    bells
        .append(
            &StreamId::new("bell", "1"),
            ExpectedVersion::NoStream,
            vec![BellEvent::Rung { times: 3 }],
            &none,
        )
        .await
        .unwrap();
    clocks
        .append(&stream(1), ExpectedVersion::Exact(2), ticks(3, 1), &none)
        .await
        .unwrap();

    let clock_events = clocks.read_all(0, 100).await.unwrap();
    assert_eq!(clock_events.iter().map(|e| e.position).collect::<Vec<_>>(), [1, 2, 4]);
    let bell_events = bells.read_all(0, 100).await.unwrap();
    assert_eq!(bell_events.iter().map(|e| e.position).collect::<Vec<_>>(), [3]);
    assert_eq!(clocks.last_position().await.unwrap(), 4);
    assert_eq!(bells.last_position().await.unwrap(), 3, "the head of the bells' log");

    // A stream id of one log is free in the other: same type and id, no clash.
    bells
        .append(
            &stream(1),
            ExpectedVersion::NoStream,
            vec![BellEvent::Rung { times: 1 }],
            &none,
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_processor_for_each_event_enum_advances_on_one_database() {
    let pool = database_or_skip!();
    let tokens = PostgresTokenStore::new(pool.clone());
    let clock_events =
        Events::new(PostgresEventStore::<ClockEvent>::new(pool.clone(), "clocks")).with_tokens(tokens.clone());
    let bell_events = Events::new(PostgresEventStore::<BellEvent>::new(pool, "bells")).with_tokens(tokens);
    let (clock_seen, bell_seen) = (Arc::new(Recorder::default()), Arc::new(BellRecorder::default()));
    let clock_processor = clock_events.processor("clocks", Arc::clone(&clock_seen)).spawn();
    let bell_processor = bell_events.processor("bells", Arc::clone(&bell_seen)).spawn();

    let (clocks, bells) = (
        clock_events.event_sourced::<Clock>(),
        bell_events.event_sourced::<Bell>(),
    );
    let (mut last_clock, mut last_bell) = (0, 0);
    for id in 1..=5 {
        last_clock = clocks
            .append(cqrs::Loaded::new(Clock::initial(&id)), ticks(1, 1))
            .await
            .unwrap()
            .events[0]
            .position;
        last_bell = bells
            .append(
                cqrs::Loaded::new(Bell::initial(&id)),
                vec![BellEvent::Rung { times: id }],
            )
            .await
            .unwrap()
            .events[0]
            .position;
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        clock_processor.caught_up(last_clock).await;
        bell_processor.caught_up(last_bell).await;
    })
    .await
    .expect("both processors advance past the other's events");

    assert_eq!(clock_seen.streams().len(), 5);
    assert_eq!(*bell_seen.0.lock().unwrap(), [1, 2, 3, 4, 5]);
    clock_processor.stop().await;
    bell_processor.stop().await;
}

struct Bell {
    id: u32,
}

impl Aggregate for Bell {
    const TYPE: &'static str = "bell";
    type Id = u32;
    type Event = BellEvent;

    fn initial(id: &u32) -> Self {
        Self { id: *id }
    }

    fn id(&self) -> &u32 {
        &self.id
    }

    fn evolve(&mut self, _: &BellEvent) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_crashed_instance_is_taken_over_from_its_stored_positions() {
    let pool = database_or_skip!();
    let events = Events::new(PostgresEventStore::<ClockEvent>::new(pool.clone(), "clocks"))
        .with_tokens(PostgresTokenStore::new(pool));
    let clocks = events.event_sourced::<Clock>();
    let (first, second) = (Arc::new(Recorder::default()), Arc::new(Recorder::default()));

    let a = events
        .processor("failover", Arc::clone(&first))
        .with_options(options(2, u32::MAX))
        .spawn();
    let mut last = 0;
    for id in 1..=10 {
        last = clocks
            .append(cqrs::Loaded::new(Clock::initial(&id)), ticks(1, 1))
            .await
            .unwrap()
            .events[0]
            .position;
    }
    tokio::time::timeout(Duration::from_secs(20), a.caught_up(last))
        .await
        .expect("a caught up");
    a.abort().await; // no release: the claims must expire

    let b = events
        .processor("failover", Arc::clone(&second))
        .with_options(options(2, u32::MAX))
        .spawn();
    for id in 11..=20 {
        last = clocks
            .append(cqrs::Loaded::new(Clock::initial(&id)), ticks(1, 1))
            .await
            .unwrap()
            .events[0]
            .position;
    }
    // `caught_up` is per instance and per owned segment: wait until b owns
    // both (their claims expire a few ms apart) and is past `last`.
    let mut progress = b.progress();
    tokio::time::timeout(
        Duration::from_secs(20),
        progress.wait_for(|status| status.segments.len() == 2 && status.position >= last),
    )
    .await
    .expect("b took both segments over")
    .unwrap();

    // Nothing is lost: every clock was handled by one instance or the other
    // (the dead one may have finished an in-flight event before stopping),
    // and the survivor did the work committed after the crash.
    let handled: BTreeSet<_> = first.streams().union(&second.streams()).cloned().collect();
    for id in 1..=20 {
        assert!(handled.contains(&stream(id).to_string()), "clock {id} handled");
    }
    assert!(
        second
            .streams()
            .iter()
            .any(|s| (11..=20).any(|id| *s == stream(id).to_string())),
        "b took over"
    );
    b.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reset_waits_for_every_claim_and_marks_the_replay() {
    let pool = database_or_skip!();
    let tokens = PostgresTokenStore::new(pool);
    tokens.initialize("p", 2).await.unwrap();
    tokens.claim("p", 0, "a", 0, 60_000).await.unwrap().unwrap();
    assert!(tokens.store("p", 0, "a", &Cursor::at(7)).await.unwrap());
    assert_eq!(tokens.replay_until("p").await.unwrap(), 0);

    assert!(!tokens.reset("p", 0, 7).await.unwrap(), "segment 0 is claimed");
    let positions: Vec<u64> = tokens
        .cursors("p")
        .await
        .unwrap()
        .iter()
        .map(Cursor::position)
        .collect();
    assert_eq!(positions, [7, 0], "segment 1 never moved");
    tokens.release("p", 0, "a").await.unwrap();

    assert!(tokens.reset("p", 0, 7).await.unwrap());
    assert_eq!(tokens.replay_until("p").await.unwrap(), 7);
    assert_eq!(
        tokens.claim("p", 0, "b", 0, 60_000).await.unwrap(),
        Some(Cursor::at(0)),
        "back at the start"
    );
}

/// Sums the ticks; snapshotted as JSON.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Tally {
    id: u32,
    total: u64,
}

impl Aggregate for Tally {
    const TYPE: &'static str = "tally";
    type Id = u32;
    type Event = ClockEvent;

    fn initial(id: &u32) -> Self {
        Self { id: *id, total: 0 }
    }

    fn id(&self) -> &u32 {
        &self.id
    }

    fn evolve(&mut self, ClockEvent::Ticked { n }: &ClockEvent) {
        self.total += n;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshots_are_kept_per_aggregate_and_revision() {
    let pool = database_or_skip!();
    let snapshots = PostgresSnapshotStore::<Tally>::new(pool.clone());
    let tallies =
        cqrs::EventSourced::new(PostgresEventStore::new(pool.clone(), "clocks")).with_snapshots(snapshots.clone(), 2);
    let mut tally = cqrs::Loaded::new(Tally::initial(&1));
    for n in 1..=3 {
        tally = tallies
            .append(tally, vec![ClockEvent::Ticked { n }])
            .await
            .unwrap()
            .aggregate;
    }

    // Taken when the stream reached version 2; the load replays only event 3.
    assert_eq!(snapshots.load(&1).await.unwrap(), Some((Tally { id: 1, total: 3 }, 2)));
    let loaded = tallies.load(&1).await.unwrap().unwrap();
    assert_eq!((loaded.total, loaded.version()), (6, Some(3)));

    snapshots.save(&Tally { id: 1, total: 0 }, 1).await.unwrap();
    assert_eq!(
        snapshots.load(&1).await.unwrap().map(|(_, version)| version),
        Some(2),
        "an older one never wins"
    );
    assert_eq!(
        PostgresSnapshotStore::<Tally>::new(pool)
            .with_revision(2)
            .load(&1)
            .await
            .unwrap(),
        None,
        "another shape"
    );
}

/// Bookings of a room: each in its own stream, tagged with the room.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, cqrs::DomainEvent, cqrs::TagResolver)]
enum RoomEvent {
    #[event(name = "room.booked")]
    Booked {
        #[tag]
        room: u32,
        by: u32,
    },
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conditional_append_lets_exactly_one_of_racing_deciders_win() {
    let pool = database_or_skip!();
    let store = PostgresEventStore::<RoomEvent>::new(pool, "rooms");
    let room = |n: u32| cqrs::criteria![room = n];
    let book = |by: u32| vec![RoomEvent::Booked { room: 1, by }];
    // Another room's booking does not stand in the way.
    store
        .append(
            &StreamId::new("booking", "other"),
            ExpectedVersion::NoStream,
            vec![RoomEvent::Booked { room: 2, by: 0 }],
            &Metadata::new(),
        )
        .await
        .unwrap();

    // 16 deciders read "room 1 is free" at the same head, then each books it
    // in its own stream, on the condition that nothing about room 1 changed.
    let (free, head) = store.source(&room(1)).await.unwrap();
    assert!(free.is_empty());
    let condition = cqrs::AppendCondition {
        criteria: room(1),
        after: head,
    };
    let attempts = (0..16).map(|by| {
        let (store, condition) = (store.clone(), condition.clone());
        tokio::spawn(async move {
            let stream = StreamId::new("booking", by.to_string());
            store
                .append_if(
                    &stream,
                    ExpectedVersion::NoStream,
                    book(by),
                    &Metadata::new(),
                    Some(&condition),
                )
                .await
        })
    });
    let mut won = 0;
    for attempt in attempts.collect::<Vec<_>>() {
        match attempt.await.unwrap() {
            Ok(_) => won += 1,
            Err(error) => assert_eq!(error, StoreError::ConditionFailed),
        }
    }
    assert_eq!(won, 1);

    let (booked, now) = store.source(&room(1)).await.unwrap();
    assert_eq!(
        (booked.len(), now),
        (1, 2),
        "one booking of room 1; the head counts room 2's too"
    );
    let others = store.source(&room(1).of_types(["room.cancelled"])).await.unwrap().0;
    assert!(others.is_empty(), "criteria filter by type too");
}

/// Seats: tagged with the room and the floor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, cqrs::DomainEvent, cqrs::TagResolver)]
enum SeatEvent {
    #[event(name = "seat.taken")]
    Taken {
        #[tag]
        room: u32,
        #[tag]
        floor: u32,
    },
    #[event(name = "floor.closed")]
    FloorClosed {
        #[tag]
        floor: u32,
    },
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn criteria_match_all_their_tags_and_any_alternative() {
    let pool = database_or_skip!();
    let store = PostgresEventStore::<SeatEvent>::new(pool, "seats");
    let events = [
        ("a", SeatEvent::Taken { room: 1, floor: 2 }), // 1
        ("b", SeatEvent::Taken { room: 1, floor: 3 }), // 2
        ("c", SeatEvent::Taken { room: 4, floor: 2 }), // 3
        ("d", SeatEvent::FloorClosed { floor: 2 }),    // 4
    ];
    for (id, event) in events {
        let stream = StreamId::new("seat", id.to_owned());
        store
            .append(&stream, ExpectedVersion::NoStream, vec![event], &Metadata::new())
            .await
            .unwrap();
    }

    let positions = |events: Vec<EventEnvelope<SeatEvent>>| events.iter().map(|e| e.position).collect::<Vec<_>>();
    let (both, head) = store.source(&cqrs::criteria![room = 1, floor = 2]).await.unwrap();
    assert_eq!((positions(both), head), (vec![1], 4), "both tags on the same event");
    let either = store
        .source(&cqrs::criteria![room = 4; room = 1, floor = 3])
        .await
        .unwrap()
        .0;
    assert_eq!(positions(either), [2, 3]);
    let closed = store
        .source(&cqrs::criteria![floor = 2 => ["floor.closed"]])
        .await
        .unwrap()
        .0;
    assert_eq!(positions(closed), [4]);
    // Several alternatives matching one event return it once.
    assert_eq!(
        positions(store.source(&cqrs::criteria![floor = 2; room = 4]).await.unwrap().0),
        [1, 3, 4]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_condition_only_fails_on_an_event_matching_all_its_tags() {
    let pool = database_or_skip!();
    let store = PostgresEventStore::<SeatEvent>::new(pool, "seats");
    let none = Metadata::new();
    let room_1_floor_2 = cqrs::AppendCondition {
        criteria: cqrs::criteria![room = 1, floor = 2],
        after: 0,
    };
    let take = |id: &str, room, floor| {
        (
            StreamId::new("seat", id.to_owned()),
            vec![SeatEvent::Taken { room, floor }],
        )
    };

    // Room 1 and floor 2 both move, but on different events: every tag's
    // last position passed the condition, the exact check clears it.
    for (stream, events) in [take("a", 1, 3), take("b", 4, 2)] {
        store
            .append(&stream, ExpectedVersion::NoStream, events, &none)
            .await
            .unwrap();
    }
    let (stream, events) = take("c", 1, 2);
    store
        .append_if(&stream, ExpectedVersion::NoStream, events, &none, Some(&room_1_floor_2))
        .await
        .unwrap();

    // Now an event with both exists after 0: refused.
    let (stream, events) = take("d", 1, 2);
    let refused = store
        .append_if(&stream, ExpectedVersion::NoStream, events, &none, Some(&room_1_floor_2))
        .await;
    assert_eq!(refused.err(), Some(StoreError::ConditionFailed));
    // A condition read after it holds again.
    let (stream, events) = take("d", 1, 2);
    let fresh = cqrs::AppendCondition {
        after: 3,
        ..room_1_floor_2
    };
    store
        .append_if(&stream, ExpectedVersion::NoStream, events, &none, Some(&fresh))
        .await
        .unwrap();
}

/// Sums each clock's ticks with a plain `total = total + n`: not
/// idempotent, so applying an event twice would show. Can fail once, after
/// writing, at one position. Its context names the transaction it writes
/// in, which commits with the processor's token.
#[derive(Default)]
struct Totals {
    fail_once_at: Option<u64>,
    failed: std::sync::atomic::AtomicBool,
    /// Announces each applied position, once committed.
    applied: Arc<cqrs::QueryUpdateEmitter<Applied, u64>>,
}

/// Subscribed to by the test, to see what the projection announced.
#[derive(Clone, cqrs::Query)]
#[query(output = u64, error = ())]
struct Applied;

#[cqrs::projection(namespace = "totals")]
impl EventHandler for Totals {
    async fn handle(
        &self,
        event: &EventEnvelope<ClockEvent>,
        context: &mut ProcessingContext<'_, PgTransaction>,
    ) -> Result<(), HandlerError> {
        let ClockEvent::Ticked { n } = event.payload;
        sqlx::query(
            "INSERT INTO clock_totals (clock, total, events) VALUES ($1, $2, 1) \
             ON CONFLICT (clock) DO UPDATE SET total = clock_totals.total + EXCLUDED.total, events = clock_totals.events + 1",
        )
        .bind(event.stream.id())
        .bind(n as i64)
        .execute(&mut **context.transaction())
        .await?;
        if Some(event.position) == self.fail_once_at && !self.failed.swap(true, Ordering::Relaxed) {
            return Err(HandlerError::msg("failed after writing"));
        }
        Ok(())
    }

    async fn committed(&self, event: &EventEnvelope<ClockEvent>) {
        self.applied.emit(|_| true, event.position);
    }

    async fn reset(&self, context: &mut ProcessingContext<'_, PgTransaction>) -> Result<(), HandlerError> {
        sqlx::query("TRUNCATE clock_totals")
            .execute(&mut **context.transaction())
            .await?;
        Ok(())
    }
}

/// `(sum of totals, sum of applied events)` of the read model.
async fn totals(pool: &PgPool) -> (i64, i64) {
    sqlx::query_as("SELECT coalesce(sum(total), 0)::bigint, coalesce(sum(events), 0)::bigint FROM clock_totals")
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_projection_commits_its_changes_with_its_token_so_each_event_applies_once() {
    let pool = database_or_skip!();
    sqlx::query("CREATE TABLE clock_totals (clock text PRIMARY KEY, total bigint NOT NULL, events bigint NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    let events = Events::new(PostgresEventStore::<ClockEvent>::new(pool.clone(), "clocks"))
        .with_tokens(PostgresTokenStore::new(pool.clone()));
    let clocks = events.event_sourced::<Clock>();
    let mut last = 0;
    for id in 1..=2 {
        let new = cqrs::Loaded::new(Clock::initial(&id));
        last = clocks
            .append(new, ticks(1, 5))
            .await
            .unwrap()
            .events
            .last()
            .unwrap()
            .position;
    }

    // Fails once at position 3, after its insert: the transaction rolls the
    // insert back with the token, and the retry applies the event once.
    let totals_projection = Totals {
        fail_once_at: Some(3),
        ..Totals::default()
    };
    let mut announced = Box::pin(totals_projection.applied.subscribe(Applied));
    let running = events.projection(totals_projection).with_options(options(2, 2)).spawn();
    tokio::time::timeout(Duration::from_secs(20), running.caught_up(last))
        .await
        .expect("caught up");
    assert_eq!(
        totals(&pool).await,
        (2 * (1 + 2 + 3 + 4 + 5), 10),
        "every tick applied exactly once"
    );
    // Emitted after each commit only: the rolled-back attempt at 3 is not.
    let mut positions: Vec<u64> =
        futures_util::StreamExt::collect(futures_util::StreamExt::take(announced.as_mut(), 10)).await;
    positions.sort_unstable();
    assert_eq!(positions, (1..=10).collect::<Vec<_>>());
    // A segment that only saw the other one's events stores its token
    // lazily (when idle): wait for it.
    let stored = async {
        while !events
            .tokens()
            .cursors("totals")
            .await
            .unwrap()
            .iter()
            .all(|cursor| cursor.covers(last))
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), stored)
        .await
        .expect("tokens moved with the projection");

    // Replay: the reset handler truncates, and the rebuild gives the same.
    running.stop().await;
    events.projection(Totals::default()).reset().await.unwrap();
    assert_eq!(totals(&pool).await, (0, 0));
    let running = events.projection(Totals::default()).with_options(options(2, 2)).spawn();
    tokio::time::timeout(Duration::from_secs(20), running.caught_up(last))
        .await
        .expect("rebuilt");
    assert_eq!(totals(&pool).await, (30, 10));
    running.stop().await;
}

/// Counts events per clock in its own table; fails once at one position.
/// Written by hand (no derive): the trait itself.
#[derive(Default)]
struct Counts {
    fail_once_at: Option<u64>,
    failed: std::sync::atomic::AtomicBool,
}

impl EventHandler<ClockEvent, PgTransaction> for Counts {
    async fn handle(
        &self,
        event: &EventEnvelope<ClockEvent>,
        context: &mut ProcessingContext<'_, PgTransaction>,
    ) -> Result<(), HandlerError> {
        if Some(event.position) == self.fail_once_at && !self.failed.swap(true, Ordering::Relaxed) {
            return Err(HandlerError::msg("second member failed"));
        }
        sqlx::query("INSERT INTO clock_counts (clock, events) VALUES ($1, 1) ON CONFLICT (clock) DO UPDATE SET events = clock_counts.events + 1")
            .bind(event.stream.id())
            .execute(&mut **context.transaction())
            .await?;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_processing_group_commits_all_its_read_models_or_none() {
    let pool = database_or_skip!();
    for table in [
        "CREATE TABLE clock_totals (clock text PRIMARY KEY, total bigint NOT NULL, events bigint NOT NULL)",
        "CREATE TABLE clock_counts (clock text PRIMARY KEY, events bigint NOT NULL)",
    ] {
        sqlx::query(table).execute(&pool).await.unwrap();
    }
    let events = Events::new(PostgresEventStore::<ClockEvent>::new(pool.clone(), "clocks"))
        .with_tokens(PostgresTokenStore::new(pool.clone()));
    let new = cqrs::Loaded::new(Clock::initial(&1));
    let last = events
        .event_sourced::<Clock>()
        .append(new, ticks(1, 4))
        .await
        .unwrap()
        .events
        .last()
        .unwrap()
        .position;

    // `Totals` writes event 2, then `Counts` fails on it: both roll back,
    // and the retry applies event 2 once to each.
    let group = (
        Totals::default(),
        Counts {
            fail_once_at: Some(2),
            ..Counts::default()
        },
    );
    let running = events.processor("group", group).spawn();
    tokio::time::timeout(Duration::from_secs(20), running.caught_up(last))
        .await
        .expect("caught up");
    assert_eq!(totals(&pool).await, (1 + 2 + 3 + 4, 4));
    let counted: i64 = sqlx::query_scalar("SELECT events FROM clock_counts")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(counted, 4);
    running.stop().await;
}

/// Tags written by hand: the parity of the tick.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, cqrs::DomainEvent)]
enum Parity {
    Ticked { n: u64 },
}

impl cqrs::TagResolver for Parity {
    fn tags(&self) -> Vec<cqrs::Tag> {
        let Self::Ticked { n } = self;
        vec![cqrs::Tag::new("parity", if n % 2 == 0 { "even" } else { "odd" })]
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hand_written_tags_are_indexed_and_guard_conditions() {
    let pool = database_or_skip!();
    let store = PostgresEventStore::<Parity>::new(pool, "parity");
    let tick = |n| vec![Parity::Ticked { n }];
    for n in 1..=4 {
        store
            .append(&stream(1), ExpectedVersion::Any, tick(n), &Metadata::new())
            .await
            .unwrap();
    }

    let even = cqrs::criteria![parity = "even"];
    let (events, head) = store.source(&even).await.unwrap();
    let read: Vec<_> = events.iter().map(|e| e.payload.clone()).collect();
    assert_eq!(read, [Parity::Ticked { n: 2 }, Parity::Ticked { n: 4 }]);

    // Only an even tick since the read refuses a decision on the even ones.
    let condition = cqrs::AppendCondition {
        criteria: even,
        after: head,
    };
    store
        .append(&stream(2), ExpectedVersion::Any, tick(5), &Metadata::new())
        .await
        .unwrap();
    store
        .append_if(
            &stream(3),
            ExpectedVersion::Any,
            tick(7),
            &Metadata::new(),
            Some(&condition),
        )
        .await
        .unwrap();
    store
        .append(&stream(2), ExpectedVersion::Any, tick(6), &Metadata::new())
        .await
        .unwrap();
    let refused = store
        .append_if(
            &stream(4),
            ExpectedVersion::Any,
            tick(9),
            &Metadata::new(),
            Some(&condition),
        )
        .await;
    assert_eq!(refused.unwrap_err(), StoreError::ConditionFailed);
    // Nothing to append succeeds at once, whatever the condition (every store).
    let nothing = store
        .append_if(
            &stream(4),
            ExpectedVersion::Exact(9),
            vec![],
            &Metadata::new(),
            Some(&condition),
        )
        .await;
    assert!(nothing.unwrap().is_empty());
}

/// Fails once, after the projection wrote: inside its transaction, so the
/// write rolls back with the token.
struct FailsOnce(std::sync::atomic::AtomicBool);

impl<E: cqrs::DomainEvent, Tx: Send> cqrs::EventInterceptor<E, Tx> for FailsOnce {
    async fn intercept<N: cqrs::EventChain<Tx>>(
        &self,
        _: &EventEnvelope<E>,
        context: &mut ProcessingContext<'_, Tx>,
        next: N,
    ) -> Result<(), HandlerError> {
        next.proceed(context).await?;
        if !self.0.swap(true, Ordering::Relaxed) {
            return Err(HandlerError::msg("failed after the write"));
        }
        Ok(())
    }
}

/// Writes an audit row per event in the processor's transaction, before
/// the projection: it commits or rolls back with it.
struct AuditRows;

impl cqrs::EventInterceptor<ClockEvent, PgTransaction> for AuditRows {
    async fn intercept<N: cqrs::EventChain<PgTransaction>>(
        &self,
        event: &EventEnvelope<ClockEvent>,
        context: &mut ProcessingContext<'_, PgTransaction>,
        next: N,
    ) -> Result<(), HandlerError> {
        sqlx::query("INSERT INTO clock_audit (position) VALUES ($1)")
            .bind(i64::try_from(event.position).unwrap())
            .execute(&mut **context.transaction())
            .await?;
        next.proceed(context).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_projection_interceptor_rolls_the_write_back() {
    let pool = database_or_skip!();
    sqlx::query("CREATE TABLE clock_totals (clock text PRIMARY KEY, total bigint NOT NULL, events bigint NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE clock_audit (position bigint NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    let events = Events::new(PostgresEventStore::<ClockEvent>::new(pool.clone(), "clocks"))
        .with_tokens(PostgresTokenStore::new(pool.clone()));
    let last = events
        .store()
        .append(&stream(1), ExpectedVersion::NoStream, ticks(1, 3), &Metadata::new())
        .await
        .unwrap();
    let last = last.last().unwrap().position;

    // FailsOnce outermost: the audit row and the total of the failed try
    // are in the same transaction, both rolled back.
    let projection = cqrs::Intercepted::new(Totals::default(), (FailsOnce(Default::default()), AuditRows));
    let running = events.projection(projection).with_options(options(1, 1)).spawn();
    tokio::time::timeout(Duration::from_secs(20), running.caught_up(last))
        .await
        .expect("caught up");
    running.stop().await;
    assert_eq!(
        totals(&pool).await,
        (1 + 2 + 3, 3),
        "the failed try left nothing behind"
    );
    let audited: i64 = sqlx::query_scalar("SELECT count(*) FROM clock_audit")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(audited, 3, "one audit row per event, none from the failed try");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batched_projection_rolls_a_failed_batch_back_and_applies_each_event_once() {
    let pool = database_or_skip!();
    sqlx::query("CREATE TABLE clock_totals (clock text PRIMARY KEY, total bigint NOT NULL, events bigint NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    let events = Events::new(PostgresEventStore::<ClockEvent>::new(pool.clone(), "clocks"))
        .with_tokens(PostgresTokenStore::new(pool.clone()));
    let clocks = events.event_sourced::<Clock>();
    let mut last = 0;
    for id in 1..=4 {
        let new = cqrs::Loaded::new(Clock::initial(&id));
        last = clocks
            .append(new, ticks(1, 50))
            .await
            .unwrap()
            .events
            .last()
            .unwrap()
            .position;
    }

    // Everything is stored before it starts, so the batches fill up; the
    // one holding position 77 fails once, after writing.
    let totals_projection = Totals {
        fail_once_at: Some(77),
        ..Totals::default()
    };
    let batched = TrackingOptions {
        batch_size: 64,
        ..options(2, 2)
    };
    let running = events.projection(totals_projection).with_options(batched).spawn();
    tokio::time::timeout(Duration::from_secs(20), running.caught_up(last))
        .await
        .expect("caught up");
    running.stop().await;
    assert_eq!(
        totals(&pool).await,
        (4 * (50 * 51 / 2), 200),
        "every tick applied exactly once"
    );
}
