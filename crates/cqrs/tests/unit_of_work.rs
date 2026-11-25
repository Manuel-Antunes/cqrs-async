//! The unit of work around event handling:
//! per event, `begin` → interceptors → `handle` → token → `commit` →
//! `committed`; a failure rolls back everything written in it, interceptors
//! included. The same unit runs dead-letter retries; inline handlers get
//! their `committed` step too.
//!
//! The unit of work here is an in-memory journal: what is written in the
//! transaction reaches the "database" only on commit, so rollbacks show.

#![cfg(feature = "tokio")]

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use cqrs::{
    Intercepted,
    aggregate::Aggregate,
    command::{EventPublisher, Pending},
    event::{DomainEvent, EventEnvelope, Metadata, StreamId},
    processing::{
        DeadLetter, DeadLetterQueue, EventChain, EventHandler, EventInterceptor, Events, HandlerError,
        InMemoryDeadLetters, InMemoryTokenStore, InlinePublisher, ParkAfter, ProcessingContext, TokenStore,
        TrackingOptions, TrackingToken, TransactionManager, retry_dead_letters,
    },
    store::{AppendEvents, Cursor, ExpectedVersion, InMemoryEventStore, StoreError},
};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
struct Ticked(u64);

/// What ran, in order, across units of work.
type Phases = Arc<Mutex<Vec<String>>>;

fn log(phases: &Phases, entry: impl Into<String>) {
    phases.lock().unwrap().push(entry.into());
}

/// A transaction manager whose unit of work is a journal of rows: committed
/// into `database`, lost if dropped; and the token store taking part in it.
/// A handle, as token stores are.
#[derive(Clone, Default)]
struct Journal {
    tokens: InMemoryTokenStore,
    phases: Phases,
    database: Arc<Mutex<Vec<String>>>,
}

impl TokenStore for Journal {
    type Tx = Vec<String>;

    async fn initialize(&self, processor: &str, segments: u32) -> Result<u32, StoreError> {
        self.tokens.initialize(processor, segments).await
    }

    async fn claim(
        &self,
        processor: &str,
        segment: u32,
        owner: &str,
        now: u64,
        until: u64,
    ) -> Result<Option<Cursor>, StoreError> {
        self.tokens.claim(processor, segment, owner, now, until).await
    }

    async fn store(&self, rows: &mut Vec<String>, token: TrackingToken<'_>) -> Result<bool, StoreError> {
        log(&self.phases, "token");
        rows.push(format!("token {}", token.cursor.position()));
        self.tokens.store(&mut (), token).await
    }

    async fn release(&self, processor: &str, segment: u32, owner: &str) -> Result<(), StoreError> {
        self.tokens.release(processor, segment, owner).await
    }

    async fn cursors(&self, processor: &str) -> Result<Vec<Cursor>, StoreError> {
        self.tokens.cursors(processor).await
    }

    async fn reset(&self, processor: &str, now: u64, replay_until: u64) -> Result<bool, StoreError> {
        self.tokens.reset(processor, now, replay_until).await
    }

    async fn replay_until(&self, processor: &str) -> Result<u64, StoreError> {
        self.tokens.replay_until(processor).await
    }
}

impl TransactionManager for Journal {
    type Tx = Vec<String>;

    async fn begin(&self) -> Result<Vec<String>, StoreError> {
        log(&self.phases, "begin");
        Ok(Vec::new())
    }

    async fn commit(&self, rows: Vec<String>) -> Result<(), StoreError> {
        log(&self.phases, "commit");
        self.database.lock().unwrap().extend(rows);
        Ok(())
    }
}

/// Writes a row per event in the unit of work; fails the first try of
/// tick `fail_on` after writing.
struct Rows {
    phases: Phases,
    fail_on: u64,
    failed: AtomicBool,
}

#[cqrs::event_handler]
impl EventHandler for Rows {
    async fn handle(
        &self,
        event: &EventEnvelope<Ticked>,
        context: &mut ProcessingContext<'_, Vec<String>>,
    ) -> Result<(), HandlerError> {
        let n = event.payload.0;
        log(&self.phases, format!("handle {n}"));
        context.transaction().push(format!("row {n}"));
        if n == self.fail_on && !self.failed.swap(true, Ordering::Relaxed) {
            return Err(HandlerError::msg("fails once"));
        }
        Ok(())
    }

    async fn committed(&self, event: &EventEnvelope<Ticked>) {
        log(&self.phases, format!("committed {}", event.payload.0));
    }
}

/// Writes its own row in the same unit of work, before the handler.
struct Stamp(Phases);

impl EventInterceptor<Ticked, Vec<String>> for Stamp {
    async fn intercept<N: EventChain<Vec<String>>>(
        &self,
        event: &EventEnvelope<Ticked>,
        context: &mut ProcessingContext<'_, Vec<String>>,
        next: N,
    ) -> Result<(), HandlerError> {
        log(&self.0, format!("intercept {}", event.payload.0));
        context.transaction().push(format!("stamp {}", event.payload.0));
        next.proceed(context).await
    }
}

fn rows(journal: &Journal, fail_on: u64) -> Rows {
    Rows {
        phases: Arc::clone(&journal.phases),
        fail_on,
        failed: AtomicBool::new(false),
    }
}

async fn tick(store: &InMemoryEventStore<Ticked>, ticks: impl IntoIterator<Item = u64>) -> Vec<EventEnvelope<Ticked>> {
    let ticks = ticks.into_iter().map(Ticked).collect();
    store
        .append(
            &mut (),
            &StreamId::new("clock", "1"),
            ExpectedVersion::Any,
            ticks,
            &Metadata::new(),
        )
        .await
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn each_event_runs_in_one_unit_of_work_and_a_failure_rolls_it_back() {
    let journal = Journal::default();
    let events = Events::new(InMemoryEventStore::new())
        .with_tokens(journal.clone())
        .with_transactions(journal.clone());
    let projection = Intercepted::new(rows(&journal, 1), Stamp(Arc::clone(&journal.phases)));
    let running = events.processor("rows", projection).spawn();
    let last = tick(events.store(), [1, 2]).await.last().unwrap().position;
    tokio::time::timeout(Duration::from_secs(60), running.caught_up(last))
        .await
        .expect("caught up");
    running.stop().await;

    let phases = journal.phases.lock().unwrap().clone();
    assert_eq!(
        phases,
        [
            // Tick 1, first try: fails after writing; nothing commits.
            "begin",
            "intercept 1",
            "handle 1",
            // Retried in a new unit.
            "begin",
            "intercept 1",
            "handle 1",
            "token",
            "commit",
            "committed 1",
            "begin",
            "intercept 2",
            "handle 2",
            "token",
            "commit",
            "committed 2",
        ]
    );
    // The failed try's stamp and row were rolled back with it.
    assert_eq!(
        *journal.database.lock().unwrap(),
        ["stamp 1", "row 1", "token 1", "stamp 2", "row 2", "token 2",]
    );
}

/// Runs `rows` (failing tick `fail_on` once) with batches of 10 over ticks
/// 1..=3 stored before it starts, so they wait in its queue together.
async fn batched(fail_on: u64) -> Journal {
    let journal = Journal::default();
    let events = Events::new(InMemoryEventStore::new())
        .with_tokens(journal.clone())
        .with_transactions(journal.clone());
    let last = tick(events.store(), [1, 2, 3]).await.last().unwrap().position;
    let options = TrackingOptions::default().batch_size(10);
    let running = events
        .processor("rows", rows(&journal, fail_on))
        .with_options(options)
        .spawn();
    tokio::time::timeout(Duration::from_secs(60), running.caught_up(last))
        .await
        .expect("caught up");
    running.stop().await;
    journal
}

#[tokio::test(start_paused = true)]
async fn a_batch_is_one_unit_of_work_with_one_token() {
    let journal = batched(0).await;
    assert_eq!(
        *journal.phases.lock().unwrap(),
        [
            "begin",
            "handle 1",
            "handle 2",
            "handle 3",
            "token",
            "commit",
            "committed 1",
            "committed 2",
            "committed 3",
        ]
    );
    assert_eq!(
        *journal.database.lock().unwrap(),
        ["row 1", "row 2", "row 3", "token 3"]
    );
}

#[tokio::test(start_paused = true)]
async fn a_failed_batch_rolls_back_and_its_events_run_one_at_a_time() {
    let journal = batched(2).await;
    assert_eq!(
        *journal.phases.lock().unwrap(),
        [
            // The batch: tick 2 fails, everything rolls back.
            "begin",
            "handle 1",
            "handle 2",
            // One at a time.
            "begin",
            "handle 1",
            "token",
            "commit",
            "committed 1",
            "begin",
            "handle 2",
            "token",
            "commit",
            "committed 2",
            "begin",
            "handle 3",
            "token",
            "commit",
            "committed 3",
        ]
    );
    assert_eq!(
        *journal.database.lock().unwrap(),
        ["row 1", "token 1", "row 2", "token 2", "row 3", "token 3"]
    );
}

/// The dead letters of a [`Journal`]: a letter's removal is a row of the
/// unit of work, like the handler's.
struct JournaledLetters {
    letters: InMemoryDeadLetters<Ticked>,
    phases: Phases,
}

impl DeadLetterQueue<Ticked, Vec<String>> for JournaledLetters {
    async fn contains(&self, processor: &str, sequence: &str) -> Result<bool, StoreError> {
        self.letters.contains(processor, sequence).await
    }

    async fn sequences(&self, processor: &str) -> Result<Vec<String>, StoreError> {
        self.letters.sequences(processor).await
    }

    async fn first(&self, processor: &str, sequence: &str) -> Result<Option<DeadLetter<Ticked>>, StoreError> {
        self.letters.first(processor, sequence).await
    }

    async fn enqueue_in(
        &self,
        rows: &mut Vec<String>,
        processor: &str,
        letter: DeadLetter<Ticked>,
    ) -> Result<(), StoreError> {
        rows.push(format!("parked {}", letter.event.payload.0));
        self.letters.enqueue(processor, letter);
        Ok(())
    }

    async fn requeue_in(
        &self,
        _: &mut Vec<String>,
        processor: &str,
        letter: &DeadLetter<Ticked>,
    ) -> Result<bool, StoreError> {
        self.letters.requeue_in(&mut (), processor, letter).await
    }

    async fn remove_in(
        &self,
        rows: &mut Vec<String>,
        processor: &str,
        letter: &DeadLetter<Ticked>,
    ) -> Result<bool, StoreError> {
        let n = letter.event.payload.0;
        log(&self.phases, format!("unpark {n}"));
        rows.push(format!("unparked {n}"));
        self.letters.remove_in(&mut (), processor, letter).await
    }
}

/// A parked event is retried in a unit of work that also takes its letter
/// out of the queue: the handler's row and the removal commit together, and
/// a failed retry leaves both undone.
#[tokio::test]
async fn dead_letter_retries_run_in_the_unit_of_work() {
    let journal = Journal::default();
    let store = InMemoryEventStore::new();
    let queue = JournaledLetters {
        letters: InMemoryDeadLetters::default(),
        phases: Arc::clone(&journal.phases),
    };
    for event in tick(&store, [1, 2]).await {
        let letter = DeadLetter::new(event.stream.to_string(), Arc::new(event), "down");
        queue.letters.enqueue("rows", letter);
    }
    let handler = rows(&journal, 2); // tick 2 fails its first retry

    let outcome = retry_dead_letters(&queue, "rows", &journal, &handler, &ParkAfter(1))
        .await
        .unwrap();
    assert_eq!((outcome.handled, outcome.still_parked), (1, 1));
    let outcome = retry_dead_letters(&queue, "rows", &journal, &handler, &ParkAfter(1))
        .await
        .unwrap();
    assert_eq!((outcome.handled, outcome.still_parked), (1, 0));

    assert_eq!(
        *journal.phases.lock().unwrap(),
        [
            "begin",
            "handle 1",
            "unpark 1",
            "commit",
            "committed 1",
            "begin",
            "handle 2", // failed: rolled back, still parked
            "begin",    // the letter requeued (attempts, cause, diagnostics)
            "commit",
            "begin",
            "handle 2",
            "unpark 2",
            "commit",
            "committed 2",
        ]
    );
    assert_eq!(
        *journal.database.lock().unwrap(),
        ["row 1", "unparked 1", "row 2", "unparked 2"],
        "each letter left the queue in the unit of work that applied its event"
    );
    assert!(queue.sequences("rows").await.unwrap().is_empty());
}

struct Clock(u32);

impl Aggregate for Clock {
    const TYPE: &'static str = "clock";
    type Id = u32;
    type Event = Ticked;

    fn initial(id: &u32) -> Self {
        Self(*id)
    }

    fn id(&self) -> &u32 {
        &self.0
    }

    fn evolve(&mut self, _: &Ticked) {}
}

/// Inline, the unit of work is the append itself: `handle`, then
/// `committed`, per event.
struct Inline(Phases);

impl<E: DomainEvent> EventHandler<E> for Inline {
    async fn handle(&self, event: &EventEnvelope<E>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        log(&self.0, format!("handle {}", event.position));
        Ok(())
    }

    async fn committed(&self, event: &EventEnvelope<E>) {
        log(&self.0, format!("committed {}", event.position));
    }
}

#[tokio::test]
async fn inline_handlers_get_their_committed_step() {
    let phases = Phases::default();
    let clocks = InlinePublisher::new(InMemoryEventStore::new()).with_processor((Inline(Arc::clone(&phases)),));
    clocks
        .commit(
            cqrs::aggregate::New::<Clock>(1).into(),
            Pending::new(vec![Ticked(1), Ticked(2)]),
        )
        .await
        .unwrap();
    assert_eq!(
        *phases.lock().unwrap(),
        ["handle 1", "committed 1", "handle 2", "committed 2"]
    );
}
