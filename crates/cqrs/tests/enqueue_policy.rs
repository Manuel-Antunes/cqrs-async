//! What happens to a failing event is a policy's decision (Axon 5's
//! `EnqueuePolicy`): retry it, park it with diagnostics, skip it; and to a
//! parked one failing again at a retry: keep it, or evict it.

#![cfg(feature = "tokio")]

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use cqrs::{
    event::{EventEnvelope, Metadata, StreamId},
    processing::{
        DeadLetter, EnqueueDecision, EventHandler, Events, HandlerError, InMemoryDeadLetters, ProcessingContext,
        TrackingOptions,
    },
    store::{AppendEvents, ExpectedVersion, InMemoryEventStore},
};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum Ticked {
    Tick(u64),
}

/// Sends a tick somewhere; the target refuses tick 2 while `refusing`.
#[derive(Default)]
struct Alerts {
    sent: Mutex<Vec<u64>>,
    refusing: AtomicBool,
}

#[cqrs::event_handler]
impl EventHandler for Alerts {
    async fn handle(&self, event: &EventEnvelope<Ticked>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        let Ticked::Tick(n) = event.payload;
        if n == 2 && self.refusing.load(Ordering::SeqCst) {
            return Err(HandlerError::msg("refused: 422"));
        }
        self.sent.lock().unwrap().push(n);
        Ok(())
    }
}

type Store = InMemoryEventStore<Ticked>;

async fn ticks(events: &Events<Ticked, Store>, ticks: impl IntoIterator<Item = (&str, u64)>) -> u64 {
    let mut last = 0;
    for (clock, n) in ticks {
        let appended = events
            .store()
            .append(
                &mut (),
                &StreamId::new("clock", clock),
                ExpectedVersion::Any,
                vec![Ticked::Tick(n)],
                &Metadata::new(),
            )
            .await
            .unwrap();
        last = appended.last().unwrap().position;
    }
    last
}

fn fast() -> TrackingOptions {
    TrackingOptions::default()
        .retry_initial(Duration::from_millis(1))
        .retry_max(Duration::from_millis(5))
}

#[tokio::test]
async fn an_event_the_policy_ignores_is_skipped_not_parked() {
    let events = Events::new(Store::new());
    let alerts = Arc::new(Alerts::default());
    alerts.refusing.store(true, Ordering::SeqCst);
    let parked = InMemoryDeadLetters::default();
    // A refusal (a 4xx) will not get better: give the event up at once.
    let policy = |_: &DeadLetter<Ticked>, cause: &HandlerError| match cause.to_string().contains("422") {
        true => EnqueueDecision::Ignore,
        false => EnqueueDecision::Retry,
    };
    let running = events
        .processor("alerts", Arc::clone(&alerts))
        .with_options(fast())
        .with_dead_letters(parked.clone(), 3)
        .with_enqueue_policy(policy)
        .spawn();

    let last = ticks(&events, [("a", 1), ("a", 2), ("a", 3)]).await;
    tokio::time::timeout(Duration::from_secs(5), running.caught_up(last))
        .await
        .expect("the ignored event does not hold the segment");
    assert_eq!(*alerts.sent.lock().unwrap(), [1, 3], "the same aggregate goes on");
    assert!(parked.letters("alerts").is_empty(), "ignored, not parked");
    running.stop().await;
}

#[tokio::test]
async fn the_policy_parks_with_diagnostics_and_evicts_what_keeps_failing() {
    let events = Events::new(Store::new());
    let alerts = Arc::new(Alerts::default());
    alerts.refusing.store(true, Ordering::SeqCst);
    let parked = InMemoryDeadLetters::default();
    // Park at the first failure, noting the target; evict after two more
    // failed retries.
    let policy = |letter: &DeadLetter<Ticked>, _: &HandlerError| match letter.is_parked() {
        false => EnqueueDecision::Enqueue(Metadata::from([("target", "alert service")])),
        true if letter.attempts >= 3 => EnqueueDecision::Evict,
        true => EnqueueDecision::Requeue(letter.diagnostics.clone()),
    };
    let processor = || {
        events
            .processor("alerts", Arc::clone(&alerts))
            .with_options(fast())
            .with_dead_letters(parked.clone(), 5)
            .with_enqueue_policy(policy)
    };
    let running = processor().spawn();
    let last = ticks(&events, [("a", 1), ("b", 2), ("b", 4)]).await;
    tokio::time::timeout(Duration::from_secs(5), running.caught_up(last))
        .await
        .unwrap();
    running.stop().await;

    // Tick 2 parked at its first failure, tick 4 behind it (same clock).
    let letters = parked.letters("alerts");
    assert_eq!(letters.len(), 2);
    assert_eq!((letters[0].attempts, letters[0].is_parked()), (1, true));
    assert_eq!(
        letters[0].diagnostics.get("target").map(String::as_str),
        Some("alert service")
    );

    // Retries fail: requeued (attempts counted, last touched), then evicted,
    // and the clock's next letter is handled.
    let retry = processor();
    let outcome = retry.retry_dead_letters().await.unwrap();
    assert_eq!((outcome.handled, outcome.still_parked, outcome.evicted), (0, 1, 0));
    let letters = parked.letters("alerts");
    assert_eq!(letters[0].attempts, 2);
    assert!(letters[0].last_touched.is_some());

    let outcome = retry.retry_dead_letters().await.unwrap();
    assert_eq!((outcome.handled, outcome.still_parked, outcome.evicted), (1, 0, 1));
    assert_eq!(
        *alerts.sent.lock().unwrap(),
        [1, 4],
        "tick 2 evicted, tick 4 then handled"
    );
    assert!(parked.letters("alerts").is_empty());
}
