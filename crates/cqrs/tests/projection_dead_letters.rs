//! A projection's namespaces with dead letters: an event its side effect
//! keeps failing on is parked (with its aggregate's later events), the other
//! aggregates go on, and a retry once the cause is fixed handles it.

#![cfg(feature = "tokio")]

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use cqrs::{
    Cqrs,
    event::{EventEnvelope, Metadata, StreamId},
    processing::{EventHandler, Events, HandlerError, InMemoryDeadLetters, ProcessingContext, TrackingOptions},
    store::{AppendEvents, ExpectedVersion, InMemoryEventStore},
};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum Ticked {
    Tick(u64),
}

#[cqrs::projection(name = "clock", events = Ticked, namespaces("alerts"))]
#[derive(Default)]
struct ClockProjection {
    /// What the alerts sent.
    sent: Mutex<Vec<u64>>,
    /// The alert service refuses tick 2 while this is set.
    refusing: AtomicBool,
}

#[cqrs::event_handler(namespace = "alerts")]
impl EventHandler for ClockProjection {
    const REPLAY: bool = false;

    async fn handle(&self, event: &EventEnvelope<Ticked>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        let Ticked::Tick(n) = event.payload;
        if n == 2 && self.refusing.load(Ordering::SeqCst) {
            return Err(HandlerError::msg("the alert service refuses tick 2"));
        }
        self.sent.lock().unwrap().push(n);
        Ok(())
    }
}

async fn tick(events: &Events<Ticked, InMemoryEventStore<Ticked>>, clock: &str, n: u64) {
    events
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
}

#[tokio::test]
async fn a_namespace_parks_what_keeps_failing_and_a_retry_handles_it() {
    let events = Events::new(InMemoryEventStore::new());
    let projection = Arc::new(ClockProjection::default());
    projection.refusing.store(true, Ordering::SeqCst);
    let parked = InMemoryDeadLetters::default();
    let fast = TrackingOptions::default()
        .retry_initial(Duration::from_millis(1))
        .retry_max(Duration::from_millis(5));
    let app = Cqrs::new(events.clone())
        .options(fast)
        .projection_with(Arc::clone(&projection), |processors| {
            processors.with_dead_letters(parked.clone(), 2)
        })
        .build();

    tick(&events, "a", 1).await;
    tick(&events, "b", 2).await; // refused: parked after two attempts
    tick(&events, "a", 3).await; // another aggregate: not held back
    tokio::time::timeout(Duration::from_secs(5), app.settled())
        .await
        .expect("parked, the event no longer holds the namespace back");
    assert_eq!(*projection.sent.lock().unwrap(), [1, 3]);
    assert_eq!(
        parked.letters("clock.alerts").len(),
        1,
        "parked under the namespace's processor"
    );

    // The cause is fixed: a retry, from processors built like the running ones.
    projection.refusing.store(false, Ordering::SeqCst);
    let outcomes = events
        .projection(Arc::clone(&projection))
        .with_dead_letters(parked.clone(), 2)
        .retry_dead_letters()
        .await
        .unwrap();
    assert_eq!(
        outcomes,
        [(
            "alerts",
            cqrs::processing::RetryOutcome {
                handled: 1,
                still_parked: 0
            }
        )]
    );
    assert_eq!(*projection.sent.lock().unwrap(), [1, 3, 2]);
    assert!(parked.letters("clock.alerts").is_empty());
    app.stop().await;
}
