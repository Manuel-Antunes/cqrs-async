//! Replaying a tracking processor: the projection is
//! cleared and rebuilt, a side-effect handler skips the replayed events.

#![cfg(feature = "tokio")]

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use cqrs::{
    event::{EventEnvelope, Metadata, StreamId},
    processing::{EventHandler, Events, HandlerError, InMemoryTokenStore, ProcessingContext, ResetError},
    store::{AppendEvents, ExpectedVersion, InMemoryEventStore},
};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
struct Ticked(u64);

type Store = InMemoryEventStore<Ticked>;

/// Rebuildable: what it saw, cleared on reset.
#[derive(Default)]
struct Projection {
    seen: Mutex<Vec<u64>>,
    resets: Mutex<u32>,
}

impl EventHandler<Ticked> for Projection {
    async fn handle(&self, event: &EventEnvelope<Ticked>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        self.seen.lock().unwrap().push(event.payload.0);
        Ok(())
    }

    async fn reset(&self, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        self.seen.lock().unwrap().clear();
        *self.resets.lock().unwrap() += 1;
        Ok(())
    }
}

/// A side effect (an e-mail): must not happen again for a replayed event.
#[derive(Default)]
struct Mailer {
    sent: Mutex<Vec<u64>>,
}

impl EventHandler<Ticked> for Mailer {
    async fn handle(&self, event: &EventEnvelope<Ticked>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        self.sent.lock().unwrap().push(event.payload.0);
        Ok(())
    }

    const REPLAY: bool = false;
}

async fn tick(events: &Events<Ticked, Store, InMemoryTokenStore>, n: u64) {
    let stream = StreamId::new("clock", n.to_string());
    events
        .store()
        .append(
            &mut (),
            &stream,
            ExpectedVersion::NoStream,
            vec![Ticked(n)],
            &Metadata::new(),
        )
        .await
        .unwrap();
}

async fn eventually(check: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !check() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("timed out");
}

#[tokio::test]
async fn a_reset_replays_into_a_cleared_projection_and_skips_side_effects() {
    let events = Events::new(InMemoryEventStore::new());
    let projection = Arc::new(Projection::default());
    let mailer = Arc::new(Mailer::default());
    for n in 1..=3 {
        tick(&events, n).await;
    }
    let running = (
        events.track("projection", Arc::clone(&projection)),
        events.track("mailer", Arc::clone(&mailer)),
    );
    eventually(|| projection.seen.lock().unwrap().len() == 3 && mailer.sent.lock().unwrap().len() == 3).await;

    // Refused while running: nothing touched.
    let refused = events.processor("projection", Arc::clone(&projection)).reset().await;
    assert!(matches!(refused, Err(ResetError::Running)));
    assert_eq!(*projection.resets.lock().unwrap(), 0);

    running.0.stop().await;
    running.1.stop().await;
    events
        .processor("projection", Arc::clone(&projection))
        .reset()
        .await
        .unwrap();
    events.processor("mailer", Arc::clone(&mailer)).reset().await.unwrap();
    assert_eq!(*projection.resets.lock().unwrap(), 1);
    assert!(projection.seen.lock().unwrap().is_empty(), "cleared before the replay");

    let _projection = events.track("projection", Arc::clone(&projection));
    let _mailer = events.track("mailer", Arc::clone(&mailer));
    tick(&events, 4).await;
    eventually(|| projection.seen.lock().unwrap().len() == 4 && mailer.sent.lock().unwrap().len() == 4).await;
    assert_eq!(*projection.seen.lock().unwrap(), [1, 2, 3, 4], "rebuilt from the start");
    assert_eq!(
        *mailer.sent.lock().unwrap(),
        [1, 2, 3, 4],
        "the replayed 1..=3 are not sent again"
    );
}

#[tokio::test]
async fn a_processing_group_replays_its_projection_but_not_its_side_effects() {
    let events = Events::new(InMemoryEventStore::new());
    let projection = Arc::new(Projection::default());
    let mailer = Arc::new(Mailer::default());
    let group = || (Arc::clone(&projection), Arc::clone(&mailer));
    for n in 1..=3 {
        tick(&events, n).await;
    }
    // One processor, one token, two handlers.
    let running = events.track("group", group());
    eventually(|| mailer.sent.lock().unwrap().len() == 3).await;
    running.stop().await;

    events.processor("group", group()).reset().await.unwrap();
    assert_eq!(*projection.resets.lock().unwrap(), 1, "the group resets its members");
    let _running = events.track("group", group());
    tick(&events, 4).await;
    eventually(|| projection.seen.lock().unwrap().len() == 4 && mailer.sent.lock().unwrap().len() == 4).await;
    assert_eq!(*projection.seen.lock().unwrap(), [1, 2, 3, 4], "the projection replays");
    assert_eq!(
        *mailer.sent.lock().unwrap(),
        [1, 2, 3, 4],
        "the mailer only gets the new event"
    );
}
