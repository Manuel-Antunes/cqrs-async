//! A projection's namespaces: each runs in a processor of its own, named
//! `"{projection}.{namespace}"`, with its own tokens; a failing namespace
//! holds only itself back; a replay reaches only the namespaces that see
//! replays.

#![cfg(feature = "tokio")]

use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

use cqrs::{
    AppendEvents, EventEnvelope, EventHandler, Events, ExpectedVersion, HandlerError, InMemoryEventStore, Metadata,
    ProcessingContext, StreamId,
};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum Ticked {
    Tick(u64),
}

#[cqrs::projection(name = "clock", events = Ticked, namespaces("counts", "alerts"))]
#[derive(Default)]
struct ClockProjection {
    /// The ticks the `counts` read model saw.
    counted: Mutex<Vec<u64>>,
    /// The ticks `alerts` sent.
    alerted: Mutex<Vec<u64>>,
    /// `alerts` is down until this is set.
    alerts_up: AtomicBool,
}

#[cqrs::event_handler(namespace = "counts")]
impl EventHandler for ClockProjection {
    async fn handle(&self, event: &EventEnvelope<Ticked>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        let Ticked::Tick(n) = event.payload;
        self.counted.lock().unwrap().push(n);
        Ok(())
    }

    async fn reset(&self, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        self.counted.lock().unwrap().clear();
        Ok(())
    }
}

#[cqrs::event_handler(namespace = "alerts")]
impl EventHandler for ClockProjection {
    const REPLAY: bool = false;

    async fn handle(
        &self,
        event: &EventEnvelope<Ticked>,
        context: &mut ProcessingContext<'_>,
    ) -> Result<(), HandlerError> {
        assert_eq!(context.processor(), "clock.alerts");
        if !self.alerts_up.load(Ordering::SeqCst) {
            return Err(HandlerError::msg("alerts are down"));
        }
        let Ticked::Tick(n) = event.payload;
        self.alerted.lock().unwrap().push(n);
        Ok(())
    }
}

async fn tick(events: &Events<Ticked, InMemoryEventStore<Ticked>>, from: u64, count: u64) -> u64 {
    let ticks = (from..from + count).map(Ticked::Tick).collect();
    let appended = events
        .store()
        .append(
            &mut (),
            &StreamId::new("clock", "1"),
            ExpectedVersion::Any,
            ticks,
            &Metadata::new(),
        )
        .await
        .unwrap();
    appended.last().unwrap().position
}

#[tokio::test(start_paused = true)]
async fn each_namespace_runs_on_its_own_and_only_read_models_replay() {
    use cqrs::{Projection, TokenStore};
    let events = Events::new(InMemoryEventStore::new());
    let projection = std::sync::Arc::new(ClockProjection::default());
    let running = events.projection(std::sync::Arc::clone(&projection)).spawn();

    // `alerts` is down: `counts` goes on regardless.
    let last = tick(&events, 1, 3).await;
    running.namespace("counts").unwrap().caught_up(last).await;
    assert_eq!(*projection.counted.lock().unwrap(), [1, 2, 3]);
    assert!(projection.alerted.lock().unwrap().is_empty());

    // Back up: `alerts` catches up from its own token.
    projection.alerts_up.store(true, Ordering::SeqCst);
    running.caught_up(last).await;
    assert_eq!(*projection.alerted.lock().unwrap(), [1, 2, 3]);
    running.stop().await;

    // Each namespace has its own token, named after the projection.
    assert_eq!(ClockProjection::processor_name(0), "clock.counts");
    let positions = |name: &str| events.tokens().positions(name);
    assert_eq!(
        (positions("clock.counts"), positions("clock.alerts")),
        (vec![3], vec![3])
    );
    assert!(
        events.tokens().cursors("clock").await.unwrap().is_empty(),
        "no token of the projection itself"
    );

    // A replay rebuilds `counts`; `alerts` sends nothing again.
    let processors = events.projection(std::sync::Arc::clone(&projection));
    processors.reset().await.unwrap();
    assert!(projection.counted.lock().unwrap().is_empty(), "cleared by its reset");
    let running = processors.spawn();
    running.caught_up(last).await;
    assert_eq!(*projection.counted.lock().unwrap(), [1, 2, 3], "rebuilt");
    assert_eq!(*projection.alerted.lock().unwrap(), [1, 2, 3], "not sent twice");
    running.stop().await;
}
