//! Interceptors around event handlers and projections in tracking
//! processors (Axon's handler interceptors on event processors): order,
//! skipping an event, failing it inside the projection's transaction.

#![cfg(feature = "tokio")]

use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use cqrs::{
    AppendEvents, DomainEvent, EventChain, EventEnvelope, EventHandler, EventInterceptor, Events, ExpectedVersion,
    HandlerError, InMemoryEventStore, Intercepted, Metadata, ProcessingContext, StreamId,
};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
struct Ticked(u64);

/// Records what runs, in order.
type Log = Arc<Mutex<Vec<String>>>;

/// Logs around each event.
struct Around(&'static str, Log);

impl<E: DomainEvent, Tx: Send> EventInterceptor<E, Tx> for Around {
    async fn intercept<N: EventChain<Tx>>(
        &self,
        event: &EventEnvelope<E>,
        context: &mut ProcessingContext<'_, Tx>,
        next: N,
    ) -> Result<(), HandlerError> {
        self.1
            .lock()
            .unwrap()
            .push(format!("{} before {}", self.0, event.position));
        let result = next.proceed(context).await;
        self.1
            .lock()
            .unwrap()
            .push(format!("{} after {}", self.0, event.position));
        result
    }
}

/// Skips odd ticks: the handler never sees them, yet they count as handled.
struct EvenOnly;

impl<Tx: Send> EventInterceptor<Ticked, Tx> for EvenOnly {
    async fn intercept<N: EventChain<Tx>>(
        &self,
        event: &EventEnvelope<Ticked>,
        context: &mut ProcessingContext<'_, Tx>,
        next: N,
    ) -> Result<(), HandlerError> {
        if event.payload.0 % 2 == 1 {
            return Ok(());
        }
        next.proceed(context).await
    }
}

/// Fails once, after the projection applied its change: the transaction
/// rolls back, the processor retries.
struct FailsOnce(AtomicBool);

impl<E: DomainEvent, Tx: Send> EventInterceptor<E, Tx> for FailsOnce {
    async fn intercept<N: EventChain<Tx>>(
        &self,
        _: &EventEnvelope<E>,
        context: &mut ProcessingContext<'_, Tx>,
        next: N,
    ) -> Result<(), HandlerError> {
        next.proceed(context).await?;
        if !self.0.swap(true, Ordering::Relaxed) {
            return Err(HandlerError::msg("failed after the change"));
        }
        Ok(())
    }
}

struct Handler(Log);

impl EventHandler<Ticked> for Handler {
    async fn handle(&self, event: &EventEnvelope<Ticked>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        self.0.lock().unwrap().push(format!("handled {}", event.payload.0));
        Ok(())
    }
}

/// In memory: the sum of the ticks.
#[derive(Default)]
struct Sum(Mutex<u64>);

#[cqrs::projection(namespace = "sum")]
impl EventHandler for Sum {
    async fn handle(&self, event: &EventEnvelope<Ticked>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        *self.0.lock().unwrap() += event.payload.0;
        Ok(())
    }
}

type Store = InMemoryEventStore<Ticked>;

async fn tick(events: &Events<Ticked, Store>, ticks: impl IntoIterator<Item = u64>) -> u64 {
    let ticks: Vec<Ticked> = ticks.into_iter().map(Ticked).collect();
    let stream = StreamId::new("clock", "1");
    let appended = events
        .store()
        .append(&mut (), &stream, ExpectedVersion::Any, ticks, &Metadata::new())
        .await
        .unwrap();
    appended.last().unwrap().position
}

async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("timed out")
}

#[tokio::test]
async fn event_interceptors_run_in_order_and_can_skip_an_event() {
    let events = Events::new(Store::default());
    let log = Log::default();
    let interceptors = (
        Around("outer", Arc::clone(&log)),
        Around("inner", Arc::clone(&log)),
        EvenOnly,
    );
    let processor = events
        .processor("ticks", Intercepted::new(Handler(Arc::clone(&log)), interceptors))
        .spawn();
    let last = tick(&events, [1, 2]).await;
    within(processor.caught_up(last)).await;
    processor.stop().await;

    let log = log.lock().unwrap();
    assert_eq!(
        *log,
        [
            "outer before 1",
            "inner before 1",
            "inner after 1", // skipped: never handled
            "outer after 1",
            "outer before 2",
            "inner before 2",
            "handled 2",
            "inner after 2",
            "outer after 2",
        ]
    );
    assert_eq!(
        events.tokens().positions("ticks"),
        [2],
        "the skipped event counts as handled"
    );
}

#[tokio::test]
async fn a_projection_interceptor_runs_inside_its_transaction() {
    let events = Events::new(Store::default());
    let sum = Arc::new(Sum::default());
    let intercepted = Intercepted::new(Arc::clone(&sum), FailsOnce(AtomicBool::new(false)));
    let projection = events.projection(intercepted).spawn();
    let last = tick(&events, [5, 7]).await;
    within(projection.caught_up(last)).await;
    projection.stop().await;
    // In memory nothing rolls back, so the first try's change stays: the
    // retry applies 5 again. A SQL transaction discards it (see the
    // Postgres tests); here the point is that the failure is retried.
    assert_eq!(*sum.0.lock().unwrap(), 5 + 5 + 7);
}
