//! A projection's subscription queries: `updates(..)` gives it a channel
//! per query, a namespace's `updated` emits through them on every instance
//! once it handled an event, and the unit's queries subscribe to them.

#![cfg(feature = "tokio")]

use std::{
    pin::pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use cqrs::{
    AppendEvents, Cqrs, EventEnvelope, EventHandler, Events, ExpectedVersion, HandlerError, InMemoryEventStore,
    Metadata, ProcessingContext, QueryHandler, QueryUpdateEmitter, StreamId,
};
use futures_util::StreamExt;

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum Ticked {
    Tick(u64),
}

/// The sum of the ticks.
#[cqrs::query(output = u64, error = ())]
#[derive(Clone, Debug)]
struct Total;

/// How many ticks there were.
#[cqrs::query(output = u64, error = ())]
#[derive(Clone, Debug)]
struct Count;

#[cqrs::projection(
    name = "clock",
    events = Ticked,
    namespaces("totals", "alerts"),
    updates(Total, Count),
    subscriptions = ClockSubscriptions
)]
#[derive(Default)]
struct ClockProjection {
    /// The read model: (sum, count).
    totals: Mutex<(u64, u64)>,
}

#[cqrs::event_handler(namespace = "totals")]
impl EventHandler for ClockProjection {
    async fn handle(&self, event: &EventEnvelope<Ticked>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        let Ticked::Tick(n) = event.payload;
        let mut totals = self.totals.lock().unwrap();
        *totals = (totals.0 + n, totals.1 + 1);
        Ok(())
    }

    async fn updated(&self, _: &EventEnvelope<Ticked>, updates: &QueryUpdateEmitter<Self>) {
        let (sum, count) = *self.totals.lock().unwrap();
        updates.emit(|_: &Total| true, sum);
        updates.emit::<Count>(|_| true, count);
    }
}

/// A side effect: no updates, so no emitter runs for it.
#[cqrs::event_handler(namespace = "alerts")]
impl EventHandler for ClockProjection {
    const REPLAY: bool = false;

    async fn handle(&self, _: &EventEnvelope<Ticked>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Answers from the projection's read model.
#[cqrs::query_gateway(handles(Total, Count))]
struct ClockQueries(Arc<ClockProjection>);

#[cqrs::query_handler]
impl QueryHandler<Total> for ClockQueries {
    async fn handle(&self, _: Total) -> Result<u64, ()> {
        Ok(self.0.totals.lock().unwrap().0)
    }
}

#[cqrs::query_handler]
impl QueryHandler<Count> for ClockQueries {
    async fn handle(&self, _: Count) -> Result<u64, ()> {
        Ok(self.0.totals.lock().unwrap().1)
    }
}

async fn tick(events: &Events<Ticked, InMemoryEventStore<Ticked>>, n: u64) {
    events
        .store()
        .append(
            &mut (),
            &StreamId::new("clock", "1"),
            ExpectedVersion::Any,
            vec![Ticked::Tick(n)],
            &Metadata::new(),
        )
        .await
        .unwrap();
}

async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("timed out")
}

#[tokio::test]
async fn a_projection_emits_its_updates_to_the_units_subscriptions() {
    use cqrs::QuerySubscriptions;

    let projection = Arc::new(ClockProjection::default());
    let events = Events::new(InMemoryEventStore::new());
    let app = Cqrs::new(events.clone())
        .queries(ClockQueries(Arc::clone(&projection)))
        .projection(Arc::clone(&projection))
        .build();
    fn subscribes_to_all<S: ClockSubscriptions>(_: &S) {}
    subscribes_to_all(app.queries());

    tick(&events, 2).await;
    let mut totals = pin!(app.queries().subscribe(Total));
    let mut counts = pin!(app.queries().subscribe(Count));
    // The current results, once the read model has what was committed.
    assert_eq!(within(totals.next()).await, Some(Ok(2)));
    assert_eq!(within(counts.next()).await, Some(Ok(1)));

    // Then what `updated` emits, each query on its own channel.
    tick(&events, 3).await;
    assert_eq!(within(totals.next()).await, Some(Ok(5)));
    assert_eq!(within(counts.next()).await, Some(Ok(2)));

    app.stop().await;
}
