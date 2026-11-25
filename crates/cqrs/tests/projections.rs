//! Projections in the token store's unit of work, and subscription queries
//! pushed by them.

#![cfg(feature = "tokio")]

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use cqrs::{
    event::{EventEnvelope, Metadata, StreamId},
    processing::{EventHandler, Events, HandlerError, ProcessingContext, ProcessorHandle, ProcessorProgress},
    query::{
        EmitUpdates, QueryGateway, QuerySubscriptions, QueryUpdateChannel, SubscriptionQueries, emit_updates, head_of,
    },
    store::{AppendEvents, ExpectedVersion, InMemoryEventStore, SubscriptionOptions},
};
use futures_util::StreamExt;

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
struct Ticked(u64);

#[cqrs::query(output = Option<u64>, error = ())]
#[derive(Clone, Debug, PartialEq)]
struct TotalOf {
    clock: String,
}

#[cqrs::query(output = u64, error = ())]
#[derive(Clone, Debug, PartialEq)]
struct GrandTotal;

/// Where the updates of each query are emitted.
#[derive(Default, cqrs::QueryUpdates)]
struct Updates {
    totals: QueryUpdateChannel<TotalOf>,
    grand: QueryUpdateChannel<GrandTotal>,
}

/// An in-memory read model: sums per clock, then pushes the new totals once
/// applied. Its context names no unit of work: it runs with any token store.
struct Totals {
    sums: Mutex<BTreeMap<String, u64>>,
    updates: Arc<Updates>,
}

#[cqrs::event_handler]
impl EventHandler for Totals {
    async fn handle(&self, event: &EventEnvelope<Ticked>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        *self
            .sums
            .lock()
            .unwrap()
            .entry(event.stream.id().to_owned())
            .or_default() += event.payload.0;
        Ok(())
    }

    async fn reset(&self, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        self.sums.lock().unwrap().clear();
        Ok(())
    }
}

/// Pushes the new totals once the processor handled an event, from the read
/// model as it is then: the emitter a projection's `updated` is, by hand.
impl EmitUpdates<Ticked> for Totals {
    async fn emit(&self, event: &EventEnvelope<Ticked>) {
        let (total, grand) = {
            let sums = self.sums.lock().unwrap();
            (sums[event.stream.id()], sums.values().sum())
        };
        let clock = event.stream.id();
        self.updates.totals.emit(|query| query.clock == clock, Some(total));
        self.updates.grand.emit(|_| true, grand);
    }
}

/// Answers from the read model.
#[cqrs::query_gateway(handles(TotalOf, GrandTotal))]
struct Queries(Arc<Totals>);

#[cqrs::query_handler]
impl cqrs::query::QueryHandler<TotalOf> for Queries {
    async fn handle(&self, query: TotalOf) -> Result<Option<u64>, ()> {
        Ok(self.0.sums.lock().unwrap().get(&query.clock).copied())
    }
}

#[cqrs::query_handler]
impl cqrs::query::QueryHandler<GrandTotal> for Queries {
    async fn handle(&self, _: GrandTotal) -> Result<u64, ()> {
        Ok(self.0.sums.lock().unwrap().values().sum())
    }
}

/// Emits only, holds nothing; no namespace: it runs in a group.
struct Ticks(Arc<QueryUpdateChannel<GrandTotal, u64>>);

#[cqrs::event_handler]
impl EventHandler for Ticks {
    async fn handle(&self, _: &EventEnvelope<Ticked>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        Ok(())
    }
}

impl EmitUpdates<Ticked> for Ticks {
    async fn emit(&self, event: &EventEnvelope<Ticked>) {
        self.0.emit(|_| true, event.position);
    }
}

/// Runs `emitter` over `events` from their head, each event once `processor`
/// handled it: how updates are pushed without a `Cqrs` unit.
async fn emitting<H: EmitUpdates<Ticked> + 'static>(
    events: &Events<Ticked, InMemoryEventStore<Ticked>>,
    processor: &ProcessorHandle,
    emitter: H,
) -> tokio::task::JoinHandle<()> {
    let head = head_of(events).await;
    let progress = ProcessorProgress::of(processor);
    tokio::spawn(emit_updates(
        head,
        events.clone(),
        progress,
        emitter,
        std::future::pending(),
    ))
}

async fn tick(events: &Events<Ticked, InMemoryEventStore<Ticked>>, clock: &str, n: u64) {
    let stream = StreamId::new("clock", clock.to_owned());
    events
        .store()
        .append(
            &mut (),
            &stream,
            ExpectedVersion::Any,
            vec![Ticked(n)],
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
async fn subscriptions_get_the_current_result_then_the_updates_for_their_query() {
    // Ticks go straight to the store, not through the live feed: the
    // processor and the emitter see them at their next poll.
    let events = Events::new(InMemoryEventStore::new())
        .with_options(SubscriptionOptions::default().poll_interval(Duration::from_millis(10)));
    let updates = Arc::new(Updates::default());
    let totals = Arc::new(Totals {
        sums: Mutex::default(),
        updates: Arc::clone(&updates),
    });
    let queries = SubscriptionQueries::new(Queries(Arc::clone(&totals)), Arc::clone(&updates));
    tick(&events, "a", 1).await;
    let projection = events.processor("totals", Arc::clone(&totals)).spawn();
    within(projection.caught_up(1)).await;
    let emitter = emitting(&events, &projection, Arc::clone(&totals)).await;

    let mut a = Box::pin(queries.subscribe(TotalOf { clock: "a".into() }));
    let mut b = Box::pin(queries.subscribe(TotalOf { clock: "b".into() }));
    let mut grand = Box::pin(queries.subscribe(GrandTotal));
    assert_eq!(within(a.next()).await, Some(Ok(Some(1))), "the current result first");
    assert_eq!(within(b.next()).await, Some(Ok(None)));
    assert_eq!(within(grand.next()).await, Some(Ok(1)));

    // The emitter reads the read model once the processor handled each
    // event: one event at a time, each update is that event's.
    tick(&events, "a", 2).await;
    assert_eq!(within(a.next()).await, Some(Ok(Some(3))));
    assert_eq!(within(grand.next()).await, Some(Ok(3)));
    tick(&events, "b", 5).await;
    assert_eq!(
        within(b.next()).await,
        Some(Ok(Some(5))),
        "only updates for its own clock"
    );
    assert_eq!(within(grand.next()).await, Some(Ok(8)));
    // Plain queries go through the same object.
    assert_eq!(queries.query(GrandTotal).await, Ok(8));

    drop(a);
    tick(&events, "a", 1).await;
    // Emitted after the totals: once it arrives, the totals were emitted.
    assert_eq!(within(grand.next()).await, Some(Ok(9)));
    assert_eq!(updates.totals.subscriptions(), 1, "a dropped subscription is forgotten");
    emitter.abort();
    projection.stop().await;
}

#[tokio::test]
async fn a_processing_group_of_projections_shares_one_token_and_one_unit_of_work() {
    let events = Events::new(InMemoryEventStore::new());
    let updates = Arc::new(Updates::default());
    let totals = Arc::new(Totals {
        sums: Mutex::default(),
        updates: Arc::clone(&updates),
    });
    let positions = Arc::new(QueryUpdateChannel::<GrandTotal, u64>::new());
    let mut seen = Box::pin(positions.subscribe(GrandTotal));

    let group = events
        .processor("group", (Arc::clone(&totals), Ticks(Arc::clone(&positions))))
        .spawn();
    let emitter = emitting(&events, &group, Ticks(Arc::clone(&positions))).await;
    for n in 1..=3 {
        tick(&events, "a", n).await;
    }
    within(group.caught_up(3)).await;
    let pushed: Vec<u64> = within(seen.by_ref().take(3).collect()).await;
    assert_eq!(pushed, [1, 2, 3], "once the group handled each, in order");
    assert_eq!(totals.sums.lock().unwrap()["a"], 6);
    assert_eq!(events.tokens().positions("group"), [3], "one token for the group");

    // Reset: the group's members clear, then the replay rebuilds.
    emitter.abort();
    group.stop().await;
    events
        .processor("group", (Arc::clone(&totals), Ticks(Arc::clone(&positions))))
        .reset()
        .await
        .unwrap();
    assert!(totals.sums.lock().unwrap().is_empty());
    let group = events
        .processor("group", (Arc::clone(&totals), Ticks(positions)))
        .spawn();
    within(group.caught_up(3)).await;
    assert_eq!(totals.sums.lock().unwrap()["a"], 6);
    group.stop().await;
}
