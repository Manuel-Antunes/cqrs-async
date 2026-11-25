//! Two processes over one log: each one's live feed carries only its own
//! commits, so the other's reach it at the next poll, unless it is told
//! that the log changed (`Events::committed_elsewhere`).

#![cfg(feature = "tokio")]

use std::{pin::pin, sync::Arc, time::Duration};

use cqrs::{
    aggregate::Loaded,
    command::{EventPublisher, Pending},
    event::EventEnvelope,
    processing::{EventHandler, Events, HandlerError, ProcessingContext},
    store::{EventFeed, InMemoryEventStore, SubscriptionOptions},
};
use futures_util::StreamExt;

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum Ticked {
    Tick,
}

#[cqrs::aggregate(name = "clock", event = Ticked)]
struct Clock {
    id: u32,
}

impl Clock {
    fn apply(&mut self, _: &Ticked) {}
}

/// A process: its own `Events` (its own live feed) over the shared store,
/// polling so rarely that only a wake-up delivers quickly.
fn process(store: &InMemoryEventStore<Ticked>) -> Events<Ticked, InMemoryEventStore<Ticked>> {
    Events::new(store.clone()).with_options(SubscriptionOptions::default().poll_interval(Duration::from_secs(30)))
}

async fn tick(events: &Events<Ticked, InMemoryEventStore<Ticked>>, id: u32, version: u64) {
    let clock = if version == 0 {
        cqrs::aggregate::New::<Clock>(id).into()
    } else {
        Loaded::at(Clock { id }, version)
    };
    events.commit(clock, Pending::new(vec![Ticked::Tick])).await.unwrap();
}

#[tokio::test]
async fn a_subscription_reads_what_another_process_committed_once_woken() {
    let store = InMemoryEventStore::new();
    let (here, there) = (process(&store), process(&store));
    let mut stream = pin!(here.subscribe(0));
    // Caught up and waiting: this process's own commit comes live.
    tick(&here, 1, 0).await;
    let first = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
    assert_eq!(first.unwrap().unwrap().unwrap().position, 1);

    // Committed by the other process: not in this one's live feed.
    tick(&there, 1, 1).await;
    let quiet = tokio::time::timeout(Duration::from_millis(300), stream.next()).await;
    assert!(quiet.is_err(), "nothing tells this process before its next poll");

    here.committed_elsewhere();
    let event = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
    assert_eq!(event.unwrap().unwrap().unwrap().position, 2);
}

/// Counts what it handles.
#[derive(Default)]
struct Count(std::sync::atomic::AtomicU64);

#[cqrs::event_handler]
impl EventHandler for Count {
    async fn handle(&self, _: &EventEnvelope<Ticked>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn a_processor_handles_what_another_process_committed_once_woken() {
    let store = InMemoryEventStore::new();
    let (here, there) = (process(&store), process(&store));
    let count = Arc::new(Count::default());
    let running = here.processor("count", Arc::clone(&count)).spawn();
    tokio::time::sleep(Duration::from_millis(100)).await; // claimed, caught up, waiting

    tick(&there, 1, 0).await;
    tick(&there, 1, 1).await;
    here.committed_elsewhere();
    tokio::time::timeout(Duration::from_secs(2), running.caught_up(2))
        .await
        .expect("woken, the processor reads the store");
    assert_eq!(count.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    running.stop().await;
}
