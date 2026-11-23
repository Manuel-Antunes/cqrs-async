use std::{collections::VecDeque, sync::Arc, time::Duration};

use crate::{Cursor, DomainEvent, EventEnvelope, EventStore, Horizon, StoreError};
use futures_core::Stream;
use futures_util::{StreamExt, stream};
use tokio::{
    sync::broadcast::{self, error::RecvError},
    time::Instant,
};

use super::LiveEvents;

/// How a [`subscribe`] stream reads.
#[derive(Clone, Copy, Debug)]
pub struct SubscriptionOptions {
    /// Events per page when reading the store.
    pub batch_size: usize,
    /// How long to wait for a live event before checking the store anyway.
    /// Bounds how late an event whose broadcast was lost (a save cancelled
    /// after its append) is delivered.
    pub poll_interval: Duration,
}

impl Default for SubscriptionOptions {
    fn default() -> Self {
        Self {
            batch_size: 256,
            poll_interval: Duration::from_secs(1),
        }
    }
}

/// Every committed event with `position > after`, in position order, then
/// every new one as it is committed. Never ends on its own; it ends after
/// yielding a store error, or when the broadcaster is gone.
///
/// It switches between two sources:
///
/// - **the store** ([`EventStore::read`]), the source of truth: used to
///   catch up at the start, and whenever the live feed may have skipped
///   something;
/// - **the broadcaster**, for low latency once caught up.
///
/// It reads through a [`Cursor`], so a store committing positions out of
/// order loses nothing: a position read past while still being written is
/// a gap, picked up when it is committed (from either source) and given up
/// only once the store says nobody can write it any more. A live event is
/// taken only if it is the next position or fills a gap; anything else, a
/// receiver that lagged, and a quiet
/// [`poll_interval`](SubscriptionOptions::poll_interval) go back to the
/// store. Every event comes out exactly once per subscription.
///
/// The receiver is created before the first read, so nothing committed in
/// between is lost. The stream owns everything it needs and does not borrow
/// `live` (`use<E, S>`: without it, edition 2024 would tie the stream to the
/// `&LiveEvents` borrow), so it can be returned from a GraphQL resolver or
/// moved into a spawned task.
pub fn subscribe<E, S>(
    store: S,
    live: &LiveEvents<E>,
    after: u64,
    options: SubscriptionOptions,
) -> impl Stream<Item = Result<Arc<EventEnvelope<E>>, StoreError>> + Send + use<E, S>
where
    E: DomainEvent,
    S: EventStore<E> + 'static,
{
    track(store, live, Start::At(after), options).map(|item| item.map(|(event, _)| event))
}

/// Where a tracked stream starts.
pub(crate) enum Start {
    /// After this position, with the gaps the store reports below it.
    At(u64),
    /// From a stored cursor.
    From(Cursor),
}

/// [`subscribe`], yielding with each event the cursor once it is seen:
/// what a tracking processor stores.
pub(crate) fn track<E, S>(
    store: S,
    live: &LiveEvents<E>,
    start: Start,
    options: SubscriptionOptions,
) -> impl Stream<Item = Result<(Arc<EventEnvelope<E>>, Cursor), StoreError>> + Send + use<E, S>
where
    E: DomainEvent,
    S: EventStore<E> + 'static,
{
    let state = State {
        store,
        live: live.subscribe(),
        start: match start {
            Start::At(after) => Some(after),
            Start::From(_) => None,
        },
        cursor: match start {
            Start::At(after) => Cursor::at(after),
            Start::From(cursor) => cursor,
        },
        ready: VecDeque::new(),
        behind: true,
        read_at: Instant::now(),
        options,
        failed: false,
    };
    stream::unfold(state, State::next)
}

/// How soon the store is read again while the cursor has gaps: a gap filled
/// by another process shows up only there.
const GAP_RECHECK: Duration = Duration::from_millis(10);

/// Where a subscription is.
struct State<E, S> {
    /// The source of truth.
    store: S,
    /// The live feed.
    live: broadcast::Receiver<Arc<EventEnvelope<E>>>,
    /// A position to ask the store a cursor for, before the first read.
    start: Option<u64>,
    /// What was yielded.
    cursor: Cursor,
    /// Events fetched but not yielded yet, in position order, with what the
    /// store knew when it returned them.
    ready: VecDeque<(Arc<EventEnvelope<E>>, Horizon)>,
    /// Whether the store may hold events not yielded yet.
    behind: bool,
    /// When the store was last read.
    read_at: Instant,
    /// Page size and polling.
    options: SubscriptionOptions,
    /// Set after yielding an error: the next call ends the stream.
    failed: bool,
}

impl<E: DomainEvent, S: EventStore<E>> State<E, S> {
    /// Produces the next event, reading the store or the live feed as needed.
    async fn next(mut self) -> Option<(Result<(Arc<EventEnvelope<E>>, Cursor), StoreError>, Self)> {
        if self.failed {
            return None;
        }
        if let Some(after) = self.start.take() {
            match self.store.cursor_at(after).await {
                Ok(cursor) => self.cursor = cursor,
                Err(error) => return self.fail(error),
            }
        }
        loop {
            if let Some((event, horizon)) = self.ready.pop_front() {
                if !self.cursor.advance(event.position, &horizon) {
                    continue; // already yielded (read from both sources)
                }
                let cursor = self.cursor.clone();
                return Some((Ok((event, cursor)), self));
            }

            if self.behind || (!self.cursor.gaps().is_empty() && self.read_at.elapsed() >= GAP_RECHECK) {
                match self.store.read(&self.cursor, self.options.batch_size).await {
                    Ok((page, horizon)) => {
                        // A full page means there may be more.
                        self.behind = page.len() == self.options.batch_size;
                        self.read_at = Instant::now();
                        // Gaps nobody can fill any more are dropped; those
                        // this page fills are kept until it is yielded.
                        let fills = |position| page.binary_search_by_key(&position, |event| event.position).is_ok();
                        self.cursor.settle(&horizon, fills);
                        self.ready.extend(page.into_iter().map(|event| (Arc::new(event), horizon)));
                        continue;
                    }
                    Err(error) => return self.fail(error),
                }
            }

            let wait = if self.cursor.gaps().is_empty() { self.options.poll_interval } else { GAP_RECHECK };
            match tokio::time::timeout(wait, self.live.recv()).await {
                Ok(Ok(event)) if self.cursor.covers(event.position) => {}
                Ok(Ok(event)) if event.position == self.cursor.position() + 1 || self.cursor.is_gap(event.position) => {
                    self.ready.push_back((event, Horizon::SETTLED));
                }
                // Past a position not seen (it may be still being written,
                // or its broadcast was lost), or a receiver that fell
                // behind: the store knows.
                Ok(Ok(_) | Err(RecvError::Lagged(_))) => self.behind = true,
                Ok(Err(RecvError::Closed)) => return None,
                // Quiet for a while: check the store in case a broadcast was
                // lost with nothing after it to reveal the gap.
                Err(_elapsed) => self.behind = true,
            }
        }
    }

    /// Yields `error`; the stream ends after it.
    fn fail<T>(mut self, error: StoreError) -> Option<(Result<T, StoreError>, Self)> {
        self.failed = true;
        Some((Err(error), self))
    }
}
