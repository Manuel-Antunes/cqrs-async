use std::{future::ready, marker::PhantomData, sync::Arc, time::Duration};

use futures_core::Stream;
use futures_util::{StreamExt, TryStreamExt, future::Either, stream};
use tokio::sync::watch;

use super::{ProcessorHandle, ProcessorStatus};
use crate::{Dispatch, DomainEvent, EventFeed, Message, QuerySubscriptions, RefreshedBy, StoreError, TokenStore};

/// Refreshes re-run the query at most once per this many events that were
/// already waiting: a burst of changes costs one query, not one per event.
const BURST: usize = 256;

/// A query gateway whose queries can also be subscribed to
/// ([`QuerySubscriptions`]).
///
/// It wraps the application's query gateway `G` (queries still work as
/// usual through it), the feed of the events the read model is built from,
/// and the progress of the processor that builds it.
pub struct LiveQueries<G, F, E, P = LocalProgress> {
    /// Answers the queries.
    queries: Arc<G>,
    /// The events the read model is projected from.
    events: F,
    /// How far the projection behind `queries` got.
    projection: P,
    /// The event type `F` streams.
    event: PhantomData<fn() -> E>,
}

impl<G, F, E> LiveQueries<G, F, E> {
    /// Subscriptions over `queries`, refreshed from `events` once
    /// `projection` (the processor feeding the read model, in this process)
    /// handled them. Right when one instance runs the projection; with
    /// several, use [`with_progress`](Self::with_progress) and a
    /// [`GlobalProgress`].
    pub fn new(queries: G, events: F, projection: &ProcessorHandle) -> Self {
        Self::with_progress(queries, events, LocalProgress(projection.progress()))
    }
}

impl<G, F, E, P> LiveQueries<G, F, E, P> {
    /// Subscriptions refreshed once `progress` says the projection handled
    /// the change.
    pub fn with_progress(queries: G, events: F, progress: P) -> LiveQueries<G, F, E, P> {
        LiveQueries {
            queries: Arc::new(queries),
            events,
            projection: progress,
            event: PhantomData,
        }
    }
}

/// How far a projection got: what a live query waits on before re-running.
pub trait ProjectionProgress: Clone + Send + Sync + 'static {
    /// Returns once the projection handled the event at `position` (or it
    /// can no longer tell, e.g. it stopped).
    fn reached(&self, position: u64) -> impl Future<Output = ()> + Send;
}

/// The progress of one processor instance, in this process: exact when it
/// is the only instance running the projection.
#[derive(Clone)]
pub struct LocalProgress(watch::Receiver<ProcessorStatus>);

impl LocalProgress {
    /// The progress of the processor `handle` runs.
    pub fn of(handle: &ProcessorHandle) -> Self {
        Self(handle.progress())
    }
}

impl ProjectionProgress for LocalProgress {
    async fn reached(&self, position: u64) {
        let mut status = self.0.clone();
        // Closed: the processor stopped; do not leave subscribers hanging.
        let _ = status.wait_for(|status| status.covers(position)).await;
    }
}

/// Several projections: reached once each of them reached the position
/// (what a [`Cqrs`](crate::Cqrs) unit's subscriptions wait for).
impl<P: ProjectionProgress> ProjectionProgress for Vec<P> {
    async fn reached(&self, position: u64) {
        for progress in self {
            progress.reached(position).await;
        }
    }
}

/// The progress of a processor across **all** its instances, read from the
/// shared token store: once every segment's cursor covers a position. What live
/// queries need when several instances share the projection's segments, as
/// a change may be projected by another instance.
pub struct GlobalProgress<T> {
    /// The shared token store.
    tokens: T,
    /// The processor's name (its token key).
    processor: Arc<str>,
    /// How often the token store is checked while waiting.
    poll: Duration,
}

impl<T> GlobalProgress<T> {
    /// The progress of `processor`, read from `tokens` every 50 ms while
    /// waiting.
    pub fn new(tokens: T, processor: &str) -> Self {
        Self {
            tokens,
            processor: processor.into(),
            poll: Duration::from_millis(50),
        }
    }
}

impl<T: Clone> Clone for GlobalProgress<T> {
    fn clone(&self) -> Self {
        Self {
            tokens: self.tokens.clone(),
            processor: Arc::clone(&self.processor),
            poll: self.poll,
        }
    }
}

impl<T: TokenStore + Clone + 'static> ProjectionProgress for GlobalProgress<T> {
    async fn reached(&self, position: u64) {
        loop {
            match self.tokens.cursors(&self.processor).await {
                Ok(cursors) if !cursors.is_empty() && cursors.iter().all(|cursor| cursor.covers(position)) => return,
                Ok(_) => {}
                Err(error) => tracing::warn!(processor = %self.processor, %error, "cannot read progress"),
            }
            tokio::time::sleep(self.poll).await;
        }
    }
}

/// Plain queries go straight to the wrapped gateway.
impl<M, G, F, E, P> Dispatch<M> for LiveQueries<G, F, E, P>
where
    M: Message,
    G: Dispatch<M>,
    F: Send + Sync,
    P: Send + Sync,
{
    fn dispatch_with(
        &self,
        message: M,
        metadata: crate::Metadata,
    ) -> impl Future<Output = Result<M::Output, M::Error>> + Send {
        self.queries.dispatch_with(message, metadata)
    }
}

impl<Q, G, F, E, P> QuerySubscriptions<Q> for LiveQueries<G, F, E, P>
where
    P: ProjectionProgress,
    E: DomainEvent,
    Q: RefreshedBy<E> + Sync,
    Q::Output: PartialEq + Clone + Sync,
    Q::Error: From<StoreError>,
    G: Dispatch<Q> + 'static,
    F: EventFeed<E> + Clone + 'static,
{
    fn subscribe(&self, query: Q) -> impl Stream<Item = Result<Q::Output, Q::Error>> + Send + use<Q, G, F, E, P> {
        let queries = Arc::clone(&self.queries);
        let events = self.events.clone();
        let projection = self.projection.clone();

        // Set up on first poll: "now" is when the client starts listening.
        stream::once(async move {
            let start = match events.last_position().await {
                Ok(start) => start,
                Err(error) => return Either::Left(stream::iter([Err(error.into())])),
            };
            // Listen first, then read: nothing committed in between is lost.
            let changes = events.subscribe(start);
            projection.reached(start).await;
            let current = queries.dispatch(query.clone()).await;

            let relevant = query.clone();
            let refreshes = changes
                .try_filter(move |event| ready(relevant.refreshed_by(event)))
                .ready_chunks(BURST)
                .then(move |burst| {
                    let (queries, query, projection) = (Arc::clone(&queries), query.clone(), projection.clone());
                    async move {
                        // A failed read ends the subscription with its error.
                        let burst = burst.into_iter().collect::<Result<Vec<_>, _>>()?;
                        // Committed out of order, a burst is not one range.
                        for event in &burst {
                            projection.reached(event.position).await;
                        }
                        queries.dispatch(query).await
                    }
                });
            Either::Right(stream::once(ready(current)).chain(refreshes))
        })
        .flatten()
        .filter_map(distinct())
    }
}

/// Drops results equal to the previous one (RxJS's
/// `distinctUntilChanged`); errors always pass.
fn distinct<T: PartialEq + Clone, Er>() -> impl FnMut(Result<T, Er>) -> std::future::Ready<Option<Result<T, Er>>> {
    let mut last: Option<T> = None;
    move |item| {
        ready(match item {
            Ok(value) if last.as_ref() == Some(&value) => None,
            Ok(value) => {
                last = Some(value.clone());
                Some(Ok(value))
            }
            Err(error) => Some(Err(error)),
        })
    }
}
