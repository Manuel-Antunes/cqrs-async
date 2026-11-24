//! Subscription queries pushed by projections (Axon's `QueryUpdateEmitter`):
//! a subscriber gets the query's current result, then every update a
//! projection emits for a query like its own.
//!
//! ```text
//! // The projection, once its change is committed:
//! self.updates.find.emit(|query: &FindPost| query.id == id, Some(summary));
//!
//! // The API:
//! let stream = queries.subscribe(FindPost { id });   // initial result, then updates
//! ```
//!
//! Without `dyn`, there is one emitter per query type; a struct of them
//! with `#[derive(QueryUpdates)]` says where each query's updates come from.

use std::{
    marker::PhantomData,
    pin::pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_core::Stream;
use futures_util::{StreamExt, stream};
use tokio::sync::mpsc;

use super::ProjectionProgress;
use crate::{Dispatch, DomainEvent, EventEnvelope, EventFeed, Message, Metadata, OutputOf, Query, QuerySubscriptions};

/// Updates a subscriber may fall behind by before it is dropped (its stream
/// ends; a client subscribes again).
const BUFFER: usize = 256;

/// Pushes updates to the subscriptions of query `Q` matching a filter. `U`
/// is what an update carries: by default the query's new result.
pub struct QueryUpdateEmitter<Q, U = OutputOf<Q>> {
    /// Each live subscription: its query and where its updates go.
    subscribers: Mutex<Vec<(Q, mpsc::Sender<U>)>>,
}

impl<Q, U> Default for QueryUpdateEmitter<Q, U> {
    fn default() -> Self {
        Self {
            subscribers: Mutex::new(Vec::new()),
        }
    }
}

impl<Q, U: Clone> QueryUpdateEmitter<Q, U> {
    /// An emitter with no subscriptions yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sends `update` to every subscription whose query `filter` accepts;
    /// returns how many got it. Subscriptions that were dropped are
    /// forgotten, and so are those too far behind (their stream ends).
    pub fn emit(&self, filter: impl Fn(&Q) -> bool, update: U) -> usize {
        let mut subscribers = self.subscribers.lock().expect("emitter poisoned");
        let mut sent = 0;
        subscribers.retain(|(query, sender)| {
            if sender.is_closed() {
                return false;
            }
            if !filter(query) {
                return true;
            }
            match sender.try_send(update.clone()) {
                Ok(()) => {
                    sent += 1;
                    true
                }
                // Full: too far behind; closed: gone. Either way, drop it.
                Err(_) => false,
            }
        });
        sent
    }

    /// Registers `query` and returns its updates, from now on.
    pub fn subscribe(&self, query: Q) -> impl Stream<Item = U> + Send + use<Q, U>
    where
        U: Send + 'static,
    {
        let (sender, receiver) = mpsc::channel(BUFFER);
        self.subscribers.lock().expect("emitter poisoned").push((query, sender));
        stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|update| (update, receiver))
        })
    }

    /// How many subscriptions are live (dropped ones are counted until the
    /// next emit forgets them).
    pub fn subscriptions(&self) -> usize {
        self.subscribers.lock().expect("emitter poisoned").len()
    }
}

/// Where the updates of query `Q` are emitted: implemented by
/// `#[derive(QueryUpdates)]` for each `QueryUpdateEmitter<Q>` field.
pub trait UpdatesOf<Q: Query>: Send + Sync {
    /// The emitter of `Q`'s updates.
    fn updates(&self) -> &QueryUpdateEmitter<Q>;
}

// Forwards every method, so an override of a provided one is never lost.
#[deny(clippy::missing_trait_methods)]
impl<Q: Query, T: UpdatesOf<Q>> UpdatesOf<Q> for Arc<T> {
    fn updates(&self) -> &QueryUpdateEmitter<Q> {
        (**self).updates()
    }
}

/// What a subscription waits for before reading its initial result.
pub trait CatchUp: Clone + Send + Sync + 'static {
    /// Returns once the read model is as recent as the subscription needs.
    fn caught_up(&self) -> impl Future<Output = ()> + Send;
}

/// Nothing: the initial result is whatever the read model holds now.
impl CatchUp for () {
    async fn caught_up(&self) {}
}

/// Read your writes: the projection has handled every event committed when
/// the subscription started (built by
/// [`SubscriptionQueries::after_projection`]).
pub struct Projected<F, P, E> {
    /// Where the committed head is read.
    feed: F,
    /// How far the projection got.
    progress: P,
    /// Emitters started with the unit: each says once it took its starting
    /// point, which a subscription's must not precede.
    emitters: Vec<tokio::sync::watch::Receiver<bool>>,
    /// The feed's event type.
    event: PhantomData<fn() -> E>,
}

impl<F: Clone, P: Clone, E> Clone for Projected<F, P, E> {
    fn clone(&self) -> Self {
        Self {
            feed: self.feed.clone(),
            progress: self.progress.clone(),
            emitters: self.emitters.clone(),
            event: PhantomData,
        }
    }
}

impl<F, P, E> CatchUp for Projected<F, P, E>
where
    E: DomainEvent,
    F: EventFeed<E> + Clone + 'static,
    P: ProjectionProgress,
{
    async fn caught_up(&self) {
        // The emitters follow the log from before this subscription's start,
        // so none of its updates is missed.
        for started in &self.emitters {
            let _ = started.clone().wait_for(|started| *started).await;
        }
        // A failing read only costs freshness: the updates follow anyway.
        let head = self.feed.last_position().await.unwrap_or(0);
        self.progress.reached(head).await;
    }
}

/// A query gateway `G` whose queries can also be subscribed to: the current
/// result from `G`, then the updates projections emit through `U` (Axon's
/// subscription queries).
///
/// Updates are local to the process: with several instances, use
/// [`LiveQueries`](crate::LiveQueries), which re-queries after the
/// projection's global progress.
pub struct SubscriptionQueries<G, U, C = ()> {
    /// Answers the initial queries (and plain ones).
    queries: Arc<G>,
    /// The emitters projections push through.
    updates: Arc<U>,
    /// What the initial query waits for.
    catch_up: C,
}

impl<G, U> SubscriptionQueries<G, U> {
    /// Subscriptions over `queries`, updated through `updates` (shared with
    /// the projections that emit). The initial result is read right away.
    pub fn new(queries: G, updates: Arc<U>) -> Self {
        Self {
            queries: Arc::new(queries),
            updates,
            catch_up: (),
        }
    }

    /// Reads each initial result once the projection (followed by
    /// `progress`) handled every event `events` had committed when the
    /// subscription started: a client subscribing right after its own
    /// command sees it.
    pub fn after_projection<E, F, P>(self, events: F, progress: P) -> SubscriptionQueries<G, U, Projected<F, P, E>>
    where
        E: DomainEvent,
        F: EventFeed<E> + Clone + 'static,
        P: ProjectionProgress,
    {
        SubscriptionQueries {
            queries: self.queries,
            updates: self.updates,
            catch_up: Projected {
                feed: events,
                progress,
                emitters: Vec::new(),
                event: PhantomData,
            },
        }
    }
}

impl<G, U, F, P, E> SubscriptionQueries<G, U, Projected<F, P, E>> {
    /// Subscriptions also wait, before their first result, until each of
    /// `emitters` took its starting point (a `true` on the channel).
    #[cfg_attr(not(feature = "tokio"), allow(dead_code))]
    pub(crate) fn after_emitters(mut self, emitters: Vec<tokio::sync::watch::Receiver<bool>>) -> Self {
        self.catch_up.emitters = emitters;
        self
    }
}

impl<G, U, C> SubscriptionQueries<G, U, C> {
    /// The emitters, to hand to projections.
    pub fn updates(&self) -> &Arc<U> {
        &self.updates
    }
}

impl<M: Message, G: Dispatch<M>, U: Send + Sync, C: Send + Sync> Dispatch<M> for SubscriptionQueries<G, U, C> {
    fn dispatch_with(
        &self,
        message: M,
        metadata: Metadata,
    ) -> impl Future<Output = Result<M::Output, M::Error>> + Send {
        self.queries.dispatch_with(message, metadata)
    }
}

impl<Q, G, U, C> QuerySubscriptions<Q> for SubscriptionQueries<G, U, C>
where
    Q: Query + Clone + Sync,
    Q::Output: Clone + PartialEq + Send + Sync,
    G: Dispatch<Q> + 'static,
    U: UpdatesOf<Q> + 'static,
    C: CatchUp,
{
    fn subscribe(&self, query: Q) -> impl Stream<Item = Result<Q::Output, Q::Error>> + Send + use<Q, G, U, C> {
        // Registered before the initial query runs: an update emitted in
        // between is not lost (at worst it repeats the initial result,
        // which `distinct` drops).
        let updates = self.updates.updates().subscribe(query.clone()).map(Ok);
        let queries = Arc::clone(&self.queries);
        let catch_up = self.catch_up.clone();
        let initial = stream::once(async move {
            catch_up.caught_up().await;
            queries.dispatch_with(query, Metadata::new()).await
        });
        distinct(initial.chain(updates))
    }
}

/// Drops results equal to the previous one.
fn distinct<T: Clone + PartialEq + Send, E: Send>(
    results: impl Stream<Item = Result<T, E>> + Send,
) -> impl Stream<Item = Result<T, E>> + Send {
    results
        .scan(None::<T>, |last, result| {
            let keep = match &result {
                Ok(value) if last.as_ref() == Some(value) => false,
                Ok(value) => {
                    *last = Some(value.clone());
                    true
                }
                Err(_) => true,
            };
            std::future::ready(Some(keep.then_some(result)))
        })
        .filter_map(std::future::ready)
}

/// Pushes the new results of the queries an event changed, once the read
/// models are up to date with it (Axon's `QueryUpdateEmitter` calls, kept
/// out of the projection's unit of work): typically a query on the read
/// model, then [`QueryUpdateEmitter::emit`].
///
/// It runs on **every** instance of the application ([`emit_updates`],
/// `Cqrs`'s `emit`), for every committed event, once the projections
/// handled it, whichever instance did: so a subscriber connected to any
/// instance sees every change. That is why it is not a projection's
/// [`committed`](crate::EventHandler::committed), which runs only on the
/// instance that handled the event.
pub trait EmitUpdates<E: DomainEvent>: Send + Sync {
    /// Emits what `event` changed. Failures are the emitter's to log: a
    /// missed update only leaves a subscriber behind until the next one.
    fn emit(&self, event: &EventEnvelope<E>) -> impl Future<Output = ()> + Send;
}

/// A shared emitter emits.
impl<E: DomainEvent, T: EmitUpdates<E>> EmitUpdates<E> for Arc<T> {
    fn emit(&self, event: &EventEnvelope<E>) -> impl Future<Output = ()> + Send {
        (**self).emit(event)
    }
}

/// Calls `emitter` for every event committed after position `after`, in
/// order, each once `progress` reached it, until `shutdown` completes: what
/// makes query updates reach the subscribers of every instance. Run it on
/// each instance (`Cqrs`'s `emit` spawns it).
///
/// A subscriber misses the updates of the events between `after` and the
/// position its subscription started at, so take `after` at the head
/// ([`head_of`]) before serving subscriptions.
pub async fn emit_updates<E, F, P, H>(
    mut after: u64,
    feed: F,
    progress: P,
    emitter: H,
    shutdown: impl Future<Output = ()> + Send,
) where
    E: DomainEvent,
    F: EventFeed<E>,
    P: super::ProjectionProgress,
    H: EmitUpdates<E>,
{
    let follow = async {
        loop {
            let mut events = pin!(feed.subscribe(after));
            while let Some(next) = events.next().await {
                match next {
                    Ok(event) => {
                        progress.reached(event.position).await;
                        emitter.emit(&event).await;
                        after = event.position;
                    }
                    Err(error) => {
                        tracing::warn!(%error, "cannot follow the events to emit updates");
                        break;
                    }
                }
            }
            tokio::time::sleep(RETRY).await; // then resume after the last one
        }
    };
    tokio::select! {
        () = follow => {}
        () = shutdown => {}
    }
}

/// The position of the last event committed to `feed`, retrying until it
/// can be read: where an emitter starts.
pub async fn head_of<E: DomainEvent, F: EventFeed<E>>(feed: &F) -> u64 {
    loop {
        match feed.last_position().await {
            Ok(head) => return head,
            Err(error) => {
                tracing::warn!(%error, "cannot read the head of the log");
                tokio::time::sleep(RETRY).await;
            }
        }
    }
}

/// Wait before following the log again after a failed read.
const RETRY: Duration = Duration::from_millis(100);
