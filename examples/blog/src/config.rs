//! Composition root: the only place that names concrete adapters. It
//! builds the blog's [`Cqrs`] unit, twice:
//!
//! - [`in_memory`]: everything synchronous, the projection and the policy
//!   run inside each commit. No runtime needed; what the domain and flow
//!   tests use.
//! - [`start`]: the asynchronous one the server runs. Commands return once
//!   committed; the projection and the policy follow on their own Tokio
//!   tasks ([`TrackingProcessor`](cqrs::TrackingProcessor)s), and queries
//!   can be subscribed to.
//!
//! The stores are created here and shared: the posts' repository reads the
//! event store, and the unit commits to it. Gateways are built with their
//! services only; the unit gives the command gateway its publisher. What the
//! unit exposes is named by the ports the message enums generate
//! ([`PostCommands`], [`PostQueries`], [`PostSubscriptions`]).

use std::sync::Arc;

use cqrs::{Cqrs, Events, InMemoryEventStore, InMemorySnapshots, InMemoryTokenStore, InlinePublisher, Intercepted};

use crate::{
    application::{
        commands::{PostCommandGateway, PostCommands},
        policies::NotifyOnPublish,
        projections::PostSummaryProjection,
        queries::{PostQueries, PostQueryGateway, PostSubscriptions, PostUpdates},
    },
    domain::{Post, PostEvent},
    infrastructure::{
        Audit, AuditLog, EventSourcedPostRepository, InMemoryNotifier, InMemoryPostSummaryRepository, Measured,
        ProjectionMetrics,
    },
};

/// The in-memory event store, shared by the repository (reads) and the unit
/// (commits, processors, subscriptions).
pub type PostStore = InMemoryEventStore<PostEvent>;

/// How many events of a post a find replays before it keeps a snapshot.
const SNAPSHOT_EVERY: u64 = 50;

/// The posts' repository over `store`: it rebuilds posts from their events,
/// keeping a snapshot of the long-lived ones.
fn posts_over(store: &PostStore) -> EventSourcedPostRepository<PostStore, InMemorySnapshots<Post>> {
    EventSourcedPostRepository::new((*store).clone()).with_snapshots(InMemorySnapshots::default(), SNAPSHOT_EVERY)
}

/// The blog, wired in memory and synchronously: the projection and the
/// notification policy run inside each commit, in the caller's task.
/// Returns the read model and the notifier too, so callers can observe the
/// side effects.
// The types are inferred by the builder: naming them is what this avoids.
#[allow(clippy::type_complexity)]
pub fn in_memory() -> (
    Cqrs<impl PostCommands, impl PostQueries>,
    Arc<InMemoryPostSummaryRepository>,
    Arc<InMemoryNotifier>,
) {
    in_memory_with(PostStore::default())
}

/// [`in_memory`] over `store`, which the caller keeps: to read the log, or
/// to build repositories and publishers of its own over it.
#[allow(clippy::type_complexity)]
pub fn in_memory_with(
    store: PostStore,
) -> (
    Cqrs<impl PostCommands, impl PostQueries>,
    Arc<InMemoryPostSummaryRepository>,
    Arc<InMemoryNotifier>,
) {
    let posts = Arc::new(posts_over(&store));
    let summaries = Arc::new(InMemoryPostSummaryRepository::default());
    let notifier = Arc::new(InMemoryNotifier::default());
    // Synchronous: no subscriptions to push updates to.
    let processors = (
        PostSummaryProjection::new(Arc::clone(&summaries), Arc::default()),
        NotifyOnPublish::new(Arc::clone(&notifier)),
    );
    let blog = Cqrs::from_publisher(InlinePublisher::<PostEvent, _, _>::new(store).with_processor(processors))
        .commands(|events| PostCommandGateway::new(posts, events))
        .queries(PostQueryGateway::new(Arc::clone(&summaries)))
        .build();
    (blog, summaries, notifier)
}

/// The post events of the asynchronous wiring, as the unit makes them from
/// its stores: the publisher, the processors' source and the subscriptions'
/// feed.
pub type PostEvents = Events<PostEvent, PostStore, InMemoryTokenStore>;

/// The running application: what [`start`] returns.
pub struct Running<C, Q> {
    /// The blog's unit, as the API serves it.
    pub app: Arc<Cqrs<C, Q, PostEvents>>,
    /// The read model the projection keeps.
    pub summaries: Arc<InMemoryPostSummaryRepository>,
    /// Where the notification policy delivers.
    pub notifier: Arc<InMemoryNotifier>,
    /// Every command handled, with its correlation id and outcome.
    pub audit: Arc<AuditLog>,
    /// Events the projection applied, and the time it took.
    pub metrics: Arc<ProjectionMetrics>,
}

impl<C, Q> Running<C, Q> {
    /// Waits until both processors handled every event committed so far:
    /// after it, queries see every change made before the call.
    pub async fn settled(&self) {
        self.app.settled().await;
    }

    /// Stops the processors (each after its current event).
    pub async fn stop(self) {
        self.app.stop().await;
    }
}

/// Wires and starts the asynchronous blog: the unit commits to the store
/// and broadcasts, two tracking processors (the projection and the
/// notification policy, each named by its namespace) follow it with their
/// own tokens, and the queries can be subscribed to. Commands go through
/// the [`Audit`] interceptor, the projection's events through [`Measured`].
///
/// # Panics
///
/// Outside a Tokio runtime (the processors are spawned).
pub fn start() -> Running<impl PostCommands, impl PostQueries + PostSubscriptions> {
    let store = PostStore::default();
    let posts = Arc::new(posts_over(&store));
    let updates = Arc::new(PostUpdates::default());
    let summaries = Arc::new(InMemoryPostSummaryRepository::default());
    let notifier = Arc::new(InMemoryNotifier::default());
    let audit = Arc::new(AuditLog::default());
    let metrics = Arc::new(ProjectionMetrics::default());

    let projection = PostSummaryProjection::new(Arc::clone(&summaries), Arc::clone(&updates));
    let app = Cqrs::new(store, InMemoryTokenStore::default())
        .commands(|events| PostCommandGateway::new(posts, events))
        .intercept(Audit(Arc::clone(&audit)))
        .queries(PostQueryGateway::new(Arc::clone(&summaries)))
        .projection(Intercepted::new(projection, Measured(Arc::clone(&metrics))))
        .policy(NotifyOnPublish::new(Arc::clone(&notifier)))
        .subscriptions(updates)
        .build();

    Running {
        app: Arc::new(app),
        summaries,
        notifier,
        audit,
        metrics,
    }
}
