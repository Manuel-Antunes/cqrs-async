//! Composition root: the only place that names concrete adapters. It
//! builds the blog's [`Cqrs`] unit three ways, over the same application:
//!
//! - [`in_memory`]: everything synchronous, the projections run inside each
//!   commit. No runtime needed; what the domain and flow tests use.
//! - [`start_in_memory`]: asynchronous and in memory. Commands return once
//!   committed; the projections follow on their own Tokio tasks
//!   ([`TrackingProcessor`](cqrs::TrackingProcessor)s), and queries can be
//!   subscribed to.
//! - [`start`]: the same on PostgreSQL, with SeaORM as the blog's own
//!   infrastructure: posts keep their state in a table, saved with their
//!   events in one transaction; the read model is written in the
//!   projection's transaction, with its token; the blog's migrations run
//!   the library's tables too.
//!
//! The application never knows which: the repository is a port
//! ([`PostRepository`](crate::application::commands::PostRepository)),
//! and each wiring picks its adapter. What the unit exposes is named by the
//! traits the gateways generate ([`PostCommands`], [`PostQueries`],
//! [`PostSubscriptions`]).

use std::sync::Arc;

use cqrs::{Cqrs, Events, InMemoryEventStore, InMemorySnapshots, InlinePublisher, Intercepted};
use cqrs_postgres::sqlx::PgPool;
use sea_orm::{DbErr, SqlxPostgresConnector};
use sea_orm_migration::MigratorTrait;

use crate::{
    application::{
        commands::{PostCommandGateway, PostCommands},
        projections::{PostProjection, PostSummaryUpdates},
        queries::{PostQueries, PostQueryGateway, PostSubscriptions, PostUpdates},
    },
    domain::{Post, PostEvent},
    infrastructure::{
        Audit, AuditLog, EventSourcedPostRepository, InMemoryNotifier, InMemoryPostSummaryRepository, Measured,
        ProjectionMetrics, SeaOrmPostRepository, SeaOrmPostSummaries, postgres::Migrator,
    },
};

/// The in-memory event store, shared by the repository (reads) and the unit
/// (commits, processors, subscriptions). A handle: clones share it.
pub type PostStore = InMemoryEventStore<PostEvent>;

/// How many events of a post a find replays before it keeps a snapshot.
const SNAPSHOT_EVERY: u64 = 50;

/// The posts' repository over `store`: it rebuilds posts from their events,
/// keeping a snapshot of the long-lived ones.
fn posts_over(store: &PostStore) -> EventSourcedPostRepository<PostStore, InMemorySnapshots<Post>> {
    EventSourcedPostRepository::new(store.clone()).with_snapshots(InMemorySnapshots::default(), SNAPSHOT_EVERY)
}

/// The blog, wired in memory and synchronously: the projection and the
/// notification run inside each commit, in the caller's task. Returns the
/// read model and the notifier too, so callers can observe the side
/// effects.
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
    let summaries = Arc::new(InMemoryPostSummaryRepository::default());
    let notifier = Arc::new(InMemoryNotifier::default());
    // Synchronous: no subscriptions to push updates to. Each namespace runs
    // isolated: a failing notification never holds the read model back.
    let projection = PostProjection::new(Arc::clone(&summaries), Arc::clone(&notifier));
    let publisher = InlinePublisher::<PostEvent, _, _>::new(store.clone()).with_projection(projection);
    let blog = Cqrs::from_publisher(publisher)
        .commands(|events| PostCommandGateway::new(posts_over(&store), events))
        .queries(PostQueryGateway::new(Arc::clone(&summaries)))
        .build();
    (blog, summaries, notifier)
}

/// The post events of the asynchronous in-memory wiring, as the unit makes
/// them from its store: the publisher, the processors' source and the
/// subscriptions' feed.
pub type PostEvents = Events<PostEvent, PostStore>;

/// The running application: what [`start`] and [`start_in_memory`] return.
/// `R` is the read model, `E` the unit's events.
pub struct Running<C, Q, R, E> {
    /// The blog's unit, as the API serves it.
    pub app: Arc<Cqrs<C, Q, E>>,
    /// The read model the projection keeps.
    pub summaries: Arc<R>,
    /// Where the notification delivers.
    pub notifier: Arc<InMemoryNotifier>,
    /// Every command handled, with its correlation id and outcome.
    pub audit: Arc<AuditLog>,
    /// Events the projection applied, and the time it took.
    pub metrics: Arc<ProjectionMetrics>,
}

impl<C, Q, R, Ev, S, T, M> Running<C, Q, R, Events<Ev, S, T, M>>
where
    Ev: cqrs::DomainEvent,
    S: cqrs::ReadLog<Ev> + Clone + 'static,
    T: Send + Sync,
    M: Send + Sync,
{
    /// Waits until both processors handled every event committed so far:
    /// after it, queries see every change made before the call.
    pub async fn settled(&self) {
        self.app.settled().await;
    }
}

impl<C, Q, R, E> Running<C, Q, R, E> {
    /// Stops the processors (each after its current event).
    pub async fn stop(self) {
        self.app.stop().await;
    }
}

/// Wires and starts the asynchronous blog in memory: the unit commits to
/// the store and broadcasts, two tracking processors (the summaries and the
/// notification, each named by its namespace) follow it with their own
/// tokens, and the queries can be subscribed to. Commands go through the
/// [`Audit`] interceptor, the projection's events through [`Measured`].
///
/// # Panics
///
/// Outside a Tokio runtime (the processors are spawned).
pub fn start_in_memory()
-> Running<impl PostCommands, impl PostQueries + PostSubscriptions, InMemoryPostSummaryRepository, PostEvents> {
    let store = PostStore::default();
    let updates = Arc::new(PostUpdates::default());
    let summaries = Arc::new(InMemoryPostSummaryRepository::default());
    let notifier = Arc::new(InMemoryNotifier::default());
    let audit = Arc::new(AuditLog::default());
    let metrics = Arc::new(ProjectionMetrics::default());

    let projection = PostProjection::new(Arc::clone(&summaries), Arc::clone(&notifier));
    let emitter = PostSummaryUpdates::new(Arc::clone(&summaries), Arc::clone(&updates));
    let app = Cqrs::new(Events::new(store.clone()))
        .commands(|events| PostCommandGateway::new(posts_over(&store), events))
        .intercept(Audit(Arc::clone(&audit)))
        .queries(PostQueryGateway::new(Arc::clone(&summaries)))
        .projection(Intercepted::new(projection, Measured(Arc::clone(&metrics))))
        .emit(emitter)
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

/// The post events on PostgreSQL: the store keeps them in the log `posts`,
/// the processors' tokens and units of work are the database's.
pub type PgPostEvents = cqrs_postgres::PgEvents<PostEvent>;

/// Wires and starts the blog on PostgreSQL, with SeaORM: runs the blog's
/// migrations (the library's tables among them), then the same unit as
/// [`start_in_memory`] over the database. Posts are kept by
/// [`SeaOrmPostRepository`] (state and events in one transaction), the read
/// model by [`SeaOrmPostSummaries`] (written in the projection's
/// transaction, with its token). Several instances can share the database:
/// the processors share their segments.
///
/// # Errors
///
/// If the migrations fail.
///
/// # Panics
///
/// Outside a Tokio runtime (the processors are spawned).
pub async fn start(
    pool: PgPool,
) -> Result<Running<impl PostCommands, impl PostQueries + PostSubscriptions, SeaOrmPostSummaries, PgPostEvents>, DbErr>
{
    Migrator::up(&SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone()), None).await?;
    let events = cqrs_postgres::Postgres::new(pool.clone()).events::<PostEvent>("posts");
    let updates = Arc::new(PostUpdates::default());
    let summaries = Arc::new(SeaOrmPostSummaries::new(pool.clone()));
    let notifier = Arc::new(InMemoryNotifier::default());
    let audit = Arc::new(AuditLog::default());
    let metrics = Arc::new(ProjectionMetrics::default());

    let projection = PostProjection::new(Arc::clone(&summaries), Arc::clone(&notifier));
    let emitter = PostSummaryUpdates::new(Arc::clone(&summaries), Arc::clone(&updates));
    let app = Cqrs::new(events)
        .commands(|events| PostCommandGateway::new(SeaOrmPostRepository, events))
        .intercept(Audit(Arc::clone(&audit)))
        .queries(PostQueryGateway::new(Arc::clone(&summaries)))
        .projection(Intercepted::new(projection, Measured(Arc::clone(&metrics))))
        .emit(emitter)
        .subscriptions(updates)
        .build();

    Ok(Running {
        app: Arc::new(app),
        summaries,
        notifier,
        audit,
        metrics,
    })
}
