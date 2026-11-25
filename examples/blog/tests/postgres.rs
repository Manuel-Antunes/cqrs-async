//! The blog on PostgreSQL with SeaORM ([`blog::start`]): the acceptance
//! test of the library's "the infrastructure is the application's" design.
//!
//! - the whole application runs on the database, and a restart picks up
//!   where it was (ids, tokens);
//! - a post's state (a SeaORM table) and its events commit together, and a
//!   lost race undoes both;
//! - the read model, written with SeaORM in the projection's transaction,
//!   applies each event once, even when the handling fails after writing.
//!
//! Skipped without `CQRS_POSTGRES_URL` (each test gets a schema of its own).

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use blog::{
    application::{
        commands::{CreatePost, PostRepository, PublishPost, RetitlePost},
        projections::PostProjection,
        queries::{FindPost, ListPublished},
    },
    domain::{Post, PostEvent, PostId},
    infrastructure::{SeaOrmPostRepository, SeaOrmPostSummaries, postgres::Migrator},
};
use cqrs::{
    Intercepted,
    aggregate::New,
    command::{CommandContext, CommandGateway},
    event::EventEnvelope,
    processing::{EventChain, EventInterceptor, HandlerError, ProcessingContext, TrackingOptions},
    query::QueryGateway,
    store::StoreError,
};
use cqrs_postgres::sqlx::{
    self, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use sea_orm::SqlxPostgresConnector;
use sea_orm_migration::MigratorTrait;

/// A pool on a fresh schema, or `None` without a database to test against.
async fn database() -> Option<PgPool> {
    let url = std::env::var("CQRS_POSTGRES_URL").ok()?;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let schema = format!("blog_{}_{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed));
    let admin = PgPool::connect(&url).await.expect("connecting");
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("creating the schema");
    let options: PgConnectOptions = url.parse().expect("a postgres url");
    let pool = PgPoolOptions::new()
        .max_connections(16)
        .connect_with(options.options([("search_path", schema.as_str())]))
        .await
        .expect("connecting to the schema");
    Some(pool)
}

macro_rules! database_or_skip {
    () => {
        match database().await {
            Some(pool) => pool,
            None => {
                eprintln!("CQRS_POSTGRES_URL not set: skipped");
                return;
            }
        }
    };
}

/// Fails within `seconds`, rather than hanging.
async fn within<T>(seconds: u64, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(seconds), future)
        .await
        .expect("in time")
}

async fn count(pool: &PgPool, sql: &'static str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_blog_runs_on_postgres_and_a_restart_picks_up_where_it_was() {
    let pool = database_or_skip!();
    let running = blog::start(pool.clone()).await.expect("migrated");
    let commands = running.app.commands();
    let id = commands.send(CreatePost { title: "first".into() }).await.unwrap();
    commands
        .send(RetitlePost {
            id,
            title: "First".into(),
        })
        .await
        .unwrap();
    commands.send(PublishPost { id }).await.unwrap();
    within(10, running.settled()).await;

    let found = running.app.queries().query(FindPost { id }).await.unwrap().unwrap();
    assert_eq!((found.title.as_str(), found.published), ("First", true));
    assert_eq!(running.app.queries().query(ListPublished).await.unwrap(), [found]);
    assert_eq!(running.notifier.sent().len(), 1, "the notification ran once");
    // The post's state is kept with its stream's version; the events too.
    let state: (String, bool, i64) = sqlx::query_as("SELECT title, published, version FROM posts WHERE id = $1")
        .bind(id.0 as i64)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, ("First".to_owned(), true, 3));
    assert_eq!(
        count(&pool, "SELECT count(*) FROM cqrs_events WHERE log = 'posts'").await,
        3
    );
    running.stop().await;

    // Restarted on the same database: the migrations are done, ids go on,
    // and the projection resumes from its token (nothing projected twice).
    let running = blog::start(pool.clone()).await.expect("migrated again");
    let next = running
        .app
        .commands()
        .send(CreatePost { title: "second".into() })
        .await
        .unwrap();
    assert!(next.0 > id.0, "ids come from the database");
    within(10, running.settled()).await;
    assert_eq!(running.metrics.handled(), 2, "only the new event, in both namespaces");
    assert_eq!(count(&pool, "SELECT count(*) FROM post_summaries").await, 2);
    running.stop().await;
}

/// The post events on PostgreSQL, as the blog wires them.
fn post_events(pool: &PgPool) -> blog::PgPostEvents {
    cqrs_postgres::Postgres::new(pool.clone()).events("posts")
}

/// What a command's unit of work does, by hand: opens the transaction, has
/// the repository save `post` after what `appender` recorded for it,
/// appends the events in the same transaction, commits and announces.
async fn save(
    unit: &blog::PgPostEvents,
    posts: &SeaOrmPostRepository,
    post: cqrs::aggregate::Loaded<Post>,
    appender: &mut CommandContext<PostEvent>,
) -> Result<(), StoreError> {
    use cqrs::command::UnitOfWork;
    type Unit = cqrs::command::InTransaction<cqrs_postgres::PgTransaction>;
    let mut tx = UnitOfWork::<PostEvent, Unit>::begin(unit).await?;
    let post = appender.record(post);
    posts.save(&post, &mut tx).await?;
    let appended = cqrs::command::append_writes::<_, Unit, _>(unit, &mut tx, appender.take_writes()).await?;
    UnitOfWork::<PostEvent, Unit>::commit(unit, tx).await?;
    UnitOfWork::<PostEvent, Unit>::announce(unit, appended).await;
    Ok(())
}

/// The post `id` as the repository finds it, in a transaction of its own.
async fn find(pool: &PgPool, id: PostId) -> Option<cqrs::aggregate::Loaded<Post>> {
    let mut tx = pool.begin().await.unwrap();
    SeaOrmPostRepository.find(id, &mut tx).await.unwrap()
}

/// A new post's id, from the repository.
async fn next_id(pool: &PgPool) -> PostId {
    let mut tx = pool.begin().await.unwrap();
    let id = SeaOrmPostRepository.next_id(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_post_state_and_its_events_commit_together_and_a_lost_race_undoes_both() {
    let pool = database_or_skip!();
    Migrator::up(&SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone()), None)
        .await
        .unwrap();
    let events = post_events(&pool);
    let posts = SeaOrmPostRepository;

    let id = next_id(&pool).await;
    let mut appender = CommandContext::new();
    Post::create(id, "draft", &mut appender).unwrap();
    save(&events, &posts, New::<Post>(id).into(), &mut appender)
        .await
        .unwrap();

    // Two writers find the same version and decide...
    let first = find(&pool, id).await.unwrap();
    let second = find(&pool, id).await.unwrap();
    assert_eq!(first.version(), Some(1));
    let (mut renamed, mut published) = (CommandContext::new(), CommandContext::new());
    first.retitle("renamed", &mut renamed).unwrap();
    second.publish(&mut published).unwrap();

    // ...the first one wins; the second's events and state are undone.
    save(&events, &posts, first, &mut renamed).await.unwrap();
    let lost = save(&events, &posts, second, &mut published).await;
    assert!(matches!(lost, Err(StoreError::Conflict { .. })), "{lost:?}");

    let kept = find(&pool, id).await.unwrap();
    assert_eq!(
        (kept.title(), kept.is_published(), kept.version()),
        ("renamed", false, Some(2))
    );
    let stream = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM cqrs_events WHERE stream_id = $1")
        .bind(id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(stream, 2, "the losing event was never stored");
}

/// Lets the projection's `summaries` namespace write, then fails the first
/// time it sees `position` there: what the processor's rollback must undo.
struct FailsOnceAfterWriting {
    position: u64,
    /// How many times it saw `position`.
    seen: Arc<AtomicU32>,
}

impl<Tx: Send> EventInterceptor<PostEvent, Tx> for FailsOnceAfterWriting {
    async fn intercept<N: EventChain<Tx>>(
        &self,
        event: &EventEnvelope<PostEvent>,
        context: &mut ProcessingContext<'_, Tx>,
        next: N,
    ) -> Result<(), HandlerError> {
        let summaries = context.processor() == "post.summaries";
        next.proceed(context).await?;
        if summaries && event.position == self.position && self.seen.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(HandlerError::msg("failed after writing"));
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_seaorm_projection_applies_each_event_once_even_when_it_fails_after_writing() {
    let pool = database_or_skip!();
    Migrator::up(&SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone()), None)
        .await
        .unwrap();
    let events = post_events(&pool);
    let posts = SeaOrmPostRepository;
    let id = next_id(&pool).await;
    let mut appender = CommandContext::new();
    Post::create(id, "draft", &mut appender).unwrap();
    save(&events, &posts, New::<Post>(id).into(), &mut appender)
        .await
        .unwrap();

    // The creation's insert is written, then the handling fails: if the
    // insert were kept, the retry would hit the primary key forever.
    let created = sqlx::query_scalar::<_, i64>("SELECT max(position) FROM cqrs_events")
        .fetch_one(&pool)
        .await
        .unwrap() as u64;
    let seen = Arc::new(AtomicU32::new(0));
    let interceptor = FailsOnceAfterWriting {
        position: created,
        seen: Arc::clone(&seen),
    };
    let summaries = Arc::new(SeaOrmPostSummaries::new(pool.clone()));
    let projection = Intercepted::new(
        PostProjection::new(
            Arc::clone(&summaries),
            Arc::new(blog::infrastructure::InMemoryNotifier::default()),
        ),
        interceptor,
    );
    let options = TrackingOptions::default().retry_initial(Duration::from_millis(10));
    let running = events.projection(projection).with_options(options).spawn();
    within(10, running.caught_up(created)).await;

    let post = find(&pool, id).await.unwrap();
    let mut appender = CommandContext::new();
    post.retitle("final", &mut appender).unwrap();
    save(&events, &posts, post, &mut appender).await.unwrap();
    let retitled = sqlx::query_scalar::<_, i64>("SELECT max(position) FROM cqrs_events")
        .fetch_one(&pool)
        .await
        .unwrap() as u64;
    within(10, running.caught_up(retitled)).await;
    running.stop().await;

    assert_eq!(seen.load(Ordering::SeqCst), 2, "failed once, then the retry passed");
    let rows: Vec<(i64, String)> = sqlx::query_as("SELECT id, title FROM post_summaries")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows, [(id.0 as i64, "final".to_owned())]);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT position FROM cqrs_tokens WHERE processor = 'post.summaries'")
            .fetch_one(&pool)
            .await
            .unwrap() as u64,
        retitled,
        "the token moved with the read model"
    );
}

/// Two instances of the blog on one database: the projection runs on one of
/// them (one segment), yet the subscribers of both get every change, as
/// each instance emits once the projection handled an event, wherever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribers_on_every_instance_get_the_changes_one_of_them_projected() {
    use cqrs::query::QuerySubscriptions;
    use futures_util::StreamExt;

    let pool = database_or_skip!();
    let a = blog::start(pool.clone()).await.expect("migrated");
    let b = blog::start(pool.clone()).await.expect("migrated");
    let mut on_a = std::pin::pin!(a.app.queries().subscribe(ListPublished));
    let mut on_b = std::pin::pin!(b.app.queries().subscribe(ListPublished));
    assert_eq!(within(10, on_a.next()).await.unwrap().unwrap(), []);
    assert_eq!(within(10, on_b.next()).await.unwrap().unwrap(), []);

    let id = a
        .app
        .commands()
        .send(CreatePost { title: "shared".into() })
        .await
        .unwrap();
    b.app.commands().send(PublishPost { id }).await.unwrap();
    for (name, subscription) in [("a", &mut on_a), ("b", &mut on_b)] {
        let published = within(10, subscription.next()).await.unwrap().unwrap();
        assert_eq!(published.len(), 1, "instance {name}");
        assert_eq!(published[0].id, id);
    }
    a.stop().await;
    b.stop().await;
}
