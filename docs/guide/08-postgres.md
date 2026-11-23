# 8. PostgreSQL

The application so far keeps everything in memory. Moving it to
PostgreSQL changes the composition root and adds adapters; the domain, the
handlers, the projection and the queries stay as they are.

> The code of this part was checked against PostgreSQL 17 when it was
> written; it is not in `examples/blog`.

## Setup

```toml
[dependencies]
cqrs-postgres = { path = "../../crates/cqrs-postgres" }
sqlx = { version = "0.9", default-features = false, features = ["runtime-tokio", "postgres"] }
```

Events must be serializable (payloads are stored as `json`): `PostEvent` derives
serde since part 1.

## Two stores, one pool

```rust
let pool = PgPool::connect(&url).await?;
cqrs_postgres::migrate(&pool).await?;   // the library's tables; idempotent

let store = PostgresEventStore::<PostEvent>::new(pool.clone(), "posts");
let posts = Arc::new(EventSourcedPostRepository::new(store.clone()));   // the adapter from part 2, unchanged
Cqrs::new(store, PostgresTokenStore::new(pool.clone()))
    .commands(PostCommandGateway::new(posts))
    // ...
```

- `PostgresEventStore`: a named log (`"posts"`), appends in parallel, a
  gap-aware cursor for readers ([Storage](../storage.md#postgresql)). It is
  a handle on the pool: clones share it. Another aggregate's event enum gets
  its own store and log name on the same database
  ([Storage § Logs](../storage.md#logs)).
- `PostgresTokenStore`: processors' positions and claims, shared by every
  instance. Its unit of work is a `PgTransaction`.

`migrate` runs at every start, behind a lock: instances starting together
take turns.

## The read model, two ways

A read model in PostgreSQL can be written in two ways, and the difference
is what "each event applied once" means.

### At least once, with idempotent writes

Implement the application's port with SQL. The projection does not change
(it calls the port), and since its context names no transaction it runs
with `PostgresTokenStore` as is. Its writes go through the pool, outside
the processor's transaction: after a crash between the write and the
token, the event is applied again. That is harmless when every write is
idempotent, as these are: an upsert, and updates that set values.

```rust
// src/infrastructure/postgres_post_summary_repository.rs
pub const READ_MODEL: &str = "
    CREATE TABLE IF NOT EXISTS post_summaries (
        id        bigint  PRIMARY KEY,
        title     text    NOT NULL,
        published boolean NOT NULL
    );
    CREATE TABLE IF NOT EXISTS post_stats (
        post     text   PRIMARY KEY,
        retitles bigint NOT NULL
    );
";

pub struct PostgresPostSummaryRepository {
    pool: PgPool,
}

impl PostgresPostSummaryRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn backend(error: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(error.to_string())
}

fn key(id: PostId) -> Result<i64, StoreError> {
    i64::try_from(id.0).map_err(backend)
}

type Row = (i64, String, bool);

fn summary((id, title, published): Row) -> Result<PostSummary, StoreError> {
    Ok(PostSummary { id: PostId(u64::try_from(id).map_err(backend)?), title, published })
}

impl PostSummaryRepository for PostgresPostSummaryRepository {
    async fn find(&self, id: PostId) -> Result<Option<PostSummary>, StoreError> {
        let row: Option<Row> = sqlx::query_as("SELECT id, title, published FROM post_summaries WHERE id = $1")
            .bind(key(id)?)
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?;
        row.map(summary).transpose()
    }

    async fn published(&self) -> Result<Vec<PostSummary>, StoreError> {
        let rows: Vec<Row> = sqlx::query_as("SELECT id, title, published FROM post_summaries WHERE published ORDER BY id")
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?;
        rows.into_iter().map(summary).collect()
    }

    async fn save(&self, summary: PostSummary) -> Result<(), StoreError> {
        // An upsert: saving the same summary twice leaves one row.
        sqlx::query(
            "INSERT INTO post_summaries (id, title, published) VALUES ($1, $2, $3) \
             ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, published = EXCLUDED.published",
        )
        .bind(key(summary.id)?)
        .bind(&summary.title)
        .bind(summary.published)
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn update(&self, id: PostId, change: impl FnOnce(&mut PostSummary) + Send) -> Result<bool, StoreError> {
        let mut tx = self.pool.begin().await.map_err(backend)?;
        let row: Option<Row> = sqlx::query_as("SELECT id, title, published FROM post_summaries WHERE id = $1 FOR UPDATE")
            .bind(key(id)?)
            .fetch_optional(&mut *tx)
            .await
            .map_err(backend)?;
        let Some(row) = row else { return Ok(false) };
        let mut summary = summary(row)?;
        change(&mut summary);
        sqlx::query("UPDATE post_summaries SET title = $2, published = $3 WHERE id = $1")
            .bind(key(id)?)
            .bind(&summary.title)
            .bind(summary.published)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(true)
    }

    async fn clear(&self) -> Result<(), StoreError> {
        sqlx::query("TRUNCATE post_summaries").execute(&self.pool).await.map_err(backend)?;
        Ok(())
    }
}
```

### Exactly once, in the processor's transaction

A counter is not idempotent: applying "retitled" twice counts two. Such a
read model writes in the unit of work, so its rows commit with the
processor's token. It names `PgTransaction` in its context, and uses SQL
directly:

```rust
// src/application/projections/post_stats.rs
use cqrs_postgres::PgTransaction;

/// How many times each post was retitled.
pub struct PostStatsProjection;

#[cqrs::projection(namespace = "post-stats")]
impl EventHandler for PostStatsProjection {
    async fn handle(&self, event: &EventEnvelope<PostEvent>, context: &mut ProcessingContext<'_, PgTransaction>) -> Result<(), HandlerError> {
        if let PostEvent::Retitled { .. } = event.payload {
            sqlx::query(
                "INSERT INTO post_stats (post, retitles) VALUES ($1, 1) \
                 ON CONFLICT (post) DO UPDATE SET retitles = post_stats.retitles + 1",
            )
            .bind(event.stream.id())
            .execute(&mut **context.transaction())   // the processor's transaction
            .await?;
        }
        Ok(())
    }

    async fn reset(&self, context: &mut ProcessingContext<'_, PgTransaction>) -> Result<(), HandlerError> {
        sqlx::query("TRUNCATE post_stats").execute(&mut **context.transaction()).await?;
        Ok(())
    }
}
```

For each event: begin, `handle` (the upsert), store the token, commit. A
crash anywhere before the commit loses the increment and the token
together; the event is handled again and counted once. A failure (`Err`)
rolls back and retries. `sqlx` errors convert into `HandlerError` with `?`.

This projection only runs with a token store whose transaction is a
`PgTransaction`: wiring it to the in-memory `Events` is a compile error.

| | At least once (port + idempotent SQL) | Exactly once (SQL in the context) |
|---|---|---|
| The projection | unchanged, generic, testable in memory | names `PgTransaction` |
| Writes | through the port, on the pool | in `context.transaction()` |
| Needs | idempotent writes | the read model in the token store's database |
| Fits | upserts, "set" updates | counters, sums, appends |

## The composition root

Against the in-memory `start` (part 7), the two stores handed to the unit
change, and the PostgreSQL read-model adapter replaces the in-memory one.
Everything else is inferred:

```rust
pub type PgPostEvents = Events<PostEvent, PostgresEventStore<PostEvent>, PostgresTokenStore>;

pub async fn start_postgres(
    pool: PgPool,
) -> Result<Cqrs<PgPostEvents, impl PostCommands, impl PostQueries + PostSubscriptions, PgPostEvents>, StoreError> {
    cqrs_postgres::migrate(&pool).await?;
    sqlx::raw_sql(READ_MODEL).execute(&pool).await.map_err(backend)?;

    let store = PostgresEventStore::<PostEvent>::new(pool.clone(), "posts");
    let posts = Arc::new(EventSourcedPostRepository::new(store.clone()));
    let summaries = Arc::new(PostgresPostSummaryRepository::new(pool.clone()));
    let updates = Arc::new(PostUpdates::default());

    Ok(Cqrs::new(store, PostgresTokenStore::new(pool))
        .commands(PostCommandGateway::new(posts))
        .queries(PostQueryGateway::new(Arc::clone(&summaries)))
        .options(TrackingOptions { segments: 4, batch_size: 100, ..TrackingOptions::default() })
        .projection(PostSummaryProjection::new(summaries, Arc::clone(&updates)))
        .projection(PostStatsProjection)
        .subscriptions(updates)
        .build())
}
```

`PostStatsProjection` names `PgTransaction`: the builder accepts it
because the token store the unit was built with is PostgreSQL's. With an
in-memory token store, the same line would not compile.

And it runs:

```rust
let blog = start_postgres(pool.clone()).await?;
let id = blog.commands().send(CreatePost { title: "On Postgres".into() }).await?;
blog.commands().send(RetitlePost { id, title: "On PostgreSQL".into() }).await?;
blog.commands().send(PublishPost { id }).await?;

blog.settled().await;
let found = blog.queries().query(FindPost { id }).await?.unwrap();
assert_eq!((found.title.as_str(), found.published), ("On PostgreSQL", true));   // post_stats: 1 retitle
blog.stop().await;
```

Add the notification policy and the interceptors exactly as in parts 5
and 7: they are generic over the unit of work.

## Interceptors in the transaction

An event interceptor can write in the same transaction, e.g. an audit row
per event that commits or rolls back with the projection:

```rust
struct AuditRows;

impl EventInterceptor<PostEvent, PgTransaction> for AuditRows {
    async fn intercept<N: EventChain<PgTransaction>>(
        &self,
        event: &EventEnvelope<PostEvent>,
        context: &mut ProcessingContext<'_, PgTransaction>,
        next: N,
    ) -> Result<(), HandlerError> {
        sqlx::query("INSERT INTO post_audit (position) VALUES ($1)")
            .bind(i64::try_from(event.position)?)
            .execute(&mut **context.transaction())
            .await?;
        next.proceed(context).await
    }
}

    // in the builder:
    .projection(Intercepted::new(PostStatsProjection, AuditRows))
```

## Operating it

**Several instances.** Run the same binary several times against the same
database. Processors with the same namespace share their segments through
`cqrs_tokens`: each instance claims some (`max_segments` caps it), renews
its claims, and takes over a dead instance's segments once its claims
expire (`claim_timeout`, 5 s by default). The segment count is fixed by
the first instance that starts the processor.

**Live queries across instances.** Subscription query updates are emitted
by the instance that projected the event. With several instances, serve
subscriptions with `LiveQueries` and `blog.events().global_progress("post-summaries")`
([Event processing](../event-processing.md#live-results-for-queries)).

**Rebuilding a read model.** Stop the processor on every instance, then:

```rust
blog.stop().await;
blog.events().projection(PostStatsProjection).reset().await?;   // tokens back to 0, TRUNCATE in one transaction
let blog = start_postgres(pool).await?;                  // the processor replays
```

A rebuild reads the log in pages, through the shared reader, into every
segment in parallel.

**A new read model** is a new namespace: its processor starts from the
beginning of the log on its first start, while the others keep their
positions.

**Failing events.** `processor.status().failing` reports the error of an
event being retried. With `with_dead_letters(queue, n)` it is parked after
`n` attempts, and `retry_dead_letters()` replays the parked events once
fixed ([Event processing § Dead letters](../event-processing.md#dead-letters)).

**Changing an event's shape.** Bump its version and register an upcaster:
`PostgresEventStore::new(pool, "posts").with_upcasters(upcast("post.created", 1, |payload| ..))`
([Storage § Upcasting](../storage.md#upcasting)).

Next: [9. The API](09-api.md).
