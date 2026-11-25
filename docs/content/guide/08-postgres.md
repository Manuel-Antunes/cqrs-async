---
title: "8. PostgreSQL"
sidebar_position: 18
---

So far the blog keeps everything in memory. On PostgreSQL, the library's
stores keep the events and the processors' tokens, and the blog's own
infrastructure keeps the rest with SeaORM: each post's state in a table,
and the read model. Both write in the library's transactions, so a post's
state commits with its events, and a summary with the processor's token.

What changes is the composition root and two adapters. The domain, the
handlers, the projection and the queries stay as they are: they are written
against the ports (`PostRepository`, `PostSummaryRepository`,
`PostSummaryStore<Tx>`), and the transaction type is a parameter.

## Setup

```toml
# examples/blog/Cargo.toml
[dependencies]
cqrs = { workspace = true, features = ["derive", "tokio"] }
cqrs-postgres.workspace = true
cqrs-sea-orm.workspace = true   # SeaORM in the library's transactions
sea-orm = { version = "2.0.4", default-features = false, features = ["macros", "sqlx-postgres", "runtime-tokio"] }
sea-orm-migration = { version = "2.0.4", default-features = false, features = ["sqlx-postgres", "runtime-tokio"] }
```

`cqrs-postgres` and `cqrs-sea-orm` need Rust 1.94 (sqlx 0.9 and SeaORM 2);
`cqrs` itself stays at 1.89. `cqrs-postgres` re-exports the `sqlx` it is
built on (`cqrs_postgres::sqlx::PgPool`): use that one to name the pool and
`PgTransaction`. Events are stored as JSON, so `PostEvent` derives serde
since part 1.

## One database, one log per aggregate root

```rust
let pg = cqrs_postgres::Postgres::new(pool);
let events = pg.events::<PostEvent>("posts");   // what Cqrs::new takes
```

`Postgres` holds what every aggregate root of the application shares on one
database: the pool, the token store (`PostgresTokenStore`) and the
transactions (`PostgresTransactions`). `events(log)` gives one root its
events service (`cqrs_postgres::PgEvents<PostEvent>`): a store on the log
`"posts"`, tokens and units of work in the database. Another root gets
another log on the same `Postgres`. All logs share one sequence of global
positions, so a processor can still read them in one order
([Storage § Logs](../docs/storage.md#logs)).

`Postgres` is a shortcut. By hand, the same is
`Events::new(PostgresEventStore::new(pool, "posts")).with_tokens(PostgresTokenStore::new(pool)).with_transactions(PostgresTransactions::new(pool))`.

## Migrations: the library's tables are one of yours

The library ships its schema as SQL (`cqrs_postgres::MIGRATIONS`), and does
not decide when it runs. The blog adds it as the first migration of its own
SeaORM migrator, next to its tables:

```rust
// src/infrastructure/postgres/migration.rs
pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(EventTables), Box::new(BlogTables)]
    }
}

/// The library's tables: `cqrs_postgres::MIGRATIONS`, as they are.
struct EventTables;

#[async_trait::async_trait]
impl MigrationTrait for EventTables {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (_, statements) in cqrs_postgres::MIGRATIONS {
            manager.get_connection().execute_unprepared(statements).await?;
        }
        Ok(())
    }
}
```

`BlogTables` creates `posts` and `post_summaries` from their SeaORM
entities, and the `post_ids` sequence. An application without a migration
tool calls `pg.migrate().await?` instead: idempotent, behind a lock, so
instances starting together take turns.

## The post's state, in the command's transaction

`PostRepository` has `type Tx`: the transaction it works in. The
event-sourced adapter from part 2 has `Tx = ()`. The SeaORM adapter has
`Tx = PgTransaction`, the command's own:

```rust
// src/infrastructure/postgres/post_repository.rs
impl PostRepository for SeaOrmPostRepository {
    type Tx = PgTransaction;

    async fn find(&self, id: PostId, tx: &mut PgTransaction) -> Result<Option<Loaded<Post>>, StoreError> {
        let key = i64::try_from(id.0).map_err(StoreError::backend)?;
        let row = post::Entity::find_by_id(key)
            .one(&SeaOrm::new(tx))
            .await
            .map_err(backend)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let version = u64::try_from(row.version).map_err(StoreError::backend)?;
        Ok(Some(Loaded::at(Post::restore(id, row.title, row.published), version)))
    }

    async fn save(&self, post: &Loaded<Post>, tx: &mut PgTransaction) -> Result<(), StoreError> {
        let row = post::ActiveModel {
            id: Set(i64::try_from(post.id().0).map_err(StoreError::backend)?),
            title: Set(post.title().to_owned()),
            published: Set(post.is_published()),
            version: Set(post.version().map_or(0, |version| version as i64)),
        };
        let upsert = OnConflict::column(post::Column::Id)
            .update_columns([post::Column::Title, post::Column::Published, post::Column::Version])
            .to_owned();
        post::Entity::insert(row)
            .on_conflict(upsert)
            .exec(&SeaOrm::new(tx))
            .await
            .map_err(backend)?;
        Ok(())
    }

    // next_id: SELECT nextval('post_ids'), in the same transaction
}
```

`SeaOrm::new(tx)` (from `cqrs-sea-orm`) is SeaORM over a borrowed sqlx
transaction: every query builder runs on it. It cannot commit the
transaction or outlive it. The opposite is not possible: SeaORM's own
`DatabaseTransaction` does not lend its connection, and the library's
statements are sqlx's, checked at compile time.

The handlers did not change. They take `tx: &mut R::Tx`, so their unit of
work is `InTransaction<PgTransaction>`: the unit begins the transaction,
the handler finds, decides, `ctx.record(post)` and saves, then the unit
appends the recorded events in the same transaction and commits once.
State and events commit together, or not at all.

Finding a post now reads one row, whatever its history. The row keeps the
version of the post's stream, and the unit appends expecting that version,
so two concurrent changes still conflict. The loser's transaction rolls
back (its row and its events), and the handler runs again on the new state.

## The read model, in the projection's transaction

The projection writes through `PostSummaryStore<Tx>`, with the `Tx` the
processor hands it in `context.transaction()`. In memory `Tx` is `()`. On
PostgreSQL it is the processor's `PgTransaction`, the one that moves its
token:

```rust
// src/infrastructure/postgres/post_summaries.rs
impl PostSummaryStore<PgTransaction> for SeaOrmPostSummaries {
    async fn save(&self, tx: &mut PgTransaction, summary: PostSummary) -> Result<(), StoreError> {
        let row = post_summary::ActiveModel {
            id: Set(key(summary.id)?),
            title: Set(summary.title),
            published: Set(summary.published),
        };
        post_summary::Entity::insert(row)
            .exec(&SeaOrm::new(tx))
            .await
            .map_err(backend)?;
        Ok(())
    }

    // update: find the row, apply the change, update it; clear: delete every row
}
```

For each batch the processor begins a transaction, the `summaries`
namespace writes its rows, the token is stored, and everything commits.
A crash before the commit loses the rows and the token together: the
events are handled again and applied once. So `save` can be a plain
insert. If a handling fails after writing, the rollback removes the row,
and the retry does not hit the primary key. `tests/postgres.rs` checks
exactly that.

The queries read the same table from the pool, through
`PostSummaryRepository`, as before.

## The composition root

```rust
// src/config.rs
pub type PgPostEvents = cqrs_postgres::PgEvents<PostEvent>;

pub async fn start(
    pool: PgPool,
) -> Result<Running<impl PostCommands, impl PostQueries + PostSubscriptions, SeaOrmPostSummaries, PgPostEvents>, DbErr>
{
    Migrator::up(&SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone()), None).await?;
    let events = cqrs_postgres::Postgres::new(pool.clone()).events::<PostEvent>("posts");
    let summaries = Arc::new(SeaOrmPostSummaries::new(pool.clone()));
    let notifier = Arc::new(InMemoryNotifier::default());
    let audit = Arc::new(AuditLog::default());
    let metrics = Arc::new(ProjectionMetrics::default());

    let projection = PostProjection::new(Arc::clone(&summaries), Arc::clone(&notifier));
    let app = Cqrs::new(events)
        .commands(|events| PostCommandGateway::new(SeaOrmPostRepository, events))
        .intercept(Audit(Arc::clone(&audit)))
        .queries(PostQueryGateway::new(Arc::clone(&summaries)))
        .projection(Intercepted::new(projection, Measured(Arc::clone(&metrics))))
        .build();

    Ok(Running { app: Arc::new(app), summaries, notifier, audit, metrics })
}
```

Against `start_in_memory` (part 7), only the migrations, the events
service and the two adapters differ. The projection is the same
`PostProjection`: its `summaries` handler is generic over `Tx`, so it
accepts the processor's `PgTransaction` because `SeaOrmPostSummaries`
implements `PostSummaryStore<PgTransaction>`. Wiring it with an adapter
that only takes `()` would not compile. The `notifications` namespace names
no transaction, and runs in any unit of work.

## Running it

The PostgreSQL tests are skipped unless `CQRS_POSTGRES_URL` points at a
database; each test makes a schema of its own:

```text
docker run -d --name cqrs-pg -e POSTGRES_PASSWORD=cqrs -e POSTGRES_USER=cqrs \
    -e POSTGRES_DB=cqrs -p 55432:5432 postgres:17-alpine
CQRS_POSTGRES_URL=postgres://cqrs:cqrs@127.0.0.1:55432/cqrs cargo test -p blog --test postgres
```

`examples/blog/tests/postgres.rs` runs the whole blog on the database and
restarts it (ids and tokens carry on), checks that a lost race undoes a
post's row and its events, that the read model applies each event once even
when the handling fails after writing, and that subscribers on two
instances both get a change one of them projected. The library's own tests
run the same way with `cargo test -p cqrs-async-postgres`.

The server takes its database from `BLOG_DATABASE_URL`:

```text
BLOG_DATABASE_URL=postgres://cqrs:cqrs@127.0.0.1:55432/cqrs cargo run -p blog
```

## Operating it

**Several instances.** Run the same binary several times against the same
database. Each namespace's processor (`post.summaries`,
`post.notifications`) shares its segments through `cqrs_tokens`: each
instance claims some, renews its claims, and takes over a dead instance's
segments once its claims expire (`claim_timeout`, 5 s by default). Set them
with `.options(TrackingOptions::default().segments(4))` on the builder; the
first instance to start a processor fixes its segment count
([Event processing § Segments](../docs/event-processing.md#segments-claims-and-instances)).

**Commits across instances.** An instance's live feed carries only its own
commits; without help, the others' reach its processors and subscriptions at
their next poll (1 s by default). `start(pool)` relays them:

```rust
// src/config.rs
let pg = cqrs_postgres::Postgres::new(pool.clone());
let events = pg.events::<PostEvent>("posts");
let listening = pg.listen(&events).await.map_err(|error| DbErr::Custom(error.to_string()))?;
```

After each commit, an instance tells the others (`NOTIFY` on the channel
`cqrs_events`, after the commit and from a connection of its own, so the
appends never wait on it); each instance `LISTEN`s and wakes its readers,
which then read the store at once. Keep the returned `Listening`: dropping it
stops the relay. A lost notification only costs the poll's latency; the
store stays the source of truth.

**Subscriptions across instances.** Every instance runs an emitter per
namespace with an `updated`: it follows the committed events and emits once
the namespace handled each one, whichever instance did. A client subscribed
to any instance gets every change, as soon as the relay above wakes it.

**Rebuilding the read model.** Stop every instance, then reset the
projection: each namespace's tokens go back to the start, then its
`reset` runs in a transaction (for `summaries`, `clear`). On the next
start, `summaries` replays the whole log; `notifications`
(`REPLAY = false`) skips what it already handled. A reset refuses to run
while an instance still holds a segment.

```rust
pg.events::<PostEvent>("posts")
    .projection(PostProjection::new(summaries, notifier))
    .reset()
    .await?;
```

**Failing events.** A failing event rolls its unit of work back and is
retried with growing waits (`retry_initial`, `retry_max`), holding back the
later events of its segment; the processor's `status().failing` reports
the error. A projection can park such events instead, in
`PostgresDeadLetters`: `.projection_with(projection, |processors|
processors.with_dead_letters(PostgresDeadLetters::new(pg.store("posts")), n))` on the
builder, and the same for a processor built by hand
([Event processing § Dead letters](../docs/event-processing.md#dead-letters)).

**Changing an event's shape.** Bump its version and give the store an
upcaster: `pg.store::<PostEvent>("posts").with_upcasters(upcast("post.created", 1, |payload| ..))`,
then build the `Events` by hand as above
([Storage § Upcasting](../docs/storage.md#upcasting)).

Next: [9. The API](09-api.md).
