---
title: "Storage"
sidebar_position: 60
---

## The ports, by capability

The event store is the source of truth. It is not one trait but four, one
per capability (`cqrs::store`), so a component asks only for what it uses
and a store for another database implements what that database can do:

| Port | Methods | Used by |
|---|---|---|
| `LoadStream<E>` | `load(stream, after)`, `replay(stream, after, apply)` | rebuilding an aggregate (`AggregateLoader`) |
| `AppendEvents<E, Tx = ()>` | `append`, `append_if` (provided); `append_tagged` (implemented) | the command's unit of work, `EventPublisher`s |
| `SourceEvents<E>` | `source(criteria)` | decisions read by criteria (`CommandContext::source`), aggregates built from tags |
| `ReadLog<E>` | `read_all(after, limit)`, `last_position()`, `read(cursor, limit)`, `cursor_at(position)` | tracking processors, subscriptions |

`EventStore<E>` is all four at once, implemented for every type that has
them: what the in-memory and the PostgreSQL stores offer.

Every stored event comes back in an `EventEnvelope<E>`:

| | |
|---|---|
| `stream` | `StreamId`: aggregate type and id (`post-42`) |
| `sequence` | its place in the stream, from 1, gapless |
| `position` | its place in the global log, increasing (not necessarily gapless) |
| `event_type`, `event_version` | its name and shape version (`#[event(name, version)]`) |
| `recorded_at`, `metadata` | when, and what the command was sent with (correlation id, user...) |
| `payload` | the event |

### Loading and replaying

`load` returns the envelopes of a stream after a sequence. `replay` hands
each event (with its sequence) to a closure and returns how many there
were: what rebuilding an aggregate needs, and all it needs. By default it
goes through `load`; the PostgreSQL store reads only the sequence, type,
version and payload, and decodes each row as it arrives (no metadata, no
stream ids, no list of envelopes). With the other optimizations of the same
change, that took the median replay of a 5000-event aggregate from 4.23 to
2.47 ms ([Performance](performance.md)).

### Appending and conditions

`append(tx, stream, expected, events, metadata)` stores events atomically
per stream, checking an `ExpectedVersion`: `NoStream` (creating an
aggregate), `Exact(n)` (the version it was loaded at) or `Any`.
`append_if(.., condition)` also checks an `AppendCondition` (Axon 5's
dynamic consistency boundary): no event matching its criteria after the
position the decision read at. The check and the write are atomic.

A store only implements `append_tagged`: writing the events with their
tags, atomically with both checks, and returning what it `Recorded`
(position, sequence, time) for each. The provided methods resolve the tags,
build the envelopes, and let an empty append succeed at once, without
checking anything.

`Tx` is the transaction the append joins. `()` means none: the store
appends on its own. A database store also appends in a transaction its
caller owns (the PostgreSQL store implements `AppendEvents<E,
sqlx::Transaction<'_, Postgres>>`), which is how a command's unit of work
commits its events with the repository's writes. Then nothing is visible,
nor announced, before that transaction commits.

Errors are `StoreError::Conflict { stream, expected, actual }`,
`StoreError::ConditionFailed` (both a lost race: `is_conflict()`, decide
again on fresh state) or `StoreError::Backend(..)`, which keeps the
original error as its source.

### Reading the log

`read(cursor, limit)` returns a `LogPage`: the events the cursor has not
seen (those filling its gaps, then those after it), the `Horizon` the store
knew when it answered, and `missing`, the positions skipped that nothing
was committed at (`None`: the store cannot tell, so every skipped position
may still be filled). The defaults suit a gapless store; a store whose
appends commit out of order implements `read` and `cursor_at` to report
gaps.

## In memory

`InMemoryEventStore<E>`: every event in a `Vec` behind one lock, gapless
positions, tags indexed in memory. It is a handle: clones share the same
events, so the repository, the unit of work and the processors each hold
one. `store.all()` returns every envelope in order (tests). It appends only
on its own (`Tx = ()`): memory has nothing to roll back.

`InMemorySnapshots<A>`, `InMemoryTokenStore` and `InMemoryDeadLetters`
complete the set. Everything in memory is lost together when the process
ends, which is why the tokens and the read model stay consistent.

## PostgreSQL

`cqrs-postgres` (MSRV 1.94, the one of `sqlx` 0.9) keeps everything over a
plain `sqlx` pool. `Postgres` is the infrastructure every aggregate root of
an application shares on one database: the pool, the token store and the
transactions. Each root gets its own log:

```rust
let pg = cqrs_postgres::Postgres::new(pool);
pg.migrate().await?;                                        // or MIGRATIONS, in your tool
let posts = Cqrs::new(pg.events::<PostEvent>("posts"))      // one log per aggregate root
    .commands(..).projection(..).build();
let users = Cqrs::new(pg.events::<UserEvent>("users")).build();
```

`pg.events(log)` is a `PgEvents<E>`: by hand,
`Events::new(PostgresEventStore::new(pool, "posts")).with_tokens(PostgresTokenStore::new(pool)).with_transactions(PostgresTransactions::new(pool))`.
Events need `Serialize + DeserializeOwned`. Every process sharing the
database shares the logs, the tokens and the claims. The crate re-exports
its `sqlx` (`cqrs_postgres::sqlx`): `PgPool` and `PgTransaction` are part
of its API.

### Migrations and the schema

The schema is `crates/cqrs-postgres/migrations/0001_cqrs.sql`, idempotent.
Two ways to run it:

- **The crate runs it**: `pg.migrate()` (or `cqrs_postgres::migrate(&pool)`)
  runs it in one implicit transaction behind an advisory lock, so instances
  starting together run it one after the other.
- **Your migration tool runs it**: `cqrs_postgres::MIGRATIONS` is the list
  of `(name, statements)`, for `sqlx migrate`, `sea-orm-migration`,
  refinery... It then owns when they run. The blog runs it as one of its
  SeaORM migrations:

```rust
// src/infrastructure/postgres/migration.rs
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

The tables keep their names (every statement of the crate is checked
against them at compile time). To keep them apart from the application's,
run the migrations and the stores in a schema of their own, chosen by the
connection's `search_path`
(`PgConnectOptions::options([("search_path", "events")])`).

| Table | |
|---|---|
| `cqrs_events` | one row per event: log, position (PK), stream type and id, sequence (unique per stream in a log), event type and version, payload and metadata (`json`: stored as written, returned as is), `recorded_at` |
| `cqrs_event_tags (log, tag, position)` | the tag index criteria read through |
| `cqrs_snapshots` | newest snapshot per stream, with its revision |
| `cqrs_tokens (processor, segment)` | position, gaps and their horizons, owner, claim expiry, replay bound |
| `cqrs_dead_letters (processor, sequence, position)` | parked events: which event, and why; the event is read back from the log |
| sequence `cqrs_position` | global positions |
| functions `cqrs_positions`, `cqrs_lock` | position assignment, domain-derived locks |

### Logs

A store reads and appends one named log, the second argument of
`PostgresEventStore::new(pool, "posts")`. One event enum per aggregate root
means one log per root, so several roots share a database:

```rust
let posts = PostgresEventStore::<PostEvent>::new(pool.clone(), "posts");
let users = PostgresEventStore::<UserEvent>::new(pool.clone(), "users");
```

Each store sees only its log: `read_all`, `read`, `source`,
`last_position`, conditions and lock keys are all per log, and the same
stream may exist in two logs. Two stores given the same name share their
events, so they must have the same event type. Tokens are per processor
name, snapshots per aggregate type.

### Global order and the cursor

Positions come from **one sequence for the whole database**: the logs
share one global order, as Axon 5's single event stream does, and a reader
following several logs (a fan-in) can rely on it. A sequence per log was
considered and dropped: it loses that order.

Appends run in parallel, so a transaction that took position 10 can commit
after the one that took 11. A reader that saw 11 and remembers only "11"
would skip 10 forever. Readers therefore track a `Cursor`
(`cqrs::store::Cursor`): the highest position seen, and the positions below
it not seen yet (`Gap { position, horizon }`). Each read returns the gaps'
events if they appeared, then the events after the cursor.

A gap is given up only once nobody can fill it. Each append takes a
transaction id before its positions (`cqrs_positions`), and each read
reports its snapshot (`xmin`, `xmax`, as a `Horizon`): a gap seen in a
snapshot can only be filled by a transaction older than its `xmax`, so once
the oldest running transaction is newer, the gap is closed. Axon gives a
gap up after a timeout, which can lose a slow transaction's event; this
cannot.

With one sequence for several logs, the positions another log takes are
missing from this log's rows. They are not gaps: a page read
also returns `LogPage::missing`, the positions up to the last event read
that **no log** committed in that snapshot, and only those become gaps. The
rest are passed over at once. Without it, a processor's cursor carried a
hundred gaps or more with two logs under load. (Strategy B, gapless
positions with a finalizer as in Axon's PostgreSQL engine, would replace it
only if it measured at least as fast.)
`cursor_at(position)` does the same for a reader starting from a position
(looking back at most 10 000 positions).

Every reader of the global log uses cursors: tracking processors (stored in
`cqrs_tokens` with their gaps), subscriptions, emitters of query updates.
`cursor.covers(p)` means the event at `p` was seen.

### Locks derived from the domain

Consistency is per piece of the domain, not per database:

- every append takes **shared** advisory locks on keys derived from its
  events: each tag (from a `#[tag]` field), each event type, the stream;
- `source(criteria)` and a conditional append take the criterion's keys
  **exclusively**.

So unconditional appends never wait for each other; a conditional append
waits only for appends under way that touch what it read, and holds off new
ones until it commits, so later ones take positions above the head the
decision was read at. Two decisions on different courses run in parallel;
two on the same course take turns. Keys are taken in sorted order, so they
never deadlock.

In the caller's transaction, a conflict aborts the transaction, as any
failed statement does in PostgreSQL: the command's unit of work rolls it
back and runs the handler again. A condition's locks are held until the
transaction ends, so keep a transaction with a conditional append short.

### SQL checked at compile time

Every statement in the crate is a `sqlx::query!`, checked against the
migrations at build time: columns, parameter and result types, nullability.
The check uses the database in `CQRS_POSTGRES_URL` (set in the crate's
`sqlx.toml`, never an application's `DATABASE_URL`), or the committed
`.sqlx/` cache when it is not set, which is the default for builds. After
changing a statement, with the migrations applied:

```text
cargo sqlx prepare -- -p cqrs-async-postgres
```

## Codecs

serde is the contract: the adapters that persist ask for
`Serialize + DeserializeOwned`, and the PostgreSQL store writes JSON
through `serde_json` directly. A `Codec<T>` (`encode`, `decode`) is only
needed where an adapter stores or sends bytes in a format that is not
serde's, such as protobuf, whose binary encoding needs field numbers
serde's data model does not carry. `Forward`, which publishes events to a
broker, takes one.

| Codec | Feature | For |
|---|---|---|
| `Json` | `json` | any `Serialize + DeserializeOwned` |
| `Protobuf` | `protobuf` | any `prost::Message` |
| your own | | implement `Codec<T>` for the types it encodes |

## Upcasting

Stored events are never rewritten. When an event's shape changes:

1. bump its version: `#[event(name = "post.created", version = 2)]`;
2. register an upcaster from the old version:

```rust
use cqrs::store::upcast;
use serde_json::json;

let store = PostgresEventStore::<PostEvent>::new(pool, "posts").with_upcasters((
    upcast("post.created", 1, |payload| payload["Created"]["tags"] = json!([])),
    upcast("post.created", 2, |payload| payload["Created"]["draft"] = json!(true)),
));
```

Each `upcast(type, from, fn)` takes the JSON of one type at one version to
the next; a tuple (up to 8) chains them, oldest step first. On read, the
store asks the chain whether any step applies to the event's type and
version (`Upcaster::applies`, a comparison of a name and an integer):

- **none applies** (every event at its current version, the usual case):
  the stored text is decoded straight into the event type, with no
  intermediate document;
- **some apply**: the text is parsed into a `serde_json::Value`
  (`StoredEvent`), each matching step rewrites it, then it is decoded.

So upcasting costs nothing for current events. Upcasters are the core's
`json` feature (`StoredEvent`, `Upcaster`, `Upcast`, `upcast` in
`cqrs::store`, also re-exported by `cqrs-postgres`), shared by any store
that keeps JSON.

## Snapshots

A `SnapshotStore<A>` keeps aggregate states so a load replays only the
events after them. `AggregateLoader::with_snapshots(snapshots, every)`
saves one whenever a load had to replay `every` events or more; commits
never snapshot. The blog's event-sourced repository passes it through:

```rust
// src/config.rs
fn posts_over(store: &PostStore) -> EventSourcedPostRepository<PostStore, InMemorySnapshots<Post>> {
    EventSourcedPostRepository::new(store.clone()).with_snapshots(InMemorySnapshots::default(), SNAPSHOT_EVERY)
}
```

`PostgresSnapshotStore::<Post>::new(pool)` keeps the newest one per stream
in `cqrs_snapshots`, as JSON, at revision 1. `with_revision(n)` stamps
them: a snapshot of another revision is ignored (the aggregate is replayed,
then snapshotted again), so bump it when the aggregate's serialized shape
changes. A snapshot that fails to decode is only a full replay. Snapshots
are never the truth: deleting them only makes loads slower. `()` is the
snapshot store that keeps none.

## Writing a store

Implement the ports your database supports: `LoadStream` (`load`; `replay`
if it can read less than an envelope), `AppendEvents` (`append_tagged`,
for `()` and for your transaction type if the database has one),
`SourceEvents` (`source`) and `ReadLog` (`read_all`, `last_position`; plus
`read` and `cursor_at` if positions can commit out of order). Keep the
version check, the condition check and the write atomic. The behavior every
store must share is in the core's tests (`dcb.rs`, `stateful.rs`): run them
against yours.
