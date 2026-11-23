# Storage

## The event store contract

`EventStore<E>` is the source of truth. Every event has:

| | |
|---|---|
| `stream` | `StreamId`: aggregate type and id (`post-42`) |
| `sequence` | its place in the stream, from 1, gapless |
| `position` | its place in the global log, increasing (not necessarily gapless) |
| `recorded_at`, `metadata` | when, and the command's correlation data |
| `payload` | the event |

| Method | |
|---|---|
| `load(stream, after)` | the stream's events with `sequence > after` |
| `append(stream, expected, events, metadata)` | appends, checking `ExpectedVersion` (`Any`, `NoStream`, `Exact(n)`) |
| `append_if(.., condition)` | also checks an `AppendCondition`, atomically (provided: resolves tags, calls `append_tagged`, builds envelopes) |
| `append_tagged(..)` | **what a store implements**: write the tagged events, return what it `Recorded` |
| `source(criteria)` | every event matching the criteria, from any stream, and the head they were read at |
| `read_all(after, limit)` | the global log, page by page |
| `read(cursor, limit)` / `cursor_at(position)` | the global log through a gap-aware cursor (defaults suit a gapless store) |
| `last_position()` | the head |

Errors are `StoreError::Conflict { stream, expected, actual }`,
`StoreError::ConditionFailed` (both: decide again on fresh state) or
`StoreError::Backend(message)`.

An empty append succeeds at once, in every store, without checking
anything.

## In memory

`InMemoryEventStore<E>`: a `Vec` behind a lock, gapless positions, tags
indexed in memory. Share it with `Arc` between the event-sourcing engine and
processors. `store.all()` returns every envelope (tests).

`InMemorySnapshots<A>`, `InMemoryTokenStore`, `InMemoryDeadLetters` complete
the in-memory set.

## PostgreSQL

```rust
let pool = PgPool::connect(&url).await?;
cqrs_postgres::migrate(&pool).await?;                  // idempotent, safe for instances starting together

let store = PostgresEventStore::<PostEvent>::new(pool.clone());
let tokens = PostgresTokenStore::new(pool.clone());
let snapshots = PostgresSnapshotStore::<Post>::new(pool.clone()).with_revision(1);
let events = Events::new(store).with_tokens(tokens);
```

Events need `Serialize + DeserializeOwned` (payloads are `jsonb`). Every
process sharing the database shares the log, the tokens and the claims.

### Tables

| Table | |
|---|---|
| `cqrs_events` | one row per event: position (PK), stream type/id, sequence (unique per stream), event type and version, payload and metadata (`jsonb`), `recorded_at` |
| `cqrs_event_tags (tag, position)` | the tag index criteria read through |
| `cqrs_snapshots` | newest snapshot per stream, with its revision |
| `cqrs_tokens (processor, segment)` | position, gaps, owner, claim expiry, replay bound |
| sequence `cqrs_position` | global positions |
| functions `cqrs_positions`, `cqrs_lock` | position assignment, domain-derived locks |

The migrations are in `crates/cqrs-postgres/migrations/` and also work
with `sqlx migrate run --source`.

### Global order and the cursor

Appends run in parallel (Axon's JDBC design): positions come from the
`cqrs_position` sequence, so a transaction that took position 10 can commit
after the one that took 11. A reader that saw 11 and remembers only "11"
would skip 10 forever.

Readers therefore track a `Cursor`:

```rust
pub struct Cursor { position: u64, gaps: Vec<Gap> }   // Gap { position, horizon }
```

- `position`: the highest position seen;
- `gaps`: positions below it not seen yet, each with the database
  horizon (snapshot `xmin`/`xmax`) of the read that revealed it.

Each read returns the gaps' events if they appeared, and the events after
`position`. A gap is given up only when the oldest running transaction is
newer than its horizon: no transaction that could still commit it exists.
Axon gives a gap up after a timeout, which can lose a slow transaction's
event; this cannot.

Every reader of the global log uses cursors: tracking processors (stored in
`cqrs_tokens` with their gaps), subscriptions, live queries.
`cursor.covers(p)` means every event up to `p` was seen.

### Locks derived from the domain

Consistency is per piece of the domain, not per database:

- every append takes **shared** advisory locks on keys derived from its
  events: each tag (`tag:course=42`), each event type, the stream;
- `source(criteria)` and a conditional append take the criterion's keys
  **exclusively** (one tag per criterion, or its types).

So unconditional appends never wait for each other; a conditional append
waits only for appends under way that touch what it read, and holds off
new ones until it committed. Two decisions on different courses run in
parallel; two on the same course take turns. Keys are taken in sorted
order, so they never deadlock.

### SQL checked at compile time

Every statement in the crate is `sqlx::query!`, checked against the
migrations at build time: columns, parameter and result types,
nullability. The check uses the database in `CQRS_POSTGRES_URL` (set in the
crate's `sqlx.toml`, never an application's `DATABASE_URL`), or the
committed `.sqlx/` cache when it is not set, which is the default for
builds. After changing a statement, with the migrations applied:

```text
cargo sqlx prepare -- -p cqrs-postgres
```

## Snapshots

```rust
let posts = events.event_sourced::<Post>().with_snapshots(PostgresSnapshotStore::new(pool.clone()), 100);
```

A snapshot is saved every `every` events (at the save that crosses the
threshold); `load` starts from the newest one and replays the events
after it. The snapshot store keeps one per stream.
`PostgresSnapshotStore::with_revision(n)` stamps them: a snapshot of
another revision is ignored, so bump it when the aggregate's serialized
shape changes. Snapshots are never the truth: deleting them only makes
loads slower.

## Upcasting

Stored events are never rewritten. When an event's shape changes:

1. bump its version: `#[event(name = "post.created", version = 2)]`;
2. register an upcaster from the old version:

```rust
use cqrs_postgres::upcast;
use serde_json::json;

let store = PostgresEventStore::<PostEvent>::new(pool).with_upcasters((
    upcast("post.created", 1, |payload| payload["Created"]["tags"] = json!([])),
    upcast("post.created", 2, |payload| payload["Created"]["draft"] = json!(true)),
));
```

Each upcaster takes the JSON of one `(type, version)` to the next version;
a tuple chains them in order. On read, a payload runs through every
upcaster that matches its type and version, then is decoded. Upcasters
are the `json` feature of the core (`StoredEvent`, `Upcaster`, `upcast`),
shared by any store that keeps JSON.

## Writing a store

Implement `EventStore<E>`: `load`, `append_tagged`, `source`, `read_all`,
`last_position`. The provided methods handle empty batches, tag
resolution, envelopes and conditions' plumbing. A store that can commit
positions out of order also implements `read` and `cursor_at` to report
gaps. Keep the version check, the condition check and the write atomic.
The behavior every store must share is in the core's tests (`dcb.rs`,
`stateful.rs`): run them against yours.
