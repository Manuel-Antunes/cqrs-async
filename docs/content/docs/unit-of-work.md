---
title: "Unit of work"
sidebar_position: 40
---

A unit of work is one transaction: what a command decides, or what a
processor does with an event, commits as a whole or not at all. There are
two, one at each end of the log:

- **the command's**: the gateway opens it, the handler and its repositories
  write in it, the library appends the committed events in it and commits;
- **the processor's**: a tracking processor opens it, the event handler
  writes its read model in it, the processor stores its token in it and
  commits.

In both, the library commits, never the handler (as in Axon). The
application still owns the transaction's type: it comes from its
`TransactionManager` (`()` in memory, `PgTransaction` on PostgreSQL, or its
own).

## The command side

### What a command runs in

A command gateway (`#[cqrs::command_gateway]`) sends each command to
`cqrs::command::dispatch_command`, which runs one unit of work through the
gateway's `#[gateway(unit)]` field (a `UnitOfWork`):

```text
begin      unit.begin()                                  -> Tx  (() without a transaction)
handle     handler.handle(&command, &mut ctx)            ctx owns tx; repositories read and write in
                                                         ctx.tx(), lent by the handler; the domain appends to ctx;
                                                         ctx.commit(entity), then repository.save(&entity, ctx.tx())
append     append_writes(unit, &mut tx, writes)          what was committed, entity by entity
                                                         ((writes, tx) = ctx.into_parts())
commit     unit.commit(tx)
announce   unit.announce(events)                         processors, subscriptions, inline handlers
```

An error at any step before the commit drops the context, and `tx` with it,
which rolls back whatever the handler and its repositories wrote. The events are announced
only once committed, so nothing downstream ever sees a rolled-back event.

The handler decides and commits to its entities; it never appends the
events nor commits the transaction. `ctx.commit(entity)` assigns the events
appended since the last call to that entity and returns the entity evolved
by them, at the version they take it to: a version of the entity, not a
database commit, and nothing is written yet. A repository that keeps state
then saves that value (`save(&entity, ctx.tx())`), in the context's
transaction:

```rust
// src/application/commands/publish_post.rs
#[cqrs::command_handler]
impl<R: PostRepository, P> CommandHandler<PublishPost> for PostCommandGateway<R, P> {
    async fn handle(
        &self,
        command: &PublishPost,
        ctx: &mut CommandContext<PostEvent, R::Tx>,
    ) -> Result<(), BlogError> {
        let post = self.posts.find(&command.id, ctx.tx()).await?.ok_or(NotFound(command.id))?;
        post.publish(ctx)?;
        let post = ctx.commit(post);
        self.posts.save(&post, ctx.tx()).await?;
        Ok(())
    }
}
```

Events appended but never committed when `handle` returns `Ok` are a bug (a
forgotten `commit`): in every build, the unit refuses the command with a
store error and appends nothing.

### Two modes, chosen by the signature

`#[cqrs::command_handler]` reads the mode from the type of `handle`'s
context:

| `ctx` is | Mode (`CommandHandler::Unit`) | What commits |
|---|---|---|
| `&mut CommandContext<Event>` | `Autocommit` | the store appends the committed events on its own, in one statement. One entity per command. The fastest path, for an event-sourced repository. |
| `&mut CommandContext<Event, Tx>` | `InTransaction<Tx>` | the unit opens a transaction of its `TransactionManager`, which the context owns; the handler's writes, the repositories' writes and the events commit together. |

The blog's handlers take `CommandContext<PostEvent, R::Tx>`, so the
repository picks the transaction: the one the engine hands out
(`events.repository::<Post>()`) takes the engine's, `()` in memory and
`PgTransaction` on PostgreSQL (its events are its state, `save` writes
nothing), and `SeaOrmPostRepository` has `Tx = PgTransaction` (each post's
row is saved with its events). A third argument to `handle` does not compile: the
transaction is `ctx.tx()`.

Two units of work are provided:

| Unit (`#[gateway(unit)]`) | `Autocommit` | `InTransaction<Tx>` |
|---|---|---|
| `Events<E, S, C, M>` | the store appends, then broadcasts | `Tx = M::Tx`: a transaction of its `TransactionManager` `M`; the store appends in it (`S: AppendEvents<E, M::Tx>`) |
| `InlinePublisher<E, S, ..>` | the store appends, then the inline handlers run | `Tx = ()` only: memory has no transaction to roll back |

`Cqrs::new(events).commands(|events| PostCommandGateway::new(repo, ids, events))`
hands the gateway its unit. On PostgreSQL,
`cqrs_postgres::Postgres::new(pool).events::<PostEvent>("posts")` gives an
`Events` whose transactions are `PostgresTransactions` (`Tx = PgTransaction`).

### A lost race runs the handler again

If the append finds that the decision was made on stale state, the unit is
rolled back and the handler runs again, with a fresh context and a fresh
transaction, on fresh state. A lost race is `StoreError::ConditionFailed`
(a [DCB condition](storage.md#appending-and-conditions) failed) or a
`StoreError::Conflict` on a stream expected at an exact version. Creating
what already exists (a `New` entity whose stream is there) is not a lost
race: running again would fail the same way, so it reaches the caller.

`CommandHandler::conflict_retries` says how many times (8 by default); a
handler overrides it in its impl:

```rust
// crates/cqrs-postgres/tests/postgres.rs
/// Decided on a stale copy, it would be decided again: refuse instead.
fn conflict_retries(&self) -> u32 {
    0
}
```

Since the handler may run more than once, it must have no side effects
besides what it writes in the context's transaction. An e-mail, a call to another service, a
message to a broker: those go to a projection namespace with
`const REPLAY: bool = false`, which runs once the events are committed (see
[Event processing](event-processing.md#namespaces-each-with-its-processor)).
The command is taken by reference (`&C`) for the same reason.

**The `ROLLS_BACK` rule.** Running the handler again is only safe if
nothing it appended survived. `UnitOfWork::ROLLS_BACK` says whether dropping
the transaction undoes the appends: true for a database
(`TransactionManager::ROLLS_BACK`, default true), false in memory (`()` and
`InlinePublisher`). When a unit that cannot roll back loses a race after
committing to more than one entity (the first append may have gone through),
the handler is not run again: the conflict reaches the caller, and nothing
is appended twice. With one entity, nothing was appended when it lost, so
it is retried as usual.

### Several entities in one command

- **`Autocommit`**: committing to more than one entity is an error, returned
  before anything is appended: their appends would be separate statements,
  not atomic. Take the unit's transaction instead.
- **`InTransaction<Tx>` on a database**: the writes are appended one after
  the other in the same transaction, so they commit together. What the
  writes were conditioned on is checked as one, on the first write; each
  later write's condition skips the events the run itself just appended
  (they match what it read), and checks only what others appended.
- **`InTransaction<()>` in memory**: appended one after the other, with
  no rollback (see the `ROLLS_BACK` rule).

A rule spanning several aggregates may also be one entity built from tags
(a [dynamic consistency
boundary](patterns.md#19-aggregates-built-from-tags-dynamic-consistency-boundaries)),
or a projection namespace reacting to the first aggregate's event.

The PostgreSQL test `state_and_events_commit_and_roll_back_together`
checks the transactional mode end to end: a handler failing after it saved
its state leaves neither state nor events; one that succeeds leaves both,
and its events reach subscribers; a stale version conflicts and undoes the
state written with it.

### Interceptors and the unit

Command interceptors (`Interceptor<M>`, chained with
`Intercepted::new(gateway, (A, B))` or the builder's `.intercept(..)`) run
around the gateway's dispatch: once per command, outside its unit of work
and outside the retries. They see the command and its metadata, and the
final result.

### Running a unit by hand

`dispatch_command(&handler, command, metadata, &unit)` is what a gateway
does, and what a test calls to run a handler without one. Outside a command
(an import, a test seeding a store), `EventPublisher::commit(entity,
Pending::new(events))` appends to one entity's stream and announces, with no
handler involved.

## The processors' side

### The phases

For each event, a tracking processor runs one unit of work:

```text
begin          TransactionManager::begin() -> Tx
  interceptors   EventInterceptor::intercept(event, &mut context, next)   (outer to inner)
    handle       EventHandler::handle(event, &mut ProcessingContext<'_, Tx>)
store token    TokenStore::store(&mut tx, token)    only if this instance still holds the segment
commit         TransactionManager::commit(tx)
```

| Axon phase | Here |
|---|---|
| pre-invocation / invocation | `begin`, interceptors, `handle` |
| prepare commit | storing the token in the same transaction |
| commit | `commit(tx)` |
| after commit | the projection namespace's `updated`, on every instance ([live queries](../guide/06-live-queries.md)) |
| on error / rollback | dropping `tx` (an `Err` anywhere before the commit) |

With a batch size above 1 (`batch = N` on a namespace's handler, or
`TrackingOptions::batch_size`), the events already waiting for a segment
share one unit of work:

```text
begin → [interceptors → handle] e1 … eN → store the token (past eN) → commit → committed e1 … eN
```

Each event still gets its own `ProcessingContext` (with its own replay
flag) over the same transaction. If any of them fails, the whole batch rolls
back and its events are handled again one unit of work each, so the
failing one is retried, or parked, on its own.

`crates/cqrs/tests/unit_of_work.rs` records every phase with an in-memory
transactional store and checks this order, including a failed attempt that
leaves nothing behind and a failed batch handled again one by one.

### Who owns the transaction

The `TransactionManager` (`cqrs::processing`), and only it: `begin` hands a
transaction out, `commit` consumes it. The token store, the dead-letter
queue and the event handler borrow it (`&mut Tx`), so none of them can
commit it, keep it or choose its type.

```rust
pub trait TransactionManager: Send + Sync {
    type Tx: Send;
    const ROLLS_BACK: bool = true;
    fn begin(&self) -> impl Future<Output = Result<Self::Tx, StoreError>> + Send;
    fn commit(&self, tx: Self::Tx) -> impl Future<Output = Result<(), StoreError>> + Send;
}
```

| Transaction manager | `Tx` | What a rollback undoes |
|---|---|---|
| `()` | `()` | nothing: in memory, a crash loses read model and tokens together |
| `cqrs_postgres::PostgresTransactions` | `PgTransaction` (`sqlx::Transaction<'static, Postgres>`) | the handler's SQL, the interceptors' SQL, the token move, the dead letter |
| your own | anything `Send` | whatever your `commit` makes durable |

The processor checks at compile time that everything takes part in the same
transaction: `TokenStore<Tx = M::Tx>`, `DeadLetterQueue<E, M::Tx>` and
`EventHandler<E, M::Tx>`. A read model written in a database transaction
cannot be run with tokens kept in memory, nor dead letters kept in memory
with tokens in a database.

`Events::new(store).with_tokens(tokens).with_transactions(transactions)`
sets both; `cqrs_postgres::Postgres::events` does it for PostgreSQL.

### Writing the read model in the unit

A handler reaches the transaction through its context. The blog's
`summaries` namespace is generic over it, and its repository writes in it:

```rust
// src/application/projections/post/summaries.rs
#[cqrs::event_handler(namespace = "summaries", batch = 64)]
impl<S, N, Tx> EventHandler for PostProjection<S, N>
where
    S: PostSummaryRepository + PostSummaryStore<Tx>,
    N: Send + Sync,
    Tx: Send,
{
    async fn handle(
        &self,
        event: &EventEnvelope<PostEvent>,
        context: &mut ProcessingContext<'_, Tx>,
    ) -> Result<(), HandlerError> {
        let tx = context.tx();
        let found = match &event.payload {
            PostEvent::Created { id, title } => {
                let summary = PostSummary {
                    id: *id,
                    title: title.clone(),
                    published: false,
                };
                self.summaries.save(tx, summary).await?;
                true
            }
            // ...
        };
        // ...
        Ok(())
    }
}
```

The read model must live where the transaction reaches: a PostgreSQL
projection keeps its tables in the database of the token store. A handler
that writes elsewhere (memory, another service) still runs in the unit of
work, but its writes are not rolled back with it: make them idempotent.

### Guarantees

- **Atomic.** The handler's writes (in `Tx`), the interceptors' writes and
  the processor's new position commit together or not at all.
- **Exactly once into the read model** for writes in `Tx`: a crash before
  the commit loses the writes and the position together, so the event is
  handled again; after it, both are there.
- **At least once for everything else**: writes outside `Tx` and side
  effects in `handle` may happen again on a retry.
- **Ownership.** `TokenStore::store` moves the token only if this instance
  still holds the segment's claim. If another instance took it over, it
  returns `false`: the unit is rolled back and the worker stops, so two
  instances never both commit the same event.
- **Order.** Events of one aggregate are handled one at a time, in order; a
  failing event holds its segment back (or is parked).
- **Batches change nothing above.** A batch commits all its events and the
  token past the last one, or none of them.

### Failure, reset and dead letters

- **Failure.** An `Err` from an interceptor or the handler drops the
  transaction and the processor retries the event in a new unit of work,
  with exponential backoff. With dead letters, after the configured number
  of attempts the event is parked, in the unit of work that moves the token
  past it.
- **Reset.** `reset()` moves the tokens back, then runs the handler's
  `reset` in a unit of work of its own (a PostgreSQL projection truncates
  its tables there). During the replay, `context.replay()` is `true` for
  the events stored before the reset, and handlers with
  `const REPLAY: bool = false` never see them (nor do their interceptors).
- **Dead-letter retries.** `retry_dead_letters()` runs each parked event in
  a unit of work like the processor's: `handle`, the letter's removal,
  commit. The removal commits with what the handler wrote, so
  neither a crash nor two retries running at once apply a parked event
  twice.

See [Event processing](event-processing.md) for the settings.

### The context

```rust
impl<'a, Tx> ProcessingContext<'a, Tx> {
    pub fn new(transaction: &'a mut Tx, replay: bool) -> Self;
    pub fn in_processor(transaction: &'a mut Tx, replay: bool, processor: &'a str) -> Self;
    pub fn tx(&mut self) -> &mut Tx;   // the unit of work
    pub fn replay(&self) -> bool;               // Axon's ReplayStatus
    pub fn processor(&self) -> &str;            // "post.summaries"; "" outside a processor
}
```

The processor builds it; handlers and interceptors receive `&mut` to it.
Tests build one directly (`ProcessingContext::new(&mut (), false)`) to call
a handler without a processor.

### Interceptors in the unit

Event interceptors (`EventInterceptor<E, Tx>`) receive the handler's
context and call the rest of the chain with it, so what they write in
`context.tx()` commits or rolls back with the handler's changes:

```rust
impl EventInterceptor<ClockEvent, PgTransaction> for AuditRows {
    async fn intercept<N: EventChain<PgTransaction>>(
        &self,
        event: &EventEnvelope<ClockEvent>,
        context: &mut ProcessingContext<'_, PgTransaction>,
        next: N,
    ) -> Result<(), HandlerError> {
        sqlx::query("INSERT INTO clock_audit (position) VALUES ($1)")
            .bind(i64::try_from(event.position).unwrap())
            .execute(&mut **context.tx())
            .await?;
        next.proceed(context).await
    }
}
```

The PostgreSQL test `a_failing_projection_interceptor_rolls_the_write_back`
runs a failing interceptor and `AuditRows` around a projection: the failed
attempt's audit row and total are both rolled back, and the retry writes
each once.

### Inline processing

An `InlinePublisher` runs its handlers in the caller's task, after the
command's unit committed. There is no transaction to share (`Tx = ()`):
the append was the commit. Each event goes through `handle`; a failure
stops that handler for the rest of the commit and
goes to the publisher's `OnFailure` policy. No retries, no tokens.

## SeaORM in the same transaction

The transaction both units hand out on PostgreSQL is sqlx's
(`PgTransaction`), the one the library's stores append and move tokens in.
`cqrs_sea_orm::SeaOrm` is a borrowed view of it that SeaORM's query
builders run on, so a repository or a read model written with SeaORM
commits with the events or with the token:

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
    // ...
}
```

`SeaOrm::new(tx)` borrows the transaction: it can neither commit it nor
outlive it. The other way round does not work: SeaORM's
`DatabaseTransaction` keeps its connection private and does not lend it, and
the library's statements are sqlx's, checked at compile time.

## Differences from Axon, and why

| Axon 5 | Here | Why |
|---|---|---|
| Resources on the context (`putResource`, `getResource` by key) | The context carries one typed resource, `Tx`; anything else is a field of the handler or interceptor | A heterogeneous resource map needs `dyn Any`. A custom `TransactionManager` can make `Tx` any struct (a transaction plus a tenant, a clock...). |
| Lifecycle callbacks registered at runtime (`onCommit`, `runOnAfterCommit`, `onError`) | Fixed hooks: rollback on `Err`; interceptors around `handle`; after the commit, a projection namespace's `updated`, on every instance | Registered closures need boxing. Every component that needs a phase implements it statically. |
| One `ProcessingContext` type for commands, queries and events | `CommandContext<E, Tx>` for commands, `ProcessingContext<Tx>` for events, none for queries | Each carries exactly what its handlers use, typed. |
| The command's transaction from the context | `ctx.tx()` on a `CommandContext<E, Tx>` | The mode (`Autocommit` or `InTransaction<Tx>`) is part of the context's type, so a unit that cannot provide that transaction does not compile. |

## Writing your own transaction manager

To keep read models (or a command's state) in another database, implement
`TransactionManager` for something that opens its transactions:

- `begin` opens a transaction on the database the read models live in;
- `commit` commits it; dropping it without a commit must roll it back;
- `ROLLS_BACK` stays `true` unless dropping it undoes nothing.

Then a `TokenStore` with the same `Tx`, whose `store` updates the token in
that transaction **only if** the owner still holds the segment's claim
(returning `false` otherwise), and, for dead letters, a
`DeadLetterQueue<E, Tx>`. `Events::new(store).with_tokens(your_tokens)
.with_transactions(your_transactions)` runs every processor in your units of
work; handlers name `ProcessingContext<'_, YourTx>` (or stay generic).
