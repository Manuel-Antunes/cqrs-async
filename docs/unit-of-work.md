# Unit of work

In Axon Framework 5, every message is handled inside a
`ProcessingContext`: the unit of work. It goes through phases
(pre-invocation, invocation, prepare commit, commit, after commit), carries
resources (the database transaction among them), and lets components hook
into its phases. An event processor handling an event and storing its
token, with the read model written in the same transaction, is the case
that matters most: it is what makes "each event applied exactly once to
the read model" hold.

This library has the same unit of work for event handling, built from
types instead of runtime registration. This page describes what happens,
in which order, and what is guaranteed.

## Event handling

### The phases

For every event, a tracking processor runs one unit of work:

```text
begin                      TransactionalTokenStore::begin() -> Tx
  interceptors (outer→inner)   EventInterceptor::intercept(event, &mut context, next)
    handle                     EventHandler::handle(event, &mut ProcessingContext<'_, Tx>)
  interceptors (inner→outer)   ...after next.proceed(context).await
store the token             TransactionalTokenStore::store_in(&mut tx, token)
commit                      TransactionalTokenStore::commit(tx)
committed                   EventHandler::committed(event)          (after commit only)
```

| Axon phase | Here |
|---|---|
| pre-invocation / invocation | `begin`, interceptors, `handle` |
| prepare commit | storing the token in the same transaction |
| commit | `commit(tx)` |
| after commit | `committed(event)` |
| on error / rollback | dropping `tx` (an `Err` anywhere before commit) |

With `batch_size` above 1 ([Event processing § Batches](event-processing.md#batches)),
the events already waiting for a segment share one unit of work:

```text
begin → [interceptors → handle] e1 … eN → store the token (past eN) → commit → committed e1 … eN
```

Each event still gets its own `ProcessingContext` (with its own replay
flag) over the same transaction. If any of them fails, the whole batch
rolls back and its events are handled one unit of work each, so the failing
one is retried, or parked, on its own.

This order is tested: `crates/cqrs/tests/unit_of_work.rs` records every
phase with an in-memory transactional store and checks it, including a
failed attempt that leaves nothing behind.

### Who owns the transaction

The token store. `TransactionalTokenStore` (Axon's transaction manager)
says what a unit of work is:

```rust
pub trait TransactionalTokenStore: TokenStore {
    type Transaction: Send;
    fn begin(&self) -> impl Future<Output = Result<Self::Transaction, StoreError>> + Send;
    fn store_in(&self, tx: &mut Self::Transaction, token: TrackingToken<'_>) -> impl Future<Output = Result<TokenUpdate, StoreError>> + Send;
    fn commit(&self, tx: Self::Transaction) -> impl Future<Output = Result<(), StoreError>> + Send;
}
```

| Token store | `Transaction` | What a rollback undoes |
|---|---|---|
| `InMemoryTokenStore` | `()` | nothing (memory has no transactions; a crash loses read model and tokens together) |
| `PostgresTokenStore` | `PgTransaction` (`sqlx::Transaction<'static, Postgres>`) | the handler's SQL, the interceptors' SQL, and the token |
| your own | anything `Send` | whatever your `commit` makes durable |

Handlers reach it through the context:

```rust
#[cqrs::projection(namespace = "views")]
impl EventHandler for AccountViews {
    async fn handle(&self, event: &EventEnvelope<AccountEvent>, context: &mut ProcessingContext<'_, PgTransaction>) -> Result<(), HandlerError> {
        sqlx::query("INSERT INTO account_views ..")
            .bind(event.stream.id())
            .execute(&mut **context.transaction())   // same transaction as the token
            .await?;
        Ok(())
    }
}
```

The read model must live where the token store's transaction can reach
it: a PostgreSQL projection keeps its tables in the token store's
database. A handler that writes elsewhere (memory, another service) still
runs in the unit of work, but its writes are not rolled back with it:
make them idempotent.

### Guarantees

- **Atomic.** The handler's writes (in `Tx`), the interceptors' writes
  and the processor's new position commit together or not at all.
- **Exactly once into the read model** for writes in `Tx`: a crash
  before the commit loses the writes and the position together, so the
  event is handled again; after it, both are there.
- **At least once for everything else**: writes outside `Tx` and side
  effects in `handle` may happen again on a retry.
- **After commit only**: `committed` runs once the commit succeeded,
  never for a rolled-back attempt. It cannot fail, and a crash between
  commit and `committed` skips it.
- **Ownership.** `store_in` stores the token only if this instance still
  holds the segment's claim. If another instance took it over
  (`TokenUpdate::ClaimLost`), the unit is rolled back and the worker
  stops, so two instances never both commit the same event.
- **Order.** Events of one aggregate are handled one at a time, in order;
  a failing event holds its segment back (or is parked).
- **Batches change nothing above.** A batch commits all its events and
  the token past the last one, or none of them.

### Failure

An `Err` from an interceptor or the handler drops the transaction (rolls
back) and the processor retries the event in a **new** unit of work, with
exponential backoff (`retry_initial`, doubling up to `retry_max`). With
dead letters configured (`with_dead_letters(queue, after)`), after `after`
attempts the event is parked, with the later events of its aggregate, and
the segment moves on.

The processor's status reports the failure (`status().failing`) while it
retries.

### The context

```rust
pub struct ProcessingContext<'a, Tx = ()> { .. }

impl<'a, Tx> ProcessingContext<'a, Tx> {
    pub fn transaction(&mut self) -> &mut Tx;   // the unit of work
    pub fn replay(&self) -> bool;               // Axon's ReplayStatus
}
```

The processor builds it; handlers and interceptors receive `&mut` to it.
Tests build one directly (`ProcessingContext::new(&mut (), false)`) to call
a handler without a processor.

### Interceptors in the unit

Event interceptors receive the same context as the handler, and call the
rest of the chain with it (Axon's `chain.proceed(message, context)`):

```rust
/// One audit row per event, in the projection's transaction.
struct AuditRows;

impl EventInterceptor<ClockEvent, PgTransaction> for AuditRows {
    async fn intercept<N: EventChain<PgTransaction>>(
        &self,
        event: &EventEnvelope<ClockEvent>,
        context: &mut ProcessingContext<'_, PgTransaction>,
        next: N,
    ) -> Result<(), HandlerError> {
        sqlx::query("INSERT INTO clock_audit (position) VALUES ($1)")
            .bind(i64::try_from(event.position).unwrap())
            .execute(&mut **context.transaction())
            .await?;
        next.proceed(context).await
    }
}
```

The PostgreSQL test `a_failing_projection_interceptor_rolls_the_write_back`
runs `(FailsOnce, AuditRows)` around a projection: the failed attempt's
audit row and total are both rolled back, and the retry writes each once.

### Replay and reset

`processor.reset()` runs the handler's `reset` in its own unit of work
(begin, `reset(&mut context)`, commit), after moving the tokens back. A
PostgreSQL projection truncates its tables there. During the replay,
`context.replay()` is `true` for events stored before the reset, and
handlers with `const REPLAY: bool = false` never see them (nor do their
interceptors).

### Dead-letter retries

`processor.retry_dead_letters()` (or
`retry_dead_letters(queue, name, &token_store, &handler)`) runs each parked
event in a unit of work like the processor's: begin, `handle`, commit,
`committed`. A failure rolls back and leaves the sequence parked. The
letter is removed after the commit: a crash in between handles it again.

### Inline processing

An `InlinePublisher` built with `with_processor(handlers)` feeds its
handlers right after each append, in the caller's task. There is no
transaction to share (`Tx = ()`): the append itself was the commit. Each
event goes through `handle`, then `committed`; a failure stops that
handler for the batch; the failure is handed to the publisher's
`on_failure` policy (logged by default). No retries.

## The command side

Axon 5 hands command handlers the same `ProcessingContext`; events
appended during the handler are published when the unit commits, at the
end. Here, a command handler's context is its `Appender<E, P>`, which
carries the publisher `P` its gateway was given:

| Axon 5 | Here |
|---|---|
| `EventAppender.forContext(context).append(e)` | `events.append(e)` (the domain sees `impl EventAppender<E>`) |
| correlation data | `events.metadata()`, stored with every event of the run |
| DCB `AppendCondition` from what the handler sourced | `events.source(..)`, `loader.load_in(..)`: conditions accumulate in the appender |
| `EventAppender` publishing through the context | `events.commit(entity)`: appends right away through the context's publisher, like NestJS's `aggregate.commit()` |
| retry on concurrency failure | the gateway re-runs the handler with a new appender, if no commit of the run succeeded |

The difference is the commit point. Each `commit` is one atomic append to one
stream, done when it is called. A handler that commits once (the usual case,
and the only one in the examples) behaves exactly like Axon's. A handler
that commits two aggregates makes two appends: if the second fails, the
first stays, and the gateway will not re-run the handler (re-running would
commit the first one twice). Keep one commit per command; a rule spanning
several aggregates uses a [dynamic consistency
boundary](patterns.md#19-aggregates-built-from-tags-dynamic-consistency-boundaries)
(one aggregate committed, the others read with `load_in` and checked by the
condition) or a policy reacting to the first aggregate's event.

## Differences from Axon, and why

| Axon 5 | Here | Why |
|---|---|---|
| Resources on the context (`putResource`, `getResource` by key) | The context carries one typed resource, `Tx`; anything else is a field of the handler or interceptor | A heterogeneous resource map needs `dyn Any`. A custom `TransactionalTokenStore` can make `Tx` any struct (a transaction plus a tenant, a clock...). |
| Lifecycle callbacks registered at runtime (`onCommit`, `runOnAfterCommit`, `onError`) | Fixed hooks: `committed` after commit; rollback on `Err`; interceptors around `handle` | Registered closures need boxing. Every component that needs a phase implements it statically. |
| Events of a command published at the end of the unit | Published at each `commit` | See [the command side](#the-command-side). A deferred, multi-stream atomic commit is the next step toward parity. |
| One `ProcessingContext` type for commands, queries and events | `Appender<E>` for commands, `ProcessingContext<Tx>` for events, none for queries | Each carries exactly what its handlers use, typed. |

## Writing a transactional token store

To keep read models in another database, implement `TokenStore` (claims
and positions) and `TransactionalTokenStore`:

- `begin` opens a transaction on the database the read models live in;
- `store_in` updates the token row in that transaction, **only if** `owner`
  still holds the claim on `segment`, and returns `ClaimLost` otherwise;
- `commit` commits it.

Dropping the transaction without committing must roll it back. Then
`Events::new(store).with_tokens(your_store)` runs every processor in your
units of work, and handlers name `ProcessingContext<'_, YourTx>` (or stay
generic).
