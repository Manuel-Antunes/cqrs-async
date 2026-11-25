# Changelog

All notable changes are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0-alpha.1] - Unreleased

The first release, not on crates.io yet. Everything is new; this lists what
the crates offer. The design decisions behind them are in
[specs/migration-plan.md](specs/migration-plan.md).

### Packages

- `cqrs-async` (`cqrs::`), `cqrs-async-macros`, `cqrs-async-postgres`
  (`cqrs_postgres::`), `cqrs-async-sea-orm` (`cqrs_sea_orm::`) and
  `cqrs-async-amqp` (`cqrs_amqp::`). MSRV 1.89 for the core, the macros and
  the AMQP crate; 1.94 (sqlx 0.9) for the PostgreSQL and SeaORM crates.

### Commands and queries

- Messages declared by attribute (`#[cqrs::command]`, `#[cqrs::query]`),
  owning their output and error types.
- Gateways that are their handlers: `#[cqrs::command_gateway(handles(..))]`
  and `#[cqrs::query_gateway(handles(..))]`, one `CommandHandler<C>` /
  `QueryHandler<Q>` impl per message, checked at compile time; the traits
  outside code names are generated.
- The command's unit of work: `CommandContext<E, Tx>` (`append`, `commit`,
  `metadata`, `tx`), appends committed once the handler returns, in
  a transaction of the application's `TransactionManager`, which the context
  owns (`CommandContext<E, Tx>` is `InTransaction<Tx>`, `CommandContext<E>`
  is `Autocommit`), or on their own; a handler re-run after a lost race.
  Events appended and never committed when a handler returns `Ok` refuse the
  command in every build, not only in debug ones. The conditions of all the
  writes of a unit are checked together, on its first write: what one
  aggregate was read from guards the writes of the others.
- Dispatch and event interceptors; metadata as correlation data.
- Commands sent later: `CommandSchedule` (`schedule_in` within the unit of
  work, `cancel`, `reschedule`), `deliver_scheduled`, in memory and on
  PostgreSQL.

### Aggregates and storage

- `#[cqrs::aggregate]`, `AggregateLoader` with snapshots, `New` / `Loaded`.
- An optional repository contract, `Repository<A>`: an adapter writes
  `find` and `save`, which take only the unit's transaction; a handler
  commits its events to the entity (`ctx.commit(entity)`) and saves it in
  that transaction. `AggregateLoader` implements it for any transaction; its
  `load_in` reads a tag-built aggregate so that what was read conditions the
  run's writes, even when it is absent or never saved.
  The application's own port can extend it.
- The engine hands out the repository of an aggregate:
  `events.repository::<Post>()` (`Events`, and `InlinePublisher` without a
  transaction) is an `AggregateLoader` over the engine's store, with the
  snapshots that store keeps for the aggregate and the engine's transaction
  type. The aggregate declares its snapshot policy,
  `#[cqrs::aggregate(.., snapshot(after_events = 100, after_sourcing = "5s"))]`
  (`SnapshotPolicy`, `Snapshotting`): a load that replayed that many events,
  or took that long, keeps a snapshot. The store must be able to keep them
  (`KeepSnapshots`: `Clone` in memory, `Serialize + DeserializeOwned` on
  PostgreSQL, `Postgres::snapshots`), checked at compile time.
  `AggregateLoader::with_snapshots(store, policy)` stays for loaders put
  together by hand, and a number still means "after that many events".
- Dynamic consistency boundaries: tags from `#[tag]` fields, `EventCriteria`,
  `AppendCondition`.
- Store ports by capability (`LoadStream`, `AppendEvents`, `SourceEvents`,
  `ReadLog`); the in-memory store; on PostgreSQL, named logs in one global
  order, gap-aware cursors that give a gap up only once no transaction can
  fill it, SQL checked at compile time, `MIGRATIONS` for the application's
  migration tool, upcasters, codecs (JSON, Protobuf).

### Event processing

- One event handler contract; projections with namespaces, each run by its
  own processor, committing its read model and its token in one unit of
  work; batches per namespace; replays and resets.
- Tracking processors with segments shared between instances, claims,
  retries; `LISTEN`/`NOTIFY` between instances on PostgreSQL.
- Dead letters, in memory and on PostgreSQL, for processors and for a
  projection's namespaces, with an `EnqueuePolicy` deciding to retry, park
  (with diagnostics), requeue, evict or ignore.
- Fan-in: a projection over several logs in their global order, through an
  enum with a variant per log's event type (`#[event(fan_in)]`).
- Subscription queries from the projection (`updates(..)`,
  `QueryUpdateEmitter<Self>`); catch-up subscriptions to the log as a
  `Stream`.

### Messaging

- The log as an outbox: `Forward` publishes each committed event through a
  `Publish` port, at least once, from its token.
- Coming back: `Subscribe` hands batches to a `Consume`r, acknowledged once
  handled.
- RabbitMQ (`cqrs-async-amqp`): publisher with confirms, subscriber with
  prefetch and requeue or dead-lettering, in the shapes of Watermill's
  presets (fanout, queue, topic).

### Examples and documentation

- `examples/blog` (GraphQL, in memory or PostgreSQL with SeaORM) and
  `examples/bank` (REST APIs, RabbitMQ, fan-in).
- The documentation site (Docusaurus) in `docs/`; benchmarks against
  cqrs-es, gated, in `benchmarks/`.

### Changed before the release, in the repository API

Nothing is published yet, but the documentation site and the examples
showed an earlier API (up to `61ee064`). Code written against it needs the
changes below. No public item of the core was removed without a
replacement; the design behind each one is in
[specs/repository-contract-plan.md](specs/repository-contract-plan.md).

Renamed:

- **`CommandContext::record(entity)` is `CommandContext::commit(entity)`.**
  A handler commits what it decided to an entity, as a new version of it,
  the way NestJS CQRS commits an aggregate's events. It is not a database
  commit: nothing is written by it, and the unit of work still appends the
  events and commits the transaction once the handler returns.
- **`ProcessingContext::transaction()` is `ProcessingContext::tx()`**, the
  same name as the command context's.

Changed:

- **The handler's transaction moved into its context.**
  `handle(&self, command, ctx: &mut CommandContext<E>, tx: &mut Tx)` is now
  `handle(&self, command, ctx: &mut CommandContext<E, Tx>)`, the
  transaction reached with `ctx.tx()`. The unit of work's mode comes from
  the context's type: `CommandContext<E>` is `Autocommit`,
  `CommandContext<E, Tx>` is `InTransaction<Tx>`. A third argument to
  `handle` is a compile error that says where the transaction went. Why:
  one value per run of a handler, as Axon 5's `ProcessingContext`, and the
  same access to the transaction on both sides of the log.
- **`AggregateLoader<A, S, N>` is `AggregateLoader<A, S, N, Tx = ()>`.**
  `Tx` is the transaction of the units of work it takes part in, which it
  never writes in; `in_transaction::<Tx>()` changes it. Why: an
  event-sourced repository could not run in a database's units of work
  (the blog's did not compile over PostgreSQL's transactions).
- **`AggregateLoader::with_snapshots(store, every)` takes
  `impl Into<SnapshotPolicy>`.** An integer still means "after that many
  events", so existing calls compile. Why: a policy also has
  `after_sourcing`, and the aggregate now declares it
  (`#[cqrs::aggregate(.., snapshot(..))]`) for the engine's repository.
- **Events appended and never committed refuse the command in every
  build** (`StoreError::Backend`, "never committed"). Before, debug builds
  panicked and release builds dropped the events silently.
- **The conditions of a unit's writes are checked together, on its first
  write.** Before, each write checked only its own condition, from the
  position the unit's previous write reached: what others appended in
  between was skipped, and a decision on two aggregates built from tags
  could commit on stale state.

Removed:

- **`EventHandler::committed`**, the step that ran after a processor's unit
  of work committed (and its counterparts on projections and inline
  handlers). Only tests used it, and what it was for, pushing query updates
  once a change is committed, is a projection namespace's `updated`, which
  also reaches the subscribers of every instance, not only the one that
  handled the event. Axon 5 has no such method either: there, the emitter
  defers to the unit's after-commit phase. Updates assembled by hand go
  through `EmitUpdates` and `emit_updates`.

Removed from the examples:

- **The blog's `EventSourcedPostRepository`**, `posts_over` and
  `SNAPSHOT_EVERY`. The repository comes from the engine
  (`events.repository::<Post>()`), the snapshot policy from the aggregate,
  and the blog's port is implemented for the engine's repository directly,
  its ids taken from the log.
- **The blog's own `find`/`save` signatures.** `PostRepository` is now the
  library's `Repository<Post>` (`find(id, tx)`, `save(post, tx)`) plus
  `next_id`.

[0.1.0-alpha.1]: https://github.com/Manuel-Antunes/cqrs-async
