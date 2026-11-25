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
- The command's unit of work: `CommandContext` (`append`, `record`,
  `metadata`), appends committed once the handler returns, in a transaction
  of the application's `TransactionManager` or on their own; a handler
  re-run after a lost race.
- Dispatch and event interceptors; metadata as correlation data.
- Commands sent later: `CommandSchedule` (`schedule_in` within the unit of
  work, `cancel`, `reschedule`), `deliver_scheduled`, in memory and on
  PostgreSQL.

### Aggregates and storage

- `#[cqrs::aggregate]`, `AggregateLoader` with snapshots, `New` / `Loaded`.
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

[0.1.0-alpha.1]: https://github.com/Manuel-Antunes/cqrs-async
