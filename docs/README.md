# cqrs_async

CQRS and event sourcing for Rust, modeled on Axon Framework 5 and NestJS CQRS,
with everything dispatched statically: no `dyn`, no `Box<dyn Future>`, no
`Any`, no `TypeId` registry. Routing a message to its handler, wiring a
projection to its unit of work, chaining interceptors: the compiler resolves
all of it, so a missing or duplicated handler is a compile error, and the
running code is plain function calls.

It is async from the ground up: every contract is a `Future` or a `Stream`,
committed events are a `Stream` you can hand to axum, async-graphql or a
Tokio task, and event processing runs on Tokio with Axon's pooled streaming
model (segments, claims, tokens, dead letters).

> **Name.** The crates in this workspace are still called `cqrs`,
> `cqrs-derive` and `cqrs-postgres`; the examples use `cqrs::` paths. They
> will be published as `cqrs_async`. Replace the paths when they are.

## What you get

| | |
|---|---|
| **Messages** | Commands and queries as plain structs; the message owns its output and error types. One handler struct per use case. |
| **Gateways** | `#[derive(Gateway)]` on a struct of handlers routes by type at compile time; ports are `CommandGateway<C>` / `QueryGateway<Q>` bounds, bundled per module by the message enums (`port = PostCommands`). |
| **Composition** | A typed builder wires an application's `Cqrs` unit (publisher, repository, gateways, interceptors, processors, subscriptions), inferring every type; switching stores changes its inputs only. |
| **Domain** | Aggregates are plain structs. Decisions take `&self` and an `EventAppender`; state changes only in `evolve`. |
| **Consistency** | Optimistic concurrency per stream, plus Axon 5's dynamic consistency boundaries (tags, criteria, conditional appends). |
| **Event processing** | One `EventHandler` contract for projections and policies, run inline or by tracking processors in a unit of work (`ProcessingContext`) that commits the read model with the token. |
| **Read side** | Read-model repositories as ports, query update emitters, subscription queries that see your own writes, catch-up event subscriptions. |
| **Cross-cutting** | Dispatch interceptors around gateways, handler interceptors around event handlers, inside their transaction. |
| **Storage** | In memory, and PostgreSQL over `sqlx` with compile-checked SQL, parallel appends and a gap-aware cursor. |
| **Testing** | Given/when/then fixture for aggregates, a tiny `block_on`, everything runnable without a runtime. |

## Read in this order

1. [Architecture](architecture.md): the moving parts, how a command becomes
   a read-model row, and how each concept maps to Axon 5 and NestJS.
2. [Guide](guide/README.md): build a blog step by step, from the domain to a
   GraphQL API on PostgreSQL. The finished code is `examples/blog`.
3. [Patterns](patterns.md): every small design pattern the library is made
   of (what problem it solves, its shape, its rules), one section each.
4. Reference:
   - [Unit of work](unit-of-work.md): `ProcessingContext`, transactions,
     what commits when, and how it compares to Axon's.
   - [Event processing](event-processing.md): tracking processors,
     segments, tokens, replay, dead letters, subscriptions, live queries.
   - [Storage](storage.md): the event store contract, the in-memory and
     PostgreSQL stores, cursors, locks, snapshots and upcasting.
   - [Performance](performance.md): benchmarks against cqrs-es and the cost
     of each abstraction.

## Crates and features

| Crate | What |
|---|---|
| `cqrs` | The core: contracts, the event-sourcing engine, in-memory stores, processors. |
| `cqrs-derive` | The macros, re-exported by `cqrs` (feature `derive`). |
| `cqrs-postgres` | Event store, token store and snapshot store on PostgreSQL. |

| Feature of `cqrs` | Default | Adds |
|---|---|---|
| `derive` | yes | `#[derive(Command, Query, Gateway, AggregateRoot, DomainEvent, TagResolver, ...)]`, `#[command_handler]`, `#[query_handler]`, `#[projection]` |
| `tokio` | yes | `Events`, tracking processors, catch-up subscriptions, subscription queries |
| `json` | no | `StoredEvent`, upcasters (turned on by `cqrs-postgres`) |

With `default-features = false` the core is runtime-agnostic: contracts,
the event-sourcing engine and the in-memory store, all `Future`s and `Stream`s from
`futures-core`.

```toml
[dependencies]
cqrs = { path = "crates/cqrs" }                 # derive + tokio
cqrs-postgres = { path = "crates/cqrs-postgres" } # when you need PostgreSQL
```

Rust 1.89 or later, edition 2024.

## Design rules

These hold everywhere in the library, and are worth keeping in an
application built on it:

- **No `dyn`.** Heterogeneous collections are tuples or type-level lists;
  extension points are generic parameters. See
  [Patterns § Static dispatch](patterns.md#static-dispatch).
- **Macros only write what you could write by hand**: trait impls and
  associated types. None adds fields to your types or rewrites them, and each
  has a hand-written equivalent.
- **The domain does not know the framework.** An aggregate is a struct;
  its decisions take `impl EventAppender<E>` (or std's `impl Extend<E>`).
- **Ports in the application, adapters in infrastructure, wiring in one
  composition root.** Handlers are generic over the ports they use.
- **The store is the source of truth.** Live feeds, query updates and
  snapshots are optimizations that can be lost and rebuilt.
