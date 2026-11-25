---
title: "Introduction"
sidebar_position: 10
---

CQRS and event sourcing for Rust, modeled on Axon Framework 5.

It is a **library, not a framework**. The application owns its
repositories, its database and its transactions, and lends them to the
handlers. The library runs the parts that are easy to get wrong: the unit
of work around a command (append what was decided, commit, retry a lost
race), and the processors that turn committed events into read models,
each with its token, its transaction and its dead letters.

Everything is **dispatched statically**. There is no handler registry
behind `dyn`, no `Box<dyn Future>`, no `Any` and no `TypeId`: routing a
command to its handler, wiring a projection's namespaces to their
processors, chaining interceptors, all of it is resolved by the type
checker. The running code is plain function calls, and the mistakes a
registry finds at startup are compile errors here:

- a command listed in a gateway's `handles(..)` without its
  `CommandHandler` impl;
- a projection namespace without its event handler (the projection cannot
  be wired), or a handler for a namespace the projection does not list;
- an update emitted for a query the projection does not list in
  `updates(..)`;
- an entity committed with events of another aggregate's type.

It is async from the ground up: every contract is a `Future` or a
`Stream`, and committed events are a `Stream` you can hand to axum,
async-graphql or a Tokio task.

## Quickstart

An account opened and credited through commands; its balance, a read model
kept by a projection, answered by a query. In memory and without a
runtime: the projection runs inline, right after each commit. This is the
quickstart of the crate's rustdoc, which compiles and runs as a test.

The domain: what happened to an account, and the account deciding.

```rust
use std::{collections::HashMap, sync::{Arc, Mutex}};

use cqrs::{
    Cqrs, aggregate::AggregateLoader, prelude::*, processing::InlinePublisher,
    store::{InMemoryEventStore, StoreError}, testing::block_on,
};

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum AccountEvent {
    Opened,
    Deposited { amount: u64 },
}

#[cqrs::aggregate(name = "account", event = AccountEvent)]
struct Account { id: u32, balance: u64 }

impl Account {
    fn apply(&mut self, event: &AccountEvent) {
        if let AccountEvent::Deposited { amount } = event {
            self.balance += amount;
        }
    }

    fn deposit(&self, amount: u64, events: &mut impl EventAppender<AccountEvent>) -> Result<(), BankError> {
        if amount == 0 {
            return Err(BankError::NothingToDeposit);
        }
        events.append(AccountEvent::Deposited { amount });
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
enum BankError { NothingToDeposit, NoSuchAccount, Store(String) }

impl From<StoreError> for BankError {
    fn from(error: StoreError) -> Self { Self::Store(error.to_string()) }
}
```

The commands, and the gateway handling them, one impl each. The gateway is
the handler: one struct with what its commands need, and the unit of work
they run in (`#[gateway(unit)]`). A handler appends through its
`CommandContext` and commits them to the entity they belong to; it never
commits the transaction.

```rust
#[cqrs::command(error = BankError)]
struct Open { id: u32 }

#[cqrs::command(error = BankError)]
struct Deposit { id: u32, amount: u64 }

#[cqrs::command_gateway(handles(Open, Deposit))]
struct Bank<P> {
    accounts: AggregateLoader<Account, InMemoryEventStore<AccountEvent>>,
    #[gateway(unit)]
    events: P,
}

#[cqrs::command_handler]
impl<P> CommandHandler<Open> for Bank<P> {
    async fn handle(&self, command: &Open, ctx: &mut CommandContext<AccountEvent>) -> Result<(), BankError> {
        ctx.append(AccountEvent::Opened);
        ctx.commit(New::<Account>(command.id)); // a new stream: fails if it exists
        Ok(())
    }
}

#[cqrs::command_handler]
impl<P> CommandHandler<Deposit> for Bank<P> {
    async fn handle(&self, command: &Deposit, ctx: &mut CommandContext<AccountEvent>) -> Result<(), BankError> {
        let account = self.accounts.find(&command.id, ctx.tx()).await?.ok_or(BankError::NoSuchAccount)?;
        account.deposit(command.amount, ctx)?;
        ctx.commit(account); // appended to its stream once the handler returns
        Ok(())
    }
}
```

The read side: a projection keeps the balances, a query reads them.

```rust
type Balances = Arc<Mutex<HashMap<u32, u64>>>;

#[cqrs::projection(name = "account", events = AccountEvent, namespaces("balances"))]
struct AccountProjection { balances: Balances }

#[cqrs::event_handler(namespace = "balances")]
impl EventHandler for AccountProjection {
    async fn handle(&self, event: &EventEnvelope<AccountEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        let id = event.aggregate_id::<Account>().ok_or_else(|| HandlerError::msg("not an account"))?;
        let mut balances = self.balances.lock().unwrap();
        let balance = balances.entry(id).or_default();
        if let AccountEvent::Deposited { amount } = event.payload {
            *balance += amount;
        }
        Ok(())
    }
}

#[cqrs::query(output = Option<u64>, error = BankError)]
struct Balance { id: u32 }

#[cqrs::query_gateway(handles(Balance))]
struct Accounts { balances: Balances }

#[cqrs::query_handler]
impl QueryHandler<Balance> for Accounts {
    async fn handle(&self, query: Balance) -> Result<Option<u64>, BankError> {
        Ok(self.balances.lock().unwrap().get(&query.id).copied())
    }
}
```

Wiring: the store, the publisher running the projection inline, and the
application's unit (`Cqrs`), built by a typed builder.

```rust
fn main() -> Result<(), BankError> {
    let balances = Balances::default();
    let publisher = InlinePublisher::new(InMemoryEventStore::new())
        .with_projection(AccountProjection { balances: Arc::clone(&balances) });
    // The publisher hands out the accounts' repository: its store, no snapshots.
    let bank = Cqrs::from_publisher(publisher)
        .commands(|events| Bank { accounts: events.repository::<Account>(), events })
        .queries(Accounts { balances })
        .build();

    block_on(async {
        bank.commands().send(Open { id: 1 }).await?;
        bank.commands().send(Deposit { id: 1, amount: 30 }).await?;
        assert_eq!(bank.commands().send(Deposit { id: 1, amount: 0 }).await, Err(BankError::NothingToDeposit));
        assert_eq!(bank.queries().query(Balance { id: 1 }).await?, Some(30));
        Ok::<(), BankError>(())
    })
}
```

Going asynchronous changes only the wiring: `Cqrs::new(Events::new(store))`
instead of `Cqrs::from_publisher(..)`, and `.projection(..)` on the builder,
which runs each namespace in a tracking processor of its own. The
[guide](../guide/index.md) takes that step, and the next ones.

## Crates and features

| Crate | What | MSRV |
|---|---|---|
| `cqrs` | The core: contracts, the command unit of work, the event-sourcing engine, in-memory stores, tracking processors, subscription queries. | 1.89 |
| `cqrs-derive` | The macros, re-exported by `cqrs` (feature `derive`). | 1.89 |
| `cqrs-postgres` | PostgreSQL over a plain `sqlx` pool: event store, token store, transactions, snapshots, dead letters, and the `Postgres` entry point (`Postgres::new(pool)`, `migrate()`, `events::<E>("posts")`). | 1.94 (sqlx 0.9) |
| `cqrs-sea-orm` | SeaORM in the same PostgreSQL transaction: `SeaOrm::new(tx)` over the unit's `PgTransaction`. | 1.94 (SeaORM 2) |

All on edition 2024. The PostgreSQL crates need 1.94 only because their
dependencies do; the core stays on 1.89.

| Feature of `cqrs` | Default | Adds |
|---|---|---|
| `derive` | yes | The macros: `#[cqrs::aggregate]`, `#[cqrs::command]`, `#[cqrs::query]`, `#[cqrs::command_gateway]`, `#[cqrs::query_gateway]`, `#[cqrs::command_handler]`, `#[cqrs::query_handler]`, `#[cqrs::projection]`, `#[cqrs::event_handler]`, `#[derive(cqrs::DomainEvent)]` |
| `tokio` | yes | `Events`: the broadcast after commit, tracking processors, catch-up subscriptions, subscription queries |
| `json` | no | Events stored as JSON, upcasters, the `Json` codec (turned on by `cqrs-postgres`) |
| `protobuf` | no | The `Protobuf` codec, for `prost` messages |

With `default-features = false` the core is runtime-agnostic: contracts,
the command unit of work, inline processing and the in-memory store, all
`Future`s and `Stream`s from `futures-core`.

```toml
[dependencies]
cqrs-async = "0.1.0-alpha.1"           # derive + tokio; written `cqrs::` in code
cqrs-async-postgres = "0.1.0-alpha.1"  # when you need PostgreSQL: `cqrs_postgres::`
cqrs-async-sea-orm = "0.1.0-alpha.1"   # when your repositories use SeaORM: `cqrs_sea_orm::`
cqrs-async-amqp = "0.1.0-alpha.1"      # when events go through RabbitMQ: `cqrs_amqp::`
```

> **Names.** The packages are `cqrs-async`, `cqrs-async-macros` (used
> through `cqrs-async`), `cqrs-async-postgres`, `cqrs-async-sea-orm` and
> `cqrs-async-amqp`; in code they are `cqrs`, `cqrs_postgres`,
> `cqrs_sea_orm` and `cqrs_amqp`, which is what
> every example writes. Renaming the dependency in `Cargo.toml` (`es = {
> package = "cqrs-async", .. }`) works too, macros included. Not on
> crates.io yet: until then, depend on them by `path`.

## Design rules

These hold everywhere in the library, and are worth keeping in an
application built on it:

- **Static dispatch.** Heterogeneous collections are tuples; extension
  points are generic parameters. See
  [Patterns § Static dispatch](patterns.md#static-dispatch).
- **Macros only write what you could write by hand**: trait impls,
  associated types, and the traits and types their callers name (the
  gateway's `commands = PostCommands`, the projection's update channels).
  None adds fields to your types, and each documents its hand-written
  equivalent.
- **The domain does not know the framework.** An aggregate is a struct;
  its decisions take `&mut impl EventAppender<E>`, and its state changes
  only in `apply` (`Aggregate::evolve`).
- **The framework commits the transaction, not the handler.** A command
  handler commits what it decided to its entities; the unit of work appends and commits, and runs the
  handler again after a lost race. So a handler has no side effects: those
  belong in a projection namespace, after the commit.
- **The store is the source of truth.** Read models, live feeds, query
  updates and snapshots can be lost and rebuilt from it.

## Read in this order

1. [Architecture](architecture.md): the modules, how a command becomes a
   read-model row, and how each Axon 5 concept maps to this library.
2. [Guide](../guide/index.md): build a blog step by step, from the domain to
   a GraphQL API on PostgreSQL. The finished code is `examples/blog`.
3. [Patterns](patterns.md): the small design patterns the library is made
   of, one section each. `examples/bank` puts two of them to work in a
   second application: a bank behind a REST API (axum) whose portfolios
   are projected from two logs (fan-in), and whose accounts' log reaches a
   compliance service through RabbitMQ (the log as an outbox).
4. Reference:
   - [Unit of work](unit-of-work.md): what commits when, on both sides.
   - [Event processing](event-processing.md): tracking processors,
     segments, tokens, replay, dead letters, subscriptions.
   - [Storage](storage.md): the store ports, in memory and PostgreSQL,
     cursors, snapshots and upcasting.
   - [Performance](performance.md): benchmarks and the cost of each
     abstraction.
