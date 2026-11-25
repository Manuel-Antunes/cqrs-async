# cqrs-async

CQRS and event sourcing for async Rust, routed at compile time: commands and
queries reach their handlers through the type checker (a missing handler is
a compile error, not a runtime one), aggregates are loaded and committed with
optimistic concurrency or dynamic consistency boundaries, and projections
commit their read model with their position in one transaction. A library,
not a framework: the application owns its repositories, its database and its
transactions.

**Documentation:** <https://manuel-antunes.github.io/cqrs-async/>: an
introduction, a guide that builds a blog step by step, and the reference
(architecture, patterns, unit of work, event processing, storage,
performance).

Status: `0.1.0-alpha.1`, not on crates.io yet. Until it is, depend on the
crates by `git` or `path`.

## Crates

| Package | Written in code | What | MSRV |
|---|---|---|---|
| [`cqrs-async`](crates/cqrs) | `cqrs::` | The core: messages and gateways, aggregates, the command's unit of work, event handlers and projections, tracking processors, subscriptions, the in-memory store, dead letters, scheduled commands, fan-in, the broker ports. | 1.89 |
| [`cqrs-async-macros`](crates/cqrs-derive) | (through `cqrs`) | The attribute macros and `#[derive(DomainEvent)]`. | 1.89 |
| [`cqrs-async-postgres`](crates/cqrs-postgres) | `cqrs_postgres::` | PostgreSQL over `sqlx`: event, token, snapshot and dead-letter stores, the command schedule, fan-in, `LISTEN`/`NOTIFY` between instances, SQL checked at compile time. | 1.94 |
| [`cqrs-async-sea-orm`](crates/cqrs-sea-orm) | `cqrs_sea_orm::` | SeaORM repositories and read models in the library's PostgreSQL transactions. | 1.94 |
| [`cqrs-async-amqp`](crates/cqrs-amqp) | `cqrs_amqp::` | RabbitMQ over `lapin`: the event log forwarded to exchanges and queues (the log as an outbox), and messages consumed back. | 1.89 |

```toml
[dependencies]
cqrs-async = { git = "https://github.com/Manuel-Antunes/cqrs-async" }           # once published: "0.1.0-alpha.1"
cqrs-async-postgres = { git = "https://github.com/Manuel-Antunes/cqrs-async" }  # when you need PostgreSQL
```

The packages are `cqrs-async*`; their libraries are named `cqrs`,
`cqrs_postgres`, `cqrs_sea_orm` and `cqrs_amqp`, which is what the code and
the documentation write.

## A taste

```rust
#[cqrs::command(error = BankError)]
struct Deposit { id: u32, amount: u64 }

#[cqrs::command_gateway(handles(Open, Deposit))]
struct Bank<P> {
    accounts: AggregateLoader<Account, InMemoryEventStore<AccountEvent>>,
    #[gateway(unit)]
    events: P,
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

The whole quickstart, compiled and run as a doctest, opens
[`crates/cqrs/src/lib.rs`](crates/cqrs/src/lib.rs).

## Examples

- [`examples/blog`](examples/blog): a layered application (domain,
  application, infrastructure, presentation) served as a GraphQL API, in
  memory or on PostgreSQL with SeaORM. The guide builds it.
- [`examples/bank`](examples/bank): two applications behind REST APIs
  (axum) talking through RabbitMQ: a bank whose portfolios are projected
  from two logs (fan-in) and whose accounts' log is its outbox, and a
  compliance service consuming it into commands of its own.

## Building and testing

```text
cargo test --workspace
```

The PostgreSQL and RabbitMQ tests run only when they have a server (they
are skipped otherwise):

```text
docker run -d --name cqrs-pg -e POSTGRES_PASSWORD=cqrs -e POSTGRES_USER=cqrs \
    -e POSTGRES_DB=cqrs -p 55432:5432 postgres:17-alpine
docker run -d --name cqrs-rabbit -p 55672:5672 rabbitmq:4-alpine
CQRS_POSTGRES_URL=postgres://cqrs:cqrs@127.0.0.1:55432/cqrs \
CQRS_AMQP_URL=amqp://127.0.0.1:55672/%2f cargo test --workspace
```

`cqrs-async-postgres` checks its SQL at compile time. The checked queries
are cached in `crates/cqrs-postgres/.sqlx`, so it builds without a database
(`SQLX_OFFLINE=true`); with `CQRS_POSTGRES_URL` set, the macros check
against that database, which must have the migrations of
`crates/cqrs-postgres/migrations` applied. The crate's documentation says
how to refresh the cache after changing a query.

Benchmarks: [`benchmarks`](benchmarks) has micro benchmarks, a load
generator and a comparison with cqrs-es on the same PostgreSQL; the numbers
and how they are gated are in the
[performance page](https://manuel-antunes.github.io/cqrs-async/docs/performance)
and [`docs/benchmarks/gate-log.md`](docs/benchmarks/gate-log.md).

The documentation site lives in [`docs/`](docs) (Docusaurus); to preview it:

```text
cd docs && npm install && npm start
```

## License

MIT, see [LICENSE](LICENSE).
