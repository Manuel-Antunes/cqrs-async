# cqrs

CQRS and event sourcing for Rust, async from the ground up: commands and
queries routed to their handlers at compile time, optimistic concurrency and
dynamic consistency boundaries, projections that commit with their position
in one transaction, and PostgreSQL storage over `sqlx`.

Status: pre-release. Nothing is published yet and the API is being reworked
(see [docs/migration-plan.md](docs/migration-plan.md)). The crates will be
renamed when they are published.

| Crate | What |
|---|---|
| [`crates/cqrs`](crates/cqrs) | The core: contracts, the event-sourcing engine, in-memory stores, processors. |
| [`crates/cqrs-derive`](crates/cqrs-derive) | The macros, re-exported by `cqrs`. |
| [`crates/cqrs-postgres`](crates/cqrs-postgres) | Event, token and snapshot stores on PostgreSQL. |
| [`examples/blog`](examples/blog) | A layered example application. |
| [`benchmarks`](benchmarks) | Micro benchmarks, a load generator and a comparison with cqrs-es. |

Start with [docs/README.md](docs/README.md) and
[docs/architecture.md](docs/architecture.md).

## Building and testing

```text
cargo test --workspace
```

The PostgreSQL tests run only when `CQRS_POSTGRES_URL` points at a database
(they are skipped otherwise):

```text
docker run -d --name cqrs-pg -e POSTGRES_PASSWORD=cqrs -e POSTGRES_USER=cqrs \
    -e POSTGRES_DB=cqrs -p 55432:5432 postgres:17-alpine
CQRS_POSTGRES_URL=postgres://cqrs:cqrs@127.0.0.1:55432/cqrs cargo test --workspace
```

`cqrs-postgres` checks its SQL at compile time. The checked queries are
cached in `crates/cqrs-postgres/.sqlx`, so it builds without a database
(`SQLX_OFFLINE=true`); with `CQRS_POSTGRES_URL` set, the macros check against
that database, which must have the migrations of
`crates/cqrs-postgres/migrations` applied. To refresh the cache after
changing a query, build with `SQLX_OFFLINE_DIR=<empty dir>` set and the
database available, then copy the files over `.sqlx`.

Rust 1.89 or later, edition 2024.

## License

MIT, see [LICENSE](LICENSE).
