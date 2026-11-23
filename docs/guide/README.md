# Guide: building a blog

This guide builds a small blogging backend, one layer at a time: posts are
drafted, retitled and published; readers query summaries and subscribe to
changes; publishing notifies someone. By the end it runs on PostgreSQL
behind a GraphQL API, with tracking processors, live queries and
interceptors.

The finished code (except PostgreSQL, added in part 8) is
`examples/blog`; every snippet here comes from it or extends it.

| Part | You build | You learn |
|---|---|---|
| [1. The domain](01-domain.md) | `Post`, its events and rules, domain tests | aggregates, decisions with an appender, events, the test fixture |
| [2. Commands](02-commands.md) | `CreatePost`, `RetitlePost`, `PublishPost`, the command gateway | messages, one handler per use case, repositories, retries |
| [3. Queries and the read model](03-queries.md) | `PostSummary`, its repository port, `FindPost`, `ListPublished` | the read side as ports, the query gateway |
| [4. The projection](04-projection.md) | `PostSummaryProjection`, the first working app | event handlers, inline processing, the composition root |
| [5. Going asynchronous](05-async.md) | tracking processors, a notification policy | `Events`, units of work, replay, segments |
| [6. Live queries](06-live-queries.md) | subscriptions to `FindPost` and `ListPublished` | query update emitters, reading your own writes |
| [7. Interceptors](07-interceptors.md) | an audit log, projection metrics | dispatch and event interceptors |
| [8. PostgreSQL](08-postgres.md) | the same app on a database | the Postgres stores, transactional projections, operations |
| [9. The API](09-api.md) | GraphQL over axum, event subscriptions | ports for the presentation layer, metadata, the event feed |

Prerequisites: Rust 1.89+ (edition 2024), and for part 8 a PostgreSQL
(`docker run -e POSTGRES_PASSWORD=cqrs -p 55432:5432 postgres:17`).

The project layout we end up with:

```text
src/
├── domain/              # Post, PostEvent, PostError, PostId
├── application/
│   ├── commands/        # one file per command + its handler, the command gateway
│   ├── queries/         # queries + handlers, the read model and its port, the query gateway
│   ├── projections/     # PostSummaryProjection
│   └── policies/        # NotifyOnPublish
├── infrastructure/      # adapters: read-model repository, notifier, interceptors
├── presentation/        # GraphQL + axum, against a ports trait
├── config.rs            # the composition root: the blog's Cqrs unit
└── lib.rs               # modules and re-exports
```
