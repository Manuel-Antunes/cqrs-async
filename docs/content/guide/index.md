---
title: "Guide: building a blog"
sidebar_position: 1
---

This guide builds a small blogging backend, one layer at a time: posts are
drafted, retitled and published; readers query summaries and subscribe to
changes; publishing notifies someone. By the end it runs on PostgreSQL
behind a GraphQL API, with tracking processors, live queries and
interceptors.

The finished code is `examples/blog`; every snippet here comes from it.

| Part | You build | You learn |
|---|---|---|
| [1. The domain](01-domain.md) | `Post`, its events and rules, domain tests | aggregates, decisions with an appender, events and tags, the test fixture |
| [2. Commands](02-commands.md) | `CreatePost`, `RetitlePost`, `PublishPost`, the repository port, the command gateway | the gateway as the handler, `CommandContext` and `record`, units of work, retries |
| [3. Queries and the read model](03-queries.md) | `PostSummary`, its ports, `FindPost`, `ListPublished`, the query gateway | the read side as ports, the query gateway |
| [4. The projection](04-projection.md) | `PostProjection` and its `summaries` namespace, the first working app | event handlers, inline processing, the composition root |
| [5. Going asynchronous](05-async.md) | tracking processors, a `notifications` namespace | `Events`, units of work, replay, segments |
| [6. Live Queries and Subscriptions](06-live-queries.md) | subscriptions to `FindPost` and `ListPublished` | query update emitters, reading your own writes |
| [7. Interceptors](07-interceptors.md) | an audit log, projection metrics | dispatch and event interceptors |
| [8. PostgreSQL](08-postgres.md) | the same app on a database, with SeaORM | the Postgres stores, one transaction for state and events, transactional projections |
| [9. The API](09-api.md) | GraphQL over axum, event subscriptions | ports for the presentation layer, metadata, the event feed |

Prerequisites: Rust 1.89+ (edition 2024) for `cqrs`; the blog itself needs
1.94, because SeaORM 2 does. For part 8, a PostgreSQL
(`docker run -e POSTGRES_PASSWORD=cqrs -p 55432:5432 postgres:17`).

The project layout we end up with:

```text
src/
├── domain/              # Post, PostEvent, PostError, PostId
├── application/
│   ├── commands/        # one file per command and its handler, the repository port, the command gateway
│   ├── queries/         # one file per query and its handler, the read model and its ports, the query gateway
│   └── projections/     # PostProjection: one namespace per file (summaries, notifications)
├── infrastructure/      # adapters: repositories, read model, notifier, interceptors, postgres/
├── presentation/        # GraphQL + axum, against a ports trait
├── config.rs            # the composition root: the blog's Cqrs unit
└── lib.rs               # modules and re-exports
```

Side effects, such as the notification, are not a folder of their own:
they are a namespace of the projection that does not replay (part 5).
