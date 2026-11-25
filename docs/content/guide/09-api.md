---
title: "9. The API"
sidebar_position: 19
---

The last layer exposes the application over HTTP: GraphQL queries and
mutations through the gateways, and subscriptions through the subscription
queries and the event feed. Like the application, it is written against
ports only: the traits the gateways and the projection generate.

## One trait for everything the API needs

```rust
// src/presentation/mod.rs
use cqrs::{Cqrs, store::EventFeed};

pub trait BlogApi: Send + Sync + 'static {
    /// Sends the commands.
    type Commands: PostCommands;
    /// Answers the queries, and streams their results as they change.
    type Queries: PostQueries + PostSubscriptions;
    /// Streams the committed events (the framework's port: no feed to write).
    type Events: EventFeed<PostEvent>;

    fn commands(&self) -> &Self::Commands;
    fn queries(&self) -> &Self::Queries;
    fn events(&self) -> &Self::Events;
}
```

Each bound comes from a macro argument met earlier:

- `PostCommands`, from `#[cqrs::command_gateway(commands = PostCommands, ..)]`:
  `CommandGateway<CreatePost> + CommandGateway<RetitlePost> + CommandGateway<PublishPost>`.
- `PostQueries`, from `#[cqrs::query_gateway(queries = PostQueries, ..)]`:
  a `QueryGateway` for each query.
- `PostSubscriptions`, from `#[cqrs::projection(.., subscriptions = PostSubscriptions)]`:
  subscribing to each query in the projection's `updates(..)`.
- `EventFeed<PostEvent>`, the library's port on the committed events.

The bounds are the contract: the API compiles against any application
that sends these commands, answers and streams these queries, and streams
these events. No resolver knows the store, the repository or the read
model.

Any CQRS unit that exposes those ports is the API, whatever its gateways,
interceptors and stores:

```rust
// src/presentation/mod.rs
impl<C, Q, E> BlogApi for Cqrs<C, Q, E>
where
    C: PostCommands + 'static,
    Q: PostQueries + PostSubscriptions + 'static,
    E: EventFeed<PostEvent> + 'static,
{
    type Commands = C;
    type Queries = Q;
    type Events = E;

    fn commands(&self) -> &C {
        Cqrs::commands(self)
    }

    fn queries(&self) -> &Q {
        Cqrs::queries(self)
    }

    fn events(&self) -> &E {
        Cqrs::events(self)
    }
}
```

A second impl forwards `Arc<A>`, which is what the server shares. So the
in-memory `start_in_memory()` and the PostgreSQL `start(pool)` serve the
same API, unchanged, and a test double needs only these ports.

## Mutations send commands, with the request's metadata

```rust
// src/presentation/graphql/mutation.rs
pub struct MutationRoot<A>(PhantomData<fn() -> A>);

#[Object(name = "Mutation")]
impl<A: BlogApi> MutationRoot<A> {
    /// Writes a draft and returns its id.
    async fn create_post(&self, ctx: &Context<'_>, title: String) -> Result<ID> {
        let api = ctx.data_unchecked::<A>();
        let id = api
            .commands()
            .send_with(CreatePost { title }, metadata(ctx))
            .await
            .map_err(from_blog)?;
        Ok(ID(id.to_string()))
    }

    /// Makes a draft public. Returns the post's id.
    async fn publish_post(&self, ctx: &Context<'_>, id: ID) -> Result<ID> {
        let api = ctx.data_unchecked::<A>();
        api.commands()
            .send_with(PublishPost { id: post_id(&id)? }, metadata(ctx))
            .await
            .map_err(from_blog)?;
        Ok(id)
    }

    // retitle_post: the same, with RetitlePost
}

/// The metadata of the request being resolved (none outside HTTP).
fn metadata(ctx: &Context<'_>) -> Metadata {
    ctx.data_opt::<Metadata>().cloned().unwrap_or_default()
}
```

Each resolver builds the command's own struct and sends it. `send_with`
is generic over the command, and its result is that command's `output` and
`error`: `PostId` for `CreatePost`, `()` for `PublishPost`. There is no
enum of commands to wrap them in, nor one of outputs to unwrap.

The same holds for any other transport. A JSON endpoint or a queue
consumer deserializes the command's type (the blog's commands derive
`serde::Deserialize`) and sends it:

```rust
let command: CreatePost = serde_json::from_slice(&body)?;
let id = api.commands().send(command).await?;
```

A transport that takes several commands on one route chooses the type from
its own envelope (a field, a topic, a path) and deserializes into it.

The HTTP layer turns a header into metadata, so every event a request
causes stores its correlation id (and the `Audit` interceptor of part 7
records it):

```rust
// src/presentation/http.rs
pub const CORRELATION_HEADER: &str = "x-correlation-id";

async fn execute<A: BlogApi>(
    State(schema): State<BlogSchema<A>>,
    headers: HeaderMap,
    request: GraphQLRequest,
) -> GraphQLResponse {
    let mut metadata = Metadata::new();
    if let Some(id) = headers.get(CORRELATION_HEADER).and_then(|value| value.to_str().ok()) {
        metadata.insert("correlation_id".to_owned(), id.to_owned());
    }
    schema.execute(request.into_inner().data(metadata)).await.into()
}
```

`BlogError` maps to GraphQL errors by variant, never by parsing messages:
an extension `code` of `DOMAIN_RULE`, `NOT_FOUND` or `CONFLICT` (retry),
and a generic `INTERNAL` error for `Store`, whose details stay in the logs.

## Subscriptions

Two kinds, both plain `Stream`s handed to async-graphql:

```rust
// src/presentation/graphql/subscription.rs
#[Subscription(name = "Subscription")]
impl<A: BlogApi> SubscriptionRoot<A> {
    /// The post now (`null` if it does not exist yet), then again each time
    /// it changes. Same shape as `Query.post`.
    async fn post(&self, ctx: &Context<'_>, id: ID) -> Result<impl Stream<Item = Result<Option<PostNode>>> + use<A>> {
        let query = FindPost { id: post_id(&id)? };
        let updates = ctx.data_unchecked::<A>().queries().subscribe(query);
        Ok(updates.map(|post| post.map(|post| post.map(PostNode::from)).map_err(from_blog)))
    }

    /// The id of each post published from now on.
    async fn post_published(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = Result<ID>> + use<A>> {
        let events = ctx.data_unchecked::<A>().events();
        let now = events.last_position().await.map_err(from_store)?;
        Ok(events.subscribe(now).filter_map(|event| {
            ready(match event {
                Ok(event) => {
                    matches!(event.payload, PostEvent::Published).then(|| Ok(ID(event.stream.id().to_owned())))
                }
                Err(error) => Some(Err(from_store(error))),
            })
        }))
    }

    // published_posts: subscribe(ListPublished); post_events(after, postId, kinds)
}
```

- **Subscription queries** (`post`, `publishedPosts`) have the same shape
  as the queries: what most clients want. `queries().subscribe(FindPost { id })`
  yields the current result, then each update the projection's `summaries`
  namespace emits in `updated` (part 6). The current result waits until the
  read model handled what was committed when the subscription started, so
  a client that subscribes right after its mutation sees it. On PostgreSQL,
  a client gets the updates on whichever instance it is connected to.
- **The event log** (`postEvents(after, postId, kinds)`, `postPublished`)
  streams committed events, filtered on the server with `StreamExt` /
  `TryStreamExt` combinators (where NestJS would pipe RxJS operators).
  `subscribe(after)` delivers every event after a position exactly once, in
  order: a client reconnecting with the last position it saw misses
  nothing.

The streams do not borrow the API (`+ use<A>`), so async-graphql can hold
them for the lifetime of the WebSocket.

## Serving

```rust
// src/presentation/http.rs
pub fn router<A: BlogApi>(schema: BlogSchema<A>) -> Router {
    Router::new()
        .route(GRAPHQL_PATH, get(graphiql).post(execute::<A>))
        .route_service(SUBSCRIPTIONS_PATH, GraphQLSubscription::new(schema.clone()))
        .route("/health", get(|| async { "ok" }))
        .with_state(schema)
}
```

`main` picks the wiring, and serves either the same way:

```rust
// src/main.rs
match std::env::var("BLOG_DATABASE_URL") {
    Ok(url) => serve(blog::start(PgPool::connect(&url).await?).await?).await,
    Err(_) => serve(blog::start_in_memory()).await,
}

/// Serves `running` until Ctrl-C, then stops its processors.
async fn serve<C, Q, R, E>(running: Running<C, Q, R, E>) -> Result<(), Box<dyn std::error::Error>>
where
    Arc<cqrs::Cqrs<C, Q, E>>: BlogApi,
{
    let schema = graphql::schema(Arc::clone(&running.app));
    // ... --schema prints the SDL and returns
    let address = std::env::var("BLOG_ADDR").unwrap_or_else(|_| "127.0.0.1:8000".to_owned());
    let listener = TcpListener::bind(&address).await?;
    axum::serve(listener, http::router(schema))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    running.stop().await;   // processors finish their current event
    Ok(())
}
```

`serve` only asks that the unit be a `BlogApi`: the presentation layer does
not know which wiring it got.

```text
cargo run -p blog                                    # in memory; GraphiQL on http://127.0.0.1:8000/graphql
BLOG_DATABASE_URL=postgres://... cargo run -p blog   # on PostgreSQL, with SeaORM
cargo run -p blog -- --schema                        # prints the SDL
```

## Testing the API

`tests/api.rs` runs against `start_in_memory()`: most tests execute
against the schema directly, and a few drive the real router,
mutations over HTTP and subscriptions over a WebSocket
(`graphql-transport-ws`). One checks what the interceptors recorded:

```rust
// tests/api.rs
#[tokio::test]
async fn interceptors_audit_every_command_and_measure_the_projection() {
    let (running, schema) = app();
    // ... `server` serves http::router(schema) on a local port; createPost with
    // "x-correlation-id: req-9", then publishPost twice (the second refused) ...
    running.settled().await;

    let entries = running.audit.entries();
    let summary: Vec<_> = entries
        .iter()
        .map(|e| (e.command, e.correlation_id.as_deref(), e.succeeded))
        .collect();
    assert_eq!(
        summary,
        [
            ("CreatePost", Some("req-9"), true),
            ("PublishPost", None, true),
            ("PublishPost", None, false)
        ]
    );
    assert_eq!(
        running.metrics.handled(),
        4,
        "created and published, each applied once in each of the projection's two namespaces"
    );
    server.abort();
    running.stop().await;
}
```

## Where to go from here

- [Patterns](../docs/patterns.md): every pattern used in this guide, with its
  rules.
- [Unit of work](../docs/unit-of-work.md): what commits when, and the
  guarantees.
- [Event processing](../docs/event-processing.md) and [Storage](../docs/storage.md):
  the operational reference.
