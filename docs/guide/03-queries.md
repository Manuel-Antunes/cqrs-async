# 3. Queries and the read model

Queries never read the event store. They read a **read model**, a shape
made for the screens that need it, kept up to date from the events (part
4). Here we define the read model, the port it is stored behind, and the
queries.

## The read model and its port

```rust
// src/application/queries/post_summary.rs
use std::future::Future;
use cqrs::StoreError;
use crate::domain::PostId;

/// A post as the read side shows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostSummary {
    pub id: PostId,
    pub title: String,
    pub published: bool,
}

/// The read model's repository: queries read from it, the projection writes
/// to it. Fed by committed events, so eventually consistent with the posts.
pub trait PostSummaryRepository: Send + Sync {
    fn find(&self, id: PostId) -> impl Future<Output = Result<Option<PostSummary>, StoreError>> + Send;

    /// Published posts, ordered by id.
    fn published(&self) -> impl Future<Output = Result<Vec<PostSummary>, StoreError>> + Send;

    fn save(&self, summary: PostSummary) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Applies `change` to the summary of `id` in place. `false`: there is none.
    fn update(
        &self,
        id: PostId,
        change: impl FnOnce(&mut PostSummary) + Send,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Removes every summary, before a replay rebuilds them.
    fn clear(&self) -> impl Future<Output = Result<(), StoreError>> + Send;
}
```

The port is in the **application** layer: what the read side needs is
application logic; where it lives is an adapter's business. Two notes on
its shape:

- It is not the write side's repository. `PostRepository` finds an
  aggregate rebuilt from its events; this one stores rows. They have
  different storage, consistency and callers, so they stay different
  types.
- `update` takes a closure (Axon's managed entity): the adapter applies the
  change in place, so a projection never does find, clone, save.

## The in-memory adapter

```rust
// src/infrastructure/post_summary_repository.rs
use std::{collections::BTreeMap, sync::Mutex};

#[derive(Default)]
pub struct InMemoryPostSummaryRepository {
    rows: Mutex<BTreeMap<PostId, PostSummary>>,   // by id: listings come out sorted
}

impl PostSummaryRepository for InMemoryPostSummaryRepository {
    async fn find(&self, id: PostId) -> Result<Option<PostSummary>, StoreError> {
        Ok(self.rows.lock().unwrap().get(&id).cloned())
    }

    async fn published(&self) -> Result<Vec<PostSummary>, StoreError> {
        Ok(self.rows.lock().unwrap().values().filter(|summary| summary.published).cloned().collect())
    }

    async fn save(&self, summary: PostSummary) -> Result<(), StoreError> {
        self.rows.lock().unwrap().insert(summary.id, summary);
        Ok(())
    }

    async fn update(&self, id: PostId, change: impl FnOnce(&mut PostSummary) + Send) -> Result<bool, StoreError> {
        Ok(self.rows.lock().unwrap().get_mut(&id).map(change).is_some())
    }

    async fn clear(&self) -> Result<(), StoreError> {
        self.rows.lock().unwrap().clear();
        Ok(())
    }
}
```

## The queries

```rust
// src/application/queries/find_post.rs
use std::sync::Arc;
use cqrs::Handler;

/// One post, published or not; `None` if it does not exist (yet).
#[derive(Clone, Debug, cqrs::Query)]
#[query(output = Option<PostSummary>, error = BlogError)]
pub struct FindPost {
    pub id: PostId,
}

pub struct FindPostHandler<M> {
    summaries: Arc<M>,
}

impl<M> FindPostHandler<M> {
    pub fn new(summaries: Arc<M>) -> Self {
        Self { summaries }
    }
}

#[cqrs::query_handler]
impl<M: PostSummaryRepository> Handler for FindPostHandler<M> {
    async fn handle(&self, query: FindPost) -> Result<Option<PostSummary>, BlogError> {
        Ok(self.summaries.find(query.id).await?)
    }
}
```

`ListPublished` is the same, with `#[query(output = Vec<PostSummary>, error = BlogError)]`
on a unit struct and `self.summaries.published()`.

Query handlers implement the plain `Handler` trait (`#[cqrs::query_handler]`
fills in `type Message`). The query is taken by value: queries are not
retried.

## The query gateway

```rust
// src/application/queries/mod.rs
#[derive(cqrs::Gateway)]
pub struct PostQueryGateway<M> {
    find: FindPostHandler<M>,
    list_published: ListPublishedHandler<M>,
}

impl<M> PostQueryGateway<M> {
    pub fn new(summaries: Arc<M>) -> Self {
        Self {
            find: FindPostHandler::new(Arc::clone(&summaries)),
            list_published: ListPublishedHandler::new(summaries),
        }
    }
}
```

`gateway.query(FindPost { id })` returns `Result<Option<PostSummary>, BlogError>`.
As with commands, `#[derive(Queries)]` on an enum plus
`#[gateway(queries = PostQuery)]` accepts any query as one message.

## Ports for whoever uses them

Commands and queries stay on separate gateways (separate buses, as in
Axon and NestJS). Code that uses them depends on what they route, not on
the gateway types. The message enums generate those ports:

```rust
#[derive(Debug, serde::Serialize, serde::Deserialize, cqrs::Commands)]
#[commands(error = BlogError, port = PostCommands, derive(Debug, PartialEq))]
pub enum PostCommand { CreatePost(CreatePost), RetitlePost(RetitlePost), PublishPost(PublishPost) }

#[derive(Clone, Debug, cqrs::Queries)]
#[queries(error = BlogError, port = PostQueries, subscriptions = PostSubscriptions, derive(Debug, PartialEq))]
pub enum PostQuery { FindPost(FindPost), ListPublished(ListPublished) }
```

`port = PostCommands` generates a trait bounded by
`CommandGateway<CreatePost> + CommandGateway<RetitlePost> + CommandGateway<PublishPost> + CommandGateway<PostCommand>`,
implemented by every type that routes them all: the gateway, the gateway
behind interceptors, a test double. `PostQueries` does the same for the
queries, and `PostSubscriptions` (used in part 6) for subscribing to them.
`impl PostCommands` is then all the code that sends commands has to name.

At this point commands store events and queries read summaries, but
nothing writes the summaries. That is the projection.

Next: [4. The projection](04-projection.md).
