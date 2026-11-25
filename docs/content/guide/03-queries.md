---
title: "3. Queries and the read model"
sidebar_position: 13
---

Queries never read the event store. They read a **read model**, a shape
made for the screens that need it, kept up to date from the events by a
projection (part 4). Here we define the read model, the ports it is kept
behind, the queries and their gateway.

## The read model and its ports

```rust
// src/application/queries/post_summary.rs
use std::future::Future;

use cqrs::store::StoreError;

use crate::domain::PostId;

/// A post as the read side shows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostSummary {
    pub id: PostId,
    pub title: String,
    pub published: bool,
}

/// The read model, as the queries read it. It is fed by committed events,
/// so it is eventually consistent with the write side.
pub trait PostSummaryRepository: Send + Sync {
    /// The summary of one post, or `None` if the read model has not seen it
    /// (it may not exist, or its creation may not be projected yet).
    fn find(&self, id: PostId) -> impl Future<Output = Result<Option<PostSummary>, StoreError>> + Send;

    /// Published posts, ordered by id.
    fn published(&self) -> impl Future<Output = Result<Vec<PostSummary>, StoreError>> + Send;
}

/// The read model, as the projection writes it: in the unit of work `Tx`
/// the processor hands it with each event, so a read model in the same
/// database as the processor's tokens commits each change with its token
/// (an event is applied once). In memory, `Tx` is `()`.
pub trait PostSummaryStore<Tx>: Send + Sync {
    /// Stores a new post's `summary`.
    fn save(&self, tx: &mut Tx, summary: PostSummary) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Changes the summary of `id` with `change`; `false` if there is none.
    fn update(
        &self,
        tx: &mut Tx,
        id: PostId,
        change: impl FnOnce(&mut PostSummary) + Send,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Removes every summary, before a replay rebuilds them.
    fn clear(&self, tx: &mut Tx) -> impl Future<Output = Result<(), StoreError>> + Send;
}
```

The ports are in the **application** layer: what the read side needs is
application logic; where it lives is an adapter's business. Why this
shape:

- **Two ports, two callers.** Queries only read, outside any transaction.
  The projection only writes, in the unit of work of the processor that
  runs it (part 4): on PostgreSQL, that is the transaction that also saves
  the processor's progress, so a change and its token commit together
  (part 8).
- **Not the write side's repository.** The posts' `PostRepository` finds an
  aggregate to decide on; this one stores rows shaped for reading. They
  differ in storage, consistency and callers, so they stay different
  types.
- **`update` takes a closure** (Axon's managed entity): the adapter applies
  the change in place, so a projection never does find, clone, save.

## The in-memory adapter

```rust
// src/infrastructure/post_summary_repository.rs
use std::{collections::BTreeMap, sync::Mutex};

use cqrs::store::StoreError;

/// The read model in memory: one row per post. Its unit of work is `()`:
/// there is nothing to roll back.
#[derive(Default)]
pub struct InMemoryPostSummaryRepository {
    /// Rows by id (so listings come out sorted).
    rows: Mutex<BTreeMap<PostId, PostSummary>>,
}

impl PostSummaryRepository for InMemoryPostSummaryRepository {
    async fn find(&self, id: PostId) -> Result<Option<PostSummary>, StoreError> {
        Ok(self.rows().get(&id).cloned())
    }

    async fn published(&self) -> Result<Vec<PostSummary>, StoreError> {
        Ok(self.rows().values().filter(|summary| summary.published).cloned().collect())
    }
}

impl PostSummaryStore<()> for InMemoryPostSummaryRepository {
    async fn save(&self, (): &mut (), summary: PostSummary) -> Result<(), StoreError> {
        self.rows().insert(summary.id, summary);
        Ok(())
    }

    async fn update(
        &self,
        (): &mut (),
        id: PostId,
        change: impl FnOnce(&mut PostSummary) + Send,
    ) -> Result<bool, StoreError> {
        Ok(self.rows().get_mut(&id).map(change).is_some())
    }

    async fn clear(&self, (): &mut ()) -> Result<(), StoreError> {
        self.rows().clear();
        Ok(())
    }
}
```

(`rows()` locks the map.) On PostgreSQL, `SeaOrmPostSummaries` implements
the same two ports (part 8).

## The query gateway

```rust
// src/application/queries/mod.rs
#[cqrs::query_gateway(queries = PostQueries, handles(FindPost, ListPublished))]
pub struct PostQueryGateway<M> {
    /// The read model, shared with the projection that writes it.
    summaries: Arc<M>,
}

impl<M> PostQueryGateway<M> {
    pub fn new(summaries: Arc<M>) -> Self {
        Self { summaries }
    }
}
```

The query counterpart of the command gateway (part 2): the gateway is the
handler, with one `QueryHandler<Q>` impl per query, sharing the read
model. A query listed in `handles(..)` without its impl does not compile.
There is no unit of work: a query writes nothing.

`queries = PostQueries` generates the trait
`QueryGateway<FindPost> + QueryGateway<ListPublished>`, implemented by
whatever answers both. Subscribing to the queries is a different trait,
`PostSubscriptions`, generated by the projection that pushes their updates
(part 6).

## The queries

```rust
// src/application/queries/find_post.rs
use cqrs::query::QueryHandler;

use super::{PostQueryGateway, PostSummary, PostSummaryRepository};
use crate::{application::BlogError, domain::PostId};

/// One post, published or not; `None` if it does not exist (yet).
#[cqrs::query(output = Option<PostSummary>, error = BlogError)]
#[derive(Clone, Debug)]
pub struct FindPost {
    pub id: PostId,
}

/// Answers from the read model.
#[cqrs::query_handler]
impl<M: PostSummaryRepository> QueryHandler<FindPost> for PostQueryGateway<M> {
    async fn handle(&self, query: FindPost) -> Result<Option<PostSummary>, BlogError> {
        Ok(self.summaries.find(query.id).await?)
    }
}
```

```rust
// src/application/queries/list_published.rs
/// Every published post, ordered by id.
#[cqrs::query(output = Vec<PostSummary>, error = BlogError)]
#[derive(Clone, Debug)]
pub struct ListPublished;

#[cqrs::query_handler]
impl<M: PostSummaryRepository> QueryHandler<ListPublished> for PostQueryGateway<M> {
    async fn handle(&self, _: ListPublished) -> Result<Vec<PostSummary>, BlogError> {
        Ok(self.summaries.published().await?)
    }
}
```

- `#[cqrs::query(output = T, error = E)]` makes the struct a `Query`. A
  query and a command are different marker traits, so a query can never be
  sent as a command, or the other way around: a compile error.
- The handler bounds only what it uses (`M: PostSummaryRepository`), never
  the projection's `PostSummaryStore`: a query cannot write.
- The query is taken by value: queries are never rerun.
- `#[cqrs::query_handler]` marks the gateway as answering the query, for
  `handles(..)` (and fills in `Q` if the impl leaves it out).

Asking is `query`:

```rust
let found: Option<PostSummary> = gateway.query(FindPost { id }).await?;
```

At this point commands store events and queries read summaries, but
nothing writes the summaries. That is the projection.

Next: [4. The projection](04-projection.md).
