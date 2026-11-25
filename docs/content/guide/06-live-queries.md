---
title: "6. Live Queries and Subscriptions"
sidebar_position: 16
---

One mechanism serves two kinds of clients:

- **streamed results of many values**: a query whose answer keeps
  arriving (an SSE stream, a live query that re-renders a list);
- **real-time events**: a client told as things happen (a GraphQL
  subscription, a WebSocket).

Both subscribe to the same query a client would ask once: `FindPost { id }`
gives the summary now, then every new version. Axon calls these
subscription queries: the query's current result, then the updates the
projection emits once it handled each change.

## The projection lists its queries

Updates come from the projection that changes the results, so it is the
projection that declares them (part 4):

```rust
// src/application/projections/post/mod.rs
#[cqrs::projection(
    name = "post",
    events = PostEvent,
    namespaces("summaries", "notifications"),
    updates(FindPost, ListPublished),
    subscriptions = PostSubscriptions
)]
pub struct PostProjection<S, N> { /* ... */ }
```

- **`updates(FindPost, ListPublished)`** gives the projection one channel
  per query. Together they are `QueryUpdateEmitter<Self>`; the struct gets
  no new fields.
- **`subscriptions = PostSubscriptions`** generates the trait of whatever
  can subscribe to all of them:
  `QuerySubscriptions<FindPost> + QuerySubscriptions<ListPublished>`. The
  queries module re-exports it, so
  callers find it next to `PostQueries`.

## Emitting

A namespace emits in `updated`, next to its `handle`:

```rust
// src/application/projections/post/summaries.rs
#[cqrs::event_handler(namespace = "summaries", batch = 64)]
impl<S, N, Tx> EventHandler for PostProjection<S, N>
where
    S: PostSummaryRepository + PostSummaryStore<Tx>,
    N: Send + Sync,
    Tx: Send,
{
    // handle and reset: part 4

    /// Pushes the summary queries' new results, on every instance, once
    /// this namespace handled the event (whichever instance did): the
    /// post's new summary, and the new list of published posts if it
    /// changed.
    async fn updated(&self, event: &EventEnvelope<PostEvent>, updates: &QueryUpdateEmitter<Self>) {
        let Ok(id) = post_of(event) else { return };
        if let Ok(Some(summary)) = self.summaries.find(id).await {
            updates.emit(|query: &FindPost| query.id == id, Some(summary));
        }
        if matches!(event.payload, PostEvent::Published)
            && let Ok(published) = self.summaries.published().await
        {
            updates.emit(|_: &ListPublished| true, published);
        }
    }
}
```

`updates.emit(filter, result)` sends `result` to the subscriptions whose
query `filter` accepts. The type of the filter's argument picks the
channel: `|query: &FindPost| ..` reaches `FindPost` subscribers, and the
result must be `FindPost`'s output. A query missing from `updates(..)`
does not compile.

**When it runs.** `updated` runs after the namespace committed the event,
outside its unit of work, so a subscriber never sees a change that was
rolled back. It runs on **every instance** of the application, once the
namespace handled the event, whichever instance did: with several
instances sharing the segments, a subscriber connected to any of them sees
every change, where emitting from the handler (as Axon does) would reach
only the subscribers of the instance that handled the event. It does not
run for replayed events.

It reads the read model rather than computing the result from the event:
the summary is already up to date, and the query and its updates then
cannot disagree.

## Subscribing

Nothing else to wire. `.projection(..)` (part 5) creates the channels,
starts one emitter per namespace that has an `updated`, and makes the
unit's queries subscribable to the projection's `updates(..)`.
`app.queries()` still answers plain queries, and now also implements
`PostSubscriptions`, which `start_in_memory` returns as
`impl PostQueries + PostSubscriptions`.

```rust
use cqrs::query::QuerySubscriptions;
use futures_util::StreamExt;

let mut post = std::pin::pin!(running.app.queries().subscribe(FindPost { id }));  // a Stream of Result<Option<PostSummary>, BlogError>
let now = post.next().await;                                                       // the current result
let next = post.next().await;                                                      // after the next change to this post
```

- **The first result waits for the read model.** It is computed once the
  namespaces that see replays (the read models: `summaries`, not
  `notifications`) handled every event committed when the subscription
  started. A client subscribing right after its own command sees it.
- **Equal results are not repeated.** The subscription listens before the
  first result is computed, so no update is lost in between; one equal to
  the result before it is dropped.
- **A slow subscriber is dropped.** One more than 256 updates behind is
  forgotten: its stream ends, and the client subscribes again.

The inline unit of part 4 (`in_memory`) has no subscriptions: there is no
runtime to push from, and `updated` does not run inline.

Part 9 serves these streams as GraphQL subscriptions (`post`,
`publishedPosts`), each one a `subscribe` on the unit's queries.

Next: [7. Interceptors](07-interceptors.md).
