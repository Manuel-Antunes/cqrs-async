# 6. Live queries

Clients want to see a post change without polling: subscribe to
`FindPost { id }` and receive the summary now, then every new version.
Axon calls these subscription queries: the query's current result, then
the updates the projection emits.

## Where updates are emitted

One emitter per query type, gathered in a struct the projections share:

```rust
// src/application/queries/mod.rs
#[derive(Default, cqrs::QueryUpdates)]
pub struct PostUpdates {
    /// New summaries of one post, for FindPost subscribers.
    pub find: cqrs::QueryUpdateEmitter<FindPost>,
    /// The new list of published posts, for ListPublished subscribers.
    pub published: cqrs::QueryUpdateEmitter<ListPublished>,
}
```

`QueryUpdateEmitter<Q>` pushes values of `Q::Output` to the subscriptions
of `Q` that match a filter. `#[derive(QueryUpdates)]` tells the
subscription side which field serves which query.

## Emitting after the commit

The projection gets the emitters and pushes in its `committed` step, which
runs only once the unit of work committed:

```rust
pub struct PostSummaryProjection<S> {
    summaries: Arc<S>,
    updates: Arc<PostUpdates>,
}

#[cqrs::projection(namespace = "post-summaries")]
impl<S: PostSummaryRepository> EventHandler for PostSummaryProjection<S> {
    async fn handle(..) -> Result<(), HandlerError> { /* as in part 4 */ }

    /// Once committed: the post's new summary, and the new list of
    /// published posts if it changed.
    async fn committed(&self, event: &EventEnvelope<PostEvent>) {
        let Ok(id) = post_of(event) else { return };
        if let Ok(Some(summary)) = self.summaries.find(id).await {
            self.updates.find.emit(|query| query.id == id, Some(summary));
        }
        if matches!(event.payload, PostEvent::Published)
            && let Ok(published) = self.summaries.published().await
        {
            self.updates.published.emit(|_| true, published);
        }
    }

    async fn reset(..) -> Result<(), HandlerError> { /* as in part 4 */ }
}
```

Emitting from `handle` would push a change that a rollback could undo.
`committed` cannot fail and never runs for a rolled-back attempt.

## Subscribing

The unit makes its queries subscribable with `.subscriptions(updates)`,
the same emitters the projection got:

```rust
// src/config.rs, in start()
let updates = Arc::new(PostUpdates::default());
let projection = PostSummaryProjection::new(Arc::clone(&summaries), Arc::clone(&updates));

let app = Cqrs::new(store, Arc::new(InMemoryTokenStore::default()))
    .commands(|events| PostCommandGateway::new(posts, events))
    .queries(PostQueryGateway::new(Arc::clone(&summaries)))
    .projection(projection)
    .policy(NotifyOnPublish::new(Arc::clone(&notifier)))
    .subscriptions(updates)
    .build();
```

`app.queries()` still answers plain queries, and now also implements
`QuerySubscriptions` for each query whose emitter is in `PostUpdates`:
the `PostSubscriptions` port from part 3. `start` returns it as
`impl PostQueries + PostSubscriptions`.

```rust
use cqrs::QuerySubscriptions;
use futures_util::StreamExt;

let mut post = running.app.queries().subscribe(FindPost { id });   // a Stream of Result<Option<PostSummary>, BlogError>
let now = post.next().await;                                       // the current result
let next = post.next().await;                                      // after the next change to this post
```

The initial result waits until every **projection** of the unit (not the
policies) handled every event committed when the subscription started, so
a client subscribing right after its own command sees it. Without that,
the first result could predate the command. Under the hood it is
`SubscriptionQueries::new(queries, updates).after_projection(events, progress)`,
which the builder assembles at `build()` with the progress of every
`.projection` it started.

## Narrowing what refreshes a query

Queries declare which events can change their result. `LiveQueries`, the
alternative to emitters that re-runs the query after the projection
handled a relevant event, uses it; it also documents the query:

```rust
impl RefreshedBy<PostEvent> for FindPost {
    fn refreshed_by(&self, event: &EventEnvelope<PostEvent>) -> bool {
        event.stream.id() == self.id.to_string()
    }
}

impl RefreshedBy<PostEvent> for ListPublished {
    /// A new draft is not published.
    fn refreshed_by(&self, event: &EventEnvelope<PostEvent>) -> bool {
        !matches!(event.payload, PostEvent::Created { .. })
    }
}
```

With several instances projecting different segments, an update is
emitted only by the instance that projected the event. Use `LiveQueries`
with `app.events().global_progress("post-summaries")` in that case: see
[Event processing § Live results](../event-processing.md#live-results-for-queries).

Next: [7. Interceptors](07-interceptors.md).
