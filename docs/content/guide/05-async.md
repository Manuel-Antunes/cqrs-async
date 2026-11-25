---
title: "5. Going asynchronous"
sidebar_position: 15
---

Inline processing makes every command wait for every handler, and loses a
failed handler's work. A real application commits the command and returns;
the read model and the notifications follow on their own, each tracking how
far it got, retrying what fails, and never applying an event twice. That is
a tracking processor.

## From the events to the unit

The asynchronous unit starts from the post events, `Events`, which it
builds everything else on:

```rust
// src/config.rs
pub fn start_in_memory()
-> Running<impl PostCommands, impl PostQueries + PostSubscriptions, InMemoryPostSummaryRepository, PostEvents> {
    let store = PostStore::default();
    let summaries = Arc::new(InMemoryPostSummaryRepository::default());
    let notifier = Arc::new(InMemoryNotifier::default());
    // ...the audit log and the metrics of part 7

    let projection = PostProjection::new(Arc::clone(&summaries), Arc::clone(&notifier));
    let app = Cqrs::new(Events::new(store.clone()))
        .commands(|events| PostCommandGateway::new(posts_over(&store), events))
        .queries(PostQueryGateway::new(Arc::clone(&summaries)))
        .projection(projection)
        .build();
    // ...
}
```

(The real function also wraps the gateway and the projection in
interceptors: part 7.)

- **`Events::new(store)`** is everything asynchronous about the post
  events: the store, a live broadcast of what is committed, and where the
  processors keep their positions and the transactions they run in (in
  memory and none, by default; part 8 gets all of them from PostgreSQL
  with `Postgres::new(pool).events::<PostEvent>("posts")`). Cloning it is
  cheap and shares all of it.
- **The events are also the publisher.** A command's unit of work appends
  to the store, commits, then broadcasts. `.commands(|events| ..)` hands
  them to the closure that builds the gateway, as in part 4.
- **The repository and the gateways are the same as in part 4**; only what
  the unit starts from changed.
- **`.projection(p)`** starts a `TrackingProcessor` per namespace, named
  after it: `"post.summaries"` and `"post.notifications"`, each with its
  own token.

## Tracking processors

A processor:

1. reads the store from its stored position (its **token**), then follows
   the live broadcast, falling back to the store whenever it misses
   something;
2. runs events in a **unit of work**: begin, `handle`, store the token,
   commit, then `committed`. The summaries take up to 64 waiting events
   per unit of work (their `batch = 64`); the notifications one;
3. on failure, rolls back and retries with backoff, holding back only the
   failing post's segment.

The namespaces are independent: a notifier outage does not hold the
summaries back, and each catches up from its own token once things work
again.

### The unit of work

With the in-memory token store, the unit of work is `()`: there is no
transaction, so "handled" and "token stored" are two steps, and a crash
loses both together (the read model is in memory too). With PostgreSQL
(part 8) the unit of work is a real transaction, and the summaries are
written in it with the token: each event applies exactly once. A side
effect outside the transaction (the notification) is at least once. See
[Unit of work](../docs/unit-of-work.md) for the full contract.

## Waiting for the read model

Commands now return before the projection ran. `PostEvents` names the
unit's events:

```rust
// src/config.rs
pub type PostEvents = Events<PostEvent, PostStore>;

pub struct Running<C, Q, R, E> {
    pub app: Arc<Cqrs<C, Q, E>>,
    pub summaries: Arc<R>,
    pub notifier: Arc<InMemoryNotifier>,
    pub audit: Arc<AuditLog>,
    pub metrics: Arc<ProjectionMetrics>,
}
```

The unit knows its processors: `app.settled()` waits until every one of
them handled every event committed so far, and `app.stop()` stops them,
each after its current event. `Running` forwards both.

```rust
use blog::application::{commands::{CreatePost, PublishPost}, queries::PostSummaryRepository};
use cqrs::command::CommandGateway;

#[tokio::test]
async fn the_read_model_follows_the_commands() {
    let running = blog::start_in_memory();
    let id = running.app.commands().send(CreatePost { title: "Async".into() }).await.unwrap();
    running.app.commands().send(PublishPost { id }).await.unwrap();

    running.settled().await;
    assert!(running.summaries.find(id).await.unwrap().unwrap().published);
    assert_eq!(running.notifier.sent(), ["post-1 published"]);
    running.stop().await;
}
```

An application with several aggregate roots has a unit per root; a tuple
of units waits and stops together: `(posts, users).settled().await`
(`cqrs::Units`).

Part 6 removes most of this waiting: subscription queries wait for the
read model themselves.

## Replaying a read model

To rebuild the summaries from scratch (a bug fixed in the projection, a
new column), stop the processors, reset the projection, and start it
again:

```rust
let events = running.app.events().clone();
let (summaries, notifier) = (Arc::clone(&running.summaries), Arc::clone(&running.notifier));
running.stop().await;   // every processor of the unit

let projection = PostProjection::new(summaries, notifier);
events.projection(projection).reset().await?;   // tokens back to 0, then `reset`: clear()
// then build the unit again over `events`: the summaries replay
```

Resetting moves every namespace's tokens back to the start and calls each
handler's `reset`. The events stored so far are then a **replay**
(`context.replay()` is `true`):

- `summaries` sees them, and rebuilds;
- `notifications` has `REPLAY = false`, so it skips them: nobody is
  notified twice. It picks up again with the first new event.

Resetting fails with `ResetError::Running` while any instance still runs
the processors.

## Scaling a processor

```rust
Cqrs::new(Events::new(store.clone()))
    // ...
    .options(TrackingOptions::default().segments(8))
    .projection(projection)
    // ...
```

`.options` applies to the processors started after it. The log is split
into 8 segments by post: posts are projected in parallel, each post's
events in order. `TrackingOptions::default().batch_size(n)` sets the batch
of every namespace that does not set its own; `batch = 64` on the
summaries wins over it.

A batch never waits to fill: a worker takes what is already queued for its
segment, so it adds no latency under light load. With a shared token store
(PostgreSQL), several instances of the application split the segments
between them and take over a dead instance's segments when its claims
expire. See [Event processing](../docs/event-processing.md).

## When an event keeps failing

A failing event is retried with exponential backoff
(`retry_initial`, `retry_max` in `TrackingOptions`), holding back only its
segment; the other namespace and the other segments go on. By default the
namespaces of a projection retry for as long as it takes, which is what a
read model needs: skipping an event would leave it wrong.

A side effect may rather park an event its target keeps refusing, so the
rest goes on. Give the projection a dead-letter queue when it is wired:

```rust
let parked = InMemoryDeadLetters::default(); // PostgresDeadLetters on PostgreSQL
let app = Cqrs::new(Events::new(store.clone()))
    // ...
    .projection_with(projection, |processors| processors.with_dead_letters(parked.clone(), 5))
    .build();
```

After five failed attempts the event is parked, under its namespace's
processor (`"post.notifications"`), with the later events of the same post
behind it, so a post's order is kept; other posts flow. Once the cause is
fixed, `events.projection(..).with_dead_letters(parked, 5).retry_dead_letters()`
handles them, namespace by namespace. See
[Event processing](../docs/event-processing.md#dead-letters).

Next: [6. Live Queries and Subscriptions](06-live-queries.md).
