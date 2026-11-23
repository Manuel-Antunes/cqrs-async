# 5. Going asynchronous

Inline processing makes every command wait for every handler, and loses a
failed handler's work. A real application commits the command and returns;
read models and policies follow on their own, each tracking how far it got,
retrying what fails, and never applying an event twice. That is a tracking
processor.

## From the stores to the unit

The asynchronous unit is built from the event store and the token store;
it makes the rest:

```rust
// src/config.rs
pub fn start() -> Running<impl PostCommands, impl PostQueries> {
    let store = PostStore::default();
    let posts = Arc::new(EventSourcedPostRepository::new(Arc::clone(&store)));
    let summaries = Arc::new(InMemoryPostSummaryRepository::default());
    let notifier = Arc::new(InMemoryNotifier::default());

    let app = Cqrs::new(store, Arc::new(InMemoryTokenStore::default()))
        .commands(PostCommandGateway::new(posts))
        .queries(PostQueryGateway::new(Arc::clone(&summaries)))
        .projection(PostSummaryProjection::new(Arc::clone(&summaries)))
        .policy(NotifyOnPublish::new(Arc::clone(&notifier)))
        .build();
    Running { app: Arc::new(app), summaries, notifier }
}
```

- **`Cqrs::new(store, tokens)`** takes both stores: where events live and
  where processors keep their positions. Both are the constructor's, so a
  unit cannot be built without deciding where tokens live.
- **The unit makes its events** (`Events`): the store, a live broadcast of
  committed events, and the token store. They are also the **publisher**:
  a commit appends to the store, then broadcasts. `.commands(gateway)`
  gives that publisher to the gateway.
- **The repository and the gateways are the same as in part 4**; only the
  stores handed to the unit changed.
- **`.projection(p)` and `.policy(p)`** each start a `TrackingProcessor`
  named after the handler's namespace.

## Tracking processors

A processor:

1. reads the store from its stored position (its **token**), then follows
   the live broadcast, falling back to the store whenever it misses
   something;
2. runs each event in a **unit of work**: begin, `handle`, store the token,
   commit, then `committed`;
3. on failure, rolls back and retries the event with backoff, holding back
   only that aggregate's segment.

The processors are independent: a notifier outage does not hold the read
model back, and each has its own token. The difference between the two
methods shows in part 6: subscriptions wait for projections, not for
policies.

### The unit of work

With the in-memory token store, the unit of work is `()`: there is no
transaction, so "handled" and "token stored" are two steps, and a crash
loses both together (the read model is in memory too). With PostgreSQL
(part 8) the unit of work is a real transaction, and a projection writing
SQL commits its rows with the token: each event applies exactly once. See
[Unit of work](../unit-of-work.md) for the full contract.

## Waiting for the read model

Commands now return before the projection ran. `Running` names the unit's
events, the type `Cqrs::new` made from the stores:

```rust
pub type PostEvents = Events<PostEvent, PostStore, Arc<InMemoryTokenStore>>;
```

The unit knows its processors: `settled()` waits until every one of them handled every event
committed so far, and `stop()` stops them.

```rust
pub struct Running<C, Q> {
    pub app: Arc<Cqrs<PostEvents, C, Q, PostEvents>>,
    pub summaries: Arc<InMemoryPostSummaryRepository>,
    pub notifier: Arc<InMemoryNotifier>,
}

impl<C, Q> Running<C, Q> {
    pub async fn settled(&self) { self.app.settled().await }
    pub async fn stop(self) { self.app.stop().await }
}
```

```rust
#[tokio::test]
async fn the_read_model_follows_the_commands() {
    let running = blog::start();
    let id = running.app.commands().send(CreatePost { title: "Async".into() }).await.unwrap();
    running.app.commands().send(PublishPost { id }).await.unwrap();

    running.settled().await;
    assert!(running.summaries.find(id).await.unwrap().unwrap().published);
    assert_eq!(running.notifier.sent(), ["post-1 published"]);
    running.stop().await;
}
```

Part 6 removes most of this waiting: subscription queries wait for the
projections themselves.

## Replaying a read model

To rebuild the summaries from scratch (a bug fix in the projection, a new
column), stop the processor, reset it, and start it again:

```rust
running.stop().await;                     // every processor of the unit
running.app.events().projection(PostSummaryProjection::new(Arc::clone(&summaries))).reset().await?;   // tokens to 0, then reset(): clear()
// then build the unit again: its projection replays
```

During the replay, events stored before the reset are a replay:
`context.replay()` is `true`, and `NotifyOnPublish` (`REPLAY = false`)
would skip them if it shared the processor. Resetting fails with
`ResetError::Running` while any instance still runs the processor.

## Scaling a processor

```rust
Cqrs::new(store, tokens)
    // ...
    .options(TrackingOptions { segments: 8, batch_size: 100, ..TrackingOptions::default() })
    .projection(PostSummaryProjection::new(Arc::clone(&summaries)))
    // ...
```

`.options` applies to the processors started after it. The log is split
into 8 segments by post; posts are projected in
parallel, each post's events in order. `batch_size: 100` handles the
events waiting for a segment in one unit of work: with PostgreSQL, one
commit for up to 100 events instead of one each. It never waits to fill a
batch, so it costs nothing under light load. With a shared token store
(PostgreSQL), several instances of the application split the segments
between them and take over a dead instance's segments when its claims
expire. See [Event processing](../event-processing.md).

## Parking what keeps failing

By default a failing event is retried forever, holding back its segment.
To park it instead after a few attempts, configure the processor itself
(a processor started outside the unit is yours to stop):

```rust
let events = running.app.events();               // the events the unit's builder made
let parked = Arc::new(InMemoryDeadLetters::default());
let notifications = events
    .projection(NotifyOnPublish::new(Arc::clone(&notifier)))
    .with_dead_letters(Arc::clone(&parked), 5)
    .spawn();

// later, once the mail server is back:
let outcome = events
    .projection(NotifyOnPublish::new(Arc::clone(&notifier)))
    .with_dead_letters(Arc::clone(&parked), 5)
    .retry_dead_letters()
    .await?;
```

The parked event's later events (same post) are parked behind it, so a
post's order is kept; other posts flow.

Next: [6. Live queries](06-live-queries.md).
