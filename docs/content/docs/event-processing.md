---
title: "Event processing"
sidebar_position: 50
---

Everything asynchronous about one event type lives behind one value,
`cqrs::processing::Events<E, S, C, M>` (feature `tokio`): the store `S`, a
live broadcast of committed events, the token store `C`, the transaction
manager `M` the processors' units of work come from, and the subscription
settings. Clones share everything.

```rust
// In memory: in-memory tokens, units of work `()`.
let events = Events::new(InMemoryEventStore::<PostEvent>::new());
// PostgreSQL: the log "posts", tokens and transactions in the database.
let events = cqrs_postgres::Postgres::new(pool).events::<PostEvent>("posts");
```

The same value is three things:

- the **unit of work** command gateways run in (see [Unit of
  work](unit-of-work.md)): it appends, commits, then broadcasts;
- the factory of **tracking processors**: `events.projection(p)` (one per
  namespace), `events.processor(name, handler)`, `events.track(name,
  handler)`;
- an **event feed**: `events.subscribe(after)`, the log as a `Stream`.

An application usually never calls the processors' side by hand:
`Cqrs::new(events)...projection(p).build()` starts the projection's
processors and keeps their handles (`unit.stop()`, `unit.settled()`).

## Namespaces, each with its processor

An aggregate root has one projection: a struct holding what its handlers
need, with one event handler per namespace. Each namespace runs in a
processor of its own, named `"{projection}.{namespace}"`, with its own
tokens, its own units of work, its own failures and its own replays:

```rust
// src/application/projections/post/mod.rs
#[cqrs::projection(
    name = "post",
    events = PostEvent,
    namespaces("summaries", "notifications"),
    updates(FindPost, ListPublished),
    subscriptions = PostSubscriptions
)]
pub struct PostProjection<S, N> {
    /// Where the summaries are kept.
    summaries: Arc<S>,
    /// Who delivers the announcements.
    notifier: Arc<N>,
}
```

The blog's two processors are `post.summaries` (a read model, in batches
of 64) and `post.notifications` (a side effect):

```rust
// src/application/projections/post/notifications.rs
#[cqrs::event_handler(namespace = "notifications")]
impl<S: Send + Sync, N: Notifier> EventHandler for PostProjection<S, N> {
    const REPLAY: bool = false;

    /// Announces the post if `event` published it.
    async fn handle(
        &self,
        event: &EventEnvelope<PostEvent>,
        _: &mut ProcessingContext<'_>,
    ) -> Result<(), HandlerError> {
        if let PostEvent::Published = event.payload {
            self.notifier.notify(format!("{} published", event.stream)).await?;
        }
        Ok(())
    }
}
```

Why separate processors:

- a failing notification holds back only the notifications, never the
  read model the queries answer from;
- a replay rebuilds the read model without sending the notifications again
  (`const REPLAY: bool = false`);
- each namespace has the batch that fits it: `batch = N` on its
  `#[cqrs::event_handler]` sets the events per unit of work of its
  processor (default 1). A namespace whose effects are not undone with its
  transaction keeps 1, since a failed batch is handled again.

This is also where side effects go. A command handler may run again after
a lost race, so it must not send the e-mail itself; a namespace with
`REPLAY = false` sends it once the event is committed, at least once. There
is no separate kind of "policy": a reaction to events is a namespace.

The compiler checks both directions: a namespace listed without its
handler, or a handler for a namespace not listed, does not compile.

`events.projection(p)` returns the namespaces' processors to configure:

```rust
let processors = events.projection(projection)
    .with_options(TrackingOptions::default().segments(8));   // a namespace's `batch` wins
processors.reset().await?;                                    // optional: replay every namespace
let running: ProjectionHandle = processors.spawn();

running.namespace("summaries");          // Option<&ProcessorHandle>
running.read_models();                   // progress of the namespaces that replay
running.caught_up(position).await;       // every namespace handled `position`
running.stop().await;
```

Through the builder, `.options(TrackingOptions..)` before `.projection(..)`
sets the same.

## Tracking processors

A `TrackingProcessor` runs one event handler (or a tuple of them, a
processing group) over the whole log, from its stored position: Axon's
pooled streaming processor.

```rust
let processor = events
    .processor("post-audit", audit)
    .with_options(TrackingOptions::default().segments(8).batch_size(64))
    .with_dead_letters(InMemoryDeadLetters::default(), 5);
let handle: ProcessorHandle = processor.spawn();
```

The handler must fit the processors' unit of work:
`H: EventHandler<E, M::Tx>`, with tokens `C: TokenStore<Tx = M::Tx>`.

### Options

`TrackingOptions` is built from `default()`, one setter per field:

| Setter | Default | |
|---|---|---|
| `segments` | 1 | Segments the log is split into, by aggregate. Fixed by whoever initializes the token store first. |
| `max_segments` | all | At most this many per instance, so several instances share them. |
| `claim_timeout` | 5 s | A claim not renewed for this long can be taken over. |
| `claim_interval` | 1 s | How often claims are renewed and free segments looked for. |
| `retry_initial` / `retry_max` | 50 ms / 5 s | Backoff between attempts of a failing event, doubling. |
| `dead_letter_after` | `None` | Attempts before parking (set by `with_dead_letters`). `None`: retry forever. |
| `segment_buffer` | 1024 | Events the shared reader may queue per segment (backpressure). |
| `segment_stall_timeout` | 1 s | A segment whose queue stays full this long is detached and reads alone. |
| `batch_size` | 1 | Events of a segment handled in one unit of work, with one token update (Axon's `batchSize`). Takes only what is already waiting; never waits to fill. |
| `progress_poll` | 50 ms | How often an instance that does not own every segment reads the others' progress. |
| `subscription` | see below | How the store is read. |

| `SubscriptionOptions` | |
|---|---|
| `batch_size` | Events per page read from the store. |
| `poll_interval` | How long to wait for a live event before reading the store anyway. Bounds how late an event whose broadcast was lost, or that another instance committed, is delivered. |

The live feed carries only this instance's commits. When something relays
the others' (on PostgreSQL, `Postgres::listen`, see
[guide part 8](../guide/08-postgres.md#operating-it)), it calls
`Events::committed_elsewhere()`, and every subscription and processor of
those events reads the store at once instead of waiting for its poll.

### Segments, claims and instances

- **Segments.** Each event belongs to segment
  `segment_of(stream, segments)`, a stable hash of the aggregate type and
  id. Each segment is handled in order by one worker; segments run in
  parallel. Events of one aggregate are therefore always handled in order,
  and a failing aggregate holds back only its segment.
- **Shared reader.** One reader per instance reads the store once and
  routes each event to its segment's queue. A full queue is backpressure.
  A queue that stays full for `segment_stall_timeout` means the segment is
  stuck: it is detached and reads on its own, so it never holds the others
  back.
- **Claims.** An instance claims segments in the token store (up to
  `max_segments`) and renews them every `claim_interval`. A crashed
  instance stops renewing; after `claim_timeout` another one takes its
  segments over from their stored positions. PostgreSQL claims use the
  database's clock, so instances on different machines agree on expiry.
- **Tokens.** A segment's token is a [`Cursor`](storage.md#global-order-and-the-cursor),
  moved in the same unit of work as the handler's changes. When a segment
  only sees other segments' events, its token is stored lazily (every 256
  events, or after 100 ms idle).

### Batches

With a batch size above 1, a segment worker takes the events already
waiting for it, up to the batch size, and handles them in one unit of work:
one transaction, one token update, one commit. Under light load each batch
is a single event, so latency does not change; under a backlog (a rebuild,
a burst of writes) batches fill up and the cost of the commit is shared.
The blog's summaries rebuild 5000 events in 0.25 s with `batch = 64`,
against 4.6 s one by one.

A failed batch is rolled back and its events are handled one at a time,
each with the usual retries and dead letters; a batch with an event of a
parked aggregate goes one at a time from the start. Batch only a handler
whose effects are all undone with its transaction.

### Dead letters

Without dead letters, a failing event is retried forever with backoff,
holding back its segment (`status().failing` says why). With
`with_dead_letters(queue, after)`, an event that failed `after` times is
parked, keyed by its aggregate (the "sequence"); every later event of that
aggregate is parked behind it without being tried, so the aggregate's
order is kept, and the segment moves on.

```rust
let outcome = processor.retry_dead_letters().await?;   // once the cause is fixed
// outcome.handled, outcome.still_parked, outcome.evicted
```

A projection takes a queue for all its namespaces, each parking under its
own processor's name: `events.projection(p).with_dead_letters(queue, after)`,
or `.projection_with(p, |processors| processors.with_dead_letters(queue, after))`
on the `Cqrs` builder. Its `retry_dead_letters()` retries every namespace
and returns each one's outcome.

Each sequence is retried oldest first, each event in its own unit of work,
and stops at its first failure. `retry_dead_letters(queue, name,
&transactions, &handler, &policy)` does the same without a processor.

#### The enqueue policy

`with_dead_letters(queue, after)` parks after `after` failures and keeps a
parked letter until a retry handles it: the policy `ParkAfter(after)`. Any
other rule is an `EnqueuePolicy` (Axon 5's), asked after each failed attempt
of an event and at each failed retry of a parked one:

```rust
let policy = |letter: &DeadLetter<PostEvent>, cause: &HandlerError| match letter.is_parked() {
    // A refusal will not get better: skip the event, do not park it.
    false if cause.to_string().contains("422") => EnqueueDecision::Ignore,
    false if letter.attempts >= 3 => EnqueueDecision::Enqueue(Metadata::from([("target", "mailer")])),
    false => EnqueueDecision::Retry,
    // Parked: give up after ten failed retries.
    true if letter.attempts >= 10 => EnqueueDecision::Evict,
    true => EnqueueDecision::Requeue(letter.diagnostics.clone()),
};
let processor = events.processor("mailer", mailer).with_dead_letters(queue, 3).with_enqueue_policy(policy);
```

| Decision | A failing event | A parked one, failing again at a retry |
|---|---|---|
| `Retry` | tried again after a backoff | kept |
| `Enqueue(diagnostics)` | parked, its aggregate's later events behind it | kept |
| `Requeue(diagnostics)` | parked | kept, with these diagnostics and `last_touched` |
| `Evict` / `Ignore` | skipped: the segment moves past it, unhandled | removed: the sequence's next letter comes first |

A letter carries what the policy needs and keeps: `attempts`, `parked_at`,
`last_touched` and `diagnostics` (stored in `cqrs_dead_letters` on
PostgreSQL). A projection takes one policy for all its namespaces:
`processors.with_dead_letters(queue, 3).with_enqueue_policy(policy)`.

A queue goes with the units of work it can join:

| Queue | Goes with | |
|---|---|---|
| `InMemoryDeadLetters` | `Tx = ()` (in-memory tokens) | lost when the process ends, with the tokens |
| `cqrs_postgres::PostgresDeadLetters::new(store)` | `PgTransaction` | table `cqrs_dead_letters`; the event is read back from `store`'s log |
| your own | `DeadLetterQueue<E, Tx>` | |

The compiler refuses an in-memory queue with a database's tokens: the token
would move past a parked event that a restart then forgets. A letter enters
the queue in the unit of work that moves the token past its event, and
leaves it in the one that applies it, so a crash can neither lose a letter
nor apply its event twice. If the queue cannot say whether an aggregate is
parked, the processor waits and asks again rather than handle an event
ahead of the parked ones.

A queue is given to a processor made with `events.processor(..)`, or to a
projection's namespaces (`events.projection(..).with_dead_letters(..)`,
`Cqrs::projection_with`). Without one, failing events are retried until
they succeed.

### Reset and replay

```rust
// every instance stopped
events.projection(PostProjection::new(..)).reset().await?;   // tokens to 0, then each handler's reset
// then spawn (or build the unit) again: it replays
```

`reset` moves every segment's token back to the start, marks the events
stored so far as a replay, then runs the handler's `reset` in a unit of
work of its own (a read model clears its tables there). It fails with
`ResetError::Running` while any instance holds a segment. During the
replay `context.replay()` is true, and namespaces with
`const REPLAY: bool = false` skip those events, so the notifications are not
sent again.

### Running: `run` and `spawn`

`processor.run(shutdown)` is a future: it claims segments, handles their
events, and, once `shutdown` completes, finishes the events in hand and
releases its segments for other instances. The caller runs it (awaited, or
in a `JoinSet` with the rest of the application); the segments' workers run
on Tokio tasks of the current runtime.

```rust
let progress = processor.progress();
tasks.spawn(processor.run(shutdown.cancelled_owned()));
```

`processor.spawn()` does that on a Tokio task and returns a
`ProcessorHandle`:

```rust
handle.status();                    // ProcessorStatus
handle.caught_up(position).await;   // this instance's segments handled `position`
handle.stop().await;                // after the events in hand; releases the claims
handle.abort().await;               // as a crash would: claims expire, others take over
```

### Progress

`ProcessorStatus` is what an instance reports: `position` (the lowest
watermark of its segments), `failing` (the error of an event being
retried), `segments`, `cursors`, `segment_count`, and `shared_cursors` (the
stored cursor of every segment, read every `progress_poll` while this
instance does not own them all).

- `status.covers(p)`: this instance's segments saw position `p`.
- `status.handled(p)`: the processor handled `p` in any of its instances
  (from its own segments when it owns them all, from the shared cursors
  otherwise). What a reader waits for.

`ProcessorProgress::of(&handle)` waits on `handled`; `GlobalProgress`
(`events.global_progress(name)`) reads the token store alone, for a process
that does not run the processor. `unit.settled().await` waits until every
processor of a `Cqrs` unit handled everything committed before the call
(`(posts, users).settled()` for several roots).

## Subscriptions

`events.subscribe(after)` (the `EventFeed` port, or
`cqrs::store::subscribe`) is a `Stream` of every committed event with
`position > after`, each once, in position order, then the new ones as they
commit. It reads the store in pages and follows the live broadcast, going
back to the store whenever the broadcast lags or misses something, and
reads through a gap-aware `Cursor`, so appends committing out of order are
never skipped. It keeps no token: the consumer remembers its last position.

## Live results for queries

Subscription queries come from the projection: `updates(FindPost,
ListPublished)` gives it one channel per query, and a namespace emits in
`updated(&self, event, updates: &QueryUpdateEmitter<Self>)` once it handled
the event. The `Cqrs` unit runs an emitter per such namespace on every
instance, so a subscriber connected to any instance sees every change, and
`app.queries().subscribe(FindPost { id })` yields the current result, once
the read models caught up with what was committed, then the updates. See
[part 6 of the guide](../guide/06-live-queries.md).

## Inline processing

Without a runtime, an `InlinePublisher` runs the handlers in the caller's
task, right after each commit:

```rust
// src/config.rs
let projection = PostProjection::new(Arc::clone(&summaries), Arc::clone(&notifier));
let publisher = InlinePublisher::<PostEvent, _, _>::new(store.clone()).with_projection(projection);
let blog = Cqrs::from_publisher(publisher)
    .commands(|events| PostCommandGateway::new(posts_over(&store), events))
    .queries(PostQueryGateway::new(Arc::clone(&summaries)))
    .build();
```

A command returns once every handler ran (`handle`, then `committed`), so
queries see its effects at once. What is fed:

| | |
|---|---|
| `with_projection(p)` | each namespace sees the whole commit, isolated from the others; no replays inline |
| `with_processor((a, b))` | a group: members in order, a failure stopping the group |
| `with_processor(Isolated((a, b)))` | each handler on its own: one failing neither stops nor delays the other |

A failing handler skips the rest of that commit, so it never sees events out
of order. Failures never undo the commit, and there is nobody to return
them to (the events are stored): they go to the publisher's `OnFailure`
policy, `LogFailures` by default (logs at error level), or any
`Fn(&ProcessingFailure)` given to `on_failure(..)`. No tokens, no retries:
for tests and simple applications.
