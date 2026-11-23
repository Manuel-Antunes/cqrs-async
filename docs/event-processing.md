# Event processing

Everything asynchronous about events lives behind one value, `Events<E, S, C>`
(feature `tokio`): the store `S`, a live broadcast of committed events,
the token store `C`, and subscription settings. Clone it freely: clones
share everything.

```rust
let events = Events::new(Arc::new(InMemoryEventStore::<PostEvent>::new()));   // in-memory tokens
let events = Events::new(PostgresEventStore::<PostEvent>::new(pool.clone()))
    .with_tokens(PostgresTokenStore::new(pool));                              // shared tokens

let posts = events.event_sourced::<Post>();           // loads and commits posts, broadcasting each commit
let handle = events.projection(summaries).spawn();    // tracking processor named by the namespace
let handle = events.track("audit", audit_handler);    // by name, default options
let stream = events.subscribe(0);                     // EventFeed: catch-up subscription
```

## Tracking processors

A `TrackingProcessor` runs one event handler (or a tuple of them) over the
whole log, from its stored position, forever: Axon's pooled streaming
processor.

```rust
let processor = events
    .projection(Intercepted::new(projection, Measured(metrics)))   // or events.processor("name", handler)
    .with_options(TrackingOptions { segments: 8, ..TrackingOptions::default() })
    .with_dead_letters(Arc::new(InMemoryDeadLetters::default()), 5);

processor.reset().await?;                  // optional: replay from the start
let handle: ProcessorHandle = processor.spawn();

handle.status();                           // ProcessorStatus: position, failing, segments, cursors
handle.caught_up(position).await;          // waits until this instance handled up to `position`
handle.stop().await;                       // after the current event; releases the claims
```

The handler type must fit the token store's unit of work:
`H: EventHandler<E, C::Transaction>`. See [Unit of work](unit-of-work.md).

### Options

| `TrackingOptions` | Default | |
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
| `subscription` | see below | How the store is read. |

| `SubscriptionOptions` | |
|---|---|
| `batch_size` | Events per page read from the store. |
| `poll_interval` | How long to wait for a live event before reading the store anyway. Bounds how late an event whose broadcast was lost is delivered. |

### Segments, claims and instances

- **Segments.** Each event belongs to segment
  `segment_of(stream, segments)` (a hash of the aggregate type and id).
  Each segment is handled in order by one worker; segments run in
  parallel. Events of one aggregate are therefore always handled in order,
  and a failing aggregate holds back only its segment.
- **Shared reader.** One reader per instance reads the store once and
  routes each event to its segment's worker queue. A full queue is
  backpressure. A queue that stays full for `segment_stall_timeout` means
  the segment is stuck: it is detached and reads on its own, so it never
  holds the others back.
- **Claims.** An instance claims segments in the token store (up to
  `max_segments`) and renews the claims every `claim_interval`. A crashed
  instance stops renewing; after `claim_timeout` another one takes its
  segments over from their stored positions.
- **Tokens.** A segment's token (its `Cursor`) moves in the same unit of
  work as the handler's changes. When a segment only sees other segments'
  events, its token is stored lazily (every 256 events, or after 100 ms
  idle): `caught_up` reflects what was handled in this process; the stored
  tokens follow shortly after.

### Batches

With `batch_size` above 1, a segment worker takes the events already
waiting for it (in its queue, or ready from the store) up to the batch
size, and handles them in one unit of work: one transaction, one token
update, one commit. Under light load each batch is a single event, so
latency does not change; under a backlog (a rebuild, a burst of writes)
batches fill up and the per-event cost of the commit is shared.

A failed batch is rolled back and its events are handled one at a time,
each with the usual retries and dead letters; a batch containing an event
of a parked aggregate goes one at a time from the start. On PostgreSQL,
`batch_size: 100` is a reasonable start for read models; measure with your
handlers.

### Replay

```rust
handle.stop().await;                               // every instance stopped
events.projection(PostSummaryProjection::new(..)).reset().await?;   // tokens to 0, then handler.reset()
let handle = events.projection(PostSummaryProjection::new(..)).spawn();   // replays
```

`reset` fails with `ResetError::Running` while any instance holds a
segment. Events stored before the reset are a replay: `context.replay()`
is true, and handlers with `const REPLAY: bool = false` skip them.

### Dead letters

With `with_dead_letters(queue, after)`, an event that failed `after` times
is parked, keyed by its aggregate (the "sequence"); every later event of
that aggregate is parked behind it without being tried, so the aggregate's
order is kept, and the segment moves on.

```rust
let outcome = events
    .projection(PostSummaryProjection::new(..))
    .with_dead_letters(Arc::clone(&queue), 5)
    .retry_dead_letters()                          // once the cause is fixed
    .await?;
// outcome.handled, outcome.still_parked
```

Each sequence is retried oldest first, each event in a unit of work, and
stops at its first failure. `InMemoryDeadLetters` is provided; a durable
queue implements `DeadLetterQueue<E>`.

## Subscriptions

`events.subscribe(after)` (the `EventFeed` port) is a `Stream` of every
committed event with `position > after`, exactly once, in position order,
then the new ones as they commit. It reads the store in pages and follows
the live broadcast, falling back to the store whenever the broadcast lags
or misses something, and reads through a gap-aware `Cursor`, so parallel
appends committing out of order are never skipped.

```rust
let after = last_seen.unwrap_or(0);
let stream = events.subscribe(after)
    .try_filter(move |event| ready(event.stream.id() == post_id));
```

Use it for GraphQL/SSE/WebSocket subscriptions, for process managers that
keep state in a task, and for anything that wants the log as a stream. It
keeps no token: the consumer remembers its last position.

## Live results for queries

Two ways to push a query's new results to subscribers. Both implement
`QuerySubscriptions<Q>`: `queries.subscribe(query)` yields the current
result, then each new one.

### Subscription queries (projections emit)

The projection pushes updates, through a `QueryUpdateEmitter<Q>`, after
its commit (Axon's subscription queries):

```rust
let updates = Arc::new(PostUpdates::default());     // #[derive(QueryUpdates)]: one emitter per query
let projection = events.projection(PostSummaryProjection::new(summaries.clone(), updates.clone())).spawn();
let queries = SubscriptionQueries::new(PostQueryGateway::new(summaries), updates)
    .after_projection(events.clone(), LocalProgress::of(&projection));
```

`after_projection` makes the initial result wait until the projection
handled every event committed when the subscription started: a client
subscribing right after its own command sees it. Updates are in-process:
the instance that projects the event emits it.

A slow subscriber falls behind by at most 256 updates; then its stream
ends and the client subscribes again.

### Live queries (re-query on change)

`LiveQueries` needs nothing from the projection: after the projection
handled an event the query is `RefreshedBy`, it runs the query again
(once per burst of events).

```rust
let live = LiveQueries::new(PostQueryGateway::new(summaries), events.clone(), &projection_handle);
// several instances share the projection's segments:
let live = LiveQueries::with_progress(gateway, events.clone(), events.global_progress("post-summaries"));
```

`LocalProgress` follows this process's processor; `GlobalProgress` reads
the shared token store, so it is correct when another instance projected
the event.

| | Subscription queries | Live queries |
|---|---|---|
| Who produces updates | the projection, in `committed` | the query handler, re-run |
| Cost per change | one emit | one query per burst |
| Multiple instances | per process | `GlobalProgress` |
| Needs | `QueryUpdateEmitter` fields, emits in projections | `RefreshedBy<E>` on the query |

## Inline processing

Without `Events`, the event-sourcing engine can feed handlers in the caller's task:

```rust
let posts = EventSourced::<Post, _, _>::with_processor(InMemoryEventStore::new(), (projection, policy));
```

Each commit returns once every handler ran (`handle`, then `committed`);
failures never undo the commit: a command handler's commit hands them to the
engine's `on_failure` policy (`LogFailures` by default, or any
`Fn(&ProcessingFailure)`), and the engine's own `append*` methods return them
in `Saved::processing`. No runtime, no tokens, no retries: tests and simple applications.
