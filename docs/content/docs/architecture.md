---
title: "Architecture"
sidebar_position: 20
---

## The two sides and what connects them

```text
                 WRITE SIDE                                       READ SIDE
 ┌─────────────────────────────────────────────┐   ┌─────────────────────────────────────┐
 │ PostCommandGateway (is the handler)          │   │ PostQueryGateway (is the handler)   │
 │   impl CommandHandler<PublishPost>           │   │   impl QueryHandler<FindPost>       │
 │       find │ decide │ commit │ save       │   │                 │ read              │
 │           ▼        ▼         ▼               │   │                 ▼                   │
 │   Repository<Post> (engine's, or yours)      │   │   PostSummaryRepository (your port) │
 │                                              │   │                 ▲ write             │
 │   unit of work (#[gateway(unit)]):           │   │   PostProjection                    │
 │   begin → handle → append → commit           │   │     namespace "summaries"           │
 │                     │                        │   │     namespace "notifications"       │
 │                     ▼                        │   │                 ▲                   │
 │               EventStore  ════════════════════════► TrackingProcessor per namespace   │
 │        (source of truth, global order)       │   │   unit of work: handle + token      │
 └─────────────────────────────────────────────┘   └─────────────────────────────────────┘
                       │                                               │ handled
                       ▼                                               ▼
              EventFeed (a Stream)                       QueryUpdateEmitter<Self>
          catch-up subscriptions                  subscription queries (live results)
```

- **Write side.** A command goes through a gateway to exactly one handler.
  The handler finds entities through a repository (the one the engine hands
  out, or the application's own), lets
  the domain decide (append events), and commits them to the entity they
  change. The framework's unit of work appends what was committed, with
  optimistic concurrency, and commits.
- **The event store** is the only source of truth. Every event gets a
  stream (`post-42`), a sequence within it, and a global position.
- **Read side.** A projection, one per aggregate root, turns committed
  events into read models and side effects, one namespace each. Queries
  read the read models, never the store.
- **What connects them** is event processing: inline, right after each
  commit, in the caller's task; or by tracking processors that follow the
  store on their own tasks, each event in a unit of work that commits the
  handler's changes with the processor's token.

## Modules

The public API is a handful of modules, and a small prelude:

| Module | What is in it |
|---|---|
| `cqrs` (root) | The macros, `Cqrs` and `CqrsBuilder` (an application's unit and its builder), `Units`, `Message`, `Dispatch`, `Interceptor`, `Intercepted` |
| `cqrs::aggregate` | The domain side: `Aggregate`, `EventAppender`, `AggregateLoader`, `Repository`, `Loaded`, `New`, `NotFound`, `SnapshotStore`, `InMemorySnapshots`, `Snapshotting`, `SnapshotPolicy`, `KeepSnapshots` |
| `cqrs::command` | `Command`, `CommandHandler<C>`, `CommandContext`, `CommandGateway`, and the unit of work: `UnitOfWork`, `UnitMode`, `Autocommit`, `InTransaction`, `dispatch_command` |
| `cqrs::query` | `Query`, `QueryHandler<Q>`, `QueryGateway`, and subscription queries: `QuerySubscriptions`, `QueryUpdateEmitter`, `QueryUpdateChannel` |
| `cqrs::event` | `DomainEvent`, `EventEnvelope`, `Metadata`, `StreamId`, and tags: `Tag`, `TagResolver`, `EventCriteria`, `AppendCondition` |
| `cqrs::store` | The store ports (`LoadStream`, `AppendEvents`, `SourceEvents`, `ReadLog`), `InMemoryEventStore`, `FanIn`, `Cursor`, `EventFeed` and `subscribe`, codecs and upcasters |
| `cqrs::processing` | `EventHandler`, `ProcessingContext`, `Projection`, `Events`, `TrackingProcessor`, `TrackingOptions`, `TokenStore`, `TransactionManager`, `DeadLetterQueue`, `InlinePublisher`, `EventInterceptor`, and brokers: `Forward`, `Publish`, `Subscribe`, `Consume`, `Incoming` |
| `cqrs::testing` | `AggregateFixture` (given / when / then) and `block_on` |
| `cqrs::prelude` | What most domain and application modules use: `use cqrs::prelude::*;` |

A domain module usually needs only `cqrs::aggregate::EventAppender` and
the event derive; an application module, the prelude.

## A command, end to end

The blog's `PublishPost`, on PostgreSQL. The handler is an impl on the
gateway, in the use case's file:

```rust
// src/application/commands/publish_post.rs
#[cqrs::command_handler]
impl<R: PostRepository, P> CommandHandler<PublishPost> for PostCommandGateway<R, P> {
    async fn handle(
        &self,
        command: &PublishPost,
        ctx: &mut CommandContext<PostEvent, R::Tx>,
    ) -> Result<(), BlogError> {
        let post = self.posts.find(&command.id, ctx.tx()).await?.ok_or(NotFound(command.id))?;
        post.publish(ctx)?;
        let post = ctx.commit(post);
        self.posts.save(&post, ctx.tx()).await?;
        Ok(())
    }
}
```

1. **Dispatch.** `app.commands().send(PublishPost { id })`. The gateway
   (`#[cqrs::command_gateway(commands = PostCommands, handles(CreatePost,
   RetitlePost, PublishPost))]`) routes it to its `CommandHandler<PublishPost>`
   impl, chosen at compile time. Interceptors added with `.intercept(..)`
   wrap the gateway.
2. **The unit of work opens.** The gateway runs the command in a unit of
   work of its `#[gateway(unit)]` field (the unit's `Events`). Because
   `handle` takes a `CommandContext<PostEvent, R::Tx>`, the mode is
   `InTransaction<Tx>`: the unit begins a transaction and the context owns
   it. With a `CommandContext<PostEvent>` the mode is `Autocommit`: no
   transaction, one entity per command.
3. **The handler decides.** It finds the post in its repository
   (`Loaded<Post>`: the state and the version it was read at), lets the
   domain append through `ctx` (an `EventAppender<PostEvent>`), and calls
   `ctx.commit(post)`, which assigns the appended events to that post and
   returns it evolved, at its new version; nothing is appended yet. Then
   `save(&post, ctx.tx())` keeps that state in the same transaction (the
   SeaORM adapter does; the event-sourced one saves nothing).
4. **The unit appends and commits.** Once `handle` returns `Ok`, the unit
   appends what was committed, in the transaction, expecting the version the
   post was loaded at, and commits: the post's row and its events commit
   together, or not at all. The handler never commits.
5. **A lost race reruns the handler.** If the stream moved on (or a DCB
   condition failed), the transaction is rolled back and the handler runs
   again on fresh state, up to `conflict_retries` (8 by default). That is
   why a handler has no side effects besides what it writes in the context's transaction: an
   e-mail belongs in a projection namespace, after the commit.
6. **The commit is announced.** `Events` broadcasts the committed envelopes
   on its live feed, which wakes the tracking processors; an
   `InlinePublisher` runs its handlers right away instead. `send` returns
   the command's output.

## From events to projections

A projection is one struct per aggregate root, holding what its handlers
need, with one `EventHandler` impl per namespace:

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

- **Namespaces.** Each namespace (`#[cqrs::event_handler(namespace =
  "summaries")]`) is run by a tracking processor of its own, named
  `"{projection}.{namespace}"` (`post.summaries`, `post.notifications`),
  with its own tokens. A failing notification never holds the read model
  back. The namespace lives on the projection, not on the event: the domain
  does not know who consumes it.
- **Tracking processors.** A processor reads the log in global order and
  splits it into segments by stream, so a post's events stay in order and
  different posts run in parallel (`TrackingOptions::segments`). Segments
  are claimed in the `TokenStore`, so several instances share the work and
  take over a dead instance's segments.
- **Tokens.** A segment's token is a `Cursor`: the highest position seen,
  plus the gaps a parallel append left behind. For each event (or batch,
  with `batch = N`) the processor opens a unit of work with its
  `TransactionManager`, runs the handler and its interceptors, stores the
  token, and commits. A read model in the same database as the tokens
  applies each event exactly once; a side effect outside it is
  at-least-once.
- **Failures.** A failing event is rolled back and retried with backoff,
  holding back only its own segment. With a `DeadLetterQueue`, after a
  number of attempts it is parked with the later events of its stream, and
  everything else keeps flowing.
- **Replay.** `reset` calls each handler's `reset` (clear the read model)
  and rewinds the tokens. Namespaces with `const REPLAY: bool = false`
  (notifications) skip the replayed events.

**Inline or asynchronous.** The same projection runs either way:

- `Cqrs::from_publisher(InlinePublisher::new(store).with_projection(p))`:
  each namespace sees each commit right after it, in the caller's task,
  isolated from the others. No runtime, deterministic: what domain and flow
  tests use. A failing handler does not undo the commit; the failure goes
  to the publisher's `on_failure` (`LogFailures`, logging at error level,
  by default).
- `Cqrs::new(Events::new(store)).projection(p)`: commands return once
  committed, and each namespace follows on its own processor. Read models
  are eventually consistent; `app.settled().await` waits for them, and
  several units together are a tuple (`(posts, users).settled().await`).

On PostgreSQL only the events change:
`cqrs_postgres::Postgres::new(pool).events::<PostEvent>("posts")` gives the
same `Events` with the database's tokens and transactions. Each aggregate
root has its own log; all logs share one global position sequence.

## Subscription queries

A subscription query is the current result of a query, then every update
of it. The projection lists its queries in `updates(..)` and gets one
channel per query, `QueryUpdateEmitter<Self>`. A namespace emits in its
`updated` step:

```rust
// src/application/projections/post/summaries.rs
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
```

`updated` runs on every instance, once that namespace handled the event,
whichever instance handled it: every subscriber gets the update, wherever
it is connected, and never sees a change that was rolled back. Emitting for
a query not listed in `updates(..)` does not compile.

A client calls `app.queries().subscribe(FindPost { id })`: the current
result, once the read-model namespaces handled what was committed when the
subscription started (so a client sees its own commands), then the
updates. `subscriptions = PostSubscriptions` names the trait the API
depends on.

## Layers in an application

The blog uses four layers, with dependencies pointing inwards:

```text
presentation ──► application ──► domain
      │               ▲
      └─► (ports) ◄── infrastructure        composition root (config.rs) wires them
```

| Layer | Contains | Knows the library as |
|---|---|---|
| `domain` | Aggregates, events, domain errors | `#[cqrs::aggregate]`, `EventAppender`, `#[derive(cqrs::DomainEvent)]` |
| `application` | Commands and the command gateway; queries and the query gateway; the projection and its namespaces; repository ports (write and read side) | `CommandHandler`, `CommandContext`, `Loaded`, `QueryHandler`, `EventHandler`, `QueryUpdateEmitter` |
| `infrastructure` | Adapters: the posts' repositories (event-sourced, SeaORM), the read model, the notifier, interceptors | The port traits it implements, `AggregateLoader`, `Interceptor`, `EventInterceptor` |
| `presentation` | GraphQL over axum, written against the generated traits | `PostCommands`, `PostQueries`, `PostSubscriptions`, `EventFeed` |
| composition root (`config.rs`) | The stores, the adapters, the `Cqrs` unit | `Cqrs::new`, `Cqrs::from_publisher`, the stores |

Projections live in the application layer: what a read model holds is
application logic, and where it is stored is a port.

## Concept map from Axon Framework 5 {#concept-map}

The rustdoc describes each item on its own terms; this table is where the
Axon names live.

| Axon Framework 5 | Here |
|---|---|
| Command and query messages | Structs with `#[cqrs::command(output = .., error = ..)]` / `#[cqrs::query(..)]`: the message owns its output and error types |
| `@CommandHandler` | A `#[cqrs::command_handler] impl CommandHandler<C>` on the command gateway, one per command |
| `@QueryHandler` | A `#[cqrs::query_handler] impl QueryHandler<Q>` on the query gateway |
| `CommandGateway`, `QueryGateway` | `CommandGateway<C>` (`send`, `send_with` for metadata), `QueryGateway<Q>` (`query`), implemented by `#[cqrs::command_gateway]` / `#[cqrs::query_gateway]` |
| `MetaData` | `Metadata`, carried by every event the command commits |
| `EventAppender` | `CommandContext<E>` (or `CommandContext<E, Tx>`) in the handler; the domain takes it as `&mut impl EventAppender<E>` |
| `ProcessingContext` | `ProcessingContext<'_, Tx>` for event handlers; a command's unit of work is the gateway's `UnitOfWork` with its `CommandContext` |
| `TransactionManager` | `TransactionManager` |
| `@EventSourcedEntity` | `#[cqrs::aggregate(name = .., event = ..)]`, or `impl Aggregate` |
| `@EventSourcedEntity(tagKey = ..)` | `#[cqrs::aggregate(tag = "..")]` (`Aggregate::criteria`) |
| `@EventSourcingHandler` | The aggregate's `apply` method, which `Aggregate::evolve` calls |
| Creating command handler | An associated function (`Post::create(id, .., ctx)`) and `ctx.commit(New::<Post>(id))` |
| `Repository`, `EventSourcedRepository` | `Repository<A>` (`find`, `save`), which the application's own port can extend; the event-sourced one is the `AggregateLoader` the engine hands out (`events.repository::<A>()`) |
| Dynamic consistency boundary: `EventCriteria`, `AppendCondition`, `@EventTag` | `EventCriteria`, `AppendCondition`, `#[tag]` on event fields (`#[derive(cqrs::DomainEvent)]`) or `impl TagResolver`; `CommandContext::source` |
| `EventStore`, `EventStorageEngine` | `EventStore`, over the ports `LoadStream`, `AppendEvents`, `SourceEvents`, `ReadLog` |
| Event handling component, processing group, `@Namespace` | A projection namespace: `namespaces(..)` on `#[cqrs::projection]`, one `#[cqrs::event_handler(namespace = "..")] impl EventHandler` each |
| `PooledStreamingEventProcessor` | `TrackingProcessor` with segments (`TrackingOptions::segments`) and claims |
| `SubscribingEventProcessor` | `InlinePublisher`: handlers run in the committing task |
| `batchSize` | `batch = N` on the namespace's `#[cqrs::event_handler]`, or `TrackingOptions::batch_size` |
| `TokenStore` | `TokenStore` (`InMemoryTokenStore`, `PostgresTokenStore`) |
| `GapAwareTrackingToken` | `Cursor` (its `Gap`s are dropped once the store's `Horizon` proves them empty, not after a timeout) |
| `SequencedDeadLetterQueue` | `DeadLetterQueue` (`InMemoryDeadLetters`, `PostgresDeadLetters`), `TrackingProcessor::with_dead_letters`, `retry_dead_letters` |
| `EnqueuePolicy`, `EnqueueDecision` (`enqueue`, `requeue`, `evict`, `ignore`) | `EnqueuePolicy`, `EnqueueDecision` (plus `Retry`), `ParkAfter` (the default), `with_enqueue_policy` |
| `MultiStreamableMessageSource` (Axon 4; not in 5) | A fan-in: an enum with a variant per log, `#[event(fan_in)]`, read as one by `Events::fan_in` (in memory, `FanIn`) or `Postgres::fan_in` (`PostgresFanIn`), in the global order |
| `EventSink.publish`, `SubscribableEventSource.subscribe` | `Publish` (driven by `Forward`, the log as outbox), `Subscribe` handing batches to a `Consume`r; RabbitMQ in `cqrs-async-amqp` (`AmqpPublisher`, `AmqpSubscriber`) |
| `CommandScheduler`, `ScheduleToken` (Axon 4; removed in 5) | `CommandSchedule` (`InMemorySchedule`, `PostgresSchedule`), `schedule_in`, `cancel`, `reschedule`, `deliver_scheduled` |
| `resetTokens`, `@ResetHandler` | `TrackingProcessor::reset`, and the handler's `reset` |
| `@DisallowReplay`, `ReplayStatus` | `const REPLAY: bool = false`, `ProcessingContext::replay()` |
| `QueryUpdateEmitter` | `QueryUpdateEmitter<Self>`, from the projection's `updates(..)`, emitted in `updated` |
| `subscriptionQuery` | `QuerySubscriptions::subscribe` (`app.queries().subscribe(query)`) |
| Dispatch and handler interceptors | `Interceptor<M>` around a gateway (`.intercept(..)`, `Intercepted`), `EventInterceptor<E, Tx>` around a projection's handlers |
| Upcasters | `Upcaster`; `Upcast`, made by `upcast(event_type, from, convert)` |
| `Converter` | `Codec` (`Json`, `Protobuf`) |
| Snapshots, `@Snapshotting` | `SnapshotStore`, `#[cqrs::aggregate(.., snapshot(after_events = N, after_sourcing = "5s"))]` on the aggregate, `Events::repository::<A>()` building its loader; `AggregateLoader::with_snapshots(store, policy)` by hand |
| `AggregateTestFixture` | `testing::AggregateFixture` |

Where the shapes differ, the reason is almost always the same: Axon
discovers handlers by reflection and keeps them in registries; here the
type system has to see every handler, so they are trait impls on a gateway
or a projection, listed in its attribute, and the compiler does the
routing.
