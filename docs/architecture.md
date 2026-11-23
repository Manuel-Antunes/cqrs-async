# Architecture

## The two sides and what connects them

```text
                 WRITE SIDE                                    READ SIDE
 ┌──────────────────────────────────────────┐   ┌───────────────────────────────────┐
 │ CommandGateway ──► CommandHandler         │   │ QueryGateway ──► query Handler    │
 │   (derived)    find  │ decide │ commit     │   │   (derived)        │ read         │
 │                      ▼        ▼           │   │                    ▼              │
 │   PostRepository (port)  EventPublisher   │   │   PostSummaryRepository (port)    │
 │                      │        │           │   │                    ▲              │
 │           EventSourced<Post> (engine)     │   │                    │ write        │
 │                      │ append (+condition)│   │        PostSummaryProjection      │
 │                      ▼                     │   │          (EventHandler)           │
 │               EventStore  ════════════════════════►  TrackingProcessor            │
 │          (source of truth, global order)   │   │   unit of work: handle + token    │
 └──────────────────────────────────────────┘   └───────────────────────────────────┘
                        │                                   │ committed
                        ▼                                   ▼
               EventFeed (Stream)                  QueryUpdateEmitter
             subscriptions, sagas            subscription queries (live results)
```

- **Write side.** A command goes through a gateway to exactly one handler.
  The handler finds aggregates through the application's own repository
  ports, lets the domain decide (append events), and commits through its
  context, which carries the publisher the gateway was given. Committing
  appends to the event store with
  optimistic concurrency (expected stream version) and, for decisions that
  read across streams, a condition (nothing matching what it read was
  appended since).
- **The event store** is the only source of truth. Every event gets a
  stream (`post-42`), a sequence within it, and a global position.
- **Read side.** Event handlers turn committed events into read models
  (projections) or side effects (policies). Queries only read the read
  models, never the store.
- **What connects them** is event processing: either inline (the
  engine feeds its handlers right after each commit, for tests and
  simple apps) or by tracking processors that follow the store on their
  own tasks, each in a unit of work that commits the handler's changes and
  the processor's position together.

## A command, end to end

1. `blog.commands().send(PublishPost { id })`: the derived gateway picks the
   field whose handler's `type Command` is `PublishPost` (at compile time)
   and calls it with a fresh `Appender<PostEvent, P>` carrying the
   command's metadata and the gateway's publisher `P`.
2. The handler finds the post in its repository (`posts.find(id)` →
   `Loaded<Post>`: the state plus the version it was read at), calls
   `post.publish(events)` (the domain appends `PostEvent::Published` or
   returns a `PostError`), and commits (`events.commit(post)`).
3. The publisher (the unit's events service) appends to `post-42`
   expecting the loaded version. If the
   stream moved on, the append fails with `StoreError::Conflict` and,
   because nothing was saved yet in this run, the gateway runs the handler
   again on fresh state (up to `conflict_retries`, default 8).
4. The append returns the committed envelopes (position, sequence, time,
   metadata). Inline handlers run now; with `Events`, the envelopes are
   broadcast on the live feed.
5. Each tracking processor reading the store (woken by the live feed, or
   polling) gets the event in its segment and runs its handler in a unit of
   work: `begin` → interceptors → `handle` → store the token → `commit` →
   `committed`. A failure rolls back and retries, or parks the event.
6. The projection's `committed` step emits the post's new summary to
   subscription queries; a client subscribed to `FindPost { id }` receives it.

## Layers in an application

The blog example uses four layers, with dependencies pointing inwards:

```text
presentation ──► application ──► domain
      │               ▲
      └─► (ports) ◄── infrastructure        composition root (config.rs) wires them
```

| Layer | Contains | Knows the framework as |
|---|---|---|
| `domain` | Aggregates, events, domain errors | `Aggregate` (usually derived), `EventAppender`, `DomainEvent`/`TagResolver` derives |
| `application` | Commands, queries and their handlers; projections, policies; repository ports (write and read side); the gateways | Messages, handlers, `Appender`, `Loaded<A>`, `EventHandler` |
| `infrastructure` | Adapters: repositories (the posts' over the event store, the read model's), notifiers, interceptors | Port traits it implements, `cqrs::load` |
| `presentation` | HTTP/GraphQL, written against a ports trait (`BlogApi`) | `CommandGateway`, `QueryGateway`, `QuerySubscriptions`, `EventFeed` bounds |
| composition root (`config.rs`) | Creates the stores and services; builds the application's `Cqrs` unit from the event and token stores (the unit makes the events, which are the publisher, and gives it to the command gateway) | `Cqrs::new`, stores |

Projections live in the application layer: what a read model holds is
application logic, and where it is stored is a port.

## Concept map

| Concept | Axon Framework 5 | NestJS CQRS | Here |
|---|---|---|---|
| Command / query | message classes | `ICommand` / `IQuery` | structs with `#[derive(Command)]` / `#[derive(Query)]`; the message declares `Output` and `Error` |
| Handler | `@CommandHandler` / `@QueryHandler` methods | `@CommandHandler` classes | one struct per message: `#[command_handler] impl CommandHandler`, `#[query_handler] impl Handler` |
| Dispatch | `CommandGateway`, `QueryGateway` | `CommandBus`, `QueryBus` | `#[derive(Gateway)]` on a struct of handlers; `CommandGateway<C>` / `QueryGateway<Q>` as ports; or the macro-free `Bus` |
| Recording events | `EventAppender` | `AggregateRoot.apply()` | `Appender<E>` handed to the handler; the domain sees `impl EventAppender<E>` |
| Aggregate | `@EventSourcedEntity` | `AggregateRoot` | a plain struct with `#[derive(AggregateRoot)]` (or `impl Aggregate`) |
| Entity built from tags | `@EventSourcedEntity(tagKey = ...)` | — | `#[aggregate(tag = "course")]` |
| Event tags | `@EventTag`, `TagResolver` | — | `#[tag]` with `#[derive(TagResolver)]`, or `impl TagResolver` |
| Dynamic consistency boundary | `EventCriteria`, `AppendCondition` | — | same names; `Appender::source`, `EventSourced::load_in` |
| Repository | `Repository`, `EventSourcedRepository` | the user's own provider | the application's own port and adapter (as in NestJS), loading with `cqrs::load` |
| Committing a decision | end of the `ProcessingContext` | `EventPublisher` injected, `aggregate.commit()` | `events.commit(entity)` in the handler; the `EventPublisher<E>` is injected into the gateway (`with_publisher`) |
| Event handling component | class with `@EventHandler` methods | `@EventsHandler` | `impl EventHandler<E, Tx>`: one `handle` that matches on the event enum |
| Projection name | `@Namespace` | — | `#[cqrs::projection(namespace = "...")]` |
| Unit of work | `ProcessingContext` | — | `ProcessingContext<'_, Tx>` + `TransactionalTokenStore` |
| Event processor | `PooledStreamingEventProcessor` | — | `TrackingProcessor` (segments, claims, shared reader) |
| Processing group | processing group | — | a tuple of handlers |
| Token store / token | `TokenStore`, `GapAwareTrackingToken` | — | `TokenStore`, `Cursor` |
| Replay | `resetTokens`, `@DisallowReplay`, `ReplayStatus` | — | `TrackingProcessor::reset`, `const REPLAY: bool = false`, `context.replay()` |
| Dead letters | `SequencedDeadLetterQueue` | — | `DeadLetterQueue`, `TrackingProcessor::retry_dead_letters` |
| Interceptors | dispatch / handler interceptors | — | `Interceptor<M>` around gateways, `EventInterceptor<E, Tx>` around handlers |
| Subscription queries | `QueryUpdateEmitter`, `subscriptionQuery` | — | `QueryUpdateEmitter<Q>`, `SubscriptionQueries`, `QuerySubscriptions<Q>` |
| Sagas | stateful event handlers | `@Saga` over an RxJS stream | `EventFeed::subscribe` (a `Stream`), or an event handler that sends commands |
| Upcasting | `Upcaster` | — | `Upcaster`, `upcast(type, version, fn)` |
| Snapshots | snapshot trigger | — | `EventSourced::with_snapshots(store, every)` |
| Aggregate tests | `AggregateTestFixture` | — | `testing::AggregateFixture` |

Where the shapes differ, the reason is almost always the same: Axon
discovers handlers by reflection and stores them behind interfaces; here
the type system has to see every handler, so they are struct fields,
tuple members or trait impls, and the compiler does the routing.

## Two wirings of the same application

The blog wires its application twice, from the same handlers:

- **Inline** (`blog::in_memory()`): `EventSourced::with_processor(store,
  (projection, policy))`. Handlers run right after each commit, in the
  caller's task; no runtime, deterministic, what domain and flow tests use.
  A failing handler is reported in `Saved::processing` but never undoes the
  commit.
- **Asynchronous** (`blog::start()`): `Events::new(store)`; the engine
  (`events.event_sourced()`) only broadcasts its commits, and each handler runs in its own tracking
  processor (`events.projection(p).spawn()`), with tokens, retries and a
  unit of work. Commands return once committed; read models follow
  (eventually consistent), and subscription queries wait for them when a
  client needs to see its own write.

Both are a `Cqrs` unit built by the same builder (`Cqrs::from_publisher`
inline, `Cqrs::new(store, tokens)` asynchronous), with the same gateways
and repository. The presentation layer serves either.
