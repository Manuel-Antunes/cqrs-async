# Patterns

The library is made of small, repeatable patterns. Each section below names
one, says what problem it solves, shows its shape, and lists its rules.
Applications use the same patterns, so the sections double as a style
guide.

- [Static dispatch](#static-dispatch): 1–6
- [Messages and gateways](#messages-and-gateways): 7–13
- [Domain](#domain): 14–20
- [Command handling](#command-handling): 21–25
- [Storage](#storage): 26–30
- [Event handling](#event-handling): 31–41
- [Interceptors](#interceptors): 42–44
- [Read side and streams](#read-side-and-streams): 45–50
- [Testing](#testing): 51–53
- [Composition](#composition): 54–55

---

## Static dispatch

### 1. Trait with `impl Future` returns

**Problem.** Async traits usually box their futures (`Pin<Box<dyn Future>>`),
costing an allocation and a virtual call per call, and hiding `Send`.

**Shape.** Every async contract returns `impl Future<Output = ...> + Send`;
implementations write a plain `async fn`.

```rust
pub trait EventPublisher<E: DomainEvent>: Send + Sync {
    fn commit<A: Aggregate<Event = E>>(&self, entity: Loaded<A>, pending: Pending<E>) -> impl Future<Output = Result<Option<u64>, StoreError>> + Send;
}

impl PostRepository for EventSourcedPostRepository {
    async fn find(&self, id: PostId) -> Result<Option<Loaded<Post>>, StoreError> { /* ... */ }
}
```

**Rules.** The `+ Send` in the trait makes `tokio::spawn`-ability part of
the contract: an implementation holding a non-`Send` value across an
`.await` does not compile. Traits that use this are not object-safe, which
is the point: nothing here is a trait object.

### 2. Tuples as heterogeneous collections

**Problem.** "A list of handlers of different types" is
`Vec<Box<dyn Handler>>` in most frameworks.

**Shape.** Implement the trait for tuples with a macro; each member keeps
its own type, and the tuple's impl calls them in order.

```rust
// A processing group: three handlers, one processor.
events.processor("read-models", (PostSummaryProjection::new(..), Search::new(..), Stats::new(..)))

// An interceptor chain, first outermost.
Intercepted::new(gateway, (Tracing, Timing, RequireUser))
```

Used for: processing groups (`EventHandler`, `EventProcessor`), interceptor
chains (`Interceptor`, `EventInterceptor`), upcaster chains (`Upcaster`).
Implemented up to 8 members.

**Rules.** The tuple's behavior is the composition's: a group's
`REPLAY` is true if any member replays, and its `handle_in` skips the
members that do not; an interceptor tuple nests.

### 3. Forwarding `Arc<T>` with every method

**Problem.** Sharing a component (`Arc<AggregateLoader<..>>`) should not change its
behavior. A blanket `impl Trait for Arc<T>` that forwards only the
required methods silently drops the overrides of provided ones.

**Shape.** Every `impl Trait for Arc<T>` forwards every method, and the
compiler checks it:

```rust
// Forwards every method, so an override of a provided one is never lost.
#[deny(clippy::missing_trait_methods)]
impl<H: EventHandler<E, Tx>, E: DomainEvent, Tx: Send> EventHandler<E, Tx> for Arc<H> {
    const REPLAY: bool = H::REPLAY;
    fn handle(..) -> .. { (**self).handle(..) }
    fn committed(..) -> .. { (**self).committed(..) }
    fn reset(..) -> .. { (**self).reset(..) }
    fn handle_in(..) -> .. { (**self).handle_in(..) }
    fn committed_in(..) -> .. { (**self).committed_in(..) }
}
```

**Rules.** Applies to every trait with provided methods. The lint once
caught a missing `dispatch` forward that would have bypassed a gateway's
override.

### 4. Generic parameters for extension points

**Problem.** Pluggable parts (the store, the token store, the snapshot
store, the read-model adapter) are usually injected as trait objects.

**Shape.** They are type parameters with sensible defaults, swapped by
builder methods that change the type:

```rust
pub struct AggregateLoader<A, S, N = ()> { .. }          // store, snapshots

let posts = AggregateLoader::new(store)                 // <Post, S, ()>
    .with_snapshots(InMemorySnapshots::default(), 100); // <Post, S, InMemorySnapshots<Post>>
```

**Rules.** `()` is the "nothing" implementation of most ports: no
processor, no snapshots, no dead-letter queue, no interceptor state, an
in-memory unit of work. The composition root does not name the
resulting types: the [CQRS unit's builder](#55-the-cqrs-unit-a-typed-builder)
infers them, and [generated ports](#54-ports-generated-from-the-message-enums)
name what comes out.

### 5. Compile errors that read like documentation

**Problem.** Static routing turns mistakes into trait errors, which are
cryptic by default.

**Shape.** `#[diagnostic::on_unimplemented]` on the routing traits:

```text
error[E0277]: `FindPost` is not a command
  = note: derive it with `#[derive(Command)]`; queries are sent with `.query(...)`
```

**Rules.** Add one wherever a user meets a trait bound instead of a
function (gateways, buses, handlers). The guarantees are tested with
`compile_fail` doctests ([pattern 53](#53-compile_fail-doctests-as-guarantees)).

### 6. Macros only write what a user could

**Problem.** Framework macros that inject fields or rewrite types make the
code impossible to read without expanding them, and lock users in.

**Shape.** Every macro emits trait impls or associated types, and each has
a documented hand-written equivalent:

| Macro | Writes | By hand |
|---|---|---|
| `#[derive(Command)]`, `#[derive(Query)]` | `impl Message` + marker | 4 lines |
| `#[derive(Gateway)]` | `impl Dispatch<M>` per field | one impl per message |
| `#[derive(AggregateRoot)]` | `impl Aggregate` | 5 items |
| `#[derive(DomainEvent)]` | `event_type`, `event_version` | a `match` |
| `#[derive(TagResolver)]` | `fn tags` | a `match` |
| `#[command_handler]`, `#[query_handler]` | the associated types | `type Command = ..; type Event = ..;` |
| `#[cqrs::projection(namespace)]` | the trait's generics, `impl Projection` | `impl EventHandler<E, Tx>` + `const NAMESPACE` |
| `#[derive(QueryUpdates)]` | `impl UpdatesOf<Q>` per field | one impl per query |

**Rules.** Nothing a macro does is required. A domain that wants no
framework attributes implements the traits.

---

## Messages and gateways

### 7. The message owns its output and error

**Problem.** If handlers declare what they return, a caller must know which
handler answers a message to know what comes back.

**Shape.**

```rust
#[derive(Debug, serde::Deserialize, cqrs::Command)]
#[command(output = PostId, error = BlogError)]   // output defaults to ()
pub struct CreatePost { pub title: String }

#[derive(Clone, Debug, cqrs::Query)]
#[query(output = Option<PostSummary>, error = BlogError)]
pub struct FindPost { pub id: PostId }
```

`Message { type Output; type Error; }` plus the `Command` or `Query` marker.
`gateway.send(CreatePost { .. })` returns `Result<PostId, BlogError>`
whoever handles it.

**Rules.** Sending a query with `.send` (or a command with `.query`) is a
compile error: the markers keep the two buses apart.

### 8. One struct per use case

**Problem.** Handler classes grouping many methods grow unbounded
dependencies.

**Shape.** One handler struct per message, holding exactly the ports that
use case needs, generic over them:

```rust
pub struct RetitlePostHandler<R> { posts: Arc<R> }

#[cqrs::command_handler]
impl<R: PostRepository> CommandHandler for RetitlePostHandler<R> {
    async fn handle(&self, command: &RetitlePost, events: &mut Appender<PostEvent>) -> Result<(), BlogError> {
        let post = self.posts.find(command.id).await?.ok_or(NotFound(command.id))?;
        post.retitle(command.title.as_str(), events)?;
        events.commit(post, &self.events).await?;
        Ok(())
    }
}
```

**Rules.** The message is an associated type (`type Message`, or
`type Command` for command handlers), not a generic parameter, so a
gateway routes by the handler type alone. Command handlers take the command
by reference: they may run again after a conflict.

### 9. Derived gateway

**Problem.** Bus registries keyed by `TypeId` fail at runtime when a
handler is missing or registered twice.

**Shape.** A struct of handlers with `#[derive(Gateway)]`: it implements
`Dispatch<M>` for each field's message, so `CommandGateway<M>` /
`QueryGateway<M>`.

```rust
#[derive(cqrs::Gateway)]
pub struct PostCommandGateway<R> {
    create: CreatePostHandler<R>,
    retitle: RetitlePostHandler<R>,
    publish: PublishPostHandler<R>,
}
```

**Rules.** No handler for a message: E0277 where it is sent. Two handlers
for one message: E0119 (conflicting impls) where the gateway is defined.
A field that is not a handler needs `#[gateway(skip)]`. The gateway must be
in the crate of its handlers (coherence). Keep command and query gateways
separate; a [CQRS unit](#55-the-cqrs-unit-a-typed-builder) exposes
`commands()` and `queries()`.

A gateway of command handlers is marked `#[gateway(commands)]` (or
`#[gateway(commands = Enum)]`). It only routes: each handler holds the
publisher it commits through (`PostCommandGateway::new(posts, events)`), and
the gateway runs it with a fresh appender, re-running it when a commit lost a
race (`dispatch_command`).

### 10. Ports as gateway bounds

**Problem.** Code that sends commands should not depend on the concrete
gateway (or on all of it).

**Shape.** Depend on the messages you send:

```rust
pub async fn create_and_publish<G>(gateway: &G, title: &str) -> Result<PostId, BlogError>
where
    G: CommandGateway<CreatePost> + CommandGateway<PublishPost>,
{ .. }

pub trait BlogApi: Send + Sync + 'static {
    type Commands: CommandGateway<CreatePost> + CommandGateway<RetitlePost> + CommandGateway<PublishPost>;
    type Queries: QueryGateway<FindPost> + QuerySubscriptions<FindPost> + ..;
    type Events: EventFeed<PostEvent>;
    fn commands(&self) -> &Self::Commands;
    ..
}
```

**Rules.** A presentation layer written against such a trait compiles
against the in-memory app, the live app and test doubles alike.

### 11. Message enums for transports

**Problem.** A queue or an HTTP endpoint deserializes "some command" and
must dispatch it.

**Shape.** An enum whose variants wrap the messages; the derive makes it a
message, with a generated output enum, and the gateway routes each variant:

```rust
#[derive(Debug, serde::Deserialize, cqrs::Commands)]
#[commands(error = BlogError, derive(Debug, PartialEq))]
pub enum PostCommand { CreatePost(CreatePost), RetitlePost(RetitlePost), PublishPost(PublishPost) }

#[derive(cqrs::Gateway)]
#[gateway(commands = PostCommand)]
pub struct PostCommandGateway<R> { .. }

let created: PostCommand = serde_json::from_str(r#"{"CreatePost": {"title": "Over the wire"}}"#)?;
let PostCommandOutput::CreatePost(id) = gateway.send(created).await? else { unreachable!() };
```

**Rules.** A variant no field handles is a compile error at the gateway.
Each message converts `From` into the enum. `#[derive(Queries)]` and
`#[gateway(queries = ...)]` are the read-side twins.

### 12. Type-level bus (no macro)

**Problem.** Sometimes you want a bus without declaring a struct.

**Shape.** A type-level list; the compiler infers each handler's position:

```rust
let bus = Bus::new()
    .register(CreatePostHandler::new(posts.clone()))
    .register(PublishPostHandler::new(posts))
    .register(FindPostHandler::new(summaries));
let id = bus.send(CreatePost { title: "hlist".into() }).await?;
let found = bus.query(FindPost { id }).await?;
```

**Rules.** No handler: "no handler for `M` is registered on this bus";
two: ambiguity (E0283). The price is an inferred index parameter in generic
code, so prefer the derived gateway for ports.

### 13. Metadata as correlation data

**Problem.** Who asked, and why, should travel with the events a command
causes, without every handler passing it along.

**Shape.** Send with metadata; the gateway puts it in the handler's
`Appender`, and every event saved in that run stores it.

```rust
let mut metadata = Metadata::new();
metadata.insert("correlation_id".into(), request_id);
blog.commands().send_with(PublishPost { id }, metadata).await?;
// later: event.metadata["correlation_id"]
```

**Rules.** Interceptors see and may change metadata before the handler
([pattern 42](#42-dispatch-interceptor)). Handlers read it from
`events.metadata()`.

---

## Domain

### 14. Decisions take `&self` and an appender

**Problem.** Aggregates that mutate themselves in command methods can
change state without recording an event, and carry a pending-events buffer
that leaks into serialization.

**Shape.** State changes only in `evolve` (through `apply`); decisions
borrow immutably and append:

```rust
impl Post {
    fn apply(&mut self, event: &PostEvent) {                       // the only mutation
        match event {
            PostEvent::Created { title, .. } | PostEvent::Retitled { title } => self.title.clone_from(title),
            PostEvent::Published => self.published = true,
        }
    }

    pub fn publish(&self, events: &mut impl EventAppender<PostEvent>) -> Result<(), PostError> {
        if self.published {
            return Err(PostError::AlreadyPublished);
        }
        events.append(PostEvent::Published);
        Ok(())
    }
}
```

**Rules.** `evolve` never validates, fails or does I/O: it runs on replay
too. A decision sees the state from before it ran; the engine applies
its events when committing. Assigning a field in a decision is a compile error
(E0594). `EventAppender<E>` is implemented for every `Extend<E> + Send`, so
a domain can take `impl Extend<PostEvent>` and not depend on the framework.

### 15. Derived aggregate root

**Shape.**

```rust
#[derive(Clone, Debug, Serialize, Deserialize, cqrs::AggregateRoot)]
#[aggregate(name = "post", event = PostEvent)]
pub struct Post { id: PostId, title: String, published: bool }
```

Writes `impl Aggregate`: `TYPE` from `name` (default: snake-case struct
name), `Id` from the `id` field (or `#[aggregate(id)]`), `initial` with the
other fields at `Default` (or `#[aggregate(initial = path)]`), `evolve`
calling `apply` (or `#[aggregate(apply = method)]`).

**Rules.** `TYPE` prefixes stream ids (`post-42`): changing it orphans
stored streams. `Id: Display` renders the stream suffix and must be stable.

### 16. The loaded aggregate carries its version

**Problem.** The version used for optimistic concurrency is persistence
data; putting it in the domain struct mixes concerns, and reusing a stale
aggregate after a failed commit is a classic bug.

**Shape.** `Loaded<A>` wraps the state with the version it was read at. It
derefs to `&A` only (no `&mut`, E0596), and `commit` consumes it:

```rust
let post = posts.find(id).await?.ok_or(NotFound(id))?;    // Loaded<Post>, from your repository
post.retitle("New", events)?;                              // &Post
events.commit(post, &self.events).await?;                  // moved: cannot be reused
let new = Loaded::new(Post::initial(&id));                 // expects no stream yet
```

Repositories return it (they get it from an `AggregateLoader`), so the
version travels from the load to the commit without the domain seeing it.

### 17. Stable event names and versions

**Shape.**

```rust
#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent, cqrs::TagResolver)]
pub enum PostEvent {
    #[event(name = "post.created")]
    Created { id: PostId, title: String },
    #[event(name = "post.retitled", version = 2)]
    Retitled { title: String },
    #[event(name = "post.published")]
    Published,
}
```

**Rules.** The stored type is the `name`, not the Rust identifier, so
renaming a variant is free. Each variant is versioned on its own (default
1); bump the version when the payload's shape changes, and register an
upcaster ([pattern 29](#29-upcaster-chains)).

### 18. Event tags, derived or hand-written

**Problem.** Criteria-based reads (and locks) need to know what each event
is about, without a second index maintained by hand.

**Shape.** Opt in with the derive and mark fields; or implement the trait
and keep the event free of attributes (Axon offers both too):

```rust
#[derive(Clone, cqrs::DomainEvent, cqrs::TagResolver)]
enum SchoolEvent {
    Subscribed { #[tag] course: u32, #[tag] student: u32 },
}

impl TagResolver for OrderEvent {                     // or by hand
    fn tags(&self) -> Vec<Tag> {
        let Self::Placed { order, region } = self;
        vec![Tag::new("order", order), Tag::new("region", region)]
    }
}
```

**Rules.** `TagResolver` is a supertrait of `DomainEvent`: every event
says its tags, even when there are none (derive it, or write an empty
`impl TagResolver for E {}`). Tags are resolved once, in the store's
provided `append_if` ([pattern 26](#26-provided-method-required-hook)).

### 19. Aggregates built from tags (dynamic consistency boundaries)

**Problem.** A rule spanning two aggregates (a course's capacity and a
student's course limit) forces a bigger aggregate or a saga.

**Shape.** Each aggregate is built from the events tagged with its id,
whatever stream holds them; a decision loads both and its commit is
conditional on neither having changed:

```rust
#[derive(cqrs::AggregateRoot)]
#[aggregate(event = SchoolEvent, tag = "course")]
struct Course { id: u32, capacity: usize, students: Vec<u32> }

let course = courses.load_in(&course_id, events).await?.ok_or(Error::NoSuchCourse)?;
let student = students.load_in(&student_id, events).await?;   // read only: still checked
course.subscribe(student_id, student.as_deref(), events)?;
events.commit(course, &self.events).await?;   // StoreError::ConditionFailed if either changed
```

Here `courses` and `students` are `AggregateLoader`s: `load_in` is
theirs, and an application repository exposing such reads forwards to it.

**Rules.** `load_in` records an `AppendCondition` in the appender, even
when the aggregate does not exist yet (absence is a fact too). A failed
condition is retried like a conflict. On PostgreSQL, decisions on
different tags run in parallel; on the same tag they take turns
([Storage § Locks](storage.md#locks-derived-from-the-domain)).

### 20. Domain errors are values, not store errors

**Shape.** The domain returns its own error enum; the application error
classifies by origin with `From` impls, so `?` does the mapping:

```rust
pub enum BlogError { Domain(PostError), NotFound(PostId), Conflict, Store(String) }
impl From<PostError> for BlogError { .. }
impl From<NotFound<PostId>> for BlogError { .. }
impl From<StoreError> for BlogError { /* Conflict | ConditionFailed => Conflict, Backend => Store */ }
```

**Rules.** An adapter (HTTP, GraphQL) maps the variants to status codes
without parsing strings. A domain error means nothing was saved.

---

## Command handling

### 21. The appender is the command's context

**Problem.** A command handler needs, besides the events it decides on, the
command's metadata, what it read across streams (for the conditional
commit), and whether retrying it is safe.

**Shape.** The gateway hands each run a fresh `Appender<E>`:

| | |
|---|---|
| `events.append(e)` (via `EventAppender`) | pending events, taken by the next `commit` |
| `events.commit(entity, &publisher)` | commits them to the entity's stream through the handler's publisher (NestJS's `aggregate.commit()`) |
| `events.metadata()` | the command's metadata, stored with every event |
| `events.source(store, criteria)` | reads by criteria and makes later commits conditional on it |
| `events.depend_on(condition)` | adds a condition (what `load_in` does) |
| `events.condition()` | what the commits check |

**Rules.** A handler that returns `Ok` with events still pending forgot a
`commit`: debug builds panic. An appender without publisher
(`Appender::new()`, `P = ()`) collects events but cannot commit: that does
not compile. See [Unit of work](unit-of-work.md#the-command-side)
for how this compares to Axon's `ProcessingContext` on the command side.

### 22. Re-run on a lost race, only if nothing was committed

**Shape.** When a commit fails with `Conflict { expected: Exact(_) }` or
`ConditionFailed` and no earlier commit of the run succeeded, the gateway runs
the handler again with a new appender, on fresh state, up to
`conflict_retries()` times (default 8):

```rust
#[cqrs::command_handler]
impl<R: Accounts> CommandHandler for ExecuteHandler<R> {
    async fn handle(..) -> .. { .. }
    fn conflict_retries(&self) -> u32 { 0 }   // let the caller retry instead
}
```

**Rules.** Handlers must have no side effects besides committing: they may run
more than once. Side effects belong in event handlers.

### 23. Your repository, the framework's publisher

**Problem.** A framework-defined `Repository<A>` trait dictates what every
repository looks like and pretends to integrate with storage on its own;
real repositories have the finders their use cases need, and an
application writes them anyway (as in NestJS).

**Shape.** Two concerns with two owners:

- **finding** is the application's: a trait with the methods it needs,
  returning `Loaded<A>`, and an adapter in infrastructure that rebuilds the
  aggregate from the event store with an `AggregateLoader`;
- **committing** is the framework's: an `EventPublisher<E>` (NestJS's
  `EventPublisher`), given to the gateway once and reached by handlers
  through their context, `events.commit(entity, &self.events)`.

```rust
// application
pub trait PostRepository: Send + Sync {
    fn find(&self, id: PostId) -> impl Future<Output = Result<Option<Loaded<Post>>, StoreError>> + Send;
}

// infrastructure
pub struct EventSourcedPostRepository<S> { loader: AggregateLoader<Post, S> }

impl<S: EventStore<PostEvent>> PostRepository for EventSourcedPostRepository<S> {
    async fn find(&self, id: PostId) -> Result<Option<Loaded<Post>>, StoreError> {
        self.loader.load(&id).await
    }
}

// composition root: the store is shared, the publisher is the unit's
let store = Arc::new(InMemoryEventStore::new());
let posts = Arc::new(EventSourcedPostRepository::new(Arc::clone(&store)));
let app = Cqrs::new(store, tokens).commands(|events| PostCommandGateway::new(posts, events)) /* ... */;
```

**Rules.** Handlers take only their services (`R: PostRepository`); the
framework never builds or passes the repository. The `AggregateLoader` is
the one way to load; with `with_snapshots` it starts from the newest
snapshot and saves a new one when a load replays `every` events or more.
Commits go through the publisher only: there is no other write path.

### 24. Snapshots as a decorator

**Shape.**

```rust
let posts = AggregateLoader::<Post, _>::new(store.clone()).with_snapshots(InMemorySnapshots::default(), 100);
let accounts = AggregateLoader::new(store).with_snapshots(PostgresSnapshotStore::new(pool), 100);
// in a repository adapter: posts.load(&id)
```

Loads start from the newest snapshot and replay the rest; a load that
replays `every` events or more saves a new one (commits never do: the
publisher serves every aggregate of an event type, the loader knows one).
A repository adapter for long-lived aggregates loads through it.

**Rules.** An optimization, never the truth: deleting snapshots only costs
time. The aggregate must be `Clone` (in memory) or serde (PostgreSQL);
bump `PostgresSnapshotStore::with_revision` when its shape changes, so old
snapshots are ignored.

### 25. Inline processing group

**Shape.** `InlinePublisher::new(store).with_processor((a, b))`: after
each commit, the publisher feeds the new envelopes to each handler (`handle`,
then `committed`), in the caller's task.

**Rules.** A failing handler stops seeing that batch, is
handed to the `on_failure` policy (`LogFailures` by default), and never undoes the
commit or blocks the others. No
retries, no tokens: for tests and simple applications. `Events` is a
processor too: as the engine's processor it only broadcasts the commit.

---

## Storage

### 26. Provided method, required hook

**Problem.** Every store would otherwise re-implement the same steps around
its write: skip empty batches, resolve tags, build envelopes.

**Shape.** The trait's public method is provided; implementations write
the minimal hook:

```rust
pub trait EventStore<E: DomainEvent> {
    fn append_if(..) -> .. { /* empty → Ok([]); tags resolved; append_tagged; envelopes built */ }
    fn append_tagged(&self, stream, expected, events: &[TaggedEvent<E>], metadata, condition)
        -> impl Future<Output = Result<Vec<Recorded>, StoreError>> + Send;   // position, sequence, time
    ..
}
```

**Rules.** Behavior that must be identical across stores lives in the
provided method, tested once.

### 27. Gap-aware cursor

**Problem.** Parallel appends take positions from a sequence, so a later
position can commit before an earlier one; a reader that only remembers
"the last position" skips the slow one forever.

**Shape.** `Cursor { position, gaps }`: the highest position seen plus the
positions below it not seen yet, each with the database horizon under which
it may still appear. Reads return the gaps' events when they show up;
a gap is given up only once every transaction that could fill it has ended.

**Rules.** Processors, subscriptions and live queries read through cursors;
`cursor.covers(p)` is "everything up to `p` was seen". Details in
[Storage § Global order](storage.md#global-order-and-the-cursor).

### 28. Locks derived from the domain

**Problem.** A global lock on the log serializes every writer; no lock
lets a conditional append miss a concurrent matching event.

**Shape.** Each append takes shared advisory locks on keys derived from its
events (each tag, each type); reading a criterion and a conditional append
take that criterion's keys exclusively. Only decisions on the same tags wait
for each other.

### 29. Upcaster chains

**Shape.**

```rust
let store = PostgresEventStore::<PostEvent>::new(pool, "posts").with_upcasters((
    upcast("post.created", 1, |payload| payload["Created"]["tags"] = json!([])),
    upcast("post.created", 2, |payload| payload["Created"]["draft"] = json!(true)),
));
```

Each `upcast(type, from_version, fn)` rewrites the JSON of one version into
the next; a tuple chains them. Stored rows never change.

### 30. Compile-checked SQL with an offline cache

**Shape.** Every statement in `cqrs-postgres` is `sqlx::query!`, checked
against the migrations at build time, through `CQRS_POSTGRES_URL` (never an
application's `DATABASE_URL`) or the committed `.sqlx/` cache. See
[Storage § SQL checked at compile time](storage.md#sql-checked-at-compile-time).

---

## Event handling

### 31. One event handler contract

**Problem.** Projections, policies and notifications are all "react to
committed events", but frameworks often give them different interfaces.

**Shape.** One trait, Axon 5's event handling component:

```rust
pub trait EventHandler<E: DomainEvent, Tx = ()>: Send + Sync {
    const REPLAY: bool = true;
    fn handle(&self, event: &EventEnvelope<E>, context: &mut ProcessingContext<'_, Tx>) -> impl Future<Output = Result<(), HandlerError>> + Send;
    fn committed(&self, event: &EventEnvelope<E>) -> impl Future<Output = ()> + Send { async {} }
    fn reset(&self, context: &mut ProcessingContext<'_, Tx>) -> impl Future<Output = Result<(), HandlerError>> + Send { async { Ok(()) } }
}
```

One `handle` matches on the event enum; the struct holds the
dependencies (a repository port, an emitter, a notifier).

**Rules.** `handle` may run more than once for an event (retries, at
least once delivery); make it idempotent or transactional. Side effects
that must not run twice go in `committed`, or in a handler with
`REPLAY = false`.

### 32. Processing context

**Shape.** `ProcessingContext<'_, Tx>` is what the processor hands every
handler with every event: the unit of work (`context.transaction()`, a
`&mut Tx`) and whether the event is a replay (`context.replay()`).

**Rules.** The context belongs to the processor; handlers only read it and
write in its transaction. See [Unit of work](unit-of-work.md).

### 33. The handler names its unit of work

**Problem.** A projection writing SQL must run in the processor's
transaction; one that writes to memory should run with any token store.

**Shape.** The `Tx` parameter. Name it to require it, leave it out to
accept any:

```rust
#[cqrs::projection(namespace = "views")]
impl EventHandler for AccountViews {        // requires PostgreSQL's transaction
    async fn handle(&self, event: &EventEnvelope<AccountEvent>, context: &mut ProcessingContext<'_, PgTransaction>) -> Result<(), HandlerError> {
        sqlx::query("..").execute(&mut **context.transaction()).await?;
        Ok(())
    }
}

#[cqrs::projection(namespace = "post-summaries")]
impl<S: PostSummaryRepository> EventHandler for PostSummaryProjection<S> {
    async fn handle(&self, event: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        // no Tx named: the macro makes it generic, so it runs in memory and on PostgreSQL
    }
}
```

**Rules.** Running a `PgTransaction` handler with the in-memory token store
is a compile error: `Events::processor` requires
`H: EventHandler<E, C::Transaction>`.

### 34. Projection namespace

**Shape.** `#[cqrs::projection(namespace = "post-summaries")]` on the impl
also implements `Projection { const NAMESPACE }`, the processor's name and
the key of its tokens (Axon's `@Namespace`); `events.projection(p)` uses it.

**Rules.** Renaming a namespace starts a new processor from the beginning
(new tokens). `Arc<P>` and `Intercepted<P, I>` keep `P`'s namespace.

### 35. Post-commit step

**Problem.** Pushing a query update, or calling an external system, from
inside `handle` happens before the commit: a rollback would leave the
outside world ahead of the read model.

**Shape.** `committed(&self, event)` runs after the unit of work commits,
only if it did:

```rust
async fn committed(&self, event: &EventEnvelope<PostEvent>) {
    let Ok(id) = post_of(event) else { return };
    if let Ok(Some(summary)) = self.summaries.find(id).await {
        self.updates.find.emit(|query| query.id == id, Some(summary));
    }
}
```

**Rules.** Cannot fail (there is nothing left to roll back); a crash
between the commit and `committed` skips it. Inline processing calls it
too, after `handle`.

### 36. Replay control

**Shape.** `processor.reset()` moves the tokens back to the start and calls
the handler's `reset` in a unit of work (clear the read model); the events
stored so far are then replayed. A handler that must not see them again
sets `const REPLAY: bool = false`:

```rust
#[cqrs::projection(namespace = "notify-on-publish")]
impl<N: Notifier> EventHandler for NotifyOnPublish<N> {
    const REPLAY: bool = false;   // a notification already went out
    async fn handle(..) -> .. { .. }
}
```

**Rules.** Every instance must be stopped before a reset
(`ResetError::Running` otherwise). Inside a group, only the members with
`REPLAY = false` skip replayed events.

### 37. Processing groups

**Shape.** A tuple of handlers is a handler: one processor, one token, one
unit of work per event for all of them.

**Rules.** A failure of any member rolls back the whole group's unit and
retries it: members of a group must be fine with being handled again
(transactional writes are). Put independent side effects in separate
processors.

### 38. Segments and claims

**Shape.** `TrackingOptions { segments: 8, ..Default::default() }`: the log
is split by aggregate (`segment_of(stream, segments)`), each segment
handled in order by the instance that claims it; one shared reader routes
events to segment workers.

**Rules.** Events of one aggregate are always in order; different
aggregates run in parallel. Instances share segments through the token
store; a dead instance's claims expire and are taken over
([Event processing](event-processing.md#segments-claims-and-instances)).

### 39. Batches per unit of work

**Problem.** One transaction per event makes a busy read model pay a commit
(an fsync on PostgreSQL) per event: the processor falls behind the writers.

**Shape.** `TrackingOptions { batch_size: 100, .. }` (Axon's `batchSize`):
a segment worker takes the events already waiting for it, up to the batch
size, and handles them in one unit of work with one token update:

```text
begin → handle e1 → handle e2 → … → handle eN → token (past eN) → commit → committed e1..eN
```

**Rules.** It never waits to fill a batch, so it adds no latency: under
light load batches are single events, under a backlog they fill up. If any
event of a batch fails, the whole batch rolls back and its events are
handled one at a time, each in its own unit of work with the usual retries
and dead letters, so a poison event is isolated exactly as without
batching. A batch holding an event of a parked aggregate goes one at a
time too. Handlers see nothing different, except that writes outside the
transaction may be repeated when a batch falls back (as with any retry).
Default 1.

### 40. Sequenced dead letters

**Shape.** `.with_dead_letters(queue, after)`: an event that failed
`after` times is parked with every later event of its aggregate, and the
segment moves on. Once fixed, `processor.retry_dead_letters()` replays
each sequence in order, each event in a unit of work.

**Rules.** Order per aggregate is kept: a parked aggregate stays parked
until its oldest letter succeeds.

### 41. Inline or tracking, same handler

**Shape.** The same `EventHandler` runs inline
(`InlinePublisher::with_processor`) or in a tracking processor
(`events.projection(h).spawn()`); only the composition root changes.

**Rules.** Inline, `Tx = ()` and there are no retries; tracking, `Tx` is
the token store's and failures retry.

---

## Interceptors

### 42. Dispatch interceptor

**Shape.** Around a whole gateway, generic over every message:

```rust
impl<M: Message> Interceptor<M> for Audit {
    async fn intercept<N, F>(&self, message: M, metadata: Metadata, next: N) -> Result<M::Output, M::Error>
    where
        N: FnOnce(M, Metadata) -> F + Send,
        F: Future<Output = Result<M::Output, M::Error>> + Send,
    {
        let correlation_id = metadata.get("correlation_id").cloned();
        let result = next(message, metadata).await;
        self.record(std::any::type_name::<M>(), correlation_id, result.is_ok());
        result
    }
}

let commands = Intercepted::new(PostCommandGateway::new(posts, publisher), Audit(log));
```

**Rules.** It may change the message or metadata, refuse (return an error
without calling `next`), or inspect the result. To refuse with a typed
error, bound the impl: `impl<C: Command> Interceptor<C> for RequireUser where C::Error: From<Unauthorized>`.

### 43. Event interceptor with a chain

**Shape.** Around an event handler, inside its unit of work, with the
handler's context (Axon's `chain.proceed(context)`):

```rust
impl<E: DomainEvent, Tx: Send> EventInterceptor<E, Tx> for Measured {
    async fn intercept<N: EventChain<Tx>>(
        &self,
        event: &EventEnvelope<E>,
        context: &mut ProcessingContext<'_, Tx>,
        next: N,
    ) -> Result<(), HandlerError> {
        let started = Instant::now();
        let result = next.proceed(context).await;
        self.record(started.elapsed(), &result);
        result
    }
}

events.projection(Intercepted::new(projection, Measured(metrics))).spawn();
```

**Rules.** What it writes in `context.transaction()` commits or rolls back
with the handler. Not calling `next` skips the event (it counts as
handled). An interceptor that writes names its `Tx`
(`impl EventInterceptor<E, PgTransaction> for AuditRows`); others stay
generic. `EventChain::proceed` consumes the chain: the handler runs at most
once per attempt.

### 44. Interceptor chains are tuples

**Shape.** `Intercepted::new(h, (Outer, Middle, Inner))`: the first is
outermost. Static nesting; one interceptor that only calls `next` costs
about nothing (see [Performance](performance.md#micro-benchmarks)).

---

## Read side and streams

### 45. Read-model repository as an application port

**Shape.** The application declares what it stores and reads; the
projection and the query handlers share the port; infrastructure
implements it:

```rust
pub trait PostSummaryRepository: Send + Sync {
    fn find(&self, id: PostId) -> impl Future<Output = Result<Option<PostSummary>, StoreError>> + Send;
    fn published(&self) -> impl Future<Output = Result<Vec<PostSummary>, StoreError>> + Send;
    fn save(&self, summary: PostSummary) -> impl Future<Output = Result<(), StoreError>> + Send;
    fn update(&self, id: PostId, change: impl FnOnce(&mut PostSummary) + Send) -> impl Future<Output = Result<bool, StoreError>> + Send;
    fn clear(&self) -> impl Future<Output = Result<(), StoreError>> + Send;
}
```

**Rules.** `update` with a closure is Axon's managed entity: change in
place, no find-clone-save. Keep the write model's repository
([pattern 23](#23-your-repository-the-frameworks-publisher)) and the read
model's repository separate types: they differ in storage and consistency.

### 46. One emitter per query type

**Shape.**

```rust
#[derive(Default, cqrs::QueryUpdates)]
pub struct PostUpdates {
    pub find: QueryUpdateEmitter<FindPost>,
    pub published: QueryUpdateEmitter<ListPublished>,
}
// in a projection's `committed`:
self.updates.find.emit(|query| query.id == id, Some(summary));
```

**Rules.** No `dyn`, so no single emitter for every query: a struct of
typed emitters, shared in an `Arc` by the projections and the subscription
queries. `emit` takes a filter over the subscribed queries.

### 47. Subscription queries that see your own writes

**Shape.**

```rust
let queries = SubscriptionQueries::new(PostQueryGateway::new(summaries), updates)
    .after_projection(events.clone(), LocalProgress::of(&projection_handle));
let stream = queries.subscribe(FindPost { id });   // initial result, then updates
```

**Rules.** The initial result is read once the projection handled every
event committed when the subscription started, so a client subscribing
right after its command sees it. Updates are local to the process; with
several instances use `LiveQueries` with `GlobalProgress`.

### 48. Queries that know what refreshes them

**Shape.**

```rust
impl RefreshedBy<PostEvent> for FindPost {
    fn refreshed_by(&self, event: &EventEnvelope<PostEvent>) -> bool {
        event.stream.id() == self.id.to_string()
    }
}
```

`LiveQueries` re-runs a subscribed query after the projection handled an
event it is refreshed by (once per burst). `true` is always correct, only
slower.

### 49. The event feed is a port

**Shape.** `EventFeed<E>::subscribe(after) -> impl Stream<..> + use<Self, E>`:
every committed event after a position, exactly once, in order, then live.
`Events` implements it; the API names the port.

```rust
let stream = app.events().subscribe(after).try_filter(move |event| ready(event.stream.id() == id));
```

**Rules.** The stream does not borrow the feed, so it can be returned
from a GraphQL resolver or moved into a task. Reconnecting with the last
position seen misses nothing.

### 50. Policies send commands

**Shape.** A saga step is an event handler holding a gateway:

```rust
#[cqrs::projection(namespace = "transfers")]
impl<B: CommandGateway<Execute> + 'static> EventHandler for TransferPolicy<B> {
    async fn handle(&self, event: &EventEnvelope<AccountEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        if let AccountEvent::TransferredOut { to, amount } = &event.payload {
            self.bank.send(Execute { account: to.clone(), command: BankCommand::Deposit { amount: *amount } }).await?;
        }
        Ok(())
    }
}
```

**Rules.** It may run more than once: make the command idempotent (or
dedupe by the event's position). A long-running process with state is a
read model of its own, or a task over `EventFeed::subscribe`.

---

## Testing

### 51. Given / when / then on aggregates

```rust
AggregateFixture::<Post>::new(ID)
    .given([created(), PostEvent::Published])
    .when(|post, events| post.publish(events))
    .expect_error(PostError::AlreadyPublished)
    .expect_no_events();
```

No store, no async: past events in, new events or an error out, plus
`expect_state` to check the state after.

### 52. No runtime in tests

`cqrs::testing::block_on` runs a future on the current thread. Inline
wiring plus the in-memory store makes command → projection → query flows
synchronous and deterministic. Tracking processors need Tokio; use
`#[tokio::test(start_paused = true)]` so retries' backoff takes no time.

### 53. `compile_fail` doctests as guarantees

A guarantee enforced by types is proven by code that must not compile:

```rust
/// ```compile_fail,E0277
/// let (blog, _, _) = blog::in_memory();
/// let _ = blog.commands().send(FindPost { id: PostId(1) });   // a query sent as a command
/// ```
```

The blog's `lib.rs` keeps one per guarantee (decision assigning state,
`&mut` to a loaded aggregate, missing handler, duplicated handler, enum
variant without handler, non-handler field).

---

## Composition

### 54. Ports generated from the message enums

**Problem.** Code that sends every command of a module (an API, a test
double, the composition root's return type) would list one
`CommandGateway<_>` bound per command, and repeat the list wherever it is
needed.

**Shape.** The message enum already lists them; its derive generates the
port:

```rust
#[derive(cqrs::Commands)]
#[commands(error = BlogError, port = PostCommands)]
pub enum PostCommand { CreatePost(CreatePost), RetitlePost(RetitlePost), PublishPost(PublishPost) }

#[derive(cqrs::Queries)]
#[queries(error = BlogError, port = PostQueries, subscriptions = PostSubscriptions)]
pub enum PostQuery { FindPost(FindPost), ListPublished(ListPublished) }
```

`PostCommands` is a trait bounded by `CommandGateway` of every command
and of the enum, with a blanket impl for every type that satisfies them;
`PostQueries` the same with `QueryGateway`; `PostSubscriptions` with
`QuerySubscriptions`. They are trait aliases, written the stable way.

**Rules.** Name them in bounds and return types: `impl PostCommands`,
`type Commands: PostCommands`. Adding a command to the enum adds it to
the port, and every implementation is checked against it.

### 55. The CQRS unit: a typed builder

**Problem.** Wiring an application by hand means building the events,
the publisher, the gateways with it, the processors, the subscriptions
over the projections' progress, and naming every resulting type in aliases
and structs. Switching stores means touching all of it.

**Shape.** `Cqrs::new(event_store, token_store)`; each step takes a value
and changes the builder's type:

```rust
let store = PostgresEventStore::new(pool.clone(), "posts");                     // or Arc::new(InMemoryEventStore::new())
let posts = Arc::new(EventSourcedPostRepository::new(store.clone()));    // yours, over the same store

let app = Cqrs::new(store, PostgresTokenStore::new(pool))  // both stores: the constructor's
    .commands(|events| PostCommandGateway::new(posts, events)) // the unit hands over its publisher
    .intercept(Audit(audit))                               // Intercepted<gateway, Audit>
    .queries(PostQueryGateway::new(summaries.clone()))
    .options(TrackingOptions { segments: 8, batch_size: 100, ..TrackingOptions::default() })
    .projection(PostSummaryProjection::new(summaries, updates.clone()))   // started; subscriptions wait for it
    .policy(NotifyOnPublish::new(notifier))                               // started; not waited for
    .subscriptions(updates)
    .build();                                              // Cqrs<C, Q, E>

app.commands().send(CreatePost { .. }).await?;
app.queries().subscribe(FindPost { id });
app.settled().await;                                       // every processor caught up
app.stop().await;
```

**Rules.**

- **What can break is in the constructor.** The event store and the token
  store are required arguments: there is no forgotten `.tokens()` leaving
  production on in-memory tokens. The unit makes its events (`Events`)
  from them, and the events are the publisher: append, then broadcast.
- **The publisher is a handler's constructor argument, handed over by the
  unit.** `.commands(|events| gateway(events))` gives the closure the
  publisher it made (shared: an `Arc`); handlers commit with
  `events.commit(entity, &self.events)`. The
  repository is the application's business: the unit never builds or
  passes it. `Cqrs::from_publisher(publisher)` starts from a hand-built
  publisher instead (e.g. inline processing, no runtime), and its unit
  takes `.events(events)` explicitly when it has processors.
- **Order is typed.** `.commands` is set once; `.projection`, `.policy`
  and `.subscriptions` need events; a projection whose context names
  `PgTransaction` is accepted only when the token store is PostgreSQL's.
  The order is proven by `compile_fail` doctests on `CqrsBuilder`.
- **Nothing is boxed.** The unit holds the concrete gateway types; sending
  through `app.commands()` costs what sending through the gateway with its
  publisher does (15.3 ns both, `benches/micro.rs`, `unit`).
- **Switching stores changes the inputs only**: the two stores
  (`start()` in memory vs `start_postgres(pool)` in the guide).
- **Return it by its ports**: `Cqrs<impl PostCommands, impl PostQueries + PostSubscriptions, PostEvents>`.
  A presentation trait implemented for any `Cqrs` with those ports serves
  every wiring.
- `.options` applies to the processors started after it; a processor
  configured beyond the options (dead letters) is started outside the
  unit, and stopped by its owner.
