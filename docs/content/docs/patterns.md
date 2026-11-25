---
title: "Patterns"
sidebar_position: 30
---

The library is made of small, repeatable patterns. Each section below names
one, says what problem it solves, shows its shape, and lists its rules.
Applications use the same patterns, so the sections double as a style
guide. Snippets marked `// src/...` or `// tests/...` come from
`examples/blog`.

- [Static dispatch](#static-dispatch): 1–6
- [Messages and gateways](#messages-and-gateways): 7–10
- [Domain](#domain): 11–16
- [Command handling](#command-handling): 17–20
- [Storage](#storage): 21–23
- [Event handling](#event-handling): 24–30
- [Interceptors](#interceptors): 31–32
- [Read side and streams](#read-side-and-streams): 33–35
- [Testing](#testing): 36–38
- [Composition](#composition): 39

---

## Static dispatch

### 1. Trait with `impl Future` returns

**Problem.** Async traits usually box their futures (`Pin<Box<dyn Future>>`):
an allocation and a virtual call per call, and `Send` hidden.

**Shape.** Every async contract returns `impl Future<Output = ...> + Send`;
implementations write a plain `async fn`.

```rust
// src/application/commands/post_repository.rs
fn find(&self, id: PostId, tx: &mut Self::Tx)
    -> impl Future<Output = Result<Option<Loaded<Post>>, StoreError>> + Send;

// src/infrastructure/event_sourced_post_repository.rs
async fn find(&self, id: PostId, (): &mut ()) -> Result<Option<Loaded<Post>>, StoreError> {
    self.loader.load(&id).await
}
```

**Rules.** The `+ Send` makes `tokio::spawn`-ability part of the contract:
an implementation holding a non-`Send` value across an `.await` does not
compile. Such traits are not object-safe, which is the point: nothing here
is a trait object.

### 2. Tuples as heterogeneous collections

**Problem.** "A list of handlers of different types" is
`Vec<Box<dyn Handler>>` in most frameworks.

**Shape.** The trait is implemented for tuples (up to 8 members); each
member keeps its type, and the tuple calls them in order.

```rust
events.processor("read-models", (Search::new(..), Stats::new(..))).spawn();   // a processing group
Intercepted::new(gateway, (Tracing, Timing, RequireUser));                    // first outermost
store.with_upcasters((upcast("post.created", 1, ..), upcast("post.created", 2, ..)));
(posts, users).settled().await;                                               // one unit per root
```

**Rules.** A tuple of event handlers is a group everywhere: one token, one
unit of work, all or none, the first failure stopping the rest; in a
replay, members with `REPLAY = false` are skipped. Inline, where each
handler should see every event whatever the others do, wrap it:
`Isolated((a, b))`.

### 3. Wrappers forward every method

**Problem.** Sharing a component (`Arc<T>`, `&T`) must not change its
behavior, but a blanket impl that forwards only the required methods
silently drops the overrides of provided ones.

**Shape.** Every forwarding impl forwards every method, checked by the
compiler:

```rust
// Forwards every method, so an override of a provided one is never lost.
#[deny(clippy::missing_trait_methods)]
impl<E: DomainEvent, U: UnitMode, T: UnitOfWork<E, U>> UnitOfWork<E, U> for std::sync::Arc<T> { .. }
```

**Rules.** Applies to every trait with provided methods (`EventHandler`,
`UnitOfWork`, `EventPublisher`, `Dispatch`...).

### 4. Type parameters for extension points

**Problem.** Pluggable parts (store, tokens, transactions, snapshots) are
usually injected as trait objects.

**Shape.** Type parameters with defaults, swapped by builder methods that
change the type:

```rust
pub struct AggregateLoader<A, S, N = ()> { .. }                // store, snapshots
pub struct Events<E, S, C = InMemoryTokenStore, M = ()> { .. } // store, tokens, transactions

AggregateLoader::<Post, _>::new(store).with_snapshots(InMemorySnapshots::default(), 50);
Events::new(store).with_tokens(tokens).with_transactions(transactions);
```

**Rules.** `()` is the "nothing" of most ports (no snapshots, no
transaction, no upcaster). The composition root never names the resulting
types: the [builder](#41-the-cqrs-unit-a-typed-builder) infers them.

### 5. Compile errors that read like documentation

**Problem.** Static routing turns mistakes into trait errors, cryptic by
default.

**Shape.** `#[diagnostic::on_unimplemented]` on the routing traits, and
spanned errors from the macros:

```text
error[E0277]: `Clock<P>` lists `Ring` in `handles(...)` but has no handler for it
   |
15 | struct Clock<P> {
   |        ^^^^^^^^ no `#[cqrs::command_handler]` / `#[cqrs::query_handler]` impl for `Ring`
```

**Rules.** Add one wherever a user meets a trait bound instead of a
function. The macros' messages are checked by UI tests
(`crates/cqrs/tests/ui`).

### 6. Macros only write what a user could

**Problem.** Macros that inject fields or rewrite types make code
unreadable without expanding them, and lock users in.

**Shape.** Every macro emits trait impls and associated types; each one's
rustdoc shows the hand-written equivalent:

| Macro | Writes |
|---|---|
| `#[cqrs::aggregate]` | `impl Aggregate` |
| `#[cqrs::command]`, `#[cqrs::query]` | `impl Message` and the `Command` / `Query` marker |
| `#[derive(cqrs::DomainEvent)]` | `event_type`, `event_version`, `TagResolver` from `#[tag]` fields |
| `#[cqrs::command_gateway]`, `#[cqrs::query_gateway]` | `impl Dispatch<M>` per message listed, the check that each has a handler, the generated trait |
| `#[cqrs::command_handler]`, `#[cqrs::query_handler]` | the `Handles<M>` marker; for commands, `type Event` and `type Unit` read from `handle` |
| `#[cqrs::projection]` | `impl Projection`, `impl Route<Tx>` (namespace to handler), the updates' channels |
| `#[cqrs::event_handler]` | the event and unit-of-work generics; with `namespace`, that namespace's handler |

**Rules.** Nothing a macro does is required: a domain that wants no
attributes implements `Aggregate` and `DomainEvent` itself.

---

## Messages and gateways

### 7. The message owns its output and error

**Problem.** If handlers declare what they return, a caller must know which
handler answers a message to know what comes back.

**Shape.**

```rust
// src/application/commands/create_post.rs
#[cqrs::command(output = PostId, error = BlogError)]   // output defaults to ()
pub struct CreatePost { pub title: String }

// src/application/queries/find_post.rs
#[cqrs::query(output = Option<PostSummary>, error = BlogError)]
pub struct FindPost { pub id: PostId }
```

`gateway.send(CreatePost { .. })` returns `Result<PostId, BlogError>`,
whoever handles it.

**Rules.** Sending a query with `.send` (or a command with `.query`) is a
compile error: the `Command` and `Query` markers keep the sides apart.

### 8. The gateway is the handler

**Problem.** Registries keyed by `TypeId` fail at runtime when a handler is
missing or registered twice; one class per handler multiplies the
dependencies to pass around.

**Shape.** One struct per side holds what its use cases need and lists
the messages it routes; each message has its own impl, in the use case's
file:

```rust
// src/application/commands/mod.rs
#[cqrs::command_gateway(commands = PostCommands, handles(CreatePost, RetitlePost, PublishPost))]
pub struct PostCommandGateway<R, P> {
    posts: R,
    #[gateway(unit)]
    events: P,
}

// src/application/commands/publish_post.rs
#[cqrs::command_handler]
impl<R: PostRepository, P> CommandHandler<PublishPost> for PostCommandGateway<R, P> {
    async fn handle(
        &self,
        command: &PublishPost,
        ctx: &mut CommandContext<PostEvent>,
        tx: &mut R::Tx,
    ) -> Result<(), BlogError> {
        let post = self.posts.find(command.id, tx).await?.ok_or(NotFound(command.id))?;
        post.publish(ctx)?;
        let post = ctx.record(post);
        self.posts.save(&post, tx).await?;
        Ok(())
    }
}
```

Queries are the same: `#[cqrs::query_gateway(queries = PostQueries, handles(FindPost, ListPublished))]`
on `PostQueryGateway<M>`, one `#[cqrs::query_handler] impl QueryHandler<FindPost>`
per query.

**Rules.** A message listed without its impl, or listed twice, does not
compile where the gateway is declared; one not listed is E0277 where it is
sent. A command gateway needs a `#[gateway(unit)]` field: the
[unit of work](#17-the-handler-records-the-framework-commits) its commands
run in. Commands are taken by reference: a handler may run again.

### 9. Traits generated by the gateways

**Problem.** Code that sends every command of a module (an API, a test
double, a return type) would repeat one `CommandGateway<_>` bound per
command wherever it is needed.

**Shape.** `commands = PostCommands` generates a trait bounded by
`CommandGateway` of each command listed, implemented by everything that
routes them; `queries = PostQueries` the same with `QueryGateway`, and the
projection's `subscriptions = PostSubscriptions` with `QuerySubscriptions`.
Code that needs fewer names only those:

```rust
// src/presentation/mod.rs
pub trait BlogApi: Send + Sync + 'static {
    type Commands: PostCommands;
    type Queries: PostQueries + PostSubscriptions;
    type Events: EventFeed<PostEvent>;
    // ..
}

// tests/flow.rs
pub async fn create_and_publish<G>(gateway: &G, title: &str) -> Result<PostId, BlogError>
where
    G: CommandGateway<CreatePost> + CommandGateway<PublishPost>,
{ .. }
```

**Rules.** Whatever stands behind the gateway (interceptors, a unit) still
implements them, so code written against them compiles against the
in-memory app, the PostgreSQL app and test doubles alike.

### 10. Metadata as correlation data

**Problem.** Who asked, and why, should travel with the events a command
causes, without every handler passing it along.

**Shape.** Send with metadata; every event recorded in that run stores it.

```rust
// src/presentation/http.rs
let mut metadata = Metadata::new();
metadata.insert("correlation_id".to_owned(), id.to_owned());
// in a resolver
app.commands().send_with(PublishPost { id }, metadata).await?;
```

**Rules.** Interceptors see and may change it before the handler
([pattern 33](#33-dispatch-interceptor)); handlers read it with
`ctx.metadata()`; event handlers find it in `event.metadata`.

---

## Domain

### 11. Decisions take `&self` and an appender

**Problem.** Aggregates that mutate themselves in command methods can
change state without recording an event, and carry a pending-events buffer
that leaks into serialization.

**Shape.** State changes only in `evolve` (here, `apply`); decisions borrow
immutably and append. A creation has no instance to decide on, so it is an
associated function:

```rust
// src/domain/post/mod.rs
impl Post {
    fn apply(&mut self, event: &PostEvent) {                       // the only mutation
        match event {
            PostEvent::Created { title, .. } | PostEvent::Retitled { title } => self.title.clone_from(title),
            PostEvent::Published => self.published = true,
        }
    }

    pub fn create(id: PostId, title: impl Into<String>, events: &mut impl EventAppender<PostEvent>)
        -> Result<(), PostError> {
        let title = valid_title(title.into())?;
        events.append(PostEvent::Created { id, title });
        Ok(())
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
too. A decision sees the state from before it ran; assigning a field in
one is a compile error (E0594). `EventAppender<E>` is implemented for every
`Extend<E> + Send`, so a test passes a `Vec`, and a domain can take
`impl Extend<PostEvent>` and not depend on the library.

### 12. Declared aggregate root

```rust
// src/domain/post/mod.rs
#[cqrs::aggregate(name = "post", event = PostEvent)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Post { id: PostId, title: String, published: bool }
```

Writes `impl Aggregate`: `TYPE` from `name`, `Id` from the `id` field (or
the one marked `#[aggregate(id)]`), `initial` with the other fields at
`Default`, `evolve` calling `apply`.

**Rules.** `TYPE` prefixes stream ids (`post-42`): changing it orphans
stored streams. `Id: Display` renders the suffix and must be stable; a
projection parses it back with `event.aggregate_id::<Post>()`.

### 13. The loaded aggregate carries its version

**Problem.** The version used for optimistic concurrency is persistence
data; in the domain struct it mixes concerns, and reusing a stale
aggregate is a classic bug.

**Shape.** `Loaded<A>` wraps the state with the version it was read at and
derefs to `&A` only (no `&mut`: E0596). `ctx.record(entity)` takes it by
value and returns it evolved, at its new version. A creation records
`New<A>`, which expects the stream not to exist:

```rust
// src/application/commands/create_post.rs
let id = self.posts.next_id(tx).await?;
Post::create(id, command.title.as_str(), ctx)?;
let post = ctx.record(New::<Post>(id));           // Loaded<Post>, after `Created`
self.posts.save(&post, tx).await?;
```

**Rules.** The version travels from `find` to the append without the
domain seeing it. A repository that stores state builds one with
`Loaded::at(post, version)`. To create what is missing (a counter starting
at zero), decide on `find(..).await?.unwrap_or_else(|| New(id).into())`.

### 14. Stable event names, versions and tags

```rust
// src/domain/post/event.rs
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, cqrs::DomainEvent)]
pub enum PostEvent {
    #[event(name = "post.created")]
    Created { id: PostId, title: String },
    #[event(name = "post.retitled")]
    Retitled { title: String },
    #[event(name = "post.published")]
    Published,
}

// crates/cqrs/tests/dcb.rs
#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum SchoolEvent {
    #[event(name = "student.subscribed")]
    Subscribed { #[tag] course: u32, #[tag] student: u32 },
    // ..
}
```

**Rules.** The stored type is the `name`, so renaming a variant is free.
Each variant is versioned on its own (`#[event(version = 2)]`, default 1):
bump it when the payload's shape changes and add an
[upcaster](#24-upcaster-chains). `#[tag]` fields become the event's tags
(keyed by field name, or `#[tag = "key"]`); without any, it has none. To
resolve tags by hand, implement `DomainEvent` and `TagResolver` yourself.

### 15. Aggregates built from tags (dynamic consistency boundaries) {#19-aggregates-built-from-tags-dynamic-consistency-boundaries}

**Problem.** A rule spanning two aggregates (a course's capacity and a
student's course limit) forces a bigger aggregate or a saga.

**Shape.** Each aggregate is built from the events tagged with its id,
whatever stream holds them; the handler loads both with `load_in`, and the
append is conditional on neither having changed:

```rust
// crates/cqrs/tests/dcb.rs
#[cqrs::aggregate(event = SchoolEvent, tag = "course")]
struct Course { id: u32, capacity: usize, students: Vec<u32> }

#[cqrs::aggregate(event = SchoolEvent, tag = "student")]
struct Student { #[aggregate(id)] id: u32, courses: usize }

// in the handler, for each course; `courses` and `students` are AggregateLoaders
let course = self.courses.load_in(&course, events).await?.ok_or(Error::NoSuchCourse)?;
let student = self.students.load_in(&command.student, events).await?;   // read only: still checked
course.subscribe(command.student, student.as_deref(), events)?;
events.record(course);   // ConditionFailed at the append if either changed
```

Without an aggregate, `ctx.source(&store, EventCriteria::tagged([..]))`
reads by criteria and conditions the run's appends the same way.

**Rules.** `load_in` adds a condition even when the aggregate does not
exist yet: absence is a fact too. A failed condition is a lost race: the
handler runs again. On PostgreSQL, decisions on different tags run in
parallel, on the same tag they take turns
([pattern 23](#23-postgresql-cursor-locks-checked-sql)).

### 16. Domain errors are values, not store errors

```rust
// src/application/error.rs
pub enum BlogError { Domain(PostError), NotFound(PostId), Conflict, Store(String) }
impl From<PostError> for BlogError { .. }
impl From<NotFound<PostId>> for BlogError { .. }
impl From<StoreError> for BlogError { /* Conflict | ConditionFailed => Conflict, other => Store */ }
```

**Rules.** The domain returns its own enum; the application error
classifies by origin, so `?` maps it and an adapter picks a status code
without parsing strings. A domain error means nothing was appended. A
command's error converts `From<StoreError>`: the unit of work's failures
reach the caller through it.

---

## Command handling

### 17. The handler records, the framework commits

**Problem.** A handler that commits can commit twice, commit half, or
forget; a repository that stores state must write in the events'
transaction.

**Shape.** The gateway hands each run a fresh `CommandContext<E>`:

| | |
|---|---|
| `ctx.append(e)` | an event, until `record` assigns it |
| `ctx.record(entity)` | the events so far are `entity`'s; returns it evolved (nothing written yet) |
| `ctx.metadata()` | the command's metadata |
| `ctx.source(&store, criteria)`, `ctx.depend_on(condition)` | conditions the appends on what was read |

Once `handle` returns `Ok`, the unit of work appends what was recorded and
commits. The signature picks its mode:

- **no `tx` argument**: `Autocommit`, one append statement, one entity;
- **`tx: &mut Tx`**: `InTransaction<Tx>`. The unit opens the transaction,
  the handler and its repositories write in it, the events are appended
  last, and everything commits together, or nothing does.

**Rules.** The handler never commits nor announces: once committed, the
unit hands the events to the processors. Events appended but not recorded
are a forgotten `record` (debug builds panic). Recording two entities in
`Autocommit` is an error: take the `tx`. See
[Unit of work § The command side](unit-of-work.md#the-command-side).

### 18. Re-run on a lost race, without side effects

**Shape.** When the append fails with a version conflict, or
`ConditionFailed`, the unit rolls back and runs the handler again on fresh
state, up to `conflict_retries()` times (default 8):

```rust
#[cqrs::command_handler]
impl<R: PostRepository, P> CommandHandler<RetitlePost> for PostCommandGateway<R, P> {
    async fn handle(..) -> Result<(), BlogError> { .. }
    fn conflict_retries(&self) -> u32 { 0 }   // let the caller retry instead
}
```

**Rules.** `New` on an existing stream is not a race: it fails at once. In
a unit that cannot roll back (in memory), a run that appended part of its
writes is not re-run: the conflict reaches the caller. Because a handler
may run more than once, it has no side effects besides what it writes in
`tx`: an e-mail or a broker message goes to a
[namespace that does not replay](#28-side-effects-are-namespaces-that-do-not-replay).

### 19. Your repository, in the unit's transaction

**Problem.** A framework-defined `Repository<A>` dictates what every
repository looks like; real ones have the finders their use cases need,
and store state their own way.

**Shape.** The application declares the port, working in the unit's
transaction; infrastructure adapts it:

```rust
// src/application/commands/post_repository.rs
pub trait PostRepository: Send + Sync {
    type Tx: Send;
    fn next_id(&self, tx: &mut Self::Tx) -> impl Future<Output = Result<PostId, StoreError>> + Send;
    fn find(&self, id: PostId, tx: &mut Self::Tx)
        -> impl Future<Output = Result<Option<Loaded<Post>>, StoreError>> + Send;
    fn save(&self, post: &Loaded<Post>, tx: &mut Self::Tx) -> impl Future<Output = Result<(), StoreError>> + Send;
}
```

`EventSourcedPostRepository` (`Tx = ()`) rebuilds posts with an
`AggregateLoader`; its `save` writes nothing. `SeaOrmPostRepository`
(`Tx = PgTransaction`) keeps each post in a table, with SeaORM, in the
transaction the events are appended in; the row keeps the stream version,
so concurrent changes still conflict.

**Rules.** Handlers take only their services (`R: PostRepository`); the
library never builds the repository. Events are appended by the unit only.

### 20. Commands sent later

**Problem.** "Remind the author in three days", "close the auction at noon":
a decision now, a command later. Kept in memory, a schedule is lost at the
next deploy; sent from a timer in the handler, it is a side effect a re-run
repeats.

**Shape.** A `CommandSchedule<C, Tx>` per command type keeps them; the
handler schedules within its unit of work, and `deliver_scheduled` sends
what is due through the gateway:

```rust
#[cqrs::command_handler]
impl<R: PostRepository, S: CommandSchedule<RemindAuthor, R::Tx>, P> CommandHandler<CreatePost> for PostCommandGateway<R, S, P> {
    async fn handle(&self, command: &CreatePost, ctx: &mut CommandContext<PostEvent>, tx: &mut R::Tx)
        -> Result<PostId, BlogError>
    {
        // ... decide and record ...
        let due = SystemTime::now() + Duration::from_secs(3 * 24 * 3600);
        self.reminders.schedule_in(tx, due, &RemindAuthor { id }, ctx.metadata()).await?;
        Ok(id)
    }
}

// On every instance: claims what is due, sends it through `gateway` (any
// CommandGateway<RemindAuthor>), removes what was handled.
deliver_scheduled(pg.schedule::<RemindAuthor>("reminders"), gateway, DeliveryOptions::default(), shutdown).await;
```

**Rules.** The schedule commits with the events, or not at all (a re-run
after a lost race schedules once). Delivery is at least once and the
command's handler decides again on the state it finds then; a failure is
tried again with backoff and given up after `max_attempts`, kept with its
error. On PostgreSQL (`PostgresSchedule`, table `cqrs_scheduled`) times are
the database's and deliverers on several instances claim different commands
(`FOR UPDATE SKIP LOCKED`). `cancel` and `reschedule` take the
`ScheduleToken` that scheduling returned. (Axon 5 removed its schedulers;
this is the shape of Axon 4's `CommandScheduler`.)

### 21. Snapshots as a decorator

```rust
// src/config.rs
EventSourcedPostRepository::new(store.clone()).with_snapshots(InMemorySnapshots::default(), SNAPSHOT_EVERY)
// inside: AggregateLoader::with_snapshots; on PostgreSQL, PostgresSnapshotStore::new(pool)
```

A load starts from the newest snapshot and replays the rest; one that
replayed `every` events or more saves a new one. Appends never do.

**Rules.** An optimization, never the truth: deleting snapshots only costs
time, and one that fails to load or save is a warning and a full replay.
Bump `PostgresSnapshotStore::with_revision` when the aggregate's shape
changes, so old snapshots are ignored. A repository that stores state in a
table needs none.

---

## Storage

### 22. Provided method, required hook

**Problem.** Every store would re-implement the same steps around its
write: skip empty batches, resolve tags, build envelopes.

**Shape.** `AppendEvents::append_if` is provided: nothing to append
succeeds at once, tags are resolved, the envelopes are built. A store
writes only `append_tagged`: the tagged events, atomically with the version
check and the condition, returning what it recorded (position, sequence,
time).

**Rules.** What must be identical across stores lives in the provided
method, tested once. One port per capability (`LoadStream`,
`AppendEvents`, `SourceEvents`, `ReadLog`); `EventStore` is all four. See
[Storage § Writing a store](storage.md#writing-a-store).

### 23. PostgreSQL: cursor, locks, checked SQL

- **Gap-aware cursor.** Parallel appends can commit a later position before
  an earlier one, so a reader keeps `Cursor { position, gaps }`, and gives a
  gap up only once every transaction that could fill it has ended
  ([Storage § Global order](storage.md#global-order-and-the-cursor)).
- **Locks derived from the domain.** An append takes shared advisory locks
  on its events' tags and types; a criterion read for a conditional append
  takes its keys exclusively. Only decisions on the same tags wait for each
  other ([Storage § Locks](storage.md#locks-derived-from-the-domain)).
- **Checked SQL.** Every statement is `sqlx::query!`, checked at build time
  through `CQRS_POSTGRES_URL` (never an application's `DATABASE_URL`) or the
  committed `.sqlx/` cache.

### 24. Upcaster chains

```rust
let store = PostgresEventStore::<PostEvent>::new(pool, "posts").with_upcasters((
    upcast("post.created", 1, |payload| payload["Created"]["tags"] = json!([])),
    upcast("post.created", 2, |payload| payload["Created"]["draft"] = json!(true)),
));
```

Each `upcast(type, from_version, fn)` rewrites the JSON of one version into
the next; a tuple chains them. Stored rows never change, and an event no
upcaster applies to is decoded directly.

**Rules.** Upcasters belong to the store, so a log with old shapes is
wired with `Events::new(pg.store(log).with_upcasters(..))` and the
`Postgres` tokens and transactions, instead of `pg.events(log)`. See
[Storage § Upcasting](storage.md#upcasting).

---

## Event handling

### 25. One event handler contract

**Problem.** Read models, notifications and integrations all "react to
committed events", but frameworks often give them different interfaces.

**Shape.** One trait, Axon 5's event handling component:

```rust
pub trait EventHandler<E: DomainEvent, Tx: Send = ()>: Send + Sync {
    const REPLAY: bool = true;
    fn handle(&self, event: &EventEnvelope<E>, context: &mut ProcessingContext<'_, Tx>)
        -> impl Future<Output = Result<(), HandlerError>> + Send;
    fn committed(&self, event: &EventEnvelope<E>) -> impl Future<Output = ()> + Send { async {} }
    fn reset(&self, context: &mut ProcessingContext<'_, Tx>)
        -> impl Future<Output = Result<(), HandlerError>> + Send { async { Ok(()) } }
}
```

`committed` runs after the unit of work committed, only if it did; `reset`
clears what was built before a replay.

**Rules.** `handle` may run more than once for an event (retries, a failed
batch): make it transactional or idempotent (`event.position` is a
deduplication key). `HandlerError` converts from any `std::error::Error`,
so `?` works.

### 26. The handler names its unit of work

**Problem.** A projection writing SQL must run in the processor's
transaction; one that writes to memory should run with any token store.

**Shape.** `ProcessingContext<'_, Tx>` carries the unit of work
(`context.transaction()`) and whether the event is a replay
(`context.replay()`). Name `Tx` to require it; leave it generic to accept
any:

```rust
// src/application/projections/post/summaries.rs
#[cqrs::event_handler(namespace = "summaries", batch = 64)]
impl<S, N, Tx> EventHandler for PostProjection<S, N>
where
    S: PostSummaryRepository + PostSummaryStore<Tx>,
    N: Send + Sync,
    Tx: Send,
{
    async fn handle(&self, event: &EventEnvelope<PostEvent>, context: &mut ProcessingContext<'_, Tx>)
        -> Result<(), HandlerError> {
        let tx = context.transaction();   // () in memory, PgTransaction on PostgreSQL
        // self.summaries.save(tx, ..) / update(tx, ..)
    }
}
```

**Rules.** A handler whose context names no `Tx` is made generic by the
macro. One that names `PgTransaction` over in-memory tokens is a compile
error ("cannot run in units of work"). What it writes in the transaction
commits with the processor's token: applied exactly once.

### 27. One projection per aggregate root, a processor per namespace

**Problem.** One processor per read model multiplies the wiring; one for
everything couples a slow side effect to the read models.

**Shape.** The projection is a struct holding what its handlers need. It
lists its namespaces; each has its own handler impl and its own processor
(`"post.summaries"`, `"post.notifications"`), with its own tokens:

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
    summaries: Arc<S>,
    notifier: Arc<N>,
}
```

**Rules.** Checked both ways at compile time: a namespace listed without a
handler does not run, a handler for an unlisted namespace does not compile.
Renaming the projection or a namespace starts its processor over (new
tokens). The namespace belongs to the projection, not the event: the
domain does not know who consumes it.

### 28. Side effects are namespaces that do not replay

**Problem.** A notification sent from a command handler is sent again when
the handler re-runs, or even if the commit fails; one sent from a read
model's handler is sent again on every rebuild.

**Shape.** A namespace of the projection with `REPLAY = false`:

```rust
// src/application/projections/post/notifications.rs
#[cqrs::event_handler(namespace = "notifications")]
impl<S: Send + Sync, N: Notifier> EventHandler for PostProjection<S, N> {
    const REPLAY: bool = false;

    async fn handle(&self, event: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_>)
        -> Result<(), HandlerError> {
        if let PostEvent::Published = event.payload {
            self.notifier.notify(format!("{} published", event.stream)).await?;
        }
        Ok(())
    }
}
```

**Rules.** It runs after the commit, at least once: make the effect
idempotent. No `batch`: a failed batch is handled again, which a
transaction undoes for a read model but not for an e-mail. A reset
(`reset()` on the processors, every instance stopped) rebuilds the read
models; these skip what they already handled. A reaction that sends
commands to another aggregate root is the same: a namespace holding that
root's command port, sending idempotent commands.

### 29. Scaling a processor: segments, batches, dead letters

Set with `TrackingOptions` (`.options(..)` on the unit, or `with_options`
on a processor), and on the handler for batches:

- **Segments** (`TrackingOptions::default().segments(8)`): the log is split
  by aggregate, each segment handled in order by the instance that claims
  it. One aggregate's events stay in order; instances share segments
  through the token store, and a dead instance's claims expire
  ([Event processing](event-processing.md#segments-claims-and-instances)).
- **Batches** (`batch = 64` on a namespace's handler, or `.batch_size(64)`
  for a processor of your own): a worker handles the events already waiting,
  up to the size, in one unit of work with one token update. It never waits
  to fill one. A failed batch rolls back and its events are handled one at
  a time. For handlers whose writes the transaction undoes: the blog's
  summaries rebuild 5000 events in 0.25 s instead of 4.6 s.
- **Dead letters** (`events.processor(name, h).with_dead_letters(queue,
  after)`): an event that failed `after` times is parked with every later
  event of its aggregate, and the segment moves on;
  `retry_dead_letters()` replays each sequence in order once fixed.

### 30. Inline or tracking, same projection

**Shape.** The same projection runs inline
(`InlinePublisher::new(store).with_projection(p)`, after each commit, in the
caller's task) or in tracking processors (`Cqrs::new(events).projection(p)`);
only the composition root changes.

**Rules.** Inline, `Tx = ()`, each namespace sees the commit isolated from
the others, and there are no retries, no replays and no subscriptions. A
failure never undoes the commit: it goes to the `on_failure` policy
(`LogFailures` by default). For tests and simple applications.

### 31. The log as an outbox

**Problem.** Publishing to a broker from a command handler loses messages
when the broker is down, or sends ones whose commit failed.

**Shape.** `Forward` is an event handler that publishes each event through
a one-method port (`Publish`), encoded by a `Codec`, keyed by its stream.
Run it in a tracking processor and the log is the outbox:

```rust
let outbox = app.events().track("outbox", Forward::new(broker, Json, "posts"));
// or a topic per event
Forward::routed(broker, Json, |event: &EventEnvelope<PostEvent>| event.event_type.replace('.', "-"));
```

**Rules.** Nothing is published that was not committed, and nothing
committed is lost: at least once, from the processor's token, retried while
the broker is down. `Outgoing.position` is the consumers' deduplication
id; the stream key keeps each aggregate in order on a broker that orders by
key. `Forward` does not replay.

**Fan-out.** One `Forward` per destination, each its own tracking processor
with its own token: a broker that is down holds only its own forwarder
back. On a broker that fans out (an AMQP fanout exchange), one forwarder
and a queue per consuming service do the same.

**Coming the other way.** A `Subscribe` port (Axon 5's
`SubscribableEventSource`) hands messages, in batches, to a `Consume`r, and
acknowledges a batch once it returned `Ok`; a failed batch is delivered
again. The consumer usually turns each message into a command:

```rust
impl Consume for OnPostPublished {
    async fn consume(&self, messages: &[Incoming]) -> Result<(), HandlerError> {
        for message in messages {
            let PostEvent::Published { id, title } = message.decode(&Json)?;
            match self.gateway.send(Notify { post: id, title }).await {
                Ok(()) | Err(NoticeError::AlreadySent) => {} // delivered twice
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}
```

Delivery is at least once: deduplicate by `Incoming.position`, or make the
command idempotent (above, the notice is a new stream per post, so a second
delivery conflicts). A message that can never be handled is better
acknowledged and logged than failed, or it comes back forever.

**RabbitMQ.** `examples/bank` runs both sides: the bank forwards its
accounts' log, a compliance service consumes it into its own commands.
`cqrs-async-amqp` implements both ports over lapin, in the
shapes of Watermill's presets, one constructor each:

```rust
let amqp = Amqp::connect("amqp://127.0.0.1:5672/%2f").await?;
// out: a fanout exchange per topic
let outbox = events.track("outbox", Forward::new(AmqpPublisher::fanout(&amqp).await?, Json, "posts"));
// in, in the notifications service: its own queue, bound to `posts`
let subscriber = AmqpSubscriber::fanout(&amqp, "posts", "notifications").await?;
subscriber.subscribe(OnPostPublished { gateway }, shutdown).await?;
```

| Constructor | Publishes to | Consumers |
|---|---|---|
| `fanout` | a fanout exchange named after the topic | every bound queue gets every message; a service's instances share its queue |
| `queue` | a durable queue named after the topic | compete for its messages |
| `topic` | one topic exchange, the topic as routing key | bind by pattern (`post.*`) |

Messages are persistent and published with confirms; the event's type,
version, position (the message id), stream and metadata travel as
properties and headers. A failed batch goes back to the queue after a
pause that grows with the failures in a row, or, with
`dead_letter_failures()`, to the queue's dead-letter exchange. Without TLS by
default: an `amqps://` broker needs the `rustls` or `native-tls` feature.

### 32. One projection over several logs (fan-in)

**Problem.** A read model needs the events of two aggregate roots in the
order they happened (a feed showing each post with its author's current
name), but each root appends to its own log, of its own event type.

**Shape.** The read side names what it joins: an enum with a variant per
log's event type, `#[event(fan_in)]`. The logs are read as one, in their
global order, and the projection is an ordinary one over that enum:

```rust
// src/application/feed.rs
#[derive(Clone, Debug, cqrs::DomainEvent)]
#[event(fan_in)]
enum FeedEvent {
    Post(PostEvent),
    User(UserEvent),
}

#[cqrs::projection(name = "feed", events = FeedEvent, namespaces("feed"))]
struct FeedProjection { /* .. */ }

// composition root, PostgreSQL
let posts = pg.events::<PostEvent>("posts");
let users = pg.events::<UserEvent>("users");
let feed: PgFanIn<FeedEvent, _> = pg.fan_in((&posts, &users));
let running = feed.projection(FeedProjection { /* .. */ }).spawn();

// in memory: the stores share their numbering
let posts = Events::new(InMemoryEventStore::<PostEvent>::new());
let users = Events::new(posts.store().beside::<UserEvent>());
let feed: Events<FeedEvent, _> = Events::fan_in((&posts, &users));
```

`examples/bank` projects its customers' portfolios this way.

**Rules.** Only a projection that joins logs has such an enum: no event
type joins every log, and the write side does not change (commands keep
appending to each root's own log; nothing is appended through the fan-in).
The enum takes each event's name, version and tags from the event it wraps,
and converts from each (`From`). Each log is decoded by its own store,
upcasters included. The order is the global one: positions come from one
sequence for the database (one numbering for in-memory stores made
`beside` each other), so the fan-in's cursor tracks gaps over all its logs
at once. The fan-in follows its sources: their commits, and the
notifications `listen` relays to them, wake its processors. Over
PostgreSQL each page is one index range per log, merged.

---

## Interceptors

### 33. Dispatch interceptor

**Shape.** Around a whole gateway, generic over every message:

```rust
// src/infrastructure/observability.rs
impl<M: Message> Interceptor<M> for Audit {
    async fn intercept<N, F>(&self, message: M, metadata: Metadata, next: N) -> Result<M::Output, M::Error>
    where
        N: FnOnce(M, Metadata) -> F + Send,
        F: Future<Output = Result<M::Output, M::Error>> + Send,
    {
        let correlation_id = metadata.get("correlation_id").cloned();
        let result = next(message, metadata).await;
        // .. record the command, its correlation id, result.is_ok()
        result
    }
}

// src/config.rs: .commands(..).intercept(Audit(Arc::clone(&audit)))
```

**Rules.** It may change the message or metadata, refuse (return an error
without calling `next`), or inspect the result. To refuse with a typed
error, bound it:
`impl<C: Command> Interceptor<C> for RequireUser where C::Error: From<Unauthorized>`.
Without the builder:
`Intercepted::new(gateway, Audit(..))`.

### 34. Event interceptor with a chain

**Shape.** Around an event handler or a projection, inside its unit of
work, with its context (Axon's `chain.proceed(context)`):

```rust
// src/infrastructure/observability.rs
impl<E: DomainEvent, Tx: Send> EventInterceptor<E, Tx> for Measured {
    async fn intercept<N: EventChain<Tx>>(
        &self,
        event: &EventEnvelope<E>,
        context: &mut ProcessingContext<'_, Tx>,
        next: N,
    ) -> Result<(), HandlerError> {
        let started = Instant::now();
        let result = next.proceed(context).await;
        // .. count and time it, trace a failure
        result
    }
}

// src/config.rs: .projection(Intercepted::new(projection, Measured(Arc::clone(&metrics))))
```

**Rules.** What it writes in `context.transaction()` commits or rolls back
with the handler; one that writes names its `Tx`, others stay generic. Not
calling `next` skips the event (it counts as handled). `proceed` consumes
the chain: the handler runs at most once per attempt. Chains are tuples,
nested statically: an interceptor that only calls `next` costs about
nothing ([Performance](performance.md#micro-benchmarks)).

---

## Read side and streams

### 35. The read model is a port, read and written apart

```rust
// src/application/queries/post_summary.rs
pub trait PostSummaryRepository: Send + Sync {             // what queries read
    fn find(&self, id: PostId) -> impl Future<Output = Result<Option<PostSummary>, StoreError>> + Send;
    fn published(&self) -> impl Future<Output = Result<Vec<PostSummary>, StoreError>> + Send;
}

pub trait PostSummaryStore<Tx>: Send + Sync {              // what the projection writes, in its unit of work
    fn save(&self, tx: &mut Tx, summary: PostSummary) -> impl Future<Output = Result<(), StoreError>> + Send;
    fn update(&self, tx: &mut Tx, id: PostId, change: impl FnOnce(&mut PostSummary) + Send)
        -> impl Future<Output = Result<bool, StoreError>> + Send;
    // clear(tx), before a replay
}
```

**Rules.** `update` with a closure is Axon's managed entity: change in
place, no find-clone-save. In the tokens' database, each change commits
with its token. Keep it apart from the write side's
[repository](#19-your-repository-in-the-units-transaction): they differ in
storage and consistency.

### 36. Subscription queries from the projection

**Problem.** A client wants a query's result, then each change, and
expects to see its own command right away.

**Shape.** The projection lists the queries it refreshes (`updates(..)`,
[pattern 27](#27-one-projection-per-aggregate-root-a-processor-per-namespace));
a namespace emits in `updated`, on every instance, once it handled the
event:

```rust
// src/application/projections/post/summaries.rs
async fn updated(&self, event: &EventEnvelope<PostEvent>, updates: &QueryUpdateEmitter<Self>) {
    let Ok(id) = post_of(event) else { return };
    if let Ok(Some(summary)) = self.summaries.find(id).await {
        updates.emit(|query: &FindPost| query.id == id, Some(summary));
    }
    // .. and ListPublished when the post was published
}

// src/presentation/graphql/subscription.rs
let updates = ctx.data_unchecked::<A>().queries().subscribe(query);   // current result, then updates
```

**Rules.** The channel is picked by the query's type: emitting a query the
projection does not list does not compile. The current result is read once
the read-model namespaces handled what was committed when the subscription
started, so a client sees its own command. Equal consecutive results are
not repeated. The client subscribes to the shape it queries: no
event-to-API conversion.

### 37. The event feed is a port

**Shape.** `EventFeed<E>::subscribe(after)`: every committed event after a
position, exactly once, in order, then live. `Events` implements it; the
API names the port (`type Events: EventFeed<PostEvent>`).

```rust
// src/presentation/graphql/subscription.rs
let events = ctx.data_unchecked::<A>().events().subscribe(after);
```

**Rules.** The stream does not borrow the feed (`use<Self, E>`), so it can
be returned from a resolver or moved into a task. Reconnecting with the
last position seen misses nothing. A long-running process with state of
its own is a projection with a read model, or a task over this feed.

---

## Testing

### 38. Given / when / then on aggregates

```rust
// tests/domain.rs
AggregateFixture::<Post>::creating(ID, |events| Post::create(ID, "  Hello ", events))
    .expect_events([created()])
    .expect_state(|post| assert_eq!(post.title(), "Hello"));

AggregateFixture::<Post>::new(ID)
    .given([created(), PostEvent::Published])
    .when(|post, events| post.publish(events))
    .expect_error(PostError::AlreadyPublished)
    .expect_no_events();
```

No store, no async: past events in, new events or an error out.
`creating` runs a static creation, which has no past.

### 39. No runtime in tests

`cqrs::testing::block_on` runs a future on the current thread; with the
inline wiring (`blog::in_memory()`), command → projection → query flows
are synchronous and deterministic. Tracking processors need Tokio:
`#[tokio::test(start_paused = true)]` makes retries' backoff take no time.

### 40. `compile_fail` doctests as guarantees

A guarantee enforced by types is proven by code that must not compile:

```rust
/// ```compile_fail,E0277
/// let (blog, _, _) = blog::in_memory();
/// let _ = blog.commands().send(FindPost { id: PostId(1) });   // a query sent as a command
/// ```
```

The blog's `lib.rs` keeps one per guarantee (a decision assigning state,
`&mut` to a loaded aggregate, a command no gateway handles, a listed
command without its handler, a query listed by a command gateway, a
command listed twice); the library's are in its rustdoc and
`crates/cqrs/tests/ui`.

---

## Composition

### 41. The CQRS unit: a typed builder

**Problem.** Wiring by hand means building the events, the gateways over
them, a processor per namespace, the subscriptions over their progress,
and naming every resulting type; switching stores touches all of it.

**Shape.** `Cqrs::new(events)`; each step takes a value and changes the
builder's type:

```rust
// src/config.rs
let projection = PostProjection::new(Arc::clone(&summaries), Arc::clone(&notifier));
let app = Cqrs::new(Events::new(store.clone()))
    .commands(|events| PostCommandGateway::new(posts_over(&store), events))
    .intercept(Audit(Arc::clone(&audit)))
    .queries(PostQueryGateway::new(Arc::clone(&summaries)))
    .projection(Intercepted::new(projection, Measured(Arc::clone(&metrics))))
    .build();

app.commands().send(CreatePost { title }).await?;
app.queries().subscribe(FindPost { id });
app.settled().await;   // every processor caught up
app.stop().await;
```

**Rules.**

- **The infrastructure is the argument.** `Events::new(store)` in memory,
  `Postgres::new(pool).events::<PostEvent>("posts")` on PostgreSQL (one
  database's store, tokens and transactions; a log per root). The blog's
  `start` changes only that and the repository.
- **The unit is the gateway's unit of work.** `.commands(|events| ..)`
  hands the closure the events for its `#[gateway(unit)]` field.
  Synchronous, without a runtime:
  `Cqrs::from_publisher(InlinePublisher::new(store).with_projection(p))`.
- **Order is typed.** `.projection` needs events; one that names
  `PgTransaction` is accepted only over PostgreSQL. A unit is one root with
  one projection; several roots are a tuple of units.
- **Nothing is boxed**: sending through `app.commands()` costs what sending
  through the gateway does. Return the unit by its traits:
  `Cqrs<impl PostCommands, impl PostQueries>`.
- `.options(..)` applies to the processors started after it; a processor
  configured beyond them (dead letters, an outbox) is started from
  `app.events()` and stopped by its owner.
