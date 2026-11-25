---
title: "2. Commands"
sidebar_position: 12
---

A command is an intent to change state. Each one gets its own file, with
its message and its handler. The handlers are methods of one struct, the
command gateway, which holds what they share: the application's
repository, and the unit of work they run in. A handler finds the post,
lets the domain decide, and records what it decided; the unit of work
appends the events and commits once the handler returns.

## The repository: yours

The application says how its handlers find and keep posts. It is a port,
with only what the use cases need:

```rust
// src/application/commands/post_repository.rs
use std::future::Future;

use cqrs::{aggregate::Loaded, store::StoreError};

use crate::domain::{Post, PostId};

pub trait PostRepository: Send + Sync {
    /// The unit of work's transaction it works in.
    type Tx: Send;

    /// An id no post has yet.
    fn next_id(&self, tx: &mut Self::Tx) -> impl Future<Output = Result<PostId, StoreError>> + Send;

    /// The post, at the version it was read at; `None` if it does not
    /// exist.
    fn find(
        &self,
        id: PostId,
        tx: &mut Self::Tx,
    ) -> impl Future<Output = Result<Option<Loaded<Post>>, StoreError>> + Send;

    /// Keeps `post` as the command left it (after its recorded events, at
    /// their version), in `tx`.
    fn save(&self, post: &Loaded<Post>, tx: &mut Self::Tx) -> impl Future<Output = Result<(), StoreError>> + Send;
}
```

- `Loaded<Post>` is the post plus the version it was read at, which the
  append checks. It derefs to `&Post` only: nobody can change the post
  through it, only through events.
- `Tx` is the transaction of the command's unit of work, lent to the
  repository. Whatever the repository writes in it commits together with
  the post's events, or not at all. Each adapter picks its own.

The blog has two adapters. In memory, a post is rebuilt from its events
with the library's `AggregateLoader`. Its events are its state, so saving
writes nothing more, and there is no transaction (`Tx = ()`):

```rust
// src/infrastructure/event_sourced_post_repository.rs
impl<S, N> PostRepository for EventSourcedPostRepository<S, N>
where
    S: LoadStream<PostEvent> + SourceEvents<PostEvent>,
    N: SnapshotStore<Post>,
{
    type Tx = ();

    async fn next_id(&self, (): &mut ()) -> Result<PostId, StoreError> {
        Ok(PostId(self.next_id.fetch_add(1, Ordering::Relaxed)))
    }

    async fn find(&self, id: PostId, (): &mut ()) -> Result<Option<Loaded<Post>>, StoreError> {
        self.loader.load(&id).await
    }

    async fn save(&self, _: &Loaded<Post>, (): &mut ()) -> Result<(), StoreError> {
        Ok(())
    }
}
```

On PostgreSQL (part 8), `SeaOrmPostRepository` keeps each post's state in
a table, with SeaORM, in the same transaction the events are appended in
(`Tx = PgTransaction`). The handlers never know which one they got.

A finder that is not by id (posts by slug, by author) belongs in a read
model (part 3): the event store finds aggregates by id only.

## The application error

Every use case fails the same way, classified by origin, so an API maps it
without parsing strings:

```rust
// src/application/error.rs
use cqrs::{aggregate::NotFound, store::StoreError};

use crate::domain::{PostError, PostId};

#[derive(Debug, PartialEq)]
pub enum BlogError {
    /// The domain refused (a 4xx).
    Domain(PostError),
    /// No post has this id (a 404).
    NotFound(PostId),
    /// Someone else changed the post in between: retry.
    Conflict,
    /// Storage failed (a 5xx).
    Store(String),
}

impl From<PostError> for BlogError {
    fn from(error: PostError) -> Self {
        Self::Domain(error)
    }
}

impl From<NotFound<PostId>> for BlogError {
    fn from(NotFound(id): NotFound<PostId>) -> Self {
        Self::NotFound(id)
    }
}

impl From<StoreError> for BlogError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::Conflict { .. } | StoreError::ConditionFailed => Self::Conflict,
            // `StoreError` may gain variants: anything else is the store's.
            other => Self::Store(other.to_string()),
        }
    }
}
```

`From<StoreError>` is required: the unit of work's own failures (an append
that conflicts, a commit that fails) reach the caller as the command's
error.

## The command gateway

```rust
// src/application/commands/mod.rs
#[cqrs::command_gateway(commands = PostCommands, handles(CreatePost, RetitlePost, PublishPost))]
pub struct PostCommandGateway<R, P> {
    /// Where posts are found and kept.
    posts: R,
    /// The unit of work every command runs in: it opens the transaction
    /// the handlers and the repository write in, appends what they
    /// recorded, and commits.
    #[gateway(unit)]
    events: P,
}

impl<R, P> PostCommandGateway<R, P> {
    pub fn new(posts: R, events: P) -> Self {
        Self { posts, events }
    }
}
```

The gateway is the handler: one struct per side, with one
`CommandHandler<C>` impl per command, in that command's file. The handlers
share the struct's fields, so there is no struct per handler and no `Arc`
to pass the repository around.

- `handles(..)` is the list of commands it routes, each to its impl,
  decided at compile time. A command listed without its impl does not
  compile, where the gateway is declared; so does a query listed as a
  command.
- `#[gateway(unit)]` marks the field the commands run in: a `UnitOfWork`,
  which the composition root provides (part 4). `P` stays generic here.
- `commands = PostCommands` also generates a trait,
  `CommandGateway<CreatePost> + CommandGateway<RetitlePost> + CommandGateway<PublishPost>`,
  implemented by whatever routes the three: this gateway, the gateway
  behind interceptors (part 7), a test double. Code that sends commands
  names `impl PostCommands`, not the gateway.

## A command and its handler

```rust
// src/application/commands/retitle_post.rs
use cqrs::{
    aggregate::NotFound,
    command::{CommandContext, CommandHandler},
};

use crate::{
    application::{
        BlogError,
        commands::{PostCommandGateway, PostRepository},
    },
    domain::{PostEvent, PostId},
};

/// Changes the title of a draft.
#[cqrs::command(error = BlogError)]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RetitlePost {
    pub id: PostId,
    pub title: String,
}

/// Loads the post, lets the domain rename it and saves the result; nothing
/// is committed if the domain refuses.
#[cqrs::command_handler]
impl<R: PostRepository, P> CommandHandler<RetitlePost> for PostCommandGateway<R, P> {
    async fn handle(
        &self,
        command: &RetitlePost,
        ctx: &mut CommandContext<PostEvent>,
        tx: &mut R::Tx,
    ) -> Result<(), BlogError> {
        let post = self.posts.find(command.id, tx).await?.ok_or(NotFound(command.id))?;
        post.retitle(command.title.as_str(), ctx)?;
        let post = ctx.record(post);
        self.posts.save(&post, tx).await?;
        Ok(())
    }
}
```

What each piece does:

- **The message owns its result type.** `#[cqrs::command(output = T, error = E)]`
  makes the struct a `Command` whose `send` returns `Result<T, E>`; `output`
  defaults to `()`. Whoever sends `RetitlePost` gets `Result<(), BlogError>`
  without knowing the handler.
- **`CommandContext` is the command's context** (Axon 5's
  `EventAppender` with its processing context): the events the domain
  appends, and the command's metadata, which every event carries. It is an
  `EventAppender<PostEvent>`, so the domain takes it as `&mut impl
  EventAppender<PostEvent>` and never sees the rest. A fresh one per run.
- **find, decide, record, save.** `ctx.record(post)` assigns the events
  appended so far to that post, and returns the post evolved by them, at
  their version. It is pure: nothing is written yet. The repository saves
  that post in `tx` (the event-sourced one does nothing; the SeaORM one
  writes the row).
- **The handler never commits.** Once `handle` returns `Ok`, the unit of
  work appends what was recorded, expecting the version the post was
  read at, and commits. If `handle` returns an error, nothing is appended
  and the transaction is rolled back. Events appended but never recorded
  are a bug (a forgotten `record`): debug builds panic.
- **The command is borrowed** (`&RetitlePost`), because the handler may run
  more than once (below).

`#[cqrs::command_handler]` reads the event type and the unit of work from
the signature, and marks the gateway as handling the command, for
`handles(..)`. By hand:

```rust
impl<R: PostRepository, P> CommandHandler<RetitlePost> for PostCommandGateway<R, P>
where
    PostCommandGateway<R, P>: Send + Sync,
{
    type Event = PostEvent;
    type Unit = InTransaction<R::Tx>; // `Autocommit` without a `tx` argument
    async fn handle(&self, command: &RetitlePost, ctx: &mut CommandContext<PostEvent>, tx: &mut R::Tx)
        -> Result<(), BlogError> { .. }
}
// For the gateway that lists `RetitlePost`.
impl<R, P> Handles<RetitlePost> for PostCommandGateway<R, P> {}
```

`PublishPost` is the same, with `post.publish(ctx)`.

### Creating

Creating has nothing to find. The repository hands out an id, the domain
decides without a post (`Post::create` is an associated function), and the
events are recorded for `New::<Post>(id)`, which expects the post's stream
not to exist:

```rust
// src/application/commands/create_post.rs
/// Creates a draft post and returns its id.
#[cqrs::command(output = PostId, error = BlogError)]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CreatePost {
    pub title: String,
}

#[cqrs::command_handler]
impl<R: PostRepository, P> CommandHandler<CreatePost> for PostCommandGateway<R, P> {
    async fn handle(
        &self,
        command: &CreatePost,
        ctx: &mut CommandContext<PostEvent>,
        tx: &mut R::Tx,
    ) -> Result<PostId, BlogError> {
        let id = self.posts.next_id(tx).await?;
        Post::create(id, command.title.as_str(), ctx)?;
        let post = ctx.record(New::<Post>(id));
        self.posts.save(&post, tx).await?;
        Ok(id)
    }
}
```

If two creations got the same id, the second append fails with a
conflict: a post is never overwritten.

A command that creates its aggregate when it is missing (a counter that
starts at zero) decides on the state before any event, and records to
whichever it got:
`self.counters.find(id).await?.unwrap_or_else(|| New(id).into())`.

## The unit of work

The handler's signature chooses how its unit of work runs:

- **Without a `tx` argument** (`handle(&self, command, ctx)`), the mode is
  `Autocommit`: what the handler recorded is appended by the store in one
  statement. The fastest path, for one entity per command. Recording a
  second entity is an error: their appends would not be atomic.
- **With `tx: &mut Tx`**, the mode is `InTransaction<Tx>`: the unit opens
  the transaction, the handler and its repository write in it, the
  recorded events are appended in it last, and the unit commits.
  Everything commits together, or nothing does.

The blog's handlers take `tx: &mut R::Tx`, because the repository works
in a transaction: `()` in memory, a `PgTransaction` on PostgreSQL. The
same handler code runs on both.

Who runs the unit is the gateway's `#[gateway(unit)]` field: an
`InlinePublisher` (in memory, the projections run right after each
commit), or `Events` (asynchronous, with the transactions of its
`TransactionManager`). Both are wired in part 4 and 5.

## Retries

The append expects the post at the version it was read at. If someone
appended to it in between, the unit is rolled back and the handler runs
again, with a fresh context and transaction, on fresh state: up to 8 times.
After that, the caller gets `BlogError::Conflict`. A handler changes the
number by overriding `conflict_retries` in its impl (0: never rerun).

Two cases are not retried: creating what exists already (a `New` whose
stream exists is not a race that rerunning can win), and a unit that cannot
roll back (memory) after more than one write went through, since rerunning
would append the first one twice.

So **a command handler has no side effects** besides writing in `tx`. An
email sent from a handler could be sent twice. Side effects belong after
the commit, in a namespace of the projection that does not replay (part 5).

## Trying it

A handler is a method of the gateway: you can call it with a context of
your own, and nothing is written. To commit, run it in a unit of work with
`dispatch_command`, which is what the gateway does for each command:

```rust
// tests/flow.rs
use cqrs::{
    command::{CommandContext, CommandHandler, dispatch_command},
    event::Metadata,
    processing::InlinePublisher,
    store::InMemoryEventStore,
    testing::block_on,
};

#[test]
fn handlers_can_be_called_directly_without_any_bus() {
    block_on(async {
        // A handler is a method of the gateway: called with an appender, it
        // decides and records, and nothing is written.
        let store = InMemoryEventStore::new();
        let gateway = PostCommandGateway::new(EventSourcedPostRepository::new(store.clone()), ());
        let mut events = CommandContext::new();
        let id = gateway
            .handle(&CreatePost { title: "direct".into() }, &mut events, &mut ())
            .await
            .unwrap();
        assert_eq!((id, events.writes().len()), (PostId(1), 1));

        // In a unit of work of the caller's: it commits (the repository
        // handed out id 1 already, to the run above).
        let unit = InlinePublisher::new(store);
        let id = dispatch_command(&gateway, CreatePost { title: "direct".into() }, Metadata::new(), &unit)
            .await
            .unwrap();
        assert_eq!(id, PostId(2));
        assert_eq!(
            dispatch_command(&gateway, PublishPost { id }, Metadata::new(), &unit).await,
            Ok(())
        );
    });
}
```

`block_on` comes with the library: these tests need no async runtime.

Through the gateway, it is `send`. Code that sends commands depends on the
commands it sends, not on the gateway:

```rust
// tests/flow.rs
pub async fn create_and_publish<G>(gateway: &G, title: &str) -> Result<PostId, BlogError>
where
    G: CommandGateway<CreatePost> + CommandGateway<PublishPost>,
{
    let id = gateway.send(CreatePost { title: title.to_owned() }).await?;
    gateway.send(PublishPost { id }).await?;
    Ok(id)
}
```

The two commands are not atomic: if publishing fails, the post stays
created as a draft. `send_with(command, metadata)` sends a command with
metadata (a correlation id, the user), which every event it causes
carries (part 9).

Next: [3. Queries and the read model](03-queries.md).
