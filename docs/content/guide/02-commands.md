---
title: "2. Commands"
sidebar_position: 12
---

A command is an intent to change state. Each one gets its own file, with
its message and its handler. The handlers are methods of one struct, the
command gateway, which holds what they share: the application's
repository and the unit of work they run in. A handler finds the post,
lets the domain decide, commits what it decided to the post and saves it;
the unit of work appends the events and commits once the handler returns.

## The repository: the engine's, or yours

The handlers find and keep posts through the application's port,
`PostRepository`: the library's `Repository<Post>` contract (`find` and
`save`) plus what the use cases need besides, here a new id. Whatever
implements it will do, and the handlers never know which one they got:

```rust
// src/application/commands/post_repository.rs
pub trait PostRepository: Repository<Post> {
    /// An id no post has yet.
    fn next_id(&self, tx: &mut Self::Tx) -> impl Future<Output = Result<PostId, StoreError>> + Send;
}
```

For a post rebuilt from its events,
the repository comes from the engine, which knows the store, where the
snapshots go and the transaction type; the aggregate says when to snapshot:

```rust
// src/domain/post/mod.rs
#[cqrs::aggregate(name = "post", event = PostEvent, snapshot(after_events = 50))]
pub struct Post { .. }

// src/config.rs
let app = Cqrs::new(Events::new(store))
    .commands(|events| PostCommandGateway::new(events.repository::<Post>(), events))
```

- `Repository<Post>` has `type Tx`, the transaction of the command's unit
  of work, which the handler's context owns. Whatever the repository writes
  in it commits together with the post's events, or not at all. Each
  repository picks its own: the engine's takes the engine's (`()` in memory,
  a `PgTransaction` on PostgreSQL).
- `find(&id, ctx.tx())` returns a `Loaded<Post>`: the post plus the version
  it was read at, which the append checks. It derefs to `&Post` only: nobody
  can change the post through it, only through events. The repository takes
  only the transaction, which the handler lends it from its context.
- `ctx.commit(post)` assigns the events the domain appended to the post,
  as its new version, and `save(&post, ctx.tx())` keeps the post in the
  unit's transaction. `save` is what a repository implements, taking only
  the transaction, and does nothing by default: one that keeps the post's
  state writes it there. The engine's does not: the post's events are its
  state.

The ids are a method of the same repository. The blog implements its port
for the engine's repository directly, with no struct of its own, taking the
next id from the log (the position after its last event: growing, not
consecutive, enough in memory):

```rust
// src/application/commands/post_repository.rs, under the port
impl<S, N, Tx> PostRepository for AggregateLoader<Post, S, N, Tx>
where
    S: LoadStream<PostEvent> + SourceEvents<PostEvent> + ReadLog<PostEvent>,
    N: SnapshotStore<Post>,
    Tx: Send,
{
    async fn next_id(&self, _: &mut Tx) -> Result<PostId, StoreError> {
        Ok(PostId(self.store().last_position().await? + 1))
    }
}
```

On PostgreSQL (part 8),
`SeaOrmPostRepository` takes the id from a sequence and keeps each post's
state in a table, with SeaORM, in the same transaction the events are
appended in (`Tx = PgTransaction`): the repository of your own, where the
engine's does not fit.

A finder that is not by id (posts by slug, by author) belongs in a read
model (part 3): the event store finds aggregates by id only.

## A command and its handler

```rust
// src/application/commands/retitle_post.rs
use cqrs::{
    aggregate::{NotFound, Repository},
    command::{CommandContext, CommandHandler},
};

use crate::{
    application::{BlogError, commands::PostCommandGateway},
    domain::{Post, PostEvent, PostId},
};

/// Changes the title of a draft.
#[cqrs::command(error = BlogError)]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RetitlePost {
    pub id: PostId,
    pub title: String,
}

/// Loads the post, lets the domain rename it, commits the result to the
/// post and saves it; nothing is appended if the domain refuses.
#[cqrs::command_handler]
impl<R: PostRepository, P> CommandHandler<RetitlePost> for PostCommandGateway<R, P> {
    async fn handle(
        &self,
        command: &RetitlePost,
        ctx: &mut CommandContext<PostEvent, R::Tx>,
    ) -> Result<(), BlogError> {
        let post = self.posts.find(&command.id, ctx.tx()).await?.ok_or(NotFound(command.id))?;
        post.retitle(command.title.as_str(), ctx)?;
        let post = ctx.commit(post);
        self.posts.save(&post, ctx.tx()).await?;
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
  appends, the command's metadata, which every event carries, and the
  unit's transaction (`ctx.tx()`), if it has one. It is an
  `EventAppender<PostEvent>`, so the domain takes it as `&mut impl
  EventAppender<PostEvent>` and never sees the rest. A fresh one per run.
- **find, decide, commit, save.** `ctx.commit(post)` assigns the events
  appended so far to that post and returns it evolved by them, at their
  version. It is a version of the post, not a database commit: nothing is
  written yet. The repository then saves that post in the transaction (the
  event-sourced one does nothing; the SeaORM one writes the row).
- **The handler never commits.** Once `handle` returns `Ok`, the unit of
  work appends what was committed to the context, expecting the version
  the post was read at, and commits the transaction. If `handle` returns an
  error, nothing is appended and the transaction is rolled back. Events
  appended but never committed are a bug (a forgotten `commit`): in every
  build, the unit refuses the
  command with a store error and appends nothing.
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
    type Unit = InTransaction<R::Tx>; // `Autocommit` for `CommandContext<PostEvent>`
    async fn handle(&self, command: &RetitlePost, ctx: &mut CommandContext<PostEvent, R::Tx>)
        -> Result<(), BlogError> { .. }
}
// For the gateway that lists `RetitlePost`.
impl<R, P> Handles<RetitlePost> for PostCommandGateway<R, P> {}
```

`PublishPost` is the same, with `post.publish(ctx)`.

### Creating

Creating has nothing to find. The repository hands out an id, the domain
decides without a post (`Post::create` is an associated function), and the
events are committed to `New::<Post>(id)`, which expects the post's stream
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
        ctx: &mut CommandContext<PostEvent, R::Tx>,
    ) -> Result<PostId, BlogError> {
        let id = self.posts.next_id(ctx.tx()).await?;
        Post::create(id, command.title.as_str(), ctx)?;
        let post = ctx.commit(New::<Post>(id));
        self.posts.save(&post, ctx.tx()).await?;
        Ok(id)
    }
}
```

If two creations got the same id, the second append fails with a
conflict: a post is never overwritten.

A command that creates its aggregate when it is missing (a counter that
starts at zero) decides on the state before any event, and commits
to whichever it got:
`self.counters.find(&id, ctx.tx()).await?.unwrap_or_else(|| New(id).into())`.

## The unit of work

The type of the handler's context chooses how its unit of work runs:

- **`CommandContext<Event>`** (one type argument) is `Autocommit`: what the
  handler committed is appended by the store in one statement. The fastest
  path, for one entity per command. Committing to a second entity is an error:
  their appends would not be atomic.
- **`CommandContext<Event, Tx>`** (two) is `InTransaction<Tx>`: the unit
  opens the transaction and the context owns it (`ctx.tx()`), the
  handler and its repository write in it, the committed events are appended
  in it last, and the unit commits. Everything commits together, or nothing
  does.

The blog's handlers take `CommandContext<PostEvent, R::Tx>`, because the
repository works in a transaction: `()` in memory, a `PgTransaction` on
PostgreSQL. The same handler code runs on both.

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

So **a command handler has no side effects** besides writing in the
context's transaction. An
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
        // decides and commits, and nothing is written.
        let store = InMemoryEventStore::new();
        let unit = InlinePublisher::new(store);
        let gateway = PostCommandGateway::new(unit.repository::<Post>(), ());
        let mut events = CommandContext::new();
        let id = gateway
            .handle(&CreatePost { title: "direct".into() }, &mut events)
            .await
            .unwrap();
        assert_eq!((id, events.writes().len()), (PostId(1), 1));

        // In a unit of work of the caller's: it commits (the ids handed out
        // id 1 already, to the run above).
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
