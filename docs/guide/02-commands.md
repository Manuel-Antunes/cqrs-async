# 2. Commands

A command is an intent to change state. Each one gets its own file with
its message and its handler, and a gateway routes them. As in NestJS,
handlers find posts through the application's own repository, and commit
what the domain decided through the context they are handed, which
carries the framework's publisher.

## The repository: yours

The application says how its handlers find posts. It is a port, with only
the finders the use cases need:

```rust
// src/application/commands/post_repository.rs
use std::future::Future;
use cqrs::{Loaded, StoreError};
use crate::domain::{Post, PostId};

pub trait PostRepository: Send + Sync {
    /// The post, at the version it was read at; `None` if it does not exist.
    fn find(&self, id: PostId) -> impl Future<Output = Result<Option<Loaded<Post>>, StoreError>> + Send;
}
```

`Loaded<Post>` is the post plus the version it was read at, which the
commit checks. The domain never sees it, and nobody can change the post
through it: it derefs to `&Post` only.

The adapter, in infrastructure, rebuilds each post from its events in the
event store, with the framework's `AggregateLoader`:

```rust
// src/infrastructure/event_sourced_post_repository.rs
use cqrs::{AggregateLoader, EventStore, Loaded, StoreError};

pub struct EventSourcedPostRepository<S> {
    loader: AggregateLoader<Post, S>,
}

impl<S> EventSourcedPostRepository<S> {
    pub fn new(store: S) -> Self {
        Self { loader: AggregateLoader::new(store) }
    }
}

impl<S: EventStore<PostEvent>> PostRepository for EventSourcedPostRepository<S> {
    async fn find(&self, id: PostId) -> Result<Option<Loaded<Post>>, StoreError> {
        self.loader.load(&id).await
    }
}
```

The store is shared (an `Arc`, or PostgreSQL's pool-backed store): the
repository reads it, and the unit that commits writes it. A finder that is
not by id (posts by slug, by author) belongs in a read model: the event
store finds aggregates by id only.

## The application error

Every use case fails the same way, classified by origin so an API maps it
without parsing strings:

```rust
// src/application/error.rs
use cqrs::{NotFound, StoreError};
use crate::domain::{PostError, PostId};

#[derive(Debug, PartialEq)]
pub enum BlogError {
    Domain(PostError),   // the domain refused (4xx)
    NotFound(PostId),    // 404
    Conflict,            // changed concurrently: retry
    Store(String),       // 5xx
}

impl From<PostError> for BlogError {
    fn from(error: PostError) -> Self { Self::Domain(error) }
}

impl From<NotFound<PostId>> for BlogError {
    fn from(NotFound(id): NotFound<PostId>) -> Self { Self::NotFound(id) }
}

impl From<StoreError> for BlogError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::Conflict { .. } | StoreError::ConditionFailed => Self::Conflict,
            StoreError::Backend(message) => Self::Store(message),
        }
    }
}
```

## A command and its handler

```rust
// src/application/commands/retitle_post.rs
use std::sync::Arc;
use cqrs::{Appender, CommandHandler, EventPublisher, NotFound};
use crate::{application::{BlogError, commands::PostRepository}, domain::{PostEvent, PostId}};

/// Changes the title of a draft.
#[derive(Debug, serde::Serialize, serde::Deserialize, cqrs::Command)]
#[command(error = BlogError)]                 // output: () by default
pub struct RetitlePost {
    pub id: PostId,
    pub title: String,
}

pub struct RetitlePostHandler<R, P> {
    posts: Arc<R>,
    events: P,        // where commits go: an EventPublisher<PostEvent>
}

impl<R, P> RetitlePostHandler<R, P> {
    pub fn new(posts: Arc<R>, events: P) -> Self {
        Self { posts, events }
    }
}

#[cqrs::command_handler]
impl<R: PostRepository, P: EventPublisher<PostEvent>> CommandHandler for RetitlePostHandler<R, P> {
    async fn handle(&self, command: &RetitlePost, events: &mut Appender<PostEvent>) -> Result<(), BlogError> {
        let post = self.posts.find(command.id).await?.ok_or(NotFound(command.id))?;
        post.retitle(command.title.as_str(), events)?;
        events.commit(post, &self.events).await?;
        Ok(())
    }
}
```

What each piece does:

- **The message owns its result type.** `#[command(output = .., error = ..)]`
  implements `Message { type Output; type Error; }` and the `Command`
  marker. Whoever sends `RetitlePost` gets `Result<(), BlogError>`, without
  knowing the handler.
- **One handler struct per use case**, holding only the services it uses,
  generic over their ports (`R: PostRepository`, `P: EventPublisher<PostEvent>`):
  the composition root picks the implementations, and a test can pass doubles. The publisher
  among them.
- **The appender is the command's context** (Axon 5's `EventAppender`
  with its processing context): the events the domain appends, the
  command's metadata (stored with every event), what the run read for
  conditional commits. A fresh one per run.
- **find → decide → `events.commit(post, &self.events)`** (NestJS's `hero.commit()`).
  `find` returns `Loaded<Post>`; you can read it (`&Post`) but not change
  it. `commit` consumes it and appends the pending events, expecting its
  version, through the handler's publisher.
- **`#[cqrs::command_handler]`** reads `type Command = RetitlePost` and
  `type Event = PostEvent` from the signature and touches nothing else. By
  hand:

  ```rust
  impl<R: PostRepository, P: EventPublisher<PostEvent>> CommandHandler for RetitlePostHandler<R, P> {
      type Command = RetitlePost;
      type Event = PostEvent;
      async fn handle(&self, command: &RetitlePost, events: &mut Appender<PostEvent>) -> .. { .. }
  }
  ```

- **Conflicts retry by themselves.** If someone committed to the post in
  between, `commit` fails with a conflict and, since nothing was committed
  yet in this run, the gateway runs the handler again on fresh state (up to
  8 times; override `conflict_retries`). So a handler must not have side
  effects besides committing.
- **The command is borrowed** (`&RetitlePost`), because the handler may
  run more than once.

Creating is the same, from the initial state, and needs no service at all:

```rust
// src/application/commands/create_post.rs
#[derive(Debug, serde::Serialize, serde::Deserialize, cqrs::Command)]
#[command(output = PostId, error = BlogError)]
pub struct CreatePost {
    pub title: String,
}

pub struct CreatePostHandler<P> {
    next_id: AtomicU64,   // enough for the example: a collision is a Conflict, never an overwrite
    events: P,
}

#[cqrs::command_handler]
impl<P: EventPublisher<PostEvent>> CommandHandler for CreatePostHandler<P> {
    async fn handle(&self, command: &CreatePost, events: &mut Appender<PostEvent>) -> Result<PostId, BlogError> {
        let id = PostId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let post = Loaded::new(Post::initial(&id));   // expects no stream yet
        post.create(command.title.as_str(), events)?;
        events.commit(post, &self.events).await?;
        Ok(id)
    }
}
```

`PublishPost` follows `RetitlePost` with `post.publish(events)`.

## The command gateway

```rust
// src/application/commands/mod.rs
#[derive(cqrs::Gateway)]
#[gateway(commands)]
pub struct PostCommandGateway<R, P> {
    create: CreatePostHandler<P>,
    retitle: RetitlePostHandler<R, P>,
    publish: PublishPostHandler<R, P>,
}

impl<R, P: Clone> PostCommandGateway<R, P> {
    pub fn new(posts: Arc<R>, events: P) -> Self {
        Self {
            create: CreatePostHandler::new(events.clone()),
            retitle: RetitlePostHandler::new(Arc::clone(&posts), events.clone()),
            publish: PublishPostHandler::new(posts, events),
        }
    }
}
```

`#[gateway(commands)]` routes each command to the field whose handler
declares it, decided at compile time. The constructor takes the services
and the publisher the handlers commit through:

```rust
let commands = PostCommandGateway::new(posts, publisher);
commands.send(PublishPost { id }).await?;
```

Sending a command no field handles, sending a query as a command, or two
fields for the same command are compile errors. In the application, the CQRS
unit hands the gateway its publisher (part 4).

### Sending any command (transports)

A queue or an HTTP endpoint that receives "some command" can deserialize
an enum of them; the gateway routes each variant:

```rust
#[derive(Debug, serde::Serialize, serde::Deserialize, cqrs::Commands)]
#[commands(error = BlogError, derive(Debug, PartialEq))]
pub enum PostCommand {
    CreatePost(CreatePost),
    RetitlePost(RetitlePost),
    PublishPost(PublishPost),
}

#[derive(cqrs::Gateway)]
#[gateway(commands = PostCommand)]       // also accepts the enum
pub struct PostCommandGateway<R, P> { .. }

let command: PostCommand = serde_json::from_str(r#"{"CreatePost": {"title": "Over the wire"}}"#)?;
let PostCommandOutput::CreatePost(id) = gateway.send(command).await? else { unreachable!() };
```

`PostCommandOutput` is generated, one variant per command's output.

## Trying it

The publisher here is an `InlinePublisher` over the same in-memory
store the repository reads (part 4 builds it for you):

```rust
use std::sync::Arc;
use cqrs::{CommandGateway, InMemoryEventStore, InlinePublisher, testing::block_on};

#[test]
fn a_post_is_created_retitled_and_published() {
    block_on(async {
        let store = Arc::new(InMemoryEventStore::new());
        let posts = Arc::new(EventSourcedPostRepository::new(Arc::clone(&store)));
        let commands = PostCommandGateway::new(Arc::clone(&posts), Arc::new(InlinePublisher::new(store)));

        let id = commands.send(CreatePost { title: "Draft".into() }).await.unwrap();
        commands.send(RetitlePost { id, title: "Final".into() }).await.unwrap();
        commands.send(PublishPost { id }).await.unwrap();

        let again = commands.send(PublishPost { id }).await;
        assert_eq!(again, Err(BlogError::Domain(PostError::AlreadyPublished)));
        assert!(posts.find(id).await.unwrap().unwrap().is_published());
    });
}
```

`block_on` comes with the library: these tests need no async runtime.

Code that sends commands depends on the commands it sends, not on the
gateway:

```rust
pub async fn create_and_publish<G>(gateway: &G, title: &str) -> Result<PostId, BlogError>
where
    G: CommandGateway<CreatePost> + CommandGateway<PublishPost>,
{
    let id = gateway.send(CreatePost { title: title.to_owned() }).await?;
    gateway.send(PublishPost { id }).await?;
    Ok(id)
}
```

Next: [3. Queries and the read model](03-queries.md).
