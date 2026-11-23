# 4. The projection

A projection turns committed events into read-model rows. It is an event
handler (Axon 5's event handling component): a struct holding its
dependencies, with one `handle` that matches on the event enum.

## The projection

```rust
// src/application/projections/post_summary.rs
use std::sync::Arc;
use cqrs::{EventEnvelope, EventHandler, HandlerError, ProcessingContext};

pub struct PostSummaryProjection<S> {
    summaries: Arc<S>,
}

impl<S> PostSummaryProjection<S> {
    pub fn new(summaries: Arc<S>) -> Self {
        Self { summaries }
    }
}

#[cqrs::projection(namespace = "post-summaries")]
impl<S: PostSummaryRepository> EventHandler for PostSummaryProjection<S> {
    async fn handle(&self, event: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        let found = match &event.payload {
            PostEvent::Created { id, title } => {
                self.summaries.save(PostSummary { id: *id, title: title.clone(), published: false }).await?;
                true
            }
            PostEvent::Retitled { title } => self.summaries.update(post_of(event)?, |s| s.title.clone_from(title)).await?,
            PostEvent::Published => self.summaries.update(post_of(event)?, |s| s.published = true).await?,
        };
        if !found {
            // A stream's events arrive in order, its creation first.
            return Err(HandlerError::msg(format!("{} has no summary: its creation was not projected", event.stream)));
        }
        Ok(())
    }

    /// Before a replay rebuilds it.
    async fn reset(&self, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        self.summaries.clear().await?;
        Ok(())
    }
}

/// The post a stream belongs to (its id is the stream's).
fn post_of(event: &EventEnvelope<PostEvent>) -> Result<PostId, HandlerError> {
    event.stream.id().parse().map_err(|_| HandlerError::msg(format!("not a post stream: {}", event.stream)))
}
```

The envelope carries the event (`payload`) and what the store recorded
about it: `stream`, `sequence`, `position`, `recorded_at`, `metadata`.

What `#[cqrs::projection(namespace = "post-summaries")]` does:

- reads the event type from `&EventEnvelope<PostEvent>`;
- reads the unit of work from `&mut ProcessingContext<'_, Tx>`. None is
  named here, so the handler becomes generic over it: it runs inline, with
  the in-memory token store, or with PostgreSQL's (part 8);
- implements `Projection`, whose `NAMESPACE` names the processor that will
  run it and keys its tokens (Axon's `@Namespace`).

By hand, it is:

```rust
impl<S: PostSummaryRepository, Tx: Send> EventHandler<PostEvent, Tx> for PostSummaryProjection<S> {
    async fn handle(&self, event: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_, Tx>) -> Result<(), HandlerError> { .. }
    async fn reset(&self, _: &mut ProcessingContext<'_, Tx>) -> Result<(), HandlerError> { .. }
}

impl<S: PostSummaryRepository> cqrs::Projection for PostSummaryProjection<S> {
    const NAMESPACE: &'static str = "post-summaries";
}
```

Things the projection does **not** do:

- **Check sequences or remember what it applied.** Whoever runs it
  delivers each aggregate's events once and in order (part 5 shows how a
  processor guarantees it with its unit of work).
- **Read the event store.** Everything it needs is in the event.

## A notification policy

A policy is the same contract with a side effect. The application declares
the port it needs:

```rust
// src/application/policies/notify_on_publish.rs
pub trait Notifier: Send + Sync {
    fn notify(&self, message: String) -> impl Future<Output = Result<(), NotificationError>> + Send;
}

pub struct NotifyOnPublish<N> {
    notifier: Arc<N>,
}

#[cqrs::projection(namespace = "notify-on-publish")]
impl<N: Notifier> EventHandler for NotifyOnPublish<N> {
    /// A notification already went out: a replay must not send it again.
    const REPLAY: bool = false;

    async fn handle(&self, event: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        if let PostEvent::Published = event.payload {
            self.notifier.notify(format!("{} published", event.stream)).await?;
        }
        Ok(())
    }
}
```

`InMemoryNotifier` (infrastructure) records the messages and can be
switched off to simulate an outage.

## Wiring it: the composition root

All the wiring lives in one file, `src/config.rs`, the only one that
names concrete adapters (as the cqrs-es demo's `config.rs` does). It
creates the stores and the services, and builds the blog's **CQRS unit**:
the publisher, the gateways and, later, the events and processors, with a
typed builder:

```rust
// src/config.rs
pub type PostStore = Arc<InMemoryEventStore<PostEvent>>;

pub fn in_memory() -> (
    Cqrs<EventSourced<Post, PostStore, impl EventProcessor<PostEvent>>, impl PostCommands, impl PostQueries>,
    Arc<InMemoryPostSummaryRepository>,
    Arc<InMemoryNotifier>,
) {
    let store = PostStore::default();
    let posts = Arc::new(EventSourcedPostRepository::new(Arc::clone(&store)));
    let summaries = Arc::new(InMemoryPostSummaryRepository::default());
    let notifier = Arc::new(InMemoryNotifier::default());
    let processors = (
        PostSummaryProjection::new(Arc::clone(&summaries)),
        NotifyOnPublish::new(Arc::clone(&notifier)),
    );
    let blog = Cqrs::from_publisher(EventSourced::with_processor(store, processors))
        .commands(PostCommandGateway::new(posts))
        .queries(PostQueryGateway::new(Arc::clone(&summaries)))
        .build();
    (blog, summaries, notifier)
}
```

- **The store is created here and shared**: the repository reads it, the
  publisher writes it.
- **The publisher** is an `EventSourced` engine over the store; with a
  processing group (the tuple), it feeds each handler every committed
  event, right after the commit. It is built by hand here
  (`Cqrs::from_publisher`) because it runs the handlers inline; from part 5
  on, `Cqrs::new(store, tokens)` makes it from the stores.
- **`.commands(gateway)` gives the gateway the unit's publisher**
  (`WithPublisher`). The gateways were built with their services only.
- **Each step changes the builder's type**, so the order is checked, and
  every type is inferred.
- **The unit keeps the concrete types**, so calls through it are the
  gateway's calls (measured: the same 15 ns either way). What it exposes
  is named by the ports from part 3: `impl PostCommands`,
  `impl PostQueries`.

The unit has `commands()`, `queries()`, `publisher()` and `events()`.

## The first end-to-end test

```rust
// tests/flow.rs
use blog::{application::{BlogError, commands::*, queries::*}, domain::PostId, in_memory};
use cqrs::{CommandGateway, QueryGateway, testing::block_on};

#[test]
fn a_command_commits_events_and_the_projection_answers_queries() {
    block_on(async {
        let (blog, _, notifier) = in_memory();

        let id = blog.commands().send(CreatePost { title: "Static dispatch".into() }).await.unwrap();
        blog.commands().send(PublishPost { id }).await.unwrap();

        let found = blog.queries().query(FindPost { id }).await.unwrap();
        assert_eq!(found, Some(PostSummary { id, title: "Static dispatch".into(), published: true }));
        assert_eq!(blog.queries().query(ListPublished).await.unwrap().len(), 1);
        assert_eq!(notifier.sent(), ["post-1 published"]);
    });
}
```

Inline, a failing handler never undoes the commit, and does not stop the
other handlers. The save reports it:

```rust
notifier.set_down(true);
let post = blog.publisher().load(&id).await?.unwrap();
let mut events = Vec::new();
post.publish(&mut events)?;
let saved = blog.publisher().append(post, events).await?;   // the engine's low-level commit

let failure = saved.processing.unwrap_err();          // NotifyOnPublish failed...
assert_eq!(saved.events.len(), 1);                     // ...but the event is stored
assert!(summaries.find(id).await?.unwrap().published); // and the projection still ran
```

Inline processing has no retries and no positions: fine for tests and
small tools. Part 5 moves the handlers to tracking processors.

A projection is also easy to test alone, by replaying events into it:

```rust
let projection = PostSummaryProjection::new(Arc::clone(&summaries));
projection.reset(&mut ProcessingContext::new(&mut (), false)).await?;
for event in blog.publisher().store().all() {
    projection.handle(&event, &mut ProcessingContext::new(&mut (), true)).await?;
}
```

Next: [5. Going asynchronous](05-async.md).
