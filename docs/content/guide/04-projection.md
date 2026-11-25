---
title: "4. The projection"
sidebar_position: 14
---

A projection turns committed events into read models and side effects.
There is one per aggregate root: a struct holding what its handlers need,
with one event handler per **namespace**. Each namespace is run on its
own, with its own position in the log, so a failing notification never
holds the read model back.

## The projection

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

impl<S, N> PostProjection<S, N> {
    pub fn new(summaries: Arc<S>, notifier: Arc<N>) -> Self {
        Self { summaries, notifier }
    }
}
```

- `name` and `namespaces(..)` name the processors that will run it, one
  per namespace: `"post.summaries"` and `"post.notifications"`. Each keys
  its own tokens (Axon's `@Namespace`).
- `events = PostEvent`: a projection follows one aggregate's events. The
  namespace lives on the projection, not on the event: the domain does
  not know who consumes it.
- `updates(..)` and `subscriptions = ..` are for subscription queries
  (part 6).

The compiler checks both ways: a namespace listed without its handler
cannot run, and a handler for a namespace not listed does not compile.

Each namespace gets a file of its own:

```text
src/application/projections/
├── mod.rs
└── post/
    ├── mod.rs             # PostProjection
    ├── summaries.rs       # the read model
    └── notifications.rs   # a side effect
```

## The summaries: a read model

```rust
// src/application/projections/post/summaries.rs
use cqrs::{
    event::EventEnvelope,
    processing::{EventHandler, HandlerError, ProcessingContext},
};

#[cqrs::event_handler(namespace = "summaries", batch = 64)]
impl<S, N, Tx> EventHandler for PostProjection<S, N>
where
    S: PostSummaryRepository + PostSummaryStore<Tx>,
    N: Send + Sync,
    Tx: Send,
{
    async fn handle(
        &self,
        event: &EventEnvelope<PostEvent>,
        context: &mut ProcessingContext<'_, Tx>,
    ) -> Result<(), HandlerError> {
        let tx = context.tx();
        let found = match &event.payload {
            PostEvent::Created { id, title } => {
                let summary = PostSummary {
                    id: *id,
                    title: title.clone(),
                    published: false,
                };
                self.summaries.save(tx, summary).await?;
                true
            }
            PostEvent::Retitled { title } => {
                self.summaries
                    .update(tx, post_of(event)?, |s| s.title.clone_from(title))
                    .await?
            }
            PostEvent::Published => {
                self.summaries
                    .update(tx, post_of(event)?, |s| s.published = true)
                    .await?
            }
        };
        if !found {
            // Events of a stream arrive in order, its creation first.
            return Err(HandlerError::msg(format!(
                "{} has no summary: its creation was not projected",
                event.stream
            )));
        }
        Ok(())
    }

    /// Before a replay rebuilds it.
    async fn reset(&self, context: &mut ProcessingContext<'_, Tx>) -> Result<(), HandlerError> {
        self.summaries.clear(context.tx()).await?;
        Ok(())
    }

    // `updated`, which pushes the new summaries to subscribers: part 6.
}

/// The post an event belongs to.
fn post_of(event: &EventEnvelope<PostEvent>) -> Result<PostId, HandlerError> {
    event
        .aggregate_id::<Post>()
        .ok_or_else(|| HandlerError::msg(format!("not a post stream: {}", event.stream)))
}
```

The envelope carries the event (`payload`) and what the store recorded
about it: `stream`, `sequence`, `position`, `recorded_at`, `metadata`.
`aggregate_id::<Post>()` reads the post's id back from the stream.

What `#[cqrs::event_handler(namespace = "summaries", batch = 64)]` does:

- reads the event type from `&EventEnvelope<PostEvent>` and the unit of
  work from `&mut ProcessingContext<'_, Tx>`;
- makes the impl the handler of the `summaries` namespace;
- `batch = 64`: up to 64 waiting events are handled in one unit of work,
  with one token update. The default is 1, as in Axon.

**The unit of work is a type parameter.** The handler writes through
`PostSummaryStore<Tx>`, the write half of the read model's port (part 3),
in the transaction the processor hands it (`context.tx()`). In
memory `Tx` is `()`; on PostgreSQL it is the processor's transaction, so
each change commits with the processor's token (part 8). The projection
does not choose: the composition root does, by picking the adapter.

**Why a batch.** Everything this handler does is undone with its
transaction, so batching is safe: a failing batch is rolled back and its
events handled again one at a time. Rebuilding 5000 events took 0.25 s
with batches of 64, instead of 4.6 s one by one.

Things the projection does **not** do:

- **Check sequences or remember what it applied.** Whoever runs it
  delivers each post's events once and in order (part 5 shows how a
  processor guarantees it with its unit of work).
- **Read the event store.** Everything it needs is in the event.

## The notifications: a side effect

The application declares the port it needs, next to the handler that uses
it:

```rust
// src/application/projections/post/notifications.rs
/// Port to whatever delivers notifications (e-mail, chat, ...).
pub trait Notifier: Send + Sync {
    fn notify(&self, message: String) -> impl Future<Output = Result<(), NotificationError>> + Send;
}

/// Announces every post that gets published. A notification already went
/// out: a replay must not send it again, so it does not see replays; and
/// it is not undone with a transaction, so it takes events one at a time
/// (no `batch`): a failing batch, handled again, would send it twice.
#[cqrs::event_handler(namespace = "notifications")]
impl<S: Send + Sync, N: Notifier> EventHandler for PostProjection<S, N> {
    const REPLAY: bool = false;

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

To the library, a side effect is just another namespace. Two settings make
the difference:

- **`REPLAY = false`**: when the projection is replayed (part 5), the
  summaries are rebuilt, but nobody is notified again.
- **No `batch`**: a notification cannot be rolled back, so a failed batch
  handled again would send some twice.

Its context names no `Tx`, so it runs in any unit of work.

This is also where side effects of a command go. A command handler is run
again when it loses a race for the post (part 2), so it must not send
anything itself: it records events, and a namespace reacts to them once
they are committed.

`InMemoryNotifier` (infrastructure) records the messages and can be
switched off to simulate an outage.

## Wiring it: the composition root

All the wiring lives in one file, `src/config.rs`, the only one that names
concrete adapters. It creates the stores and the services, and builds the
blog's **CQRS unit** with a typed builder:

```rust
// src/config.rs
pub type PostStore = InMemoryEventStore<PostEvent>;

pub fn in_memory_with(
    store: PostStore,
) -> (
    Cqrs<impl PostCommands, impl PostQueries>,
    Arc<InMemoryPostSummaryRepository>,
    Arc<InMemoryNotifier>,
) {
    let summaries = Arc::new(InMemoryPostSummaryRepository::default());
    let notifier = Arc::new(InMemoryNotifier::default());
    // Synchronous: no subscriptions to push updates to. Each namespace runs
    // isolated: a failing notification never holds the read model back.
    let projection = PostProjection::new(Arc::clone(&summaries), Arc::clone(&notifier));
    let publisher = InlinePublisher::<PostEvent, _, _>::new(store).with_projection(projection);
    let blog = Cqrs::from_publisher(publisher)
        .commands(|events| PostCommandGateway::new(events.repository::<Post>(), events))
        .queries(PostQueryGateway::new(Arc::clone(&summaries)))
        .build();
    (blog, summaries, notifier)
}
```

`in_memory()` is `in_memory_with(PostStore::default())`;
`events.repository::<Post>()` is the event-sourced repository from part 2:
the publisher hands it out, over its own store.

- **The store is created here and shared**: the publisher writes it, and the
  repository the publisher hands out reads it.
- **The publisher** is an `InlinePublisher` over the store. With
  `with_projection`, it runs every namespace right after each commit, in
  the caller's task, no runtime needed. It is built by hand
  (`Cqrs::from_publisher`) because it runs the handlers inline; part 5
  starts from `Cqrs::new(Events::new(store))` instead.
- **`.commands(|events| ..)` hands the closure the unit's publisher**, which
  the command gateway keeps as its `#[gateway(unit)]` field: the unit of
  work every command commits through. The query gateway needs none.
- **Each step changes the builder's type**, so the order is checked, and
  every type is inferred. What the unit exposes is named by the traits
  the gateways generate: `impl PostCommands`, `impl PostQueries`.

The unit has `commands()`, `queries()` and `events()`.

## The first end-to-end test

```rust
// tests/flow.rs
use blog::{application::{commands::*, queries::*}, in_memory};
use cqrs::{command::CommandGateway, query::QueryGateway, testing::block_on};

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
other namespace. The publisher logs the failure (or hands it to what you
give `InlinePublisher::on_failure(|failure| ..)`), and the command still
succeeds:

```rust
notifier.set_down(true);
blog.commands().send(PublishPost { id }).await.unwrap(); // the notification failed (logged)...
assert!(summaries.find(id).await.unwrap().unwrap().published); // ...but the event is stored and the summary updated
```

Inline processing has no retries, no positions and no replays: fine for
tests and small tools. Part 5 moves the namespaces to tracking processors.

A projection is also easy to test alone, by replaying events into one of
its namespaces (`summaries` is index 0, in the order `namespaces(..)`
lists them):

```rust
// tests/flow.rs
use cqrs::processing::{ProcessingContext, Route};

let projection = PostProjection::new(Arc::clone(&summaries), Arc::new(InMemoryNotifier::default()));
projection.reset(0, &mut ProcessingContext::new(&mut (), false)).await.unwrap();
for event in store.all() {          // the store `in_memory_with(store)` was given
    projection.handle(0, &event, &mut ProcessingContext::new(&mut (), true)).await.unwrap();
}
assert_eq!(summaries.find(id).await.unwrap().unwrap().title, "b");
```

Next: [5. Going asynchronous](05-async.md).
