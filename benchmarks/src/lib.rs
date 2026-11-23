//! Shared fixtures for the benchmarks.
//!
//! - `benches/micro.rs` (divan): single operations, in nanoseconds.
//! - `src/bin/load.rs` (Tokio harness): throughput and latency under
//!   concurrency, written to `results/<label>.md` so runs can be compared.
//!
//! Every use of the event-processing API lives in [`live`], so a refactor of
//! that API changes one module and the scenarios stay identical.

use blog::domain::{Post, PostEvent, PostId};
use cqrs::{EventSourced, EventStore, ExpectedVersion, InMemoryEventStore, Metadata, StreamId};

pub mod live;

/// Runs a future that never waits (everything in memory) by polling it once:
/// measures the operation, not an executor.
///
/// # Panics
///
/// If the future is not ready on the first poll.
pub fn now<F: std::future::Future>(future: F) -> F::Output {
    futures_util::FutureExt::now_or_never(future).expect("ready at once")
}

/// A trivial command and its handler: dispatch cost with no work behind it.
pub mod ping {
    use cqrs::Handler;

    /// Returns its input plus one.
    #[derive(cqrs::Command)]
    #[command(output = u64, error = ())]
    pub struct Ping(pub u64);

    /// Handles [`Ping`].
    pub struct PingHandler;

    impl Handler for PingHandler {
        type Message = Ping;

        async fn handle(&self, ping: Ping) -> Result<u64, ()> {
            Ok(ping.0 + 1)
        }
    }

    /// A derived gateway with one route.
    #[derive(cqrs::Gateway)]
    pub struct PingGateway {
        /// The only handler.
        pub ping: PingHandler,
    }

    /// What [`PingCommandHandler`] would record.
    #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent, cqrs::TagResolver)]
    pub struct Pinged;

    /// Handles [`Ping`] as a command handler: through an appender with a
    /// publisher, committing nothing.
    pub struct PingCommandHandler;

    #[cqrs::command_handler]
    impl cqrs::CommandHandler for PingCommandHandler {
        async fn handle(&self, ping: &Ping, _: &mut cqrs::Appender<Pinged>) -> Result<u64, ()> {
            Ok(ping.0 + 1)
        }
    }

    /// A gateway of commands with one route.
    #[derive(cqrs::Gateway)]
    #[gateway(commands)]
    pub struct PingCommands {
        /// The only handler.
        pub ping: PingCommandHandler,
    }

    /// A publisher that stores nothing.
    pub struct NoStore;

    impl cqrs::EventPublisher<Pinged> for NoStore {
        async fn commit<A: cqrs::Aggregate<Event = Pinged>>(
            &self,
            _: cqrs::Loaded<A>,
            _: cqrs::Pending<Pinged>,
        ) -> Result<Option<u64>, cqrs::StoreError> {
            Ok(None)
        }
    }
}

/// The history of one post: created, then `events - 1` retitles.
pub fn post_history(id: PostId, events: usize) -> Vec<PostEvent> {
    let mut history = Vec::with_capacity(events);
    history.push(PostEvent::Created {
        id,
        title: "title 0".to_owned(),
    });
    history.extend((1..events).map(|n| PostEvent::Retitled {
        title: format!("title {n}"),
    }));
    history
}

/// Appends a post's history straight to `store`, in one batch.
pub async fn seed_post<S: EventStore<PostEvent>>(store: &S, id: PostId, events: usize) {
    store
        .append(
            &StreamId::of::<Post>(&id),
            ExpectedVersion::NoStream,
            post_history(id, events),
            &Metadata::new(),
        )
        .await
        .expect("seeding a fresh stream");
}

/// Appends `posts` posts of `events_per_post` events each.
pub async fn seed_posts<S: EventStore<PostEvent>>(store: &S, posts: u64, events_per_post: usize) {
    for id in 1..=posts {
        seed_post(store, PostId(id), events_per_post).await;
    }
}

/// A repository with no processing, over the in-memory store.
pub type PlainPosts = EventSourced<Post, InMemoryEventStore<PostEvent>>;
