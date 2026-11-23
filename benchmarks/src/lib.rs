//! Shared fixtures for the benchmarks.
//!
//! - `benches/micro.rs` (divan): single operations, in nanoseconds.
//! - `src/bin/load.rs` (Tokio harness): throughput and latency under
//!   concurrency, written to `results/<label>.md` so runs can be compared.
//!
//! Every use of the event-processing API lives in [`live`], so a refactor of
//! that API changes one module and the scenarios stay identical.

use blog::domain::{Post, PostEvent, PostId};
use std::sync::Arc;

use cqrs::{
    AggregateLoader, EventPublisher, EventStore, ExpectedVersion, InMemoryEventStore, InlinePublisher, Loaded,
    Metadata, Pending, SnapshotStore, StoreError, StreamId,
};

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

    /// Handles [`Ping`] as a command handler: through an appender, holding
    /// a publisher, committing nothing.
    pub struct PingCommandHandler<P> {
        /// Where a commit would go (never used).
        pub events: P,
    }

    #[cqrs::command_handler]
    impl<P: cqrs::EventPublisher<Pinged>> cqrs::CommandHandler for PingCommandHandler<P> {
        async fn handle(&self, ping: &Ping, _: &mut cqrs::Appender<Pinged>) -> Result<u64, ()> {
            Ok(ping.0 + 1)
        }
    }

    /// A gateway of commands with one route.
    #[derive(cqrs::Gateway)]
    #[gateway(commands)]
    pub struct PingCommands<P> {
        /// The only handler.
        pub ping: PingCommandHandler<P>,
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

/// The shared in-memory store of the posts.
pub type PostsStore = Arc<InMemoryEventStore<PostEvent>>;

/// What an application has around the in-memory store with no processing: a
/// loader (its repository's adapter) and an inline publisher (what command
/// handlers commit through).
pub struct PlainPosts<N = ()> {
    /// The store both share.
    pub store: PostsStore,
    /// Rebuilds posts.
    pub loader: AggregateLoader<Post, PostsStore, N>,
    /// Commits what is decided.
    pub publisher: InlinePublisher<PostEvent, PostsStore>,
}

impl PlainPosts {
    /// An empty store, loading without snapshots.
    pub fn new() -> Self {
        let store: PostsStore = Arc::new(InMemoryEventStore::new());
        Self {
            loader: AggregateLoader::new(Arc::clone(&store)),
            publisher: InlinePublisher::new(Arc::clone(&store)),
            store,
        }
    }
}

impl Default for PlainPosts {
    fn default() -> Self {
        Self::new()
    }
}

impl<N> PlainPosts<N> {
    /// Loads with a snapshot every `every` replayed events.
    pub fn with_snapshots<N2>(self, snapshots: N2, every: u64) -> PlainPosts<N2> {
        PlainPosts {
            store: self.store,
            loader: self.loader.with_snapshots(snapshots, every),
            publisher: self.publisher,
        }
    }
}

impl<N: SnapshotStore<Post>> PlainPosts<N> {
    /// The post `id`, found, if it has events.
    ///
    /// # Panics
    ///
    /// If the (in-memory) store fails.
    pub async fn find(&self, id: PostId) -> Option<Loaded<Post>> {
        self.loader.load(&id).await.expect("in-memory load")
    }

    /// Commits `events` to `post`, expecting the version it was loaded at.
    ///
    /// # Errors
    ///
    /// The store's.
    pub async fn commit(&self, post: Loaded<Post>, events: Vec<PostEvent>) -> Result<Option<u64>, StoreError> {
        self.publisher.commit(post, Pending::new(events)).await
    }
}
