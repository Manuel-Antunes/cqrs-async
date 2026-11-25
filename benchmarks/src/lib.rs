//! Shared fixtures for the benchmarks.
//!
//! - `benches/micro.rs` (divan): single operations, in nanoseconds.
//! - `src/bin/load.rs` (Tokio harness): throughput and latency under
//!   concurrency, written to `results/<label>.md` so runs can be compared.
//!
//! Every use of the event-processing API lives in [`live`], so a refactor of
//! that API changes one module and the scenarios stay identical.

use blog::domain::{Post, PostEvent, PostId};

use cqrs::{
    aggregate::{AggregateLoader, Loaded, SnapshotStore},
    command::{EventPublisher, Pending},
    event::{Metadata, StreamId},
    processing::InlinePublisher,
    store::{EventStore, ExpectedVersion, InMemoryEventStore, StoreError},
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
    /// Returns its input plus one.
    #[cqrs::command(output = u64, error = cqrs::store::StoreError)]
    pub struct Ping(pub u64);

    /// Answers its input plus one: routing with no unit of work.
    #[cqrs::query(output = u64, error = cqrs::store::StoreError)]
    pub struct Echo(pub u64);

    /// A query gateway with one route.
    #[cqrs::query_gateway(handles(Echo))]
    pub struct PingGateway;

    #[cqrs::query_handler]
    impl cqrs::query::QueryHandler<Echo> for PingGateway {
        async fn handle(&self, echo: Echo) -> Result<u64, cqrs::store::StoreError> {
            Ok(echo.0 + 1)
        }
    }

    /// What [`PingCommands`] would record.
    #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
    pub struct Pinged;

    /// A gateway of commands with one route.
    #[cqrs::command_gateway(handles(Ping))]
    pub struct PingCommands<P> {
        /// The unit of work (one that stores nothing).
        #[gateway(unit)]
        pub events: P,
    }

    /// Handles [`Ping`] as a command handler: through an appender and a
    /// unit of work, recording nothing.
    #[cqrs::command_handler]
    impl<P> cqrs::command::CommandHandler<Ping> for PingCommands<P> {
        async fn handle(
            &self,
            ping: &Ping,
            _: &mut cqrs::command::CommandContext<Pinged>,
        ) -> Result<u64, cqrs::store::StoreError> {
            Ok(ping.0 + 1)
        }
    }

    /// A unit of work that stores nothing.
    pub struct NoStore;

    impl cqrs::command::UnitOfWork<Pinged, cqrs::command::Autocommit> for NoStore {
        async fn begin(&self) -> Result<(), cqrs::store::StoreError> {
            Ok(())
        }

        async fn append(
            &self,
            (): &mut (),
            _: cqrs::command::Write<Pinged>,
        ) -> Result<Vec<cqrs::event::EventEnvelope<Pinged>>, cqrs::store::StoreError> {
            Ok(Vec::new())
        }

        async fn commit(&self, (): ()) -> Result<(), cqrs::store::StoreError> {
            Ok(())
        }

        async fn announce(&self, _: Vec<cqrs::event::EventEnvelope<Pinged>>) {}
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
            &mut (),
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
pub type PostsStore = InMemoryEventStore<PostEvent>;

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
        let store: PostsStore = InMemoryEventStore::new();
        Self {
            loader: AggregateLoader::new(store.clone()),
            publisher: InlinePublisher::new(store.clone()),
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
    pub async fn commit(
        &self,
        post: impl Into<Loaded<Post>>,
        events: Vec<PostEvent>,
    ) -> Result<Option<u64>, StoreError> {
        self.publisher.commit(post.into(), Pending::new(events)).await
    }
}
