use std::sync::atomic::{AtomicU64, Ordering};

use cqrs::{AggregateLoader, Appender, EventPublisher, LoadStream, Loaded, SnapshotStore, SourceEvents, StoreError};

use crate::{
    application::commands::PostRepository,
    domain::{Post, PostEvent, PostId},
};

/// The posts' repository by their events: each post rebuilt from its events
/// in the event store (`S`), from a snapshot when the repository keeps them
/// (`N`), and saved by committing the new events through the publisher `P`
/// (the unit's events service, or an inline publisher).
pub struct EventSourcedPostRepository<S, P, N = ()> {
    /// Rebuilds the posts.
    loader: AggregateLoader<Post, S, N>,
    /// Where the new events are committed.
    events: P,
    /// Ids are sequential per process: enough in memory, where a restart
    /// loses the posts too. A collision would surface as a conflict, never
    /// as an overwrite.
    next_id: AtomicU64,
}

impl<S, P> EventSourcedPostRepository<S, P> {
    /// A repository reading posts from `store` and committing through
    /// `events`, replaying each post's whole stream.
    pub fn new(store: S, events: P) -> Self {
        Self {
            loader: AggregateLoader::new(store),
            events,
            next_id: AtomicU64::new(1),
        }
    }
}

impl<S, P, N> EventSourcedPostRepository<S, P, N> {
    /// Keeps a snapshot of a post in `snapshots` whenever finding it replayed
    /// `every` events or more, so the next find replays only what came after.
    pub fn with_snapshots<N2>(self, snapshots: N2, every: u64) -> EventSourcedPostRepository<S, P, N2> {
        EventSourcedPostRepository {
            loader: self.loader.with_snapshots(snapshots, every),
            events: self.events,
            next_id: self.next_id,
        }
    }
}

impl<S, P, N> PostRepository for EventSourcedPostRepository<S, P, N>
where
    S: LoadStream<PostEvent> + SourceEvents<PostEvent>,
    P: EventPublisher<PostEvent>,
    N: SnapshotStore<Post>,
{
    async fn next_id(&self) -> Result<PostId, StoreError> {
        Ok(PostId(self.next_id.fetch_add(1, Ordering::Relaxed)))
    }

    async fn find(&self, id: PostId) -> Result<Option<Loaded<Post>>, StoreError> {
        self.loader.load(&id).await
    }

    async fn save(&self, post: Loaded<Post>, appender: &mut Appender<PostEvent>) -> Result<(), StoreError> {
        appender.commit(post, &self.events).await
    }
}
