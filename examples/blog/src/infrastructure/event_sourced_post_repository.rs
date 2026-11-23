use cqrs::{AggregateLoader, EventStore, Loaded, SnapshotStore, StoreError};

use crate::{
    application::commands::PostRepository,
    domain::{Post, PostEvent, PostId},
};

/// The posts' repository: each post rebuilt from its events in the event
/// store (`S`, shared with the unit that commits them), from a snapshot when
/// the repository keeps them (`N`).
pub struct EventSourcedPostRepository<S, N = ()> {
    /// Rebuilds the posts.
    loader: AggregateLoader<Post, S, N>,
}

impl<S> EventSourcedPostRepository<S> {
    /// A repository reading posts from `store`, replaying each post's whole
    /// stream.
    pub fn new(store: S) -> Self {
        Self {
            loader: AggregateLoader::new(store),
        }
    }
}

impl<S, N> EventSourcedPostRepository<S, N> {
    /// Keeps a snapshot of a post in `snapshots` whenever finding it replayed
    /// `every` events or more, so the next find replays only what came after.
    pub fn with_snapshots<N2>(self, snapshots: N2, every: u64) -> EventSourcedPostRepository<S, N2> {
        EventSourcedPostRepository {
            loader: self.loader.with_snapshots(snapshots, every),
        }
    }
}

impl<S, N> PostRepository for EventSourcedPostRepository<S, N>
where
    S: EventStore<PostEvent>,
    N: SnapshotStore<Post>,
{
    async fn find(&self, id: PostId) -> Result<Option<Loaded<Post>>, StoreError> {
        self.loader.load(&id).await
    }
}
