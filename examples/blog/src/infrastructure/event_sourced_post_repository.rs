use std::sync::atomic::{AtomicU64, Ordering};

use cqrs::{
    aggregate::{AggregateLoader, Loaded, SnapshotStore},
    store::{LoadStream, SourceEvents, StoreError},
};

use crate::{
    application::commands::PostRepository,
    domain::{Post, PostEvent, PostId},
};

/// The posts' repository by their events: each post rebuilt from its events
/// in the event store (`S`), from a snapshot when the repository keeps them
/// (`N`). Its events are its state, appended by the command's unit of work:
/// saving writes nothing more, so it needs no transaction (`Tx = ()`).
pub struct EventSourcedPostRepository<S, N = ()> {
    /// Rebuilds the posts.
    loader: AggregateLoader<Post, S, N>,
    /// Ids are sequential per process: enough in memory, where a restart
    /// loses the posts too. A collision would surface as a conflict, never
    /// as an overwrite.
    next_id: AtomicU64,
}

impl<S> EventSourcedPostRepository<S> {
    /// A repository reading posts from `store`, replaying each post's whole
    /// stream.
    pub fn new(store: S) -> Self {
        Self {
            loader: AggregateLoader::new(store),
            next_id: AtomicU64::new(1),
        }
    }
}

impl<S, N> EventSourcedPostRepository<S, N> {
    /// Keeps a snapshot of a post in `snapshots` whenever finding it replayed
    /// `every` events or more, so the next find replays only what came after.
    pub fn with_snapshots<N2>(self, snapshots: N2, every: u64) -> EventSourcedPostRepository<S, N2> {
        EventSourcedPostRepository {
            loader: self.loader.with_snapshots(snapshots, every),
            next_id: self.next_id,
        }
    }
}

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
