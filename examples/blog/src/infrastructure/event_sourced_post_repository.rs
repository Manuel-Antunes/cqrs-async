use cqrs::{EventStore, Loaded, StoreError};

use crate::{
    application::commands::PostRepository,
    domain::{Post, PostEvent, PostId},
};

/// The posts' repository: each post rebuilt from its events in the event
/// store (`S`, shared with the unit that commits them).
pub struct EventSourcedPostRepository<S> {
    /// Where the posts' events are.
    store: S,
}

impl<S> EventSourcedPostRepository<S> {
    /// A repository reading posts from `store`.
    pub fn new(store: S) -> Self {
        Self { store }
    }
}

impl<S: EventStore<PostEvent>> PostRepository for EventSourcedPostRepository<S> {
    async fn find(&self, id: PostId) -> Result<Option<Loaded<Post>>, StoreError> {
        cqrs::load(&self.store, &id).await
    }
}
