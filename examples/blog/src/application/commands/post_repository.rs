use std::future::Future;

use cqrs::{Appender, Loaded, StoreError};

use crate::domain::{Post, PostEvent, PostId};

/// Where the command handlers find posts and keep what they decided: the
/// application's own port (as a repository in NestJS or Axon), with only
/// what its use cases need. An adapter decides how, and the handlers never
/// know:
///
/// - [`EventSourcedPostRepository`](crate::infrastructure::EventSourcedPostRepository)
///   rebuilds each post from its events and commits the new ones through
///   the library's publisher;
/// - [`SeaOrmPostRepository`](crate::infrastructure::SeaOrmPostRepository)
///   keeps each post's state in a table, with SeaORM, and appends the
///   events in the same transaction.
pub trait PostRepository: Send + Sync {
    /// An id no post has yet.
    fn next_id(&self) -> impl Future<Output = Result<PostId, StoreError>> + Send;

    /// The post, at the version it was read at; `None` if it does not
    /// exist.
    fn find(&self, id: PostId) -> impl Future<Output = Result<Option<Loaded<Post>>, StoreError>> + Send;

    /// Keeps what the domain decided on `post` (the events pending in
    /// `appender`), expecting it at the version it was found at (or not to
    /// exist, for a [`New`](cqrs::New) one).
    fn save(
        &self,
        post: Loaded<Post>,
        appender: &mut Appender<PostEvent>,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}
