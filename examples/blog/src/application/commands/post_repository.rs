use std::future::Future;

use cqrs::{Loaded, StoreError};

use crate::domain::{Post, PostId};

/// Where the command handlers find posts: the application's own port (as a
/// repository in NestJS), with only the finders its use cases need. An
/// adapter decides how: the blog's rebuilds each post from its events
/// ([`EventSourcedPostRepository`](crate::infrastructure::EventSourcedPostRepository)).
///
/// Committing is not its job: handlers commit what the domain decided
/// through an [`EventPublisher`](cqrs::EventPublisher).
pub trait PostRepository: Send + Sync {
    /// The post, at the version it was read at; `None` if it does not
    /// exist.
    fn find(&self, id: PostId) -> impl Future<Output = Result<Option<Loaded<Post>>, StoreError>> + Send;
}
