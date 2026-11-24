use std::future::Future;

use cqrs::{Loaded, StoreError};

use crate::domain::{Post, PostId};

/// Where the command handlers find posts and keep their state: the
/// application's own port (as a repository in NestJS or Axon), with only
/// what its use cases need. It works in the transaction of the command's
/// unit of work (`Tx`), lent by the gateway: what it writes commits with
/// the post's events, or not at all. An adapter decides how, and the
/// handlers never know:
///
/// - [`EventSourcedPostRepository`](crate::infrastructure::EventSourcedPostRepository)
///   rebuilds each post from its events: its events are its state, so
///   saving writes nothing more (`Tx = ()`, in memory);
/// - [`SeaOrmPostRepository`](crate::infrastructure::SeaOrmPostRepository)
///   keeps each post's state in a table, with SeaORM, in the PostgreSQL
///   transaction the events are appended in.
pub trait PostRepository: Send + Sync {
    /// The unit of work's transaction it works in.
    type Tx: Send;

    /// An id no post has yet.
    fn next_id(&self, tx: &mut Self::Tx) -> impl Future<Output = Result<PostId, StoreError>> + Send;

    /// The post, at the version it was read at; `None` if it does not
    /// exist.
    fn find(
        &self,
        id: PostId,
        tx: &mut Self::Tx,
    ) -> impl Future<Output = Result<Option<Loaded<Post>>, StoreError>> + Send;

    /// Keeps `post` as the command left it (after its recorded events, at
    /// their version), in `tx`.
    fn save(&self, post: &Loaded<Post>, tx: &mut Self::Tx) -> impl Future<Output = Result<(), StoreError>> + Send;
}
