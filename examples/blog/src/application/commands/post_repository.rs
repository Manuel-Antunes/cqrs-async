use std::future::Future;

use cqrs::{
    aggregate::{AggregateLoader, Repository, SnapshotStore},
    store::{LoadStream, ReadLog, SourceEvents, StoreError},
};

use crate::domain::{Post, PostEvent, PostId};

/// Where the command handlers find posts and keep them: the application's
/// own port. Finding and saving a post are the library's [`Repository`]
/// contract (`find`, and `save`, which records the post's events and
/// persists it); the port adds only what its use cases need besides. It works
/// in the transaction of the command's unit of work (`Tx`), which the handler
/// lends it from its context: what it writes commits with the post's events,
/// or not at all. An adapter decides how, and the handlers never know:
///
/// - the repository the engine hands out (`events.repository::<Post>()`)
///   rebuilds each post from its events, and takes the ids from the log's
///   positions (the impl below);
/// - [`SeaOrmPostRepository`](crate::infrastructure::SeaOrmPostRepository)
///   keeps each post's state in a table, with SeaORM, in the PostgreSQL
///   transaction the events are appended in, and takes the ids from a
///   sequence.
pub trait PostRepository: Repository<Post> {
    /// An id no post has yet.
    fn next_id(&self, tx: &mut Self::Tx) -> impl Future<Output = Result<PostId, StoreError>> + Send;
}

/// The repository the engine hands out (`events.repository::<Post>()`, which
/// rebuilds each post from its events) is a [`PostRepository`]: all it lacks
/// is the ids, taken from the log itself.
///
/// The next id is the position after the log's last event: no post has it,
/// as every post's id is at most the position of its first event. Ids grow
/// but are not consecutive, and two posts created at the same moment get the
/// same one, the second then failing with a conflict (never an overwrite):
/// enough in memory and for tests. The PostgreSQL repository takes them from
/// a sequence instead.
impl<S, N, Tx> PostRepository for AggregateLoader<Post, S, N, Tx>
where
    S: LoadStream<PostEvent> + SourceEvents<PostEvent> + ReadLog<PostEvent>,
    N: SnapshotStore<Post>,
    Tx: Send,
{
    async fn next_id(&self, _: &mut Tx) -> Result<PostId, StoreError> {
        Ok(PostId(self.store().last_position().await? + 1))
    }
}
