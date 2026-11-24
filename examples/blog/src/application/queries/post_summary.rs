use std::future::Future;

use cqrs::StoreError;

use crate::domain::PostId;

/// A post as the read side shows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostSummary {
    /// The post's id.
    pub id: PostId,
    /// Its current title.
    pub title: String,
    /// Whether it is public.
    pub published: bool,
}

/// The read model, as the queries read it (a port, as Axon's tutorial has
/// `CourseStatsRepository`). It is fed by committed events, so it is
/// eventually consistent with the write side.
pub trait PostSummaryRepository: Send + Sync {
    /// The summary of one post, or `None` if the read model has not seen it
    /// (it may not exist, or its creation may not be projected yet).
    fn find(&self, id: PostId) -> impl Future<Output = Result<Option<PostSummary>, StoreError>> + Send;

    /// Published posts, ordered by id.
    fn published(&self) -> impl Future<Output = Result<Vec<PostSummary>, StoreError>> + Send;
}

/// The read model, as the projection writes it: in the unit of work `Tx`
/// the processor hands it with each event, so a read model in the same
/// database as the processor's tokens commits each change with its token
/// (an event is applied once). In memory, `Tx` is `()`.
pub trait PostSummaryStore<Tx>: Send + Sync {
    /// Stores a new post's `summary`.
    fn save(&self, tx: &mut Tx, summary: PostSummary) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Changes the summary of `id` with `change`; `false` if there is none.
    fn update(
        &self,
        tx: &mut Tx,
        id: PostId,
        change: impl FnOnce(&mut PostSummary) + Send,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Removes every summary, before a replay rebuilds them.
    fn clear(&self, tx: &mut Tx) -> impl Future<Output = Result<(), StoreError>> + Send;
}
