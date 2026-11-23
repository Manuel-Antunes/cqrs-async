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

/// The read model's repository (a port, as Axon's tutorial has
/// `CourseStatsRepository`): the queries read [`PostSummary`] rows from it,
/// the projection writes them. It is fed by committed events, so it is eventually
/// consistent with the write side.
pub trait PostSummaryRepository: Send + Sync {
    /// The summary of one post, or `None` if the read model has not seen it
    /// (it may not exist, or its creation may not be projected yet).
    fn find(
        &self,
        id: PostId,
    ) -> impl Future<Output = Result<Option<PostSummary>, StoreError>> + Send;

    /// Published posts, ordered by id.
    fn published(&self) -> impl Future<Output = Result<Vec<PostSummary>, StoreError>> + Send;

    /// Stores a new post's `summary`.
    fn save(&self, summary: PostSummary) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Applies `change` to the summary of post `id`, in place (Axon's
    /// managed entity: an SQL adapter selects it for update, then writes
    /// it back). `false`: there is none.
    fn update(
        &self,
        id: PostId,
        change: impl FnOnce(&mut PostSummary) + Send,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Removes every summary, before a replay rebuilds them.
    fn clear(&self) -> impl Future<Output = Result<(), StoreError>> + Send;
}
