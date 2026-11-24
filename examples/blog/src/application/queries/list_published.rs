use std::sync::Arc;

use cqrs::Handler;

use super::{PostSummary, PostSummaryRepository};
use crate::application::BlogError;

/// Every published post, ordered by id.
#[cqrs::query(output = Vec<PostSummary>, error = BlogError)]
#[derive(Clone, Debug)]
pub struct ListPublished;

/// Handles [`ListPublished`] by reading the read model.
pub struct ListPublishedHandler<M> {
    /// The read model to answer from.
    summaries: Arc<M>,
}

impl<M> ListPublishedHandler<M> {
    /// A handler over `summaries`.
    pub fn new(summaries: Arc<M>) -> Self {
        Self { summaries }
    }
}

#[cqrs::query_handler]
impl<M: PostSummaryRepository> Handler for ListPublishedHandler<M> {
    async fn handle(&self, _: ListPublished) -> Result<Vec<PostSummary>, BlogError> {
        Ok(self.summaries.published().await?)
    }
}
