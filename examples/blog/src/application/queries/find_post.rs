use std::sync::Arc;

use cqrs::Handler;

use super::{PostSummary, PostSummaryRepository};
use crate::{application::BlogError, domain::PostId};

/// One post, published or not; `None` if it does not exist (yet).
#[cqrs::query(output = Option<PostSummary>, error = BlogError)]
#[derive(Clone, Debug)]
pub struct FindPost {
    /// The post to look up.
    pub id: PostId,
}

/// Handles [`FindPost`] by reading the read model.
pub struct FindPostHandler<M> {
    /// The read model to answer from.
    summaries: Arc<M>,
}

impl<M> FindPostHandler<M> {
    /// A handler over `summaries`.
    pub fn new(summaries: Arc<M>) -> Self {
        Self { summaries }
    }
}

#[cqrs::query_handler]
impl<M: PostSummaryRepository> Handler for FindPostHandler<M> {
    async fn handle(&self, query: FindPost) -> Result<Option<PostSummary>, BlogError> {
        Ok(self.summaries.find(query.id).await?)
    }
}
