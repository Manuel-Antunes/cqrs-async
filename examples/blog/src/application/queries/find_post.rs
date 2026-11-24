use cqrs::QueryHandler;

use super::{PostQueryGateway, PostSummary, PostSummaryRepository};
use crate::{application::BlogError, domain::PostId};

/// One post, published or not; `None` if it does not exist (yet).
#[cqrs::query(output = Option<PostSummary>, error = BlogError)]
#[derive(Clone, Debug)]
pub struct FindPost {
    /// The post to look up.
    pub id: PostId,
}

/// Answers from the read model.
#[cqrs::query_handler]
impl<M: PostSummaryRepository> QueryHandler<FindPost> for PostQueryGateway<M> {
    async fn handle(&self, query: FindPost) -> Result<Option<PostSummary>, BlogError> {
        Ok(self.summaries.find(query.id).await?)
    }
}
