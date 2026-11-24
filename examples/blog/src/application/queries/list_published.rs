use cqrs::QueryHandler;

use super::{PostQueryGateway, PostSummary, PostSummaryRepository};
use crate::application::BlogError;

/// Every published post, ordered by id.
#[cqrs::query(output = Vec<PostSummary>, error = BlogError)]
#[derive(Clone, Debug)]
pub struct ListPublished;

/// Answers from the read model.
#[cqrs::query_handler]
impl<M: PostSummaryRepository> QueryHandler<ListPublished> for PostQueryGateway<M> {
    async fn handle(&self, _: ListPublished) -> Result<Vec<PostSummary>, BlogError> {
        Ok(self.summaries.published().await?)
    }
}
