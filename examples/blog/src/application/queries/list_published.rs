use std::sync::Arc;

use cqrs::{EventEnvelope, Handler, RefreshedBy};

use super::{PostSummaryRepository, PostSummary};
use crate::{application::BlogError, domain::PostEvent};

/// Every published post, ordered by id.
#[derive(Clone, Debug, cqrs::Query)]
#[query(output = Vec<PostSummary>, error = BlogError)]
pub struct ListPublished;

/// A new draft is not published: only later events can change the list.
impl RefreshedBy<PostEvent> for ListPublished {
    fn refreshed_by(&self, event: &EventEnvelope<PostEvent>) -> bool {
        !matches!(event.payload, PostEvent::Created { .. })
    }
}

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
