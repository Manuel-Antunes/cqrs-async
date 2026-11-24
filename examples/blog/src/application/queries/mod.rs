//! Queries: messages that only read. They are answered from the read model
//! ([`PostSummaryRepository`]), never from the event store.

/// Looking up one post.
mod find_post;
/// Listing the public posts.
mod list_published;
/// The read model the queries answer from, and its repository (port).
mod post_summary;

use std::sync::Arc;

pub use find_post::FindPost;
pub use list_published::ListPublished;
pub use post_summary::{PostSummary, PostSummaryRepository, PostSummaryStore};

/// Subscribing to the queries: their updates come from the projection.
pub use crate::application::projections::PostSubscriptions;

/// Read side: answers every query of the posts from the read model, one
/// `QueryHandler` impl each (in the query's file). Also generates the
/// trait [`PostQueries`] (asking them all); subscribing to them is
/// [`PostSubscriptions`], from the projection that pushes their updates.
#[cqrs::query_gateway(queries = PostQueries, handles(FindPost, ListPublished))]
pub struct PostQueryGateway<M> {
    /// The read model, shared with the projection that writes it.
    summaries: Arc<M>,
}

impl<M> PostQueryGateway<M> {
    /// The gateway over the read model.
    pub fn new(summaries: Arc<M>) -> Self {
        Self { summaries }
    }
}
