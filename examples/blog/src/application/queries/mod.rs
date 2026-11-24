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

/// Where the projection pushes the queries' new results (Axon's
/// `QueryUpdateEmitter`), one emitter per query type: subscribers to a query
/// get its current result, then these updates.
#[derive(Default, cqrs::QueryUpdates)]
pub struct PostUpdates {
    /// New summaries of one post, for [`FindPost`] subscribers.
    pub find: cqrs::QueryUpdateEmitter<FindPost>,
    /// The new list of published posts, for [`ListPublished`] subscribers.
    pub published: cqrs::QueryUpdateEmitter<ListPublished>,
}

/// Read side: answers every query of the posts from the read model, one
/// `QueryHandler` impl each (in the query's file). Also generates the
/// traits [`PostQueries`] (asking them all) and [`PostSubscriptions`]
/// (subscribing to them all).
#[cqrs::query_gateway(
    queries = PostQueries,
    subscriptions = PostSubscriptions,
    handles(FindPost, ListPublished)
)]
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
