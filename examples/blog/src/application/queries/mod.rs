//! Queries: messages that only read. They are answered from the read model
//! ([`PostSummaryRepository`]), never from the event store.

/// Looking up one post.
mod find_post;
/// Listing the public posts.
mod list_published;
/// The read model the queries answer from, and its repository (port).
mod post_summary;

use std::sync::Arc;

pub use find_post::{FindPost, FindPostHandler};
pub use list_published::{ListPublished, ListPublishedHandler};
pub use post_summary::{PostSummary, PostSummaryRepository, PostSummaryStore};

use crate::application::BlogError;

/// Every query of the read side, as one message. Also generates the ports
/// [`PostQueries`] (asking them all) and [`PostSubscriptions`] (subscribing
/// to them all).
#[derive(Clone, Debug, cqrs::Queries)]
#[queries(error = BlogError, port = PostQueries, subscriptions = PostSubscriptions, derive(Debug, PartialEq))]
pub enum PostQuery {
    /// See [`FindPost`].
    FindPost(FindPost),
    /// See [`ListPublished`].
    ListPublished(ListPublished),
}

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

/// Read side: routes each query to the field whose handler declares it, or
/// each [`PostQuery`] variant to its handler.
#[derive(cqrs::Gateway)]
#[gateway(queries = PostQuery)]
pub struct PostQueryGateway<M> {
    /// Handles [`FindPost`].
    find: FindPostHandler<M>,
    /// Handles [`ListPublished`].
    list_published: ListPublishedHandler<M>,
}

impl<M> PostQueryGateway<M> {
    /// Builds every query handler over the same read model.
    pub fn new(summaries: Arc<M>) -> Self {
        Self {
            find: FindPostHandler::new(Arc::clone(&summaries)),
            list_published: ListPublishedHandler::new(summaries),
        }
    }
}
