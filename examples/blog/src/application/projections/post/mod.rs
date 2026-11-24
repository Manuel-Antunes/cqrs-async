//! The posts' projection: one struct with what its handlers need, one
//! handler per namespace, each run by a processor of its own (`post.summaries`,
//! `post.notifications`), with its own tokens.

/// Announcing published posts: a side effect.
mod notifications;
/// The summaries the queries answer from: a read model.
mod summaries;

use std::sync::Arc;

use crate::{
    application::queries::{FindPost, ListPublished},
    domain::PostEvent,
};
pub use notifications::{NotificationError, Notifier};

/// Everything the posts' events are projected into: the summaries' read
/// model (namespace `summaries`) and the notifications (namespace
/// `notifications`). The summaries' changes are pushed to the subscribers
/// of [`FindPost`] and [`ListPublished`]; [`PostSubscriptions`] is the
/// trait of whatever subscribes to both.
#[cqrs::projection(
    name = "post",
    events = PostEvent,
    namespaces("summaries", "notifications"),
    updates(FindPost, ListPublished),
    subscriptions = PostSubscriptions
)]
pub struct PostProjection<S, N> {
    /// Where the summaries are kept.
    summaries: Arc<S>,
    /// Who delivers the announcements.
    notifier: Arc<N>,
}

impl<S, N> PostProjection<S, N> {
    /// The projection, writing summaries to `summaries` and announcing
    /// through `notifier`.
    pub fn new(summaries: Arc<S>, notifier: Arc<N>) -> Self {
        Self { summaries, notifier }
    }
}
