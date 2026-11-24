//! The posts' projection: one struct with what its handlers need, one
//! handler per namespace, each run by a processor of its own (`post.summaries`,
//! `post.notifications`), with its own tokens.

/// Announcing published posts: a side effect.
mod notifications;
/// The summaries the queries answer from: a read model.
mod summaries;

use std::sync::Arc;

pub use notifications::{NotificationError, Notifier};
pub use summaries::PostSummaryUpdates;

use crate::domain::PostEvent;

/// Everything the posts' events are projected into: the summaries' read
/// model (namespace `summaries`) and the notifications (namespace
/// `notifications`).
#[cqrs::projection(name = "post", events = PostEvent, namespaces("summaries", "notifications"))]
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
