//! Projections: event handling components fed with committed events, each
//! run by a processor named after its namespace. They are written against
//! ports; the adapters live in the infrastructure.
//!
//! What they do with an event is theirs to say, and the library sees no
//! difference: one keeps a read model up to date (and rebuilds it on a
//! replay), another has a side effect, such as a notification, and does not
//! see replays (`REPLAY = false`), so it never happens twice.

/// Announcing published posts: a side effect.
mod notify_on_publish;
/// The summaries the queries answer from: a read model.
mod post_summary;

pub use notify_on_publish::{NotificationError, Notifier, NotifyOnPublish};
pub use post_summary::{PostSummaryProjection, PostSummaryUpdates};
