//! Policies: "when this happened, do that". They run on committed events,
//! after the commit, so a failing policy never undoes it.

/// Announcing published posts.
mod notify_on_publish;

pub use notify_on_publish::{NotificationError, Notifier, NotifyOnPublish};
