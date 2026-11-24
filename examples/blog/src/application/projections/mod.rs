//! Projections: one per aggregate, fed with its committed events, written
//! against ports (the adapters live in the infrastructure).
//!
//! A projection has namespaces, each with its own handler and processor.
//! What a handler does with an event is its own business, and the library
//! sees no difference: one keeps a read model up to date (and rebuilds it on
//! a replay), another has a side effect, such as a notification, and does
//! not see replays (`REPLAY = false`), so it never happens twice.

/// The posts' projection.
mod post;

pub use post::{NotificationError, Notifier, PostProjection, PostProjectionUpdates, PostSubscriptions};
