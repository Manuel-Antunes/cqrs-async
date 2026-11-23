//! Projections: they keep the read models up to date from committed
//! events, written against the read models' ports (the adapters live in
//! the infrastructure).

/// The summaries the queries answer from.
mod post_summary;

pub use post_summary::PostSummaryProjection;
