//! How far a projection got: what subscriptions and query updates wait on,
//! so a reader never sees a read model older than the change it was told
//! about.

use std::{sync::Arc, time::Duration};

use tokio::sync::watch;

use super::{ProcessorHandle, ProcessorStatus};
use crate::TokenStore;

/// How far a projection got.
pub trait ProjectionProgress: Clone + Send + Sync + 'static {
    /// Returns once the projection handled the event at `position` (or it
    /// can no longer tell, e.g. it stopped).
    fn reached(&self, position: u64) -> impl Future<Output = ()> + Send;
}

/// The progress of a processor as one of its instances sees it
/// ([`ProcessorStatus::handled`]): from its own segments while it owns them
/// all, at no cost; from every instance's stored tokens otherwise, so an
/// event projected by another instance counts too.
#[derive(Clone)]
pub struct ProcessorProgress(watch::Receiver<ProcessorStatus>);

impl ProcessorProgress {
    /// The progress of the processor `handle` runs.
    pub fn of(handle: &ProcessorHandle) -> Self {
        Self(handle.progress())
    }

    /// The progress published to `status` (a processor's
    /// [`progress`](crate::TrackingProcessor::progress)).
    pub fn new(status: watch::Receiver<ProcessorStatus>) -> Self {
        Self(status)
    }
}

impl ProjectionProgress for ProcessorProgress {
    async fn reached(&self, position: u64) {
        let mut status = self.0.clone();
        // Closed: the processor stopped; do not leave subscribers hanging.
        let _ = status.wait_for(|status| status.handled(position)).await;
    }
}

/// Several projections: reached once each of them reached the position
/// (what a [`Cqrs`](crate::Cqrs) unit's subscriptions wait for).
impl<P: ProjectionProgress> ProjectionProgress for Vec<P> {
    async fn reached(&self, position: u64) {
        for progress in self {
            progress.reached(position).await;
        }
    }
}

/// The progress of a processor across all its instances, read from the
/// shared token store alone: once every segment's cursor covers a position.
/// For a process that does not run the processor itself; one that does
/// gets the same from [`ProcessorProgress`].
pub struct GlobalProgress<T> {
    /// The shared token store.
    tokens: T,
    /// The processor's name (its token key).
    processor: Arc<str>,
    /// How often the token store is checked while waiting.
    poll: Duration,
}

impl<T> GlobalProgress<T> {
    /// The progress of `processor`, read from `tokens` every 50 ms while
    /// waiting.
    pub fn new(tokens: T, processor: &str) -> Self {
        Self {
            tokens,
            processor: processor.into(),
            poll: Duration::from_millis(50),
        }
    }
}

impl<T: Clone> Clone for GlobalProgress<T> {
    fn clone(&self) -> Self {
        Self {
            tokens: self.tokens.clone(),
            processor: Arc::clone(&self.processor),
            poll: self.poll,
        }
    }
}

impl<T: TokenStore + Clone + 'static> ProjectionProgress for GlobalProgress<T> {
    async fn reached(&self, position: u64) {
        loop {
            match self.tokens.cursors(&self.processor).await {
                Ok(cursors) if !cursors.is_empty() && cursors.iter().all(|cursor| cursor.covers(position)) => return,
                Ok(_) => {}
                Err(error) => tracing::warn!(processor = %self.processor, %error, "cannot read progress"),
            }
            tokio::time::sleep(self.poll).await;
        }
    }
}
