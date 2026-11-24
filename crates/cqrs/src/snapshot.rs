//! Snapshots: the [`SnapshotStore`] port. An optimization, never the source
//! of truth: the event stream always is.

use std::{
    collections::HashMap,
    future::{Future, ready},
    sync::{Arc, Mutex},
};

use crate::{Aggregate, StoreError, StreamId};

/// Saved aggregate states, so loading replays only the events after them.
///
/// Implemented for `()`: no snapshots, every load replays the whole stream.
pub trait SnapshotStore<A: Aggregate>: Send + Sync {
    /// The most recent snapshot of `id` and the stream version it was taken
    /// at, if any.
    fn load(&self, id: &A::Id) -> impl Future<Output = Result<Option<(A, u64)>, StoreError>> + Send;

    /// Saves `aggregate` as it is at stream `version`. Keeping only the
    /// newest snapshot per aggregate is enough.
    fn save(&self, aggregate: &A, version: u64) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// No snapshots.
impl<A: Aggregate> SnapshotStore<A> for () {
    fn load(&self, _: &A::Id) -> impl Future<Output = Result<Option<(A, u64)>, StoreError>> + Send {
        ready(Ok(None))
    }

    fn save(&self, _: &A, _: u64) -> impl Future<Output = Result<(), StoreError>> + Send {
        ready(Ok(()))
    }
}

/// Snapshots kept in memory, as clones of the aggregate. A handle: clones
/// share the same snapshots.
pub struct InMemorySnapshots<A> {
    /// The newest snapshot per stream, with its version.
    snapshots: Arc<Mutex<HashMap<StreamId, (A, u64)>>>,
}

impl<A> Default for InMemorySnapshots<A> {
    fn default() -> Self {
        Self {
            snapshots: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<A> Clone for InMemorySnapshots<A> {
    fn clone(&self) -> Self {
        Self {
            snapshots: Arc::clone(&self.snapshots),
        }
    }
}

impl<A: Aggregate + Clone> SnapshotStore<A> for InMemorySnapshots<A> {
    fn load(&self, id: &A::Id) -> impl Future<Output = Result<Option<(A, u64)>, StoreError>> + Send {
        let snapshots = self.snapshots.lock().expect("snapshots poisoned");
        ready(Ok(snapshots.get(&StreamId::of::<A>(id)).cloned()))
    }

    fn save(&self, aggregate: &A, version: u64) -> impl Future<Output = Result<(), StoreError>> + Send {
        let mut snapshots = self.snapshots.lock().expect("snapshots poisoned");
        let entry = snapshots.entry(StreamId::of::<A>(aggregate.id()));
        // Concurrent writers may save out of order: keep the newest.
        entry
            .and_modify(|(stored, at)| {
                if version > *at {
                    (*stored, *at) = (aggregate.clone(), version);
                }
            })
            .or_insert_with(|| (aggregate.clone(), version));
        ready(Ok(()))
    }
}
