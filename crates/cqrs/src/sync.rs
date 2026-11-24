//! Locks that outlive a panic. A mutex is poisoned when a thread panics
//! while holding it; a library should not turn that one panic into a panic
//! of every other thread using the same store, processor or emitter. What
//! these locks guard (maps of tokens, routes, subscribers, in-memory logs)
//! stays usable, so the guard is taken back.

use std::sync::{Mutex, MutexGuard, PoisonError};

/// A [`Mutex`] that ignores poisoning.
pub(crate) trait Locked<T> {
    /// The guard, even if a thread panicked while holding it.
    fn locked(&self) -> MutexGuard<'_, T>;

    /// The value, through `&mut`, even if the mutex is poisoned.
    #[cfg_attr(not(feature = "tokio"), allow(dead_code))]
    fn locked_mut(&mut self) -> &mut T;
}

impl<T> Locked<T> for Mutex<T> {
    fn locked(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn locked_mut(&mut self) -> &mut T {
        self.get_mut().unwrap_or_else(PoisonError::into_inner)
    }
}
