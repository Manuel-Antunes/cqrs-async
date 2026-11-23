//! Events that kept failing: the [`DeadLetterQueue`] port (Axon's sequenced
//! dead-letter queue). Parking an event also parks the later events of its
//! sequence (its aggregate), so they are never handled out of order, while
//! the other sequences keep flowing.

use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
};

use crate::{DomainEvent, EventEnvelope, EventHandler, ProcessingContext, StoreError, TransactionalTokenStore};

/// An event a processor gave up on, for now.
#[derive(Debug)]
pub struct DeadLetter<E> {
    /// Its sequence: the aggregate stream it belongs to.
    pub sequence: String,
    /// The event.
    pub event: Arc<EventEnvelope<E>>,
    /// Why it was parked: the handler's last error, or that an earlier event
    /// of the sequence is parked.
    pub cause: String,
}

impl<E> Clone for DeadLetter<E> {
    fn clone(&self) -> Self {
        Self {
            sequence: self.sequence.clone(),
            event: Arc::clone(&self.event),
            cause: self.cause.clone(),
        }
    }
}

/// Parked events per processor and sequence, oldest first.
///
/// Implemented for `()`: no queue, nothing is ever parked.
pub trait DeadLetterQueue<E>: Send + Sync {
    /// Whether `sequence` has parked events (later events must queue behind).
    fn contains(&self, processor: &str, sequence: &str) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Parks `letter` at the end of its sequence.
    fn enqueue(&self, processor: &str, letter: DeadLetter<E>) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// The sequences with parked events.
    fn sequences(&self, processor: &str) -> impl Future<Output = Result<Vec<String>, StoreError>> + Send;

    /// The oldest parked event of `sequence`.
    fn first(
        &self,
        processor: &str,
        sequence: &str,
    ) -> impl Future<Output = Result<Option<DeadLetter<E>>, StoreError>> + Send;

    /// Removes the oldest parked event of `sequence` (after it was handled).
    fn remove_first(&self, processor: &str, sequence: &str) -> impl Future<Output = Result<(), StoreError>> + Send;
}

impl<E: Send + Sync> DeadLetterQueue<E> for () {
    async fn contains(&self, _: &str, _: &str) -> Result<bool, StoreError> {
        Ok(false)
    }

    async fn enqueue(&self, processor: &str, _: DeadLetter<E>) -> Result<(), StoreError> {
        Err(StoreError::Backend(format!("{processor} has no dead-letter queue")))
    }

    async fn sequences(&self, _: &str) -> Result<Vec<String>, StoreError> {
        Ok(Vec::new())
    }

    async fn first(&self, _: &str, _: &str) -> Result<Option<DeadLetter<E>>, StoreError> {
        Ok(None)
    }

    async fn remove_first(&self, _: &str, _: &str) -> Result<(), StoreError> {
        Ok(())
    }
}

/// Parked events by (processor, sequence), oldest first.
type Queues<E> = BTreeMap<(String, String), Vec<DeadLetter<E>>>;

/// Dead letters kept in memory.
pub struct InMemoryDeadLetters<E> {
    /// Per processor, per sequence, oldest first.
    letters: Mutex<Queues<E>>,
}

impl<E> Default for InMemoryDeadLetters<E> {
    fn default() -> Self {
        Self {
            letters: Mutex::new(BTreeMap::new()),
        }
    }
}

impl<E> InMemoryDeadLetters<E> {
    /// Every parked event of `processor`, by sequence then age.
    pub fn letters(&self, processor: &str) -> Vec<DeadLetter<E>> {
        let letters = self.letters.lock().expect("dead letters poisoned");
        letters
            .iter()
            .filter(|((owner, _), _)| owner == processor)
            .flat_map(|(_, queue)| queue.iter().cloned())
            .collect()
    }
}

impl<E: Send + Sync> DeadLetterQueue<E> for InMemoryDeadLetters<E> {
    async fn contains(&self, processor: &str, sequence: &str) -> Result<bool, StoreError> {
        let letters = self.letters.lock().expect("dead letters poisoned");
        Ok(letters.contains_key(&(processor.to_owned(), sequence.to_owned())))
    }

    async fn enqueue(&self, processor: &str, letter: DeadLetter<E>) -> Result<(), StoreError> {
        let mut letters = self.letters.lock().expect("dead letters poisoned");
        letters
            .entry((processor.to_owned(), letter.sequence.clone()))
            .or_default()
            .push(letter);
        Ok(())
    }

    async fn sequences(&self, processor: &str) -> Result<Vec<String>, StoreError> {
        let letters = self.letters.lock().expect("dead letters poisoned");
        Ok(letters
            .keys()
            .filter(|(owner, _)| owner == processor)
            .map(|(_, sequence)| sequence.clone())
            .collect())
    }

    async fn first(&self, processor: &str, sequence: &str) -> Result<Option<DeadLetter<E>>, StoreError> {
        let letters = self.letters.lock().expect("dead letters poisoned");
        Ok(letters
            .get(&(processor.to_owned(), sequence.to_owned()))
            .and_then(|queue| queue.first().cloned()))
    }

    async fn remove_first(&self, processor: &str, sequence: &str) -> Result<(), StoreError> {
        let mut letters = self.letters.lock().expect("dead letters poisoned");
        let key = (processor.to_owned(), sequence.to_owned());
        if let Some(queue) = letters.get_mut(&key) {
            if !queue.is_empty() {
                queue.remove(0);
            }
            if queue.is_empty() {
                letters.remove(&key);
            }
        }
        Ok(())
    }
}

/// A shared dead-letter queue is a dead-letter queue.
// Forwards every method, so an override of a provided one is never lost.
#[deny(clippy::missing_trait_methods)]
impl<E, Q: DeadLetterQueue<E>> DeadLetterQueue<E> for Arc<Q> {
    fn contains(&self, processor: &str, sequence: &str) -> impl Future<Output = Result<bool, StoreError>> + Send {
        (**self).contains(processor, sequence)
    }

    fn enqueue(&self, processor: &str, letter: DeadLetter<E>) -> impl Future<Output = Result<(), StoreError>> + Send {
        (**self).enqueue(processor, letter)
    }

    fn sequences(&self, processor: &str) -> impl Future<Output = Result<Vec<String>, StoreError>> + Send {
        (**self).sequences(processor)
    }

    fn first(
        &self,
        processor: &str,
        sequence: &str,
    ) -> impl Future<Output = Result<Option<DeadLetter<E>>, StoreError>> + Send {
        (**self).first(processor, sequence)
    }

    fn remove_first(&self, processor: &str, sequence: &str) -> impl Future<Output = Result<(), StoreError>> + Send {
        (**self).remove_first(processor, sequence)
    }
}

/// What [`retry_dead_letters`] did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetryOutcome {
    /// Parked events handled successfully (and removed).
    pub handled: usize,
    /// Sequences still parked, because their oldest event failed again.
    pub still_parked: usize,
}

/// Retries `processor`'s parked events with `handler`, each sequence in
/// order, stopping a sequence at its first failure (so it stays ordered).
/// Run it once the cause is fixed, with the same handler and token store
/// the processor uses (or [`TrackingProcessor::retry_dead_letters`]).
///
/// Each event runs in its own unit of work from `units`, as in the
/// processor: `handle`, commit, then `committed`. A failure rolls its
/// changes back. The letter is removed after the commit, so a crash in
/// between handles it again: at least once, like any processor.
///
/// # Errors
///
/// If the queue itself fails, or a unit of work cannot begin or commit.
///
/// [`TrackingProcessor::retry_dead_letters`]: crate::TrackingProcessor::retry_dead_letters
pub async fn retry_dead_letters<E, Q, T, H>(
    queue: &Q,
    processor: &str,
    units: &T,
    handler: &H,
) -> Result<RetryOutcome, StoreError>
where
    E: DomainEvent,
    Q: DeadLetterQueue<E>,
    T: TransactionalTokenStore,
    H: EventHandler<E, T::Transaction>,
{
    let mut outcome = RetryOutcome::default();
    for sequence in queue.sequences(processor).await? {
        while let Some(letter) = queue.first(processor, &sequence).await? {
            let mut tx = units.begin().await?;
            if handler
                .handle(&letter.event, &mut ProcessingContext::new(&mut tx, false))
                .await
                .is_err()
            {
                outcome.still_parked += 1;
                break; // dropping `tx` rolls back
            }
            units.commit(tx).await?;
            handler.committed(&letter.event).await;
            queue.remove_first(processor, &sequence).await?;
            outcome.handled += 1;
        }
    }
    Ok(outcome)
}
