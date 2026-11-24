//! Events that kept failing: the [`DeadLetterQueue`] port (Axon's sequenced
//! dead-letter queue). Parking an event also parks the later events of its
//! sequence (its aggregate), so they are never handled out of order, while
//! the other sequences keep flowing.

use crate::sync::Locked;
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
};

use crate::{DomainEvent, EventEnvelope, EventHandler, ProcessingContext, StoreError, TransactionManager};

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
/// `Tx` is the processor's unit of work (its
/// [`TransactionManager::Tx`](crate::TransactionManager::Tx)). A letter
/// enters and leaves the queue inside one: parked in the unit of work that
/// moves the token past its event, removed in the one that applies it. So a
/// crash can neither lose a letter nor apply its event twice, and a queue
/// only fits the units of work it can join: `InMemoryDeadLetters` goes with
/// tokens in memory (`Tx = ()`), not with a database's, where its letters
/// would be lost on restart with the tokens already past them.
///
/// ```compile_fail,E0277
/// use cqrs::{DeadLetterQueue, InMemoryDeadLetters};
/// /// A database's unit of work.
/// struct SqlTransaction;
/// fn parks_in<Q: DeadLetterQueue<u8, SqlTransaction>>(_: Q) {}
/// parks_in(InMemoryDeadLetters::<u8>::default());
/// ```
///
/// Implemented for `()`: no queue, nothing is ever parked.
pub trait DeadLetterQueue<E, Tx = ()>: Send + Sync {
    /// Whether `sequence` has parked events (later events must queue behind).
    fn contains(&self, processor: &str, sequence: &str) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// The sequences with parked events.
    fn sequences(&self, processor: &str) -> impl Future<Output = Result<Vec<String>, StoreError>> + Send;

    /// The oldest parked event of `sequence`.
    fn first(
        &self,
        processor: &str,
        sequence: &str,
    ) -> impl Future<Output = Result<Option<DeadLetter<E>>, StoreError>> + Send;

    /// Parks `letter` at the end of its sequence, within `tx`. Parking the
    /// same event again (after a crash that lost the token move, say) leaves
    /// one letter.
    fn enqueue_in(
        &self,
        tx: &mut Tx,
        processor: &str,
        letter: DeadLetter<E>,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Removes `letter` within `tx`, once its event was handled there.
    /// `false`: it is no longer parked (another retry handled it), and the
    /// caller must roll `tx` back so the event is not applied twice.
    fn remove_in(
        &self,
        tx: &mut Tx,
        processor: &str,
        letter: &DeadLetter<E>,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;
}

impl<E: Send + Sync, Tx: Send> DeadLetterQueue<E, Tx> for () {
    async fn contains(&self, _: &str, _: &str) -> Result<bool, StoreError> {
        Ok(false)
    }

    async fn sequences(&self, _: &str) -> Result<Vec<String>, StoreError> {
        Ok(Vec::new())
    }

    async fn first(&self, _: &str, _: &str) -> Result<Option<DeadLetter<E>>, StoreError> {
        Ok(None)
    }

    async fn enqueue_in(&self, _: &mut Tx, processor: &str, _: DeadLetter<E>) -> Result<(), StoreError> {
        Err(StoreError::backend(format!("{processor} has no dead-letter queue")))
    }

    async fn remove_in(&self, _: &mut Tx, _: &str, _: &DeadLetter<E>) -> Result<bool, StoreError> {
        Ok(false)
    }
}

/// Parked events by (processor, sequence), oldest first.
type Queues<E> = BTreeMap<(String, String), Vec<DeadLetter<E>>>;

/// Dead letters kept in memory. A handle: clones share the same letters.
pub struct InMemoryDeadLetters<E> {
    /// Per processor, per sequence, oldest first.
    letters: Arc<Mutex<Queues<E>>>,
}

impl<E> Default for InMemoryDeadLetters<E> {
    fn default() -> Self {
        Self {
            letters: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl<E> Clone for InMemoryDeadLetters<E> {
    fn clone(&self) -> Self {
        Self {
            letters: Arc::clone(&self.letters),
        }
    }
}

impl<E> InMemoryDeadLetters<E> {
    /// Every parked event of `processor`, by sequence then age.
    pub fn letters(&self, processor: &str) -> Vec<DeadLetter<E>> {
        let letters = self.letters.locked();
        letters
            .iter()
            .filter(|((owner, _), _)| owner == processor)
            .flat_map(|(_, queue)| queue.iter().cloned())
            .collect()
    }

    /// Parks `letter` at the end of its sequence, unless that event is
    /// parked already.
    pub fn enqueue(&self, processor: &str, letter: DeadLetter<E>) {
        let mut letters = self.letters.locked();
        let queue = letters
            .entry((processor.to_owned(), letter.sequence.clone()))
            .or_default();
        if !queue
            .iter()
            .any(|parked| parked.event.position == letter.event.position)
        {
            queue.push(letter);
        }
    }
}

/// In memory there is no unit of work to join (`Tx = ()`, the one of
/// [`InMemoryTokenStore`](crate::InMemoryTokenStore)): parking and removing
/// are immediate, and lost with the tokens when the process ends.
impl<E: Send + Sync> DeadLetterQueue<E> for InMemoryDeadLetters<E> {
    async fn contains(&self, processor: &str, sequence: &str) -> Result<bool, StoreError> {
        let letters = self.letters.locked();
        Ok(letters.contains_key(&(processor.to_owned(), sequence.to_owned())))
    }

    async fn sequences(&self, processor: &str) -> Result<Vec<String>, StoreError> {
        let letters = self.letters.locked();
        Ok(letters
            .keys()
            .filter(|(owner, _)| owner == processor)
            .map(|(_, sequence)| sequence.clone())
            .collect())
    }

    async fn first(&self, processor: &str, sequence: &str) -> Result<Option<DeadLetter<E>>, StoreError> {
        let letters = self.letters.locked();
        Ok(letters
            .get(&(processor.to_owned(), sequence.to_owned()))
            .and_then(|queue| queue.first().cloned()))
    }

    async fn enqueue_in(&self, (): &mut (), processor: &str, letter: DeadLetter<E>) -> Result<(), StoreError> {
        self.enqueue(processor, letter);
        Ok(())
    }

    async fn remove_in(&self, (): &mut (), processor: &str, letter: &DeadLetter<E>) -> Result<bool, StoreError> {
        let mut letters = self.letters.locked();
        let key = (processor.to_owned(), letter.sequence.clone());
        let Some(queue) = letters.get_mut(&key) else {
            return Ok(false);
        };
        let parked = queue.len();
        queue.retain(|other| other.event.position != letter.event.position);
        let removed = queue.len() < parked;
        if queue.is_empty() {
            letters.remove(&key);
        }
        Ok(removed)
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
/// Run it once the cause is fixed, with the same handler and transaction
/// manager the processor uses (or [`TrackingProcessor::retry_dead_letters`]).
///
/// Each event runs in its own unit of work from `transactions`, as in the
/// processor: `handle`, the letter's removal, commit, then `committed`. A
/// failure rolls its changes back and the letter stays. The removal commits
/// with what the handler wrote: in a database transaction, a crash (or
/// another retry running at the same time) never applies a parked event
/// twice, nor removes one that was not handled.
///
/// # Errors
///
/// If the queue itself fails, or a unit of work cannot begin or commit.
///
/// [`TrackingProcessor::retry_dead_letters`]: crate::TrackingProcessor::retry_dead_letters
pub async fn retry_dead_letters<E, Q, M, H>(
    queue: &Q,
    processor: &str,
    transactions: &M,
    handler: &H,
) -> Result<RetryOutcome, StoreError>
where
    E: DomainEvent,
    Q: DeadLetterQueue<E, M::Tx>,
    M: TransactionManager,
    H: EventHandler<E, M::Tx>,
{
    let mut outcome = RetryOutcome::default();
    for sequence in queue.sequences(processor).await? {
        while let Some(letter) = queue.first(processor, &sequence).await? {
            let mut tx = transactions.begin().await?;
            if handler
                .handle(
                    &letter.event,
                    &mut ProcessingContext::in_processor(&mut tx, false, processor),
                )
                .await
                .is_err()
            {
                outcome.still_parked += 1;
                break; // dropping `tx` rolls back
            }
            if !queue.remove_in(&mut tx, processor, &letter).await? {
                // Another retry handled it meanwhile: dropping `tx` undoes
                // what this one did, and the next letter is looked up again.
                continue;
            }
            transactions.commit(tx).await?;
            handler.committed(&letter.event).await;
            outcome.handled += 1;
        }
    }
    Ok(outcome)
}
