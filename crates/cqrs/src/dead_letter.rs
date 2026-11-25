//! Events that kept failing: the [`DeadLetterQueue`] port. Parking an event also parks the later events of its
//! sequence (its aggregate), so they are never handled out of order, while
//! the other sequences keep flowing.

use crate::sync::Locked;
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use crate::{
    DomainEvent, EventEnvelope, EventHandler, HandlerError, Metadata, ProcessingContext, StoreError, TransactionManager,
};

/// An event a processor gave up on, for now; or, while its
/// [`EnqueuePolicy`] decides, one that is about to be (not
/// [`parked_at`](Self::parked_at) yet).
#[derive(Debug)]
#[non_exhaustive]
pub struct DeadLetter<E> {
    /// Its sequence: the aggregate stream it belongs to.
    pub sequence: String,
    /// The event.
    pub event: Arc<EventEnvelope<E>>,
    /// Why it was parked: the handler's last error, or that an earlier event
    /// of the sequence is parked.
    pub cause: String,
    /// Failed attempts: before it was parked, then at each retry.
    pub attempts: u32,
    /// When it was parked; `None` while the policy decides whether to.
    pub parked_at: Option<SystemTime>,
    /// When a retry last failed on it (the policy requeued it).
    pub last_touched: Option<SystemTime>,
    /// What the policy kept with it (an error code, the service that
    /// refused, a retry count of its own...).
    pub diagnostics: Metadata,
}

impl<E> DeadLetter<E> {
    /// `event` of `sequence`, failed with `cause`: not parked yet, no
    /// attempt counted, no diagnostics.
    pub fn new(sequence: impl Into<String>, event: Arc<EventEnvelope<E>>, cause: impl Into<String>) -> Self {
        Self {
            sequence: sequence.into(),
            event,
            cause: cause.into(),
            attempts: 0,
            parked_at: None,
            last_touched: None,
            diagnostics: Metadata::new(),
        }
    }

    /// Whether it is in the queue (a retry is deciding), rather than an
    /// event failing for the first times.
    pub fn is_parked(&self) -> bool {
        self.parked_at.is_some()
    }
}

impl<E> Clone for DeadLetter<E> {
    fn clone(&self) -> Self {
        Self {
            sequence: self.sequence.clone(),
            event: Arc::clone(&self.event),
            cause: self.cause.clone(),
            attempts: self.attempts,
            parked_at: self.parked_at,
            last_touched: self.last_touched,
            diagnostics: self.diagnostics.clone(),
        }
    }
}

/// What to do with an event its handler failed on (Axon 5's
/// `EnqueueDecision`). Its meaning depends on whether the letter is parked
/// already ([`DeadLetter::is_parked`]):
///
/// | Decision | A failing event | A parked one, failing again at a retry |
/// |---|---|---|
/// | `Retry` | tried again after a backoff, holding its segment | kept, as `Requeue` without diagnostics |
/// | `Enqueue` | parked, with its aggregate's later events behind it | kept, as `Requeue` |
/// | `Requeue` | parked, as `Enqueue` | kept, with the new diagnostics and `last_touched` |
/// | `Evict` | skipped, as `Ignore` | removed: its sequence's next letter comes first |
/// | `Ignore` | skipped: the segment moves past it, unhandled and not parked | removed, as `Evict` |
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EnqueueDecision {
    /// Not yet: try it again.
    Retry,
    /// Park it, keeping `diagnostics` with it.
    Enqueue(Metadata),
    /// Keep it parked, with `diagnostics` (it replaces the letter's).
    Requeue(Metadata),
    /// Drop the parked letter.
    Evict,
    /// Give the failing event up: neither handled nor parked.
    Ignore,
}

/// Decides what happens to an event its handler failed on (Axon 5's
/// `EnqueuePolicy`): asked after every failed attempt of an event that is
/// not parked, and at each failed retry of a parked one. Only processors
/// with a dead-letter queue ask it.
///
/// A closure is one: `|letter: &DeadLetter<E>, cause: &HandlerError| ..`.
/// The default is [`ParkAfter`].
pub trait EnqueuePolicy<E>: Send + Sync {
    /// The decision for `letter`, which `cause` just failed.
    fn decide(&self, letter: &DeadLetter<E>, cause: &HandlerError) -> EnqueueDecision;
}

impl<E, F> EnqueuePolicy<E> for F
where
    F: Fn(&DeadLetter<E>, &HandlerError) -> EnqueueDecision + Send + Sync,
{
    fn decide(&self, letter: &DeadLetter<E>, cause: &HandlerError) -> EnqueueDecision {
        self(letter, cause)
    }
}

/// The default policy: retries an event `0`-1 times, parks it at the
/// `0`th failure, and keeps a parked letter until a retry handles it
/// (`with_dead_letters(queue, after)` is this policy).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParkAfter(pub u32);

impl<E> EnqueuePolicy<E> for ParkAfter {
    fn decide(&self, letter: &DeadLetter<E>, _: &HandlerError) -> EnqueueDecision {
        match letter.is_parked() {
            true => EnqueueDecision::Requeue(letter.diagnostics.clone()),
            false if letter.attempts >= self.0.max(1) => EnqueueDecision::Enqueue(Metadata::new()),
            false => EnqueueDecision::Retry,
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
/// use cqrs::processing::{DeadLetterQueue, InMemoryDeadLetters};
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

    /// Keeps `letter` parked, within `tx`, with its `attempts`,
    /// `last_touched` and `diagnostics` as given (a retry failed and the
    /// policy requeued it). `false`: it is no longer parked.
    fn requeue_in(
        &self,
        tx: &mut Tx,
        processor: &str,
        letter: &DeadLetter<E>,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

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

    async fn requeue_in(&self, _: &mut Tx, _: &str, _: &DeadLetter<E>) -> Result<bool, StoreError> {
        Ok(false)
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

impl<E> ::std::fmt::Debug for InMemoryDeadLetters<E> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("InMemoryDeadLetters").finish_non_exhaustive()
    }
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

    async fn requeue_in(&self, (): &mut (), processor: &str, letter: &DeadLetter<E>) -> Result<bool, StoreError> {
        let mut letters = self.letters.locked();
        let parked = letters
            .get_mut(&(processor.to_owned(), letter.sequence.clone()))
            .and_then(|queue| {
                queue
                    .iter_mut()
                    .find(|parked| parked.event.position == letter.event.position)
            });
        let Some(parked) = parked else {
            return Ok(false);
        };
        parked.attempts = letter.attempts;
        parked.cause.clone_from(&letter.cause);
        parked.last_touched = letter.last_touched;
        parked.diagnostics = letter.diagnostics.clone();
        Ok(true)
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
    /// Parked events the policy evicted after failing again.
    pub evicted: usize,
}

/// Retries `processor`'s parked events with `handler`, each sequence in
/// order. When a retry fails, `policy` decides: requeue the letter (it
/// stays first, and the sequence waits for the next retry) or evict it (the
/// sequence's next letter is tried). Run it once the cause is fixed, with
/// the same handler and transaction manager the processor uses (or
/// [`TrackingProcessor::retry_dead_letters`]).
///
/// Each event runs in its own unit of work from `transactions`, as in the
/// processor: `handle`, the letter's removal, commit, then `committed`. A
/// failure rolls its changes back; the requeue or eviction commits in a
/// unit of its own. The removal commits with what the handler wrote: in a
/// database transaction, a crash (or another retry running at the same
/// time) never applies a parked event twice, nor removes one that was not
/// handled.
///
/// # Errors
///
/// If the queue itself fails, or a unit of work cannot begin or commit.
///
/// [`TrackingProcessor::retry_dead_letters`]: crate::TrackingProcessor::retry_dead_letters
pub async fn retry_dead_letters<E, Q, M, H, P>(
    queue: &Q,
    processor: &str,
    transactions: &M,
    handler: &H,
    policy: &P,
) -> Result<RetryOutcome, StoreError>
where
    E: DomainEvent,
    Q: DeadLetterQueue<E, M::Tx>,
    M: TransactionManager,
    H: EventHandler<E, M::Tx>,
    P: EnqueuePolicy<E> + ?Sized,
{
    let mut outcome = RetryOutcome::default();
    for sequence in queue.sequences(processor).await? {
        while let Some(mut letter) = queue.first(processor, &sequence).await? {
            let mut tx = transactions.begin().await?;
            let handled = handler
                .handle(
                    &letter.event,
                    &mut ProcessingContext::in_processor(&mut tx, false, processor),
                )
                .await;
            if let Err(cause) = handled {
                drop(tx); // rolls back what the handler wrote
                letter.attempts += 1;
                let mut tx = transactions.begin().await?;
                match policy.decide(&letter, &cause) {
                    EnqueueDecision::Evict | EnqueueDecision::Ignore => {
                        if queue.remove_in(&mut tx, processor, &letter).await? {
                            transactions.commit(tx).await?;
                            outcome.evicted += 1;
                        }
                        continue; // the sequence's next letter, if any
                    }
                    EnqueueDecision::Requeue(diagnostics) | EnqueueDecision::Enqueue(diagnostics) => {
                        letter.diagnostics = diagnostics;
                    }
                    EnqueueDecision::Retry => {}
                }
                letter.last_touched = Some(SystemTime::now());
                letter.cause = cause.to_string();
                if queue.requeue_in(&mut tx, processor, &letter).await? {
                    transactions.commit(tx).await?;
                }
                outcome.still_parked += 1;
                break; // the sequence waits for the next retry, in order
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
