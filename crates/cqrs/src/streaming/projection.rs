//! Running a projection: one tracking processor per namespace, each with its
//! own tokens (`"{projection}.{namespace}"`) and units of work.

use std::sync::Arc;

use super::{ProcessorHandle, ProcessorProgress, TrackingOptions, TrackingProcessor};
use crate::{DeadLetterQueue, DomainEvent, ReadLog, Route, TokenStore, TransactionManager, projection::NamespaceOf};

/// The processors of a projection's namespaces, to configure and
/// [`spawn`](Self::spawn): [`Events::projection`](super::Events::projection).
#[must_use = "a projection does nothing until it is spawned"]
pub struct ProjectionProcessors<E, S, P, C, M, D = ()> {
    /// One per namespace, in the projection's order.
    processors: Vec<TrackingProcessor<E, S, NamespaceOf<P>, C, M, D>>,
}

impl<E, S, P, C, M, D> ::std::fmt::Debug for ProjectionProcessors<E, S, P, C, M, D> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("ProjectionProcessors").finish_non_exhaustive()
    }
}

impl<E, S, P, C, M> ProjectionProcessors<E, S, P, C, M>
where
    E: DomainEvent,
    S: ReadLog<E> + Clone + 'static,
    P: Route<M::Tx, Event = E> + 'static,
    C: TokenStore<Tx = M::Tx> + Clone + 'static,
    M: TransactionManager + Clone + 'static,
{
    /// The processors of `projection`'s namespaces over `processor` (what
    /// builds one, given its name and handler).
    pub(crate) fn new(
        projection: P,
        mut processor: impl FnMut(String, NamespaceOf<P>) -> TrackingProcessor<E, S, NamespaceOf<P>, C, M>,
    ) -> Self {
        let projection = Arc::new(projection);
        let processors = (0..P::NAMESPACES.len())
            .map(|namespace| {
                let handler = NamespaceOf {
                    projection: Arc::clone(&projection),
                    namespace,
                };
                processor(P::processor_name(namespace), handler)
            })
            .collect();
        Self::with_batches(processors)
    }

    /// Parks a namespace's events in `queue` after `after` failed attempts
    /// (with the later events of their aggregate, in that namespace),
    /// instead of retrying them forever: a side effect whose target keeps
    /// refusing one event no longer holds back the rest. Each namespace
    /// parks under its own processor's name (`"post.notifications"`), so
    /// one queue serves them all; it joins the namespaces' units of work.
    pub fn with_dead_letters<D>(self, queue: D, after: u32) -> ProjectionProcessors<E, S, P, C, M, D>
    where
        D: DeadLetterQueue<E, M::Tx> + Clone + 'static,
    {
        ProjectionProcessors {
            processors: self
                .processors
                .into_iter()
                .map(|processor| processor.with_dead_letters(queue.clone(), after))
                .collect(),
        }
    }
}

impl<E, S, P, C, M, D> ProjectionProcessors<E, S, P, C, M, D>
where
    E: DomainEvent,
    S: ReadLog<E> + Clone + 'static,
    P: Route<M::Tx, Event = E> + 'static,
    C: TokenStore<Tx = M::Tx> + Clone + 'static,
    M: TransactionManager + Clone + 'static,
    D: DeadLetterQueue<E, M::Tx> + 'static,
{
    /// `processors` with the batch each namespace asks for, if it does.
    fn with_batches(processors: Vec<TrackingProcessor<E, S, NamespaceOf<P>, C, M, D>>) -> Self {
        let processors = processors
            .into_iter()
            .enumerate()
            .map(|(namespace, processor)| match P::batch_size(namespace) {
                Some(batch_size) => {
                    let options = TrackingOptions {
                        batch_size,
                        ..processor.options()
                    };
                    processor.with_options(options)
                }
                None => processor,
            })
            .collect();
        Self { processors }
    }

    /// Settings for every namespace's processor; a namespace that sets its
    /// batch (`batch = N` on its handler) keeps it.
    pub fn with_options(self, options: TrackingOptions) -> Self {
        Self::with_batches(
            self.processors
                .into_iter()
                .map(|processor| processor.with_options(options))
                .collect(),
        )
    }

    /// Replays the projection from the start: resets every namespace's
    /// processor ([`TrackingProcessor::reset`]). The namespaces that see
    /// replays (read models) rebuild from the whole log once spawned; the
    /// others skip what they already handled. Every instance must be stopped.
    ///
    /// # Errors
    ///
    /// The first namespace that could not reset (the ones before it are
    /// reset): call it again.
    pub async fn reset(&self) -> Result<(), super::ResetError> {
        for processor in &self.processors {
            processor.reset().await?;
        }
        Ok(())
    }

    /// Retries the events each namespace parked
    /// ([`with_dead_letters`](ProjectionProcessors::with_dead_letters)),
    /// namespace by namespace: see
    /// [`TrackingProcessor::retry_dead_letters`]. Run it once the cause is
    /// fixed, from processors built like the running ones, not spawned.
    ///
    /// # Errors
    ///
    /// The first namespace whose queue or unit of work failed (the ones
    /// before it are retried): call it again.
    pub async fn retry_dead_letters(&self) -> Result<Vec<(&'static str, crate::RetryOutcome)>, crate::StoreError> {
        let mut outcomes = Vec::new();
        for (namespace, processor) in self.processors.iter().enumerate() {
            outcomes.push((P::NAMESPACES[namespace], processor.retry_dead_letters().await?));
        }
        Ok(outcomes)
    }

    /// Starts every namespace's processor on Tokio tasks.
    ///
    /// # Panics
    ///
    /// Outside a Tokio runtime.
    pub fn spawn(self) -> ProjectionHandle {
        let namespaces = self
            .processors
            .into_iter()
            .enumerate()
            .map(|(namespace, processor)| Namespace {
                name: P::NAMESPACES[namespace],
                replays: P::replays(namespace),
                handle: processor.spawn(),
            })
            .collect();
        ProjectionHandle { namespaces }
    }
}

/// One running namespace of a projection.
struct Namespace {
    /// The namespace.
    name: &'static str,
    /// Whether it sees replays (a read model) or not (a side effect).
    replays: bool,
    /// Its processor.
    handle: ProcessorHandle,
}

/// Controls a running projection: its namespaces' processors.
#[must_use = "dropping the handle leaves the projection running, with no way to stop it"]
pub struct ProjectionHandle {
    /// In the projection's order.
    namespaces: Vec<Namespace>,
}

impl ::std::fmt::Debug for ProjectionHandle {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("ProjectionHandle").finish_non_exhaustive()
    }
}

impl ProjectionHandle {
    /// The processor of `namespace`, if the projection has it.
    pub fn namespace(&self, namespace: &str) -> Option<&ProcessorHandle> {
        self.namespaces
            .iter()
            .find(|running| running.name == namespace)
            .map(|running| &running.handle)
    }

    /// The progress of every namespace.
    pub fn progress(&self) -> Vec<ProcessorProgress> {
        self.namespaces
            .iter()
            .map(|running| ProcessorProgress::of(&running.handle))
            .collect()
    }

    /// The progress of the namespaces that see replays: the read models,
    /// which a reader waits for (a side effect, a notification, is not).
    pub fn read_models(&self) -> Vec<ProcessorProgress> {
        self.namespaces
            .iter()
            .filter(|running| running.replays)
            .map(|running| ProcessorProgress::of(&running.handle))
            .collect()
    }

    /// Waits until every namespace handled the event at `position` (see
    /// [`ProcessorHandle::caught_up`]).
    pub async fn caught_up(&self, position: u64) {
        for running in &self.namespaces {
            running.handle.caught_up(position).await;
        }
    }

    /// Stops every namespace's processor (each after its current event).
    pub async fn stop(self) {
        for running in self.namespaces {
            running.handle.stop().await;
        }
    }

    /// The processors, to keep with others.
    pub(crate) fn into_handles(self) -> Vec<ProcessorHandle> {
        self.namespaces.into_iter().map(|running| running.handle).collect()
    }
}
