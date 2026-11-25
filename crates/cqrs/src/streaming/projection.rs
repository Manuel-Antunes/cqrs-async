//! Running a projection: one tracking processor per namespace, each with its
//! own tokens (`"{projection}.{namespace}"`) and units of work.

use std::sync::Arc;

use super::{ProcessorHandle, ProcessorProgress, TrackingOptions, TrackingProcessor};
use crate::{DeadLetterQueue, DomainEvent, ReadLog, Route, TokenStore, TransactionManager, projection::NamespaceOf};

/// The processors of a projection's namespaces, to configure and
/// [`spawn`](Self::spawn): [`Events::projection`](super::Events::projection).
#[must_use = "a projection does nothing until it is spawned"]
pub struct ProjectionProcessors<E, S, P, C, M> {
    /// One per namespace, in the projection's order.
    processors: Vec<TrackingProcessor<E, S, NamespaceOf<P>, C, M>>,
}

impl<E, S, P, C, M> ::std::fmt::Debug for ProjectionProcessors<E, S, P, C, M> {
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
    (): DeadLetterQueue<E, M::Tx>,
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

    /// `processors` with the batch each namespace asks for, if it does.
    fn with_batches(processors: Vec<TrackingProcessor<E, S, NamespaceOf<P>, C, M>>) -> Self {
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
