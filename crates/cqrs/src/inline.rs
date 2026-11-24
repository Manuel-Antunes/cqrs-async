//! Inline processing: [`InlinePublisher`] appends what command handlers
//! decide and runs the handlers of its [`EventProcessor`] group in the same
//! call, right after the commit, with no runtime (tests and simple
//! applications; [`Events`](crate::Events) is the asynchronous publisher).

use std::{any::type_name, fmt, future::Future, marker::PhantomData};

use crate::{
    Aggregate, AppendEvents, DomainEvent, EventEnvelope, EventHandler, EventPublisher, HandlerError, Loaded, Pending,
    StoreError,
};

/// Runs committed events through a processing group, in-process and right
/// after the commit, in the same call as the commit. Projections are then
/// up to date when the commit returns; a failure goes to the publisher's
/// [`OnFailure`] policy and never undoes the commit.
///
/// Implemented for `()` (no processing), for tuples of [`EventHandler`]s up
/// to 8 elements (a group: members in order, a failure stopping the group,
/// as a tuple means everywhere) and for [`Isolated`] tuples (each member on
/// its own): a fixed, typed fan-out with no `Vec<Box<dyn _>>`.
pub trait EventProcessor<E: DomainEvent>: Send + Sync {
    /// Hands `events` (one commit, in order) to every handler of the group.
    fn process(&self, events: &[EventEnvelope<E>]) -> impl Future<Output = Result<(), ProcessingFailure>> + Send;
}

/// Handlers that failed. Each handler is isolated: one failing does not stop
/// the others, and a failing handler skips the rest of the batch so it never
/// sees events out of order.
#[derive(Debug)]
pub struct ProcessingFailure {
    /// One entry per failed handler (never empty).
    pub failures: Vec<HandlerFailure>,
}

/// One handler that failed while processing a commit.
#[derive(Debug)]
pub struct HandlerFailure {
    /// The handler's type name, to tell which one failed.
    pub handler: &'static str,
    /// Global position of the event it failed on: where a retry resumes.
    pub position: u64,
    /// What the handler returned.
    pub error: HandlerError,
}

impl fmt::Display for ProcessingFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} event handler(s) failed", self.failures.len())
    }
}

impl<E: DomainEvent> EventProcessor<E> for () {
    async fn process(&self, _: &[EventEnvelope<E>]) -> Result<(), ProcessingFailure> {
        Ok(())
    }
}

/// Feeds `events` to one handler, stopping at its first error (recorded in
/// `failures`) so it never sees later events before an earlier one.
async fn run_handler<E: DomainEvent, H: EventHandler<E>>(
    handler: &H,
    events: &[EventEnvelope<E>],
    failures: &mut Vec<HandlerFailure>,
) {
    for event in events {
        // The events are committed already: each one's post-commit step
        // follows its handling, as in a processor's unit of work.
        if let Err(error) = handler
            .handle(event, &mut crate::ProcessingContext::new(&mut (), false))
            .await
        {
            failures.push(HandlerFailure {
                handler: type_name::<H>(),
                position: event.position,
                error,
            });
            return;
        }
        handler.committed(event).await;
    }
}

/// Handlers that each see every event, whatever the others do: one failing
/// (a notification) neither stops nor delays another (a projection). What a
/// tuple of inline handlers means once wrapped:
/// `InlinePublisher::new(store).with_processor(Isolated((projection, notifier)))`.
/// A bare tuple is a group instead.
#[derive(Clone, Copy, Debug, Default)]
pub struct Isolated<T>(pub T);

/// Implements [`EventProcessor`] for a tuple of handlers: a group, fed each
/// event in turn, stopping at its first failure.
macro_rules! tuple_processor {
    ($($handler:ident $index:tt),+) => {
        impl<E: DomainEvent, $($handler: EventHandler<E>),+> EventProcessor<E> for ($($handler,)+) {
            async fn process(&self, events: &[EventEnvelope<E>]) -> Result<(), ProcessingFailure> {
                let mut failures = Vec::new();
                run_handler(self, events, &mut failures).await;
                if failures.is_empty() {
                    Ok(())
                } else {
                    Err(ProcessingFailure { failures })
                }
            }
        }

        impl<E: DomainEvent, $($handler: EventHandler<E>),+> EventProcessor<E> for Isolated<($($handler,)+)> {
            async fn process(&self, events: &[EventEnvelope<E>]) -> Result<(), ProcessingFailure> {
                let handlers = &self.0;
                let mut failures = Vec::new();
                $(run_handler(&handlers.$index, events, &mut failures).await;)+
                if failures.is_empty() {
                    Ok(())
                } else {
                    Err(ProcessingFailure { failures })
                }
            }
        }
    };
}

tuple_processor!(H0 0);
tuple_processor!(H0 0, H1 1);
tuple_processor!(H0 0, H1 1, H2 2);
tuple_processor!(H0 0, H1 1, H2 2, H3 3);
tuple_processor!(H0 0, H1 1, H2 2, H3 3, H4 4);
tuple_processor!(H0 0, H1 1, H2 2, H3 3, H4 4, H5 5);
tuple_processor!(H0 0, H1 1, H2 2, H3 3, H4 4, H5 5, H6 6);
tuple_processor!(H0 0, H1 1, H2 2, H3 3, H4 4, H5 5, H6 6, H7 7);

/// What happens to an [`EventProcessor`] failure after a commit that went
/// through the [`EventPublisher`] port: nobody is left to return it to (the
/// events are stored), so it is handed to the policy. Implemented for
/// [`LogFailures`] and for any `Fn(&ProcessingFailure)`.
pub trait OnFailure: Send + Sync {
    /// Called once per commit whose inline handlers failed.
    fn failed(&self, failure: &ProcessingFailure);
}

/// The default [`OnFailure`] policy: logs the failure at error level.
#[derive(Clone, Copy, Debug, Default)]
pub struct LogFailures;

impl OnFailure for LogFailures {
    fn failed(&self, failure: &ProcessingFailure) {
        for failed in &failure.failures {
            tracing::error!(
                handler = failed.handler,
                position = failed.position,
                error = %failed.error,
                "inline event handler failed after the commit; the events are stored",
            );
        }
    }
}

impl<F: Fn(&ProcessingFailure) + Send + Sync> OnFailure for F {
    fn failed(&self, failure: &ProcessingFailure) {
        self(failure);
    }
}

/// Appends what command handlers commit to the store `S`, with optimistic
/// concurrency and the commit's condition, then hands the committed events to
/// its processing group `P` in the caller's task. One publisher serves every
/// aggregate of the event type `E`.
///
/// A processing failure never undoes the commit (the events are stored): it
/// goes to the [`OnFailure`] policy, which logs it unless
/// [`on_failure`](Self::on_failure) says otherwise.
///
/// ```text
/// let events = InlinePublisher::new(store).with_processor(Isolated((projection, notifier)));
/// let unit = Cqrs::from_publisher(events).commands(gateway).build();
/// ```
pub struct InlinePublisher<E, S, P = (), F = LogFailures> {
    /// Where the event streams live.
    store: S,
    /// Who receives the events after each commit.
    processor: P,
    /// What happens when a handler of `processor` fails.
    on_failure: F,
    /// Ties the publisher to one event type without storing one.
    event: PhantomData<fn() -> E>,
}

impl<E, S> InlinePublisher<E, S> {
    /// A publisher over `store` whose commits feed no event handler.
    pub fn new(store: S) -> Self {
        Self {
            store,
            processor: (),
            on_failure: LogFailures,
            event: PhantomData,
        }
    }
}

impl<E, S, P, F> InlinePublisher<E, S, P, F> {
    /// Hands every commit to `processor` (an [`EventProcessor`], e.g. a tuple
    /// of event handlers).
    pub fn with_processor<P2>(self, processor: P2) -> InlinePublisher<E, S, P2, F> {
        InlinePublisher {
            store: self.store,
            processor,
            on_failure: self.on_failure,
            event: PhantomData,
        }
    }

    /// What to do when an inline handler fails after a commit.
    ///
    /// The commit stands: the events are stored, and returning the failure to
    /// the command's sender would tell it the command failed. So the failure
    /// goes to `policy`. The default, [`LogFailures`], logs it at error level;
    /// any `Fn(&ProcessingFailure)` can replace it.
    pub fn on_failure<F2>(self, policy: F2) -> InlinePublisher<E, S, P, F2> {
        InlinePublisher {
            store: self.store,
            processor: self.processor,
            on_failure: policy,
            event: PhantomData,
        }
    }

    /// The underlying event store, e.g. to read every event in order.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// The processing group fed after each commit.
    pub fn processor(&self) -> &P {
        &self.processor
    }
}

impl<E, S, P, F> EventPublisher<E> for InlinePublisher<E, S, P, F>
where
    E: DomainEvent,
    S: AppendEvents<E>,
    P: EventProcessor<E>,
    F: OnFailure,
{
    fn commit<A: Aggregate<Event = E>>(
        &self,
        entity: Loaded<A>,
        pending: Pending<E>,
    ) -> impl Future<Output = Result<Option<u64>, StoreError>> + Send {
        publish(&self.store, &self.processor, &self.on_failure, entity, pending)
    }
}

/// Appends `pending` to `entity`'s stream in `store`, expecting the version
/// it was loaded at, then hands the commit to `processor`: what every
/// publisher does. The last position, if anything was appended.
pub(crate) async fn publish<E, A, S, P, F>(
    store: &S,
    processor: &P,
    on_failure: &F,
    entity: Loaded<A>,
    pending: Pending<E>,
) -> Result<Option<u64>, StoreError>
where
    E: DomainEvent,
    A: Aggregate<Event = E>,
    S: AppendEvents<E>,
    P: EventProcessor<E>,
    F: OnFailure,
{
    let (stream, expected, loaded_condition) = entity.into_commit();
    let condition = crate::AppendCondition::both(loaded_condition, pending.condition);
    let committed = store
        .append_if(
            &mut (),
            &stream,
            expected,
            pending.events,
            &pending.metadata,
            condition.as_ref(),
        )
        .await?;
    // Processing failures do not undo the commit: a tracking processor
    // retries; for inline ones the failure policy decides.
    if let Err(failure) = processor.process(&committed).await {
        on_failure.failed(&failure);
    }
    Ok(committed.last().map(|event| event.position))
}

/// Appends `pending` to `entity`'s stream in `store`, within `tx`, expecting
/// the version it was loaded at and the conditions of what was read: what
/// every [`PublishIn`](crate::PublishIn) does.
#[cfg_attr(not(feature = "tokio"), allow(dead_code))] // only `Events` appends in a transaction
pub(crate) async fn append_in<E, A, S, Tx>(
    store: &S,
    tx: &mut Tx,
    entity: &Loaded<A>,
    pending: Pending<E>,
) -> Result<Vec<EventEnvelope<E>>, StoreError>
where
    E: DomainEvent,
    A: Aggregate<Event = E>,
    S: AppendEvents<E, Tx>,
    Tx: Send,
{
    let (stream, expected) = entity.target();
    let condition = crate::AppendCondition::both(entity.condition().cloned(), pending.condition);
    store
        .append_if(
            tx,
            &stream,
            expected,
            pending.events,
            &pending.metadata,
            condition.as_ref(),
        )
        .await
}
