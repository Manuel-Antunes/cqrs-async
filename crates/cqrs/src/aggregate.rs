//! Domain side of event sourcing: the [`Aggregate`] contract, and the
//! [`EventAppender`] its decisions record events into (Axon 5's
//! `EventAppender`, passed in rather than held by the aggregate).

use std::fmt;

use crate::DomainEvent;

/// The domain half of an event-sourced aggregate: identity, the event type
/// and how an event changes the state.
///
/// The state is never stored directly: it is rebuilt by starting from
/// [`initial`](Self::initial) and feeding every stored event to
/// [`evolve`](Self::evolve), in order. So `evolve` must be a pure state
/// transition (no validation, no I/O, no failure): it runs both when a new
/// event is recorded and when history is replayed, and both must end in the
/// same state. Validation belongs in the methods that decide to record an
/// event.
///
/// Those decision methods take `&self` and an [`EventAppender`]:
///
/// ```
/// # use cqrs::{Aggregate, EventAppender};
/// # #[derive(cqrs::DomainEvent, cqrs::TagResolver)] enum PostEvent { Published }
/// # struct Post { id: u64, published: bool }
/// # #[derive(Debug)] enum PostError { AlreadyPublished }
/// impl Post {
///     pub fn publish(&self, events: &mut impl EventAppender<PostEvent>) -> Result<(), PostError> {
///         if self.published {
///             return Err(PostError::AlreadyPublished);
///         }
///         events.append(PostEvent::Published);
///         Ok(())
///     }
/// }
/// ```
///
/// `&self` makes the rule "state only changes through events" a compile
/// error to break: a decision cannot assign a field, only `evolve` can. The
/// aggregate holds no buffer of pending events either: it is plain state.
///
/// Creating one is the exception: there is no instance to decide on yet, so
/// the creation is an associated function, committed to
/// [`New`](crate::New) (Axon 5's static creational command handler):
///
/// ```
/// # use cqrs::EventAppender;
/// # #[derive(cqrs::DomainEvent, cqrs::TagResolver)] enum PostEvent { Created { id: u64, title: String } }
/// # struct Post;
/// # #[derive(Debug)] enum PostError { EmptyTitle }
/// impl Post {
///     pub fn create(id: u64, title: &str, events: &mut impl EventAppender<PostEvent>) -> Result<(), PostError> {
///         if title.trim().is_empty() {
///             return Err(PostError::EmptyTitle);
///         }
///         events.append(PostEvent::Created { id, title: title.trim().to_owned() });
///         Ok(())
///     }
/// }
/// ```
pub trait Aggregate: Send + Sync + Sized + 'static {
    /// Stable name of the aggregate kind, used as the prefix of its stream ids
    /// (`"post"` → stream `post-42`). Changing it orphans the stored streams.
    const TYPE: &'static str;

    /// Identifies one aggregate instance. `Display` renders the stream id
    /// suffix, so it must be stable and unique per aggregate kind.
    type Id: Clone + fmt::Display + Send + Sync + 'static;

    /// The facts this aggregate records, usually one enum with a variant per
    /// kind of change.
    type Event: DomainEvent;

    /// The state before any event: what replay starts from (Axon 5's entity
    /// creator, from the identifier). A creation is not decided on it: it is
    /// an associated function of the aggregate, committed to
    /// [`New`](crate::New).
    fn initial(id: &Self::Id) -> Self;

    /// The identity of this instance, used to find its stream when saving.
    fn id(&self) -> &Self::Id;

    /// Applies one event to the state. Called for every event, both new and
    /// replayed, so it must not validate, fail or have side effects.
    fn evolve(&mut self, event: &Self::Event);

    /// The events `id` is built from when they are not just its stream
    /// (Axon 5's entity sourced by tags, `#[aggregate(tag = "...")]` when
    /// derived): e.g. every event tagged `course=42`, whichever stream holds
    /// it. Such an aggregate is consistent through an
    /// [`AppendCondition`](crate::AppendCondition) instead of its stream
    /// version. `None` (default): its own stream.
    fn criteria(id: &Self::Id) -> Option<crate::EventCriteria> {
        let _ = id;
        None
    }
}

/// Where a decision records the events it produces.
///
/// Every `Extend<E>` is one, so a `Vec<E>` collects them (what the engine
/// passes, and what a test passes):
///
/// ```
/// # use cqrs::EventAppender;
/// fn publish(events: &mut impl EventAppender<&'static str>) {
///     events.append("published");
/// }
/// let mut events = Vec::new();
/// publish(&mut events);
/// assert_eq!(events, ["published"]);
/// ```
///
/// Decisions take `impl EventAppender`, where `append` is this trait's. On a
/// concrete `Vec`, `events.append(..)` would be `Vec`'s own method (it joins
/// two vectors) and not compile with an event.
///
/// A domain that wants no dependency on this crate can take
/// `&mut impl Extend<E>` instead: the engine's `Vec` fits either.
///
/// Appending does not change the aggregate: the publisher applies the
/// events with [`Aggregate::evolve`] once the decision returns, so a decision
/// sees the state from before it ran.
///
/// It is `Send`, so an `async` command handler holding `&mut impl
/// EventAppender<E>` across `.await`s still returns a `Send` future.
pub trait EventAppender<E>: Send {
    /// Records `event` as a consequence of the current decision.
    fn append(&mut self, event: E);
}

impl<E, T: Extend<E> + Send> EventAppender<E> for T {
    fn append(&mut self, event: E) {
        self.extend([event]);
    }
}
