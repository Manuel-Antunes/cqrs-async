//! Fan-in: several logs read as one, in their global order, by a projection
//! whose event is an enum with a variant per log's event type
//! (`#[derive(DomainEvent)] #[event(fan_in)]`).
//!
//! The write side does not change: each aggregate root appends to its own
//! log, of its own event type. The read side joins them, and only where a
//! projection needs to: no event type joins every log.

use std::{fmt, future::Future, marker::PhantomData};

use crate::{EventEnvelope, InMemoryEventStore, ReadLog, StoreError};

/// Several in-memory logs read as one ([`ReadLog`] of `U`), in position
/// order: each event comes out as the `U` its type converts into
/// (`U: From<E>` for each log's `E`, which `#[event(fan_in)]` derives).
///
/// The logs must share their numbering ([`InMemoryEventStore::beside`]),
/// so their positions say which event came first: [`new`](Self::new) checks
/// it. With the `tokio` feature, `Events::fan_in` makes one from the logs'
/// `Events` and wakes its readers when any of them commits.
///
/// ```
/// # use cqrs::store::{FanIn, InMemoryEventStore, ReadLog};
/// #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
/// enum PostEvent { Published }
/// #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
/// enum UserEvent { Renamed }
///
/// #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
/// #[event(fan_in)]
/// enum FeedEvent {
///     Post(PostEvent),
///     User(UserEvent),
/// }
///
/// let posts = InMemoryEventStore::<PostEvent>::new();
/// let users = posts.beside::<UserEvent>();
/// let feed: FanIn<FeedEvent, _> = FanIn::new((posts, users));
/// # let _ = feed;
/// ```
pub struct FanIn<U, L> {
    /// The logs, a tuple of stores.
    logs: L,
    /// The event they are read as.
    event: PhantomData<fn() -> U>,
}

impl<U, L: InMemoryLogs<U>> FanIn<U, L> {
    /// Reads `logs` (a tuple of 2 to 8 stores) as one.
    ///
    /// # Panics
    ///
    /// If the stores do not share their numbering: made with
    /// [`InMemoryEventStore::new`] each, their positions overlap and say
    /// nothing of which event came first.
    pub fn new(logs: L) -> Self {
        assert!(
            logs.numbered_alike(),
            "the logs of a fan-in share their numbering: make them with `InMemoryEventStore::beside`"
        );
        Self {
            logs,
            event: PhantomData,
        }
    }
}

impl<U, L> FanIn<U, L> {
    /// The logs.
    pub fn logs(&self) -> &L {
        &self.logs
    }
}

impl<U, L: Clone> Clone for FanIn<U, L> {
    fn clone(&self) -> Self {
        Self {
            logs: self.logs.clone(),
            event: PhantomData,
        }
    }
}

impl<U, L> fmt::Debug for FanIn<U, L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FanIn").finish_non_exhaustive()
    }
}

impl<U, L> ReadLog<U> for FanIn<U, L>
where
    U: Send + Sync,
    L: InMemoryLogs<U>,
{
    /// The last position any log reached is read first, then each log up
    /// to it: a position below it was taken before it, under its log's
    /// lock, so its event is stored by the time that log is read. Nothing
    /// is skipped, and the merged page is in order.
    fn read_all(
        &self,
        after: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<EventEnvelope<U>>, StoreError>> + Send {
        let up_to = self.logs.last();
        let mut events = Vec::new();
        self.logs.read(after, up_to, limit, &mut events);
        events.sort_unstable_by_key(|event| event.position);
        events.truncate(limit);
        std::future::ready(Ok(events))
    }

    fn last_position(&self) -> impl Future<Output = Result<u64, StoreError>> + Send {
        std::future::ready(Ok(self.logs.last()))
    }
}

/// A tuple of 2 to 8 [`InMemoryEventStore`]s whose event types convert into
/// `U`: what a [`FanIn`] reads.
pub trait InMemoryLogs<U>: sealed::Sealed + Send + Sync {
    /// Whether the stores share their numbering.
    #[doc(hidden)]
    fn numbered_alike(&self) -> bool;

    /// The last position of any of them.
    #[doc(hidden)]
    fn last(&self) -> u64;

    /// Up to `limit` events of each with `after < position <= up_to`, as
    /// `U`, appended to `into` (not merged).
    #[doc(hidden)]
    fn read(&self, after: u64, up_to: u64, limit: usize, into: &mut Vec<EventEnvelope<U>>);
}

/// The seal of [`InMemoryLogs`]: only tuples of stores.
mod sealed {
    /// Implemented for tuples of `InMemoryEventStore`s.
    pub trait Sealed {}
}

/// `event`, its payload converted into `U`.
pub(crate) fn convert<E, U: From<E>>(event: EventEnvelope<E>) -> EventEnvelope<U> {
    EventEnvelope {
        position: event.position,
        stream: event.stream,
        sequence: event.sequence,
        event_type: event.event_type,
        event_version: event.event_version,
        recorded_at: event.recorded_at,
        metadata: event.metadata,
        payload: U::from(event.payload),
    }
}

/// Implements [`InMemoryLogs`] for a tuple of stores, the first one named
/// apart (the others' numbering is compared to it).
macro_rules! in_memory_logs {
    ($first:ident $(, $rest:ident)+) => {
        impl<$first, $($rest),+> sealed::Sealed
            for (InMemoryEventStore<$first>, $(InMemoryEventStore<$rest>),+) {}

        #[allow(non_snake_case)]
        impl<U, $first, $($rest),+> InMemoryLogs<U>
            for (InMemoryEventStore<$first>, $(InMemoryEventStore<$rest>),+)
        where
            U: From<$first> $(+ From<$rest>)+,
            $first: Clone + Send + Sync,
            $($rest: Clone + Send + Sync,)+
        {
            fn numbered_alike(&self) -> bool {
                let ($first, $($rest),+) = self;
                true $(&& $first.numbered_like($rest))+
            }

            fn last(&self) -> u64 {
                let ($first, $($rest),+) = self;
                $first.last() $(.max($rest.last()))+
            }

            fn read(&self, after: u64, up_to: u64, limit: usize, into: &mut Vec<EventEnvelope<U>>) {
                let ($first, $($rest),+) = self;
                into.extend($first.read_after(after, up_to, limit).into_iter().map(convert));
                $(into.extend($rest.read_after(after, up_to, limit).into_iter().map(convert));)+
            }
        }
    };
}

in_memory_logs!(A, B);
in_memory_logs!(A, B, C);
in_memory_logs!(A, B, C, D);
in_memory_logs!(A, B, C, D, E);
in_memory_logs!(A, B, C, D, E, F);
in_memory_logs!(A, B, C, D, E, F, G);
in_memory_logs!(A, B, C, D, E, F, G, H);
