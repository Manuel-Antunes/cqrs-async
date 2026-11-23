//! Test utilities: a given/when/then fixture for aggregates (domain rules
//! tested without a store: past events in, new events or an error out) and a
//! tiny executor, so the core is tested without choosing an async runtime.

use std::{
    fmt::Debug,
    future::Future,
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread::{self, Thread},
};

use crate::Aggregate;

/// Runs a future to completion on the current thread, parking it while the
/// future is pending. Enough for tests; not a replacement for a runtime.
pub fn block_on<F: Future>(future: F) -> F::Output {
    /// Wakes the blocked thread when the future can make progress.
    struct Unpark(Thread);

    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

/// Given/when/then test of an aggregate's behavior: *given* past events,
/// *when* a decision runs, *then* expect the events it appended, the error it
/// returned or the resulting state. Mirrors the engine: the decision gets
/// `&A` and an appender, and the events are applied only if it succeeds.
///
/// ```
/// # use cqrs::{Aggregate, EventAppender, testing::AggregateFixture};
/// # #[derive(Debug, PartialEq, cqrs::DomainEvent, cqrs::TagResolver)] enum Event { Opened, Closed }
/// # #[derive(Debug, PartialEq)] struct Door { open: bool }
/// # impl Aggregate for Door {
/// #     const TYPE: &'static str = "door"; type Id = u8; type Event = Event;
/// #     fn initial(_: &u8) -> Self { Door { open: false } }
/// #     fn id(&self) -> &u8 { &0 }
/// #     fn evolve(&mut self, e: &Event) { self.open = *e == Event::Opened }
/// # }
/// # impl Door {
/// #     fn close(&self, events: &mut impl EventAppender<Event>) -> Result<(), &'static str> {
/// #         if !self.open { return Err("already closed") } events.append(Event::Closed); Ok(())
/// #     }
/// # }
/// AggregateFixture::<Door>::new(0)
///     .given([Event::Opened])
///     .when(|door, events| door.close(events))
///     .expect_events([Event::Closed])
///     .expect_state(|door| assert!(!door.open));
/// ```
pub struct AggregateFixture<A> {
    /// The aggregate being built from history.
    aggregate: A,
}

impl<A: Aggregate> AggregateFixture<A> {
    /// Starts from [`Aggregate::initial`], with no history: what a creation
    /// decision runs on.
    pub fn new(id: A::Id) -> Self {
        Self {
            aggregate: A::initial(&id),
        }
    }

    /// Replays past events.
    #[must_use]
    pub fn given(mut self, history: impl IntoIterator<Item = A::Event>) -> Self {
        for event in history {
            self.aggregate.evolve(&event);
        }
        self
    }

    /// Runs a decision on the aggregate built so far. Its events are applied
    /// to the state only if it succeeds, as the engine does.
    pub fn when<T, Er>(mut self, decide: impl FnOnce(&A, &mut Vec<A::Event>) -> Result<T, Er>) -> Then<A, T, Er> {
        let mut events = Vec::new();
        let result = decide(&self.aggregate, &mut events);
        if result.is_err() {
            events.clear();
        }
        for event in &events {
            self.aggregate.evolve(event);
        }
        Then {
            aggregate: self.aggregate,
            events,
            result,
        }
    }
}

/// The outcome of [`AggregateFixture::when`], to assert on.
pub struct Then<A: Aggregate, T, Er> {
    /// The aggregate after the decision's events (unchanged on error).
    aggregate: A,
    /// What the decision appended (empty on error).
    events: Vec<A::Event>,
    /// What the decision returned.
    result: Result<T, Er>,
}

impl<A: Aggregate, T: Debug, Er: Debug> Then<A, T, Er> {
    /// Asserts success and exactly these events, in order.
    #[track_caller]
    pub fn expect_events(self, expected: impl IntoIterator<Item = A::Event>) -> Self
    where
        A::Event: PartialEq + Debug,
    {
        let expected: Vec<_> = expected.into_iter().collect();
        assert!(self.result.is_ok(), "expected success, got {:?}", self.result);
        assert_eq!(self.events, expected);
        self
    }

    /// Asserts that nothing was appended.
    #[track_caller]
    pub fn expect_no_events(self) -> Self
    where
        A::Event: Debug,
    {
        assert!(self.events.is_empty(), "expected no events, got {:?}", self.events);
        self
    }

    /// Asserts this error.
    #[track_caller]
    pub fn expect_error(self, expected: Er) -> Self
    where
        Er: PartialEq,
    {
        match &self.result {
            Err(error) => assert_eq!(error, &expected),
            Ok(value) => panic!("expected error {expected:?}, got Ok({value:?})"),
        }
        self
    }

    /// Checks the state after the decision's events.
    #[track_caller]
    pub fn expect_state(self, check: impl FnOnce(&A)) -> Self {
        check(&self.aggregate);
        self
    }

    /// What the decision returned.
    pub fn into_result(self) -> Result<T, Er> {
        self.result
    }
}
