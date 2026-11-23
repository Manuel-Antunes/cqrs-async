//! Dynamic consistency boundaries (Axon 5's DCB): events carry [`Tag`]s,
//! a decision reads the events matching [`EventCriteria`] wherever they are
//! stored, and its events are appended only if nothing matching was
//! appended since ([`AppendCondition`]).
//!
//! A stream still orders one aggregate's events; criteria let one decision
//! span several streams (a course and a student) without a bigger aggregate.

use std::{borrow::Cow, fmt};

use crate::{DomainEvent, EventEnvelope};

/// A label on an event (`course=42`), what [`EventCriteria`] select by.
/// Derived with `#[derive(TagResolver)]` and `#[tag]` on event fields.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Tag {
    /// What the value is (`"course"`).
    pub key: Cow<'static, str>,
    /// Which one (`"42"`).
    pub value: String,
}

impl Tag {
    /// The tag `key=value`.
    pub fn new(key: impl Into<Cow<'static, str>>, value: impl fmt::Display) -> Self {
        Self {
            key: key.into(),
            value: value.to_string(),
        }
    }
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.key, self.value)
    }
}

/// The tags an event carries (Axon's `TagResolver`): what criteria select
/// it by, and what a conditional append locks. Implemented on the event
/// itself, two ways, as in Axon:
///
/// - **derived** (recommended): `#[derive(TagResolver)]` and `#[tag]` on the
///   fields to tag by (`@EventTag`); with none, the event has no tags.
/// - **by hand**, keeping the event free of the framework's attributes:
///
/// ```
/// use cqrs::{Tag, TagResolver};
///
/// #[derive(Clone, cqrs::DomainEvent)]
/// enum OrderEvent {
///     Placed { order: u32, region: String },
/// }
///
/// impl TagResolver for OrderEvent {
///     fn tags(&self) -> Vec<Tag> {
///         let Self::Placed { order, region } = self;
///         vec![Tag::new("order", order), Tag::new("region", region)]
///     }
/// }
///
/// let placed = OrderEvent::Placed { order: 7, region: "eu".into() };
/// assert_eq!(placed.tags(), cqrs::tags![order = 7, region = "eu"]);
/// ```
pub trait TagResolver {
    /// The event's tags. None by default.
    fn tags(&self) -> Vec<Tag> {
        Vec::new()
    }
}

/// One way to match: the event is of one of `types` (any type if empty)
/// and has every tag in `tags`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Criterion {
    /// Accepted event types; empty accepts any.
    pub types: Vec<&'static str>,
    /// Tags the event must all have.
    pub tags: Vec<Tag>,
}

/// Which events a decision depends on: those matching any of its
/// criteria.
///
/// ```
/// # use cqrs::{EventCriteria, Tag};
/// // Everything about course 42, plus the subscriptions of student 7.
/// let criteria = EventCriteria::tagged([Tag::new("course", 42)])
///     .or(EventCriteria::tagged([Tag::new("student", 7)]).of_types(["student.subscribed"]));
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EventCriteria {
    /// Alternatives: an event matches if it matches one.
    pub criteria: Vec<Criterion>,
}

impl EventCriteria {
    /// Events with all of `tags`, of any type.
    pub fn tagged(tags: impl IntoIterator<Item = Tag>) -> Self {
        Self {
            criteria: vec![Criterion {
                types: Vec::new(),
                tags: tags.into_iter().collect(),
            }],
        }
    }

    /// Narrows every alternative to events of `types`.
    #[must_use]
    pub fn of_types(mut self, types: impl IntoIterator<Item = &'static str> + Clone) -> Self {
        for criterion in &mut self.criteria {
            criterion.types = types.clone().into_iter().collect();
        }
        self
    }

    /// Events matching `self` or `other`.
    #[must_use]
    pub fn or(mut self, other: Self) -> Self {
        self.criteria.extend(other.criteria);
        self
    }

    /// Whether an event of `event_type` with `tags` matches.
    pub fn matches(&self, event_type: &str, tags: &[Tag]) -> bool {
        self.criteria.iter().any(|criterion| {
            (criterion.types.is_empty() || criterion.types.contains(&event_type))
                && criterion.tags.iter().all(|tag| tags.contains(tag))
        })
    }

    /// Whether a committed `event` matches.
    pub fn matches_event<E: DomainEvent>(&self, event: &EventEnvelope<E>) -> bool {
        self.matches(&event.event_type, &event.payload.tags())
    }
}

/// Appends only if no event matching `criteria` was appended after
/// position `after` (the head when the decision read them).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendCondition {
    /// What the decision read.
    pub criteria: EventCriteria,
    /// Where the store's head was when it read them.
    pub after: u64,
}

impl AppendCondition {
    /// Both conditions at once: either's criteria, checked from the earlier
    /// of their positions.
    #[must_use]
    pub fn and(self, other: Self) -> Self {
        Self {
            criteria: self.criteria.or(other.criteria),
            after: self.after.min(other.after),
        }
    }

    /// [`and`](Self::and) of two optional conditions.
    pub fn both(first: Option<Self>, second: Option<Self>) -> Option<Self> {
        match (first, second) {
            (Some(first), Some(second)) => Some(first.and(second)),
            (first, second) => first.or(second),
        }
    }
}

/// An array of [`Tag`]s, keys written as identifiers:
/// `tags![course = 42, student = id]`.
///
/// ```
/// # use cqrs::{Tag, tags};
/// assert_eq!(tags![course = 42, student = "ana"], [Tag::new("course", 42), Tag::new("student", "ana")]);
/// ```
#[macro_export]
macro_rules! tags {
    ($($key:ident = $value:expr),* $(,)?) => {
        [$($crate::Tag::new(::core::stringify!($key), $value)),*]
    };
}

/// [`EventCriteria`]: alternatives separated by `;`, each the tags an event
/// must all carry, optionally narrowed to some event types after `=>`.
///
/// ```
/// # use cqrs::{EventCriteria, Tag, criteria};
/// let course = 42;
/// let student = 7;
/// // Everything about the course, or the student's subscriptions.
/// let built = criteria![course = course; student = student => ["student.subscribed"]];
/// assert_eq!(
///     built,
///     EventCriteria::tagged([Tag::new("course", 42)])
///         .or(EventCriteria::tagged([Tag::new("student", 7)]).of_types(["student.subscribed"])),
/// );
/// ```
#[macro_export]
macro_rules! criteria {
    ($($($key:ident = $value:expr),+ $(=> [$($kind:expr),* $(,)?])?);+ $(;)?) => {
        $crate::EventCriteria {
            criteria: ::std::vec![$(
                $crate::Criterion {
                    types: ::std::vec![$($($kind),*)?],
                    tags: ::std::vec::Vec::from($crate::tags![$($key = $value),+]),
                }
            ),+],
        }
    };
}
