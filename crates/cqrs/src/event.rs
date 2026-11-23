//! Events and their identity in the store: the [`DomainEvent`] contract, the
//! [`StreamId`] of an aggregate's history and the [`EventEnvelope`] a
//! committed event travels in.

use std::{borrow::Cow, collections::BTreeMap, fmt, time::SystemTime};

use crate::Aggregate;

/// A fact that happened in the domain. Serialization, `Clone` and equality
/// are required only by the adapters and tests that need them.
/// Its tags come from [`TagResolver`](crate::TagResolver), which the derive
/// implements too.
pub trait DomainEvent: crate::TagResolver + Send + Sync + 'static {
    /// Stable name of this kind of event (e.g. `"post.published"`), stored
    /// next to the payload so readers know how to decode it. Renaming the
    /// Rust type must not change it.
    fn event_type(&self) -> &'static str;

    /// Schema version of the payload. Bump it when the shape changes, so
    /// readers (upcasters) can convert old events; `1` by default.
    fn event_version(&self) -> u32 {
        1
    }
}

/// The stream holding one aggregate's history, e.g. `post-42`.
///
/// Built from the aggregate's [`TYPE`](Aggregate::TYPE) and id, so two kinds
/// of aggregates with the same id never share a stream.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamId {
    /// The aggregate kind ([`Aggregate::TYPE`]): borrowed when built from the
    /// type, owned when read back from a database.
    aggregate_type: Cow<'static, str>,
    /// The aggregate id, rendered with `Display`.
    id: String,
}

impl StreamId {
    /// The stream of aggregate `A` with this id.
    pub fn of<A: Aggregate>(id: &A::Id) -> Self {
        Self {
            aggregate_type: Cow::Borrowed(A::TYPE),
            id: id.to_string(),
        }
    }

    /// A stream from its stored parts, for storage adapters reading events
    /// back. Application code uses [`of`](Self::of).
    pub fn new(aggregate_type: impl Into<Cow<'static, str>>, id: impl Into<String>) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            id: id.into(),
        }
    }

    /// The aggregate kind, e.g. `"post"`.
    pub fn aggregate_type(&self) -> &str {
        &self.aggregate_type
    }

    /// The aggregate id as text, e.g. `"42"`.
    pub fn id(&self) -> &str {
        &self.id
    }
}

/// `<type>-<id>`, e.g. `post-42`.
impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.aggregate_type, self.id)
    }
}

/// Free-form context stored with each event: correlation id, causation id,
/// tenant, actor... Kept as plain strings so the core imposes no schema.
pub type Metadata = BTreeMap<String, String>;

/// A committed event: the payload plus everything the store assigned when
/// it was appended. This is what event handlers receive.
#[derive(Clone, Debug)]
pub struct EventEnvelope<E> {
    /// Position in the whole store, increasing across streams. Stable, so
    /// usable as an idempotency key and as the checkpoint of a processor that
    /// reads the store in order.
    pub position: u64,
    /// The stream (aggregate) the event belongs to.
    pub stream: StreamId,
    /// 1-based position inside the stream: after this event, the aggregate is
    /// at this version.
    pub sequence: u64,
    /// [`DomainEvent::event_type`] at the time of the append.
    pub event_type: String,
    /// The payload's schema version: [`DomainEvent::event_version`] at the
    /// time of the append, or the version a store upcast it to.
    pub event_version: u32,
    /// When the store committed the event.
    pub recorded_at: SystemTime,
    /// Context passed by the writer (see [`Metadata`]).
    pub metadata: Metadata,
    /// The domain event itself.
    pub payload: E,
}
