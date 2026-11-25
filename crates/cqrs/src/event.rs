//! Events and their identity in the store: the [`DomainEvent`] contract, the
//! [`StreamId`] of an aggregate's history and the [`EventEnvelope`] a
//! committed event travels in.

use std::{borrow::Cow, collections::BTreeMap, fmt, sync::Arc, time::SystemTime};

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
///
/// Shared, not copied: every event of a commit carries the metadata the
/// command was sent with, and cloning it is a reference count, whatever it
/// holds. It reads as a map (`metadata.get("user")`, through `Deref`);
/// [`insert`](Self::insert) copies it first if another holder shares it.
#[derive(Clone, Default)]
pub struct Metadata(Option<Arc<BTreeMap<String, String>>>);

/// What empty metadata reads as: no allocation for it.
static EMPTY: BTreeMap<String, String> = BTreeMap::new();

impl Metadata {
    /// Empty metadata (no allocation).
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets `key` to `value`; returns the value it replaces (as
    /// [`BTreeMap::insert`]).
    pub fn insert(&mut self, key: String, value: String) -> Option<String> {
        self.map_mut().insert(key, value)
    }

    /// Removes `key`; returns its value.
    pub fn remove(&mut self, key: &str) -> Option<String> {
        if !self.contains_key(key) {
            return None;
        }
        self.map_mut().remove(key)
    }

    /// The map, owned (copied only if another holder shares it).
    pub fn into_map(self) -> BTreeMap<String, String> {
        self.0
            .map(|map| Arc::try_unwrap(map).unwrap_or_else(|shared| (*shared).clone()))
            .unwrap_or_default()
    }

    /// The map, to change: copied first if another holder shares it.
    fn map_mut(&mut self) -> &mut BTreeMap<String, String> {
        Arc::make_mut(self.0.get_or_insert_with(Arc::default))
    }
}

impl std::ops::Deref for Metadata {
    type Target = BTreeMap<String, String>;

    fn deref(&self) -> &BTreeMap<String, String> {
        self.0.as_deref().unwrap_or(&EMPTY)
    }
}

impl PartialEq for Metadata {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Eq for Metadata {}

impl std::hash::Hash for Metadata {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (**self).hash(state);
    }
}

impl fmt::Debug for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl From<BTreeMap<String, String>> for Metadata {
    fn from(map: BTreeMap<String, String>) -> Self {
        Self((!map.is_empty()).then(|| Arc::new(map)))
    }
}

impl<K: Into<String>, V: Into<String>, const N: usize> From<[(K, V); N]> for Metadata {
    fn from(pairs: [(K, V); N]) -> Self {
        pairs.into_iter().collect()
    }
}

impl<K: Into<String>, V: Into<String>> FromIterator<(K, V)> for Metadata {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(pairs: I) -> Self {
        let map: BTreeMap<String, String> = pairs
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        Self::from(map)
    }
}

impl<K: Into<String>, V: Into<String>> Extend<(K, V)> for Metadata {
    fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, pairs: I) {
        let mut pairs = pairs.into_iter().peekable();
        if pairs.peek().is_some() {
            self.map_mut()
                .extend(pairs.map(|(key, value)| (key.into(), value.into())));
        }
    }
}

impl<'a> IntoIterator for &'a Metadata {
    type Item = (&'a String, &'a String);
    type IntoIter = std::collections::btree_map::Iter<'a, String, String>;

    fn into_iter(self) -> Self::IntoIter {
        (**self).iter()
    }
}

/// As a JSON object (the map it holds).
#[cfg(feature = "json")]
impl serde::Serialize for Metadata {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (**self).serialize(serializer)
    }
}

#[cfg(feature = "json")]
impl<'de> serde::Deserialize<'de> for Metadata {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        BTreeMap::deserialize(deserializer).map(Self::from)
    }
}

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
    /// [`DomainEvent::event_type`] at the time of the append: borrowed from
    /// it in memory, the stored name when read back (or the name an
    /// upcaster gave it).
    pub event_type: Cow<'static, str>,
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

impl<E> EventEnvelope<E> {
    /// The id of the aggregate `A` the event belongs to, parsed from its
    /// stream: `None` if the stream is not one of `A`'s, or its id does not
    /// parse. What a projection keys its rows by.
    ///
    /// ```
    /// # use cqrs::{aggregate::Aggregate, event::{EventEnvelope, Metadata, StreamId}};
    /// # #[derive(Clone, Debug, cqrs::DomainEvent)] enum Ticked { Once }
    /// #[cqrs::aggregate(name = "clock", event = Ticked)]
    /// struct Clock { id: u32 }
    /// # impl Clock { fn apply(&mut self, _: &Ticked) {} }
    /// # let event = EventEnvelope {
    /// #     position: 1, stream: StreamId::of::<Clock>(&7), sequence: 1, event_type: "Once".into(),
    /// #     event_version: 1, recorded_at: std::time::SystemTime::UNIX_EPOCH, metadata: Metadata::new(),
    /// #     payload: Ticked::Once,
    /// # };
    /// assert_eq!(event.aggregate_id::<Clock>(), Some(7));
    /// ```
    pub fn aggregate_id<A>(&self) -> Option<A::Id>
    where
        A: Aggregate,
        A::Id: std::str::FromStr,
    {
        if self.stream.aggregate_type() != A::TYPE {
            return None;
        }
        self.stream.id().parse().ok()
    }
}

#[cfg(test)]
mod metadata_tests {
    use super::Metadata;

    #[test]
    fn a_clone_shares_until_one_side_changes() {
        let sent = Metadata::from([("user", "ana")]);
        let mut copy = sent.clone();
        copy.insert("tenant".into(), "acme".into());
        assert_eq!(sent.len(), 1, "the original is untouched");
        assert_eq!(copy.get("tenant").map(String::as_str), Some("acme"));
        assert_eq!(copy.get("user").map(String::as_str), Some("ana"));
    }

    #[test]
    fn empty_is_empty_however_it_got_there() {
        let mut emptied = Metadata::from([("user", "ana")]);
        assert_eq!(emptied.remove("user").as_deref(), Some("ana"));
        assert_eq!(emptied, Metadata::new());
        assert!(Metadata::new().is_empty());
        let mut extended = Metadata::new();
        extended.extend(std::iter::empty::<(String, String)>());
        assert_eq!(extended, Metadata::default());
    }

    #[cfg(feature = "json")]
    #[test]
    fn it_is_a_json_object() {
        let metadata = Metadata::from([("correlation_id", "req-42")]);
        let json = serde_json::to_string(&metadata).unwrap();
        assert_eq!(json, r#"{"correlation_id":"req-42"}"#);
        assert_eq!(serde_json::from_str::<Metadata>(&json).unwrap(), metadata);
        assert_eq!(serde_json::to_string(&Metadata::new()).unwrap(), "{}");
    }
}
