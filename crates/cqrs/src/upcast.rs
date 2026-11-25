//! Events as stores keep them, and reading ones written in an older shape
//! (Axon's upcasters). Shared by every store that keeps JSON (the `json`
//! feature).
//!
//! An event's payload is stored as JSON with its type and version. When the
//! Rust type changes, bump [`DomainEvent::event_version`](crate::DomainEvent::event_version)
//! and register an upcaster that rewrites the old JSON into the new shape;
//! the stored rows are never changed.
//!
//! ```text
//! // v1: Created { title }   v2: Created { title, tags }
//! let store = PostgresEventStore::<PostEvent>::new(pool, "posts").with_upcasters((
//!     upcast("post.created", 1, |payload| payload["Created"]["tags"] = json!([])),
//!     upcast("post.created", 2, |payload| payload["Created"]["draft"] = json!(true)),
//! ));
//! ```

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{DomainEvent, StoreError};

/// An event as read from the store, before it is decoded into the Rust
/// type: what an [`Upcaster`] rewrites.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredEvent {
    /// The event's type name ([`DomainEvent::event_type`]).
    pub event_type: String,
    /// The schema version the payload is in.
    pub version: u32,
    /// The payload, as `serde_json` encoded the Rust value.
    pub payload: Value,
}

impl StoredEvent {
    /// `event` as a store writes it: its type, its version and its payload
    /// as JSON.
    ///
    /// # Errors
    ///
    /// If `serde_json` cannot encode it.
    pub fn encode<E: DomainEvent + Serialize>(event: &E) -> Result<Self, StoreError> {
        Ok(Self {
            event_type: event.event_type().to_owned(),
            version: event.event_version(),
            payload: serde_json::to_value(event).map_err(StoreError::backend)?,
        })
    }

    /// Brings it to the current shape with `upcaster`, then decodes it:
    /// the event, with the type name and version it ends up at.
    ///
    /// # Errors
    ///
    /// If the payload, once upcast, does not decode into `E`.
    pub fn decode<E: DeserializeOwned>(mut self, upcaster: &impl Upcaster) -> Result<(E, String, u32), StoreError> {
        upcaster.upcast(&mut self);
        let event = serde_json::from_value(self.payload).map_err(|error| {
            StoreError::backend(format!("cannot decode {} v{}: {error}", self.event_type, self.version))
        })?;
        Ok((event, self.event_type, self.version))
    }
}

impl StoredEvent {
    /// Decodes an event read as JSON text: straight into `E` when `upcaster`
    /// does not apply to its type and version (the usual case), through
    /// [`decode`](Self::decode) otherwise. The event, with the type name and
    /// version it ends up at.
    ///
    /// # Errors
    ///
    /// If the payload does not decode into `E`.
    pub fn decode_text<E: DeserializeOwned>(
        event_type: String,
        version: u32,
        payload: &str,
        upcaster: &impl Upcaster,
    ) -> Result<(E, String, u32), StoreError> {
        let invalid =
            |error: serde_json::Error| StoreError::backend(format!("cannot decode {event_type} v{version}: {error}"));
        if !upcaster.applies(&event_type, version) {
            let event = serde_json::from_str(payload).map_err(invalid)?;
            return Ok((event, event_type, version));
        }
        let payload = serde_json::from_str(payload).map_err(invalid)?;
        Self {
            event_type,
            version,
            payload,
        }
        .decode(upcaster)
    }
}

/// Rewrites stored events from an older shape into a newer one, before they
/// are decoded. Implemented by [`Upcast`] (one type, one version step), by
/// `()` (nothing to do) and by tuples of up to 8, which run in order: put
/// the oldest step first.
pub trait Upcaster: Send + Sync {
    /// Rewrites `event` in place if it applies to it, bumping its version.
    fn upcast(&self, event: &mut StoredEvent);

    /// Whether [`upcast`](Self::upcast) would change an event of
    /// `event_type` at `version`. When it would not, the store decodes the
    /// payload straight into the event type, with no intermediate
    /// [`Value`]. `true` (the default) is always correct, only slower.
    fn applies(&self, event_type: &str, version: u32) -> bool {
        let _ = (event_type, version);
        true
    }
}

impl Upcaster for () {
    fn upcast(&self, _: &mut StoredEvent) {}

    fn applies(&self, _: &str, _: u32) -> bool {
        false
    }
}

/// Implements [`Upcaster`] for a tuple: each element in turn.
macro_rules! tuple_upcaster {
    ($($upcaster:ident $index:tt),+) => {
        impl<$($upcaster: Upcaster),+> Upcaster for ($($upcaster,)+) {
            fn upcast(&self, event: &mut StoredEvent) {
                $(self.$index.upcast(event);)+
            }

            fn applies(&self, event_type: &str, version: u32) -> bool {
                $(self.$index.applies(event_type, version))||+
            }
        }
    };
}

tuple_upcaster!(A 0);
tuple_upcaster!(A 0, B 1);
tuple_upcaster!(A 0, B 1, C 2);
tuple_upcaster!(A 0, B 1, C 2, D 3);
tuple_upcaster!(A 0, B 1, C 2, D 3, E 4);
tuple_upcaster!(A 0, B 1, C 2, D 3, E 4, F 5);
tuple_upcaster!(A 0, B 1, C 2, D 3, E 4, F 5, G 6);
tuple_upcaster!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);

/// One step: events of one type at one version are converted to the next
/// version (Axon's `SingleEventUpcaster`). Built by [`upcast`].
pub struct Upcast<F> {
    /// The type converted.
    event_type: &'static str,
    /// The version converted from; the result is at `from + 1`.
    from: u32,
    /// Rewrites the payload.
    convert: F,
}

impl<F> ::std::fmt::Debug for Upcast<F> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.debug_struct("Upcast").finish_non_exhaustive()
    }
}

/// Converts the payload of `event_type` events at version `from` with
/// `convert`; they come out at version `from + 1`.
pub fn upcast<F: Fn(&mut Value) + Send + Sync>(event_type: &'static str, from: u32, convert: F) -> Upcast<F> {
    Upcast {
        event_type,
        from,
        convert,
    }
}

impl<F: Fn(&mut Value) + Send + Sync> Upcaster for Upcast<F> {
    fn upcast(&self, event: &mut StoredEvent) {
        if event.event_type == self.event_type && event.version == self.from {
            (self.convert)(&mut event.payload);
            event.version += 1;
        }
    }

    fn applies(&self, event_type: &str, version: u32) -> bool {
        event_type == self.event_type && version == self.from
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn events_no_step_applies_to_decode_straight_from_text() {
        let chain = (
            upcast("created", 1, |p| p["tags"] = json!([])),
            upcast("created", 2, |p| p["draft"] = json!(true)),
        );
        assert!(chain.applies("created", 1) && chain.applies("created", 2));
        assert!(!chain.applies("created", 3) && !chain.applies("published", 1) && !().applies("created", 1));

        let (current, _, version): (Value, _, _) =
            StoredEvent::decode_text("created".into(), 3, r#"{"a":1}"#, &chain).unwrap();
        assert_eq!((current, version), (json!({ "a": 1 }), 3));
        let (old, _, version): (Value, _, _) = StoredEvent::decode_text("created".into(), 1, "{}", &chain).unwrap();
        assert_eq!((old, version), (json!({ "tags": [], "draft": true }), 3));
    }

    #[test]
    fn steps_apply_in_order_and_only_to_their_type_and_version() {
        let chain = (
            upcast("created", 1, |p| p["tags"] = json!([])),
            upcast("created", 2, |p| p["draft"] = json!(true)),
        );
        let mut old = StoredEvent {
            event_type: "created".into(),
            version: 1,
            payload: json!({}),
        };
        chain.upcast(&mut old);
        assert_eq!((old.version, old.payload), (3, json!({ "tags": [], "draft": true })));

        let mut other = StoredEvent {
            event_type: "published".into(),
            version: 1,
            payload: json!({}),
        };
        chain.upcast(&mut other);
        assert_eq!((other.version, other.payload), (1, json!({})));
    }
}
