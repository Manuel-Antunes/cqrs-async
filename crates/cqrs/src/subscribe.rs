//! Messages coming from a broker (Axon 5's `SubscribableEventSource`): a
//! [`Subscribe`] port hands them, in batches, to a [`Consume`]r, and
//! acknowledges a batch once the consumer returned `Ok`.
//!
//! The pair of [`Publish`](crate::processing::Publish): what one service
//! forwards from its log, another consumes, deciding what to do with each
//! message (usually, sending a command through its gateway). Delivery is at
//! least once: a batch the consumer failed is delivered again, so a consumer
//! deduplicates by [`Incoming::position`] or makes its commands idempotent.

use std::{future::Future, sync::Arc};

use crate::{Codec, CodecError, HandlerError, Metadata};

/// One message from a broker, as a [`Subscribe`] port hands it to a
/// [`Consume`]r.
///
/// What [`Forward`](crate::processing::Forward) published comes back with
/// every field; a message from another producer may only have a topic and a
/// payload.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Incoming {
    /// Where it came from (the queue, or the routing key).
    pub topic: String,
    /// Its aggregate's stream (`post-42`), when the producer sent one.
    pub key: Option<String>,
    /// Its event's name, when the producer sent one.
    pub event_type: Option<String>,
    /// The version of its event's shape (`1` when the producer sent none).
    pub event_version: u32,
    /// Its position in the producer's log: the id to deduplicate by.
    pub position: Option<u64>,
    /// The message, encoded.
    pub payload: Vec<u8>,
    /// What it was committed with (correlation id, user...).
    pub metadata: Metadata,
    /// Whether the broker delivered it before (a consumer failed it, or
    /// died holding it).
    pub redelivered: bool,
}

impl Incoming {
    /// A message on `topic` carrying `payload`, with nothing else known: an
    /// adapter fills in what its broker tells.
    pub fn new(topic: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            topic: topic.into(),
            key: None,
            event_type: None,
            event_version: 1,
            position: None,
            payload,
            metadata: Metadata::new(),
            redelivered: false,
        }
    }

    /// The payload, decoded by `codec`.
    ///
    /// # Errors
    ///
    /// If the payload is not a `T` in `codec`'s format.
    pub fn decode<T>(&self, codec: &impl Codec<T>) -> Result<T, CodecError> {
        codec.decode(&self.payload)
    }
}

/// Handles a batch of [`Incoming`] messages: what a [`Subscribe`] port
/// delivers to.
///
/// `Ok` acknowledges the whole batch; an error has the broker deliver it
/// again (or, if so configured, dead-letter it). A message that can never
/// be handled (one that does not decode, say) is better acknowledged and
/// logged than failed: failing it has it delivered again forever.
pub trait Consume: Send + Sync {
    /// Handles `messages`, in the order the broker delivered them.
    fn consume(&self, messages: &[Incoming]) -> impl Future<Output = Result<(), HandlerError>> + Send;
}

impl<C: Consume> Consume for Arc<C> {
    fn consume(&self, messages: &[Incoming]) -> impl Future<Output = Result<(), HandlerError>> + Send {
        (**self).consume(messages)
    }
}

impl<C: Consume> Consume for &C {
    fn consume(&self, messages: &[Incoming]) -> impl Future<Output = Result<(), HandlerError>> + Send {
        (**self).consume(messages)
    }
}

/// Delivers a broker's messages to a [`Consume`]r: the port an adapter
/// implements, configured with where it reads from.
pub trait Subscribe: Send + Sync {
    /// The broker's error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Delivers messages to `consumer`, in batches, acknowledging each batch
    /// it handled, until `shutdown` completes; the batch in hand is
    /// finished first.
    ///
    /// # Errors
    ///
    /// When the broker fails (the connection is lost): the caller decides
    /// whether, and when, to subscribe again.
    fn subscribe<C: Consume>(
        &self,
        consumer: C,
        shutdown: impl Future<Output = ()> + Send,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
