//! The event log as an outbox (Watermill's forwarder): a tracking processor
//! whose handler, [`Forward`], publishes each event to a message broker.
//!
//! The event is in the log already, committed with the decision that caused
//! it; the processor carries it to the broker at least once, from its
//! token, retrying (and parking, with a dead-letter queue) when the broker
//! is down. Nothing is published that was not committed, and nothing
//! committed is lost.
//!
//! The broker is a port of one method ([`Publish`]): an adapter for SQS,
//! AMQP, Kafka or anything else implements it, in the application or in a
//! crate of its own. Coming the other way, the application's consumer sends
//! commands through its gateway: no subscriber port is needed.

use std::{borrow::Cow, future::Future};

use crate::{Codec, DomainEvent, EventEnvelope, EventHandler, HandlerError, Metadata, ProcessingContext};

/// One event, as [`Forward`] hands it to a broker.
#[derive(Clone, Copy, Debug)]
pub struct Outgoing<'a> {
    /// Where it goes.
    pub topic: &'a str,
    /// Its aggregate's stream (`post-42`): a partition or ordering key, so a
    /// broker that orders by key keeps each aggregate's events in order.
    pub key: &'a str,
    /// Its name ([`DomainEvent::event_type`]).
    pub event_type: &'a str,
    /// The version of its shape.
    pub event_version: u32,
    /// Its position in the log: a deduplication id for consumers, as
    /// delivery is at least once.
    pub position: u64,
    /// The event, encoded by the forwarder's codec.
    pub payload: &'a [u8],
    /// What it was committed with (correlation id, user...).
    pub metadata: &'a Metadata,
}

/// Publishes to a message broker: what [`Forward`] needs of one.
pub trait Publish: Send + Sync {
    /// The broker's error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Sends `message`; returns once the broker accepted it.
    fn publish(&self, message: Outgoing<'_>) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// The topic each event goes to: one for all ([`Forward::new`]), or chosen
/// per event ([`Forward::routed`]).
pub trait TopicOf<E>: Send + Sync {
    /// The topic of `event`.
    fn topic_of<'a>(&'a self, event: &EventEnvelope<E>) -> Cow<'a, str>;
}

/// The same topic for every event.
#[derive(Clone, Debug)]
pub struct FixedTopic(String);

impl<E> TopicOf<E> for FixedTopic {
    fn topic_of<'a>(&'a self, _: &EventEnvelope<E>) -> Cow<'a, str> {
        Cow::Borrowed(&self.0)
    }
}

/// A topic chosen per event by a function.
#[derive(Clone, Debug)]
pub struct RoutedTopic<F>(F);

impl<E, F: Fn(&EventEnvelope<E>) -> String + Send + Sync> TopicOf<E> for RoutedTopic<F> {
    fn topic_of<'a>(&'a self, event: &EventEnvelope<E>) -> Cow<'a, str> {
        Cow::Owned((self.0)(event))
    }
}

/// An [`EventHandler`] publishing every event to a broker through `P`,
/// encoded by `C`, on the topic `T` chooses: run it in a tracking processor
/// (`events.track("outbox", forward)`) and the log is the outbox.
///
/// It writes nothing in the processor's unit of work, so it runs with any
/// (`Tx`), and it does not see replays: an event once published is not sent
/// again because a projection was rebuilt.
#[derive(Clone, Debug)]
pub struct Forward<P, C, T = FixedTopic> {
    /// The broker.
    publisher: P,
    /// How events become bytes.
    codec: C,
    /// Where each event goes.
    topic: T,
}

impl<P, C> Forward<P, C> {
    /// Publishes every event through `publisher`, encoded by `codec`, to
    /// `topic`.
    pub fn new(publisher: P, codec: C, topic: impl Into<String>) -> Self {
        Self {
            publisher,
            codec,
            topic: FixedTopic(topic.into()),
        }
    }

    /// Publishes every event through `publisher`, encoded by `codec`, to the
    /// topic `route` names for it (by its type, say).
    pub fn routed<E, F>(publisher: P, codec: C, route: F) -> Forward<P, C, RoutedTopic<F>>
    where
        F: Fn(&EventEnvelope<E>) -> String + Send + Sync,
    {
        Forward {
            publisher,
            codec,
            topic: RoutedTopic(route),
        }
    }
}

impl<E, Tx, P, C, T> EventHandler<E, Tx> for Forward<P, C, T>
where
    E: DomainEvent,
    Tx: Send,
    P: Publish,
    C: Codec<E>,
    T: TopicOf<E>,
{
    const REPLAY: bool = false;

    async fn handle(&self, event: &EventEnvelope<E>, _: &mut ProcessingContext<'_, Tx>) -> Result<(), HandlerError> {
        let payload = self.codec.encode(&event.payload)?;
        let topic = self.topic.topic_of(event);
        let key = event.stream.to_string();
        let message = Outgoing {
            topic: &topic,
            key: &key,
            event_type: &event.event_type,
            event_version: event.event_version,
            position: event.position,
            payload: &payload,
            metadata: &event.metadata,
        };
        self.publisher.publish(message).await?;
        Ok(())
    }
}
