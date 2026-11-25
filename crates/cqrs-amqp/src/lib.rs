//! AMQP 0-9-1 (RabbitMQ) for [`cqrs`], over [`lapin`]: an [`AmqpPublisher`]
//! for [`Forward`](cqrs::processing::Forward), so the event log is the
//! outbox of a broker, and an [`AmqpSubscriber`] handing what comes back
//! to a [`Consume`]r.
//!
//! Both come in the three shapes of Watermill's presets, one constructor
//! each:
//!
//! | Constructor | Publishes to | Consumers |
//! |---|---|---|
//! | `fanout` | a fanout exchange named after the topic | each queue bound to it gets every message: one queue per service (fan-out), the instances of a service sharing it (competing) |
//! | `queue` | a durable queue named after the topic | compete for its messages |
//! | `topic` | one topic exchange, the topic as routing key | bind a queue with a pattern (`post.*`) |
//!
//! Messages are persistent, published with confirms (the publisher returns
//! once the broker has them), and carry what [`Outgoing`] has: the event's
//! type, its version, its position (the message id, for deduplication), its
//! aggregate's stream and its metadata. A subscriber acknowledges a batch
//! once its consumer returned `Ok`, and has a failed one delivered again,
//! after a pause growing with the failures in a row (or dead-lettered, see
//! [`AmqpSubscriber::dead_letter_failures`]).
//!
//! ```no_run
//! use cqrs::processing::{Consume, Events, Forward, HandlerError, Incoming, Subscribe};
//! use cqrs_amqp::{Amqp, AmqpPublisher, AmqpSubscriber};
//! # #[derive(Clone, Debug, PartialEq, cqrs::DomainEvent, serde::Serialize, serde::Deserialize)]
//! # enum PostEvent { Published }
//! # async fn wire(events: Events<PostEvent, cqrs::store::InMemoryEventStore<PostEvent>>) -> Result<(), cqrs_amqp::AmqpError> {
//! let amqp = Amqp::connect("amqp://127.0.0.1:5672/%2f").await?;
//!
//! // Out: the log, forwarded to the `posts` fanout exchange.
//! let publisher = AmqpPublisher::fanout(&amqp).await?;
//! let _outbox = events.track("outbox", Forward::new(publisher, cqrs::store::Json, "posts"));
//!
//! // In, in another service: its own queue, bound to `posts`.
//! struct Notify;
//! impl Consume for Notify {
//!     async fn consume(&self, messages: &[Incoming]) -> Result<(), HandlerError> {
//!         for message in messages {
//!             let _event: PostEvent = message.decode(&cqrs::store::Json)?;
//!             // gateway.send(..).await?
//!         }
//!         Ok(())
//!     }
//! }
//! let subscriber = AmqpSubscriber::fanout(&amqp, "posts", "notifications").await?;
//! subscriber.subscribe(Notify, std::future::pending()).await?;
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs, missing_debug_implementations)]

use std::{
    collections::HashSet,
    fmt,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use cqrs::{
    event::Metadata,
    processing::{Consume, Incoming, Outgoing, Publish, Subscribe},
};
use futures_util::{FutureExt as _, StreamExt as _};
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
    message::Delivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions, BasicQosOptions,
        ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable, ShortString},
};

pub use lapin;

/// The header carrying the event's aggregate stream.
const KEY: &str = "cqrs-key";
/// The header carrying the version of the event's shape.
const VERSION: &str = "cqrs-event-version";
/// The header carrying the event's metadata, as a nested table.
const METADATA: &str = "cqrs-metadata";
/// Delivery mode 2: written to disk by the broker.
const PERSISTENT: u8 = 2;

/// A connection to the broker, shared by the publishers and subscribers
/// made from it (each opens its own channels).
#[derive(Clone)]
pub struct Amqp {
    /// The connection.
    connection: Arc<Connection>,
}

impl fmt::Debug for Amqp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Amqp")
            .field("connected", &self.connection.status().connected())
            .finish()
    }
}

impl Amqp {
    /// Connects to the broker at `uri` (`amqp://user:pass@host:5672/%2f`).
    ///
    /// # Errors
    ///
    /// If the broker cannot be reached, or refuses the connection.
    pub async fn connect(uri: &str) -> Result<Self, AmqpError> {
        let connection = Connection::connect(uri, ConnectionProperties::default()).await?;
        Ok(Self::new(connection))
    }

    /// Uses `connection`, opened by the application (with its own
    /// properties, TLS or authentication).
    pub fn new(connection: Connection) -> Self {
        Self {
            connection: Arc::new(connection),
        }
    }

    /// The connection, for what this crate does not cover.
    pub fn connection(&self) -> &Connection {
        &self.connection
    }
}

/// Why the broker could not take or deliver a message.
#[derive(Debug)]
#[non_exhaustive]
pub enum AmqpError {
    /// The connection or channel failed, or the broker refused a method.
    Broker(lapin::Error),
    /// The broker refused the message (a negative publisher confirm).
    Refused {
        /// Where it was going.
        topic: String,
    },
}

impl fmt::Display for AmqpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Broker(error) => write!(f, "AMQP: {error}"),
            Self::Refused { topic } => write!(f, "the broker refused a message to `{topic}`"),
        }
    }
}

impl std::error::Error for AmqpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Broker(error) => Some(error),
            Self::Refused { .. } => None,
        }
    }
}

impl From<lapin::Error> for AmqpError {
    fn from(error: lapin::Error) -> Self {
        Self::Broker(error)
    }
}

/// Where a publisher sends a topic's messages.
#[derive(Clone, Debug)]
enum Route {
    /// To the fanout exchange named after the topic.
    Fanout,
    /// To the queue named after the topic (through the default exchange).
    Queue,
    /// To this topic exchange, the topic as routing key.
    Topic(String),
}

/// Publishes to RabbitMQ, with confirms: the [`Publish`] port that
/// [`Forward`](cqrs::processing::Forward) sends the log through.
///
/// What a topic needs (its exchange or queue) is declared, durable, the
/// first time it is published to.
#[derive(Clone, Debug)]
pub struct AmqpPublisher {
    /// Its channel, in confirm mode.
    channel: Channel,
    /// Where topics go.
    route: Route,
    /// The topics declared already.
    declared: Arc<Mutex<HashSet<String>>>,
}

impl AmqpPublisher {
    /// Publishes each topic to a fanout exchange of its name: every queue
    /// bound to it gets every message ([`AmqpSubscriber::fanout`]).
    ///
    /// # Errors
    ///
    /// If a channel cannot be opened.
    pub async fn fanout(amqp: &Amqp) -> Result<Self, AmqpError> {
        Self::open(amqp, Route::Fanout).await
    }

    /// Publishes each topic to a durable queue of its name, whose consumers
    /// compete for its messages ([`AmqpSubscriber::queue`]).
    ///
    /// # Errors
    ///
    /// If a channel cannot be opened.
    pub async fn queue(amqp: &Amqp) -> Result<Self, AmqpError> {
        Self::open(amqp, Route::Queue).await
    }

    /// Publishes to the topic exchange `exchange`, each topic as the routing
    /// key: queues bind to the topics they want by pattern
    /// ([`AmqpSubscriber::topic`]).
    ///
    /// # Errors
    ///
    /// If a channel cannot be opened, or the exchange declared.
    pub async fn topic(amqp: &Amqp, exchange: impl Into<String>) -> Result<Self, AmqpError> {
        let exchange = exchange.into();
        let publisher = Self::open(amqp, Route::Topic(exchange.clone())).await?;
        declare_exchange(&publisher.channel, &exchange, ExchangeKind::Topic).await?;
        Ok(publisher)
    }

    /// A publisher on a new channel in confirm mode.
    async fn open(amqp: &Amqp, route: Route) -> Result<Self, AmqpError> {
        let channel = amqp.connection.create_channel().await?;
        channel.confirm_select(ConfirmSelectOptions::default()).await?;
        Ok(Self {
            channel,
            route,
            declared: Arc::default(),
        })
    }

    /// Declares what `topic` is published to, once.
    async fn declare(&self, topic: &str) -> Result<(), AmqpError> {
        if self.declared.lock().unwrap_or_else(|e| e.into_inner()).contains(topic) {
            return Ok(());
        }
        match self.route {
            Route::Fanout => declare_exchange(&self.channel, topic, ExchangeKind::Fanout).await?,
            Route::Queue => declare_queue(&self.channel, topic).await?,
            Route::Topic(_) => {}
        }
        self.declared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(topic.to_owned());
        Ok(())
    }
}

impl Publish for AmqpPublisher {
    type Error = AmqpError;

    async fn publish(&self, message: Outgoing<'_>) -> Result<(), AmqpError> {
        self.declare(message.topic).await?;
        let (exchange, routing_key) = match &self.route {
            Route::Fanout => (message.topic, ""),
            Route::Queue => ("", message.topic),
            Route::Topic(exchange) => (exchange.as_str(), message.topic),
        };
        let confirmation = self
            .channel
            .basic_publish(
                exchange.into(),
                routing_key.into(),
                BasicPublishOptions::default(),
                message.payload,
                properties(&message),
            )
            .await?
            .await?;
        if confirmation.is_nack() {
            return Err(AmqpError::Refused {
                topic: message.topic.to_owned(),
            });
        }
        Ok(())
    }
}

/// The properties and headers carrying what `message` has besides its
/// payload.
fn properties(message: &Outgoing<'_>) -> BasicProperties {
    let mut headers = FieldTable::default();
    headers.insert(KEY.into(), AMQPValue::LongString(message.key.into()));
    headers.insert(VERSION.into(), AMQPValue::LongUInt(message.event_version));
    if !message.metadata.is_empty() {
        let mut metadata = FieldTable::default();
        for (key, value) in message.metadata {
            metadata.insert(key.as_str().into(), AMQPValue::LongString(value.as_str().into()));
        }
        headers.insert(METADATA.into(), AMQPValue::FieldTable(metadata));
    }
    BasicProperties::default()
        .with_delivery_mode(PERSISTENT)
        .with_type(message.event_type.into())
        .with_message_id(message.position.to_string().into())
        .with_headers(headers)
}

/// Consumes a RabbitMQ queue: the [`Subscribe`] port handing its messages,
/// in batches of up to the prefetch, to a [`Consume`]r.
///
/// The queue (and its exchange and binding) is declared, durable, when the
/// subscriber is made, so what is published from then on waits for it.
/// Each [`subscribe`](Subscribe::subscribe) opens its own channel; the
/// instances of a service subscribing to the same queue compete for its
/// messages.
#[derive(Clone, Debug)]
pub struct AmqpSubscriber {
    /// The connection channels are opened on.
    amqp: Amqp,
    /// The queue consumed.
    queue: String,
    /// The most messages in hand, unacknowledged, and so the largest batch.
    prefetch: u16,
    /// Whether a failed batch goes back to the queue (or is dead-lettered).
    requeue: bool,
}

impl AmqpSubscriber {
    /// Consumes `queue`, bound to the fanout exchange of `topic`
    /// ([`AmqpPublisher::fanout`]): every queue bound to it gets every
    /// message.
    ///
    /// # Errors
    ///
    /// If the exchange, queue or binding cannot be declared.
    pub async fn fanout(amqp: &Amqp, topic: &str, queue: impl Into<String>) -> Result<Self, AmqpError> {
        let queue = queue.into();
        let channel = amqp.connection.create_channel().await?;
        declare_exchange(&channel, topic, ExchangeKind::Fanout).await?;
        declare_queue(&channel, &queue).await?;
        bind(&channel, &queue, topic, "").await?;
        close(channel).await;
        Ok(Self::new(amqp, queue))
    }

    /// Consumes the queue of `topic` ([`AmqpPublisher::queue`]), competing
    /// with its other consumers.
    ///
    /// # Errors
    ///
    /// If the queue cannot be declared.
    pub async fn queue(amqp: &Amqp, topic: &str) -> Result<Self, AmqpError> {
        let channel = amqp.connection.create_channel().await?;
        declare_queue(&channel, topic).await?;
        close(channel).await;
        Ok(Self::new(amqp, topic.to_owned()))
    }

    /// Consumes `queue`, bound to the topic exchange `exchange`
    /// ([`AmqpPublisher::topic`]) for the topics matching `pattern` (`post.*`
    /// for one word, `post.#` for any).
    ///
    /// # Errors
    ///
    /// If the exchange, queue or binding cannot be declared.
    pub async fn topic(
        amqp: &Amqp,
        exchange: &str,
        pattern: &str,
        queue: impl Into<String>,
    ) -> Result<Self, AmqpError> {
        let queue = queue.into();
        let channel = amqp.connection.create_channel().await?;
        declare_exchange(&channel, exchange, ExchangeKind::Topic).await?;
        declare_queue(&channel, &queue).await?;
        bind(&channel, &queue, exchange, pattern).await?;
        close(channel).await;
        Ok(Self::new(amqp, queue))
    }

    /// A subscriber to `queue`, declared already.
    fn new(amqp: &Amqp, queue: String) -> Self {
        Self {
            amqp: amqp.clone(),
            queue,
            prefetch: 64,
            requeue: true,
        }
    }

    /// Holds at most `prefetch` messages unacknowledged (64 by default):
    /// the largest batch handed to the consumer.
    #[must_use]
    pub fn prefetch(mut self, prefetch: u16) -> Self {
        self.prefetch = prefetch.max(1);
        self
    }

    /// Rejects a failed batch without requeueing it: the broker drops it,
    /// or sends it to the queue's dead-letter exchange
    /// (`x-dead-letter-exchange`, set by a policy). By default a failed
    /// batch goes back to the queue, to be delivered again, after a pause
    /// (50 ms, doubling with each failure in a row, up to 5 s).
    #[must_use]
    pub fn dead_letter_failures(mut self) -> Self {
        self.requeue = false;
        self
    }

    /// The queue consumed.
    pub fn queue_name(&self) -> &str {
        &self.queue
    }
}

impl Subscribe for AmqpSubscriber {
    type Error = AmqpError;

    async fn subscribe<C: Consume>(
        &self,
        consumer: C,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), AmqpError> {
        let channel = self.amqp.connection.create_channel().await?;
        channel.basic_qos(self.prefetch, BasicQosOptions::default()).await?;
        let mut deliveries = channel
            .basic_consume(
                self.queue.as_str().into(),
                ShortString::from(""),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;
        let mut shutdown = std::pin::pin!(shutdown);
        let mut batch = Vec::with_capacity(usize::from(self.prefetch));
        let mut failures = 0;
        let result = loop {
            let first = tokio::select! {
                biased;
                () = &mut shutdown => break Ok(()),
                next = deliveries.next() => next,
            };
            match first {
                Some(Ok(delivery)) => batch.push(delivery),
                Some(Err(error)) => break Err(error.into()),
                None => break Ok(()),
            }
            // What else the broker has sent already, without waiting.
            while batch.len() < usize::from(self.prefetch) {
                match deliveries.next().now_or_never() {
                    Some(Some(Ok(delivery))) => batch.push(delivery),
                    _ => break,
                }
            }
            let messages: Vec<Incoming> = batch.iter().map(incoming).collect();
            let handled = consumer.consume(&messages).await;
            let Some(last) = batch.last() else { continue };
            // Up to the last delivery: the channel is this subscription's own.
            let mut stopping = false;
            let settled = match handled {
                Ok(()) => {
                    failures = 0;
                    last.acker.ack(BasicAckOptions { multiple: true }).await
                }
                Err(error) => {
                    tracing::warn!(queue = %self.queue, messages = batch.len(), %error, "a consumer failed a batch");
                    if self.requeue {
                        // Held a while before going back: a consumer failing
                        // every time does not have it redelivered in a loop.
                        failures += 1;
                        tokio::select! {
                            biased;
                            () = &mut shutdown => stopping = true,
                            () = tokio::time::sleep(backoff(failures)) => {}
                        }
                    }
                    let options = BasicNackOptions {
                        multiple: true,
                        requeue: self.requeue,
                    };
                    last.acker.nack(options).await
                }
            };
            if let Err(error) = settled {
                break Err(error.into());
            }
            batch.clear();
            if stopping {
                break Ok(());
            }
        };
        // Closing the channel returns what it still holds to the queue.
        close(channel).await;
        result
    }
}

/// How long a failed batch is held before it is requeued: 50 ms after the
/// first failure in a row, doubling up to 5 s.
fn backoff(failures: u32) -> Duration {
    const FIRST: Duration = Duration::from_millis(50);
    const MOST: Duration = Duration::from_secs(5);
    FIRST.saturating_mul(1 << failures.saturating_sub(1).min(16)).min(MOST)
}

/// What `delivery` carries, as the core sees it.
fn incoming(delivery: &Delivery) -> Incoming {
    // The publisher's topic: the routing key, or the fanout exchange.
    let topic = match delivery.routing_key.as_str() {
        "" => delivery.exchange.as_str(),
        key => key,
    };
    let mut message = Incoming::new(topic, delivery.data.clone());
    let properties = &delivery.properties;
    message.redelivered = delivery.redelivered;
    message.event_type = properties.kind().as_ref().map(|kind| kind.as_str().to_owned());
    message.position = properties.message_id().as_ref().and_then(|id| id.as_str().parse().ok());
    if let Some(headers) = properties.headers() {
        let headers = headers.inner();
        message.key = headers.get(KEY).and_then(text);
        if let Some(version) = headers.get(VERSION).and_then(number) {
            message.event_version = version;
        }
        if let Some(AMQPValue::FieldTable(metadata)) = headers.get(METADATA) {
            message.metadata = metadata
                .inner()
                .iter()
                .filter_map(|(key, value)| Some((key.as_str(), text(value)?)))
                .collect::<Metadata>();
        }
    }
    message
}

/// A header's text, sent as either string type.
fn text(value: &AMQPValue) -> Option<String> {
    match value {
        AMQPValue::LongString(text) => Some(String::from_utf8_lossy(text.as_bytes()).into_owned()),
        AMQPValue::ShortString(text) => Some(text.as_str().to_owned()),
        _ => None,
    }
}

/// A header's unsigned number, sent as any integer type that holds one.
fn number(value: &AMQPValue) -> Option<u32> {
    match *value {
        AMQPValue::LongUInt(n) => Some(n),
        AMQPValue::ShortUInt(n) => Some(u32::from(n)),
        AMQPValue::ShortShortUInt(n) => Some(u32::from(n)),
        AMQPValue::LongInt(n) => u32::try_from(n).ok(),
        AMQPValue::LongLongInt(n) => u32::try_from(n).ok(),
        _ => None,
    }
}

/// Declares the durable exchange `name` of `kind`.
async fn declare_exchange(channel: &Channel, name: &str, kind: ExchangeKind) -> Result<(), AmqpError> {
    let options = ExchangeDeclareOptions {
        durable: true,
        ..ExchangeDeclareOptions::default()
    };
    channel
        .exchange_declare(name.into(), kind, options, FieldTable::default())
        .await?;
    Ok(())
}

/// Declares the durable queue `name`.
async fn declare_queue(channel: &Channel, name: &str) -> Result<(), AmqpError> {
    let options = QueueDeclareOptions {
        durable: true,
        ..QueueDeclareOptions::default()
    };
    channel
        .queue_declare(name.into(), options, FieldTable::default())
        .await?;
    Ok(())
}

/// Binds `queue` to `exchange` for `routing_key`.
async fn bind(channel: &Channel, queue: &str, exchange: &str, routing_key: &str) -> Result<(), AmqpError> {
    channel
        .queue_bind(
            queue.into(),
            exchange.into(),
            routing_key.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    Ok(())
}

/// Closes `channel`; a failure to close one that is closing anyway is not
/// worth reporting.
async fn close(channel: Channel) {
    let _ = channel.close(200, "done".into()).await;
}
