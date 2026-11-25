//! Against a real RabbitMQ: set `CQRS_AMQP_URL` (skipped otherwise).
//!
//! ```text
//! docker run -d --name cqrs-rabbit -p 55672:5672 rabbitmq:4-alpine
//! CQRS_AMQP_URL=amqp://127.0.0.1:55672/%2f cargo test -p cqrs-async-amqp
//! ```
//!
//! Each test uses exchanges and queues of its own, deleted at the end, so
//! they run in parallel.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use cqrs::{
    command::{CommandContext, CommandGateway, CommandHandler},
    event::{Metadata, StreamId},
    processing::{Consume, Events, Forward, HandlerError, Incoming, InlinePublisher, Outgoing, Publish, Subscribe},
    store::{AppendEvents, ExpectedVersion, InMemoryEventStore, Json, ReadLog, StoreError},
};
use cqrs_amqp::{Amqp, AmqpPublisher, AmqpSubscriber};
use tokio::sync::oneshot;

/// The broker, and a prefix no other test (or run) uses; `None` when there
/// is no broker to test against.
async fn broker() -> Option<(Amqp, String)> {
    let url = std::env::var("CQRS_AMQP_URL").ok()?;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let run = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros();
    let prefix = format!("cqrs-test-{run}-{}", NEXT.fetch_add(1, Ordering::Relaxed));
    let amqp = Amqp::connect(&url).await.expect("connecting to RabbitMQ");
    Some((amqp, prefix))
}

/// Deletes what a test declared.
async fn clean(amqp: &Amqp, exchanges: &[&str], queues: &[&str]) {
    let channel = amqp.connection().create_channel().await.unwrap();
    for queue in queues {
        let _ = channel
            .queue_delete(
                (*queue).into(),
                cqrs_amqp::lapin::options::QueueDeleteOptions::default(),
            )
            .await;
    }
    for exchange in exchanges {
        let _ = channel
            .exchange_delete(
                (*exchange).into(),
                cqrs_amqp::lapin::options::ExchangeDeleteOptions::default(),
            )
            .await;
    }
}

/// A consumer keeping what it was given, failing the first `failures`
/// batches.
#[derive(Default)]
struct Collect {
    seen: Mutex<Vec<Incoming>>,
    failures: Mutex<u32>,
}

impl Consume for Collect {
    async fn consume(&self, messages: &[Incoming]) -> Result<(), HandlerError> {
        let failing = {
            let mut failures = self.failures.lock().unwrap();
            let failing = *failures > 0;
            *failures = failures.saturating_sub(1);
            failing
        };
        self.seen.lock().unwrap().extend_from_slice(messages);
        if failing {
            return Err(HandlerError::msg("not now"));
        }
        Ok(())
    }
}

impl Collect {
    /// What was seen, once it is `n` messages (or after 10 s).
    async fn wait_for(&self, n: usize) -> Vec<Incoming> {
        for _ in 0..1000 {
            if self.seen.lock().unwrap().len() >= n {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        self.seen.lock().unwrap().clone()
    }
}

/// Subscribes `consumer` in a task, until the returned sender is used.
fn spawn(subscriber: AmqpSubscriber, consumer: Arc<Collect>) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let (stop, stopped) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        subscriber
            .subscribe(consumer, async {
                let _ = stopped.await;
            })
            .await
            .expect("subscribed until told to stop");
    });
    (stop, task)
}

/// A message as `Forward` would hand it.
fn outgoing<'a>(topic: &'a str, payload: &'a [u8], position: u64, metadata: &'a Metadata) -> Outgoing<'a> {
    Outgoing {
        topic,
        key: "post-1",
        event_type: "post.published",
        event_version: 2,
        position,
        payload,
        metadata,
    }
}

#[tokio::test]
async fn a_fanout_topic_reaches_every_bound_queue_with_what_was_published() {
    let Some((amqp, prefix)) = broker().await else { return };
    let topic = format!("{prefix}-posts");
    let (search, mail) = (format!("{prefix}-search"), format!("{prefix}-mail"));
    // The queues exist before the first message: nothing is missed.
    let search_queue = AmqpSubscriber::fanout(&amqp, &topic, &search).await.unwrap();
    let mail_queue = AmqpSubscriber::fanout(&amqp, &topic, &mail).await.unwrap();

    let publisher = AmqpPublisher::fanout(&amqp).await.unwrap();
    let metadata = Metadata::from([("correlation", "c-1"), ("user", "ana")]);
    publisher
        .publish(outgoing(&topic, b"{\"id\":1}", 41, &metadata))
        .await
        .unwrap();

    let (search_seen, mail_seen) = (Arc::new(Collect::default()), Arc::new(Collect::default()));
    let (stop_search, searching) = spawn(search_queue, Arc::clone(&search_seen));
    let (stop_mail, mailing) = spawn(mail_queue, Arc::clone(&mail_seen));
    let (search_got, mail_got) = (search_seen.wait_for(1).await, mail_seen.wait_for(1).await);
    let _ = (stop_search.send(()), stop_mail.send(()));
    let _ = (searching.await, mailing.await);

    assert_eq!(search_got, mail_got, "every queue gets the message");
    let message = &search_got[0];
    assert_eq!(message.topic, topic);
    assert_eq!(message.key.as_deref(), Some("post-1"));
    assert_eq!(message.event_type.as_deref(), Some("post.published"));
    assert_eq!(message.event_version, 2);
    assert_eq!(message.position, Some(41));
    assert_eq!(message.payload, b"{\"id\":1}");
    assert_eq!(message.metadata, metadata);
    assert!(!message.redelivered);
    clean(&amqp, &[&topic], &[&search, &mail]).await;
}

#[tokio::test]
async fn a_topic_exchange_routes_by_pattern() {
    let Some((amqp, prefix)) = broker().await else { return };
    let exchange = format!("{prefix}-events");
    let queue = format!("{prefix}-posts");
    let posts = AmqpSubscriber::topic(&amqp, &exchange, "post.*", &queue).await.unwrap();
    let publisher = AmqpPublisher::topic(&amqp, &exchange).await.unwrap();
    let metadata = Metadata::new();
    for (position, topic) in [(1, "user.created"), (2, "post.published"), (3, "post.retitled")] {
        publisher
            .publish(outgoing(topic, b"{}", position, &metadata))
            .await
            .unwrap();
    }

    let seen = Arc::new(Collect::default());
    let (stop, task) = spawn(posts, Arc::clone(&seen));
    let got = seen.wait_for(2).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = stop.send(());
    let _ = task.await;
    let topics: Vec<_> = seen.seen.lock().unwrap().iter().map(|m| m.topic.clone()).collect();
    assert_eq!(topics, ["post.published", "post.retitled"], "{got:?}");
    clean(&amqp, &[&exchange], &[&queue]).await;
}

#[tokio::test]
async fn a_failed_batch_is_delivered_again_or_dead_lettered() {
    let Some((amqp, prefix)) = broker().await else { return };
    let publisher = AmqpPublisher::queue(&amqp).await.unwrap();
    let metadata = Metadata::new();

    // Requeued: the same message comes back, marked as redelivered.
    let retried = format!("{prefix}-retried");
    let subscriber = AmqpSubscriber::queue(&amqp, &retried).await.unwrap();
    publisher.publish(outgoing(&retried, b"1", 1, &metadata)).await.unwrap();
    let seen = Arc::new(Collect::default());
    *seen.failures.lock().unwrap() = 1;
    let (stop, task) = spawn(subscriber, Arc::clone(&seen));
    let got = seen.wait_for(2).await;
    let _ = stop.send(());
    let _ = task.await;
    assert_eq!(got.len(), 2);
    assert_eq!((got[0].position, got[0].redelivered), (Some(1), false));
    assert_eq!((got[1].position, got[1].redelivered), (Some(1), true));

    // Dead-lettered: rejected, not requeued; the next message is handled.
    let dropped = format!("{prefix}-dropped");
    let subscriber = AmqpSubscriber::queue(&amqp, &dropped)
        .await
        .unwrap()
        .prefetch(1)
        .dead_letter_failures();
    publisher.publish(outgoing(&dropped, b"1", 1, &metadata)).await.unwrap();
    publisher.publish(outgoing(&dropped, b"2", 2, &metadata)).await.unwrap();
    let seen = Arc::new(Collect::default());
    *seen.failures.lock().unwrap() = 1;
    let (stop, task) = spawn(subscriber, Arc::clone(&seen));
    seen.wait_for(2).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = stop.send(());
    let _ = task.await;
    let positions: Vec<_> = seen.seen.lock().unwrap().iter().map(|m| m.position).collect();
    assert_eq!(positions, [Some(1), Some(2)], "message 1 is not delivered again");
    clean(&amqp, &[], &[&retried, &dropped]).await;
}

#[tokio::test]
async fn messages_in_hand_at_shutdown_go_back_to_the_queue() {
    let Some((amqp, prefix)) = broker().await else { return };
    let queue = format!("{prefix}-orders");
    let publisher = AmqpPublisher::queue(&amqp).await.unwrap();
    let subscriber = AmqpSubscriber::queue(&amqp, &queue).await.unwrap().prefetch(1);
    let metadata = Metadata::new();
    for position in 1..=3 {
        publisher
            .publish(outgoing(&queue, b"{}", position, &metadata))
            .await
            .unwrap();
    }
    // One consumer takes the first message, failing it, and stops.
    let first = Arc::new(Collect::default());
    *first.failures.lock().unwrap() = u32::MAX;
    let (stop, task) = spawn(subscriber.clone(), Arc::clone(&first));
    first.wait_for(1).await;
    let _ = stop.send(());
    let _ = task.await;

    let second = Arc::new(Collect::default());
    let (stop, task) = spawn(subscriber, Arc::clone(&second));
    let got = second.wait_for(3).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = stop.send(());
    let _ = task.await;
    let mut positions: Vec<_> = got.iter().filter_map(|m| m.position).collect();
    positions.sort_unstable();
    positions.dedup();
    assert_eq!(positions, [1, 2, 3], "nothing is lost");
    clean(&amqp, &[], &[&queue]).await;
}

// The "done" criterion of the plan: one service's log, forwarded to
// RabbitMQ as its outbox, consumed by another service that sends commands
// to its gateway.

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, cqrs::DomainEvent)]
enum PostEvent {
    Published { id: u32, title: String },
}

#[derive(Clone, Debug, PartialEq, cqrs::DomainEvent)]
enum NoticeEvent {
    Sent { post: u32, title: String },
}

#[cqrs::aggregate(name = "notice", event = NoticeEvent)]
struct Notice {
    #[aggregate(id)]
    post: u32,
}

impl Notice {
    fn apply(&mut self, _: &NoticeEvent) {}
}

#[derive(Debug, PartialEq)]
enum NoticeError {
    AlreadySent,
    Store(String),
}

impl std::fmt::Display for NoticeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for NoticeError {}

impl From<StoreError> for NoticeError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::Conflict { .. } => Self::AlreadySent,
            error => Self::Store(error.to_string()),
        }
    }
}

/// Sends the notice of a published post, once.
#[cqrs::command(error = NoticeError)]
struct Notify {
    post: u32,
    title: String,
}

type Notices = InMemoryEventStore<NoticeEvent>;

#[cqrs::command_gateway(handles(Notify))]
struct Notifications {
    #[gateway(unit)]
    events: InlinePublisher<NoticeEvent, Notices>,
}

#[cqrs::command_handler]
impl CommandHandler<Notify> for Notifications {
    async fn handle(&self, command: &Notify, ctx: &mut CommandContext<NoticeEvent>) -> Result<(), NoticeError> {
        ctx.append(NoticeEvent::Sent {
            post: command.post,
            title: command.title.clone(),
        });
        // A new stream per post: a redelivered message conflicts.
        ctx.record(cqrs::aggregate::New::<Notice>(command.post));
        Ok(())
    }
}

/// The inbound side of the notifications service: each published post
/// becomes a `Notify` command.
struct OnPostPublished {
    gateway: Arc<Notifications>,
}

impl Consume for OnPostPublished {
    async fn consume(&self, messages: &[Incoming]) -> Result<(), HandlerError> {
        for message in messages {
            let PostEvent::Published { id, title } = message.decode(&Json)?;
            match self.gateway.send(Notify { post: id, title }).await {
                // Delivered twice: already done.
                Ok(()) | Err(NoticeError::AlreadySent) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn the_log_is_forwarded_to_rabbitmq_and_consumed_back_as_commands() {
    let Some((amqp, prefix)) = broker().await else { return };
    let topic = format!("{prefix}-posts");
    let queue = format!("{prefix}-notifications");

    // The notifications service: its queue, bound to the posts' topic.
    let notices = Notices::new();
    let gateway = Arc::new(Notifications {
        events: InlinePublisher::new(notices.clone()),
    });
    let subscriber = AmqpSubscriber::fanout(&amqp, &topic, &queue).await.unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let consumer = OnPostPublished {
        gateway: Arc::clone(&gateway),
    };
    let consuming = tokio::spawn(async move {
        subscriber
            .subscribe(consumer, async {
                let _ = stopped.await;
            })
            .await
    });

    // The blog: its log, forwarded by a tracking processor.
    let posts = Events::new(InMemoryEventStore::<PostEvent>::new());
    let publisher = AmqpPublisher::fanout(&amqp).await.unwrap();
    let outbox = posts.track("outbox", Forward::new(publisher.clone(), Json, topic.as_str()));
    let mut last = 0;
    for (id, title) in [(1, "Hello"), (2, "Again")] {
        let committed = posts
            .store()
            .append(
                &mut (),
                &StreamId::new("post", id.to_string()),
                ExpectedVersion::NoStream,
                vec![PostEvent::Published {
                    id,
                    title: title.to_owned(),
                }],
                &Metadata::new(),
            )
            .await
            .unwrap();
        last = committed.last().unwrap().position;
    }
    outbox.caught_up(last).await;
    outbox.stop().await;
    // At least once: the same event published again is not sent twice.
    let published_again = br#"{"Published":{"id":1,"title":"Hello"}}"#;
    publisher
        .publish(outgoing(&topic, published_again, 1, &Metadata::new()))
        .await
        .unwrap();

    for _ in 0..1000 {
        if notices.read_all(0, 10).await.unwrap().len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = stop.send(());
    consuming.await.unwrap().unwrap();

    let sent: Vec<_> = notices
        .read_all(0, 10)
        .await
        .unwrap()
        .into_iter()
        .map(|event| event.payload)
        .collect();
    assert_eq!(
        sent,
        [
            NoticeEvent::Sent {
                post: 1,
                title: "Hello".to_owned()
            },
            NoticeEvent::Sent {
                post: 2,
                title: "Again".to_owned()
            },
        ]
    );
    clean(&amqp, &[&topic], &[&queue]).await;
}
