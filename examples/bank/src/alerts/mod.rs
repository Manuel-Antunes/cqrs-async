//! Another application: a compliance service that follows the bank's money
//! through RabbitMQ and raises an alert for every large withdrawal.
//!
//! It shares nothing with the bank but the broker and the shape of the
//! messages: its own log, its own aggregate, its own gateway. The bank's
//! events come in as messages; the consumer turns the ones that matter into
//! commands. Delivery is at least once, so the command is idempotent: an
//! alert's stream is keyed by the position of the bank's event, and a
//! second delivery finds it there already.

pub mod api;

use std::sync::Arc;

use cqrs::{
    aggregate::New,
    command::{CommandContext, CommandGateway, CommandHandler},
    processing::{Consume, Events, HandlerError, Incoming, Subscribe},
    store::{InMemoryEventStore, Json, ReadLog, StoreError},
};
use cqrs_amqp::{Amqp, AmqpError, AmqpSubscriber};
use serde::{Deserialize, Serialize};
use tokio::{sync::oneshot, task::JoinHandle};

use crate::banking::domain::AccountEvent;

/// The queue this service reads the bank's events from, bound to
/// [`ACCOUNTS_TOPIC`](crate::banking::ACCOUNTS_TOPIC): every instance of
/// the service shares it.
pub const QUEUE: &str = "compliance.withdrawals";

/// What happened in the compliance service.
#[derive(Clone, Debug, PartialEq, Eq, cqrs::DomainEvent)]
pub enum AlertEvent {
    /// A withdrawal of at least the threshold.
    #[event(name = "alert.raised")]
    Raised {
        /// The account.
        account: u32,
        /// How much was taken out, in cents.
        amount: u64,
    },
}

/// An alert, keyed by the position of the bank's event that raised it.
#[cqrs::aggregate(name = "alert", event = AlertEvent)]
#[derive(Debug)]
pub struct Alert {
    /// The bank event's position.
    pub id: u64,
}

impl Alert {
    fn apply(&mut self, _: &AlertEvent) {}
}

/// Why an alert was not raised.
#[derive(Debug, PartialEq, Eq)]
pub enum AlertError {
    /// Raised already: the bank's event was delivered again.
    AlreadyRaised,
    /// The store failed.
    Store(String),
}

impl std::fmt::Display for AlertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyRaised => f.write_str("raised already"),
            Self::Store(error) => write!(f, "store: {error}"),
        }
    }
}

impl std::error::Error for AlertError {}

impl From<StoreError> for AlertError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::Conflict { .. } => Self::AlreadyRaised,
            error => Self::Store(error.to_string()),
        }
    }
}

/// Raises an alert for the bank event at `source`.
#[cqrs::command(error = AlertError)]
#[derive(Clone, Debug)]
pub struct RaiseAlert {
    /// The bank event's position: the alert's id.
    pub source: u64,
    /// The account.
    pub account: u32,
    /// The amount withdrawn, in cents.
    pub amount: u64,
}

/// The alerts' log.
pub type AlertLog = Events<AlertEvent, InMemoryEventStore<AlertEvent>>;

/// The compliance service's commands.
#[cqrs::command_gateway(handles(RaiseAlert))]
#[derive(Debug)]
pub struct AlertGateway {
    /// The unit of work, over the alerts' log.
    #[gateway(unit)]
    pub events: AlertLog,
}

#[cqrs::command_handler]
impl CommandHandler<RaiseAlert> for AlertGateway {
    async fn handle(&self, command: &RaiseAlert, ctx: &mut CommandContext<AlertEvent>) -> Result<(), AlertError> {
        ctx.append(AlertEvent::Raised {
            account: command.account,
            amount: command.amount,
        });
        // A new stream per bank event: a second delivery conflicts.
        ctx.commit(New::<Alert>(command.source));
        Ok(())
    }
}

/// The inbound side: the bank's account events, as messages; the large
/// withdrawals become [`RaiseAlert`] commands.
#[derive(Debug)]
pub struct LargeWithdrawals {
    /// Where the commands go.
    pub gateway: Arc<AlertGateway>,
    /// From how much a withdrawal is large, in cents.
    pub threshold: u64,
}

impl Consume for LargeWithdrawals {
    async fn consume(&self, messages: &[Incoming]) -> Result<(), HandlerError> {
        for message in messages {
            // Only withdrawals matter here; the others are acknowledged.
            if message.event_type.as_deref() != Some("account.withdrawn") {
                continue;
            }
            let AccountEvent::Withdrawn { amount } = message.decode(&Json)? else {
                continue;
            };
            if amount < self.threshold {
                continue;
            }
            // A message this service cannot place would come back forever
            // if failed: it is logged and acknowledged instead.
            let (Some(source), Some(account)) = (message.position, account_of(message)) else {
                tracing::warn!(?message.key, "a withdrawal without position or account: skipped");
                continue;
            };
            // The bank's metadata (its correlation id) goes on with the alert.
            let command = RaiseAlert {
                source,
                account,
                amount,
            };
            match self.gateway.send_with(command, message.metadata.clone()).await {
                Ok(()) | Err(AlertError::AlreadyRaised) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

/// The account number in a message's key (its stream, `account-7`).
fn account_of(message: &Incoming) -> Option<u32> {
    message.key.as_deref()?.strip_prefix("account-")?.parse().ok()
}

/// An alert, as the service shows it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlertView {
    /// The account.
    pub account: u32,
    /// How much was withdrawn, in cents.
    pub amount: u64,
    /// The correlation id of the bank request that withdrew it, if any.
    pub correlation_id: Option<String>,
}

/// The compliance service, running: consuming its queue until stopped.
#[derive(Debug)]
pub struct Compliance {
    /// Its commands.
    pub gateway: Arc<AlertGateway>,
    /// Tells the subscription to stop.
    stop: oneshot::Sender<()>,
    /// The subscription.
    consuming: JoinHandle<Result<(), AmqpError>>,
}

impl Compliance {
    /// Starts the service: its log, its gateway, and its queue `queue`
    /// ([`QUEUE`] in production) bound to the bank's `topic` (declared now,
    /// so what the bank publishes from now on waits there).
    ///
    /// # Errors
    ///
    /// If the queue cannot be declared.
    pub async fn start(amqp: &Amqp, topic: &str, queue: &str, threshold: u64) -> Result<Self, AmqpError> {
        let gateway = Arc::new(AlertGateway {
            events: AlertLog::new(InMemoryEventStore::new()),
        });
        let subscriber = AmqpSubscriber::fanout(amqp, topic, queue).await?;
        let consumer = LargeWithdrawals {
            gateway: Arc::clone(&gateway),
            threshold,
        };
        let (stop, stopped) = oneshot::channel::<()>();
        let consuming = tokio::spawn(async move {
            subscriber
                .subscribe(consumer, async {
                    let _ = stopped.await;
                })
                .await
        });
        Ok(Self {
            gateway,
            stop,
            consuming,
        })
    }

    /// The alerts raised so far, in order.
    pub async fn alerts(&self) -> Vec<AlertView> {
        let events = self
            .gateway
            .events
            .store()
            .read_all(0, usize::MAX)
            .await
            .unwrap_or_default();
        events
            .into_iter()
            .map(|event| {
                let AlertEvent::Raised { account, amount } = event.payload;
                AlertView {
                    account,
                    amount,
                    correlation_id: event.metadata.get("correlation_id").cloned(),
                }
            })
            .collect()
    }

    /// Stops consuming; the batch in hand is finished first.
    ///
    /// # Errors
    ///
    /// If the subscription had failed.
    pub async fn stop(self) -> Result<(), AmqpError> {
        let _ = self.stop.send(());
        self.consuming.await.unwrap_or(Ok(()))
    }
}
