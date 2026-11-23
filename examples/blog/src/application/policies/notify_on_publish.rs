use std::{fmt, future::Future, sync::Arc};

use cqrs::{EventEnvelope, EventHandler, HandlerError, ProcessingContext};

use crate::domain::PostEvent;

/// Port to whatever delivers notifications (e-mail, chat, ...).
pub trait Notifier: Send + Sync {
    /// Delivers `message` to the readers.
    fn notify(&self, message: String)
    -> impl Future<Output = Result<(), NotificationError>> + Send;
}

/// The notification could not be delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationError(pub String);

impl fmt::Display for NotificationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for NotificationError {}

/// Announces every post that gets published, in a processor of its own.
pub struct NotifyOnPublish<N> {
    /// Who delivers the announcement.
    notifier: Arc<N>,
}

impl<N> NotifyOnPublish<N> {
    /// A policy that announces through `notifier`.
    pub fn new(notifier: Arc<N>) -> Self {
        Self { notifier }
    }
}

#[cqrs::projection(namespace = "notify-on-publish")]
impl<N: Notifier> EventHandler for NotifyOnPublish<N> {
    /// A notification already went out: a replay must not send it again.
    const REPLAY: bool = false;

    /// Announces the post if `event` published it.
    async fn handle(&self, event: &EventEnvelope<PostEvent>, _: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
        if let PostEvent::Published = event.payload {
            self.notifier
                .notify(format!("{} published", event.stream))
                .await?;
        }
        Ok(())
    }
}
