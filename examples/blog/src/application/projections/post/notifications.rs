//! The `notifications` namespace: a side effect, announcing what gets
//! published.

use std::{fmt, future::Future};

use cqrs::{
    event::EventEnvelope,
    processing::{EventHandler, HandlerError, ProcessingContext},
};

use super::PostProjection;
use crate::domain::PostEvent;

/// Port to whatever delivers notifications (e-mail, chat, ...).
pub trait Notifier: Send + Sync {
    /// Delivers `message` to the readers.
    fn notify(&self, message: String) -> impl Future<Output = Result<(), NotificationError>> + Send;
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

/// Announces every post that gets published. A notification already went
/// out: a replay must not send it again, so it does not see replays; and
/// it is not undone with a transaction, so it takes events one at a time
/// (no `batch`): a failing batch, handled again, would send it twice.
#[cqrs::event_handler(namespace = "notifications")]
impl<S: Send + Sync, N: Notifier> EventHandler for PostProjection<S, N> {
    const REPLAY: bool = false;

    /// Announces the post if `event` published it.
    async fn handle(
        &self,
        event: &EventEnvelope<PostEvent>,
        _: &mut ProcessingContext<'_>,
    ) -> Result<(), HandlerError> {
        if let PostEvent::Published = event.payload {
            self.notifier.notify(format!("{} published", event.stream)).await?;
        }
        Ok(())
    }
}
