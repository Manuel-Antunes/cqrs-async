use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

use crate::application::policies::{NotificationError, Notifier};

/// Records notifications instead of sending them. It can be switched off to
/// show that a failing policy neither undoes the commit nor blocks the other
/// event handlers.
#[derive(Default)]
pub struct InMemoryNotifier {
    /// Every message delivered so far, in order.
    sent: Mutex<Vec<String>>,
    /// When `true`, every delivery fails (simulates an outage).
    down: AtomicBool,
}

impl InMemoryNotifier {
    /// The messages delivered so far, oldest first.
    pub fn sent(&self) -> Vec<String> {
        self.sent.lock().expect("notifier poisoned").clone()
    }

    /// Simulates an outage (`true`) or a recovery (`false`) of the delivery
    /// channel.
    pub fn set_down(&self, down: bool) {
        self.down.store(down, Ordering::Relaxed);
    }
}

impl Notifier for InMemoryNotifier {
    async fn notify(&self, message: String) -> Result<(), NotificationError> {
        if self.down.load(Ordering::Relaxed) {
            return Err(NotificationError("mail server unavailable".into()));
        }
        self.sent.lock().expect("notifier poisoned").push(message);
        Ok(())
    }
}
