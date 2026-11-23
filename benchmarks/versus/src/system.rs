//! What a scenario needs from a system: both frameworks implement it, and
//! the scenarios are generic over it (statically dispatched).

use std::future::Future;

use crate::bank::{BankCommand, BankError, Balances};

pub trait System: Send + Sync + 'static {
    /// One attempt at `command`: a lost race comes back as
    /// [`BankError::Conflict`] (the harness retries).
    fn execute(&self, account: &str, command: BankCommand) -> impl Future<Output = Result<(), BankError>> + Send;

    /// Returns once every committed event reached the read model and the
    /// transfer policy (and the commands it sent, theirs too).
    fn settle(&self) -> impl Future<Output = ()> + Send;

    /// The read model.
    fn views(&self) -> impl Future<Output = Balances> + Send;

    /// The balances the event log says.
    fn truth(&self) -> impl Future<Output = Balances> + Send;

    /// Clears the read model and rebuilds it from every event; returns how
    /// many events were replayed.
    fn rebuild(&self) -> impl Future<Output = u64> + Send;

    /// Writes `deposits` deposits straight into the log of an opened
    /// `account` (not through commands), for long-history scenarios.
    fn seed(&self, account: &str, deposits: usize) -> impl Future<Output = ()> + Send;

    /// Stops what runs in the background and closes the connections, so
    /// the next system starts on a quiet database.
    fn close(&self) -> impl Future<Output = ()> + Send;
}
