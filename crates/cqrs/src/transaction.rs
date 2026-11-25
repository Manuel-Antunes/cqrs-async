//! Units of work: the [`TransactionManager`] port. It is the only component that opens and commits a transaction;
//! token stores, dead-letter queues and event handlers take part in one by
//! borrowing it (`&mut Tx`), so none of them can commit it, keep it, or
//! choose its type.

use std::future::{Future, ready};

use crate::StoreError;

/// Opens and commits the units of work a tracking processor handles events
/// in.
///
/// Ownership is the protocol: [`begin`](Self::begin) hands a transaction
/// out, the participants borrow it, and [`commit`](Self::commit) consumes
/// it. A transaction dropped without a commit is rolled back, and an event handler's
/// [`committed`](crate::EventHandler::committed) runs once the commit
/// returned (its after-commit phase).
///
/// The processor checks at compile time that its token store, its
/// dead-letter queue and its handler all take part in the same `Tx`: a read
/// model written in a database transaction cannot be run with tokens kept
/// elsewhere.
///
/// Implemented for `()`, the unit of work of everything in memory: there is
/// nothing to open nor to roll back. A database adapter implements it for
/// its pool (`cqrs_postgres::PostgresTransactions`); an application can
/// implement it for its own.
pub trait TransactionManager: Send + Sync {
    /// The transaction participants borrow.
    type Tx: Send;

    /// Whether dropping a transaction undoes what was written in it (a
    /// database's does; memory's `()` does not). A command's unit of work
    /// runs its handler again after a lost race only if nothing it wrote
    /// can survive.
    const ROLLS_BACK: bool = true;

    /// Opens a transaction.
    fn begin(&self) -> impl Future<Output = Result<Self::Tx, StoreError>> + Send;

    /// Commits `tx`: what the participants wrote in it becomes visible
    /// together.
    fn commit(&self, tx: Self::Tx) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// In memory: nothing to open, nothing to roll back.
impl TransactionManager for () {
    type Tx = ();

    const ROLLS_BACK: bool = false;

    fn begin(&self) -> impl Future<Output = Result<(), StoreError>> + Send {
        ready(Ok(()))
    }

    fn commit(&self, (): ()) -> impl Future<Output = Result<(), StoreError>> + Send {
        ready(Ok(()))
    }
}
