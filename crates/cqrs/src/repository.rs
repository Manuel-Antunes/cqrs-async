//! The contract of a repository for command handlers: [`Repository`] finds an
//! aggregate, and saves its state with what a run of a handler decided.
//!
//! It is optional. The application can declare its own port with the finders
//! its use cases need; making it a [`Repository`] too gives its handlers
//! `find` and `save`, both working in the unit's transaction.

use std::{future::Future, sync::Arc};

use crate::{Aggregate, Loaded, StoreError};

/// Where the command handlers find the aggregate `A` and keep its state.
///
/// An implementation writes [`find`](Self::find) and, if it keeps the
/// aggregate's state, [`save`](Self::save): both take only the transaction
/// of the unit of work. A handler lends them the one its context owns, lets
/// the domain append events to the context, [commits](crate::command::CommandContext::commit)
/// them to the aggregate and saves it:
///
/// ```text
/// let post = self.posts.find(&command.id, ctx.tx()).await?.ok_or(NotFound(command.id))?;
/// post.publish(ctx)?;
/// let post = ctx.commit(post);
/// self.posts.save(&post, ctx.tx()).await?;
/// ```
///
/// The commit comes first because what is saved is the aggregate after its
/// events, at its new version. It is not a database commit: nothing is
/// written by it, and the unit of work appends the events and commits the
/// transaction once the handler returns, so what `save` wrote and the events
/// commit together, or not at all.
///
/// `save` writes nothing by default: a repository that rebuilds the
/// aggregate from its events (they are its state, which
/// [`AggregateLoader`](crate::AggregateLoader) is) has nothing to keep, and a
/// handler that uses one alone may skip it. A handler generic over the
/// repository calls both, and whatever the repository is, it works.
///
/// The application's own port can have this trait as a supertrait, and add its
/// finders (a next id, a lookup by title) to it.
pub trait Repository<A: Aggregate>: Send + Sync {
    /// The transaction of the units of work it takes part in (`()`: none).
    type Tx: Send;

    /// The aggregate `id`, at the version it was read at (`None`: it does
    /// not exist), read in `tx`.
    ///
    /// # Errors
    ///
    /// The store's.
    fn find(&self, id: &A::Id, tx: &mut Self::Tx)
    -> impl Future<Output = Result<Option<Loaded<A>>, StoreError>> + Send;

    /// Keeps `entity`, as a run left it (after its committed events, at their
    /// version), in `tx`. By default nothing: its events are its state.
    ///
    /// # Errors
    ///
    /// The store's.
    fn save(&self, entity: &Loaded<A>, tx: &mut Self::Tx) -> impl Future<Output = Result<(), StoreError>> + Send {
        let _ = (entity, tx);
        async { Ok(()) }
    }
}

/// A shared repository is one.
// Forwards every method, so an override of a provided one is never lost.
#[deny(clippy::missing_trait_methods)]
impl<A: Aggregate, R: Repository<A>> Repository<A> for Arc<R> {
    type Tx = R::Tx;

    fn find(
        &self,
        id: &A::Id,
        tx: &mut Self::Tx,
    ) -> impl Future<Output = Result<Option<Loaded<A>>, StoreError>> + Send {
        (**self).find(id, tx)
    }

    fn save(&self, entity: &Loaded<A>, tx: &mut Self::Tx) -> impl Future<Output = Result<(), StoreError>> + Send {
        (**self).save(entity, tx)
    }
}
