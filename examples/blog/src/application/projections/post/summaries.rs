//! The `summaries` namespace: the read model the queries answer from.

use cqrs::{
    event::EventEnvelope,
    processing::{EventHandler, HandlerError, ProcessingContext},
    query::QueryUpdateEmitter,
};

use super::PostProjection;
use crate::{
    application::queries::{FindPost, ListPublished, PostSummary, PostSummaryRepository, PostSummaryStore},
    domain::{PostEvent, PostId},
};

/// Keeps the [`PostSummaryRepository`] read model in step with the posts'
/// events. Each event is applied once, in order: the processor commits the
/// change and its token in one unit of work, so there is no bookkeeping of
/// what was applied (as in Axon, with the token store next to the read
/// model). Generic over the unit of work it writes in: the adapter (in
/// memory, SeaORM in the processor's PostgreSQL transaction) is the
/// composition root's choice.
///
/// Everything it does is undone with its transaction, so it takes events by
/// batches of 64 (one transaction and one token update for all of them): a
/// rebuild of 5000 events took 0.25 s instead of 4.6 s. A failing batch is
/// rolled back and handled again one event at a time.
#[cqrs::event_handler(namespace = "summaries", batch = 64)]
impl<S, N, Tx> EventHandler for PostProjection<S, N>
where
    S: PostSummaryRepository + PostSummaryStore<Tx>,
    N: Send + Sync,
    Tx: Send,
{
    async fn handle(
        &self,
        event: &EventEnvelope<PostEvent>,
        context: &mut ProcessingContext<'_, Tx>,
    ) -> Result<(), HandlerError> {
        let tx = context.transaction();
        let found = match &event.payload {
            PostEvent::Created { id, title } => {
                let summary = PostSummary {
                    id: *id,
                    title: title.clone(),
                    published: false,
                };
                self.summaries.save(tx, summary).await?;
                true
            }
            PostEvent::Retitled { title } => {
                self.summaries
                    .update(tx, post_of(event)?, |s| s.title.clone_from(title))
                    .await?
            }
            PostEvent::Published => {
                self.summaries
                    .update(tx, post_of(event)?, |s| s.published = true)
                    .await?
            }
        };
        if !found {
            // Events of a stream arrive in order, its creation first.
            return Err(HandlerError::msg(format!(
                "{} has no summary: its creation was not projected",
                event.stream
            )));
        }
        Ok(())
    }

    /// Before a replay rebuilds it.
    async fn reset(&self, context: &mut ProcessingContext<'_, Tx>) -> Result<(), HandlerError> {
        self.summaries.clear(context.transaction()).await?;
        Ok(())
    }

    /// Pushes the summary queries' new results, on every instance, once
    /// this namespace handled the event (whichever instance did): the
    /// post's new summary, and the new list of published posts if it
    /// changed.
    async fn updated(&self, event: &EventEnvelope<PostEvent>, updates: &QueryUpdateEmitter<Self>) {
        let Ok(id) = post_of(event) else { return };
        if let Ok(Some(summary)) = self.summaries.find(id).await {
            updates.emit(|query: &FindPost| query.id == id, Some(summary));
        }
        if matches!(event.payload, PostEvent::Published)
            && let Ok(published) = self.summaries.published().await
        {
            updates.emit(|_: &ListPublished| true, published);
        }
    }
}

/// The post a stream belongs to (its id is the stream's).
fn post_of(event: &EventEnvelope<PostEvent>) -> Result<PostId, HandlerError> {
    event
        .stream
        .id()
        .parse()
        .map_err(|_| HandlerError::msg(format!("not a post stream: {}", event.stream)))
}
