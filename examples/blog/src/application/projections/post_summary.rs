use std::sync::Arc;

use cqrs::{EventEnvelope, EventHandler, HandlerError, ProcessingContext};

use crate::{
    application::queries::{PostSummary, PostSummaryRepository, PostSummaryStore, PostUpdates},
    domain::{PostEvent, PostId},
};

/// Keeps the [`PostSummaryRepository`] read model in step with the posts' events,
/// and pushes the queries' new results once each change is committed
/// (Axon 5's projection: an event handling component with its dependencies,
/// in a processor named by its namespace).
///
/// Each event is applied once, in order: the processor commits the change
/// and its token in one unit of work, so there is no bookkeeping of what was
/// applied (as in Axon, with the token store next to the read model).
///
/// Generic over the read model's ports and over the unit of work it writes
/// in: the adapter (in memory, SeaORM in the processor's PostgreSQL
/// transaction) is the composition root's choice.
pub struct PostSummaryProjection<S> {
    /// Where the rows are kept.
    summaries: Arc<S>,
    /// Where the queries' new results go.
    updates: Arc<PostUpdates>,
}

impl<S> PostSummaryProjection<S> {
    /// A projection writing to `summaries`, pushing through `updates`.
    pub fn new(summaries: Arc<S>, updates: Arc<PostUpdates>) -> Self {
        Self { summaries, updates }
    }
}

#[cqrs::projection(namespace = "post-summaries")]
impl<S, Tx> EventHandler for PostSummaryProjection<S>
where
    S: PostSummaryRepository + PostSummaryStore<Tx>,
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

    /// Once committed: the post's new summary, and the new list of published
    /// posts if it changed.
    async fn committed(&self, event: &EventEnvelope<PostEvent>) {
        let Ok(id) = post_of(event) else { return };
        if let Ok(Some(summary)) = self.summaries.find(id).await {
            self.updates.find.emit(|query| query.id == id, Some(summary));
        }
        if matches!(event.payload, PostEvent::Published)
            && let Ok(published) = self.summaries.published().await
        {
            self.updates.published.emit(|_| true, published);
        }
    }

    /// Before a replay rebuilds it.
    async fn reset(&self, context: &mut ProcessingContext<'_, Tx>) -> Result<(), HandlerError> {
        self.summaries.clear(context.transaction()).await?;
        Ok(())
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
