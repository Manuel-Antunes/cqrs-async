use std::{collections::BTreeMap, sync::Mutex};

use cqrs::StoreError;

use crate::{
    application::queries::{PostSummary, PostSummaryRepository, PostSummaryStore},
    domain::PostId,
};

/// The read model in memory: one row per post. Only storage: what the rows
/// hold is the
/// [`PostSummaryProjection`](crate::application::projections::PostSummaryProjection)'s
/// business. Its unit of work is `()`: there is nothing to roll back.
#[derive(Default)]
pub struct InMemoryPostSummaryRepository {
    /// Rows by id (so listings come out sorted).
    rows: Mutex<BTreeMap<PostId, PostSummary>>,
}

impl InMemoryPostSummaryRepository {
    /// Locks the rows. A poisoned lock means a previous write panicked
    /// halfway, so the rows can no longer be trusted: panic too.
    fn rows(&self) -> std::sync::MutexGuard<'_, BTreeMap<PostId, PostSummary>> {
        self.rows.lock().expect("read model poisoned")
    }
}

impl PostSummaryRepository for InMemoryPostSummaryRepository {
    async fn find(&self, id: PostId) -> Result<Option<PostSummary>, StoreError> {
        Ok(self.rows().get(&id).cloned())
    }

    async fn published(&self) -> Result<Vec<PostSummary>, StoreError> {
        Ok(self
            .rows()
            .values()
            .filter(|summary| summary.published)
            .cloned()
            .collect())
    }
}

impl PostSummaryStore<()> for InMemoryPostSummaryRepository {
    async fn save(&self, (): &mut (), summary: PostSummary) -> Result<(), StoreError> {
        self.rows().insert(summary.id, summary);
        Ok(())
    }

    async fn update(
        &self,
        (): &mut (),
        id: PostId,
        change: impl FnOnce(&mut PostSummary) + Send,
    ) -> Result<bool, StoreError> {
        Ok(self.rows().get_mut(&id).map(change).is_some())
    }

    async fn clear(&self, (): &mut ()) -> Result<(), StoreError> {
        self.rows().clear();
        Ok(())
    }
}
