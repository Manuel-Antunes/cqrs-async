//! Progress of event processors: the [`TokenStore`] port (Axon's token
//! store). Per processor, the event log is split into segments; each segment
//! has a token and is claimed by at most one owner at a time, so several
//! instances share the work without handling an event twice. A token is a
//! [`Cursor`]: how far the segment got, and the positions it skipped because
//! they were still being written.

use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
};

use crate::{Cursor, StoreError};

/// Where processors keep, per segment, how far they got and who owns it.
///
/// Claims expire: an instance that dies stops renewing its claims and,
/// once they expire, another one takes the segments over from their stored
/// positions. Times are milliseconds on a clock the caller provides (a SQL
/// adapter may use the database's own clock instead).
///
/// Moving a token past an event is done in the unit of work the event was
/// handled in ([`store`](Self::store) borrows its `Tx`), so a read model in
/// the same database commits with its token: each event is applied once.
/// Claims and resets are bookkeeping of their own, outside any unit of work.
pub trait TokenStore: Send + Sync {
    /// The unit of work tokens are moved in: that of the
    /// [`TransactionManager`](crate::TransactionManager) the processor runs
    /// with (`()` in memory, a database transaction in SQL).
    type Tx: Send;

    /// Gives `processor` `segments` segments at the start, unless it
    /// already has some. Returns how many segments it has: the count is
    /// fixed by whoever initializes first.
    fn initialize(&self, processor: &str, segments: u32) -> impl Future<Output = Result<u32, StoreError>> + Send;

    /// Claims `segment` for `owner` until `until`, if it is unclaimed, its
    /// claim expired (`until` before `now`) or `owner` already holds it (then
    /// this renews the claim). Returns the segment's cursor, or `None` if
    /// someone else holds it.
    fn claim(
        &self,
        processor: &str,
        segment: u32,
        owner: &str,
        now: u64,
        until: u64,
    ) -> impl Future<Output = Result<Option<Cursor>, StoreError>> + Send;

    /// Moves `token`'s segment to its cursor within `tx`, if its owner still
    /// holds it. `false`: the claim was lost; the caller drops `tx` (rolling
    /// back what was handled in it) and stops processing the segment.
    fn store(
        &self,
        tx: &mut Self::Tx,
        token: TrackingToken<'_>,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Gives `segment` up, if `owner` holds it.
    fn release(
        &self,
        processor: &str,
        segment: u32,
        owner: &str,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// The stored cursor of each of `processor`'s segments, whoever owns
    /// them: the event at a position was handled, by some instance, once
    /// they all cover it. The progress of the processor as a whole.
    fn cursors(&self, processor: &str) -> impl Future<Output = Result<Vec<Cursor>, StoreError>> + Send;

    /// Moves every segment of `processor` back to the start and marks the
    /// events up to `replay_until` as a replay, unless some segment is
    /// claimed (a claim expired before `now` does not count). `false`: an
    /// instance still runs it.
    fn reset(
        &self,
        processor: &str,
        now: u64,
        replay_until: u64,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Up to where events are a replay since the last [`reset`](Self::reset)
    /// (0: never reset).
    fn replay_until(&self, processor: &str) -> impl Future<Output = Result<u64, StoreError>> + Send;
}

/// Which token a tracking processor moves past an event, in the same unit
/// of work as the handler's changes ([`TokenStore::store`]): Axon's token
/// stored with the projection.
#[derive(Clone, Copy, Debug)]
pub struct TrackingToken<'a> {
    /// The processor's name.
    pub processor: &'a str,
    /// The segment the event belongs to.
    pub segment: u32,
    /// The instance holding the segment's claim: the token only moves if it
    /// still does.
    pub owner: &'a str,
    /// Whether the event is replayed (the processor was
    /// [reset](crate::TrackingProcessor::reset) since it was first handled).
    pub replay: bool,
    /// The segment's cursor once the event is handled: what to store.
    pub cursor: &'a Cursor,
}

/// One segment's token.
#[derive(Clone, Debug, Default)]
struct Token {
    /// Everything it saw was handled.
    cursor: Cursor,
    /// Who holds the segment, and until when.
    claim: Option<(String, u64)>,
    /// Events up to here are a replay.
    replay_until: u64,
}

/// Tokens kept in memory: shared by the processors of one process. A
/// handle: clones share the same tokens.
#[derive(Clone, Default)]
pub struct InMemoryTokenStore {
    /// Segments per processor name.
    processors: Arc<Mutex<HashMap<String, Vec<Token>>>>,
}

impl InMemoryTokenStore {
    /// The stored position of each segment of `processor`.
    pub fn positions(&self, processor: &str) -> Vec<u64> {
        self.stored(processor).iter().map(Cursor::position).collect()
    }

    /// The stored cursor of each segment of `processor`.
    fn stored(&self, processor: &str) -> Vec<Cursor> {
        let processors = self.processors.lock().expect("token store poisoned");
        processors
            .get(processor)
            .map(|tokens| tokens.iter().map(|token| token.cursor.clone()).collect())
            .unwrap_or_default()
    }

    /// Runs `f` on `processor`'s segment, if it exists.
    fn with<T>(&self, processor: &str, segment: u32, f: impl FnOnce(&mut Token) -> T) -> Result<T, StoreError> {
        let mut processors = self.processors.lock().expect("token store poisoned");
        processors
            .get_mut(processor)
            .and_then(|tokens| tokens.get_mut(segment as usize))
            .map(f)
            .ok_or_else(|| StoreError::backend(format!("{processor} has no segment {segment}")))
    }
}

/// In memory, a crash loses the read model and the tokens together: there
/// is nothing to roll back, and the unit of work is `()`.
impl TokenStore for InMemoryTokenStore {
    type Tx = ();

    async fn initialize(&self, processor: &str, segments: u32) -> Result<u32, StoreError> {
        let mut processors = self.processors.lock().expect("token store poisoned");
        let tokens = processors
            .entry(processor.to_owned())
            .or_insert_with(|| vec![Token::default(); segments.max(1) as usize]);
        Ok(tokens.len() as u32)
    }

    async fn claim(
        &self,
        processor: &str,
        segment: u32,
        owner: &str,
        now: u64,
        until: u64,
    ) -> Result<Option<Cursor>, StoreError> {
        self.with(processor, segment, |token| {
            let free = match &token.claim {
                None => true,
                Some((holder, expiry)) => holder == owner || *expiry < now,
            };
            free.then(|| {
                token.claim = Some((owner.to_owned(), until));
                token.cursor.clone()
            })
        })
    }

    async fn store(&self, (): &mut (), token: TrackingToken<'_>) -> Result<bool, StoreError> {
        self.with(token.processor, token.segment, |stored| {
            let owns = stored.claim.as_ref().is_some_and(|(holder, _)| holder == token.owner);
            if owns {
                stored.cursor = token.cursor.clone();
            }
            owns
        })
    }

    async fn release(&self, processor: &str, segment: u32, owner: &str) -> Result<(), StoreError> {
        self.with(processor, segment, |token| {
            if token.claim.as_ref().is_some_and(|(holder, _)| holder == owner) {
                token.claim = None;
            }
        })
    }

    async fn cursors(&self, processor: &str) -> Result<Vec<Cursor>, StoreError> {
        Ok(self.stored(processor))
    }

    async fn reset(&self, processor: &str, now: u64, replay_until: u64) -> Result<bool, StoreError> {
        let mut processors = self.processors.lock().expect("token store poisoned");
        let Some(tokens) = processors.get_mut(processor) else {
            return Ok(true);
        };
        if tokens
            .iter()
            .any(|token| token.claim.as_ref().is_some_and(|(_, until)| *until >= now))
        {
            return Ok(false);
        }
        tokens.fill(Token {
            cursor: Cursor::default(),
            claim: None,
            replay_until,
        });
        Ok(true)
    }

    async fn replay_until(&self, processor: &str) -> Result<u64, StoreError> {
        let processors = self.processors.lock().expect("token store poisoned");
        let tokens = processors.get(processor).map(Vec::as_slice).unwrap_or_default();
        Ok(tokens.iter().map(|token| token.replay_until).max().unwrap_or(0))
    }
}

/// The segment an aggregate stream belongs to, out of `segments`: FNV-1a of
/// the stream id, stable across processes and releases (unlike std's
/// `DefaultHasher`), so every instance agrees.
pub fn segment_of(stream: &crate::StreamId, segments: u32) -> u32 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in stream.aggregate_type().bytes().chain(*b"-").chain(stream.id().bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    (hash % u64::from(segments.max(1))) as u32
}
