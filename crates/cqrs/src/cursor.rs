//! How far a reader of the whole log got (Axon's `GapAwareTrackingToken`).
//!
//! A store whose appends run in parallel commits positions out of order: 11
//! may be visible while 10 is still being written. A reader keeps going past
//! 10, remembering it as a [`Gap`], and picks it up if it is committed later.
//! Axon gives up on a gap after a timeout, so a transaction slower than that
//! loses its event; here a gap is given up only once the store proves nobody
//! can fill it any more (its [`Horizon`]).

/// A position skipped because it was not committed yet when it was read
/// past.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Gap {
    /// The missing position.
    pub position: u64,
    /// Whoever could still fill it started before this point of the store's
    /// clock (for PostgreSQL: a transaction id).
    pub horizon: u64,
}

/// What a store knew when it answered a read: which writers had certainly
/// finished, and which ones could have taken the positions it returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Horizon {
    /// Every writer that started before this point has finished: gaps whose
    /// horizon is at most this can no longer be filled.
    pub finished_below: u64,
    /// Every writer that could have taken a position up to the ones read
    /// started before this point: the horizon of the gaps the read reveals.
    pub started_below: u64,
}

impl Horizon {
    /// A store whose positions never leave gaps (appends one at a time).
    pub const SETTLED: Self = Self {
        finished_below: u64::MAX,
        started_below: u64::MAX,
    };
}

/// Everything up to `position` was seen, except the [`gaps`](Self::gaps).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Cursor {
    /// The highest position seen.
    position: u64,
    /// Positions below it not seen yet, in order.
    gaps: Vec<Gap>,
}

impl Cursor {
    /// Everything up to `position` seen, no gaps.
    pub const fn at(position: u64) -> Self {
        Self {
            position,
            gaps: Vec::new(),
        }
    }

    /// A cursor as stored (gaps in any order; those above `position` are
    /// ignored).
    pub fn new(position: u64, gaps: impl IntoIterator<Item = Gap>) -> Self {
        let mut gaps: Vec<Gap> = gaps.into_iter().filter(|gap| gap.position <= position).collect();
        gaps.sort_unstable_by_key(|gap| gap.position);
        gaps.dedup_by_key(|gap| gap.position);
        Self { position, gaps }
    }

    /// The highest position seen.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// The positions below [`position`](Self::position) not seen yet.
    pub fn gaps(&self) -> &[Gap] {
        &self.gaps
    }

    /// Whether the event at `position` was seen.
    pub fn covers(&self, position: u64) -> bool {
        position <= self.position && !self.is_gap(position)
    }

    /// Whether `position` is a gap: seen past, not seen yet.
    pub fn is_gap(&self, position: u64) -> bool {
        self.gaps.binary_search_by_key(&position, |gap| gap.position).is_ok()
    }

    /// Every position up to here was seen: the lowest gap's predecessor, or
    /// the position.
    pub fn watermark(&self) -> u64 {
        self.gaps.first().map_or(self.position, |gap| gap.position - 1)
    }

    /// Records that the event at `position` was seen. Positions skipped to
    /// reach it become gaps with the read's horizon. `false`: it was already
    /// seen.
    pub fn advance(&mut self, position: u64, horizon: &Horizon) -> bool {
        if position <= self.position {
            return match self.gaps.binary_search_by_key(&position, |gap| gap.position) {
                Ok(index) => {
                    self.gaps.remove(index);
                    true
                }
                Err(_) => false,
            };
        }
        // A gapless store never skips; if one did, its gaps settle at once.
        if horizon.started_below != u64::MAX {
            let skipped = self.position + 1..position;
            self.gaps.extend(skipped.map(|position| Gap {
                position,
                horizon: horizon.started_below,
            }));
        }
        self.position = position;
        true
    }

    /// Gives up the gaps nobody can fill any more, except `pending` ones (read
    /// in the same answer, not seen yet).
    pub fn settle(&mut self, horizon: &Horizon, pending: impl Fn(u64) -> bool) {
        self.gaps
            .retain(|gap| gap.horizon > horizon.finished_below || pending(gap.position));
    }

    /// Whether this cursor saw everything `other` saw.
    pub fn includes(&self, other: &Self) -> bool {
        other.position <= self.position
            && self
                .gaps
                .iter()
                .take_while(|gap| gap.position <= other.position)
                .all(|gap| other.is_gap(gap.position))
    }

    /// A cursor that saw everything either saw.
    #[must_use]
    pub fn union(&self, other: &Self) -> Self {
        if self.includes(other) {
            return self.clone();
        }
        if other.includes(self) {
            return other.clone();
        }
        let missed = |cursor: &Self, by: &Self| {
            cursor
                .gaps
                .iter()
                .copied()
                .filter(|gap| !by.covers(gap.position))
                .collect::<Vec<_>>()
        };
        let gaps = missed(self, other).into_iter().chain(missed(other, self));
        Self::new(self.position.max(other.position), gaps)
    }

    /// A cursor that saw only what all of `cursors` saw: what a reader
    /// serving them all must start from.
    pub fn common<'a>(cursors: impl IntoIterator<Item = &'a Self>) -> Option<Self> {
        let cursors: Vec<&Self> = cursors.into_iter().collect();
        let position = cursors.iter().map(|cursor| cursor.position).min()?;
        let gaps = cursors.iter().flat_map(|cursor| cursor.gaps.iter().copied());
        Some(Self::new(position, gaps))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const READ: Horizon = Horizon {
        finished_below: 10,
        started_below: 20,
    };

    #[test]
    fn reading_past_unwritten_positions_leaves_gaps_until_filled() {
        let mut cursor = Cursor::at(2);
        assert!(cursor.advance(5, &READ));
        assert_eq!(
            cursor.gaps(),
            [
                Gap {
                    position: 3,
                    horizon: 20
                },
                Gap {
                    position: 4,
                    horizon: 20
                }
            ]
        );
        assert!(cursor.covers(5) && cursor.covers(2) && !cursor.covers(3) && !cursor.covers(6));
        assert_eq!(cursor.watermark(), 2);
        assert!(cursor.advance(4, &READ), "a gap filled");
        assert!(!cursor.advance(4, &READ) && !cursor.advance(5, &READ), "seen already");
        assert_eq!(cursor.watermark(), 2);
        assert!(cursor.advance(3, &READ));
        assert_eq!(cursor, Cursor::at(5));
    }

    #[test]
    fn a_gap_is_given_up_once_every_writer_that_could_fill_it_finished() {
        let mut cursor = Cursor::at(0);
        cursor.advance(
            2,
            &Horizon {
                finished_below: 5,
                started_below: 7,
            },
        );
        cursor.advance(
            4,
            &Horizon {
                finished_below: 6,
                started_below: 9,
            },
        );
        cursor.settle(
            &Horizon {
                finished_below: 6,
                started_below: 9,
            },
            |_| false,
        );
        assert_eq!(cursor.gaps().len(), 2, "writers before 7 and 9 may still commit");
        cursor.settle(
            &Horizon {
                finished_below: 7,
                started_below: 9,
            },
            |position| position == 1,
        );
        assert_eq!(
            cursor.gaps(),
            [
                Gap {
                    position: 1,
                    horizon: 7
                },
                Gap {
                    position: 3,
                    horizon: 9
                }
            ],
            "pending ones stay"
        );
        cursor.settle(
            &Horizon {
                finished_below: 9,
                started_below: 9,
            },
            |_| false,
        );
        assert_eq!(cursor, Cursor::at(4));
    }

    #[test]
    fn a_gapless_store_never_leaves_gaps() {
        let mut cursor = Cursor::at(0);
        cursor.advance(3, &Horizon::SETTLED);
        assert_eq!(cursor, Cursor::at(3));
    }

    #[test]
    fn readers_compare_and_combine_cursors() {
        let gap = |position| Gap { position, horizon: 1 };
        let ahead = Cursor::new(10, [gap(4)]);
        let behind = Cursor::new(6, [gap(4), gap(5)]);
        assert!(ahead.includes(&behind) && !behind.includes(&ahead));
        assert!(!Cursor::at(10).includes(&Cursor::new(12, [gap(11)])));
        assert!(!Cursor::new(10, [gap(3)]).includes(&Cursor::at(5)), "it missed 3");
        let common = Cursor::common([&ahead, &behind, &Cursor::new(8, [gap(2)])]).unwrap();
        assert_eq!(common, Cursor::new(6, [gap(2), gap(4), gap(5)]));
        assert!(ahead.includes(&common) && behind.includes(&common));
        assert_eq!(ahead.union(&behind), ahead);
        assert_eq!(
            Cursor::new(10, [gap(4), gap(7)]).union(&Cursor::new(8, [gap(7)])),
            Cursor::new(10, [gap(7)])
        );
        assert_eq!(
            Cursor::new(5, [gap(2)]).union(&Cursor::new(9, [gap(4)])),
            Cursor::at(9),
            "each saw what the other missed"
        );
    }
}
