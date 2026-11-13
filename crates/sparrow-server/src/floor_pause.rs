// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! What the daemon does while the free-space floor pauses writes (#257): it keeps accepting
//! connections, answers writes with the floor-labelled retryable refusal, and serves reads. This
//! module counts those three things for `status` and writes the journal lines of an episode.
//!
//! The pause itself is the store's: [`sparrow_core::SpaceFloor::observe`] records its start and
//! end in one ledger, whichever duty observes it. Nothing here measures the volume or owns a
//! timer; the journal is advanced by the daemon's floor observer tick (one a second, the
//! cadence the maintenance worker observes at), because the ending of a pause caused by another
//! process freeing space has no event to wait on.

use sparrow_core::SpaceFloor;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// How often a pause that continues is journaled again. A long episode is never silent: the
/// first observation after this much time announces it with the counters so far.
pub const PAUSE_REPORT_INTERVAL_MS: u64 = 60_000;

/// Counters of the pause, shared by every connection of the daemon, and the floor they read.
#[derive(Clone, Debug)]
pub struct FloorPause {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    floor: SpaceFloor,
    accepted: AtomicU64,
    writes_refused: AtomicU64,
    reads_served: AtomicU64,
    read_staging_refused: AtomicU64,
}

/// A copy of the counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FloorPauseCounts {
    /// Unix milliseconds the current pause began; zero when not paused.
    pub paused_since: u64,
    /// Connections accepted while paused.
    pub connections_accepted: u64,
    /// Writes answered with the floor label.
    pub writes_refused: u64,
    /// Reads served to completion while paused.
    pub reads_served: u64,
    /// Reads refused because the staging they need would spend the floor's reserve.
    pub read_staging_refused: u64,
}

impl FloorPause {
    /// Count against `floor`, the store's one free-space floor.
    #[must_use]
    pub fn new(floor: &SpaceFloor) -> Self {
        Self {
            inner: Arc::new(Inner {
                floor: floor.clone(),
                accepted: AtomicU64::new(0),
                writes_refused: AtomicU64::new(0),
                reads_served: AtomicU64::new(0),
                read_staging_refused: AtomicU64::new(0),
            }),
        }
    }

    /// Whether the floor is paused as the daemon last observed it.
    #[must_use]
    pub fn paused(&self) -> bool {
        self.inner.floor.paused()
    }

    /// One connection was accepted; it counts if the floor is paused.
    pub fn connection_accepted(&self) {
        if self.paused() {
            self.inner.accepted.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A write was refused with `error`; it counts if the error carries the floor label.
    pub(crate) fn write_refused(&self, error: &sparrow_core::Error) {
        if error.is_free_space_floor() {
            self.inner.writes_refused.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A read finished. `paused` is whether the floor was paused when it started; it counts as
    /// served if it succeeded, and as a staging refusal if the floor label refused it.
    pub(crate) fn read_finished<T>(&self, paused: bool, result: &Result<T, sparrow_core::Error>) {
        match result {
            Ok(_) if paused => {
                self.inner.reads_served.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) if error.is_free_space_floor() => {
                self.inner
                    .read_staging_refused
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// The counters now.
    #[must_use]
    pub fn counts(&self) -> FloorPauseCounts {
        FloorPauseCounts {
            paused_since: self.inner.floor.status().paused_since,
            connections_accepted: self.inner.accepted.load(Ordering::Relaxed),
            writes_refused: self.inner.writes_refused.load(Ordering::Relaxed),
            reads_served: self.inner.reads_served.load(Ordering::Relaxed),
            read_staging_refused: self.inner.read_staging_refused.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn encode(&self, mut out: crate::sparrow_capnp::floor_pause_status::Builder<'_>) {
        let counts = self.counts();
        out.set_paused_since(counts.paused_since);
        out.set_connections_accepted(counts.connections_accepted);
        out.set_writes_refused(counts.writes_refused);
        out.set_reads_served(counts.reads_served);
        out.set_read_staging_refused(counts.read_staging_refused);
    }
}

/// The journal lines of pause episodes: one when a pause begins, one each report interval while
/// it lasts, one when it ends. A pause that begins within one interval of the last line is not
/// announced then, so a volume hovering at the floor cannot flood the journal; if it lasts the
/// interval, the periodic line announces it, and its end is journaled only for an announced
/// episode. Every pause is counted in status whatever the journal said. Advanced by the floor
/// observer with the clock it observed at, so it needs no timer of its own and a test drives it
/// with chosen instants.
#[derive(Debug, Default)]
pub struct PauseJournal {
    episode: Option<Episode>,
    /// Unix milliseconds of the last line written, of any episode.
    last_line: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct Episode {
    began: u64,
    /// When this episode was last announced, or when the line it is waiting out began.
    last: u64,
    announced: bool,
}

impl PauseJournal {
    /// Advance with one floor observation at `now` (Unix milliseconds): `shortfall` is the
    /// observed shortfall, `None` when free space meets the floor.
    pub fn observe(
        &mut self,
        shortfall: Option<&str>,
        now: u64,
        pause: &FloorPause,
    ) -> Option<String> {
        let counts = pause.counts();
        let totals = format!(
            "connections_accepted={} writes_refused={} reads_served={} read_staging_refused={}",
            counts.connections_accepted,
            counts.writes_refused,
            counts.reads_served,
            counts.read_staging_refused
        );
        let quiet = |last: Option<u64>| {
            last.is_some_and(|last| now.saturating_sub(last) < PAUSE_REPORT_INTERVAL_MS)
        };
        match (shortfall, self.episode) {
            (Some(shortfall), None) => {
                if quiet(self.last_line) {
                    self.episode = Some(Episode {
                        began: now,
                        last: self.last_line.unwrap_or(now),
                        announced: false,
                    });
                    return None;
                }
                self.episode = Some(Episode {
                    began: now,
                    last: now,
                    announced: true,
                });
                self.last_line = Some(now);
                Some(format!(
                    "sparrowd: free space is below the floor: {shortfall}; still accepting connections, answering writes with the retryable floor-labelled STORAGE_PRESSURE, serving reads ({totals})"
                ))
            }
            (Some(_), Some(episode)) if !quiet(Some(episode.last)) => {
                self.episode = Some(Episode {
                    last: now,
                    announced: true,
                    ..episode
                });
                self.last_line = Some(now);
                Some(format!(
                    "sparrowd: free space has stayed below the floor for {} s; writes are still refused with the floor label ({totals})",
                    now.saturating_sub(episode.began) / 1000
                ))
            }
            (None, Some(episode)) => {
                self.episode = None;
                if !episode.announced {
                    return None;
                }
                self.last_line = Some(now);
                Some(format!(
                    "sparrowd: free space restored after {} s; writes resume ({totals})",
                    now.saturating_sub(episode.began) / 1000
                ))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pause() -> (tempfile::TempDir, FloorPause) {
        let dir = tempfile::tempdir().unwrap();
        let floor = SpaceFloor::new(dir.path(), 1);
        (dir, FloorPause::new(&floor))
    }

    /// Value (#243 lesson): a long episode must never be silent, and a flapping volume must
    /// not flood the journal. Protects, with chosen instants and no clock: one line when a
    /// pause begins, one per report interval while it lasts, one when it ends; a pause that
    /// begins and ends inside the interval after a line writes nothing; one that begins
    /// inside the interval and outlasts it is announced by the next observation past it.
    #[test]
    fn a_long_pause_is_announced_each_interval_and_a_flapping_one_is_not_a_flood() {
        let (_dir, pause) = pause();
        let mut journal = PauseJournal::default();
        let mut line = |shortfall: Option<&str>, now: u64| journal.observe(shortfall, now, &pause);
        assert!(line(None, 0).is_none());
        assert!(
            line(Some("low"), 1_000)
                .unwrap()
                .contains("below the floor: low")
        );
        assert!(line(Some("low"), 30_000).is_none());
        let again = line(Some("low"), 61_000).unwrap();
        assert!(again.contains("stayed below the floor for 60 s"), "{again}");
        assert!(line(Some("low"), 100_000).is_none());
        let end = line(None, 130_000).unwrap();
        assert!(end.contains("restored after 129 s"), "{end}");
        // Flapping inside the interval after that line: nothing is written, either way.
        assert!(line(Some("low"), 131_000).is_none());
        assert!(line(None, 132_000).is_none());
        // A pause that begins inside the interval and lasts past it is announced then.
        assert!(line(Some("low"), 133_000).is_none());
        assert!(line(Some("low"), 189_000).is_none());
        assert!(line(Some("low"), 190_000).unwrap().contains("stayed below"));
        assert!(line(None, 191_000).unwrap().contains("restored"));
    }

    /// Value (#257): the counters must count what they name and nothing else. Protects: an
    /// accept counts only while paused; only a floor-labelled error counts as a floor refusal
    /// (budget pressure does not); a read counts as served only when it began paused and
    /// succeeded, and a floor refusal of a read counts as a staging refusal.
    #[test]
    fn counters_count_only_floor_events() {
        let dir = tempfile::tempdir().unwrap();
        let floor = SpaceFloor::new(dir.path(), u64::MAX / 4);
        let pause = FloorPause::new(&floor);
        pause.connection_accepted();
        assert_eq!(pause.counts().connections_accepted, 0);
        assert!(floor.observe(5).unwrap().is_some());
        pause.connection_accepted();
        let budget = sparrow_core::Error::new("STORAGE_PRESSURE", "write would cross the reserve");
        pause.write_refused(&budget);
        pause.write_refused(&floor.check().unwrap_err());
        pause.read_finished(true, &Ok(()));
        pause.read_finished(false, &Ok(()));
        pause.read_finished::<()>(true, &Err(floor.check().unwrap_err()));
        pause.read_finished::<()>(true, &Err(budget));
        assert_eq!(
            pause.counts(),
            FloorPauseCounts {
                paused_since: 5,
                connections_accepted: 1,
                writes_refused: 1,
                reads_served: 1,
                read_staging_refused: 1,
            }
        );
    }
}
