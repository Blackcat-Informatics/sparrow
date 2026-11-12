// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The one manual compaction a stalled pressure wait may force (#211).
//!
//! A retirement that flushes to a table no smaller than what it freed leaves linked bytes
//! where they were, and the native picker queues nothing for a few tombstones. The wait for
//! recovery would then never end. The pressure worker forces one compaction per wait here.
//! It runs on its own thread, never under the writer lock and never inside the maintenance
//! loop, so intake and the other duties continue while it works; the worker only observes it
//! through the native `num-running-compactions` property and [`Forced::poll`]. It is bounded
//! to one range compaction of each family, in turn, and starts only when the filesystem has
//! the maintenance reserve free. The native call returns no status (the binding discards it),
//! so a finished compaction is never reported as a success: the measured drop in linked
//! bytes is the only outcome.

use super::{Error, FAMILIES, RetentionWaker};
use rocksdb::{CompactOptions, DB};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

/// Progress of the single forced compaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Slot {
    /// None running and none waiting to be observed.
    Idle,
    /// The thread is compacting.
    Running,
    /// The thread ended; `Some` carries why it ended abnormally. Cleared by
    /// [`Forced::acknowledge`].
    Finished(Option<String>),
}

pub(super) struct Forced {
    slot: Mutex<Slot>,
    changed: Condvar,
    /// Wakes the maintenance worker when the compaction ends.
    waker: Arc<Mutex<Option<RetentionWaker>>>,
}

impl Forced {
    pub(super) fn new(waker: Arc<Mutex<Option<RetentionWaker>>>) -> Arc<Self> {
        Arc::new(Self {
            slot: Mutex::new(Slot::Idle),
            changed: Condvar::new(),
            waker,
        })
    }

    fn slot(&self) -> MutexGuard<'_, Slot> {
        self.slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn poll(&self) -> Slot {
        self.slot().clone()
    }

    /// Take note of a finished compaction so the next wait may force another.
    pub(super) fn acknowledge(&self) {
        let mut slot = self.slot();
        if matches!(*slot, Slot::Finished(_)) {
            *slot = Slot::Idle;
        }
    }

    /// Start the compaction unless one is already running or unacknowledged. Returns
    /// whether a compaction was started, so callers count only real ones.
    pub(super) fn start(self: &Arc<Self>, db: Arc<DB>) -> Result<bool, Error> {
        {
            let mut slot = self.slot();
            if *slot != Slot::Idle {
                return Ok(false);
            }
            *slot = Slot::Running;
        }
        let owner = Arc::clone(self);
        std::thread::Builder::new()
            .name("sparrow-forced-compaction".into())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    compact(&db);
                }));
                *owner.slot() = Slot::Finished(
                    outcome
                        .err()
                        .map(|_| "the forced compaction thread panicked".to_owned()),
                );
                owner.changed.notify_all();
                let waker = owner.waker.lock().ok().and_then(|slot| slot.clone());
                if let Some(waker) = waker {
                    waker();
                }
            })
            .map(|_| true)
            .map_err(|error| {
                *self.slot() = Slot::Idle;
                Error::new(
                    "STORAGE_FAILURE",
                    format!("cannot start the forced compaction thread: {error}"),
                )
            })
    }

    /// Leave a finished, unacknowledged compaction in the slot, as one that ended after its
    /// wait was cleared would.
    #[cfg(feature = "test-support")]
    pub(super) fn leave_finished(&self) {
        *self.slot() = Slot::Finished(None);
    }

    /// Block until no compaction is running, for tests that must observe its end.
    #[cfg(any(test, feature = "test-support"))]
    pub(super) fn wait_finished(&self) {
        let mut slot = self.slot();
        while *slot == Slot::Running {
            slot = self
                .changed
                .wait(slot)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

/// One range compaction of every family. It leaves automatic compaction running beside it
/// and keeps the native default of skipping a rewrite of the bottom level alone, so tombstones
/// merge down and the data they cover goes, and nothing else is rewritten.
fn compact(db: &DB) {
    let mut options = CompactOptions::default();
    options.set_exclusive_manual_compaction(false);
    for family in FAMILIES {
        if let Some(handle) = db.cf_handle(family) {
            db.compact_range_cf_opt::<&[u8], &[u8]>(handle, None, None, &options);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Value: a forced compaction that is counted but not run breaks "one per stuck wait"
    // (#211 review). Protects: a finished slot refuses a new start until acknowledged, a
    // refusal reports that nothing started, and acknowledging lets the next wait run one.
    #[test]
    fn a_finished_compaction_must_be_acknowledged_before_the_next_wait_may_start_one() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(DB::open_default(dir.path()).unwrap());
        let forced = Forced::new(Arc::default());
        assert!(forced.start(Arc::clone(&db)).unwrap());
        forced.wait_finished();
        assert_eq!(forced.poll(), Slot::Finished(None));
        assert!(!forced.start(Arc::clone(&db)).unwrap());
        forced.acknowledge();
        assert_eq!(forced.poll(), Slot::Idle);
        assert!(forced.start(db).unwrap());
        forced.wait_finished();
    }
}
