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
//! the maintenance reserve free. The native call returns no status (the binding discards
//! it: `rocksdb_compact_range_cf_opt` is `void` in the C API and the Rust binding returns
//! nothing), so a failed compaction is detected the one way RocksDB shows it: it raises
//! `rocksdb.background-errors`. The thread samples that count around the compaction, and
//! the pass keeps a baseline of it (see [`native_errors`]), so only a new error fails a
//! pass and an old one never holds intake. The thread also times itself and reads each
//! family's table and blob bytes before and after, so the first real run on a store
//! measures itself.

use super::{Error, FAMILIES, RetentionWaker};
use rocksdb::{CompactOptions, DB};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Instant;

/// One family's table and blob bytes either side of the forced compaction.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FamilySize {
    /// The column family.
    pub family: String,
    /// Table and blob file bytes before the compaction (the same two properties the linked
    /// footprint counts).
    pub before_bytes: u64,
    /// Table and blob file bytes after it.
    pub after_bytes: u64,
}

/// What a finished forced compaction measured about itself.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Outcome {
    /// Why it ended abnormally or failed; `None` for one that raised no error.
    pub(super) failure: Option<String>,
    /// Wall time the compaction took, in milliseconds.
    pub(super) elapsed_ms: u64,
    /// Table and blob bytes of every family before and after.
    pub(super) families: Vec<FamilySize>,
}

/// Summed `rocksdb.background-errors` of every family: cumulative since open, so a caller
/// compares it with a baseline rather than with zero.
pub(super) fn native_errors(db: &DB) -> Result<u64, Error> {
    let mut total = 0_u64;
    for family in FAMILIES {
        total = total.saturating_add(property(db, family, "rocksdb.background-errors")?);
    }
    Ok(total)
}

fn property(db: &DB, family: &str, name: &str) -> Result<u64, Error> {
    let handle = db
        .cf_handle(family)
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "native maintenance family missing"))?;
    db.property_int_value_cf(handle, name)?.ok_or_else(|| {
        Error::new(
            "STORAGE_FAILURE",
            format!("required native maintenance property unavailable: {family}/{name}"),
        )
    })
}

/// A fault a test arms for the next forced compaction.
#[cfg(feature = "test-support")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// The thread panics.
    Panic,
    /// The compaction raises one native background error.
    NativeError,
}

/// Progress of the single forced compaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Slot {
    /// None running and none waiting to be observed.
    Idle,
    /// The thread is compacting.
    Running,
    /// The thread ended; `Some` carries why it ended abnormally. Cleared by
    /// [`Forced::acknowledge`].
    Finished(Outcome),
}

pub(super) struct Forced {
    slot: Mutex<Slot>,
    changed: Condvar,
    /// Wakes the maintenance worker when the compaction ends.
    waker: Arc<Mutex<Option<RetentionWaker>>>,
    /// Native errors a test simulates, added to the real count.
    #[cfg(feature = "test-support")]
    simulated_errors: std::sync::atomic::AtomicU64,
    /// The fault the next compaction takes.
    #[cfg(feature = "test-support")]
    fault: Mutex<Option<Fault>>,
    /// While set, the commit that ends a wait fails, as a store that cannot take a write
    /// would.
    #[cfg(feature = "test-support")]
    wait_end_refused: std::sync::atomic::AtomicBool,
    /// While set, the retirement a pass runs fails, as a store that cannot retire would.
    #[cfg(feature = "test-support")]
    retirement_refused: std::sync::atomic::AtomicBool,
}

impl Forced {
    pub(super) fn new(waker: Arc<Mutex<Option<RetentionWaker>>>) -> Arc<Self> {
        Arc::new(Self {
            slot: Mutex::new(Slot::Idle),
            changed: Condvar::new(),
            waker,
            #[cfg(feature = "test-support")]
            simulated_errors: std::sync::atomic::AtomicU64::new(0),
            #[cfg(feature = "test-support")]
            fault: Mutex::new(None),
            #[cfg(feature = "test-support")]
            wait_end_refused: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "test-support")]
            retirement_refused: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// The one count of native background errors the pass and the thread both compare with
    /// their baselines.
    pub(super) fn native_errors(&self, db: &DB) -> Result<u64, Error> {
        let total = native_errors(db)?;
        #[cfg(feature = "test-support")]
        let total = total.saturating_add(
            self.simulated_errors
                .load(std::sync::atomic::Ordering::SeqCst),
        );
        Ok(total)
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
                let started = Instant::now();
                let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    owner.compact_measured(&db)
                }));
                let mut outcome = ran.unwrap_or_else(|_| Outcome {
                    failure: Some("the forced compaction thread panicked".to_owned()),
                    ..Outcome::default()
                });
                outcome.elapsed_ms =
                    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                // Release the database before publishing the end: whoever sees `Finished` (a
                // test waiting for native quiet, then closing the store and reopening its
                // directory) must find no handle of this thread left holding the RocksDB lock.
                drop(db);
                *owner.slot() = Slot::Finished(outcome);
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
        *self.slot() = Slot::Finished(Outcome::default());
    }

    /// Arm a fault for the next forced compaction.
    #[cfg(feature = "test-support")]
    pub(super) fn arm(&self, fault: Fault) {
        *self
            .fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(fault);
    }

    /// Make the commit that ends a wait fail (or succeed again).
    #[cfg(feature = "test-support")]
    pub(super) fn refuse_wait_end(&self, refused: bool) {
        self.wait_end_refused
            .store(refused, std::sync::atomic::Ordering::SeqCst);
    }

    /// Whether a test made the wait-ending commit fail.
    #[cfg(feature = "test-support")]
    pub(super) fn wait_end_refused(&self) -> bool {
        self.wait_end_refused
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Make the retirement of every pass fail (or stop failing).
    #[cfg(feature = "test-support")]
    pub(super) fn refuse_retirement(&self, refused: bool) {
        self.retirement_refused
            .store(refused, std::sync::atomic::Ordering::SeqCst);
    }

    /// Whether a test made the retirement fail.
    #[cfg(feature = "test-support")]
    pub(super) fn retirement_refused(&self) -> bool {
        self.retirement_refused
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Count `count` more native errors, as a failing compaction would.
    #[cfg(feature = "test-support")]
    pub(super) fn simulate_native_errors(&self, count: u64) {
        self.simulated_errors
            .fetch_add(count, std::sync::atomic::Ordering::SeqCst);
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

impl Forced {
    /// The compaction, with the native error count and each family's table and blob bytes sampled
    /// around it. A higher error count is the failure the binding's `void` call hides.
    fn compact_measured(&self, db: &DB) -> Outcome {
        let mut failures = Vec::new();
        let sizes = |failures: &mut Vec<String>| {
            FAMILIES
                .iter()
                .map(|family| {
                    // Blob files hold the payload family's bulk and the linked footprint
                    // counts them, so the measure must too.
                    [
                        "rocksdb.total-sst-files-size",
                        "rocksdb.total-blob-file-size",
                    ]
                    .into_iter()
                    .map(|name| {
                        property(db, family, name).unwrap_or_else(|error| {
                            failures.push(error.to_string());
                            0
                        })
                    })
                    .fold(0_u64, u64::saturating_add)
                })
                .collect::<Vec<_>>()
        };
        let before = sizes(&mut failures);
        let errors_before = self.native_errors(db);
        #[cfg(feature = "test-support")]
        match self
            .fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            Some(Fault::Panic) => panic!("injected forced compaction panic"),
            Some(Fault::NativeError) => self.simulate_native_errors(1),
            None => {}
        }
        compact(db);
        match (errors_before, self.native_errors(db)) {
            (Ok(before), Ok(after)) if after > before => failures.push(format!(
                "the forced compaction raised {} RocksDB background errors",
                after - before
            )),
            (Ok(_), Ok(_)) => {}
            (Err(error), _) | (_, Err(error)) => failures.push(error.to_string()),
        }
        let after = sizes(&mut failures);
        Outcome {
            failure: (!failures.is_empty()).then(|| failures.join("; ")),
            elapsed_ms: 0,
            families: FAMILIES
                .iter()
                .zip(before.into_iter().zip(after))
                .map(|(family, (before_bytes, after_bytes))| FamilySize {
                    family: (*family).to_owned(),
                    before_bytes,
                    after_bytes,
                })
                .collect(),
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

    fn open_families(path: &std::path::Path) -> DB {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        DB::open_cf(&options, path, FAMILIES).unwrap()
    }

    // Value: a forced compaction that is counted but not run breaks "one per stuck wait"
    // (#211 review). Protects: a finished slot refuses a new start until acknowledged, a
    // refusal reports that nothing started, and acknowledging lets the next wait run one.
    #[test]
    fn a_finished_compaction_must_be_acknowledged_before_the_next_wait_may_start_one() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(open_families(dir.path()));
        let forced = Forced::new(Arc::default());
        assert!(forced.start(Arc::clone(&db)).unwrap());
        forced.wait_finished();
        // Finished means the thread holds no handle of the database (#348): a caller that sees
        // the end may close the store and reopen its directory at once.
        assert_eq!(
            Arc::strong_count(&db),
            1,
            "a finished compaction holds the database"
        );
        let Slot::Finished(outcome) = forced.poll() else {
            panic!("the compaction did not finish");
        };
        assert_eq!(outcome.failure, None);
        assert!(!forced.start(Arc::clone(&db)).unwrap());
        forced.acknowledge();
        assert_eq!(forced.poll(), Slot::Idle);
        assert!(forced.start(db).unwrap());
        forced.wait_finished();
    }

    // Value: #218 review: the forced thread is the one place a compaction can fail without
    // the pass seeing it, so a panic or a native error must reach the slot, and a failed slot
    // must still return to Idle and allow the next wait's compaction (no wedge). Protects:
    // a panic and a raised native error each end as Finished with a failure, the sizes
    // of every family are reported, and acknowledging returns the slot to Idle.
    #[cfg(feature = "test-support")]
    #[test]
    fn a_failed_forced_compaction_reports_why_and_leaves_the_slot_reusable() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(open_families(dir.path()));
        let forced = Forced::new(Arc::default());
        for (fault, expected) in [
            (Fault::Panic, "panicked"),
            (Fault::NativeError, "background errors"),
        ] {
            forced.arm(fault);
            assert!(forced.start(Arc::clone(&db)).unwrap());
            forced.wait_finished();
            assert_eq!(
                Arc::strong_count(&db),
                1,
                "a failed compaction holds the database"
            );
            let Slot::Finished(outcome) = forced.poll() else {
                panic!("the compaction did not finish");
            };
            assert!(outcome.failure.unwrap().contains(expected));
            forced.acknowledge();
            assert_eq!(forced.poll(), Slot::Idle);
        }
        assert!(forced.start(db).unwrap());
        forced.wait_finished();
    }
}
