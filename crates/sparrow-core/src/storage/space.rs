// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One filesystem free-space floor shared by every store write and daemon admission.

use super::{Error, RetentionWaker};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

/// Every pause the free-space floor imposed on this store since it opened, shared by all
/// clones of its [`SpaceFloor`]. Edges are detected where the floor is observed with a
/// clock ([`SpaceFloor::observe`]), so a pause is counted once however many duties see it.
#[derive(Debug)]
struct Ledger {
    paused: AtomicBool,
    /// Unix milliseconds the current pause began; zero while not paused.
    since: AtomicU64,
    pauses: AtomicU64,
    resumes: AtomicU64,
    /// Writes refused by [`SpaceFloor::check`].
    refused: AtomicU64,
    /// Run, without blocking, by whichever [`SpaceFloor::observe`] sees the pause end (#247).
    /// Several duties subscribe (maintenance, the consume and tail watches).
    resume_wakers: std::sync::Mutex<Resumers>,
    next_subscription: AtomicU64,
    /// Dev-only: the free bytes every measurement reports, `u64::MAX` for the real volume.
    #[cfg(feature = "test-support")]
    free_override: AtomicU64,
    /// Dev-only: answer every [`SpaceFloor::check`] as below the floor.
    #[cfg(feature = "test-support")]
    forced: AtomicBool,
    /// Dev-only: the error code the next [`SpaceFloor::check`] answers once, then clears.
    #[cfg(feature = "test-support")]
    once: std::sync::Mutex<Option<&'static str>>,
}

/// The subscribed resume callbacks; a closure has no `Debug`, so this reports only the count.
struct Resumers(Vec<(u64, RetentionWaker)>);

impl std::fmt::Debug for Resumers {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("Resumers")
            .field(&self.0.len())
            .finish()
    }
}

/// A subscription to the floor's resume edge; dropping it unsubscribes.
#[must_use = "dropping the subscription unsubscribes it"]
pub struct ResumeSubscription {
    place: Arc<Place>,
    id: u64,
}

impl Drop for ResumeSubscription {
    fn drop(&mut self) {
        if let Ok(mut resumers) = self.place.ledger.resume_wakers.lock() {
            resumers.0.retain(|(id, _)| *id != self.id);
        }
    }
}

impl Ledger {
    fn new() -> Self {
        Self {
            paused: AtomicBool::new(false),
            since: AtomicU64::new(0),
            pauses: AtomicU64::new(0),
            resumes: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            resume_wakers: std::sync::Mutex::new(Resumers(Vec::new())),
            next_subscription: AtomicU64::new(0),
            #[cfg(feature = "test-support")]
            free_override: AtomicU64::new(u64::MAX),
            #[cfg(feature = "test-support")]
            forced: AtomicBool::new(false),
            #[cfg(feature = "test-support")]
            once: std::sync::Mutex::new(None),
        }
    }
}

/// The text every free-space-floor refusal message starts with. The refusal keeps the code
/// `STORAGE_PRESSURE`, so every caller that holds and retries it keeps working; this prefix is
/// how [`Error::is_free_space_floor`] tells it from budget pressure.
pub const FLOOR_REFUSAL_PREFIX: &str = "free-space floor: ";

/// Total disk bytes all reads together may stage while the floor is paused (R5). Far below the
/// default 4 GiB floor, so the reserve is not the thing a read spends.
pub const PAUSED_READ_STAGING_BYTES: u64 = 32 * 1024 * 1024;

fn floor_refusal(detail: &str) -> Error {
    Error::new(
        "STORAGE_PRESSURE",
        format!("{FLOOR_REFUSAL_PREFIX}{detail}"),
    )
}

impl Error {
    /// Whether this is a retryable `STORAGE_PRESSURE` refusal made by the free-space floor,
    /// as opposed to the store's own budget pressure.
    #[must_use]
    pub fn is_free_space_floor(&self) -> bool {
        self.code == "STORAGE_PRESSURE" && self.message.starts_with(FLOOR_REFUSAL_PREFIX)
    }
}

/// What the floor has done since the store opened.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FloorStatus {
    /// Unix milliseconds the current pause began; zero while writes are admitted.
    pub paused_since: u64,
    /// Pauses begun.
    pub pauses: u64,
    /// Pauses ended.
    pub resumes: u64,
    /// Writes refused while below the floor.
    pub refused_writes: u64,
}

/// Free space the filesystem holding a store must keep before Sparrow writes.
///
/// The physical `disk_budget` bounds the store's own bytes; it cannot see other
/// writers filling the same filesystem. A write that fails with no space left
/// latches durability uncertainty until the store is reopened, so writes stop
/// here first with a retriable refusal.
#[derive(Clone, Debug)]
pub struct SpaceFloor {
    /// One allocation for the directory and the ledger, so the floor stays two words wide
    /// inside every store handle.
    place: Arc<Place>,
    min_free: u64,
}

#[derive(Debug)]
struct Place {
    dir: std::path::PathBuf,
    ledger: Ledger,
}

impl SpaceFloor {
    /// A floor for the filesystem that holds (or will hold) `dir`.
    #[must_use]
    pub fn new(dir: &Path, min_free: u64) -> Self {
        Self {
            place: Arc::new(Place {
                dir: dir.to_path_buf(),
                ledger: Ledger::new(),
            }),
            min_free,
        }
    }

    /// Refuse while unprivileged free space is below the floor.
    pub fn check(&self) -> Result<(), Error> {
        #[cfg(feature = "test-support")]
        if let Some(code) = self
            .place
            .ledger
            .once
            .lock()
            .ok()
            .and_then(|mut once| once.take())
        {
            return Err(Error::new(code, "write refused once by a test"));
        }
        #[cfg(feature = "test-support")]
        if self.place.ledger.forced.load(Ordering::Acquire) {
            self.place.ledger.refused.fetch_add(1, Ordering::Relaxed);
            return Err(floor_refusal("write refused by a test"));
        }
        match self.shortfall()? {
            Some(shortfall) => {
                self.place.ledger.refused.fetch_add(1, Ordering::Relaxed);
                Err(floor_refusal(&shortfall))
            }
            None => Ok(()),
        }
    }

    /// Whether the floor is paused as the daemon's observers last saw it ([`SpaceFloor::observe`]).
    /// It reads the ledger, never the volume, so it is cheap on every request.
    #[must_use]
    pub fn paused(&self) -> bool {
        self.place.ledger.paused.load(Ordering::Acquire)
    }

    /// Disk bytes a read may stage now, at most `wanted`, with `staged` bytes already staged by
    /// other reads. Above the floor that is `wanted`. While the floor is paused all reads
    /// together stage at most [`PAUSED_READ_STAGING_BYTES`], and never so much that free space
    /// falls below half the floor: the floor's reserve is what the store's own flush and
    /// compaction need, and a read must not spend it. Zero means a read that needs staging is
    /// refused with the floor label until space returns.
    pub fn read_staging_allowance(&self, wanted: u64, staged: u64) -> Result<u64, Error> {
        let free = self.free()?;
        if free >= self.min_free {
            return Ok(wanted);
        }
        Ok(wanted
            .min(PAUSED_READ_STAGING_BYTES.saturating_sub(staged))
            // Reservations other reads hold may not be written yet, so the live free space does
            // not show them: take them out of the headroom, or concurrent reads would each
            // see all of it.
            .min(
                free.saturating_sub(self.min_free / 2)
                    .saturating_sub(staged),
            ))
    }

    /// [`SpaceFloor::shortfall`] that also records the start and end of a pause at `now`
    /// (Unix milliseconds). The daemon's duties call it so a pause leaves a trace in status.
    pub fn observe(&self, now: u64) -> Result<Option<String>, Error> {
        let shortfall = self.shortfall()?;
        let paused = shortfall.is_some();
        let was = self.place.ledger.paused.swap(paused, Ordering::AcqRel);
        if paused && !was {
            // At least one, so a pause at the clock's zero still reads as paused.
            self.place.ledger.since.store(now.max(1), Ordering::Release);
            self.place.ledger.pauses.fetch_add(1, Ordering::Relaxed);
        } else if !paused && was {
            self.place.ledger.since.store(0, Ordering::Release);
            self.place.ledger.resumes.fetch_add(1, Ordering::Relaxed);
            // The edge is signalled here, where it is consumed, so whichever duty or loop
            // observes it first wakes the waiters; none can consume it unseen (#247).
            let wakers: Vec<RetentionWaker> = self
                .place
                .ledger
                .resume_wakers
                .lock()
                .map(|resumers| resumers.0.iter().map(|(_, waker)| waker.clone()).collect())
                .unwrap_or_default();
            for waker in wakers {
                waker();
            }
        }
        Ok(shortfall)
    }

    /// Subscribe `waker` to the edge: it runs, once per edge, in the observation that sees a
    /// pause end. It runs on the observing thread, so it must not block. Any number of duties
    /// subscribe; the subscription ends when the returned guard drops.
    pub fn subscribe_resume(&self, waker: RetentionWaker) -> ResumeSubscription {
        let id = self
            .place
            .ledger
            .next_subscription
            .fetch_add(1, Ordering::Relaxed);
        if let Ok(mut resumers) = self.place.ledger.resume_wakers.lock() {
            resumers.0.push((id, waker));
        }
        ResumeSubscription {
            place: Arc::clone(&self.place),
            id,
        }
    }

    /// Dev-only: report `free` bytes free on every measurement (`None` measures the real
    /// volume), so a test moves the observed pause and its end the way free space does.
    #[cfg(feature = "test-support")]
    pub fn set_free_for_tests(&self, free: Option<u64>) {
        self.place
            .ledger
            .free_override
            .store(free.unwrap_or(u64::MAX), Ordering::Release);
    }

    /// Dev-only: refuse (or again admit) every write at the admission check, as a store under
    /// pressure does, without moving the measured free space the floor observers read.
    #[cfg(feature = "test-support")]
    pub fn refuse_writes_for_tests(&self, refused: bool) {
        self.place.ledger.forced.store(refused, Ordering::Release);
    }

    /// Dev-only: answer the next write admission check with `code` once, as a refusal no
    /// retry changes would (the one after it is admitted again).
    #[cfg(feature = "test-support")]
    pub fn refuse_next_write_for_tests(&self, code: &'static str) {
        if let Ok(mut once) = self.place.ledger.once.lock() {
            *once = Some(code);
        }
    }

    /// The pauses and refusals recorded so far.
    #[must_use]
    pub fn status(&self) -> FloorStatus {
        FloorStatus {
            paused_since: self.place.ledger.since.load(Ordering::Acquire),
            pauses: self.place.ledger.pauses.load(Ordering::Relaxed),
            resumes: self.place.ledger.resumes.load(Ordering::Relaxed),
            refused_writes: self.place.ledger.refused.load(Ordering::Relaxed),
        }
    }

    /// Describe a shortfall of `needed` bytes of free space, if any. Pressure maintenance
    /// needs the store's maintenance reserve free, not the whole floor: the reserve is the
    /// capacity the budget withholds for the compaction and control writes it makes.
    pub(super) fn shortfall_below(&self, needed: u64) -> Result<Option<String>, Error> {
        let free = self.free()?;
        Ok((free < needed).then(|| {
            format!(
                "only {free} bytes free on the filesystem holding {}; pressure maintenance resumes at {needed} bytes",
                self.place.dir.display(),
            )
        }))
    }

    /// Describe a free-space shortfall, if any. A failed measurement is an error,
    /// never a shortfall, so callers that pause on low space cannot hide it.
    pub fn shortfall(&self) -> Result<Option<String>, Error> {
        self.shortfall_after(0)
    }

    /// Describe the shortfall left after `extra` more bytes are written, for
    /// maintenance that holds transient files above the floor.
    pub(super) fn shortfall_after(&self, extra: u64) -> Result<Option<String>, Error> {
        let free = self.free()?;
        let needed = self.min_free.saturating_add(extra);
        let resumes = if extra == 0 {
            "writes resume"
        } else {
            "it resumes"
        };
        Ok((free < needed).then(|| {
            format!(
                "only {free} bytes free on the filesystem holding {}; {resumes} at {needed} bytes",
                self.place.dir.display(),
            )
        }))
    }

    /// The floor itself, in bytes.
    pub(super) fn min_free(&self) -> u64 {
        self.min_free
    }

    /// Measured at the nearest existing ancestor, which shares the filesystem
    /// of a store directory that has not been created yet.
    fn free(&self) -> Result<u64, Error> {
        #[cfg(feature = "test-support")]
        {
            let free = self.place.ledger.free_override.load(Ordering::Acquire);
            if free != u64::MAX {
                return Ok(free);
            }
        }
        let measured = self
            .place
            .dir
            .ancestors()
            // A relative directory's last ancestor is empty: the working directory.
            .map(|path| {
                if path.as_os_str().is_empty() {
                    Path::new(".")
                } else {
                    path
                }
            })
            .find(|path| path.exists())
            .ok_or_else(|| {
                Error::new(
                    "STORAGE_FAILURE",
                    "store directory has no existing ancestor",
                )
            })?;
        let stat = rustix::fs::statvfs(measured).map_err(|error| {
            Error::new(
                "STORAGE_FAILURE",
                format!("free space of {}: {error}", measured.display()),
            )
        })?;
        Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Value: a floor pause left no trace in status (#211). Protects: a pause and its end
    // are each counted once however many duties observe the floor, the pause carries the
    // clock of its first observation, and refusals below the floor are counted.
    #[test]
    fn a_pause_and_its_end_are_counted_once_with_the_clock_of_the_first_observation() {
        let dir = tempfile::tempdir().unwrap();
        let low = SpaceFloor::new(dir.path(), u64::MAX / 4);
        assert_eq!(low.status(), FloorStatus::default());
        assert!(low.observe(5_000).unwrap().is_some());
        assert!(low.observe(6_000).unwrap().is_some());
        assert_eq!(low.check().unwrap_err().code, "STORAGE_PRESSURE");
        assert_eq!(
            low.status(),
            FloorStatus {
                paused_since: 5_000,
                pauses: 1,
                resumes: 0,
                refused_writes: 1
            }
        );
        // The same ledger seen by a floor whose threshold the free space now meets.
        let met = SpaceFloor {
            min_free: 1,
            ..low.clone()
        };
        assert!(met.observe(9_000).unwrap().is_none());
        assert!(met.observe(9_500).unwrap().is_none());
        assert_eq!(
            low.status(),
            FloorStatus {
                paused_since: 0,
                pauses: 1,
                resumes: 1,
                refused_writes: 1
            }
        );
        assert!(low.observe(10_000).unwrap().is_some());
        assert_eq!(low.status().pauses, 2);
    }

    // Value (#247): the resume edge was visible only to the observer that compared counters
    // around its own call, so another loop consuming it left the waiters unwoken. Protects:
    // the resume waker runs exactly once, in the observation that consumes the edge, whoever
    // that is, and not on the pause or on later observations.
    #[test]
    fn the_resume_waker_runs_once_in_the_observation_that_consumes_the_edge() {
        let dir = tempfile::tempdir().unwrap();
        let low = SpaceFloor::new(dir.path(), u64::MAX / 4);
        let woken = Arc::new(AtomicU64::new(0));
        let _first = low.subscribe_resume(Arc::new({
            let woken = Arc::clone(&woken);
            move || {
                woken.fetch_add(1, Ordering::SeqCst);
            }
        }));
        assert!(low.observe(5_000).unwrap().is_some());
        assert_eq!(woken.load(Ordering::SeqCst), 0);
        let met = SpaceFloor {
            min_free: 1,
            ..low.clone()
        };
        assert!(met.observe(6_000).unwrap().is_none());
        assert!(low.observe(7_000).unwrap().is_some());
        assert!(met.observe(8_000).unwrap().is_none());
        assert_eq!(woken.load(Ordering::SeqCst), 2, "one per resume edge");
        assert!(met.observe(9_000).unwrap().is_none());
        assert_eq!(woken.load(Ordering::SeqCst), 2);
    }

    // Value (#257, R5): reads staged up to 64 MiB of disk each with no regard for the floor, so
    // serving reads through a pause could spend the reserve the store's own flush and
    // compaction need. Protects: above the floor a read stages what it asked for; while paused
    // all reads together stage at most PAUSED_READ_STAGING_BYTES, and none so much that free
    // space falls below half the floor.
    #[cfg(feature = "test-support")]
    #[test]
    fn paused_reads_stage_a_bounded_share_and_leave_half_the_floor_alone() {
        const MIB: u64 = 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let floor = SpaceFloor::new(dir.path(), 1024 * MIB);
        floor.set_free_for_tests(Some(2048 * MIB));
        assert_eq!(
            floor.read_staging_allowance(64 * MIB, 500 * MIB).unwrap(),
            64 * MIB
        );
        floor.set_free_for_tests(Some(1024 * MIB - 1));
        assert_eq!(floor.read_staging_allowance(64 * MIB, 0).unwrap(), 32 * MIB);
        assert_eq!(
            floor.read_staging_allowance(64 * MIB, 30 * MIB).unwrap(),
            2 * MIB
        );
        assert_eq!(floor.read_staging_allowance(64 * MIB, 32 * MIB).unwrap(), 0);
        floor.set_free_for_tests(Some(513 * MIB));
        assert_eq!(floor.read_staging_allowance(64 * MIB, 0).unwrap(), MIB);
        floor.set_free_for_tests(Some(100 * MIB));
        assert_eq!(floor.read_staging_allowance(64 * MIB, 0).unwrap(), 0);
    }

    // Value (#257): the resume hook had one slot, owned by maintenance, so no other duty could
    // wait on the edge. Protects: every subscriber runs once per edge, and a dropped
    // subscription is no longer run.
    #[test]
    fn every_resume_subscriber_is_woken_and_a_dropped_one_is_not() {
        let dir = tempfile::tempdir().unwrap();
        let low = SpaceFloor::new(dir.path(), u64::MAX / 4);
        let met = SpaceFloor {
            min_free: 1,
            ..low.clone()
        };
        let counter = || {
            let count = Arc::new(AtomicU64::new(0));
            let waker: RetentionWaker = Arc::new({
                let count = Arc::clone(&count);
                move || {
                    count.fetch_add(1, Ordering::SeqCst);
                }
            });
            (count, waker)
        };
        let (first, first_waker) = counter();
        let (second, second_waker) = counter();
        let _first = low.subscribe_resume(first_waker);
        let second_subscription = low.subscribe_resume(second_waker);
        assert!(low.observe(1).unwrap().is_some());
        assert!(met.observe(2).unwrap().is_none());
        assert_eq!(
            (first.load(Ordering::SeqCst), second.load(Ordering::SeqCst)),
            (1, 1)
        );
        drop(second_subscription);
        assert!(low.observe(3).unwrap().is_some());
        assert!(met.observe(4).unwrap().is_none());
        assert_eq!(
            (first.load(Ordering::SeqCst), second.load(Ordering::SeqCst)),
            (2, 1)
        );
    }

    // Value (#257, review): the live volume does not yet show what earlier reads reserved, so
    // each concurrent read saw the whole headroom and together they could pass it (10 MiB of
    // headroom gave 10, 10, 10 and 2 MiB). Protects: reads that reserve in turn, each told the
    // bytes already reserved, are given no more than the headroom between them.
    #[cfg(feature = "test-support")]
    #[test]
    fn concurrent_paused_reads_together_stay_inside_the_headroom() {
        const MIB: u64 = 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let floor = SpaceFloor::new(dir.path(), 1024 * MIB);
        // Nothing reserved has been written, so the volume shows the same headroom to all.
        floor.set_free_for_tests(Some(512 * MIB + 10 * MIB));
        let mut reserved = 0;
        for _ in 0..4 {
            reserved += floor.read_staging_allowance(64 * MIB, reserved).unwrap();
        }
        assert_eq!(reserved, 10 * MIB);
    }

    // Value (#257): the daemon must tell a floor refusal from budget pressure without parsing
    // prose at every caller. Protects: check() marks exactly its own refusal, keeping the
    // code STORAGE_PRESSURE that older callers hold and retry.
    #[test]
    fn only_the_floor_check_labels_its_refusal_as_the_floor() {
        let dir = tempfile::tempdir().unwrap();
        let low = SpaceFloor::new(dir.path(), u64::MAX / 4);
        let refused = low.check().unwrap_err();
        assert_eq!(refused.code, "STORAGE_PRESSURE");
        assert!(refused.is_free_space_floor(), "{refused}");
        assert!(
            !Error::new(
                "STORAGE_PRESSURE",
                "write would cross the maintenance reserve"
            )
            .is_free_space_floor()
        );
        assert!(
            !Error::new("RESOURCE_LIMIT", format!("{FLOOR_REFUSAL_PREFIX}x")).is_free_space_floor()
        );
    }
}
