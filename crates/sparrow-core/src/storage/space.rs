// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One filesystem free-space floor shared by every store write and daemon admission.

use super::Error;
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
#[derive(Debug, Default)]
struct Ledger {
    paused: AtomicBool,
    /// Unix milliseconds the current pause began; zero while not paused.
    since: AtomicU64,
    pauses: AtomicU64,
    resumes: AtomicU64,
    /// Writes refused by [`SpaceFloor::check`].
    refused: AtomicU64,
    /// Dev-only: answer every [`SpaceFloor::check`] as below the floor.
    #[cfg(feature = "test-support")]
    forced: AtomicBool,
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
                ledger: Ledger::default(),
            }),
            min_free,
        }
    }

    /// Refuse while unprivileged free space is below the floor.
    pub fn check(&self) -> Result<(), Error> {
        #[cfg(feature = "test-support")]
        if self.place.ledger.forced.load(Ordering::Acquire) {
            self.place.ledger.refused.fetch_add(1, Ordering::Relaxed);
            return Err(Error::new("STORAGE_PRESSURE", "write refused by a test"));
        }
        match self.shortfall()? {
            Some(shortfall) => {
                self.place.ledger.refused.fetch_add(1, Ordering::Relaxed);
                Err(Error::new("STORAGE_PRESSURE", shortfall))
            }
            None => Ok(()),
        }
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
        }
        Ok(shortfall)
    }

    /// Dev-only: refuse (or again admit) every write at the admission check, as a store under
    /// pressure does, without moving the measured free space the listeners' accept pause reads.
    #[cfg(feature = "test-support")]
    pub fn refuse_writes_for_tests(&self, refused: bool) {
        self.place.ledger.forced.store(refused, Ordering::Release);
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
}
