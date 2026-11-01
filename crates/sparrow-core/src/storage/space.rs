// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One filesystem free-space floor shared by every store write and daemon admission.

use super::Error;
use std::{path::Path, sync::Arc};

/// Free space the filesystem holding a store must keep before Sparrow writes.
///
/// The physical `disk_budget` bounds the store's own bytes; it cannot see other
/// writers filling the same filesystem. A write that fails with no space left
/// latches durability uncertainty until the store is reopened, so writes stop
/// here first with a retriable refusal.
#[derive(Clone, Debug)]
pub struct SpaceFloor {
    dir: Arc<Path>,
    min_free: u64,
}

impl SpaceFloor {
    /// A floor for the filesystem that holds (or will hold) `dir`.
    #[must_use]
    pub fn new(dir: &Path, min_free: u64) -> Self {
        Self {
            dir: Arc::from(dir),
            min_free,
        }
    }

    /// Refuse while unprivileged free space is below the floor.
    pub fn check(&self) -> Result<(), Error> {
        match self.shortfall()? {
            Some(shortfall) => Err(Error::new("STORAGE_PRESSURE", shortfall)),
            None => Ok(()),
        }
    }

    /// Describe a free-space shortfall, if any. A failed measurement is an error,
    /// never a shortfall, so callers that pause on low space cannot hide it.
    pub fn shortfall(&self) -> Result<Option<String>, Error> {
        let free = self.free()?;
        Ok((free < self.min_free).then(|| {
            format!(
                "only {free} bytes free on the filesystem holding {}; writes resume at {} bytes",
                self.dir.display(),
                self.min_free
            )
        }))
    }

    /// Measured at the nearest existing ancestor, which shares the filesystem
    /// of a store directory that has not been created yet.
    fn free(&self) -> Result<u64, Error> {
        let measured = self
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
