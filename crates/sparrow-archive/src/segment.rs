// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use std::fs;
use std::path::{Path, PathBuf};

use crate::decode::{Decoded, decode};
use crate::error::ArchiveError;

/// One record read back from a segment, with its bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchivedRecord {
    /// Original record identity, `record:<hex>`.
    pub id: String,
    /// Original parent artifact identity, `artifact:<hex>`.
    pub artifact_id: String,
    /// Original source label.
    pub source: String,
    /// Original receipt time in Unix milliseconds.
    pub received_at: u64,
    /// Half-open byte range in the parent artifact.
    pub byte_start: u64,
    /// End of the range, including the original delimiter.
    pub byte_end: u64,
    /// The record's exact bytes.
    pub bytes: Vec<u8>,
}

/// One artifact's metadata read back from a segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchivedArtifact {
    /// Original artifact identity, `artifact:<hex>`.
    pub id: String,
    /// Original source label.
    pub source: String,
    /// The format the artifact was split into records by.
    pub format: String,
    /// Original receipt time in Unix milliseconds.
    pub received_at: u64,
    /// Full original byte count.
    pub byte_length: u64,
    /// The BLAKE3 hash of all the artifact's own bytes, `blake3:` and 64 lower-case hex
    /// digits: the key `payloadLookup` uses on the live store.
    pub payload_id: String,
}

/// What `verify` reports for a segment that decoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Summary {
    /// Records with metadata in the segment.
    pub records: usize,
    /// Artifacts with metadata in the segment.
    pub artifacts: usize,
    /// Blobs in the segment.
    pub blobs: usize,
}

/// One segment, held in memory and decoded.
#[derive(Debug)]
pub struct Segment {
    path: PathBuf,
    decoded: Decoded,
}

fn io(path: &Path) -> impl FnOnce(std::io::Error) -> ArchiveError + '_ {
    move |source| ArchiveError::Io {
        path: path.to_owned(),
        source,
    }
}

/// Every segment file (`*.gts`) under `root`, sorted by path. `.staging` is never searched.
/// This writes nothing.
///
/// # Errors
/// An unreadable directory.
pub fn list_segments(root: &Path) -> Result<Vec<PathBuf>, ArchiveError> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_owned()];
    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(&dir).map_err(io(&dir))? {
            let entry = entry.map_err(io(&dir))?;
            let kind = entry.file_type().map_err(io(&dir))?;
            let path = entry.path();
            if kind.is_dir() && entry.file_name() != ".staging" {
                pending.push(path);
            } else if kind.is_file() && path.extension().is_some_and(|e| e == "gts") {
                found.push(path);
            }
        }
    }
    found.sort();
    Ok(found)
}

impl Segment {
    /// Read the segment file at `path` into memory and decode it. Writes nothing.
    ///
    /// # Errors
    /// An unreadable file, or the decoder's refusal of the contents.
    pub fn open(path: &Path) -> Result<Self, ArchiveError> {
        let bytes = fs::read(path).map_err(io(path))?;
        Self::from_bytes(path, &bytes)
    }

    /// Decode a segment already in memory, such as one the segment writer just prepared.
    /// `name` only names it in errors.
    ///
    /// # Errors
    /// The decoder's refusal of the contents.
    pub fn from_bytes(name: &Path, bytes: &[u8]) -> Result<Self, ArchiveError> {
        let decoded = decode(bytes).map_err(|reason| ArchiveError::Corrupt {
            path: name.to_owned(),
            reason,
        })?;
        Ok(Self {
            path: name.to_owned(),
            decoded,
        })
    }

    /// The segment's file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Counts for `verify`.
    #[must_use]
    pub fn summary(&self) -> Summary {
        Summary {
            records: self.decoded.records.len(),
            artifacts: self.decoded.artifacts.len(),
            blobs: self.decoded.blobs,
        }
    }

    /// Every record in the segment with its bytes, sorted by identity.
    #[must_use]
    pub fn records(&self) -> &[ArchivedRecord] {
        &self.decoded.records
    }

    /// Every artifact in the segment, sorted by identity.
    #[must_use]
    pub fn artifacts(&self) -> &[ArchivedArtifact] {
        &self.decoded.artifacts
    }
}
