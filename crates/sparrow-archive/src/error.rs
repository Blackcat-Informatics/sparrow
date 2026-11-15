// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use std::fmt;
use std::path::PathBuf;

/// Why a segment could not be read, decoded or restored. Every variant names the path or
/// evidence it concerns, so a failure is never a silent pass.
#[derive(Debug)]
pub enum ArchiveError {
    /// A file or directory could not be read.
    Io {
        /// The path that failed.
        path: PathBuf,
        /// The operating system's cause.
        source: std::io::Error,
    },
    /// PurRDF refused the segment's tables (corruption, schema drift, a blob digest mismatch).
    Corrupt {
        /// The segment directory.
        path: PathBuf,
        /// PurRDF's reason.
        reason: String,
    },
    /// The tables decode, but the evidence in them is not complete or consistent.
    Incomplete {
        /// What is missing or inconsistent, naming the evidence.
        reason: String,
    },
    /// The destination store refused a restore.
    Store(sparrow_core::Error),
}

impl fmt::Display for ArchiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => write!(f, "cannot read {}: {source}", path.display()),
            Self::Corrupt { path, reason } => {
                write!(f, "segment {} is corrupt: {reason}", path.display())
            }
            Self::Incomplete { reason } => write!(f, "segment evidence is incomplete: {reason}"),
            Self::Store(error) => write!(f, "store refused the restore: {error}"),
        }
    }
}

impl std::error::Error for ArchiveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Store(error) => Some(error),
            _ => None,
        }
    }
}
