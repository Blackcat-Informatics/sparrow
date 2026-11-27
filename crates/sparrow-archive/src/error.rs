// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use std::fmt;
use std::path::PathBuf;

/// Why a segment could not be read, decoded or extracted. Every variant names the path or
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
    /// No segment holds the artifact.
    NotFound {
        /// The artifact identity asked for.
        artifact_id: String,
    },
    /// The payload identity asked for is not `blake3:` followed by 64 lower-case hex digits.
    InvalidPayloadId {
        /// The value as given.
        value: String,
    },
}

impl fmt::Display for ArchiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => write!(f, "cannot read {}: {source}", path.display()),
            Self::Corrupt { path, reason } => {
                write!(f, "segment {} is corrupt: {reason}", path.display())
            }
            Self::Incomplete { reason } => write!(f, "segment evidence is incomplete: {reason}"),
            Self::NotFound { artifact_id } => {
                write!(f, "no segment under the directory holds {artifact_id}")
            }
            Self::InvalidPayloadId { value } => write!(
                f,
                "{value:?} is not a payload id: expected blake3: and 64 lower-case hex digits"
            ),
        }
    }
}

impl std::error::Error for ArchiveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        let Self::Io { source, .. } = self else {
            return None;
        };
        Some(source)
    }
}
