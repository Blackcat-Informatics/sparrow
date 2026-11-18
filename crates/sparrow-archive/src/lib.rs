// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Offline reader for archived Sparrow segments (`docs/archive-format.md`).
//!
//! A segment is one GTS file, read into memory and decoded by one function,
//! `decode::decode`, the only code that knows the byte format. Nothing here opens a live
//! store or writes to the archive; [`extract`] returns one artifact's bytes to the caller.

mod decode;
mod error;
mod extract;
mod segment;

pub use error::ArchiveError;
pub use extract::extract;
pub use segment::{ArchivedArtifact, ArchivedRecord, Segment, Summary, list_segments};

/// Decode one segment file and discard the result: `Ok` when it reads as GTS and every
/// record's bytes hash to the digest its link quad names, otherwise the reason it does not.
/// This is the decoder `verify` and [`extract`] run, and the one the archive worker runs on
/// each segment before it commits the watermark, so a blob that does not match its link is
/// refused before the original can be retired.
///
/// # Errors
/// The first problem found: a reader diagnostic, a missing segment head, a missing or
/// malformed fact, or a record whose bytes do not match its link digest.
pub fn verify(bytes: &[u8]) -> Result<(), String> {
    decode::decode(bytes).map(drop)
}
