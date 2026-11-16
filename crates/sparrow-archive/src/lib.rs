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
