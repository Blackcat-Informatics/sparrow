// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Offline reader for archived Sparrow segments (`docs/archive-format.md`).
//!
//! A segment is one GTS file, read into memory and decoded by one function,
//! `decode::decode`, the only code that knows the byte format. Nothing here opens a live store or writes to the archive;
//! [`restore`] writes only into the store the caller hands it.

mod decode;
mod error;
mod restore;
mod segment;

pub use error::ArchiveError;
pub use restore::{RestoreReport, restore};
pub use segment::{ArchivedArtifact, ArchivedRecord, Segment, Summary, list_segments};
