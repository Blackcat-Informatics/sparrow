// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Files tailed without a stored checkpoint, and registry rows retired for room (#224).
//!
//! A full generation registry never refuses a file. A finished row is retired, or the file is
//! tailed under a generation that exists only in the writer. Such a file is read again from its
//! start after a restart, so the records already stored for it are at risk of duplication. This
//! ledger counts them per file, in memory for the life of the process, and status shows them.
//! Nothing here is a stored format.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

/// Files named individually in a report; the rest fold into the `other` counters.
pub const MAX_NAMED_UNRECORDED_TAILS: usize = 16;
/// Distinct files the ledger tracks. Further files still count, in the overflow totals.
const MAX_TRACKED_FILES: usize = 1024;

/// One file tailed without a stored checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnrecordedTail {
    /// Configured watch.
    pub watch: String,
    /// Relative path, with any non-UTF-8 byte shown as U+FFFD.
    pub path: String,
    /// Logical source label of the file's most recent unrecorded generation.
    pub source: String,
    /// Generations of this file that had no stored checkpoint.
    pub generations: u64,
    /// Records stored under those generations; a restart stores them again.
    pub records: u64,
    /// File bytes behind those records.
    pub bytes: u64,
}

/// Bounded status view of the ledger.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TailRegistryEvidence {
    /// The files with the most bytes at risk, at most [`MAX_NAMED_UNRECORDED_TAILS`].
    pub named: Vec<UnrecordedTail>,
    /// Tracked files beyond the named ones.
    pub other_files: u64,
    /// Generations of those files, including files the ledger no longer tracks.
    pub other_generations: u64,
    /// Records of those files, including files the ledger no longer tracks.
    pub other_records: u64,
    /// Bytes of those files, including files the ledger no longer tracks.
    pub other_bytes: u64,
    /// False when files beyond the tracked limit exist, so `other_files` is a lower bound.
    pub other_files_exact: bool,
    /// Total generations over every file.
    pub total_generations: u64,
    /// Total records over every file.
    pub total_records: u64,
    /// Total bytes over every file.
    pub total_bytes: u64,
    /// Finished rows retired from the stored registry to make room.
    pub retired_terminal: u64,
    /// Of those, rows of a complete generation whose file, if it is still present, is read
    /// again from its start.
    pub retired_duplicate_risk: u64,
}

type Key = (String, String);

#[derive(Default)]
struct State {
    tracked: BTreeMap<Key, UnrecordedTail>,
    untracked: (u64, u64, u64),
    retired: (u64, u64),
}

/// Shared by every clone of a store.
#[derive(Default)]
pub(super) struct Ledger(Mutex<State>);

impl Ledger {
    fn with<T>(&self, apply: impl FnOnce(&mut State) -> T) -> T {
        apply(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// A file began a generation that has no stored checkpoint.
    pub(super) fn started(&self, watch: &str, path: &[u8], source: &str) {
        let key = (watch.to_owned(), String::from_utf8_lossy(path).into_owned());
        self.with(|state| {
            let full = state.tracked.len() >= MAX_TRACKED_FILES;
            if full && !state.tracked.contains_key(&key) {
                state.untracked.0 = state.untracked.0.saturating_add(1);
                return;
            }
            let row = state
                .tracked
                .entry(key.clone())
                .or_insert_with(|| UnrecordedTail {
                    watch: key.0,
                    path: key.1,
                    source: source.to_owned(),
                    generations: 0,
                    records: 0,
                    bytes: 0,
                });
            source.clone_into(&mut row.source);
            row.generations = row.generations.saturating_add(1);
        });
    }

    /// Records were stored under a generation that has no stored checkpoint.
    pub(super) fn published(&self, watch: &str, path: &[u8], records: u64, bytes: u64) {
        let key = (watch.to_owned(), String::from_utf8_lossy(path).into_owned());
        self.with(|state| {
            if let Some(row) = state.tracked.get_mut(&key) {
                row.records = row.records.saturating_add(records);
                row.bytes = row.bytes.saturating_add(bytes);
            } else {
                state.untracked.1 = state.untracked.1.saturating_add(records);
                state.untracked.2 = state.untracked.2.saturating_add(bytes);
            }
        });
    }

    /// A finished row left the stored registry; `duplicate_risk` for a complete generation.
    pub(super) fn retired(&self, duplicate_risk: bool) {
        self.with(|state| {
            state.retired.0 = state.retired.0.saturating_add(1);
            state.retired.1 = state.retired.1.saturating_add(u64::from(duplicate_risk));
        });
    }

    pub(super) fn report(&self) -> TailRegistryEvidence {
        self.with(|state| {
            let mut rows: Vec<UnrecordedTail> = state.tracked.values().cloned().collect();
            // Most bytes at risk first; the BTreeMap order keeps ties deterministic.
            rows.sort_by_key(|row| std::cmp::Reverse(row.bytes));
            let rest = rows.split_off(rows.len().min(MAX_NAMED_UNRECORDED_TAILS));
            let mut report = TailRegistryEvidence {
                other_files: rest.len() as u64,
                other_generations: state.untracked.0,
                other_records: state.untracked.1,
                other_bytes: state.untracked.2,
                other_files_exact: state.untracked == (0, 0, 0),
                total_generations: state.untracked.0,
                total_records: state.untracked.1,
                total_bytes: state.untracked.2,
                retired_terminal: state.retired.0,
                retired_duplicate_risk: state.retired.1,
                ..TailRegistryEvidence::default()
            };
            for row in &rest {
                report.other_generations = report.other_generations.saturating_add(row.generations);
                report.other_records = report.other_records.saturating_add(row.records);
                report.other_bytes = report.other_bytes.saturating_add(row.bytes);
            }
            for row in rows.iter().chain(&rest) {
                report.total_generations = report.total_generations.saturating_add(row.generations);
                report.total_records = report.total_records.saturating_add(row.records);
                report.total_bytes = report.total_bytes.saturating_add(row.bytes);
            }
            report.named = rows;
            report
        })
    }
}
