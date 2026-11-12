// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Count of gap evidence lost because a stream registry could not hold a window (#215).
//!
//! A full producer-stream or receiver-window registry never refuses a log: the log is stored
//! without a window position. What is lost is that stream's gap evidence for those positions.
//! The loss is counted here, per stream, in memory for the life of the process, and shown in
//! status. Nothing here is a stored format.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

/// Streams named individually in a report; the rest fold into the `other` counters.
pub const MAX_NAMED_LOST_STREAMS: usize = 16;
/// Distinct streams the ledger tracks. Further streams still count, in the overflow totals.
const MAX_TRACKED_LOST_STREAMS: usize = 1024;

/// Which registry could not hold the stream's window.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LostStreamKind {
    /// A client producer stream, identified by source and producer instance.
    Producer,
    /// A syslog receiver window, identified by source and receiver instance.
    Receiver,
}

/// One stream whose gap evidence was not recorded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LostStream {
    /// Registry that was full.
    pub kind: LostStreamKind,
    /// Exact source label.
    pub source: String,
    /// Producer or receiver instance.
    pub instance: String,
    /// Accepted batches (or receiver observations) stored without a window position.
    pub batches: u64,
    /// Positions (producer sequences or receiver observations) with no recorded evidence.
    pub events: u64,
}

/// Bounded status view of the ledger.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamEvidenceLoss {
    /// The streams with the most lost positions, at most [`MAX_NAMED_LOST_STREAMS`].
    pub named: Vec<LostStream>,
    /// Tracked streams beyond the named ones.
    pub other_streams: u64,
    /// Batches of those streams, including streams the ledger no longer tracks.
    pub other_batches: u64,
    /// Positions of those streams, including streams the ledger no longer tracks.
    pub other_events: u64,
    /// False when streams beyond the tracked limit exist, so `other_streams` is a lower bound.
    pub other_streams_exact: bool,
    /// Total batches over every stream.
    pub total_batches: u64,
    /// Total positions over every stream.
    pub total_events: u64,
}

type Key = (LostStreamKind, String, String);

#[derive(Default)]
struct State {
    tracked: BTreeMap<Key, (u64, u64)>,
    untracked: (u64, u64),
}

/// Shared by every clone of a store.
#[derive(Default)]
pub(super) struct Ledger(Mutex<State>);

impl Ledger {
    pub(super) fn record(&self, kind: LostStreamKind, source: &str, instance: &str, events: u64) {
        let mut guard = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let State { tracked, untracked } = &mut *guard;
        let key = (kind, source.to_owned(), instance.to_owned());
        let full = tracked.len() >= MAX_TRACKED_LOST_STREAMS;
        let slot = match tracked.get_mut(&key) {
            Some(slot) => slot,
            None if full => {
                untracked.0 = untracked.0.saturating_add(1);
                untracked.1 = untracked.1.saturating_add(events);
                return;
            }
            None => tracked.entry(key).or_default(),
        };
        slot.0 = slot.0.saturating_add(1);
        slot.1 = slot.1.saturating_add(events);
    }

    pub(super) fn report(&self) -> StreamEvidenceLoss {
        let state = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let mut rows: Vec<LostStream> = state
            .tracked
            .iter()
            .map(|((kind, source, instance), (batches, events))| LostStream {
                kind: *kind,
                source: source.clone(),
                instance: instance.clone(),
                batches: *batches,
                events: *events,
            })
            .collect();
        // Most lost first; the BTreeMap order keeps ties deterministic.
        rows.sort_by_key(|row| std::cmp::Reverse(row.events));
        let rest = rows.split_off(rows.len().min(MAX_NAMED_LOST_STREAMS));
        let mut report = StreamEvidenceLoss {
            other_streams: rest.len() as u64,
            other_batches: state.untracked.0,
            other_events: state.untracked.1,
            other_streams_exact: state.untracked == (0, 0),
            ..StreamEvidenceLoss::default()
        };
        for row in &rest {
            report.other_batches = report.other_batches.saturating_add(row.batches);
            report.other_events = report.other_events.saturating_add(row.events);
        }
        for row in rows.iter().chain(&rest) {
            report.total_batches = report.total_batches.saturating_add(row.batches);
            report.total_events = report.total_events.saturating_add(row.events);
        }
        report.total_batches = report.total_batches.saturating_add(state.untracked.0);
        report.total_events = report.total_events.saturating_add(state.untracked.1);
        report.named = rows;
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// INVARIANT (#215): past the tracked-stream limit a lost stream is still counted, in the
    /// overflow totals, and the report says `other_streams` is only a lower bound. No lost
    /// position disappears from the totals.
    #[test]
    fn streams_past_the_tracking_limit_still_count_and_mark_the_report_inexact() {
        let ledger = Ledger::default();
        for index in 0..MAX_TRACKED_LOST_STREAMS {
            ledger.record(LostStreamKind::Producer, "s", &index.to_string(), 1);
        }
        assert!(ledger.report().other_streams_exact);
        ledger.record(LostStreamKind::Producer, "s", "beyond", 5);
        ledger.record(LostStreamKind::Producer, "s", "beyond", 5);
        ledger.record(LostStreamKind::Producer, "s", "0", 1);
        let report = ledger.report();
        assert!(!report.other_streams_exact);
        assert_eq!(report.named.len(), MAX_NAMED_LOST_STREAMS);
        assert_eq!(
            report.other_streams,
            (MAX_TRACKED_LOST_STREAMS - MAX_NAMED_LOST_STREAMS) as u64
        );
        assert_eq!(
            report.total_events,
            MAX_TRACKED_LOST_STREAMS as u64 + 10 + 1
        );
        assert_eq!(report.total_batches, MAX_TRACKED_LOST_STREAMS as u64 + 3);
        assert!(report.other_events >= 10);
    }
}
