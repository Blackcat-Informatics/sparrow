// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The archive worker's state as `status` reports it (#365).
//!
//! The worker publishes a small copy each time a step ends; `status` reads the copy under its
//! own lock. It never waits on the worker and never takes the store's writer lock, so a worker
//! stuck on a hung mount still reports where it stopped.

use crate::sparrow_capnp as wire;
use sparrow_core::MARK_PROJECTION_FAILED;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// The record id and reason of a named record, or two empty strings when there is none.
fn named(record: Option<&RecordReason>) -> (&str, &str) {
    record.map_or(("", ""), |record| {
        (record.record_id.as_str(), record.reason)
    })
}

/// The longest reason status carries, in characters. A decoder or filesystem message can be
/// long; the journal line holds it whole.
const REASON_CHARS: usize = 512;

/// Where the worker is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Phase {
    /// The daemon runs no archive worker.
    #[default]
    NotConfigured,
    /// Between passes, nothing failed.
    Idle,
    /// Choosing the next segment from the store.
    Preparing,
    /// Checking the root and writing the segment's files.
    Writing,
    /// Reading the files back and decoding them, then committing.
    Verifying,
    /// The last pass failed; the worker waits to retry and the watermark has not moved.
    Holding,
}

/// The last failed pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Failure {
    /// The error class.
    pub(crate) code: &'static str,
    /// The segment file's name; empty when the failure came before a segment was chosen.
    pub(crate) segment: String,
    /// The cause, cut to a bounded length.
    pub(crate) reason: String,
    /// Unix milliseconds the failure began; absent while the clock could not be read.
    pub(crate) since: Option<u64>,
}

/// A record the archive names: the one a hold stands on, or the last one marked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecordReason {
    /// The record's id.
    pub(crate) record_id: String,
    /// Why the record is held or marked.
    pub(crate) reason: &'static str,
}

/// A copy of the published state.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Snapshot {
    pub(crate) phase: Phase,
    /// The durable `archive:through` watermark; absent before the first commit.
    pub(crate) through: Option<String>,
    /// A prepared segment is recorded and not yet committed.
    pub(crate) intent_in_flight: bool,
    pub(crate) failure: Option<Failure>,
    /// Passes that panicked and were restarted since the daemon started.
    pub(crate) restarts: u64,
    /// The record the last pass that reached its hold check found held; absent when none.
    pub(crate) held: Option<RecordReason>,
    /// Records marked (projection absent) in the last committed segment.
    pub(crate) last_segment_marked: u64,
    /// Records marked in committed segments since the daemon started.
    pub(crate) marked_total: u64,
    /// The last record marked in a committed segment since the daemon started.
    pub(crate) last_marked: Option<RecordReason>,
}

/// The shared copy: the worker writes it, `status` reads it.
#[derive(Clone, Debug, Default)]
pub struct ArchiveStatus {
    inner: Arc<Mutex<Snapshot>>,
}

impl ArchiveStatus {
    /// A daemon with no archive worker: phase `notConfigured`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, Snapshot> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[must_use]
    pub(crate) fn snapshot(&self) -> Snapshot {
        self.lock().clone()
    }

    pub(crate) fn phase(&self, phase: Phase) {
        self.lock().phase = phase;
    }

    pub(crate) fn position(&self, through: Option<String>, intent_in_flight: bool) {
        let mut state = self.lock();
        state.through = through;
        state.intent_in_flight = intent_in_flight;
    }

    /// A pass ended without error: the worker is idle and no failure stands.
    pub(crate) fn pass_succeeded(&self) {
        let mut state = self.lock();
        state.phase = Phase::Idle;
        state.failure = None;
    }

    /// Count a pass that panicked and was restarted; returns the count.
    pub(crate) fn restarted(&self) -> u64 {
        let mut state = self.lock();
        state.restarts += 1;
        state.restarts
    }

    /// The record the hold check found, or `None` when the head is free.
    pub(crate) fn held(&self, held: Option<RecordReason>) {
        self.lock().held = held;
    }

    /// A segment committed with these marked records, in eviction order.
    pub(crate) fn committed_marks(&self, marked: &[String]) {
        let mut state = self.lock();
        state.last_segment_marked = marked.len() as u64;
        state.marked_total += marked.len() as u64;
        if let Some(record_id) = marked.last() {
            state.last_marked = Some(RecordReason {
                record_id: record_id.clone(),
                reason: MARK_PROJECTION_FAILED,
            });
        }
    }

    /// A pass failed at `now`, when the clock could be read. A failure with the same code and
    /// segment as the one standing keeps its start, so `since` dates the trouble and not the
    /// latest retry; a start missing for want of a clock is filled in once `now` is known.
    pub(crate) fn pass_failed(
        &self,
        code: &'static str,
        segment: &str,
        reason: &str,
        now: Option<u64>,
    ) {
        let mut state = self.lock();
        let since = match &state.failure {
            Some(standing) if standing.code == code && standing.segment == segment => {
                standing.since.or(now)
            }
            _ => now,
        };
        state.phase = Phase::Holding;
        state.failure = Some(Failure {
            code,
            segment: segment.to_owned(),
            reason: reason.chars().take(REASON_CHARS).collect(),
            since,
        });
    }

    pub(crate) fn encode(&self, mut out: wire::archive_worker_status::Builder<'_>) {
        let state = self.snapshot();
        out.set_phase(match state.phase {
            Phase::NotConfigured => wire::ArchivePhase::NotConfigured,
            Phase::Idle => wire::ArchivePhase::Idle,
            Phase::Preparing => wire::ArchivePhase::Preparing,
            Phase::Writing => wire::ArchivePhase::Writing,
            Phase::Verifying => wire::ArchivePhase::Verifying,
            Phase::Holding => wire::ArchivePhase::Holding,
        });
        out.set_through(state.through.as_deref().unwrap_or(""));
        out.set_intent_in_flight(state.intent_in_flight);
        out.set_restarts(state.restarts);
        let (held_id, held_reason) = named(state.held.as_ref());
        out.set_held_record_id(held_id);
        out.set_held_reason(held_reason);
        out.set_last_segment_marked(state.last_segment_marked);
        out.set_marked_total(state.marked_total);
        let (marked_id, marked_reason) = named(state.last_marked.as_ref());
        out.set_last_marked_record_id(marked_id);
        out.set_last_marked_reason(marked_reason);
        match state.failure {
            None => out.init_failure().set_absent(()),
            Some(failure) => {
                let mut value = out.init_failure().init_value();
                value.set_code(failure.code);
                value.set_segment(&failure.segment);
                value.set_reason(&failure.reason);
                let mut since = value.init_since();
                match failure.since {
                    None => since.set_absent(()),
                    Some(millis) => since.init_value().set_unix_millis(millis),
                }
            }
        }
    }
}
