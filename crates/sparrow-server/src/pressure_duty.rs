// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The pressure duty's schedule and journal lines (#211).
//!
//! A pass used to run on a fixed one-second tick and its status was discarded, so a wait that
//! never ended left no trace but the refusals it caused. This owner turns each pass's status
//! into the next delay and the lines the daemon journals. It reads no clock: the pass's own
//! clock reading travels in the status, so every decision is reproducible from the statuses.
//!
//! The delay is one second while the store is ready or a pass made progress. A pass that
//! made none doubles it up to [`BACKOFF_CEILING`]: waiting a shorter time cannot change the
//! outcome of a wait for native work. Events that can change the outcome (a pin released,
//! an archive commit, a forced compaction ending) wake the worker at once instead.

use sparrow_core::{PressurePhase, PressureStatus};
use std::time::Duration;

/// Delay while ready or while passes make progress.
pub(crate) const BASE_DELAY: Duration = Duration::from_secs(1);
/// Longest delay between passes that make no progress.
pub(crate) const BACKOFF_CEILING: Duration = Duration::from_secs(30);
/// Least time between journal lines about passes that made no progress.
const QUIET_REPORT_MS: u64 = 60_000;

/// What the worker does after a pass.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Verdict {
    /// Journal lines, in order.
    pub(crate) lines: Vec<String>,
    /// Time to the next pass, unless an event wakes the worker first.
    pub(crate) delay: Duration,
}

/// The facts whose change counts as progress.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Signature {
    phase: PressurePhase,
    linked_bytes: u64,
    passes: u64,
    forced_compactions: u64,
    forced_failures: u64,
    waits_released: u64,
    pending_flushes: u64,
    pending_compactions: u64,
}

impl Signature {
    fn of(status: &PressureStatus) -> Self {
        Self {
            phase: status.phase,
            linked_bytes: status.linked_bytes,
            passes: status.passes,
            forced_compactions: status.forced_compactions,
            forced_failures: status.forced_failures,
            waits_released: status.waits_released,
            pending_flushes: status.pending_flushes,
            pending_compactions: status.pending_compactions,
        }
    }
}

pub(crate) struct Duty {
    last: Option<Signature>,
    phase: PressurePhase,
    /// Start of the episode being reported, to give its length when it ends.
    episode_since: u64,
    delay: Duration,
    /// Clock reading of the last journal line about a pass that made no progress.
    reported_at: Option<u64>,
    /// Passes without progress since that line.
    quiet: u64,
    /// Forced-compaction failures already journaled.
    failures_reported: u64,
}

impl Default for Duty {
    fn default() -> Self {
        Self {
            last: None,
            phase: PressurePhase::Ready,
            episode_since: 0,
            delay: BASE_DELAY,
            reported_at: None,
            quiet: 0,
            failures_reported: 0,
        }
    }
}

pub(crate) const fn phase_name(phase: PressurePhase) -> &'static str {
    match phase {
        PressurePhase::Ready => "ready",
        PressurePhase::Requested => "requested",
        PressurePhase::AwaitingReclamation => "awaiting_reclamation",
        PressurePhase::Blocked => "blocked",
        PressurePhase::AwaitingArchive => "awaiting_archive",
    }
}

fn seconds(ms: u64) -> String {
    format!("{}.{:03}s", ms / 1000, ms % 1000)
}

fn detail(status: &PressureStatus) -> String {
    format!(
        "requested_bytes={} linked_bytes={} wait_before_bytes={} wait_age={} episode_age={} passes={} evicted_records={} forced_compactions={} waits_released={} stalled_passes={} refused_writes={} forced_compaction_ms={} forced_failures={} native_errors={} waiting_on={}",
        status.requested_bytes,
        status.linked_bytes,
        status.wait_before_bytes,
        seconds(status.wait_age_ms()),
        seconds(status.episode_age_ms()),
        status.passes,
        status.evicted_records,
        status.forced_compactions,
        status.waits_released,
        status.stalled_passes,
        status.refused_writes,
        status.forced_compaction_ms,
        status.forced_failures,
        status.native_errors,
        status.waiting_on.as_deref().unwrap_or("nothing"),
    )
}

impl Duty {
    /// Account for one completed pass.
    pub(crate) fn observe(&mut self, status: &PressureStatus) -> Verdict {
        let mut lines = Vec::new();
        let signature = Signature::of(status);
        let progress = self.last != Some(signature);
        self.last = Some(signature);
        // A failed forced compaction is journaled the pass it is seen, never rate-limited.
        if status.forced_failures > self.failures_reported {
            self.failures_reported = status.forced_failures;
            lines.push(format!(
                "sparrowd: a forced compaction failed after {} ms: {}",
                status.forced_compaction_ms,
                status.failure.as_deref().unwrap_or("no reason recorded")
            ));
        }
        if status.phase != self.phase {
            let from = self.phase;
            if status.phase == PressurePhase::Ready {
                lines.push(format!(
                    "sparrowd: pressure {} -> ready after {}: {}",
                    phase_name(from),
                    seconds(status.observed_at.saturating_sub(self.episode_since)),
                    detail(status)
                ));
            } else {
                lines.push(format!(
                    "sparrowd: pressure {} -> {}: {}",
                    phase_name(from),
                    phase_name(status.phase),
                    detail(status)
                ));
            }
            self.phase = status.phase;
            self.reported_at = Some(status.observed_at);
            self.quiet = 0;
        }
        if status.phase != PressurePhase::Ready {
            self.episode_since = status.episode_since;
            if !progress {
                self.quiet += 1;
                let due = self
                    .reported_at
                    .is_none_or(|at| status.observed_at.saturating_sub(at) >= QUIET_REPORT_MS);
                if due {
                    lines.push(format!(
                        "sparrowd: pressure still {}; {} passes made no progress since the last report: {}",
                        phase_name(status.phase),
                        self.quiet,
                        detail(status)
                    ));
                    self.reported_at = Some(status.observed_at);
                    self.quiet = 0;
                }
            }
        }
        self.delay = if status.phase == PressurePhase::Ready || progress {
            BASE_DELAY
        } else {
            self.delay.saturating_mul(2).min(BACKOFF_CEILING)
        };
        Verdict {
            lines,
            delay: self.delay,
        }
    }

    /// Account for a pass that failed at `now`; the failure is a latched `Blocked` state
    /// in the store, and it is retried on the same backoff.
    pub(crate) fn failed(&mut self, now: u64, error: &str) -> Verdict {
        self.last = None;
        self.quiet += 1;
        let due = self
            .reported_at
            .is_none_or(|at| now.saturating_sub(at) >= QUIET_REPORT_MS);
        let mut lines = Vec::new();
        if due {
            lines.push(format!(
                "sparrowd: pressure maintenance failed ({} failures since the last report): {error}",
                self.quiet
            ));
            self.reported_at = Some(now);
            self.quiet = 0;
        }
        self.phase = PressurePhase::Blocked;
        self.delay = self.delay.saturating_mul(2).min(BACKOFF_CEILING);
        Verdict {
            lines,
            delay: self.delay,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn waiting(at: u64, linked: u64) -> PressureStatus {
        PressureStatus {
            phase: PressurePhase::AwaitingReclamation,
            observed_at: at,
            linked_bytes: linked,
            episode_since: 1_000,
            wait_since: 1_000,
            wait_before_bytes: 500,
            requested_bytes: 64,
            waiting_on: Some("a forced compaction is running".into()),
            ..PressureStatus::default()
        }
    }

    // Value: the old duty discarded the status, so a wait that never ended left no trace.
    // Protects: each entry to and exit from a non-ready phase is journaled once, with the
    // measures an operator needs, and the exit names how long the episode lasted.
    #[test]
    fn entry_and_exit_of_a_non_ready_phase_are_journaled_once() {
        let mut duty = Duty::default();
        let entered = duty.observe(&waiting(2_000, 900));
        assert_eq!(entered.lines.len(), 1);
        assert!(entered.lines[0].contains("ready -> awaiting_reclamation"));
        assert!(entered.lines[0].contains("wait_age=1.000s"));
        assert!(entered.lines[0].contains("waiting_on=a forced compaction is running"));
        let ready = duty.observe(&PressureStatus {
            observed_at: 9_000,
            ..PressureStatus::default()
        });
        assert_eq!(ready.lines.len(), 1);
        assert!(ready.lines[0].contains("awaiting_reclamation -> ready after 8.000s"));
        assert_eq!(ready.delay, BASE_DELAY);
        // Staying ready says nothing.
        assert!(duty.observe(&PressureStatus::default()).lines.is_empty());
    }

    // Value: a one-second retry of a wait only native work can end is wasted work, and
    // an unbounded backoff would leave a finished wait unnoticed.
    // Protects: passes without progress back off to the ceiling and no further; one
    // sign of progress returns the schedule to the base delay.
    #[test]
    fn passes_without_progress_back_off_to_the_ceiling_and_progress_resets_it() {
        let mut duty = Duty::default();
        duty.observe(&waiting(2_000, 900));
        let mut delays = Vec::new();
        for step in 1..=7 {
            delays.push(duty.observe(&waiting(2_000 + step, 900)).delay);
        }
        assert_eq!(
            delays.iter().map(Duration::as_secs).collect::<Vec<_>>(),
            [2, 4, 8, 16, 30, 30, 30]
        );
        // Linked bytes moved: the wait is progressing, so look again soon.
        assert_eq!(duty.observe(&waiting(3_000, 800)).delay, BASE_DELAY);
    }

    // Value: item 3 of #211: every pass without progress is journaled, but a wait that
    // lasts a day must not write a line per second.
    // Protects: the quiet-pass report is rate limited by the passes' own clock and
    // carries the number of passes it stands for.
    #[test]
    fn passes_without_progress_are_journaled_at_a_bounded_rate_with_their_count() {
        let mut duty = Duty::default();
        assert_eq!(duty.observe(&waiting(0, 900)).lines.len(), 1);
        for second in 1..=59 {
            assert!(
                duty.observe(&waiting(second * 1_000, 900)).lines.is_empty(),
                "second {second}"
            );
        }
        let report = duty.observe(&waiting(60_000, 900));
        assert_eq!(report.lines.len(), 1);
        assert!(report.lines[0].contains("60 passes made no progress"));
        assert!(duty.observe(&waiting(61_000, 900)).lines.is_empty());
    }

    // Value: a failing pass (a background error needing a restart) is the one state the
    // store cannot end; it must stay loud without flooding the journal.
    // Protects: failures are journaled at the same bounded rate and back off.
    #[test]
    fn failing_passes_are_journaled_at_a_bounded_rate_and_back_off() {
        let mut duty = Duty::default();
        let first = duty.failed(0, "background errors");
        assert_eq!(first.lines.len(), 1);
        assert!(first.lines[0].contains("background errors"));
        assert_eq!(first.delay, Duration::from_secs(2));
        assert!(duty.failed(1_000, "background errors").lines.is_empty());
        let later = duty.failed(60_000, "background errors");
        assert!(later.lines[0].contains("2 failures since the last report"));
    }
}
