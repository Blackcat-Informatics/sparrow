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
//! an archive commit, a forced compaction ending, the free-space floor resuming, a refused
//! write requesting a pass) wake the worker at once instead.
//!
//! The backoff is the named observer of native `RocksDB` flush and compaction state (#247).
//! The store has no completion listener, so the pass reads the pending counts itself, and the
//! cadence is the reason: one second while anything moves, doubling to 30 seconds when
//! nothing does, because a shorter wait cannot change what native work has done.

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
    /// Native background errors already journaled.
    native_reported: u64,
    /// Failed passes whose status could not be read, this session.
    unreadable_statuses: u64,
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
            native_reported: 0,
            unreadable_statuses: 0,
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
        let forced_failed = status.forced_failures > self.failures_reported;
        if forced_failed {
            self.failures_reported = status.forced_failures;
            lines.push(format!(
                "sparrowd: a forced compaction failed after {} ms: {}",
                status.forced_compaction_ms,
                status.failure.as_deref().unwrap_or("no reason recorded")
            ));
        }
        // A RocksDB error raised while no pressure pass was waiting on native work fails no
        // pass; it is journaled the pass it is seen, whatever the phase. A forced compaction
        // collected in this pass that raised the error carries it in its own line above (the
        // status text names both), so the error is not journaled a second time.
        if status.native_errors > self.native_reported {
            self.native_reported = status.native_errors;
            if !forced_failed {
                lines.push(format!(
                    "sparrowd: {}",
                    status
                        .failure
                        .as_deref()
                        .unwrap_or("RocksDB reported new background errors; no reason recorded")
                ));
            }
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

    /// Account for a pass that failed at `now`. The store shows `Blocked` until the next
    /// pass, which is retried on the same backoff. A RocksDB error or a forced-compaction
    /// failure new since the last report is journaled at once whatever the rate limit says,
    /// and carries its reason; the same failure repeated stays rate limited. `status` is the
    /// store's status after the failed pass, or why it could not be read: that failure is
    /// journaled and counted every time, never swallowed.
    pub(crate) fn failed(
        &mut self,
        now: u64,
        error: &str,
        status: Result<&PressureStatus, &str>,
    ) -> Verdict {
        self.last = None;
        self.quiet += 1;
        let mut lines = Vec::new();
        let mut new_failure = false;
        match status {
            Ok(status) => {
                new_failure = status.native_errors > self.native_reported
                    || status.forced_failures > self.failures_reported;
                self.native_reported = status.native_errors;
                // The reason rides in `error`, so this failure is not reported a second time.
                self.failures_reported = self.failures_reported.max(status.forced_failures);
            }
            Err(unreadable) => {
                self.unreadable_statuses += 1;
                lines.push(format!(
                    "sparrowd: the pressure status could not be read after a failed pass ({} so far this session): {unreadable}",
                    self.unreadable_statuses
                ));
            }
        }
        let due = new_failure
            || self
                .reported_at
                .is_none_or(|at| now.saturating_sub(at) >= QUIET_REPORT_MS);
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

    // Value: a failing pass (a store fault that persists) must stay loud without flooding
    // the journal.
    // Protects: failures are journaled at the same bounded rate and back off.
    #[test]
    fn failing_passes_are_journaled_at_a_bounded_rate_and_back_off() {
        let mut duty = Duty::default();
        let first = duty.failed(0, "background errors", Ok(&PressureStatus::default()));
        assert_eq!(first.lines.len(), 1);
        assert!(first.lines[0].contains("background errors"));
        assert_eq!(first.delay, Duration::from_secs(2));
        assert!(
            duty.failed(1_000, "background errors", Ok(&PressureStatus::default()))
                .lines
                .is_empty()
        );
        let later = duty.failed(60_000, "background errors", Ok(&PressureStatus::default()));
        assert!(later.lines[0].contains("2 failures since the last report"));
    }

    // Value: #218 ruling: a new RocksDB error is surfaced at once, and the forced-failure
    // line names the real reason. Protects: a failed pass that carries a new native error is
    // journaled inside the rate-limit window with its reason, is not journaled twice, and
    // an error already reported falls back to the bounded rate.
    #[test]
    fn a_new_native_error_is_journaled_at_once_with_its_reason() {
        let mut duty = Duty::default();
        assert_eq!(duty.observe(&waiting(0, 900)).lines.len(), 1);
        let status = PressureStatus {
            native_errors: 1,
            forced_failures: 1,
            ..waiting(1_000, 900)
        };
        let reason = "RocksDB reported 1 new background errors; the forced compaction failed: io";
        let first = duty.failed(1_000, reason, Ok(&status));
        assert_eq!(first.lines.len(), 1);
        assert!(first.lines[0].contains("the forced compaction failed: io"));
        assert!(duty.failed(2_000, reason, Ok(&status)).lines.is_empty());
        // The failure was journaled with its reason; no second line follows it.
        assert!(
            !duty
                .observe(&status)
                .lines
                .iter()
                .any(|line| line.contains("forced compaction failed after"))
        );
    }

    // Value: a forced compaction that failed without a native error (a panic) is journaled
    // by the ordinary pass. Protects: the line carries the status failure as its reason, not
    // a placeholder.
    #[test]
    fn a_forced_failure_line_carries_the_real_reason() {
        let mut duty = Duty::default();
        let status = PressureStatus {
            forced_failures: 1,
            forced_compaction_ms: 7,
            failure: Some("the forced compaction failed: the thread panicked".into()),
            ..waiting(0, 900)
        };
        let lines = duty.observe(&status).lines;
        assert!(lines[0].contains("after 7 ms: the forced compaction failed: the thread panicked"));
    }

    // Value: #220 item 3: when the wait-ending commit fails after a forced compaction failure,
    // the duty marks that forced failure reported, so its own reason was never journaled.
    // Protects: a failed pass that carries a new forced failure is journaled at once inside
    // the rate-limit window, with the reason the error carries.
    #[test]
    fn a_forced_failure_behind_a_failed_wait_end_is_journaled_at_once() {
        let mut duty = Duty::default();
        let quiet = PressureStatus::default();
        assert_eq!(
            duty.failed(0, "cannot take a write", Ok(&quiet))
                .lines
                .len(),
            1
        );
        let status = PressureStatus {
            forced_failures: 1,
            ..waiting(1_000, 900)
        };
        let reason = "cannot take a write; the forced compaction failed: the thread panicked";
        let lines = duty.failed(1_000, reason, Ok(&status)).lines;
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("the forced compaction failed: the thread panicked"));
    }

    // Value: #220 item 4: a failed status read used to fall back to the rate-limited path
    // without a word. Protects: every unreadable status is journaled with its cause and a
    // running count, even inside the rate-limit window.
    #[test]
    fn an_unreadable_status_after_a_failed_pass_is_journaled_and_counted() {
        let mut duty = Duty::default();
        duty.failed(0, "pass failed", Err("store poisoned"));
        let second = duty.failed(1_000, "pass failed", Err("store poisoned"));
        assert_eq!(second.lines.len(), 1);
        assert!(second.lines[0].contains("(2 so far this session): store poisoned"));
    }

    // Value: #220 item 5: a RocksDB error raised outside a pressure episode fails no pass and
    // used to reach the journal only at the next episode. Protects: a ready status with a new
    // native error is journaled at once with its reason, and only once.
    #[test]
    fn a_native_error_outside_an_episode_is_journaled_once_at_once() {
        let mut duty = Duty::default();
        let status = PressureStatus {
            native_errors: 1,
            failure: Some("RocksDB reported 1 new background errors".into()),
            ..PressureStatus::default()
        };
        let first = duty.observe(&status);
        assert_eq!(first.lines.len(), 1);
        assert!(first.lines[0].contains("RocksDB reported 1 new background errors"));
        assert!(duty.observe(&status).lines.is_empty());
    }

    // Value: #235 item 2: a forced compaction that raised a native error, collected by a pass
    // that ends Ready, was journaled twice with the same text. Protects: one line, carrying
    // the forced reason, and neither count is journaled again by a later pass.
    #[test]
    fn a_forced_native_failure_in_a_ready_pass_is_journaled_once_with_its_reason() {
        let mut duty = Duty::default();
        let status = PressureStatus {
            native_errors: 1,
            forced_failures: 1,
            forced_compaction_ms: 7,
            failure: Some(
                "the forced compaction failed: native error; RocksDB reported 1 new background errors (1 this session)"
                    .into(),
            ),
            ..PressureStatus::default()
        };
        let first = duty.observe(&status);
        assert_eq!(first.lines.len(), 1, "{:?}", first.lines);
        assert!(first.lines[0].contains("the forced compaction failed: native error"));
        assert!(duty.observe(&status).lines.is_empty());
    }
}
