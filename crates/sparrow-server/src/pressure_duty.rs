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
/// Least time between journal lines about free-space-floor episodes (#267). One limiter
/// serves the begin, bound and end lines, so a volume hovering at the floor cannot flood the
/// journal, and a line is never written per refused write.
const FLOOR_LINE_MS: u64 = 60_000;

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
    evicted: u64,
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
            // In a floor episode a pass that retired nothing (blocked on a pin or the archive
            // watermark) is not progress: only what it retired is.
            passes: if status.floor_requested_bytes > 0 {
                0
            } else {
                status.passes
            },
            evicted: status
                .evicted_records
                .saturating_add(status.evicted_recordless_artifacts),
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
    /// A pass has measured the floor episode that is still open.
    floor_active: bool,
    /// The open episode's bound-reached line was already due.
    floor_bound_seen: bool,
    /// Clock reading of the last floor line.
    floor_line_at: Option<u64>,
    /// Floor lines held back by the limiter since the last one written.
    floor_suppressed: u64,
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
            floor_active: false,
            floor_bound_seen: false,
            floor_line_at: None,
            floor_suppressed: 0,
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
        "requested_bytes={} linked_bytes={} wait_before_bytes={} wait_age={} episode_age={} passes={} evicted_records={} forced_compactions={} waits_released={} stalled_passes={} refused_writes={} forced_compaction_ms={} forced_failures={} native_errors={} floor_requests={} floor_requested_bytes={} floor_passes={} floor_freed_bytes={} waiting_on={}",
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
        status.floor_requests,
        status.floor_requested_bytes,
        status.floor_passes,
        status.floor_freed_bytes,
        status.waiting_on.as_deref().unwrap_or("nothing"),
    )
}

impl Duty {
    /// The journal lines for the edges of a free-space-floor episode: it begins, it reaches
    /// its bound, it ends. All three pass through one limiter; a line held back is counted
    /// into the next one written.
    fn floor_lines(&mut self, status: &PressureStatus) -> Vec<String> {
        let active = status.floor_requested_bytes > 0;
        let mut events = Vec::new();
        if active && !self.floor_active {
            events.push(format!(
                "episode begins: free space is {} bytes below the floor and a refused write asked for reclamation",
                status.floor_requested_bytes
            ));
        }
        if active && status.floor_bound_reached && !self.floor_bound_seen {
            self.floor_bound_seen = true;
            events.push(format!(
                "episode bound reached: {} bytes freed, free space is still {} bytes short; nothing more is retired for this episode",
                status.floor_freed_bytes, status.floor_requested_bytes
            ));
        }
        if !active && self.floor_active {
            events.push(format!(
                "episode ends: free space is back at the floor after {} bytes freed",
                status.floor_freed_bytes
            ));
        }
        if !active {
            self.floor_bound_seen = false;
        }
        self.floor_active = active;
        let mut lines = Vec::new();
        for event in events {
            let due = self
                .floor_line_at
                .is_none_or(|at| status.observed_at.saturating_sub(at) >= FLOOR_LINE_MS);
            if !due {
                self.floor_suppressed += 1;
                continue;
            }
            let held = std::mem::take(&mut self.floor_suppressed);
            let note = if held == 0 {
                String::new()
            } else {
                format!(" ({held} floor events were not journaled since the last floor line)")
            };
            lines.push(format!(
                "sparrowd: free-space floor {event}{note}: {}",
                detail(status)
            ));
            self.floor_line_at = Some(status.observed_at);
        }
        lines
    }

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
        lines.extend(self.floor_lines(status));
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

    fn floor_episode(at: u64, short: u64) -> PressureStatus {
        PressureStatus {
            phase: PressurePhase::Requested,
            observed_at: at,
            episode_since: 1_000,
            floor_requests: 1,
            floor_requested_bytes: short,
            ..PressureStatus::default()
        }
    }

    fn floor_lines(verdict: &Verdict) -> Vec<&String> {
        verdict
            .lines
            .iter()
            .filter(|line| line.contains("free-space floor"))
            .collect()
    }

    // Value: #267 (the PR 244 lesson): a floor that refuses writes for hours, or a volume
    // hovering at it, must never write a journal line per refusal or per episode.
    // Protects: the begin, bound and end lines share one 60-second limiter; a passes' worth
    // of an open episode adds none; lines held back are counted into the next one written.
    #[test]
    fn floor_episode_lines_share_one_limiter_and_count_what_it_held_back() {
        let mut duty = Duty::default();
        let began = duty.observe(&floor_episode(1_000, 2_048));
        assert_eq!(floor_lines(&began).len(), 1);
        assert!(floor_lines(&began)[0].contains("episode begins"));
        assert!(floor_lines(&began)[0].contains("2048 bytes below the floor"));
        assert!(floor_lines(&began)[0].contains("floor_requests=1"));
        // An open episode, pass after pass, journals nothing about the floor.
        for second in 2..=9 {
            let open = duty.observe(&floor_episode(second * 1_000, 2_048));
            assert!(floor_lines(&open).is_empty(), "second {second}");
        }
        // It ends and another begins inside the minute: both held back.
        let ended = duty.observe(&PressureStatus {
            observed_at: 10_000,
            floor_requests: 1,
            floor_freed_bytes: 512,
            ..PressureStatus::default()
        });
        assert!(floor_lines(&ended).is_empty());
        assert!(floor_lines(&duty.observe(&floor_episode(20_000, 2_048))).is_empty());
        // Past the minute the bound edge is written and says what was held back.
        let bound = duty.observe(&PressureStatus {
            floor_bound_reached: true,
            floor_freed_bytes: 4_096,
            ..floor_episode(61_000, 2_048)
        });
        assert_eq!(floor_lines(&bound).len(), 1);
        assert!(floor_lines(&bound)[0].contains("episode bound reached"));
        assert!(floor_lines(&bound)[0].contains("2 floor events were not journaled"));
        // The bound line is written once per episode.
        let again = duty.observe(&PressureStatus {
            floor_bound_reached: true,
            floor_freed_bytes: 4_096,
            ..floor_episode(130_000, 2_048)
        });
        assert!(floor_lines(&again).is_empty());
    }

    // Value: #267 review: a floor episode on an archive store blocked on a pin or the archive
    // watermark ran a pass a second, and the pass counter read as progress, so the journal
    // stayed silent for as long as it lasted. Protects: in a floor episode a pass that retired
    // nothing is no progress, so the quiet report is journaled at its bounded rate with the
    // pass count it stands for.
    #[test]
    fn a_floor_episode_that_retires_nothing_is_journaled_as_no_progress() {
        let blocked = |at: u64, passes: u64| PressureStatus {
            phase: PressurePhase::Blocked,
            passes,
            stalled_passes: passes,
            waiting_on: Some("reclamation tried: the archive watermark has not reached".into()),
            ..floor_episode(at, 2_048)
        };
        let mut duty = Duty::default();
        assert!(!duty.observe(&blocked(0, 1)).lines.is_empty());
        for second in 1..=59 {
            let quiet = duty.observe(&blocked(second * 1_000, 1 + second));
            assert!(
                !quiet
                    .lines
                    .iter()
                    .any(|line| line.contains("pressure still")),
                "second {second}"
            );
        }
        let report = duty.observe(&blocked(60_000, 61));
        assert!(
            report
                .lines
                .iter()
                .any(|line| line.contains("pressure still blocked; 60 passes made no progress")),
            "{:?}",
            report.lines
        );
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
