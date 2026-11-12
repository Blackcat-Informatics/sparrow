// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Hang guard for tests that wait on events rather than timers.
//!
//! The workspace runs plain `cargo test` (Makefile, CI), which has no per-test timeout,
//! and a nextest slow-timeout does not exist here to configure. A test that waits on a
//! native event (a child's ready line, a flush or compaction) can therefore hang and
//! hold the gate until the CI job limit. This guard runs the body on a worker thread
//! and waits for its result with a deadline. The deadline is a hang detector only: a
//! passing body returns the moment it finishes, so the passing path adds no wall-clock
//! wait. Choose the limit as a multiple of the slowest measured run, never as the
//! expected duration.

use std::fmt;
use std::sync::mpsc::{RecvTimeoutError, channel};
use std::time::Duration;

/// A body that did not finish before its deadline.
#[derive(Debug)]
pub struct Hang(String);

impl fmt::Display for Hang {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Runs `body` on a worker thread and returns its value, or [`Hang`] naming the test and
/// what it was waiting on if `limit` passes first. A panic in `body` is re-raised on the
/// caller. After a hang the worker is abandoned (it cannot be cancelled); the caller must
/// release any process or resource it owns before failing.
pub fn try_run_within<T: Send + 'static>(
    test: &str,
    waiting_on: &str,
    limit: Duration,
    body: impl FnOnce() -> T + Send + 'static,
) -> Result<T, Hang> {
    let (sender, receiver) = channel();
    let worker = std::thread::Builder::new()
        .name(format!("{test}-deadline"))
        .spawn(move || {
            let _ = sender.send(body());
        })
        .expect("INVARIANT: the operating system can spawn a test worker thread");
    match receiver.recv_timeout(limit) {
        Ok(value) => {
            worker
                .join()
                .expect("INVARIANT: a worker that sent its result has not panicked");
            Ok(value)
        }
        Err(RecvTimeoutError::Disconnected) => match worker.join() {
            Err(panic) => std::panic::resume_unwind(panic),
            Ok(()) => unreachable!("INVARIANT: a worker drops its sender only by panicking"),
        },
        Err(RecvTimeoutError::Timeout) => Err(Hang(format!(
            "{test} made no progress for {limit:?} while waiting on {waiting_on}; \
             a healthy run is several times faster, so this is a hang, not host load"
        ))),
    }
}

/// [`try_run_within`] that fails the calling test with the [`Hang`] message.
pub fn run_within<T: Send + 'static>(
    test: &str,
    waiting_on: &str,
    limit: Duration,
    body: impl FnOnce() -> T + Send + 'static,
) -> T {
    try_run_within(test, waiting_on, limit, body).unwrap_or_else(|hang| panic!("{hang}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::channel;

    // Invariant: a body that cannot finish yields a Hang naming the test and the wait
    // target. The body blocks on a channel the test holds, so the deadline fires every
    // time; the test then releases the body so no worker is left behind.
    #[test]
    fn hang_names_the_test_and_wait_target_then_releases_the_worker() {
        let (release, blocked) = channel::<()>();
        let result = try_run_within(
            "sample_test",
            "the sample event",
            Duration::from_millis(30),
            move || blocked.recv().is_ok(),
        );
        let message = result.unwrap_err().to_string();
        assert!(message.contains("sample_test"), "{message}");
        assert!(message.contains("the sample event"), "{message}");
        release.send(()).unwrap();
    }

    // Invariant: a panic in the body reaches the caller with its original payload, not
    // the timeout text, so a real assertion failure is never hidden by the guard.
    #[test]
    fn body_panic_propagates_with_its_original_payload() {
        let caught = std::panic::catch_unwind(|| {
            let _ = try_run_within(
                "sample_test",
                "the sample event",
                Duration::from_secs(60),
                || panic!("original body failure"),
            );
        });
        let payload = caught.unwrap_err();
        assert_eq!(
            payload.downcast_ref::<&str>().copied(),
            Some("original body failure")
        );
    }

    // Invariant: a body that finishes returns its value. A generous limit is used and
    // elapsed time is not asserted, so the case cannot flake on host load.
    #[test]
    fn finished_body_returns_its_value() {
        let result = try_run_within(
            "sample_test",
            "the sample event",
            Duration::from_secs(60),
            || 7_u32,
        );
        assert_eq!(result.unwrap(), 7);
    }
}
