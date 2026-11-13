// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The signal that tells a held syslog frame its refusal may have cleared (#243, #247).
//!
//! A held frame blocks on this signal, its stop, and nothing else: it has no timer of its
//! own. Every ending the daemon can see sends it:
//!
//! - a pressure pass ends, or fails;
//! - a pin release or commit, an archive commit, or a forced compaction changes what a pass
//!   can do;
//! - the free-space floor resumes, sent by the observation that sees it, whichever duty or
//!   loop that is (see [`PressureWake::resume_waker`]).
//!
//! Two endings are external and have no event to wait on: free space returning because
//! another process freed it, and native `RocksDB` flush or compaction work completing (the
//! store has no listener hooked to it). The one observer of both is the maintenance
//! worker's one-second free-space observation, the cadence at which the daemon measures the
//! floor everywhere else, and it calls [`PressureWake::observe_tick`]: while any frame is
//! held it wakes them all to evaluate again. One tick serves every held frame; a frame has
//! no clock of its own.

use sparrow_core::RetentionWaker;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::watch;

/// A counter any number of holders watch; sending never blocks and never fails.
#[derive(Clone)]
pub struct PressureWake {
    sender: Arc<watch::Sender<u64>>,
    /// Frames now held for pressure, so the observer tick wakes only when one waits.
    held: Arc<AtomicUsize>,
}

impl Default for PressureWake {
    fn default() -> Self {
        Self {
            sender: Arc::new(watch::channel(0).0),
            held: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl PressureWake {
    /// Tell every subscriber that pressure may have changed.
    pub fn wake(&self) {
        self.sender
            .send_modify(|count| *count = count.wrapping_add(1));
    }

    /// How many sends there have been, for a caller that must show a wake went out.
    #[must_use]
    pub fn sent(&self) -> u64 {
        *self.sender.borrow()
    }

    /// The observer tick of the external endings: wake every held frame to evaluate again,
    /// but only while one is held. Called once per observation by the one observer.
    pub fn observe_tick(&self) {
        if self.held.load(Ordering::Acquire) != 0 {
            self.wake();
        }
    }

    /// The callback for the free-space floor's resume edge: it wakes held frames, then runs
    /// `also`. Installed on the floor, it runs in whichever observation consumes the edge.
    #[must_use]
    pub fn resume_waker(&self, also: impl Fn() + Send + Sync + 'static) -> RetentionWaker {
        let wake = self.clone();
        Arc::new(move || {
            wake.wake();
            also();
        })
    }

    /// A subscriber that sees every send after this call.
    pub(crate) fn subscribe(&self) -> Waiter {
        Waiter {
            receiver: self.sender.subscribe(),
            // The waiter owns the sender, so its channel never closes under it.
            _sender: Arc::clone(&self.sender),
            held: Arc::clone(&self.held),
        }
    }
}

/// One connection's view of the signal.
pub(crate) struct Waiter {
    receiver: watch::Receiver<u64>,
    _sender: Arc<watch::Sender<u64>>,
    held: Arc<AtomicUsize>,
}

impl Waiter {
    /// Mark every send so far as seen. Call it before an attempt: a send that lands during
    /// the attempt then still ends the wait after it.
    pub(crate) fn mark_seen(&mut self) {
        self.receiver.borrow_and_update();
    }

    /// Resolve at a send after the last [`Waiter::mark_seen`].
    pub(crate) async fn woken(&mut self) {
        // INVARIANT: the waiter owns a sender, so the channel is open.
        let _ = self.receiver.changed().await;
    }

    /// Count this frame as held until the returned guard drops.
    pub(crate) fn hold(&self) -> Held {
        self.held.fetch_add(1, Ordering::AcqRel);
        Held(Arc::clone(&self.held))
    }
}

/// A frame held for pressure; the observer tick wakes while any exists.
pub(crate) struct Held(Arc<AtomicUsize>);

impl Drop for Held {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
