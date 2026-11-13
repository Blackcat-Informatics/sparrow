// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The signal that tells a held syslog frame its refusal may have cleared (#243).
//!
//! Maintenance sends it when a pressure pass ends, when a pin release, an archive commit or a
//! forced compaction changes what a pass can do, and when the free-space floor resumes. A
//! waiter that misses a send is not stuck: it also retries on its own bounded backoff.

use std::sync::Arc;
use tokio::sync::watch;

/// A counter any number of holders watch; sending never blocks and never fails.
#[derive(Clone)]
pub struct PressureWake(Arc<watch::Sender<u64>>);

impl Default for PressureWake {
    fn default() -> Self {
        Self(Arc::new(watch::channel(0).0))
    }
}

impl PressureWake {
    /// Tell every subscriber that pressure may have changed.
    pub fn wake(&self) {
        self.0.send_modify(|count| *count = count.wrapping_add(1));
    }

    /// A subscriber that sees every send after this call.
    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.0.subscribe()
    }
}
