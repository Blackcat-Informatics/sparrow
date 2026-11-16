// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Refusals of an intake whose RDF would exceed a staging limit (#340).
//!
//! The limit is enforced where the RDF write stages its dictionary and index rows, so the
//! count lives there too: every caller that stages through a counted write is covered,
//! including a refusal an ingest absorbs into a failed projection state.

use std::sync::{Arc, Mutex, OnceLock, PoisonError};

/// One refused staging, named for the operator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagingRefusal {
    /// The limit that refused: `write` (64 MiB batch) or `dictionary` (32 MiB terms).
    pub limit: &'static str,
    /// Source of the artifact being staged, empty when the writer did not know it.
    pub source: String,
    /// Identity of the graph being staged: a record, an artifact or a projection.
    pub graph: String,
    /// Bytes the write anticipated when it was refused.
    pub bytes: u64,
}

/// Called once per refusal, on the writer that was refused. It must not block.
pub type StagingObserver = Arc<dyn Fn(&StagingRefusal, u64) + Send + Sync>;

/// Refusals counted since the store opened, and the latest one. Shared by every counted
/// RDF write and `Store::properties`, which reads it without the writer lock.
#[derive(Default)]
pub(super) struct StagingLedger {
    state: Mutex<(u64, Option<StagingRefusal>)>,
    observer: OnceLock<StagingObserver>,
}

impl StagingLedger {
    pub(super) fn record(&self, refusal: StagingRefusal) {
        let total = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.0 = state.0.saturating_add(1);
            state.1 = Some(refusal.clone());
            state.0
        };
        if let Some(observer) = self.observer.get() {
            observer(&refusal, total);
        }
    }

    pub(super) fn snapshot(&self) -> (u64, Option<StagingRefusal>) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(super) fn observe(&self, observer: StagingObserver) -> bool {
        self.observer.set(observer).is_ok()
    }
}
