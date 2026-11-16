// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The journal line of an intake refused for RDF staging (#340). The store counts the refusal
//! where it is raised and reports it in `Status.stagingRefusals`; this module only names it for
//! the operator, so a refusal an ingest absorbs into a failed projection is not silent.

use sparrow_core::{Error, StagingRefusal, Store};
use std::sync::Arc;

/// Refusals journaled one by one before the line thins out to powers of two.
const EVERY_REFUSAL: u64 = 16;

/// The operator line for the `total`th refusal, or `None` when it is not journaled. The first
/// sixteen are, then the 32nd, 64th and each doubling after, so a loop of refused artifacts
/// writes `O(log n)` lines. The line carries the running total, never record bytes.
#[must_use]
pub fn journal_line(refusal: &StagingRefusal, total: u64) -> Option<String> {
    (total <= EVERY_REFUSAL || total.is_power_of_two()).then(|| {
        format!(
            "sparrowd: rdf staging refused limit={} source={:?} graph={:?} anticipated_bytes={} refusals_total={total}",
            refusal.limit, refusal.source, refusal.graph, refusal.bytes
        )
    })
}

/// Journal every refusal the store counts, from now until it closes.
///
/// # Errors
/// Returns the store's refusal when an observer is already installed.
pub fn install(store: &Store) -> Result<(), Error> {
    store.observe_staging_refusals(Arc::new(|refusal, total| {
        if let Some(line) = journal_line(refusal, total) {
            eprintln!("{line}");
            tracing::warn!(
                limit = refusal.limit,
                source = %refusal.source,
                graph = %refusal.graph,
                anticipated_bytes = refusal.bytes,
                refusals_total = total,
                "rdf staging refused"
            );
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The journal stays bounded under a stream of refusals and names the source and size.
    #[test]
    fn journal_thins_to_powers_of_two_after_sixteen_and_names_source_and_size() {
        let refusal = StagingRefusal {
            limit: "write",
            source: "app".to_owned(),
            graph: "record:x".to_owned(),
            bytes: 70_000_000,
        };
        let lines = (1..=1000)
            .filter_map(|total| journal_line(&refusal, total))
            .count();
        assert_eq!(lines, 16 + 5, "17..=1000 journals 32, 64, 128, 256 and 512");
        let line = journal_line(&refusal, 1).unwrap();
        assert!(line.contains("source=\"app\"") && line.contains("anticipated_bytes=70000000"));
    }
}
