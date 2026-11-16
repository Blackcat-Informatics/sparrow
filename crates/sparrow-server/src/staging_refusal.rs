// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The journal line of an intake refused for RDF staging (#340). The store counts the refusal
//! where it is raised and reports it in `Status.stagingRefusals`; this module only names it for
//! the operator, so a refusal an ingest absorbs into a failed projection is not silent.

use sparrow_core::{Error, StagingRefusal, Store};
use std::io::Write;
use std::sync::Arc;

/// The operator line for one refusal, the `total`th since the store opened. It carries the
/// running total and never record bytes.
#[must_use]
pub fn journal_line(refusal: &StagingRefusal, total: u64) -> String {
    format!(
        "sparrowd: rdf staging refused limit={} source={:?} graph={:?} anticipated_bytes={} refusals_total={total}",
        refusal.limit, refusal.source, refusal.graph, refusal.bytes
    )
}

/// Write the line to `out`, ignoring a failed write. The observer runs while the store's writer
/// lock is held, and `eprintln!` panics when stderr cannot be written (a closed pipe, a full
/// disk); that panic would poison the writer lock and refuse every later intake until restart.
/// The status counter already holds the fact, so a lost line loses nothing.
fn write_line(out: &mut impl Write, line: &str) {
    let _ = writeln!(out, "{line}");
}

/// Journal every refusal the store counts, one line each, from now until it closes.
///
/// # Errors
/// Returns the store's refusal when an observer is already installed.
pub fn install(store: &Store) -> Result<(), Error> {
    store.observe_staging_refusals(Arc::new(|refusal, total| {
        write_line(&mut std::io::stderr().lock(), &journal_line(refusal, total));
        tracing::warn!(
            limit = refusal.limit,
            source = %refusal.source,
            graph = %refusal.graph,
            anticipated_bytes = refusal.bytes,
            refusals_total = total,
            "rdf staging refused"
        );
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Broken;

    impl Write for Broken {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }
    }

    /// A refusal is journaled as one line naming its source and size, and an unwritable
    /// stderr is ignored instead of panicking under the writer lock, which would poison it.
    #[test]
    fn a_refusal_is_one_line_and_an_unwritable_journal_cannot_panic() {
        let refusal = StagingRefusal {
            limit: "write",
            source: "app".to_owned(),
            graph: "record:x".to_owned(),
            bytes: 70_000_000,
        };
        let line = journal_line(&refusal, 3);
        assert!(line.contains("source=\"app\"") && line.contains("anticipated_bytes=70000000"));
        let mut out = Vec::new();
        write_line(&mut out, &line);
        assert_eq!(out, format!("{line}\n").into_bytes());
        write_line(&mut Broken, &line);
    }
}
