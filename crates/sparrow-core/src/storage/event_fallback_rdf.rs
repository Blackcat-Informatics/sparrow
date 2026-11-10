// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Receipt-time event fallback facts, derived at read time.
//!
//! A record graph carries `eventTime` exactly when its record has a source instant. For a
//! record without one, the reader derives `eventTimeFallback` and `eventTimeBasis` from the
//! record's receipt time ([`super::rdf_index::fallback_metadata`]); ingest stores neither.
//! Graphs an earlier writer or the retired backfill (#20) already gave the facts keep them,
//! and the view serves those rows as stored.

use purrdf::TermValue;

/// Whether a record graph's stored rows already give its subject `eventTime` or
/// `eventTimeFallback`: the facts are current and the graph needs no backfill. The
/// dictionary identities come from the caller's view of the dictionary.
pub(super) fn carries_event_time(
    rows: &[(u8, [u64; 4])],
    subject: Option<u64>,
    predicates: [Option<u64>; 2],
) -> bool {
    subject.is_some_and(|subject| {
        rows.iter().any(|(stream, row)| {
            *stream == 0 && row[0] == subject && predicates.contains(&Some(row[1]))
        })
    })
}

/// The predicates [`carries_event_time`] looks for.
pub(super) fn event_time_predicates() -> [TermValue; 2] {
    use crate::RDF_VOCABULARY_NAMESPACE as NS;
    ["eventTime", "eventTimeFallback"].map(|name| TermValue::iri(format!("{NS}{name}")))
}
