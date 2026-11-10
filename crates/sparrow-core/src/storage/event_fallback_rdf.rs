// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Receipt-time event fallback facts, derived at read time.
//!
//! A record graph carries `eventTime` exactly when its record has a source instant. For a
//! record without one, the reader derives `eventTimeFallback` and `eventTimeBasis` from the
//! record's receipt time ([`super::rdf_index::fallback_metadata`]); ingest stores neither.
//! Graphs an earlier writer or the retired backfill (#20) already gave the facts keep them,
//! and the view serves those rows as stored.

#[cfg(feature = "test-support")]
use super::{Error, rdf_index};
#[cfg(feature = "test-support")]
use purrdf::TermFactory;
use purrdf::TermValue;
#[cfg(feature = "test-support")]
use rocksdb::WriteBatch;

/// Whether a record graph's stored rows already give its subject `eventTime` or
/// `eventTimeFallback`: the graph stores its facts and the reader derives none. The
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

/// Store the receipt-time fallback facts on the record metadata graphs `keep` selects (by
/// graph owner identity), as the retired backfill and earlier writers did, and leave a
/// partial backfill checkpoint with its counter row, the shape of a live schema 71 store
/// part way through the retired backfill. Offline test fixtures only; absent unless the
/// dev-only test-support feature is selected.
#[cfg(feature = "test-support")]
pub fn store_event_fallback_rdf_for_legacy_fixture(
    db: &rocksdb::DB,
    mut keep: impl FnMut(&str) -> bool,
) -> Result<(), Error> {
    use crate::RDF_VOCABULARY_NAMESPACE as NS;
    const SOURCE: &str = "rdf:graph:record:";
    const BOUND: usize = 32 * 1024 * 1024;
    let fail =
        |message: &str| Error::new("STORAGE_FAILURE", format!("fallback fixture: {message}"));
    let owners = db
        .prefix_iterator(SOURCE)
        .map(|row| row.map_err(Error::from))
        .take_while(|row| {
            row.as_ref()
                .map_or(true, |(key, _)| key.starts_with(SOURCE.as_bytes()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut last = None;
    for (key, bytes) in owners {
        let key = std::str::from_utf8(&key).map_err(|_| fail("invalid graph identity"))?;
        let id = key
            .strip_prefix("rdf:graph:")
            .ok_or_else(|| fail("invalid metadata owner"))?;
        if !keep(id) {
            continue;
        }
        let mut rdf = rdf_index::RdfWrite::new(db)?;
        let subject_value = TermValue::iri(rdf_index::entity_iri(id));
        let stored =
            rdf_index::stored_metadata(db, &mut rdf, id, &bytes, BOUND, &|m: &str| fail(m))?;
        let mut builder = stored.builder;
        let received = stored
            .primary
            .iter()
            .filter(|row| stored.values[&row[0]] == subject_value)
            .find_map(
                |row| match (&stored.values[&row[1]], &stored.values[&row[2]]) {
                    (TermValue::Iri(p), TermValue::Literal { lexical_form, .. })
                        if p.strip_prefix(NS) == Some("receivedAt") =>
                    {
                        Some(lexical_form.clone())
                    }
                    _ => None,
                },
            )
            .ok_or_else(|| fail("record graph has no receipt time"))?;
        let subject = builder.intern_value(&subject_value)?;
        rdf_index::fallback_metadata(
            &mut builder,
            subject,
            crate::EventPlacement::receipt(crate::parse_instant(&received)?),
        )?;
        let mut batch = WriteBatch::default();
        rdf.replace_metadata(
            &mut batch,
            id,
            rdf_index::metadata_dataset(builder)?.as_ref(),
        )?;
        rdf.finish(&mut batch)?;
        super::catalog_upgrade::commit(db, batch)?;
        last = Some(key.to_owned());
    }
    let checkpoint = format!(
        r#"{{"after":{},"complete":false}}"#,
        last.map_or_else(|| "null".to_owned(), |key| format!("\"{key}\""))
    );
    db.put("event-fallback-rdf-upgrade", checkpoint)?;
    db.put("store-counter:done:event-fallback-rdf", 7_u64.to_be_bytes())?;
    Ok(())
}
