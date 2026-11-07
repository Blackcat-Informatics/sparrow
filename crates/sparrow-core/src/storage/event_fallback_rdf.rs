// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Receipt-time event fallback facts for record metadata graphs written before schema 69.
//!
//! The facts are derived from each graph alone, as the provenance backfill does, so a
//! graph prepared by an unfinished staged import is covered before its record is public.
//! A record graph carries `eventTime` exactly when its record has a source instant;
//! without one, the graph's own `receivedAt` is the receipt-time placement.

use super::{Error, artifact_rdf, catalog_upgrade, rdf_index, writer_format};
use purrdf::{TermFactory, TermValue};
use rocksdb::{DB, WriteBatch};

const PAGE_BYTES: usize = 32 * 1024 * 1024;
const JOB: catalog_upgrade::Job = catalog_upgrade::Job {
    checkpoint: "event-fallback-rdf-upgrade",
    source: "rdf:graph:record:",
    target: b"event-fallback-rdf:",
    page_bytes: PAGE_BYTES,
    page_entries: 1,
    charge: |bytes| catalog_upgrade::json_charge(bytes, PAGE_BYTES),
    publish,
};

/// Fence older writers, then add `eventTimeFallback` and `eventTimeBasis` to every retained
/// record graph whose source time is unknown. Stored records are only read.
pub(super) fn upgrade(db: &DB, disk_budget: u64, reserve: u64) -> Result<(), Error> {
    let format = db
        .get("store_format")?
        .ok_or_else(|| failure("writer format missing"))?;
    if !writer_format::has_event_fallback(&format) {
        let mut batch = WriteBatch::default();
        catalog_upgrade::seed(db, &JOB, &mut batch)?;
        batch.put(
            "store_format",
            writer_format::enable_event_fallback(&format)?,
        );
        catalog_upgrade::commit(db, batch)?;
    }
    catalog_upgrade::run(
        db,
        &JOB,
        Some(&|batch| artifact_rdf::admit(db, disk_budget, reserve, batch)),
    )
}

fn failure(message: &str) -> Error {
    Error::new("STORAGE_FAILURE", format!("event fallback RDF: {message}"))
}

fn publish(
    db: &DB,
    key: &str,
    bytes: &[u8],
    charge: usize,
    batch: &mut WriteBatch,
) -> Result<(), Error> {
    use crate::RDF_VOCABULARY_NAMESPACE as NS;
    let id = key
        .strip_prefix("rdf:graph:")
        .ok_or_else(|| failure("invalid metadata owner"))?;
    let rdf_index::StoredMetadata {
        mut builder,
        values,
        primary,
        mut rdf,
    } = rdf_index::stored_metadata(db, id, bytes, PAGE_BYTES - charge, &failure)?;
    let subject_value = TermValue::iri(rdf_index::entity_iri(id));
    let mut received = None;
    for row in &primary {
        if values[&row[0]] != subject_value {
            continue;
        }
        let TermValue::Iri(predicate) = &values[&row[1]] else {
            continue;
        };
        match predicate.strip_prefix(NS) {
            // A source instant, or facts already current: nothing to add.
            Some("eventTime" | "eventTimeFallback") => return Ok(()),
            Some("receivedAt") => {
                let TermValue::Literal {
                    lexical_form,
                    datatype,
                    language: None,
                    direction: None,
                } = &values[&row[2]]
                else {
                    return Err(failure("receipt time is not a typed literal"));
                };
                if datatype != "http://www.w3.org/2001/XMLSchema#dateTime"
                    || received.replace(lexical_form).is_some()
                {
                    return Err(failure("receipt time is not one xsd:dateTime"));
                }
            }
            _ => {}
        }
    }
    let received = received.ok_or_else(|| failure("record graph has no receipt time"))?;
    let placement = crate::EventPlacement::receipt(crate::parse_instant(received)?);
    let subject = builder.intern_value(&subject_value)?;
    rdf_index::fallback_metadata(&mut builder, subject, placement)?;
    let dataset = rdf_index::metadata_dataset(builder)?;
    rdf.replace_metadata(batch, id, dataset.as_ref())?;
    rdf.finish(batch)
}

/// Rebuild every record metadata graph without its fallback facts, recreating the graph a
/// writer before schema 69 published. Offline test fixtures only; absent unless the dev-only
/// test-support feature is selected.
#[cfg(feature = "test-support")]
pub fn remove_event_fallback_rdf_for_legacy_fixture(db: &DB) -> Result<(), Error> {
    use crate::RDF_VOCABULARY_NAMESPACE as NS;
    let fallback = [
        format!("{NS}eventTimeFallback"),
        format!("{NS}eventTimeBasis"),
    ];
    let owners = db
        .prefix_iterator(JOB.source)
        .map(|row| row.map_err(Error::from))
        .take_while(|row| {
            row.as_ref()
                .map_or(true, |(key, _)| key.starts_with(JOB.source.as_bytes()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for (key, bytes) in owners {
        let key = std::str::from_utf8(&key)
            .map_err(|_| failure("legacy fixture graph identity is invalid"))?;
        let id = key
            .strip_prefix("rdf:graph:")
            .ok_or_else(|| failure("invalid metadata owner"))?;
        let stored = rdf_index::stored_metadata(db, id, &bytes, PAGE_BYTES, &failure)?;
        let mut builder = purrdf::RdfDatasetBuilder::new();
        for row in &stored.primary {
            if matches!(&stored.values[&row[1]], TermValue::Iri(p) if fallback.contains(p)) {
                continue;
            }
            let subject = builder.intern_value(&stored.values[&row[0]])?;
            let predicate = builder.intern_value(&stored.values[&row[1]])?;
            let object = builder.intern_value(&stored.values[&row[2]])?;
            builder.push_quad(subject, predicate, object, None)?;
        }
        let mut rdf = stored.rdf;
        let mut batch = WriteBatch::default();
        rdf.replace_metadata(
            &mut batch,
            id,
            rdf_index::metadata_dataset(builder)?.as_ref(),
        )?;
        rdf.finish(&mut batch)?;
        catalog_upgrade::commit(db, batch)?;
    }
    Ok(())
}
