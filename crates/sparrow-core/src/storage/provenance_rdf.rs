// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Minimal Source/Stream facts share their retained occurrence's RDF ownership.

use super::{Error, artifact_rdf, catalog_upgrade, rdf_index, writer_format};
use purrdf::{TermFactory, TermValue};
use rocksdb::{DB, WriteBatch};
use std::collections::BTreeMap;

const PAGE_BYTES: usize = 32 * 1024 * 1024;
const RECORDS: catalog_upgrade::Job = catalog_upgrade::Job {
    checkpoint: "record-provenance-rdf-upgrade",
    source: "rdf:graph:record:",
    target: b"provenance-rdf:record:",
    page_bytes: PAGE_BYTES,
    page_entries: 1,
    charge: |bytes| catalog_upgrade::json_charge(bytes, PAGE_BYTES),
    publish,
};
const ARTIFACTS: catalog_upgrade::Job = catalog_upgrade::Job {
    checkpoint: "artifact-provenance-rdf-upgrade",
    source: "rdf:graph:artifact:",
    target: b"provenance-rdf:artifact:",
    ..RECORDS
};

pub(super) fn upgrade(db: &DB, disk_budget: u64, reserve: u64) -> Result<(), Error> {
    let format = db
        .get("store_format")?
        .ok_or_else(|| failure("writer format missing"))?;
    if !writer_format::has_provenance(&format) {
        let mut batch = WriteBatch::default();
        for job in [&RECORDS, &ARTIFACTS] {
            catalog_upgrade::seed(db, job, &mut batch)?;
        }
        batch.put("store_format", writer_format::enable_provenance(&format)?);
        catalog_upgrade::commit(db, batch)?;
    }
    for job in [&RECORDS, &ARTIFACTS] {
        catalog_upgrade::run(
            db,
            job,
            Some(&|batch| artifact_rdf::admit(db, disk_budget, reserve, batch)),
        )?;
    }
    Ok(())
}

fn failure(message: &str) -> Error {
    Error::new("STORAGE_FAILURE", format!("provenance RDF: {message}"))
}

fn publish(
    db: &DB,
    key: &str,
    bytes: &[u8],
    charge: usize,
    batch: &mut WriteBatch,
) -> Result<(), Error> {
    let id = key
        .strip_prefix("rdf:graph:")
        .ok_or_else(|| failure("invalid metadata owner"))?;
    let rdf_index::StoredMetadata {
        mut builder,
        values,
        primary,
        mut rdf,
    } = rdf_index::stored_metadata(db, id, bytes, PAGE_BYTES - charge, &failure)?;
    let subject_value = TermValue::iri(super::rdf_index::entity_iri(id));
    let mut fields = BTreeMap::new();
    for row in primary {
        if values[&row[0]] == subject_value
            && let TermValue::Iri(predicate) = &values[&row[1]]
            && let Some(name) = predicate.strip_prefix("urn:sparrow:vocab:")
            && [
                "source",
                "sourceName",
                "stream",
                "fileGeneration",
                "receiverWindow",
            ]
            .contains(&name)
            && fields
                .insert(name.to_owned(), values[&row[2]].clone())
                .is_some()
        {
            return Err(failure(
                "multiple values for an occurrence provenance field",
            ));
        }
    }
    let Some(TermValue::Literal {
        lexical_form,
        datatype,
        language: None,
        direction: None,
    }) = fields.get("sourceName")
    else {
        return Err(failure("source name missing or not a plain string"));
    };
    if datatype != "http://www.w3.org/2001/XMLSchema#string"
        || fields.get("source")
            != Some(&TermValue::iri(super::source_read::source_iri(
                lexical_form,
            )))
    {
        return Err(failure("source identity disagrees with exact source name"));
    }
    let source = rdf_index::source_metadata(&mut builder, lexical_form)?;
    let subject = builder.intern_value(&subject_value)?;
    let mut origin = None;
    for (name, kind) in [
        ("fileGeneration", "file-generation"),
        ("receiverWindow", "receiver-window"),
    ] {
        if let Some(value) = fields.get(name)
            && origin.replace((value, kind)).is_some()
        {
            return Err(failure("contradictory stream origins"));
        }
    }
    if let Some(stream) = fields.get("stream") {
        if let Some((specific, _)) = origin {
            if stream != specific {
                return Err(failure("stream link contradicts specific origin"));
            }
        } else {
            origin = Some((stream, "producer"));
        }
    }
    if let Some((stream, kind)) = origin {
        let TermValue::Iri(iri) = stream else {
            return Err(failure("stream identity is not an IRI"));
        };
        rdf_index::stream_metadata(&mut builder, subject, source, iri, kind)?;
    }
    let dataset = rdf_index::metadata_dataset(builder)?;
    rdf.replace_metadata(batch, id, &dataset)?;
    rdf.finish(batch)
}
