// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Minimal Source/Stream facts share their retained occurrence's RDF ownership.

use super::{
    Error, artifact_rdf, catalog_upgrade, integer, parse, rdf_index, read_field, writer_format,
};
use purrdf::{RdfDatasetBuilder, TermFactory, TermValue, projections::ProjectionTerm};
use rocksdb::{DB, WriteBatch};
use std::collections::{BTreeMap, BTreeSet};

const PAGE_BYTES: usize = 32 * 1024 * 1024;
const RECORDS: catalog_upgrade::Job = catalog_upgrade::Job {
    checkpoint: "record-provenance-rdf-upgrade",
    source: "rdf:graph:record:",
    target: b"provenance-rdf:record:",
    page_bytes: PAGE_BYTES,
    page_entries: 1,
    charge,
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

fn charge(bytes: &[u8]) -> Result<usize, Error> {
    let charge = bytes.len().saturating_mul(65);
    if charge > PAGE_BYTES {
        return Err(Error::new(
            "RESOURCE_LIMIT",
            "provenance manifest exceeds migration admission",
        ));
    }
    Ok(charge)
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
    let manifest = parse(bytes)?;
    let owner = integer(&manifest, "owner")?;
    let rows = rdf_index::manifest_rows(&manifest)?;
    let primary: BTreeSet<_> = rows
        .iter()
        .filter(|(s, _)| *s == 0)
        .map(|(_, r)| *r)
        .collect();
    if rows.iter().any(|(s, r)| *s != 0 || r[3] != owner) {
        return Err(failure(
            "metadata stream or graph slots contradict native plain-quad ownership",
        ));
    }
    let mut remaining = PAGE_BYTES - charge;
    let limits = rdf_index::term_limits()?;
    let mut values = BTreeMap::new();
    let mut native = BTreeMap::new();
    let mut builder = RdfDatasetBuilder::new();
    for term in rows
        .iter()
        .flat_map(|(_, row)| row)
        .copied()
        .collect::<BTreeSet<_>>()
    {
        let bytes = db
            .get_pinned(rdf_index::term_key(term))?
            .ok_or_else(|| failure("metadata term missing"))?;
        if bytes.len() > 65536 {
            return Err(failure("metadata term exceeds encoded bound"));
        }
        remaining = remaining
            .checked_sub(bytes.len().saturating_mul(256).saturating_add(4096))
            .ok_or_else(|| {
                Error::new(
                    "RESOURCE_LIMIT",
                    "provenance term decoding exceeds migration admission",
                )
            })?;
        let value = ProjectionTerm::from_canonical_json(&bytes, limits)
            .and_then(|v| v.to_term_value(limits))
            .map_err(|e| failure(&e.to_string()))?;
        native.insert(term, builder.intern_value(&value)?);
        values.insert(term, value);
    }
    if values.get(&owner) != Some(&TermValue::iri(rdf_index::graph_iri(id))) {
        return Err(failure("owner dictionary identity mismatch"));
    }
    if read_field::<Vec<String>>(&manifest, "graphs")? != [owner.to_string()] {
        return Err(failure("metadata graph declarations disagree with owner"));
    }
    let mut rdf = rdf_index::RdfWrite::new(db)?;
    let mut closure = BTreeSet::new();
    for (id, value) in &values {
        if rdf.closure(value, &mut closure)? != *id {
            return Err(failure("metadata dictionary identity is inconsistent"));
        }
    }
    let declared = read_field::<Vec<String>>(&manifest, "terms")?;
    if declared != closure.iter().map(u64::to_string).collect::<Vec<_>>() {
        return Err(failure(
            "metadata term ownership disagrees with native closure",
        ));
    }
    let subject_value = TermValue::iri(format!("urn:sparrow:{id}"));
    let mut fields = BTreeMap::new();
    for row in primary {
        builder.push_quad(native[&row[0]], native[&row[1]], native[&row[2]], None)?;
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
