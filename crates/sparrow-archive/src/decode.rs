// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The one place that knows the segment byte format (a GTS file, read by
//! `purrdf::gts::reader::read`). [`decode`] turns a segment's bytes into records and
//! artifacts; replacing the format replaces this module only. Listing, naming and
//! extraction never see the format.

use std::collections::BTreeMap;

use purrdf::gts::writer::digest_string;

use crate::segment::{ArchivedArtifact, ArchivedRecord};

/// The metadata vocabulary a segment's graphs use (`sparrow-core` writes it; this is its
/// one consumer). A record's bytes are the blob its archive link quad names with
/// `payloadBlake3`, `blake3:<hex>` (written by the segment encoder in `sparrow-core`).
mod vocabulary {
    pub(super) const NS: &str = sparrow_core::RDF_VOCABULARY_NAMESPACE;
    pub(super) const TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    pub(super) const ENTITY: &str = "urn:sparrow:";
    pub(super) const PAYLOAD_BLAKE3: &str = "urn:sparrow:vocab:payloadBlake3";
    pub(super) const RECORD: &str = "urn:sparrow:vocab:Record";
    pub(super) const ARTIFACT: &str = "urn:sparrow:vocab:Artifact";
}

/// Everything a segment holds, decoded.
#[derive(Debug)]
pub(crate) struct Decoded {
    pub(crate) records: Vec<ArchivedRecord>,
    pub(crate) artifacts: Vec<ArchivedArtifact>,
    pub(crate) blobs: usize,
}

/// Decode one segment file. The reader checks the frame hash chain; it does not hash a
/// blob's decompressed bytes, so each record's bytes are hashed here and compared with the
/// digest its link quad names. Any digest mismatch, any diagnostic, or a file with no segment head, is a refusal, never a partial read.
pub(crate) fn decode(bytes: &[u8]) -> Result<Decoded, String> {
    let mut graph = purrdf::gts::reader::read(bytes, false, None);
    if let Some(first) = graph.diagnostics.first() {
        return Err(format!(
            "{} ({} problems): {}",
            first.code,
            graph.diagnostics.len(),
            first.detail
        ));
    }
    if graph.segment_heads.is_empty() {
        return Err("no segment head".to_owned());
    }
    let mut facts: BTreeMap<String, Facts> = BTreeMap::new();
    for quad in graph.quad_terms() {
        let (Some(subject), Some(predicate)) =
            (quad.subject.iri_value(), quad.predicate.iri_value())
        else {
            continue;
        };
        let entry = facts.entry(subject.to_owned()).or_default();
        let object = quad
            .object
            .iri_value()
            .or(quad.object.value.as_deref())
            .unwrap_or_default()
            .to_owned();
        if predicate == vocabulary::TYPE {
            match object.as_str() {
                vocabulary::RECORD => entry.kind = Kind::Record,
                vocabulary::ARTIFACT => entry.kind = Kind::Artifact,
                _ => {}
            }
            continue;
        }
        if predicate == vocabulary::PAYLOAD_BLAKE3 {
            entry.payload = Some(object);
            continue;
        }
        match predicate.strip_prefix(vocabulary::NS) {
            Some("sourceName") => entry.source = Some(object),
            Some("receivedAt") => entry.received_at = Some(object),
            Some("artifact") => entry.artifact = Some(object),
            Some("format") => entry.format = Some(object),
            Some("sourceByteStart") => entry.byte_start = Some(object),
            Some("sourceByteEnd") => entry.byte_end = Some(object),
            Some("byteLength") => entry.byte_length = Some(object),
            _ => {}
        }
    }
    let mut records = Vec::new();
    let mut artifacts = Vec::new();
    for (iri, facts) in facts {
        let id = iri
            .strip_prefix(vocabulary::ENTITY)
            .unwrap_or(&iri)
            .to_owned();
        let need =
            |value: Option<String>, what: &str| value.ok_or_else(|| format!("{id} has no {what}"));
        match facts.kind {
            Kind::Other => {}
            Kind::Artifact => artifacts.push(ArchivedArtifact {
                source: need(facts.source, "sourceName")?,
                format: need(facts.format, "format")?,
                received_at: instant(&id, &need(facts.received_at, "receivedAt")?)?,
                byte_length: integer(&id, "byteLength", facts.byte_length)?,
                id,
            }),
            Kind::Record => {
                let digest = need(facts.payload, "payloadBlake3 link")?;
                let bytes = graph
                    .blob_bytes_cloned(&digest)
                    .map_err(|error| format!("record {id}: blob {digest}: {error}"))?
                    .ok_or_else(|| format!("record {id} has no blob {digest}"))?;
                if digest_string(&bytes) != digest {
                    return Err(format!(
                        "record {id}: its bytes do not match the digest {digest} its link names"
                    ));
                }
                let artifact = need(facts.artifact, "artifact")?;
                records.push(ArchivedRecord {
                    artifact_id: artifact
                        .strip_prefix(vocabulary::ENTITY)
                        .unwrap_or(&artifact)
                        .to_owned(),
                    source: need(facts.source, "sourceName")?,
                    received_at: instant(&id, &need(facts.received_at, "receivedAt")?)?,
                    byte_start: integer(&id, "sourceByteStart", facts.byte_start)?,
                    byte_end: integer(&id, "sourceByteEnd", facts.byte_end)?,
                    id,
                    bytes,
                });
            }
        }
    }
    Ok(Decoded {
        records,
        artifacts,
        blobs: graph.blobs.len(),
    })
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum Kind {
    #[default]
    Other,
    Record,
    Artifact,
}

#[derive(Default)]
struct Facts {
    kind: Kind,
    source: Option<String>,
    received_at: Option<String>,
    artifact: Option<String>,
    format: Option<String>,
    byte_start: Option<String>,
    byte_end: Option<String>,
    byte_length: Option<String>,
    payload: Option<String>,
}

fn integer(id: &str, what: &str, value: Option<String>) -> Result<u64, String> {
    let text = value.ok_or_else(|| format!("{id} has no {what}"))?;
    text.parse()
        .map_err(|_| format!("{id} has a non-integer {what} {text:?}"))
}

fn instant(id: &str, text: &str) -> Result<u64, String> {
    sparrow_core::parse_instant(text)
        .map_err(|error| format!("{id} has an unreadable receivedAt {text:?}: {error}"))
}
