// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Persistent RDF dictionaries and three distinct six-permutation quad streams.

use super::{Error, Record, Store, Writer, integer, parse, read_field};
use purrdf::{
    RdfDataset, TermValue,
    projections::{ProjectionLimits, ProjectionTerm},
};
use purrdf_lex::json::{Object, Value};
use rocksdb::{DB, WriteBatch};
use std::collections::{BTreeMap, BTreeSet};

/// Initial bounded graph admission; raw evidence remains independent.
const MAX_ROWS: usize = 32768;
const MAX_STAGING: usize = 32 * 1024 * 1024;
const MAX_BATCH: usize = 64 * 1024 * 1024;
pub(super) const ORDERS: [(&str, [usize; 4]); 6] = [
    ("SPOG", [0, 1, 2, 3]),
    ("POSG", [1, 2, 0, 3]),
    ("OSPG", [2, 0, 1, 3]),
    ("GSPO", [3, 0, 1, 2]),
    ("GPOS", [3, 1, 2, 0]),
    ("GOSP", [3, 2, 0, 1]),
];

pub(crate) fn term_limits() -> Result<ProjectionLimits, Error> {
    ProjectionLimits::new(1, 65536, MAX_STAGING, MAX_STAGING, 16)
        .map_err(|e| Error::new("PROJECTION_FAILURE", e.to_string()))
}

pub(crate) fn graph_iri(id: &str) -> String {
    format!("urn:sparrow:graph:{id}")
}

pub(super) fn profile_identity() -> Result<String, Error> {
    let mut bytes = Vec::new();
    for value in [
        "sparrow-rdf-index-v2;graph-declarations;term-count;encoded-term-bound;canonical-projection-term-json;three-streams;six-permutations;owner-last;u64be;default-graph-to-owner".to_owned(),
        crate::projection::json_profile()?.identity().to_hex(),
        crate::projection::markdown_profile()?.contract_id().to_hex(),
        format!("rows={MAX_ROWS};staging={MAX_STAGING};batch={MAX_BATCH};term=65536;depth=16"),
    ] { purrdf_hash::frame::frame_le(&mut bytes, value.as_bytes()); }
    Ok(purrdf_hash::hex::encode(
        purrdf_hash::blake3::hash(&bytes).as_bytes(),
    ))
}

impl Store {
    /// Inspect a retained record's persisted RDF state and exact native stream counts.
    /// This operation does not rebuild a dataset or imply SPARQL/backend qualification.
    pub fn rdf_inspect(&self, id: &str, now: u64) -> Result<Value, Error> {
        if !id.starts_with("record:") {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "RDF inspection requires a record ID",
            ));
        }
        let writer = self.lock()?;
        let record: Record = writer.get(id)?;
        writer.validate_event_record(&record)?;
        if !writer.live_record(&record, now, &writer.protected(now)?) {
            return Err(Error::new(
                "EVIDENCE_UNAVAILABLE",
                "RDF record expired or was evicted",
            ));
        }
        let metadata = manifest_counts(&writer, &record.id)?;
        let diagnostic_owner = super::diagnostic::graph_owner(&record);
        let diagnostic_rows = diagnostic_owner
            .as_ref()
            .map(|id| manifest_counts(&writer, id))
            .transpose()?
            .map_or(0, |counts| counts[0]);
        let mut result = Object::new()
            .with("store_id", writer.id.clone())
            .with("record_id", record.id.clone())
            .with("membership_at", crate::format_instant(now)?)
            .with("metadata_graph", graph_iri(&record.id))
            .with("metadata_primary_rows", metadata[0].to_string());
        result = result
            .with(
                "diagnostic_graph",
                diagnostic_owner.map(|id| graph_iri(&id)),
            )
            .with("diagnostic_primary_rows", diagnostic_rows.to_string());
        let mut counts = [0; 3];
        if record.rdf_projection_id.is_empty() {
            result = result
                .with("state", "not_applicable")
                .with("content_graph", Value::Null)
                .with("diagnostic", Value::Null);
        } else {
            let bytes = writer
                .db
                .get(format!("rdf:state:{}", record.rdf_projection_id))?
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "RDF projection state missing"))?;
            let state = parse(&bytes)?;
            let status: String = read_field(&state, "state")?;
            match status.as_str() {
                "complete" => {
                    counts = manifest_counts(&writer, &record.rdf_projection_id)?;
                    result = result
                        .with("state", status)
                        .with("content_graph", graph_iri(&record.rdf_projection_id))
                        .with("diagnostic", Value::Null);
                }
                "failed" => {
                    result = result
                        .with("state", status)
                        .with("content_graph", Value::Null)
                        .with("diagnostic", read_field::<String>(&state, "diagnostic")?);
                }
                _ => {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "invalid RDF projection state",
                    ));
                }
            }
        }
        Ok(result
            .with("primary_rows", counts[0].to_string())
            .with("reifier_rows", counts[1].to_string())
            .with("annotation_rows", counts[2].to_string())
            .into())
    }
}

fn manifest_counts(writer: &Writer, id: &str) -> Result<[u64; 3], Error> {
    let bytes = writer
        .db
        .get(format!("rdf:graph:{id}"))?
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "RDF graph manifest missing"))?;
    let manifest = parse(&bytes)?;
    let rows = manifest
        .get("rows")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "RDF manifest rows missing"))?;
    if rows.len() > MAX_ROWS {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "RDF manifest exceeds admitted row count",
        ));
    }
    let mut counts = [0; 3];
    for row in rows {
        let stream = row
            .as_array()
            .and_then(|v| v.first())
            .and_then(Value::as_str)
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v < 3)
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "invalid RDF stream discriminator"))?;
        counts[stream] += 1;
    }
    Ok(counts)
}

pub(super) fn encoded(value: &TermValue) -> Result<Vec<u8>, Error> {
    let limits = term_limits()?;
    ProjectionTerm::from_term_value(value, limits)
        .and_then(|t| t.to_canonical_json(limits))
        .map_err(|e| Error::new("PROJECTION_FAILURE", e.to_string()))
}

pub(super) fn counter(db: &DB, key: &str) -> Result<u64, Error> {
    match db.get(key)? {
        Some(bytes) => bytes
            .as_slice()
            .try_into()
            .map(u64::from_be_bytes)
            .map_err(|_| Error::new("STORAGE_FAILURE", format!("invalid RDF counter {key}"))),
        None => Ok(0),
    }
}

pub(super) fn required_counter(db: &DB, key: &str) -> Result<u64, Error> {
    let bytes = db.get(key)?.ok_or_else(|| {
        Error::new(
            "STORAGE_FAILURE",
            format!("required RDF counter {key} missing"),
        )
    })?;
    bytes
        .as_slice()
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| Error::new("STORAGE_FAILURE", format!("invalid RDF counter {key}")))
}

pub(super) fn term_key(id: u64) -> String {
    format!("rdf:term:{id:020}")
}
pub(super) fn reverse_key(bytes: &[u8]) -> String {
    format!(
        "rdf:reverse:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(bytes).as_bytes())
    )
}

pub(super) fn graph_key(graph: u64, owner: u64) -> Vec<u8> {
    let mut key = b"rdf:g:".to_vec();
    key.extend_from_slice(&graph.to_be_bytes());
    key.extend_from_slice(&owner.to_be_bytes());
    key
}

fn index_key(owner: u64, stream: u8, name: &str, order: [usize; 4], row: &[u64; 4]) -> Vec<u8> {
    let mut key = format!("rdf:q:{stream}:{name}:").into_bytes();
    for position in order {
        key.extend_from_slice(&row[position].to_be_bytes());
    }
    key.extend_from_slice(&owner.to_be_bytes());
    key
}

/// One ingestion/maintenance batch's dictionary and reference changes.
pub(super) struct RdfWrite<'a> {
    db: &'a DB,
    next: u64,
    terms: BTreeMap<Vec<u8>, u64>,
    references: BTreeMap<u64, u64>,
    owners: BTreeSet<String>,
    staged_bytes: usize,
    max_encoded: u64,
}

impl<'a> RdfWrite<'a> {
    pub(super) fn new(db: &'a DB) -> Result<Self, Error> {
        Ok(Self {
            db,
            next: required_counter(db, "rdf:next")?,
            terms: BTreeMap::new(),
            references: BTreeMap::new(),
            owners: BTreeSet::new(),
            staged_bytes: 0,
            max_encoded: required_counter(db, "rdf:max-encoded")?,
        })
    }

    fn intern(&mut self, value: &TermValue) -> Result<u64, Error> {
        let bytes = encoded(value)?;
        if let Some(id) = self.terms.get(&bytes) {
            return Ok(*id);
        }
        let reverse = reverse_key(&bytes);
        if let Some(stored) = self.db.get(reverse)? {
            let id = u64::from_be_bytes(
                stored
                    .as_slice()
                    .try_into()
                    .map_err(|_| Error::new("STORAGE_FAILURE", "invalid RDF reverse identity"))?,
            );
            if self.db.get(term_key(id))?.as_deref() != Some(bytes.as_slice()) {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "RDF forward/reverse identity mismatch",
                ));
            }
            return Ok(id);
        }
        self.staged_bytes = self.staged_bytes.saturating_add(bytes.len());
        if self.staged_bytes > MAX_STAGING {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "RDF dictionary staging exceeds 32 MiB",
            ));
        }
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "RDF term identity overflow"))?;
        self.max_encoded = self.max_encoded.max(bytes.len() as u64);
        self.terms.insert(bytes, self.next);
        Ok(self.next)
    }

    pub(super) fn closure(
        &mut self,
        value: &TermValue,
        terms: &mut BTreeSet<u64>,
    ) -> Result<u64, Error> {
        let id = self.intern(value)?;
        terms.insert(id);
        match value {
            TermValue::Literal { datatype, .. } => {
                self.closure(&TermValue::iri(datatype), terms)?;
            }
            TermValue::Triple { s, p, o } => {
                for child in [&**s, &**p, &**o] {
                    self.closure(child, terms)?;
                }
            }
            TermValue::Iri(_) | TermValue::Blank { .. } => {}
        }
        Ok(id)
    }

    /// Persist a complete native graph, preserving native stream and graph slots.
    pub(super) fn graph(
        &mut self,
        batch: &mut WriteBatch,
        id: &str,
        dataset: &RdfDataset,
    ) -> Result<(), Error> {
        let checkpoint_next = self.next;
        let checkpoint_bytes = self.staged_bytes;
        let checkpoint_max = self.max_encoded;
        let result = self.graph_inner(batch, id, dataset, false);
        if result.is_err() {
            self.terms.retain(|_, ordinal| *ordinal <= checkpoint_next);
            self.next = checkpoint_next;
            self.staged_bytes = checkpoint_bytes;
            self.max_encoded = checkpoint_max;
        }
        result
    }

    fn graph_inner(
        &mut self,
        batch: &mut WriteBatch,
        id: &str,
        dataset: &RdfDataset,
        replacing: bool,
    ) -> Result<(), Error> {
        if self.owners.contains(id)
            || (!replacing && self.db.get(format!("rdf:graph:{id}"))?.is_some())
        {
            return Ok(());
        }
        let provenance = (id.starts_with("record:") || id.starts_with("artifact:"))
            && self
                .db
                .get_pinned("store_format")?
                .is_some_and(|f| super::writer_format::has_provenance(&f));
        let mut terms = BTreeSet::new();
        let owner = self.closure(&TermValue::iri(graph_iri(id)), &mut terms)?;
        let mut graphs = BTreeSet::from([owner]);
        let mut rows = Vec::new();
        for g in dataset.named_graphs() {
            let value = dataset.term_value(g);
            graphs.insert(self.closure(&value, &mut terms)?);
        }
        for (stream, input) in [
            (0, dataset.quads().take(MAX_ROWS + 1).collect::<Vec<_>>()),
            (
                1,
                dataset
                    .reifier_quads()
                    .take(MAX_ROWS + 1)
                    .collect::<Vec<_>>(),
            ),
            (
                2,
                dataset
                    .annotation_quads()
                    .take(MAX_ROWS + 1)
                    .collect::<Vec<_>>(),
            ),
        ] {
            if rows.len().saturating_add(input.len()) > MAX_ROWS {
                return Err(Error::new("RESOURCE_LIMIT", "RDF graph exceeds 32768 rows"));
            }
            for row in input {
                let mut mapped = [0; 4];
                for (position, native) in [Some(row.s), Some(row.p), Some(row.o), row.g]
                    .into_iter()
                    .enumerate()
                {
                    mapped[position] = match native {
                        None => owner,
                        Some(native) => self.closure(&dataset.term_value(native), &mut terms)?,
                    };
                }
                rows.push((stream, mapped));
            }
        }
        let manifest = Object::new()
            .with("owner", owner.to_string())
            .with("terms", Value::array(terms.iter().map(|id| id.to_string())))
            .with(
                "graphs",
                Value::array(graphs.iter().map(|id| id.to_string())),
            )
            .with(
                "rows",
                Value::array(rows.iter().map(|(stream, row)| {
                    Value::array(
                        std::iter::once(stream.to_string())
                            .chain(row.iter().map(|v| v.to_string())),
                    )
                })),
            );
        let manifest = purrdf_lex::json::write_compact(&manifest.into());
        // Reserve both the physical index writes and deferred dictionary writes.
        // All fallible preparation precedes reference changes and batch mutation.
        let anticipated = batch
            .size_in_bytes()
            .saturating_add(manifest.len())
            .saturating_add(rows.len().saturating_mul(6 * 96))
            .saturating_add(self.staged_bytes)
            .saturating_add(self.terms.len().saturating_mul(128));
        if anticipated > MAX_BATCH {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "RDF write staging exceeds 64 MiB",
            ));
        }
        let references = terms
            .iter()
            .map(|&term| {
                let count = match self.references.get(&term) {
                    Some(count) => *count,
                    None => counter(self.db, &format!("rdf:ref:{term:020}"))?,
                };
                let count = count
                    .checked_add(1)
                    .ok_or_else(|| Error::new("STORAGE_FAILURE", "RDF term reference overflow"))?;
                Ok((term, count))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        self.references.extend(references);
        for &(stream, row) in &rows {
            for (name, order) in ORDERS {
                batch.put(index_key(owner, stream, name, order, &row), []);
            }
        }
        batch.put(format!("rdf:graph:{id}"), manifest);
        if provenance {
            batch.put(format!("provenance-rdf:{id}"), []);
        }
        for graph in graphs {
            batch.put(graph_key(graph, owner), []);
        }
        self.owners.insert(id.to_owned());
        Ok(())
    }

    /// Replace one committed owner atomically through the same dictionary/ref law.
    pub(super) fn replace_metadata(
        &mut self,
        batch: &mut WriteBatch,
        id: &str,
        dataset: &RdfDataset,
    ) -> Result<(), Error> {
        if self.owners.contains(id) {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "cannot replace a staged RDF owner",
            ));
        }
        self.remove(batch, id)?;
        self.graph_inner(batch, id, dataset, true)
    }

    fn adjust(&mut self, id: u64, add: bool) -> Result<(), Error> {
        let count = match self.references.get(&id) {
            Some(count) => *count,
            None => counter(self.db, &format!("rdf:ref:{id:020}"))?,
        };
        self.references.insert(
            id,
            if add {
                count.checked_add(1)
            } else {
                count.checked_sub(1)
            }
            .ok_or_else(|| {
                Error::new(
                    "STORAGE_FAILURE",
                    "RDF term reference overflow or underflow",
                )
            })?,
        );
        Ok(())
    }

    pub(super) fn remove(&mut self, batch: &mut WriteBatch, id: &str) -> Result<(), Error> {
        let key = format!("rdf:graph:{id}");
        let value = self
            .db
            .get(&key)?
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "RDF graph manifest missing"))?;
        let manifest = parse(&value)?;
        let owner = integer(&manifest, "owner")?;
        for (stream, row) in manifest_rows(&manifest)? {
            for (name, order) in ORDERS {
                batch.delete(index_key(owner, stream, name, order, &row));
            }
        }
        for term in read_field::<Vec<String>>(&manifest, "terms")? {
            self.adjust(
                term.parse()
                    .map_err(|_| Error::new("STORAGE_FAILURE", "invalid RDF term reference"))?,
                false,
            )?;
        }
        for graph in read_field::<Vec<String>>(&manifest, "graphs")? {
            let graph = graph
                .parse::<u64>()
                .ok()
                .filter(|id| *id > 0)
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "invalid named graph ID"))?;
            batch.delete(graph_key(graph, owner));
        }
        batch.delete(key);
        batch.delete(format!("provenance-rdf:{id}"));
        Ok(())
    }

    pub(super) fn finish(self, batch: &mut WriteBatch) -> Result<(), Error> {
        batch.put("rdf:next", self.next.to_be_bytes());
        batch.put("rdf:max-encoded", self.max_encoded.to_be_bytes());
        let count = required_counter(self.db, "rdf:term-count")?
            .checked_add(self.terms.len() as u64)
            .and_then(|count| {
                count.checked_sub(
                    self.references
                        .values()
                        .filter(|count| **count == 0)
                        .count() as u64,
                )
            })
            .ok_or_else(|| {
                Error::new(
                    "STORAGE_FAILURE",
                    "RDF dictionary count overflow or underflow",
                )
            })?;
        batch.put("rdf:term-count", count.to_be_bytes());
        for (bytes, id) in self.terms {
            batch.put(term_key(id), &bytes);
            batch.put(reverse_key(&bytes), id.to_be_bytes());
        }
        for (id, count) in self.references {
            let key = format!("rdf:ref:{id:020}");
            if count == 0 {
                let bytes = self.db.get(term_key(id))?.ok_or_else(|| {
                    Error::new("STORAGE_FAILURE", "unreachable RDF term is missing")
                })?;
                batch.delete(reverse_key(&bytes));
                batch.delete(term_key(id));
                batch.delete(key);
            } else {
                batch.put(key, count.to_be_bytes());
            }
        }
        Ok(())
    }
}

pub(super) fn manifest_rows(manifest: &Value) -> Result<Vec<(u8, [u64; 4])>, Error> {
    let rows = manifest
        .get("rows")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "RDF rows missing"))?;
    if rows.len() > MAX_ROWS {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "RDF manifest exceeds row bound",
        ));
    }
    rows.iter()
        .map(|row| {
            let values = row
                .as_array()
                .filter(|v| v.len() == 5)
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "invalid RDF row width"))?;
            let mut numbers = [0; 5];
            for (number, value) in numbers.iter_mut().zip(values) {
                *number = value
                    .as_str()
                    .and_then(|v| v.parse::<u64>().ok())
                    .ok_or_else(|| Error::new("STORAGE_FAILURE", "invalid RDF row ID"))?;
            }
            if numbers[0] > 2 || numbers[1..].contains(&0) {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "invalid RDF stream or term ID",
                ));
            }
            Ok((
                numbers[0] as u8,
                [numbers[1], numbers[2], numbers[3], numbers[4]],
            ))
        })
        .collect()
}

pub(super) fn metadata_dataset(
    builder: purrdf::RdfDatasetBuilder,
) -> Result<std::sync::Arc<RdfDataset>, Error> {
    builder
        .freeze()
        .map_err(|e| Error::new("PROJECTION_FAILURE", e.to_string()))
}

fn occurrence_metadata(
    id: &str,
    source: &str,
    received_at: u64,
    expires_at: u64,
    class: &str,
    payload: &str,
) -> Result<
    (
        purrdf::RdfDatasetBuilder,
        purrdf::ir::TermId,
        purrdf::ir::TermId,
    ),
    Error,
> {
    use purrdf::{RdfDatasetBuilder, RdfLiteral};
    const NS: &str = "urn:sparrow:vocab:";
    let mut builder = RdfDatasetBuilder::new();
    let subject = builder.intern_iri(&format!("urn:sparrow:{id}"))?;
    let source_node = source_metadata(&mut builder, source)?;
    for (predicate, object) in [
        (
            format!("{NS}payload"),
            builder.intern_iri(&format!("urn:sparrow:payload:{payload}"))?,
        ),
        (
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type".into(),
            builder.intern_iri(&format!("{NS}{class}"))?,
        ),
        (format!("{NS}source"), source_node),
        (
            format!("{NS}sourceName"),
            builder.intern_literal(RdfLiteral::simple(source))?,
        ),
        (
            format!("{NS}receivedAt"),
            builder.intern_literal(RdfLiteral::typed(
                crate::format_instant(received_at)?,
                "http://www.w3.org/2001/XMLSchema#dateTime",
            ))?,
        ),
        (
            format!("{NS}expiresAt"),
            builder.intern_literal(RdfLiteral::typed(
                crate::format_instant(expires_at)?,
                "http://www.w3.org/2001/XMLSchema#dateTime",
            ))?,
        ),
    ] {
        let predicate = builder.intern_iri(&predicate)?;
        builder.push_quad(subject, predicate, object, None)?;
    }
    Ok((builder, subject, source_node))
}

pub(super) fn source_metadata(
    builder: &mut purrdf::RdfDatasetBuilder,
    source: &str,
) -> Result<purrdf::ir::TermId, Error> {
    let node = builder.intern_iri(&super::source_read::source_iri(source))?;
    let predicate = builder.intern_iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")?;
    let class = builder.intern_iri("urn:sparrow:vocab:Source")?;
    builder.push_quad(node, predicate, class, None)?;
    let predicate = builder.intern_iri("urn:sparrow:vocab:sourceName")?;
    let name = builder.intern_literal(purrdf::RdfLiteral::simple(source))?;
    builder.push_quad(node, predicate, name, None)?;
    Ok(node)
}

pub(super) fn stream_metadata(
    builder: &mut purrdf::RdfDatasetBuilder,
    subject: purrdf::ir::TermId,
    source: purrdf::ir::TermId,
    stream_iri: &str,
    kind: &str,
) -> Result<(), Error> {
    let stream = builder.intern_iri(stream_iri)?;
    let link = builder.intern_iri("urn:sparrow:vocab:stream")?;
    builder.push_quad(subject, link, stream, None)?;
    let predicate = builder.intern_iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")?;
    let class = builder.intern_iri("urn:sparrow:vocab:Stream")?;
    builder.push_quad(stream, predicate, class, None)?;
    let predicate = builder.intern_iri("urn:sparrow:vocab:source")?;
    builder.push_quad(stream, predicate, source, None)?;
    let predicate = builder.intern_iri("urn:sparrow:vocab:streamKind")?;
    let kind = builder.intern_literal(purrdf::RdfLiteral::simple(kind))?;
    builder.push_quad(stream, predicate, kind, None)?;
    Ok(())
}

pub(super) fn artifact_metadata(
    artifact: &super::Artifact,
) -> Result<std::sync::Arc<RdfDataset>, Error> {
    use purrdf::RdfLiteral;
    let payload = artifact.payload_id.strip_prefix("blake3:").ok_or_else(|| {
        Error::new(
            "STORAGE_FAILURE",
            "artifact payload identity has no hash profile",
        )
    })?;
    let (mut builder, subject, _) = occurrence_metadata(
        &artifact.id,
        &artifact.source,
        artifact.received_at,
        artifact.expires_at,
        "Artifact",
        payload,
    )?;
    for (name, object) in [
        (
            "byteLength",
            builder.intern_literal(RdfLiteral::typed(
                artifact.byte_length.to_string(),
                "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
            ))?,
        ),
        (
            "format",
            builder.intern_literal(RdfLiteral::simple(&artifact.format))?,
        ),
    ] {
        let predicate = builder.intern_iri(&format!("urn:sparrow:vocab:{name}"))?;
        builder.push_quad(subject, predicate, object, None)?;
    }
    metadata_dataset(builder)
}

pub(super) fn metadata(
    record: &Record,
    payload: &str,
    state: Option<&Value>,
    facts: Option<&Value>,
) -> Result<std::sync::Arc<RdfDataset>, Error> {
    use purrdf::RdfLiteral;
    const NS: &str = "urn:sparrow:vocab:";
    let (mut builder, subject, source) = occurrence_metadata(
        &record.id,
        &record.source,
        record.received_at,
        record.expires_at,
        "Record",
        payload,
    )?;
    let mut origin = None;
    for candidate in [
        record
            .tail
            .as_ref()
            .map(|p| (p.generation_id.as_str(), "file-generation")),
        record
            .producer
            .as_ref()
            .map(|p| (p.stream_id.as_str(), "producer")),
        record
            .receiver
            .as_ref()
            .map(|p| (p.window_id.as_str(), "receiver-window")),
    ]
    .into_iter()
    .flatten()
    {
        if origin.replace(candidate).is_some() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "record has contradictory stream origins",
            ));
        }
    }
    if let Some((id, kind)) = origin {
        stream_metadata(
            &mut builder,
            subject,
            source,
            &format!("urn:sparrow:{id}"),
            kind,
        )?;
    }
    for (predicate, object) in [
        (
            format!("{NS}projectionState"),
            builder.intern_literal(RdfLiteral::simple(
                state
                    .and_then(|s| s.get("state"))
                    .and_then(Value::as_str)
                    .unwrap_or("not_applicable"),
            ))?,
        ),
        (
            format!("{NS}artifact"),
            builder.intern_iri(&format!("urn:sparrow:{}", record.artifact_id))?,
        ),
        (
            format!("{NS}sourceByteStart"),
            builder.intern_literal(RdfLiteral::typed(
                record.byte_start.to_string(),
                "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
            ))?,
        ),
        (
            format!("{NS}sourceByteEnd"),
            builder.intern_literal(RdfLiteral::typed(
                record.byte_end.to_string(),
                "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
            ))?,
        ),
    ] {
        let predicate = builder.intern_iri(&predicate)?;
        builder.push_quad(subject, predicate, object, None)?;
    }
    if let Some(position) = &record.tail {
        let predicate = builder.intern_iri(&format!("{NS}fileGeneration"))?;
        let object = builder.intern_iri(&format!("urn:sparrow:{}", position.generation_id))?;
        builder.push_quad(subject, predicate, object, None)?;
        for (name, value) in [
            ("fileByteStart", position.source_start),
            ("fileByteEnd", position.source_end),
        ] {
            let predicate = builder.intern_iri(&format!("{NS}{name}"))?;
            let object = builder.intern_literal(RdfLiteral::typed(
                value.to_string(),
                "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
            ))?;
            builder.push_quad(subject, predicate, object, None)?;
        }
    }
    if let Some(snapshot) = &record.syslog {
        let predicate = builder.intern_iri(&format!("{NS}syslogProfile"))?;
        let object = builder.intern_iri(&format!("urn:sparrow:{}", snapshot.profile_id))?;
        builder.push_quad(subject, predicate, object, None)?;
        if let Some(priority) = snapshot.priority {
            for (name, value) in [
                ("syslogPriority", priority),
                ("syslogFacility", priority / 8),
                ("syslogSeverity", priority % 8),
            ] {
                let predicate = builder.intern_iri(&format!("{NS}{name}"))?;
                let object = builder.intern_literal(RdfLiteral::typed(
                    value.to_string(),
                    "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
                ))?;
                builder.push_quad(subject, predicate, object, None)?;
            }
        }
        for field in &snapshot.fields {
            let predicate = builder.intern_iri(&format!("{NS}syslogHeader:{}", field.kind))?;
            let object = builder.intern_literal(RdfLiteral::simple(&field.value))?;
            builder.push_quad(subject, predicate, object, None)?;
        }
        if let Some(diagnostic) = &snapshot.diagnostic {
            let predicate = builder.intern_iri(&format!("{NS}syslogDiagnostic"))?;
            let object = builder.intern_literal(RdfLiteral::simple(diagnostic))?;
            builder.push_quad(subject, predicate, object, None)?;
        }
    }
    if let Some(position) = &record.producer {
        let predicate = builder.intern_iri(&format!("{NS}producerSequence"))?;
        let object = builder.intern_literal(RdfLiteral::typed(
            position.sequence.to_string(),
            "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
        ))?;
        builder.push_quad(subject, predicate, object, None)?;
    }
    if let Some(position) = &record.receiver {
        let predicate = builder.intern_iri(&format!("{NS}receiverWindow"))?;
        let object = builder.intern_iri(&format!("urn:sparrow:{}", position.window_id))?;
        builder.push_quad(subject, predicate, object, None)?;
        let predicate = builder.intern_iri(&format!("{NS}receiverSequence"))?;
        let object = builder.intern_literal(RdfLiteral::typed(
            position.sequence.to_string(),
            "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
        ))?;
        builder.push_quad(subject, predicate, object, None)?;
    }
    if let crate::EventTime::Known {
        millis, profile_id, ..
    } = &record.event_time
    {
        let predicate = builder.intern_iri(&format!("{NS}eventTime"))?;
        let object = builder.intern_literal(RdfLiteral::typed(
            crate::format_instant(*millis)?,
            "http://www.w3.org/2001/XMLSchema#dateTime",
        ))?;
        builder.push_quad(subject, predicate, object, None)?;
        let predicate = builder.intern_iri(&format!("{NS}sourceTimeProfile"))?;
        let object = builder.intern_iri(&format!("urn:sparrow:{profile_id}"))?;
        builder.push_quad(subject, predicate, object, None)?;
    }
    if state.and_then(|s| s.get("state")).and_then(Value::as_str) == Some("complete") {
        let predicate = builder.intern_iri(&format!("{NS}projection"))?;
        let object = builder.intern_iri(&graph_iri(&record.rdf_projection_id))?;
        builder.push_quad(subject, predicate, object, None)?;
    }
    super::tracing_rdf::append(&mut builder, record, subject, facts)?;
    metadata_dataset(builder)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_dictionary(db: &DB) {
        for key in ["rdf:term-count", "rdf:max-encoded", "rdf:next"] {
            db.put(key, 0_u64.to_be_bytes()).unwrap();
        }
    }

    #[test]
    fn provenance_upgrade_covers_private_owners_and_preserves_native_rows_after_reopen() {
        use purrdf::{RdfDatasetBuilder, RdfLiteral};
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        empty_dictionary(&db);
        db.put("store_format", b"sparrow.store/59").unwrap();
        let source = super::super::source_read::source_iri("original source");
        for (id, field, stream) in [
            ("record:private", "stream", "producer-stream:original"),
            ("record:tail", "fileGeneration", "file-generation:original"),
            (
                "record:receiver",
                "receiverWindow",
                "receiver-window:original",
            ),
            ("artifact:ordinary", "", ""),
        ] {
            let mut builder = RdfDatasetBuilder::new();
            let subject = builder.intern_iri(&format!("urn:sparrow:{id}")).unwrap();
            for (predicate, object) in [
                ("source", builder.intern_iri(&source).unwrap()),
                (
                    "sourceName",
                    builder
                        .intern_literal(RdfLiteral::simple("original source"))
                        .unwrap(),
                ),
            ] {
                let predicate = builder
                    .intern_iri(&format!("urn:sparrow:vocab:{predicate}"))
                    .unwrap();
                builder.push_quad(subject, predicate, object, None).unwrap();
            }
            if !field.is_empty() {
                let predicate = builder
                    .intern_iri(&format!("urn:sparrow:vocab:{field}"))
                    .unwrap();
                let object = builder
                    .intern_iri(&format!("urn:sparrow:{stream}"))
                    .unwrap();
                builder.push_quad(subject, predicate, object, None).unwrap();
            }
            let mut rdf = RdfWrite::new(&db).unwrap();
            let mut batch = WriteBatch::default();
            rdf.graph(&mut batch, id, &builder.freeze().unwrap())
                .unwrap();
            rdf.finish(&mut batch).unwrap();
            db.write(batch).unwrap();
            assert!(
                db.get(id).unwrap().is_none(),
                "private graph has no public catalog row"
            );
        }
        let receiver_bytes = db.get("rdf:graph:record:receiver").unwrap().unwrap();
        let mut receiver_manifest = parse(&receiver_bytes).unwrap();
        let receiver_subject = manifest_rows(&receiver_manifest).unwrap()[0].1[0];
        *receiver_manifest.get_mut("terms").unwrap() = Value::array(std::iter::empty::<String>());
        db.put(
            "rdf:graph:record:receiver",
            purrdf_lex::json::write_compact(&receiver_manifest),
        )
        .unwrap();
        assert_eq!(
            super::super::provenance_rdf::upgrade(&db, 128 * 1024 * 1024, 16 * 1024 * 1024)
                .unwrap_err()
                .code,
            "STORAGE_FAILURE"
        );
        let count_after_private = required_counter(&db, "rdf:term-count").unwrap();
        db.put("rdf:graph:record:receiver", receiver_bytes).unwrap();
        let missing_key = term_key(receiver_subject);
        let saved = db.get(&missing_key).unwrap().unwrap();
        db.delete(&missing_key).unwrap();
        assert_eq!(
            super::super::provenance_rdf::upgrade(&db, 128 * 1024 * 1024, 16 * 1024 * 1024)
                .unwrap_err()
                .code,
            "STORAGE_FAILURE"
        );
        assert!(db.get("provenance-rdf:record:private").unwrap().is_some());
        assert!(db.get("provenance-rdf:record:receiver").unwrap().is_none());
        assert_eq!(
            required_counter(&db, "rdf:term-count").unwrap(),
            count_after_private
        );
        assert_eq!(
            parse(&db.get("record-provenance-rdf-upgrade").unwrap().unwrap()).unwrap()["after"]
                .as_str(),
            Some("rdf:graph:record:private")
        );
        db.put(missing_key, saved).unwrap();
        drop(db);
        let db = DB::open_default(dir.path()).unwrap();
        super::super::provenance_rdf::upgrade(&db, 128 * 1024 * 1024, 16 * 1024 * 1024).unwrap();
        assert_eq!(
            db.get("store_format").unwrap().unwrap(),
            b"sparrow.store/70"
        );
        let before = db
            .iterator(rocksdb::IteratorMode::Start)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        for (id, expected_rows) in [
            ("record:private", 8),
            ("record:tail", 9),
            ("record:receiver", 9),
            ("artifact:ordinary", 4),
        ] {
            let manifest = parse(&db.get(format!("rdf:graph:{id}")).unwrap().unwrap()).unwrap();
            let rows = manifest_rows(&manifest).unwrap();
            assert_eq!(
                rows.iter().filter(|(stream, _)| *stream == 0).count(),
                expected_rows
            );
            assert!(
                db.get(format!("provenance-rdf:{id}"))
                    .unwrap()
                    .unwrap()
                    .is_empty()
            );
        }
        drop(db);
        let db = DB::open_default(dir.path()).unwrap();
        super::super::provenance_rdf::upgrade(&db, 128 * 1024 * 1024, 16 * 1024 * 1024).unwrap();
        assert_eq!(
            db.iterator(rocksdb::IteratorMode::Start)
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            before
        );
        // Native retirement removes the new markers and all shared dictionary references.
        for id in [
            "record:private",
            "record:tail",
            "record:receiver",
            "artifact:ordinary",
        ] {
            let mut rdf = RdfWrite::new(&db).unwrap();
            let mut batch = WriteBatch::default();
            rdf.remove(&mut batch, id).unwrap();
            rdf.finish(&mut batch).unwrap();
            db.write(batch).unwrap();
            assert!(db.get(format!("provenance-rdf:{id}")).unwrap().is_none());
        }
        assert_eq!(required_counter(&db, "rdf:term-count").unwrap(), 0);
    }

    #[test]
    fn scoped_full_streams_seek_selected_graphs_and_preserve_default_union_deduplication() {
        use purrdf::{DatasetView, FallibleDatasetView, GraphMatch};
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        empty_dictionary(&db);
        let mut selected = purrdf::RdfDatasetBuilder::new();
        let s = selected.intern_iri("urn:selected").unwrap();
        let p = selected.intern_iri("urn:p").unwrap();
        let o = selected.intern_iri("urn:o").unwrap();
        let a = selected.intern_iri("urn:graph:a").unwrap();
        let b = selected.intern_iri("urn:graph:b").unwrap();
        let empty = selected.intern_iri("urn:empty").unwrap();
        selected.declare_named_graph(empty).unwrap();
        selected.push_quad(s, p, o, Some(a)).unwrap();
        selected.push_quad(s, p, o, Some(b)).unwrap();
        let selected = selected.freeze().unwrap();
        let mut outside = purrdf::RdfDatasetBuilder::new();
        let foreign = outside.intern_iri("urn:foreign").unwrap();
        let p = outside.intern_iri("urn:p").unwrap();
        let shared = outside.intern_iri("urn:graph:a").unwrap();
        outside
            .push_quad(foreign, p, foreign, Some(shared))
            .unwrap();
        for n in 0..2000 {
            let o = outside.intern_iri(&format!("urn:outside:{n}")).unwrap();
            outside.push_quad(foreign, p, o, None).unwrap();
        }
        let outside = outside.freeze().unwrap();
        let mut batch = WriteBatch::default();
        let mut write = RdfWrite::new(&db).unwrap();
        write.graph(&mut batch, "selected-a", &selected).unwrap();
        write.graph(&mut batch, "selected-b", &selected).unwrap();
        write.graph(&mut batch, "outside", &outside).unwrap();
        write.finish(&mut batch).unwrap();
        db.write(batch).unwrap();
        let view = super::super::rdf_view::StoredRdfView::new(
            &db,
            crate::RdfReadLimits {
                requests: 2000,
                row_visits: 500,
                ..Default::default()
            },
            &["selected-a".into(), "selected-b".into()],
        )
        .unwrap();
        let before = view.operation_status().evidence().clone();
        let unpulled = view.quads();
        let after = view.operation_status().evidence().clone();
        assert_eq!(before.requests, after.requests);
        assert_eq!(before.row_visits, after.row_visits);
        assert_eq!(before.admitted_bytes, after.admitted_bytes);
        drop(unpulled);
        let rows = view.quads().collect::<Vec<_>>();
        assert_eq!(
            rows.len(),
            3,
            "two named statements plus one deduplicated default-union statement"
        );
        assert_eq!(rows.iter().filter(|row| row.g.is_none()).count(), 1);
        assert_eq!(
            view.quads_for_pattern(None, None, None, GraphMatch::Default)
                .count(),
            1
        );
        assert_eq!(
            view.named_graphs().count(),
            5,
            "retain both owners and the explicit empty declaration"
        );
        assert!(
            view.read_error().is_none(),
            "the 2000 foreign default-graph rows must not consume this scope's visit budget"
        );
    }

    #[test]
    fn refused_graphs_leave_dictionary_references_and_batch_unchanged() {
        use purrdf::{RdfDatasetBuilder, RdfLiteral};
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        empty_dictionary(&db);
        let mut builder = RdfDatasetBuilder::new();
        let s = builder.intern_iri("urn:fixture:s").unwrap();
        let p = builder.intern_iri("urn:fixture:p").unwrap();
        let o = builder
            .intern_literal(RdfLiteral::typed(
                "x".repeat(70000),
                "http://www.w3.org/2001/XMLSchema#string",
            ))
            .unwrap();
        builder.push_quad(s, p, o, None).unwrap();
        let oversized = builder.freeze().unwrap();
        let mut builder = RdfDatasetBuilder::new();
        let s = builder.intern_iri("urn:fixture:s").unwrap();
        let p = builder.intern_iri("urn:fixture:p").unwrap();
        let o = builder.intern_iri("urn:fixture:o").unwrap();
        builder.push_quad(s, p, o, None).unwrap();
        let valid = builder.freeze().unwrap();
        let mut write = RdfWrite::new(&db).unwrap();
        let mut batch = WriteBatch::default();
        let initial_size = batch.size_in_bytes();
        assert_eq!(
            write
                .graph(&mut batch, "refused", &oversized)
                .unwrap_err()
                .code,
            "PROJECTION_FAILURE"
        );
        assert_eq!(write.next, 0);
        assert_eq!(write.staged_bytes, 0);
        assert!(write.terms.is_empty());
        assert!(write.references.is_empty());
        assert!(write.owners.is_empty());
        assert_eq!(batch.size_in_bytes(), initial_size);
        // Fail after earlier reference counts have already been prepared.
        db.put("rdf:ref:00000000000000000003", u64::MAX.to_be_bytes())
            .unwrap();
        assert_eq!(
            write
                .graph(&mut batch, "overflow", &valid)
                .unwrap_err()
                .code,
            "STORAGE_FAILURE"
        );
        assert_eq!(write.next, 0);
        assert_eq!(write.staged_bytes, 0);
        assert!(write.terms.is_empty());
        assert!(write.references.is_empty());
        assert!(write.owners.is_empty());
        assert_eq!(batch.size_in_bytes(), initial_size);
        db.delete("rdf:ref:00000000000000000003").unwrap();
        write.graph(&mut batch, "accepted", &valid).unwrap();
        write.finish(&mut batch).unwrap();
        db.write(batch).unwrap();
        assert!(db.get("rdf:graph:accepted").unwrap().is_some());
        assert!(db.get("rdf:graph:refused").unwrap().is_none());
        assert!(db.get("rdf:graph:overflow").unwrap().is_none());
    }

    #[test]
    fn dictionary_and_indexes_preserve_native_rdf12_layers_and_empty_graphs() {
        use purrdf::{BlankScope, RdfDatasetBuilder, RdfLiteral, RdfTextDirection};
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        empty_dictionary(&db);
        let mut builder = RdfDatasetBuilder::new();
        let s = builder.intern_iri("urn:fixture:subject").unwrap();
        let p = builder.intern_iri("urn:fixture:predicate").unwrap();
        let first_blank = builder.intern_blank("same", BlankScope(0)).unwrap();
        let second_blank = builder.intern_blank("same", BlankScope(1)).unwrap();
        let literal = builder
            .intern_literal(RdfLiteral {
                lexical_form: "مرحبا".into(),
                datatype: None,
                language: Some("ar".into()),
                direction: Some(RdfTextDirection::Rtl),
            })
            .unwrap();
        let inner = builder.intern_triple(s, p, first_blank).unwrap();
        let nested = builder.intern_triple(s, p, inner).unwrap();
        let reifier = builder.intern_iri("urn:fixture:reifier").unwrap();
        let graph = builder.intern_iri("urn:fixture:graph").unwrap();
        let empty = builder.intern_iri("urn:fixture:empty").unwrap();
        builder.declare_named_graph(empty).unwrap();
        builder.push_quad(s, p, first_blank, None).unwrap();
        builder.push_quad(s, p, second_blank, Some(graph)).unwrap();
        builder
            .push_reifier_in_graph(reifier, nested, Some(graph))
            .unwrap();
        builder
            .push_annotation_in_graph(reifier, p, literal, Some(graph))
            .unwrap();
        let native = builder.freeze().unwrap();
        let mut batch = WriteBatch::default();
        let mut write = RdfWrite::new(&db).unwrap();
        write.graph(&mut batch, "rdf12-fixture", &native).unwrap();
        write.finish(&mut batch).unwrap();
        db.write(batch).unwrap();
        let manifest = parse(&db.get("rdf:graph:rdf12-fixture").unwrap().unwrap()).unwrap();
        assert_eq!(manifest["graphs"].as_array().unwrap().len(), 3);
        {
            use purrdf::{DatasetView, TermValue};
            let view = super::super::rdf_view::StoredRdfView::new(
                &db,
                crate::RdfReadLimits::default(),
                &["rdf12-fixture".into()],
            )
            .unwrap();
            assert_eq!(view.quads().count(), 4);
            assert_eq!(view.reifier_quads().count(), 2);
            assert_eq!(view.annotation_quads().count(), 2);
            assert_eq!(view.named_graphs().count(), 3);
            assert_eq!(
                view.quads_for_pattern(None, None, None, purrdf::GraphMatch::Default)
                    .count(),
                2
            );
            for id in [first_blank, second_blank, literal, inner, nested, empty] {
                let value = native.as_ref().term_value(id);
                let stored = view.term_id_by_value(&value).unwrap().unwrap();
                assert_eq!(view.term_value(stored).unwrap(), value);
            }
            let owner = view
                .term_id_by_value(&TermValue::iri(graph_iri("rdf12-fixture")))
                .unwrap()
                .unwrap();
            let s = view
                .term_id_by_value(&native.as_ref().term_value(s))
                .unwrap()
                .unwrap();
            let p = view
                .term_id_by_value(&native.as_ref().term_value(p))
                .unwrap()
                .unwrap();
            let o = view
                .term_id_by_value(&native.as_ref().term_value(first_blank))
                .unwrap()
                .unwrap();
            for mask in 0..8 {
                let s = (mask & 1 != 0).then_some(s);
                let p = (mask & 2 != 0).then_some(p);
                let o = (mask & 4 != 0).then_some(o);
                for graph in [purrdf::GraphMatch::Any, purrdf::GraphMatch::Named(owner)] {
                    let mut expected: Vec<_> = view
                        .quads()
                        .filter(|q| {
                            s.is_none_or(|s| q.s == s)
                                && p.is_none_or(|p| q.p == p)
                                && o.is_none_or(|o| q.o == o)
                                && graph.matches(q.g)
                        })
                        .collect();
                    let plan = view.probe_plan(s.is_some(), p.is_some(), o.is_some(), graph);
                    let mut found: Vec<_> = view
                        .quads_for_pattern_with_plan(&plan, s, p, o, graph)
                        .collect();
                    expected.sort_by_key(|q| (q.s, q.p, q.o, q.g));
                    found.sort_by_key(|q| (q.s, q.p, q.o, q.g));
                    assert_eq!(found, expected);
                }
            }
            assert!(view.read_error().is_none());
        }
        for (stream, count) in [(0, 2), (1, 1), (2, 1)] {
            for (name, _) in ORDERS {
                let prefix = format!("rdf:q:{stream}:{name}:");
                assert_eq!(
                    db.prefix_iterator(&prefix)
                        .filter(|e| e.as_ref().unwrap().0.starts_with(prefix.as_bytes()))
                        .count(),
                    count
                );
            }
        }
        let bytes: BTreeSet<_> = db
            .prefix_iterator("rdf:term:")
            .filter_map(|e| {
                let (key, value) = e.unwrap();
                key.starts_with(b"rdf:term:").then_some(value.to_vec())
            })
            .collect();
        for id in [first_blank, second_blank, literal, inner, nested, empty] {
            let value = native.as_ref().term_value(id);
            let identity = encoded(&value).unwrap();
            assert!(bytes.contains(&identity));
            assert_eq!(
                ProjectionTerm::from_canonical_json(&identity, term_limits().unwrap())
                    .unwrap()
                    .to_term_value(term_limits().unwrap())
                    .unwrap(),
                value
            );
        }
        assert_ne!(
            encoded(&native.as_ref().term_value(first_blank)).unwrap(),
            encoded(&native.as_ref().term_value(second_blank)).unwrap()
        );
        {
            use purrdf::{DatasetView, FallibleDatasetView, ViewOperationStatus};
            let value = native.as_ref().term_value(literal);
            let identity = encoded(&value).unwrap();
            let id = counter(&db, &reverse_key(&identity)).unwrap();
            let key = term_key(id);
            let saved = db.get(&key).unwrap().unwrap();
            db.delete(&key).unwrap();
            let view = super::super::rdf_view::StoredRdfView::new(
                &db,
                crate::RdfReadLimits::default(),
                &["rdf12-fixture".into()],
            )
            .unwrap();
            let id = purrdf::ir::GlobalTermId::checked_from_index(id - 1).unwrap();
            let error = view.resolve(id).err().unwrap();
            assert_eq!(error.code, "STORAGE_FAILURE");
            assert!(std::sync::Arc::ptr_eq(&error, &view.read_error().unwrap()));
            assert!(view.quads().next().is_none());
            assert!(matches!(
                view.operation_status(),
                ViewOperationStatus::Failed { .. }
            ));
            drop(view);
            db.put(&key, saved).unwrap();
            let owner = counter(
                &db,
                &reverse_key(&encoded(&TermValue::iri(graph_iri("rdf12-fixture"))).unwrap()),
            )
            .unwrap();
            let subject = counter(
                &db,
                &reverse_key(&encoded(&native.as_ref().term_value(s)).unwrap()),
            )
            .unwrap();
            let subject_id = purrdf::ir::GlobalTermId::checked_from_index(subject - 1).unwrap();
            for (name, row, subject_filter) in [
                ("SPOG", [subject, 0, 0, owner], Some(subject_id)),
                ("GSPO", [0, 0, 0, owner], None),
            ] {
                let order = ORDERS
                    .iter()
                    .find(|(candidate, _)| *candidate == name)
                    .unwrap()
                    .1;
                let broken = index_key(owner, 0, name, order, &row);
                db.put(&broken, []).unwrap();
                let view = super::super::rdf_view::StoredRdfView::new(
                    &db,
                    crate::RdfReadLimits::default(),
                    &["rdf12-fixture".into()],
                )
                .unwrap();
                let failure = purrdf::dataset_view::checkpointed_drain(&view, |view| {
                    if let Some(subject) = subject_filter {
                        view.quads_for_pattern(Some(subject), None, None, purrdf::GraphMatch::Any)
                            .collect::<Vec<_>>()
                    } else {
                        view.quads().collect::<Vec<_>>()
                    }
                })
                .unwrap_err();
                assert_eq!(
                    failure.checkpoint,
                    purrdf::dataset_view::DrainCheckpoint::After
                );
                assert_eq!(failure.error.code, "STORAGE_FAILURE");
                drop(view);
                db.delete(broken).unwrap();
            }
            let broken = graph_key(0, 1);
            db.put(&broken, []).unwrap();
            let view = super::super::rdf_view::StoredRdfView::new(
                &db,
                crate::RdfReadLimits::default(),
                &["rdf12-fixture".into()],
            )
            .unwrap();
            assert_eq!(
                purrdf::dataset_view::checkpointed_drain(&view, |view| view
                    .named_graphs()
                    .collect::<Vec<_>>())
                .unwrap_err()
                .error
                .code,
                "STORAGE_FAILURE"
            );
            drop(view);
            db.delete(broken).unwrap();
        }
        let mut batch = WriteBatch::default();
        let mut write = RdfWrite::new(&db).unwrap();
        write.remove(&mut batch, "rdf12-fixture").unwrap();
        write.finish(&mut batch).unwrap();
        db.write(batch).unwrap();
        for prefix in [
            "rdf:term:",
            "rdf:reverse:",
            "rdf:ref:",
            "rdf:q:",
            "rdf:g:",
            "rdf:graph:",
        ] {
            assert!(
                !db.prefix_iterator(prefix)
                    .any(|e| e.unwrap().0.starts_with(prefix.as_bytes()))
            );
        }
        assert_eq!(counter(&db, "rdf:term-count").unwrap(), 0);
    }
}
