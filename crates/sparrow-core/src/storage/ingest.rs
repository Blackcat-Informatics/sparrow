// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One native record/dependency preparation path for catalog publication.

use super::{CHUNK, Error, Record, Writer, blake3, hex, json, parse, rdf_index, source_time};
use purrdf_lex::json::{Object, Value};
use rocksdb::WriteBatch;
use std::collections::BTreeMap;

pub(super) struct RecordInput<'a> {
    pub id: String,
    pub artifact_id: &'a str,
    pub source: &'a str,
    pub format: &'a str,
    pub byte_start: u64,
    pub bytes: &'a [u8],
    pub received_at: u64,
    pub expires_at: u64,
    pub source_time_profile: &'a Option<source_time::SourceTimeProfile>,
}

pub(super) struct RecordBatch<'a> {
    writer: &'a Writer,
    pub batch: WriteBatch,
    content: BTreeMap<String, Vec<u8>>,
    chunks: Vec<String>,
    projections: BTreeMap<String, (Value, u64)>,
    rdf_projections: BTreeMap<String, (Value, u64)>,
    rdf: rdf_index::RdfWrite<'a>,
    profile: purrdf::json::Profile,
    markdown: purrdf::markdown::Profile,
    byte_count: u64,
    record_count: u64,
}

pub(super) struct PreparedRecords {
    pub batch: WriteBatch,
    pub chunks: Vec<String>,
}

impl<'a> RecordBatch<'a> {
    pub(super) fn new(writer: &'a Writer) -> Result<Self, Error> {
        Ok(Self {
            writer,
            batch: WriteBatch::default(),
            content: BTreeMap::new(),
            chunks: Vec::new(),
            projections: BTreeMap::new(),
            rdf_projections: BTreeMap::new(),
            rdf: rdf_index::RdfWrite::new(&writer.db)?,
            profile: crate::projection::json_profile()?,
            markdown: crate::projection::markdown_profile()?,
            byte_count: 0,
            record_count: 0,
        })
    }

    pub(super) fn record(&mut self, input: RecordInput<'_>) -> Result<Record, Error> {
        if self.record_count >= self.writer.config.max_records as u64 {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "prepared batch exceeds configured record count",
            ));
        }
        let source = input.source;
        let format = input.format;
        let bytes = input.bytes;
        let start = input.byte_start;
        let end = start
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "record byte range overflow"))?;
        let now = input.received_at;
        let expires_at = input.expires_at;
        let source_time_profile = input.source_time_profile;
        let digest = hex::encode(blake3::hash(bytes).as_bytes());
        let mut native_graph = None;
        let projection_id = if ["json", "jsonl"].contains(&format) {
            let key = format!(
                "projection:json:{}:{digest}",
                self.profile.identity().to_hex()
            );
            if let Some((_, count)) = self.projections.get_mut(&key) {
                *count += 1;
            } else {
                let facts = match self.writer.db.get(&key)? {
                    Some(stored) => parse(&stored)?,
                    None => {
                        let (facts, graph) =
                            crate::projection::json_facts(bytes, &digest, &self.profile)?;
                        native_graph = Some(graph);
                        facts
                    }
                };
                self.projections.insert(key.clone(), (facts, 1));
            }
            key
        } else {
            String::new()
        };
        let rdf_projection_id = if !projection_id.is_empty() {
            format!("rdf-{projection_id}")
        } else if format == "markdown" {
            format!(
                "rdf-projection:markdown:{}:{digest}",
                self.markdown.contract_id().to_hex()
            )
        } else {
            String::new()
        };
        if !rdf_projection_id.is_empty() {
            if let Some((_, count)) = self.rdf_projections.get_mut(&rdf_projection_id) {
                *count += 1;
            } else {
                let key = format!("rdf:state:{rdf_projection_id}");
                let state = match self.writer.db.get(&key)? {
                    Some(stored) => parse(&stored)?,
                    None => {
                        let result = if format == "markdown" {
                            crate::projection::markdown_graph(bytes, &digest, &self.markdown)
                        } else {
                            native_graph.ok_or_else(|| {
                                Error::new(
                                    "STORAGE_FAILURE",
                                    "existing JSON facts have no required RDF projection state",
                                )
                            })?
                        };
                        match result.and_then(|graph| {
                            self.rdf.graph(&mut self.batch, &rdf_projection_id, &graph)
                        }) {
                            Ok(()) => Object::new()
                                .with("state", "complete")
                                .with("graph", rdf_index::graph_iri(&rdf_projection_id))
                                .into(),
                            Err(error)
                                if ["RESOURCE_LIMIT", "PROJECTION_FAILURE"]
                                    .contains(&error.code) =>
                            {
                                Object::new()
                                    .with("state", "failed")
                                    .with("diagnostic", error.to_string())
                                    .into()
                            }
                            Err(error) => return Err(error),
                        }
                    }
                };
                self.rdf_projections
                    .insert(rdf_projection_id.clone(), (state, 1));
            }
        }
        let mut record_chunks = Vec::new();
        for chunk in bytes.chunks(CHUNK) {
            let digest = hex::encode(blake3::hash(chunk).as_bytes());
            self.content
                .entry(digest.clone())
                .or_insert_with(|| chunk.to_vec());
            record_chunks.push(digest.clone());
            self.chunks.push(digest);
        }
        let record = Record {
            id: input.id,
            artifact_id: input.artifact_id.to_owned(),
            source: source.to_owned(),
            received_at: now,
            expires_at,
            byte_start: start,
            byte_end: end,
            projection_id: projection_id.clone(),
            event_time: source_time::capture(
                source_time_profile.as_ref(),
                self.projections.get(&projection_id).map(|(facts, _)| facts),
                start,
                end,
            )?,
            rdf_projection_id,
            chunks: record_chunks,
            evicted: false,
        };
        let json = self
            .projections
            .get(&record.projection_id)
            .map(|(facts, _)| facts);
        let text = crate::text::facts(
            &self.writer.config.text_analyzer,
            bytes,
            json,
            format == "opaque",
        );
        self.writer.put_text(&mut self.batch, &record.id, &text)?;
        let rdf_state = self
            .rdf_projections
            .get(&record.rdf_projection_id)
            .map(|(state, _)| state);
        self.rdf.graph(
            &mut self.batch,
            &record.id,
            rdf_index::metadata(&record, &digest, rdf_state)?.as_ref(),
        )?;
        self.byte_count = self
            .byte_count
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| Error::new("RESOURCE_LIMIT", "prepared byte count overflow"))?;
        self.record_count += 1;
        Ok(record)
    }

    pub(super) fn finish(mut self) -> Result<PreparedRecords, Error> {
        let cf = self
            .writer
            .db
            .cf_handle("payload")
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "payload family missing"))?;
        let mut additions = BTreeMap::<String, u64>::new();
        for digest in &self.chunks {
            *additions.entry(digest.clone()).or_default() += 2;
        }
        let mut new_bytes = 0_u64;
        for (id, (state, added)) in self.rdf_projections {
            let key = format!("rdf:projection-ref:{id}");
            let previous = self
                .writer
                .db
                .get(&key)?
                .map(|v| {
                    v.as_slice()
                        .try_into()
                        .map(u64::from_be_bytes)
                        .map_err(|_| {
                            Error::new("STORAGE_FAILURE", "invalid RDF projection counter")
                        })
                })
                .transpose()?
                .unwrap_or(0);
            self.batch.put(
                key,
                previous
                    .checked_add(added)
                    .ok_or_else(|| {
                        Error::new("STORAGE_FAILURE", "RDF projection reference overflow")
                    })?
                    .to_be_bytes(),
            );
            self.batch
                .put(format!("rdf:state:{id}"), json::write_compact(&state));
        }
        self.rdf.finish(&mut self.batch)?;
        for (key, (facts, added)) in self.projections {
            let ref_key = format!("projection-ref:{key}");
            let previous = self
                .writer
                .db
                .get(&ref_key)?
                .map(|bytes| {
                    bytes
                        .as_slice()
                        .try_into()
                        .map(u64::from_be_bytes)
                        .map_err(|_| {
                            Error::new("STORAGE_FAILURE", "invalid projection reference counter")
                        })
                })
                .transpose()?
                .unwrap_or(0);
            let encoded = json::write_compact(&facts);
            new_bytes = new_bytes.saturating_add(encoded.len() as u64);
            self.batch.put(&key, encoded);
            self.batch.put(
                ref_key,
                previous
                    .checked_add(added)
                    .ok_or_else(|| Error::new("STORAGE_FAILURE", "projection reference overflow"))?
                    .to_be_bytes(),
            );
        }
        for (digest, payload) in self.content {
            let reference_key = format!("ref:{digest}");
            let count = match self.writer.db.get(&reference_key)? {
                Some(v) => u64::from_be_bytes(v.as_slice().try_into().map_err(|_| {
                    Error::new("STORAGE_FAILURE", "invalid payload reference counter")
                })?),
                None => {
                    new_bytes += payload.len() as u64;
                    self.batch.put_cf(cf, &digest, &payload);
                    0
                }
            };
            let count = count.checked_add(additions[&digest]).ok_or_else(|| {
                Error::new("STORAGE_FAILURE", "payload reference counter overflow")
            })?;
            self.batch.put(reference_key, count.to_be_bytes());
        }
        let estimate = self
            .byte_count
            .saturating_add(new_bytes)
            .saturating_add(self.record_count.saturating_mul(2048))
            .saturating_add(4096)
            .max(
                (self.batch.size_in_bytes() as u64)
                    .saturating_mul(2)
                    .saturating_add(4096),
            );
        if self.writer.physical_bytes()?.saturating_add(estimate)
            > self.writer.config.disk_budget - self.writer.config.maintenance_reserve
        {
            return Err(Error::new(
                "STORAGE_PRESSURE",
                "insufficient capacity below maintenance reserve; release pins or reclaim storage before retrying",
            ));
        }

        Ok(PreparedRecords {
            batch: self.batch,
            chunks: self.chunks,
        })
    }
}

impl Writer {
    /// Both upload paths compare the original source/format and exact payload identity.
    /// Legacy receipts already contain these fields in their artifact catalog.
    pub(super) fn accepted_ingest(
        &self,
        request_id: &str,
        source: &str,
        format: &str,
        byte_length: u64,
        payload_id: &str,
        now: u64,
    ) -> Result<Option<super::Artifact>, Error> {
        let artifact = match self.retry(&format!("request:ingest:{request_id}"), now)? {
            Some(saved) => {
                <super::Artifact as super::Catalog>::decode(super::field(&saved, "receipt")?)?
            }
            None => match super::publication::published(
                self,
                &super::import::import_id(request_id),
                now,
            )? {
                Some((_, artifact)) => artifact,
                None => return Ok(None),
            },
        };
        if artifact.source != source
            || artifact.format != format
            || artifact.byte_length != byte_length
            || artifact.payload_id != payload_id
        {
            return Err(Error::new(
                "CONFLICT",
                "request_id was committed with different input",
            ));
        }
        Ok(Some(artifact))
    }

    pub(super) fn ingest_receipt(
        &self,
        batch: &mut WriteBatch,
        request_id: &str,
        artifact: &super::Artifact,
        now: u64,
    ) -> Result<(), Error> {
        let receipt: Value = Object::new()
            .with(
                "expires_at",
                now.checked_add(super::RETRY_WINDOW)
                    .ok_or_else(|| Error::new("INVALID_ARGUMENT", "retry deadline overflow"))?
                    .to_string(),
            )
            .with("receipt", super::Catalog::encode(artifact))
            .into();
        batch.put(
            format!("request:ingest:{request_id}"),
            json::write_compact(&receipt),
        );
        Ok(())
    }
}
