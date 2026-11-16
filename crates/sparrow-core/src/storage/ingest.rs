// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One native record/dependency preparation path for catalog publication.

use super::{CHUNK, Error, Record, Writer, blake3, hex, json, parse, rdf_index, source_time};
use purrdf_lex::json::{Object, Value};
use rocksdb::WriteBatch;
use std::collections::BTreeMap;

#[derive(Clone, Copy)]
pub(super) enum Origin<'a> {
    Ordinary,
    Producer(&'a super::producer_stream::ProducerBatch),
    /// A producer batch from the daemon's event route: like `Producer`, and the source's
    /// `/event_time` mapping is bound on first use, or counted lost when the registry is full.
    Event(&'a super::producer_stream::ProducerBatch),
    Receiver(&'a super::receiver_stream::SyslogReceiver),
    Tail(super::tail::Append<'a>),
}

impl<'a> Origin<'a> {
    pub(super) fn receiver(self) -> Option<&'a super::receiver_stream::SyslogReceiver> {
        match self {
            Self::Receiver(value) => Some(value),
            _ => None,
        }
    }
    pub(super) fn producer(self) -> Option<&'a super::producer_stream::ProducerBatch> {
        match self {
            Self::Producer(value) | Self::Event(value) => Some(value),
            _ => None,
        }
    }
    pub(super) fn maps_event_time(self) -> bool {
        matches!(self, Self::Event(_))
    }
    pub(super) fn tail(self) -> Option<super::tail::Append<'a>> {
        match self {
            Self::Tail(value) => Some(value),
            _ => None,
        }
    }
}

pub(super) enum RecordPayload<'a> {
    Buffered(&'a [u8]),
    /// Exact normalized chunks already durably owned by this private record and parent.
    Prepared {
        byte_length: u64,
        digest: &'a str,
        chunks: Vec<String>,
    },
}

pub(super) struct RecordInput<'a> {
    pub id: String,
    pub artifact_id: &'a str,
    pub source: &'a str,
    pub format: &'a str,
    pub byte_start: u64,
    pub payload: RecordPayload<'a>,
    pub received_at: u64,
    pub expires_at: u64,
    pub source_time_profile: &'a Option<source_time::SourceTimeProfile>,
    pub diagnostic_profile: Option<&'a super::diagnostic::DiagnosticProfile>,
    pub producer: Option<super::producer_stream::ProducerPosition>,
    pub receiver: Option<super::receiver_stream::ReceiverPosition>,
    pub tail: Option<super::tail::TailPosition>,
    pub intake: Option<super::intake::Intake>,
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
    pub estimate: u64,
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
            rdf: rdf_index::RdfWrite::counted(&writer.db, &writer.staging)?,
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
        let (raw_bytes, byte_length, digest, prepared_chunks) = match input.payload {
            RecordPayload::Buffered(bytes) => (
                Some(bytes),
                bytes.len() as u64,
                hex::encode(blake3::hash(bytes).as_bytes()),
                None,
            ),
            RecordPayload::Prepared {
                byte_length,
                digest,
                chunks,
            } => {
                if byte_length <= crate::projection::RECORD_BYTES {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "prepared raw record must exceed materialization admission",
                    ));
                }
                (None, byte_length, digest.to_owned(), Some(chunks))
            }
        };
        let bytes = raw_bytes.filter(|_| byte_length <= crate::projection::RECORD_BYTES);
        let start = input.byte_start;
        let end = start
            .checked_add(byte_length)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "record byte range overflow"))?;
        self.rdf.set_source(input.source);
        let now = input.received_at;
        let expires_at = input.expires_at;
        let source_time_profile = input.source_time_profile;
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
                        let (facts, graph) = crate::projection::json_facts(
                            bytes,
                            byte_length,
                            &digest,
                            &self.profile,
                        )?;
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
                            crate::projection::markdown_graph(
                                bytes,
                                byte_length,
                                &digest,
                                &self.markdown,
                            )
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
        let mut record_chunks = prepared_chunks.unwrap_or_default();
        for chunk in raw_bytes.into_iter().flat_map(|bytes| bytes.chunks(CHUNK)) {
            let digest = hex::encode(blake3::hash(chunk).as_bytes());
            self.content
                .entry(digest.clone())
                .or_insert_with(|| chunk.to_vec());
            record_chunks.push(digest.clone());
            self.chunks.push(digest);
        }
        let syslog = (format == "syslog")
            .then(|| {
                super::syslog::capture(
                    bytes.ok_or_else(|| {
                        Error::new(
                            "RESOURCE_LIMIT",
                            "syslog packets require bounded materialized bytes",
                        )
                    })?,
                    start,
                )
            })
            .transpose()?;
        let event_time = match &syslog {
            Some(snapshot) => snapshot.event_time(),
            None => source_time::capture(
                source_time_profile.as_ref(),
                self.projections.get(&projection_id).map(|(facts, _)| facts),
                (format, bytes),
                start,
                end,
            )?,
        };
        let diagnostics = match &syslog {
            Some(snapshot) => snapshot.diagnostics(),
            None => super::diagnostic::capture(
                input.diagnostic_profile,
                self.projections.get(&projection_id).map(|(facts, _)| facts),
                start,
                end,
            )?,
        };
        let record = Record {
            receiver: input.receiver,
            tail: input.tail,
            intake: input.intake,
            syslog,
            producer: input.producer,
            id: input.id,
            artifact_id: input.artifact_id.to_owned(),
            source: source.to_owned(),
            received_at: now,
            expires_at,
            byte_start: start,
            byte_end: end,
            projection_id: projection_id.clone(),
            event_time,
            rdf_projection_id,
            diagnostics,
            chunks: record_chunks,
            evicted: false,
            retired: false,
        };
        let json = self
            .projections
            .get(&record.projection_id)
            .map(|(facts, _)| facts);
        let text = crate::text::facts(
            &self.writer.config.text_analyzer,
            bytes,
            byte_length,
            json,
            format == "opaque",
        )?;
        super::tail_ranges::publish(&mut self.batch, &record);
        self.writer.put_text(&mut self.batch, &record.id, &text)?;
        let rdf_state = self
            .rdf_projections
            .get(&record.rdf_projection_id)
            .map(|(state, _)| state);
        self.rdf.graph(
            &mut self.batch,
            &record.id,
            rdf_index::metadata(&record, &digest, rdf_state, json)?.as_ref(),
        )?;
        if let Some(owner) = super::diagnostic::graph_owner(&record) {
            self.rdf.graph(
                &mut self.batch,
                &owner,
                super::diagnostic::rdf(&record)?.as_ref(),
            )?;
        }
        self.byte_count = self
            .byte_count
            .checked_add(raw_bytes.map_or(0, |bytes| bytes.len() as u64))
            .ok_or_else(|| Error::new("RESOURCE_LIMIT", "prepared byte count overflow"))?;
        self.record_count += 1;
        Ok(record)
    }

    pub(super) fn finish_artifact(
        mut self,
        mut artifact: super::Artifact,
    ) -> Result<(WriteBatch, super::Artifact, u64), Error> {
        super::artifact_rdf::stage(&self.writer.db, &mut self.rdf, &mut self.batch, &artifact)?;
        let writer = self.writer;
        let prepared = self.prepare()?;
        artifact.chunks = prepared.chunks;
        writer.require_retirable(artifact.chunks.len(), artifact.record_ids.len())?;
        Ok((prepared.batch, artifact, prepared.estimate))
    }

    pub(super) fn prepare(mut self) -> Result<PreparedRecords, Error> {
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
        new_bytes = new_bytes.saturating_add(self.writer.prepare_payloads(
            &mut self.batch,
            self.content,
            &self.chunks,
        )?);
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
        Ok(PreparedRecords {
            batch: self.batch,
            chunks: self.chunks,
            estimate,
        })
    }
}

impl Writer {
    /// One payload ownership writer for buffered records and bounded raw checkpoints.
    pub(super) fn prepare_payloads(
        &self,
        batch: &mut WriteBatch,
        content: BTreeMap<String, Vec<u8>>,
        chunks: &[String],
    ) -> Result<u64, Error> {
        let cf = self
            .db
            .cf_handle("payload")
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "payload family missing"))?;
        let mut additions = BTreeMap::<String, u64>::new();
        for digest in chunks {
            *additions.entry(digest.clone()).or_default() += 2;
        }
        let mut new_bytes = 0_u64;
        for (digest, payload) in content {
            if payload.is_empty()
                || payload.len() > CHUNK
                || hex::encode(blake3::hash(&payload).as_bytes()) != digest
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "invalid normalized payload preparation",
                ));
            }
            let reference_key = format!("ref:{digest}");
            let count = match self.db.get(&reference_key)? {
                Some(v) => {
                    let count = u64::from_be_bytes(v.as_slice().try_into().map_err(|_| {
                        Error::new("STORAGE_FAILURE", "invalid payload reference counter")
                    })?);
                    if count == 0 || self.payload_chunk(&digest)? != payload {
                        return Err(Error::new(
                            "STORAGE_FAILURE",
                            "existing payload contradicts its ownership",
                        ));
                    }
                    count
                }
                None => {
                    new_bytes += payload.len() as u64;
                    batch.put_cf(cf, &digest, &payload);
                    0
                }
            };
            let added = additions.remove(&digest).ok_or_else(|| {
                Error::new(
                    "STORAGE_FAILURE",
                    "prepared payload has no occurrence references",
                )
            })?;
            let count = count.checked_add(added).ok_or_else(|| {
                Error::new("STORAGE_FAILURE", "payload reference counter overflow")
            })?;
            batch.put(reference_key, count.to_be_bytes());
        }
        if !additions.is_empty() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "prepared occurrence references have no payload bytes",
            ));
        }
        Ok(new_bytes)
    }
}

impl Writer {
    /// Both upload paths compare the original source/format and exact payload identity.
    /// Legacy receipts already contain these fields in their artifact catalog.
    #[expect(
        clippy::too_many_arguments,
        reason = "compare all immutable request provenance through one owner"
    )]
    pub(super) fn accepted_ingest(
        &self,
        request_id: &str,
        source: &str,
        format: &str,
        byte_length: u64,
        payload_id: &str,
        origin: Origin<'_>,
        intake: Option<&super::intake::Intake>,
        now: u64,
    ) -> Result<Option<super::Artifact>, Error> {
        let artifact = match self.retry(&format!("request:ingest:{request_id}"), now)? {
            Some(saved) => {
                if saved
                    .get("producer")
                    .filter(|value| !value.is_null())
                    .cloned()
                    != origin
                        .producer()
                        .map(super::producer_stream::ProducerBatch::encode)
                    || saved.get("tail").filter(|value| !value.is_null()).cloned()
                        != origin.tail().map(|input| input.encode())
                    || saved
                        .get("receiver")
                        .filter(|value| !value.is_null())
                        .cloned()
                        != origin.receiver().map(|input| input.encode())
                {
                    return Err(Error::new(
                        "CONFLICT",
                        "request_id was committed with different intake provenance",
                    ));
                }
                let artifact =
                    <super::Artifact as super::Catalog>::decode(super::field(&saved, "receipt")?)?;
                // A set part whose saved artifact is gone is not accepted: the daemon sends it
                // again from the claim's source (#285). The wire's part upload reaches here
                // first, inside the two-day window, so the decision is made here too.
                if !super::publication::part_answerable(self, request_id, &artifact)? {
                    return Ok(None);
                }
                artifact
            }
            None => match super::publication::published(
                self,
                &super::import::import_id(request_id),
                now,
            )? {
                Some((_, artifact)) if matches!(origin, Origin::Ordinary) => artifact,
                Some(_) => {
                    return Err(Error::new(
                        "CONFLICT",
                        "staged artifact has no accepted producer provenance",
                    ));
                }
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
        // A retry from another caller never returns, or re-attributes, the original receipt.
        if super::intake::admit_import(artifact.intake.as_ref(), intake).is_err() {
            return Err(Error::new(
                "CONFLICT",
                "request_id was committed with different intake provenance",
            ));
        }
        Ok(Some(artifact))
    }

    pub(super) fn ingest_receipt(
        &self,
        batch: &mut WriteBatch,
        request_id: &str,
        artifact: &super::Artifact,
        origin: Origin<'_>,
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
            .with(
                "producer",
                origin
                    .producer()
                    .map(super::producer_stream::ProducerBatch::encode),
            )
            .with("tail", origin.tail().map(|input| input.encode()))
            .with("receiver", origin.receiver().map(|input| input.encode()))
            .into();
        batch.put(
            format!("request:ingest:{request_id}"),
            json::write_compact(&receipt),
        );
        Ok(())
    }
}
