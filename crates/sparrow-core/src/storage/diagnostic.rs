// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Immutable source interpretation and bounded diagnostic occurrence evidence.

use super::{Catalog, Error, Record, Store, Writer, blake3, hex, integer, json, parse, read_field};
use purrdf_lex::json::{Object, Value};
use rocksdb::{DB, WriteBatch, WriteOptions};
use std::collections::BTreeMap;

/// Store vocabulary dimensions, interpreted as exact native decoded strings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum DiagnosticDimension {
    /// Logical service identifier.
    Service,
    /// Producer host identifier.
    Host,
    /// Producer severity spelling; no implicit case folding or numeric conversion.
    Severity,
    /// Producer trace identity.
    TraceId,
    /// Producer span identity.
    SpanId,
}

impl DiagnosticDimension {
    /// Closed vocabulary spelling used in profiles, filters and diagnostic RDF.
    pub fn name(self) -> &'static str {
        match self {
            Self::Service => "service",
            Self::Host => "host",
            Self::Severity => "severity",
            Self::TraceId => "trace_id",
            Self::SpanId => "span_id",
        }
    }

    /// Decode a declared dimension; never guess from a source field name.
    pub fn from_name(name: &str) -> Result<Self, Error> {
        match name {
            "service" => Ok(Self::Service),
            "host" => Ok(Self::Host),
            "severity" => Ok(Self::Severity),
            "trace_id" => Ok(Self::TraceId),
            "span_id" => Ok(Self::SpanId),
            _ => Err(Error::new(
                "INVALID_ARGUMENT",
                "diagnostic dimension must be service, host, severity, trace_id or span_id",
            )),
        }
    }
}

/// Immutable explicit source mapping. Reinterpretation requires another source ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiagnosticProfile {
    /// Content identity including source, interpretation and sorted mappings.
    pub id: String,
    /// Exact logical source identity.
    pub source: String,
    /// At most one native concrete RFC 6901 pointer per diagnostic dimension.
    pub mappings: BTreeMap<DiagnosticDimension, String>,
}

impl DiagnosticProfile {
    fn new(source: &str, mappings: BTreeMap<DiagnosticDimension, String>) -> Result<Self, Error> {
        if source.is_empty()
            || source.len() > 512
            || mappings.is_empty()
            || mappings
                .values()
                .any(|path| path.len() > 4096 || purrdf_lex::json_pointer::tokens(path).is_none())
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "require source 1..512 bytes and 1..5 valid native JSON pointers of at most 4096 bytes",
            ));
        }
        let mut framed = Vec::new();
        for value in ["sparrow-diagnostic-decoded-exact-v1", source] {
            purrdf_hash::frame::frame_le(&mut framed, value.as_bytes());
        }
        for (dimension, path) in &mappings {
            purrdf_hash::frame::frame_le(&mut framed, dimension.name().as_bytes());
            purrdf_hash::frame::frame_le(&mut framed, path.as_bytes());
        }
        Ok(Self {
            id: format!(
                "diagnostic-profile:{}",
                hex::encode(blake3::hash(&framed).as_bytes())
            ),
            source: source.into(),
            mappings,
        })
    }
}

impl Catalog for DiagnosticProfile {
    fn encode(&self) -> Value {
        Object::new()
            .with("id", self.id.clone())
            .with("source", self.source.clone())
            .with(
                "mappings",
                Value::array(self.mappings.iter().map(|(dimension, path)| {
                    Object::new()
                        .with("dimension", dimension.name())
                        .with("path", path.clone())
                })),
            )
            .into()
    }
    fn decode(value: &Value) -> Result<Self, Error> {
        let rows = value
            .get("mappings")
            .and_then(Value::as_array)
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "diagnostic mappings missing"))?;
        let mut mappings = BTreeMap::new();
        for row in rows {
            let dimension =
                DiagnosticDimension::from_name(&read_field::<String>(row, "dimension")?)
                    .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
            if mappings
                .insert(dimension, read_field(row, "path")?)
                .is_some()
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "duplicate diagnostic mapping dimension",
                ));
            }
        }
        let profile = Self::new(&read_field::<String>(value, "source")?, mappings)
            .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
        if profile.id != read_field::<String>(value, "id")? {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "diagnostic profile identity mismatch",
            ));
        }
        Ok(profile)
    }
}

/// Exact native interpretation, or explicit unavailable/ambiguous evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiagnosticValue {
    /// A unique nonempty decoded string of at most 512 UTF-8 bytes.
    Known {
        /// Exact decoded string; original spelling remains in the source bytes.
        value: String,
        /// Native ordered JSON occurrence identity.
        occurrence: u64,
        /// Original artifact span start.
        source_start: u64,
        /// Original artifact span end.
        source_end: u64,
    },
    /// Stable reason: not_json, projection_failed, missing_field, ambiguous_field,
    /// wrong_kind, decoding_failure or value_limit.
    Unknown(String),
}

/// Captured per-record evidence, never reinterpreted through a later mapping.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiagnosticSnapshot {
    /// Immutable mapping identity, absent for unprofiled and legacy records.
    pub profile_id: Option<String>,
    /// One result per declared dimension, including failures.
    pub fields: BTreeMap<DiagnosticDimension, DiagnosticValue>,
}

impl DiagnosticSnapshot {
    pub(super) fn encode(&self) -> Value {
        Object::new()
            .with("profile_id", self.profile_id.clone())
            .with(
                "fields",
                Value::array(self.fields.iter().map(|(dimension, value)| {
                    let row = Object::new().with("dimension", dimension.name());
                    match value {
                        DiagnosticValue::Known {
                            value,
                            occurrence,
                            source_start,
                            source_end,
                        } => row
                            .with("state", "known")
                            .with("value", value.clone())
                            .with("occurrence", occurrence.to_string())
                            .with("source_start", source_start.to_string())
                            .with("source_end", source_end.to_string()),
                        DiagnosticValue::Unknown(reason) => {
                            row.with("state", "unknown").with("reason", reason.clone())
                        }
                    }
                })),
            )
            .into()
    }

    pub(super) fn decode(value: &Value, start: u64, end: u64) -> Result<Self, Error> {
        let profile_id: Option<String> = read_field(value, "profile_id")?;
        if profile_id.as_ref().is_some_and(|id| {
            id.strip_prefix("diagnostic-profile:")
                .and_then(purrdf_hash::hex::decode_32_canonical)
                .is_none()
        }) {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "invalid diagnostic profile identity",
            ));
        }
        let rows = value
            .get("fields")
            .and_then(Value::as_array)
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "diagnostic fields missing"))?;
        if rows.len() > 5 || profile_id.is_some() == rows.is_empty() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "diagnostic profile presence disagrees with fields",
            ));
        }
        let mut fields = BTreeMap::new();
        for row in rows {
            let dimension =
                DiagnosticDimension::from_name(&read_field::<String>(row, "dimension")?)
                    .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
            let value = match read_field::<String>(row, "state")?.as_str() {
                "known" => {
                    let value: String = read_field(row, "value")?;
                    let source_start = integer(row, "source_start")?;
                    let source_end = integer(row, "source_end")?;
                    if value.is_empty()
                        || value.len() > 512
                        || source_start < start
                        || source_start > source_end
                        || source_end > end
                    {
                        return Err(Error::new(
                            "STORAGE_FAILURE",
                            "invalid diagnostic value/span",
                        ));
                    }
                    DiagnosticValue::Known {
                        value,
                        occurrence: integer(row, "occurrence")?,
                        source_start,
                        source_end,
                    }
                }
                "unknown" => {
                    let reason: String = read_field(row, "reason")?;
                    if ![
                        "not_json",
                        "projection_failed",
                        "missing_field",
                        "ambiguous_field",
                        "wrong_kind",
                        "decoding_failure",
                        "value_limit",
                    ]
                    .contains(&reason.as_str())
                    {
                        return Err(Error::new(
                            "STORAGE_FAILURE",
                            "unknown diagnostic failure reason",
                        ));
                    }
                    DiagnosticValue::Unknown(reason)
                }
                _ => return Err(Error::new("STORAGE_FAILURE", "unknown diagnostic state")),
            };
            if fields.insert(dimension, value).is_some() {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "duplicate captured diagnostic dimension",
                ));
            }
        }
        Ok(Self { profile_id, fields })
    }
}

impl purrdf_lex::json::record::FromJson for DiagnosticSnapshot {
    fn from_json(value: &Value) -> Result<Self, purrdf_lex::json::record::DecodeError> {
        Self::decode(value, 0, u64::MAX).map_err(purrdf_lex::json::record::DecodeError::custom)
    }
}

/// Unique mapped native occurrence; duplicate fields are ambiguous even if equal.
pub(super) enum MappedOccurrence<'a> {
    Missing,
    Ambiguous,
    Value(&'a Value),
}

pub(super) fn mapped_occurrence<'a>(
    values: &'a [Value],
    path: &str,
) -> Result<MappedOccurrence<'a>, Error> {
    let mut selected = None;
    for value in values {
        if read_field::<String>(value, "path")? == path {
            if selected.is_some() {
                return Ok(MappedOccurrence::Ambiguous);
            }
            selected = Some(value);
        }
    }
    Ok(selected.map_or(MappedOccurrence::Missing, MappedOccurrence::Value))
}

pub(super) fn capture(
    profile: Option<&DiagnosticProfile>,
    facts: Option<&Value>,
    start: u64,
    end: u64,
) -> Result<DiagnosticSnapshot, Error> {
    let Some(profile) = profile else {
        return Ok(DiagnosticSnapshot::default());
    };
    let unavailable = match facts {
        None => Some("not_json"),
        Some(facts) => match read_field::<String>(facts, "state")?.as_str() {
            "complete" => None,
            "failed" => Some("projection_failed"),
            _ => {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "unknown diagnostic projection state",
                ));
            }
        },
    };
    let mut fields = BTreeMap::new();
    for (dimension, path) in &profile.mappings {
        let result = if let Some(reason) = unavailable {
            DiagnosticValue::Unknown(reason.into())
        } else {
            let values = facts
                .and_then(|v| v.get("values"))
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    Error::new("STORAGE_FAILURE", "native diagnostic occurrences missing")
                })?;
            match mapped_occurrence(values, path)? {
                MappedOccurrence::Missing => DiagnosticValue::Unknown("missing_field".into()),
                MappedOccurrence::Ambiguous => DiagnosticValue::Unknown("ambiguous_field".into()),
                MappedOccurrence::Value(value) => {
                    if read_field::<String>(value, "kind")? != "string" {
                        DiagnosticValue::Unknown("wrong_kind".into())
                    } else if value.get("decoding_error").is_some() {
                        DiagnosticValue::Unknown("decoding_failure".into())
                    } else {
                        let decoded: String = read_field(value, "decoded")?;
                        if decoded.is_empty() || decoded.len() > 512 {
                            DiagnosticValue::Unknown("value_limit".into())
                        } else {
                            DiagnosticValue::Known {
                                value: decoded,
                                occurrence: integer(value, "occurrence")?,
                                source_start: start
                                    .checked_add(integer(value, "start")?)
                                    .ok_or_else(|| {
                                        Error::new("STORAGE_FAILURE", "diagnostic span overflow")
                                    })?,
                                source_end: start.checked_add(integer(value, "end")?).ok_or_else(
                                    || Error::new("STORAGE_FAILURE", "diagnostic span overflow"),
                                )?,
                            }
                        }
                    }
                }
            }
        };
        fields.insert(*dimension, result);
    }
    let snapshot = DiagnosticSnapshot {
        profile_id: Some(profile.id.clone()),
        fields,
    };
    DiagnosticSnapshot::decode(&snapshot.encode(), start, end)
}

pub(super) fn source_key(source: &str) -> String {
    format!(
        "source-diagnostic:{}",
        hex::encode(blake3::hash(source.as_bytes()).as_bytes())
    )
}

pub(super) fn graph_owner(record: &Record) -> Option<String> {
    record
        .diagnostics
        .profile_id
        .as_ref()
        .map(|_| format!("diagnostic:{}", record.id))
}

/// Build only Sparrow's interpretation vocabulary through the native RDF builder.
pub(super) fn rdf(record: &Record) -> Result<std::sync::Arc<purrdf::RdfDataset>, Error> {
    use purrdf::{RdfDatasetBuilder, RdfLiteral};
    let mut builder = RdfDatasetBuilder::new();
    let subject = builder.intern_iri(&format!("urn:sparrow:{}", record.id));
    for (dimension, value) in &record.diagnostics.fields {
        let evidence = builder.intern_iri(&format!(
            "urn:sparrow:diagnostic:{}:{}",
            record.id,
            dimension.name()
        ));
        let predicate = builder.intern_iri("urn:sparrow:vocab:diagnosticEvidence");
        builder.push_quad(subject, predicate, evidence, None);
        if let DiagnosticValue::Known { value, .. } = value {
            let literal = builder.intern_literal(RdfLiteral::simple(value));
            let predicate = builder.intern_iri(&format!(
                "urn:sparrow:vocab:{}",
                match dimension {
                    DiagnosticDimension::TraceId => "traceId",
                    DiagnosticDimension::SpanId => "spanId",
                    _ => dimension.name(),
                }
            ));
            builder.push_quad(subject, predicate, literal, None);
        }
        let mut facts = vec![(
            "dimension",
            builder.intern_literal(RdfLiteral::simple(dimension.name())),
        )];
        if let Some(profile_id) = &record.diagnostics.profile_id {
            facts.push((
                "sourceDiagnosticProfile",
                builder.intern_iri(&format!("urn:sparrow:{profile_id}")),
            ));
        }
        match value {
            DiagnosticValue::Known {
                value,
                occurrence,
                source_start,
                source_end,
            } => {
                facts.push(("value", builder.intern_literal(RdfLiteral::simple(value))));
                for (name, number) in [
                    ("occurrence", occurrence),
                    ("sourceByteStart", source_start),
                    ("sourceByteEnd", source_end),
                ] {
                    facts.push((
                        name,
                        builder.intern_literal(RdfLiteral::typed(
                            number.to_string(),
                            "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
                        )),
                    ));
                }
            }
            DiagnosticValue::Unknown(reason) => facts.push((
                "diagnosticReason",
                builder.intern_literal(RdfLiteral::simple(reason)),
            )),
        }
        for (name, object) in facts {
            let predicate = builder.intern_iri(&format!("urn:sparrow:vocab:{name}"));
            builder.push_quad(evidence, predicate, object, None);
        }
    }
    builder
        .freeze()
        .map_err(|e| Error::new("PROJECTION_FAILURE", e.to_string()))
}

/// Diagnostic membership evidence over the visited live source/time selection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DiagnosticCoverage {
    /// Records whose diagnostic scope was evaluated.
    pub evaluated: u64,
    /// Records certainly in diagnostic scope.
    pub matched: u64,
    /// Records certainly excluded by a known dimension.
    pub excluded: u64,
    /// Records whose scope membership cannot be decided.
    pub unknown: u64,
}

impl DiagnosticCoverage {
    /// Combine disjoint context directions with the anchor.
    pub fn merge(&mut self, other: Self) {
        self.evaluated += other.evaluated;
        self.matched += other.matched;
        self.excluded += other.excluded;
        self.unknown += other.unknown;
    }

    /// A completed traversal still has uncertain membership when mappings are unknown.
    pub fn complete(self, limited: bool) -> bool {
        !limited && self.unknown == 0
    }

    /// Preserve unknown totals while exposing covered lower bounds separately.
    pub fn count(self, value: u64, limited: bool) -> Value {
        if self.complete(limited) {
            Value::from(value.to_string())
        } else {
            Value::Null
        }
    }

    /// One native JSON coverage codec shared by every query surface.
    pub fn encode(self, visited: u64, eligible: u64, failures: [u64; 3], limited: bool) -> Value {
        Object::new()
            .with("visited", visited.to_string())
            .with("eligible_records", self.count(eligible, limited))
            .with("known_eligible_records", eligible.to_string())
            .with("failed_json_records", failures[0].to_string())
            .with("non_json_records", failures[1].to_string())
            .with("decoding_failures", failures[2].to_string())
            .with(
                "diagnostic_scope",
                Object::new()
                    .with("evaluated", self.evaluated.to_string())
                    .with("matched", self.matched.to_string())
                    .with("excluded", self.excluded.to_string())
                    .with("unknown", self.unknown.to_string())
                    .with("membership_complete", self.complete(limited)),
            )
            .into()
    }
}

impl Writer {
    pub(super) fn matches_diagnostic_scope(
        &self,
        scope: &crate::QueryScope,
        record: &Record,
        coverage: &mut DiagnosticCoverage,
        strict: bool,
    ) -> Result<bool, Error> {
        self.validate_diagnostic_record(record)?;
        coverage.evaluated += 1;
        let mut matched = true;
        let mut unknown = None;
        for (dimension, alternatives) in &scope.diagnostics {
            match record.diagnostics.fields.get(dimension) {
                Some(DiagnosticValue::Known { value, .. }) => {
                    matched &= alternatives.ids().iter().any(|wanted| wanted == value)
                }
                _ => unknown = Some(dimension),
            }
        }
        if !matched {
            coverage.excluded += 1;
            return Ok(false);
        }
        if let Some(dimension) = unknown {
            if strict {
                return Err(Error::new(
                    "INDEX_NOT_READY",
                    format!(
                        "{} diagnostic scope cannot be certified for record {}; inspect its captured diagnostics or narrow scope",
                        dimension.name(),
                        record.id
                    ),
                ));
            }
            coverage.unknown += 1;
            return Ok(false);
        }
        coverage.matched += 1;
        Ok(true)
    }

    pub(super) fn diagnostic_profile(
        &self,
        source: &str,
    ) -> Result<Option<DiagnosticProfile>, Error> {
        let Some(bytes) = self.db.get(source_key(source))? else {
            return Ok(None);
        };
        let profile = DiagnosticProfile::decode(&parse(&bytes)?)?;
        if profile.source != source {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "diagnostic source binding mismatch",
            ));
        }
        Ok(Some(profile))
    }

    pub(super) fn validate_diagnostic_record(&self, record: &Record) -> Result<(), Error> {
        if let Some(id) = &record.diagnostics.profile_id {
            let profile = self.diagnostic_profile(&record.source)?.ok_or_else(|| {
                Error::new("STORAGE_FAILURE", "captured diagnostic profile missing")
            })?;
            if profile.id != *id || !profile.mappings.keys().eq(record.diagnostics.fields.keys()) {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "captured diagnostic mapping disagrees with immutable binding",
                ));
            }
        }
        Ok(())
    }
}

impl Store {
    /// Durably define an immutable explicit diagnostic source interpretation.
    /// Identical retries preserve the profile; conflicts require a new source ID.
    pub fn define_diagnostic_profile(
        &self,
        source: &str,
        mappings: BTreeMap<DiagnosticDimension, String>,
    ) -> Result<DiagnosticProfile, Error> {
        let profile = DiagnosticProfile::new(source, mappings)?;
        let writer = self.lock()?;
        if let Some(old) = writer.diagnostic_profile(source)? {
            if old != profile {
                return Err(Error::new(
                    "CONFLICT",
                    "diagnostic mapping is immutable; use a new source ID",
                ));
            }
            return Ok(old);
        }
        let encoded = json::write_compact(&profile.encode());
        writer.admit_source_profile("source-diagnostic:", encoded.as_bytes(), |key, bytes| {
            let retained = DiagnosticProfile::decode(&parse(bytes)?)?;
            if key != source_key(&retained.source).as_bytes() {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "diagnostic registry key mismatch",
                ));
            }
            Ok(())
        })?;
        let mut batch = WriteBatch::default();
        batch.put(source_key(source), encoded);
        writer.commit(batch)?;
        Ok(profile)
    }

    /// Retrieve the immutable source mapping; historical unprofiled records stay unprofiled.
    pub fn diagnostic_profile(&self, source: &str) -> Result<Option<DiagnosticProfile>, Error> {
        if source.is_empty() || source.len() > 512 {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "source must be 1..512 bytes",
            ));
        }
        self.lock()?.diagnostic_profile(source)
    }
}

pub(super) fn fence_format(db: &DB) -> Result<(), Error> {
    if let Some(legacy) = db
        .get("store_format")?
        .as_deref()
        .and_then(super::recordless::legacy_format)
        .filter(|legacy| !legacy.needs_recordless_index)
    {
        if !matches!(
            db.get("store_format")?.as_deref(),
            Some(
                b"sparrow.store/13"
                    | b"sparrow.store/14"
                    | b"sparrow.store/15"
                    | b"sparrow.store/16"
            )
        ) && let Some(row) = db.prefix_iterator("source-diagnostic:").next()
            && row?.0.starts_with(b"source-diagnostic:")
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "legacy store has unfenced diagnostic mappings",
            ));
        }
        if db.get(super::producer_stream::KEY)?.is_some() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "legacy store has unfenced producer stream registry",
            ));
        }
        let mut batch = WriteBatch::default();
        batch.put("store_format", legacy.target);
        batch.put(super::producer_stream::KEY, b"[]");
        let mut options = WriteOptions::default();
        options.set_sync(true);
        options.disable_wal(false);
        db.write_opt(batch, &options)?;
    }
    Ok(())
}
