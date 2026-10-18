// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Fixed native query recipes. No local relationship evaluator or query parser.

use super::{Error, Store};
use crate::{
    DiagnosticDimension, QueryCancellation, QueryScope, QuerySelection, RdfGraphLayers,
    RdfReadLimits, RdfScopeCoverage, SparqlRead, SparqlReadPage,
};
use purrdf::{TermValue, sparql::QueryGovernors};
use purrdf_lex::json::{Object, Value};

/// Identity of the declared-equality and explicit-interval correlation law.
pub const CORRELATION_PROFILE: &str = "sparrow-correlation/1";

/// Which recorded clock supplies an explicit temporal relationship.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CorrelationClock {
    /// Server acceptance time, never substituted for event time.
    Received,
    /// Known source event time; missing interpretations remain incomplete coverage.
    Event,
}

/// Both endpoints must fall in this explicit half-open interval.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CorrelationInterval {
    /// Receipt or known event time.
    pub clock: CorrelationClock,
    /// Inclusive UTC Unix millisecond bound.
    pub from: u64,
    /// Exclusive UTC Unix millisecond bound.
    pub before: u64,
}

/// Select seed records inside the requested outer scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CorrelationSelector {
    /// At most 32 concrete record/complete-artifact references, validated before filters.
    Evidence(Vec<String>),
    /// Exact declared decoded diagnostic string, without identity or case guesses.
    Diagnostic(DiagnosticDimension, String),
    /// Exact source label.
    Source(String),
    /// Known record time in an explicit half-open interval.
    Time(CorrelationInterval),
}

/// An observed relationship, never an inferred causal edge or global span identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CorrelationRelation {
    /// Equal known declared trace-ID strings.
    TraceId,
    /// Equal known declared span-ID strings, which may be reused by producers.
    SpanId,
    /// The same recorded Source entity.
    Source,
    /// Both known times lie in the explicitly requested interval.
    Time(CorrelationInterval),
}

/// Borrowed governed inputs for the fixed native read recipes.
pub struct CorrelateRead<'a> {
    /// Eligible outer occurrence membership and filters.
    pub scope: &'a QueryScope,
    /// Seeds are selected inside that same immutable scope.
    pub selector: &'a CorrelationSelector,
    /// Which observable relationship to return.
    pub relation: CorrelationRelation,
    /// Native engine governors; federation is always disabled.
    pub governors: &'a QueryGovernors,
    /// Host source limits, shared with ordinary native SPARQL reads.
    pub limits: RdfReadLimits,
}

/// Frozen operation parameters beside the original native certainty and scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CorrelationCapture {
    /// Versioned recipe and typed-parameter fingerprint, independent of data/snapshot.
    pub query_id: String,
    /// Original normalized seed selector.
    pub selector: CorrelationSelector,
    /// Original declared relationship.
    pub relation: CorrelationRelation,
}

impl CorrelationCapture {
    /// Whether all required interpretations and selected graph coverage are known.
    pub fn complete(&self, coverage: &RdfScopeCoverage) -> bool {
        let known = |dimension| {
            coverage
                .diagnostic_known
                .get(&dimension)
                .copied()
                .unwrap_or(0)
                == coverage.selected_records
        };
        let clock = |interval: CorrelationInterval| {
            interval.clock == CorrelationClock::Received
                || coverage.event_time_known == coverage.selected_records
        };
        coverage.complete()
            && match &self.selector {
                CorrelationSelector::Diagnostic(dimension, _) => known(*dimension),
                CorrelationSelector::Time(interval) => clock(*interval),
                _ => true,
            }
            && match self.relation {
                CorrelationRelation::TraceId => known(DiagnosticDimension::TraceId),
                CorrelationRelation::SpanId => known(DiagnosticDimension::SpanId),
                CorrelationRelation::Time(interval) => clock(interval),
                CorrelationRelation::Source => true,
            }
    }

    /// Native JSON description of the normalized recipe and its typed parameters.
    pub fn descriptor(&self) -> Value {
        Object::new()
            .with("profile", CORRELATION_PROFILE)
            .with("query_id", self.query_id.clone())
            .with("selector", selector_value(&self.selector))
            .with("relation", relation_value(self.relation))
            .into()
    }

    /// Check a returned descriptor against the sole recipe/identity owner.
    pub fn validate(&self) -> Result<(), Error> {
        if prepare(&self.selector, self.relation)?.capture != *self {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "correlation descriptor disagrees with its recipe fingerprint",
            ));
        }
        Ok(())
    }
}

fn interval_value(interval: CorrelationInterval) -> Value {
    Object::new()
        .with(
            "clock",
            match interval.clock {
                CorrelationClock::Received => "received",
                CorrelationClock::Event => "event",
            },
        )
        .with("from_ms", interval.from.to_string())
        .with("before_ms", interval.before.to_string())
        .into()
}

fn selector_value(selector: &CorrelationSelector) -> Value {
    match selector {
        CorrelationSelector::Evidence(ids) => Object::new()
            .with("kind", "evidence")
            .with("ids", Value::array(ids.iter().cloned()))
            .into(),
        CorrelationSelector::Diagnostic(dimension, value) => Object::new()
            .with("kind", "diagnostic")
            .with("dimension", dimension.name())
            .with("value", value.clone())
            .into(),
        CorrelationSelector::Source(value) => Object::new()
            .with("kind", "source")
            .with("value", value.clone())
            .into(),
        CorrelationSelector::Time(interval) => Object::new()
            .with("kind", "time")
            .with("interval", interval_value(*interval))
            .into(),
    }
}

fn relation_value(relation: CorrelationRelation) -> Value {
    match relation {
        CorrelationRelation::TraceId => "shared_trace_id".into(),
        CorrelationRelation::SpanId => "shared_span_id".into(),
        CorrelationRelation::Source => "shared_source".into(),
        CorrelationRelation::Time(interval) => Object::new()
            .with("kind", "cooccurs_in_interval")
            .with("interval", interval_value(interval))
            .into(),
    }
}

pub(super) struct Prepared {
    pub query: String,
    pub bindings: Vec<(String, TermValue)>,
    pub capture: CorrelationCapture,
}

fn time_clause(
    interval: CorrelationInterval,
    prefix: &str,
    subject: &str,
    bindings: &mut Vec<(String, TermValue)>,
) -> Result<String, Error> {
    if interval.from >= interval.before {
        return Err(Error::new(
            "INVALID_ARGUMENT",
            "correlation interval must be increasing and half-open",
        ));
    }
    let predicate = match interval.clock {
        CorrelationClock::Received => "receivedAt",
        CorrelationClock::Event => "eventTime",
    };
    for (name, time) in [("from", interval.from), ("before", interval.before)] {
        bindings.push((
            format!("{prefix}_{name}"),
            TermValue::typed_literal(
                crate::format_instant(time)?,
                "http://www.w3.org/2001/XMLSchema#dateTime",
            ),
        ));
    }
    Ok(format!(
        "?{subject} <urn:sparrow:vocab:{predicate}> ?{prefix}_time . FILTER(?{prefix}_time >= ?{prefix}_from && ?{prefix}_time < ?{prefix}_before) "
    ))
}

fn prepare(
    selector: &CorrelationSelector,
    relation: CorrelationRelation,
) -> Result<Prepared, Error> {
    let mut selector = selector.clone();
    let mut bindings = Vec::new();
    let mut body = String::new();
    match &mut selector {
        CorrelationSelector::Evidence(ids) => {
            if ids.is_empty() || ids.len() > 32 {
                return Err(Error::new(
                    "INVALID_ARGUMENT",
                    "correlation requires 1..32 seed references",
                ));
            }
            QuerySelection::Evidence(ids.clone()).validate()?;
            ids.sort();
            ids.dedup();
            let mut alternatives = Vec::new();
            for (index, id) in ids.iter().enumerate() {
                let name = format!("selected_{index}");
                alternatives.push(format!(
                    "?{} = ?{name}",
                    if id.starts_with("record:") {
                        "seed"
                    } else {
                        "seed_artifact"
                    }
                ));
                bindings.push((name, TermValue::iri(format!("urn:sparrow:{id}"))));
            }
            body.push_str(&format!("FILTER({}) ", alternatives.join(" || ")));
        }
        CorrelationSelector::Diagnostic(dimension, value) => {
            if value.is_empty() || value.len() > 512 {
                return Err(Error::new(
                    "INVALID_ARGUMENT",
                    "diagnostic seed value requires 1..512 bytes",
                ));
            }
            bindings.push((
                "selected_value".into(),
                TermValue::simple_literal(value.clone()),
            ));
            body.push_str(&format!(
                "?seed <{}> ?selected_value . ",
                dimension.predicate_iri()
            ));
        }
        CorrelationSelector::Source(value) => {
            if value.is_empty() || value.len() > 512 {
                return Err(Error::new(
                    "INVALID_ARGUMENT",
                    "source seed requires 1..512 bytes",
                ));
            }
            bindings.push((
                "selected_source".into(),
                TermValue::simple_literal(value.clone()),
            ));
            body.push_str("FILTER(?seed_source_name = ?selected_source) ");
        }
        CorrelationSelector::Time(interval) => {
            body.push_str(&time_clause(*interval, "selected", "seed", &mut bindings)?)
        }
    }
    match relation {
        CorrelationRelation::TraceId | CorrelationRelation::SpanId => {
            let dimension = if relation == CorrelationRelation::TraceId {
                DiagnosticDimension::TraceId
            } else {
                DiagnosticDimension::SpanId
            };
            body.push_str(&format!(
                "?seed <{0}> ?relationship_value . ?related <{0}> ?relationship_value . ",
                dimension.predicate_iri()
            ));
        }
        CorrelationRelation::Source => body.push_str(
            "FILTER(?seed_source = ?related_source) BIND(?seed_source AS ?relationship_value) ",
        ),
        CorrelationRelation::Time(interval) => {
            body.push_str(&time_clause(
                interval,
                "seed_window",
                "seed",
                &mut bindings,
            )?);
            body.push_str(&time_clause(
                interval,
                "related_window",
                "related",
                &mut bindings,
            )?);
        }
    }
    let query = format!(
        r#"PREFIX s: <urn:sparrow:vocab:>
SELECT (STRAFTER(STR(?seed), "urn:sparrow:") AS ?seed_id)
       (STRAFTER(STR(?related), "urn:sparrow:") AS ?record_id)
       ?seed ?related ?seed_artifact ?related_artifact ?seed_source ?related_source
       ?seed_source_name ?related_source_name ?seed_received ?related_received
       ?seed_start ?seed_end ?related_start ?related_end ?relationship_value
       ?seed_window_time ?related_window_time
WHERE {{
 ?seed a s:Record ; s:artifact ?seed_artifact ; s:source ?seed_source ; s:sourceName ?seed_source_name ; s:receivedAt ?seed_received ; s:sourceByteStart ?seed_start ; s:sourceByteEnd ?seed_end .
 ?related a s:Record ; s:artifact ?related_artifact ; s:source ?related_source ; s:sourceName ?related_source_name ; s:receivedAt ?related_received ; s:sourceByteStart ?related_start ; s:sourceByteEnd ?related_end .
 FILTER(?seed != ?related)
 {body}
}} ORDER BY ?seed_received ?seed ?related_received ?related"#
    );
    let mut capture = CorrelationCapture {
        query_id: String::new(),
        selector,
        relation,
    };
    let mut digest = super::blake3::Hasher::new();
    // Only recipe structure and native typed parameter values enter this key.
    let encoded = super::json::write_compact(&capture.descriptor());
    for part in [
        CORRELATION_PROFILE.as_bytes(),
        query.as_bytes(),
        encoded.as_bytes(),
    ] {
        purrdf_hash::frame::frame_le_with(part, &mut |chunk| {
            digest.update(chunk);
        });
    }
    capture.query_id = format!(
        "correlation-query:{}",
        super::hex::encode(digest.finalize().as_bytes())
    );
    Ok(Prepared {
        query,
        bindings,
        capture,
    })
}

impl Store {
    /// Correlate through the native governed evaluator over one captured scope.
    pub fn correlate_read(
        &self,
        read: &CorrelateRead<'_>,
        now: u64,
        cancellation: &QueryCancellation,
    ) -> Result<SparqlReadPage, crate::RdfReadError> {
        let prepared = prepare(read.selector, read.relation).map_err(std::sync::Arc::new)?;
        self.capture_sparql(
            &SparqlRead {
                scope: read.scope,
                layers: RdfGraphLayers {
                    metadata: true,
                    diagnostic: true,
                    content: false,
                },
                request: purrdf::SparqlRequest {
                    query: &prepared.query,
                    base_iri: None,
                    substitutions: &prepared.bindings,
                },
                governors: read.governors,
                limits: read.limits,
            },
            Some(&prepared.capture),
            now,
            cancellation,
        )
    }
}
