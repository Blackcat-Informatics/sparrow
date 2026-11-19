// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Query-scoped native relations over stored text facts and source spans.

use super::*;
use crate::storage::text_index;
use std::collections::BTreeMap;

mod producer;
#[cfg(test)]
mod tests;

/// Retained-record lexical corpus law shared with ordinary text searches.
pub const STORED_TEXT_CORPUS_PROFILE: &str = crate::text::PROFILE;
/// Stored ranking/span column shapes and cooperative source-work law.
pub const STORED_TEXT_RELATION_PROFILE: &str = "sparrow-stored-text-relations-v1";

/// Caller-selected predicates for retained-record ranking and source spans.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SparqlTextRelations {
    /// `?doc <iri> (needle ?recordId ?score ?rank ?matched)`.
    pub ranked_iri: String,
    /// `?doc <iri> (needle ?recordId ?term ?start ?end ?basis ?path ?coarse)`.
    /// Start/end are record-local byte offsets; decoded scalar spans can be coarse.
    pub spans_iri: String,
}

impl SparqlTextRelations {
    pub(super) fn validate(&self) -> Result<(), Error> {
        if self.ranked_iri == self.spans_iri {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "text relation IRIs must be distinct",
            ));
        }
        for iri in [&self.ranked_iri, &self.spans_iri] {
            if iri.len() > 1024 {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "text relation IRI exceeds 1024 bytes",
                ));
            }
            let parsed = purrdf_core::parse_iri(iri)
                .map_err(|error| Error::new("INVALID_ARGUMENT", error.to_string()))?;
            if parsed.scheme().is_none() {
                return Err(Error::new(
                    "INVALID_ARGUMENT",
                    "text relation requires an absolute IRI",
                ));
            }
        }
        Ok(())
    }
}

/// Independent text-index coverage over the query's selected occurrences.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SparqlTextCoverage {
    /// Selected occurrences with complete text facts, including zero-token inputs.
    pub ready: u64,
    /// Selected occurrences whose applicable text facts failed or are incomplete.
    pub failed: u64,
    /// Selected occurrences outside the retained-record text profile.
    pub not_applicable: u64,
}

/// Owned corpus/profile evidence; retaining it never retains the snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SparqlTextCapture {
    /// Original caller-selected predicates and their declared relation shapes.
    pub relations: SparqlTextRelations,
    /// Stable identity of the ordered covered documents and their token lengths.
    pub corpus_id: String,
    /// Fingerprint of the captured native analyzer law.
    pub analyzer_id: String,
    /// Covered nonempty documents used by native prepared scoring.
    pub documents: u64,
    /// Covered token total used by native prepared scoring.
    pub tokens: u128,
    /// Projection coverage, independently of native query certainty.
    pub coverage: SparqlTextCoverage,
}

impl SparqlTextCapture {
    /// Whether the selected record corpus has complete membership and text coverage.
    pub fn membership_complete(&self, scope: &RdfScopeCoverage) -> bool {
        scope.diagnostics.complete(false)
            && self.coverage.failed == 0
            && self.coverage.not_applicable == 0
    }

    pub(super) fn transport_bytes(&self) -> usize {
        4096usize.saturating_add(
            self.relations
                .ranked_iri
                .len()
                .saturating_add(self.relations.spans_iri.len())
                .saturating_add(self.corpus_id.len())
                .saturating_add(self.analyzer_id.len())
                .saturating_mul(6),
        )
    }
}

pub(super) struct TextDocument {
    pub length: u64,
    pub byte_length: u64,
    pub rdf_iri: String,
}

pub(super) struct TextScope {
    pub analyzer: purrdf::text::Analyzer,
    pub analyzer_id: String,
    pub generation: Arc<str>,
    pub documents: BTreeMap<String, TextDocument>,
    pub tokens: u128,
    pub scan_budget: usize,
    pub coverage: SparqlTextCoverage,
}

impl TextScope {
    pub(super) fn new(
        analyzer: purrdf::text::Analyzer,
        scan_budget: usize,
        store_id: &str,
        sequence: u64,
    ) -> Self {
        let analyzer_id = purrdf_hash::hex::encode(&analyzer.fingerprint());
        Self {
            analyzer,
            analyzer_id,
            generation: Arc::from(format!("{store_id}@{sequence}")),
            documents: BTreeMap::new(),
            tokens: 0,
            scan_budget,
            coverage: SparqlTextCoverage::default(),
        }
    }
}

impl StoredRdfView<'_> {
    pub(super) fn text_environment(
        &self,
        capture: &SparqlTextCapture,
    ) -> Result<purrdf::sparql::ExtensionEnv<'_>, ReadError> {
        producer::environment(self, capture)
    }
    // Same catalog-body law used by the membership capture: borrow the native
    // value, admit its encoded size, then retain 64x before decoding it.
    fn text_catalog(&self, key: &[u8]) -> Result<EncodedGuard<'_>, ReadError> {
        self.budget.request(key.len() as u64, false)?;
        let bytes = self
            .snapshot
            .get_pinned(key)
            .map_err(|error| self.budget.fail(error.into()))?
            .ok_or_else(|| {
                self.budget.fail(Error::new(
                    "STORAGE_FAILURE",
                    "selected stored-text catalog body missing",
                ))
            })?;
        self.budget.admit_bytes(bytes.len() as u64)?;
        let capacity = (bytes.len() as u64)
            .checked_mul(64)
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| {
                self.budget.fail(Error::new(
                    "RESOURCE_LIMIT",
                    "stored-text catalog admission overflow",
                ))
            })?;
        let reservation = self.budget.reserve(capacity)?;
        Ok(EncodedGuard {
            bytes,
            _reservation: reservation,
        })
    }

    pub(super) fn include_text(&mut self, record: &Record, strict: bool) -> Result<(), ReadError> {
        let Some(text) = self.text_scope.as_ref() else {
            return Ok(());
        };
        let encoded = self.text_catalog(format!("text-record:{}", record.id).as_bytes())?;
        let facts =
            crate::storage::parse(&encoded.bytes).map_err(|error| self.budget.fail(error))?;
        let metadata = text_index::text_metadata(&facts, &text.analyzer_id, strict)
            .map_err(|error| self.budget.fail(error))?;
        let length = metadata.length;
        let failed = metadata.failed;
        let not_applicable = metadata.not_applicable;
        drop(facts);
        drop(encoded);
        if length > 0 {
            Arc::get_mut(&mut self._scope)
                .expect("text membership precedes native owned-query admission")
                .grow(512)?;
        }
        let text = self.text_scope.as_mut().expect("text capture initialized");
        text.coverage.failed += u64::from(failed);
        text.coverage.not_applicable += u64::from(not_applicable);
        text.coverage.ready += u64::from(!failed && !not_applicable);
        if length > 0 {
            let byte_length = record
                .byte_end
                .checked_sub(record.byte_start)
                .ok_or_else(|| {
                    self.budget.fail(Error::new(
                        "STORAGE_FAILURE",
                        "text record byte interval reversed",
                    ))
                })?;
            text.tokens = text.tokens.checked_add(u128::from(length)).ok_or_else(|| {
                self.budget.fail(Error::new(
                    "STORAGE_FAILURE",
                    "text corpus token count overflow",
                ))
            })?;
            text.documents.insert(
                record.id.clone(),
                TextDocument {
                    length,
                    byte_length,
                    rdf_iri: rdf_index::entity_iri(&record.id),
                },
            );
        }
        Ok(())
    }

    pub(super) fn text_capture(
        &self,
        store_id: &str,
        relations: &SparqlTextRelations,
    ) -> Result<SparqlTextCapture, ReadError> {
        let text = self.text_scope.as_ref().ok_or_else(|| {
            self.budget.fail(Error::new(
                "STORAGE_FAILURE",
                "requested text scope was not captured",
            ))
        })?;
        let corpus_id = text_index::corpus_identity(
            store_id,
            &text.analyzer.fingerprint(),
            text.documents
                .iter()
                .map(|(id, document)| (id.as_str(), document.length)),
            || {
                self.budget
                    .checkpoint()
                    .0
                    .map_or(Ok(()), |error| Err(Error::clone(&error)))
            },
        )
        .map_err(|error| self.budget.fail(error))?;
        Ok(SparqlTextCapture {
            relations: relations.clone(),
            corpus_id,
            analyzer_id: text.analyzer_id.clone(),
            documents: text.documents.len() as u64,
            tokens: text.tokens,
            coverage: text.coverage.clone(),
        })
    }
}
