// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Store-owned graph membership; RDF meaning and query execution stay in PurRDF.

use super::*;
use crate::storage::{
    catalog_lease::{CaptureAdmission, SCAN_BYTES},
    diagnostic,
};
use crate::{DiagnosticCoverage, QueryCancellation, QueryScope};
use std::time::Instant;

/// Coverage of a completed graph-owner capture, independently of query certainty.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RdfScopeCoverage {
    /// Chronological candidates visited, before liveness/source/diagnostic filters.
    pub visited: u64,
    /// Known selected record occurrences, never a count of shared content graphs.
    /// The exact total is unknown when diagnostic membership is incomplete.
    pub selected_records: u64,
    /// Selected occurrences with a ready content projection.
    pub content_ready: u64,
    /// Selected occurrences with a failed content projection.
    pub content_failed: u64,
    /// Selected occurrences to which content projection does not apply.
    pub content_not_applicable: u64,
    /// Diagnostic membership evidence from the shared record matcher.
    pub diagnostics: DiagnosticCoverage,
    /// Catalog reads admitted across reference expansion and candidate loading.
    pub catalog_entries: usize,
}

impl RdfScopeCoverage {
    /// All requested applicable content is ready and membership is known.
    pub fn complete(&self) -> bool {
        self.diagnostics.complete(false) && self.content_failed == 0
    }
}

impl StoredRdfView<'_> {
    fn include_owner(&mut self, id: &str) -> Result<(), ReadError> {
        let owner = self
            .term_id_by_value(&TermValue::iri(rdf_index::graph_iri(id)))?
            .ok_or_else(|| {
                self.budget
                    .fail(Error::new("STORAGE_FAILURE", "RDF graph owner missing"))
            })?;
        if !self.owners.contains(&ordinal(owner)) {
            Arc::get_mut(&mut self._scope)
                .expect("scope capture precedes owned query admission")
                .grow(512)?;
            self.owners.insert(ordinal(owner));
        }
        Ok(())
    }

    /// One graph-layer/readiness law for explicit and filtered record capture.
    pub(super) fn include_record(
        &mut self,
        record: &Record,
        layers: RdfGraphLayers,
        strict: bool,
        coverage: &mut RdfScopeCoverage,
    ) -> Result<(), ReadError> {
        if layers.metadata {
            self.include_owner(&record.id)?;
        }
        if layers.diagnostic
            && let Some(owner) = diagnostic::graph_owner(record)
        {
            self.include_owner(&owner)?;
        }
        if !layers.content {
            return Ok(());
        }
        if record.rdf_projection_id.is_empty() {
            coverage.content_not_applicable += 1;
            return Ok(());
        }
        let status = {
            let _decode = self.budget.reserve(4096 * 64)?;
            let key = format!("rdf:state:{}", record.rdf_projection_id);
            let bytes = self.point(key.as_bytes(), 4096)?.ok_or_else(|| {
                self.budget.fail(Error::new(
                    "STORAGE_FAILURE",
                    "RDF projection state missing",
                ))
            })?;
            let state = crate::storage::parse(&bytes).map_err(|e| self.budget.fail(e))?;
            crate::storage::read_field::<String>(&state, "state")
                .map_err(|e| self.budget.fail(e))?
        };
        match status.as_str() {
            "complete" => {
                coverage.content_ready += 1;
                self.include_owner(&record.rdf_projection_id)?;
            }
            "failed" => {
                coverage.content_failed += 1;
                if strict {
                    return Err(self.budget.fail(Error::new(
                        "INDEX_NOT_READY",
                        "selected record RDF projection failed; inspect its diagnostic",
                    )));
                }
            }
            _ => {
                return Err(self.budget.fail(Error::new(
                    "STORAGE_FAILURE",
                    "invalid RDF projection state",
                )));
            }
        }
        Ok(())
    }
}

impl Store {
    pub(super) fn rdf_read_lease(&self) -> Result<std::sync::MutexGuard<'_, ()>, ReadError> {
        self.rdf_read.try_lock().map_err(|error| {
            Arc::new(match error {
                std::sync::TryLockError::WouldBlock => Error::new(
                    "RESOURCE_LIMIT",
                    "store already has an active RDF snapshot; retry after its lease ends",
                ),
                std::sync::TryLockError::Poisoned(_) => {
                    Error::new("STORAGE_FAILURE", "RDF snapshot admission owner poisoned")
                }
            })
        })
    }

    /// Capture live, artifact, record or pin membership with the shared temporal
    /// and diagnostic filters, then execute against that same native snapshot.
    /// `scope.limit` is a future result-page ceiling, never a membership truncation.
    /// Exhausted capture admission refuses the whole operation before its callback.
    /// The callback runs without the writer lock; cancellation remains effective
    /// at the native source/governor checkpoints throughout query execution.
    pub fn with_rdf_scope<T>(
        &self,
        scope: &QueryScope,
        layers: RdfGraphLayers,
        now: u64,
        limits: RdfReadLimits,
        cancellation: &QueryCancellation,
        operation: impl FnOnce(&StoredRdfView<'_>, &RdfScopeCoverage) -> T,
    ) -> Result<T, ReadError> {
        scope.validate().map_err(Arc::new)?;
        cancellation.checkpoint().map_err(Arc::new)?;
        let _lease = self.rdf_read_lease()?;
        let writer = self.lock().map_err(Arc::new)?;
        let db = Arc::clone(&writer.db);
        let mut view = StoredRdfView::new_cancellable(&db, limits, &[], cancellation.clone())?;
        // Shared catalog admission charges every decoded entry at 64x encoded
        // bytes. Keep this whole capture allowance live until temporary maps drop.
        let _catalog = view.budget.reserve(SCAN_BYTES as u64)?;
        let mut remaining = SCAN_BYTES;
        let pin_maximum = writer
            .config
            .max_pin_members
            .saturating_mul(128)
            .saturating_add(4096);
        let budget = Arc::clone(&view.budget);
        // All reference-expansion catalog bodies together fit this encoded
        // bound under the shared 64x charge. Admit it before any such read.
        budget.request((SCAN_BYTES / 64) as u64, false)?;
        let protected = writer
            .protected_matching(
                now,
                |_| true,
                || {
                    budget
                        .request(pin_maximum as u64, true)
                        .and_then(|_| budget.reserve((pin_maximum as u64).saturating_mul(64)))
                        .map_err(|error| Error::new(error.code, error.message.clone()))
                },
                Some(pin_maximum),
                Some(&mut remaining),
            )
            .map_err(|e| budget.fail(e))?;
        let mut scanned = 0;
        let mut admission = CaptureAdmission {
            cancellation,
            started: Instant::now(),
            scan_budget: scope.scan_budget,
            scanned: &mut scanned,
            remaining: &mut remaining,
        };
        let members = writer
            .query_selection_membership(&scope.selection, now, &protected, Some(&mut admission))
            .map_err(|e| budget.fail(e))?
            .map(|members| members.records);
        let mut candidates = writer
            .query_candidates_from_members(scope, members, |id| {
                writer.get_admitted(id, |bytes| admission.entry(bytes.len()))
            })
            .map_err(|e| budget.fail(e))?;
        let mut coverage = RdfScopeCoverage::default();
        loop {
            budget.request(super::super::catalog_lease::ENTRY_BYTES as u64, true)?;
            let Some(record) = candidates.next() else {
                break;
            };
            let record = record.map_err(|e| budget.fail(e))?;
            coverage.visited += 1;
            if !writer.live_record(&record, now, &protected)
                || !scope.matches_source(&record.source)
                || !writer
                    .matches_diagnostic_scope(
                        scope,
                        &record,
                        &mut coverage.diagnostics,
                        scope.require_complete_index,
                    )
                    .map_err(|e| budget.fail(e))?
            {
                continue;
            }
            writer
                .validate_event_record(&record)
                .map_err(|e| budget.fail(e))?;
            coverage.selected_records += 1;
            view.include_record(&record, layers, scope.require_complete_index, &mut coverage)?;
        }
        drop(candidates);
        coverage.catalog_entries = scanned;
        drop(protected);
        drop(_catalog);
        drop(writer);
        purrdf::dataset_view::checkpointed_drain(&view, |view| operation(view, &coverage))
            .map_err(|failure| failure.error)
    }
}
