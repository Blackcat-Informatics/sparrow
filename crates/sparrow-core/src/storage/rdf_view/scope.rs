// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Store-owned graph membership; RDF meaning and query execution stay in PurRDF.

use super::*;
use crate::storage::temporal_index::{TemporalRows, row_bound};
use crate::storage::{
    catalog_lease::{CaptureAdmission, SCAN_BYTES},
    diagnostic,
};
use crate::{DiagnosticCoverage, QueryCancellation, QueryScope};
use std::time::Instant;

fn catalog_entry(
    budget: &Budget,
    id: &str,
    bytes: &[u8],
    admission: &mut CaptureAdmission<'_>,
) -> Result<(), Error> {
    admission.entry(bytes.len())?;
    budget
        .admit_bytes((id.len() + bytes.len()) as u64)
        .map_err(|error| Error::new(error.code, error.message.clone()))
}

fn artifact_catalog(
    writer: &crate::storage::Writer,
    budget: &Budget,
    admission: &mut CaptureAdmission<'_>,
    key: &[u8],
    value: &[u8],
) -> Result<crate::storage::manifest_retire::Header, Error> {
    use crate::storage::artifact_rdf;
    crate::storage::temporal_index::check_width(artifact_rdf::TIME, key, value)?;
    admission.entry(key.len() + value.len())?;
    let id = std::str::from_utf8(value)
        .map_err(|_| Error::new("STORAGE_FAILURE", "invalid artifact receipt identity"))?;
    if !id.starts_with("artifact:") || id.len() <= 9 {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "artifact receipt requires an artifact identity",
        ));
    }
    let bytes = writer.db.get_pinned(id)?.ok_or_else(|| {
        Error::new(
            "STORAGE_FAILURE",
            "artifact receipt references a missing catalog",
        )
    })?;
    budget
        .admit_bytes((id.len() + bytes.len()) as u64)
        .map_err(|error| Error::new(error.code, error.message.clone()))?;
    let header = artifact_rdf::catalog(&writer.db, id, &bytes, admission.remaining)?;
    if header.record_count == 0 {
        budget
            .request(row_bound("empty-time:"), false)
            .map_err(|error| Error::new(error.code, error.message.clone()))?;
        if writer
            .db
            .get_pinned(crate::storage::recordless::key_parts(
                header.artifact.received_at,
                id,
            ))?
            .as_deref()
            != Some(id.as_bytes())
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "empty artifact receipt index is missing or inconsistent",
            ));
        }
    }
    if key != artifact_rdf::time_key(header.artifact.received_at, id).as_bytes()
        || header.artifact.evicted
    {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "artifact receipt index disagrees with its catalog",
        ));
    }
    Ok(header)
}

/// Coverage of a completed graph-owner capture, independently of query certainty.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RdfScopeCoverage {
    /// Chronological candidates visited, before liveness/source/diagnostic filters.
    pub visited: u64,
    /// Known selected record occurrences, never a count of shared content graphs.
    /// The exact total is unknown when diagnostic membership is incomplete.
    pub selected_records: u64,
    /// Artifact candidates visited independently of record occurrences.
    pub artifacts_visited: u64,
    /// Proven complete-artifact membership; no content projection is implied.
    pub selected_artifacts: u64,
    /// Otherwise eligible empty artifacts lacking requested diagnostic dimensions.
    pub artifact_diagnostic_unknown: u64,
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
        self.diagnostics.complete(false)
            && self.content_failed == 0
            && self.artifact_diagnostic_unknown == 0
    }
}

impl StoredRdfView<'_> {
    fn include_owner(&mut self, id: &str) -> Result<(), ReadError> {
        if id.starts_with("record:") || id.starts_with("artifact:") {
            let marker = self
                .snapshot
                .get_pinned(format!("provenance-rdf:{id}"))
                .map_err(|e| self.budget.fail(e.into()))?;
            if marker.as_deref() != Some([].as_slice()) {
                return Err(self.budget.fail(Error::new(
                    "STORAGE_FAILURE",
                    "occurrence provenance RDF marker missing or invalid",
                )));
            }
        }
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

    #[allow(clippy::too_many_arguments)]
    fn include_artifact(
        &mut self,
        writer: &crate::storage::Writer,
        header: &crate::storage::manifest_retire::Header,
        matched_artifacts: &BTreeSet<String>,
        scope: &QueryScope,
        layers: RdfGraphLayers,
        now: u64,
        protected: &BTreeSet<String>,
        coverage: &mut RdfScopeCoverage,
    ) -> Result<(), ReadError> {
        let artifact = &header.artifact;
        coverage.artifacts_visited += 1;
        if !writer.live_artifact(artifact, now, protected)
            || !scope.matches_source(&artifact.source)
        {
            return Ok(());
        }
        if !scope.diagnostics.is_empty() && header.record_count != 0 {
            if !matched_artifacts.contains(&artifact.id) {
                return Ok(());
            }
        } else if !scope.diagnostics.is_empty() {
            coverage.artifact_diagnostic_unknown += 1;
            if scope.require_complete_index {
                return Err(self.budget.fail(Error::new("INDEX_NOT_READY","selected recordless artifact has no diagnostic record; omit diagnostic filters or select records")));
            }
            return Ok(());
        }
        coverage.selected_artifacts += 1;
        if layers.metadata {
            let marker = format!("artifact-rdf:{}", artifact.id);
            let value = self.point(marker.as_bytes(), 8)?.ok_or_else(|| {
                self.budget.fail(Error::new(
                    "STORAGE_FAILURE",
                    "artifact RDF ownership marker missing",
                ))
            })?;
            if value.as_ref() != artifact.received_at.to_be_bytes() {
                return Err(self.budget.fail(Error::new(
                    "STORAGE_FAILURE",
                    "artifact RDF receipt marker disagrees with its catalog",
                )));
            }
            drop(value);
            let graph = format!("rdf:graph:{}", artifact.id);
            let manifest = self.point(graph.as_bytes(), 65536)?.ok_or_else(|| {
                self.budget
                    .fail(Error::new("STORAGE_FAILURE", "artifact RDF graph missing"))
            })?;
            drop(manifest);
            self.include_owner(&artifact.id)?;
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
        let selection = writer
            .query_selection_membership(&scope.selection, now, &protected, Some(&mut admission))
            .map_err(|e| budget.fail(e))?;
        let (members, artifacts) = match selection {
            Some(members) => (Some(members.records), Some(members.artifact_times)),
            None => (None, None),
        };
        let mut candidates = writer
            .query_candidates_from_members(scope, members, |id| {
                writer.get_admitted(id, |bytes| {
                    catalog_entry(&budget, id, bytes, &mut admission)
                })
            })
            .map_err(|e| budget.fail(e))?;
        let mut coverage = RdfScopeCoverage::default();
        let mut matched_artifacts = BTreeSet::new();
        loop {
            budget.request(row_bound("time:"), true)?;
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
            if !scope.diagnostics.is_empty() {
                matched_artifacts.insert(record.artifact_id.clone());
            }
            view.include_record(&record, layers, scope.require_complete_index, &mut coverage)?;
        }
        drop(candidates);
        // Whole-artifact membership is independent of its record expansion.
        if let Some(members) = artifacts {
            for (id, time) in members {
                if time < scope.received_from || time >= scope.received_before {
                    continue;
                }
                let key = crate::storage::artifact_rdf::time_key(time, &id);
                budget.request(row_bound(crate::storage::artifact_rdf::TIME), true)?;
                let value = writer
                    .db
                    .get_pinned(&key)
                    .map_err(|e| budget.fail(e.into()))?
                    .ok_or_else(|| {
                        budget.fail(Error::new(
                            "STORAGE_FAILURE",
                            "selected artifact temporal index missing",
                        ))
                    })?;
                let artifact =
                    artifact_catalog(&writer, &budget, &mut admission, key.as_bytes(), &value)
                        .map_err(|e| budget.fail(e))?;
                view.include_artifact(
                    &writer,
                    &artifact,
                    &matched_artifacts,
                    scope,
                    layers,
                    now,
                    &protected,
                    &mut coverage,
                )?;
            }
        } else {
            let mut rows = TemporalRows::new(
                crate::storage::artifact_rdf::TIME,
                scope.received_from,
                scope.received_before,
                |options| writer.db.raw_iterator_opt(options),
            );
            loop {
                budget.request(row_bound(crate::storage::artifact_rdf::TIME), true)?;
                let Some(artifact) = rows.read_next(|key, value| {
                    artifact_catalog(&writer, &budget, &mut admission, key, value)
                }) else {
                    break;
                };
                let artifact = artifact.map_err(|e| budget.fail(e))?;
                view.include_artifact(
                    &writer,
                    &artifact,
                    &matched_artifacts,
                    scope,
                    layers,
                    now,
                    &protected,
                    &mut coverage,
                )?;
            }
        }
        coverage.catalog_entries = scanned;
        drop(matched_artifacts);
        drop(protected);
        drop(_catalog);
        drop(writer);
        purrdf::dataset_view::checkpointed_drain(&view, |view| operation(view, &coverage))
            .map_err(|failure| failure.error)
    }
}
