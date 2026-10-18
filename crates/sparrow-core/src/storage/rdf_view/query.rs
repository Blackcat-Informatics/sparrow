// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Native governed read forms over Sparrow's captured graph membership.

use super::*;
use crate::{QueryCancellation, QueryScope};
use purrdf::sparql::{
    FallibleScopedResult, FallibleSparqlResult, GovernedEvidence, NativeSparqlEngine,
    QueryGovernors, QueryOptions, RetainedQueryExplanation,
};

/// Borrowed query inputs; native PurRDF owns parsing, substitutions and governors.
pub struct SparqlRead<'a> {
    /// Retained occurrence membership and temporal/diagnostic filters.
    pub scope: &'a QueryScope,
    /// Physical graph layers visible to this execution.
    pub layers: RdfGraphLayers,
    /// Native read request, including typed variable substitutions.
    pub request: purrdf::SparqlRequest<'a>,
    /// Native execution ceilings, in addition to host source admission.
    pub governors: &'a QueryGovernors,
    /// Source reads, workspace and lease ceilings.
    pub limits: RdfReadLimits,
}

/// Native payload ownership beside independent host coverage evidence.
/// The default query payload retains certified answer/partial outcomes. The
/// explanation specialization describes measured work, not query answers.
/// Operational refusal retains its original cause without a successful prefix.
pub struct ScopedSparqlResult<
    Outcome = FallibleSparqlResult<ReadError, GovernedEvidence<RdfReadEvidence>>,
> {
    /// Persistent store identity.
    pub store_id: String,
    /// Frozen logical scope and resolved pin revision, independent of native certainty.
    pub scope: crate::storage::Value,
    /// Fixed native correlation recipe and parameters, absent for raw SPARQL.
    pub correlation: Option<crate::CorrelationCapture>,
    _scope_admission: OwnedWorkspaceReservation<ReadError>,
    /// Native storage sequence of the captured membership and graph indexes.
    pub sequence: u64,
    /// Captured eligibility clock in UTC Unix milliseconds.
    pub as_of: u64,
    /// Requested physical graph layers, retained for continuation receipts.
    pub layers: RdfGraphLayers,
    /// Effective host source ceilings, retained without renewing their lease.
    pub limits: RdfReadLimits,
    /// Independent scope and applicable projection coverage.
    pub coverage: RdfScopeCoverage,
    /// Native retained payload and evidence, or its typed diagnostic/failure.
    /// Query payloads carry answer certificates; explanations carry measured work.
    /// Retained carriers keep their original admitted owner after capture ends.
    pub outcome: Outcome,
}

/// A measured native ledger under the same source/scope ownership as queries.
/// A stopped explanation describes measured work; it does not certify answers.
pub type ScopedSparqlExplanation = ScopedSparqlResult<
    FallibleScopedResult<RetainedQueryExplanation, ReadError, GovernedEvidence<RdfReadEvidence>>,
>;

impl<Outcome: std::fmt::Debug> std::fmt::Debug for ScopedSparqlResult<Outcome> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopedSparqlResult")
            .field("store_id", &self.store_id)
            .field("scope", &self.scope)
            .field("correlation", &self.correlation)
            .field("sequence", &self.sequence)
            .field("as_of", &self.as_of)
            .field("layers", &self.layers)
            .field("limits", &self.limits)
            .field("coverage", &self.coverage)
            .field("outcome", &self.outcome)
            .finish_non_exhaustive()
    }
}

impl Store {
    /// Execute SELECT, ASK, CONSTRUCT or DESCRIBE through native PurRDF over one
    /// captured scope. UPDATE is refused by the native read-query preparation.
    /// Federation is disabled: graph queries never perform implicit network reads.
    pub fn sparql_query(
        &self,
        read: &SparqlRead<'_>,
        now: u64,
        cancellation: &QueryCancellation,
    ) -> Result<ScopedSparqlResult, ReadError> {
        self.with_sparql_result((read, None), now, cancellation, |_, result| result)
    }

    /// Measure the original native request, substitutions and finite governors
    /// over the same captured view used by ordinary queries. The returned ledger
    /// retains native admission after the view and snapshot have been released.
    pub fn sparql_explain(
        &self,
        read: &SparqlRead<'_>,
        now: u64,
        cancellation: &QueryCancellation,
    ) -> Result<ScopedSparqlExplanation, ReadError> {
        self.with_sparql_explanation(read, now, cancellation, |_, result| result)
    }

    #[expect(
        clippy::result_large_err,
        reason = "native failures retain original receipts; boxing a refusal would allocate after admission failure"
    )]
    pub(super) fn with_sparql_explanation<T>(
        &self,
        read: &SparqlRead<'_>,
        now: u64,
        cancellation: &QueryCancellation,
        publish: impl FnOnce(&StoredRdfView<'_>, ScopedSparqlExplanation) -> T,
    ) -> Result<T, ReadError> {
        self.with_native_result(
            (read, None),
            now,
            cancellation,
            |engine, view, governors| {
                engine.explain_governed_fallible_view(
                    view,
                    read.request,
                    QueryOptions::default(),
                    governors,
                )
            },
            publish,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "native failures retain original receipts; boxing a refusal would allocate after admission failure"
    )]
    pub(super) fn with_sparql_result<T>(
        &self,
        query: (&SparqlRead<'_>, Option<&crate::CorrelationCapture>),
        now: u64,
        cancellation: &QueryCancellation,
        publish: impl FnOnce(&StoredRdfView<'_>, ScopedSparqlResult) -> T,
    ) -> Result<T, ReadError> {
        self.with_native_result(
            query,
            now,
            cancellation,
            |engine, view, governors| {
                engine.query_governed_fallible_view(
                    view,
                    query.0.request,
                    QueryOptions::default(),
                    governors,
                )
            },
            publish,
        )
    }

    fn with_native_result<Outcome, T>(
        &self,
        query: (&SparqlRead<'_>, Option<&crate::CorrelationCapture>),
        now: u64,
        cancellation: &QueryCancellation,
        execute: impl FnOnce(&NativeSparqlEngine, &StoredRdfView<'_>, &QueryGovernors) -> Outcome,
        publish: impl FnOnce(&StoredRdfView<'_>, ScopedSparqlResult<Outcome>) -> T,
    ) -> Result<T, ReadError> {
        let (read, correlation) = query;
        if read.request.query.len() > crate::query::RECORD_QUERY_TEXT_BYTES {
            return Err(Arc::new(Error::new(
                "RESOURCE_LIMIT",
                "SPARQL query exceeds 64 KiB input",
            )));
        }
        let store_id = self.store_id().map_err(Arc::new)?;
        self.capture_rdf_scope(
            (read.scope, correlation),
            read.layers,
            now,
            read.limits,
            cancellation,
            |view, coverage| {
                if read.scope.require_complete_index && correlation.is_some_and(|capture| !capture.complete(coverage)) {
                    return Err(view.budget.fail(Error::new("INDEX_NOT_READY", "correlation requires known diagnostic/event-time interpretations throughout its selected scope")));
                }
                let scope_admission = view
                    .budget
                    .reserve_owned(crate::QUERY_SCOPE_WORKSPACE_BYTES as u64, &view._scope)?;
                let scope = view
                    .scope_receipt
                    .as_ref()
                    .ok_or_else(|| {
                        view.budget.fail(Error::new(
                            "STORAGE_FAILURE",
                            "native query scope was not captured",
                        ))
                    })?
                    .clone();
                let engine = NativeSparqlEngine::new();
                let governors = read.governors.clone().with_max_remote_requests(0);
                let outcome = execute(&engine, view, &governors);
                drop(engine);
                let result = ScopedSparqlResult {
                    store_id,
                    scope,
                    correlation: correlation.cloned(),
                    _scope_admission: scope_admission,
                    sequence: view.snapshot_sequence_number(),
                    as_of: now,
                    layers: read.layers,
                    limits: read.limits,
                    coverage: coverage.clone(),
                    outcome,
                };
                Ok(publish(view, result))
            },
        )?
    }
}
