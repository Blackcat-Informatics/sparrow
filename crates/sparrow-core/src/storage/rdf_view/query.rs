// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Native governed read forms over Sparrow's captured graph membership.

use super::*;
use crate::{QueryCancellation, QueryScope};
use purrdf::sparql::{
    FallibleSparqlResult, GovernedEvidence, NativeSparqlEngine, QueryGovernors, QueryOptions,
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

/// Native certainty and ownership, beside independent host coverage evidence.
/// A governor stop remains a typed certified partial outcome. An operational
/// refusal never becomes a successful prefix; the native error retains its cause.
pub struct ScopedSparqlResult {
    /// Persistent store identity.
    pub store_id: String,
    /// Native storage sequence of the captured membership and graph indexes.
    pub sequence: u64,
    /// Captured eligibility clock in UTC Unix milliseconds.
    pub as_of: u64,
    /// Independent scope and applicable projection coverage.
    pub coverage: RdfScopeCoverage,
    /// Complete native result or typed diagnostic/partial/failure outcome.
    /// Retained carriers keep their original admitted owner after capture ends.
    pub outcome: FallibleSparqlResult<ReadError, GovernedEvidence<RdfReadEvidence>>,
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
        if read.request.query.len() > crate::query::RECORD_QUERY_TEXT_BYTES {
            return Err(Arc::new(Error::new(
                "RESOURCE_LIMIT",
                "SPARQL query exceeds 64 KiB input",
            )));
        }
        let store_id = self.store_id().map_err(Arc::new)?;
        self.with_rdf_scope(
            read.scope,
            read.layers,
            now,
            read.limits,
            cancellation,
            |view, coverage| {
                let engine = NativeSparqlEngine::new();
                let governors = read.governors.clone().with_max_remote_requests(0);
                let outcome = engine.query_governed_fallible_view(
                    view,
                    read.request,
                    QueryOptions::default(),
                    &governors,
                );
                ScopedSparqlResult {
                    store_id,
                    sequence: view.snapshot_sequence_number(),
                    as_of: now,
                    coverage: coverage.clone(),
                    outcome,
                }
            },
        )
    }
}
