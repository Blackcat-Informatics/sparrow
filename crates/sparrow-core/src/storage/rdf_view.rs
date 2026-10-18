// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Native RDF reads over physical RocksDB indexes, without resident dataset replay.

use super::{Catalog, Error, Record, Store, rdf_index};
use purrdf::{
    BlankScope, DatasetView, FallibleDatasetView, GraphMatch, QuadIds, RdfStoreCapabilities,
    TermRef, TermValue, ViewOperationStatus, projections::ProjectionTerm,
};
use purrdf::{
    dataset_view::{OwnedWorkspaceReservation, TermGuard, WorkspaceReservation},
    ir::GlobalTermId,
};
use rocksdb::{DB, DBIteratorWithThreadMode, Direction, IteratorMode, Snapshot};
use std::{collections::BTreeSet, sync::Arc};

mod budget;
mod explanation;
pub(crate) mod pages;
mod query;
mod scope;
mod sparse;
use budget::{Budget, Reservation};
pub use explanation::SparqlExplanationItem;
pub use pages::{
    NativePageBody, SparqlExplanationPageBody, SparqlExplanationReadPage, SparqlGraphKind,
    SparqlItem, SparqlPageBody, SparqlReadPage, sparql_term_node_bytes, sparql_term_wire_bytes,
};
pub use query::{ScopedSparqlExplanation, ScopedSparqlResult, SparqlRead};
pub use scope::RdfScopeCoverage;
use sparse::RowCursor;

/// Host source law: bounded borrowed index entries and admitted encoded catalog bodies.
pub const RDF_SOURCE_ADMISSION_PROFILE: &str = "sparrow-rdf-source-v4";

/// Sticky operational root cause shared by native readers and their guards.
pub type ReadError = Arc<Error>;

/// Independent occurrence metadata, normalized diagnostic and shared content layers.
/// Selection changes visible graph owners, never RDF semantics or evidence lifetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RdfGraphLayers {
    /// Record/source/time/byte provenance and projection links.
    pub metadata: bool,
    /// Interpreted diagnostic values, unknown reasons and original-byte witnesses.
    pub diagnostic: bool,
    /// Native JSON/Markdown content projections, shared by equal payloads.
    pub content: bool,
}

impl RdfGraphLayers {
    /// Existing stored-read behavior: all three layers, with strict content readiness.
    pub const ALL: Self = Self {
        metadata: true,
        diagnostic: true,
        content: true,
    };
}

/// Inclusive host ceilings for one stored RDF operation; zero is a hard limit.
#[derive(Clone, Copy, Debug)]
pub struct RdfReadLimits {
    /// Live admitted host allocations, including guards and native operator scratch.
    pub live_bytes: u64,
    /// Cumulative point/iterator read admissions.
    pub requests: u64,
    /// Cumulative admitted maximum value/key bytes, before engine reads.
    pub io_bytes: u64,
    /// Maximum index-row admissions, including outside-owner and boundary probes.
    pub row_visits: u64,
    /// Read lease duration, 1..60000 milliseconds.
    pub lease_ms: u64,
}

impl Default for RdfReadLimits {
    fn default() -> Self {
        Self {
            live_bytes: 128 * 1024 * 1024,
            requests: 100000,
            io_bytes: 128 * 1024 * 1024,
            row_visits: 100000,
            lease_ms: 30000,
        }
    }
}

/// Deterministic resource evidence sampled with the sticky source error.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RdfReadEvidence {
    /// Admitted point/iterator reads.
    pub requests: u64,
    /// Sum of read admission bounds, not measured native engine I/O.
    pub admitted_bytes: u64,
    /// Index-row admissions, including end/boundary probes.
    pub row_visits: u64,
    /// Currently retained host capacity.
    pub live_bytes: u64,
    /// Maximum simultaneously retained host capacity.
    pub peak_live_bytes: u64,
}

/// One native read view, scoped to concrete retained graph owners.
/// Scope is captured under the serialized owner; execution uses a native snapshot
/// after releasing that owner, for this bounded callback lease.
pub struct StoredRdfView<'a> {
    snapshot: Snapshot<'a>,
    owners: BTreeSet<u64>,
    budget: Arc<Budget>,
    _scope: Arc<Reservation>,
    terms: u64,
    max_encoded: u64,
    scope_receipt: Option<super::Value>,
}

/// Owned native term carrier whose strings live through this admitted guard.
pub struct StoredTermGuard<'a> {
    value: ProjectionTerm,
    links: [Option<GlobalTermId>; 3],
    _reservation: Reservation,
    _view: std::marker::PhantomData<&'a ()>,
}

/// Opaque physical probe plan minted by this stored view.
#[derive(Clone, Copy)]
pub struct StoredProbePlan(usize);

struct EncodedGuard<'a> {
    bytes: rocksdb::DBPinnableSlice<'a>,
    _reservation: Reservation,
}

impl TermGuard<GlobalTermId> for StoredTermGuard<'_> {
    fn term(&self) -> TermRef<'_, GlobalTermId> {
        match &self.value {
            ProjectionTerm::Iri { value } => TermRef::Iri(value),
            ProjectionTerm::Blank { label, scope } => TermRef::Blank {
                label,
                scope: BlankScope(*scope),
            },
            ProjectionTerm::Literal {
                lexical,
                language,
                direction,
                ..
            } => TermRef::Literal {
                lexical,
                datatype: self.links[0].expect("validated datatype link"),
                language: language.as_deref(),
                direction: *direction,
            },
            ProjectionTerm::Triple { .. } => TermRef::Triple {
                s: self.links[0].expect("validated subject link"),
                p: self.links[1].expect("validated predicate link"),
                o: self.links[2].expect("validated object link"),
            },
        }
    }
}

fn ordinal(id: GlobalTermId) -> u64 {
    id.index() + 1
}
fn global(ordinal: u64) -> Result<GlobalTermId, Error> {
    ordinal
        .checked_sub(1)
        .and_then(GlobalTermId::checked_from_index)
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "zero RDF dictionary identity"))
}

// Admission only: native ProjectionTerm still owns every RDF validity/identity law.
// Check the caller's borrowed allocation before its native conversion can clone it.
fn input_capacity(value: &TermValue, depth: usize, remaining: &mut u64) -> Result<(), Error> {
    if depth > 16 {
        return Err(Error::new(
            "RESOURCE_LIMIT",
            "RDF lookup input exceeds admitted depth",
        ));
    }
    let strings = match value {
        TermValue::Iri(iri) => iri.len(),
        TermValue::Blank { label, .. } => label.len(),
        TermValue::Literal {
            lexical_form,
            datatype,
            language,
            ..
        } => lexical_form
            .len()
            .saturating_add(datatype.len())
            .saturating_add(language.as_ref().map_or(0, String::len)),
        TermValue::Triple { .. } => 0,
    };
    let bytes = (strings as u64).saturating_add(size_of::<TermValue>() as u64);
    *remaining = remaining.checked_sub(bytes).ok_or_else(|| {
        Error::new(
            "RESOURCE_LIMIT",
            "RDF lookup input exceeds 64 KiB allocation admission",
        )
    })?;
    if let TermValue::Triple { s, p, o } = value {
        for child in [&**s, &**p, &**o] {
            input_capacity(child, depth + 1, remaining)?;
        }
    }
    Ok(())
}

impl<'a> StoredRdfView<'a> {
    pub(super) fn new(
        db: &'a DB,
        limits: RdfReadLimits,
        graph_ids: &[String],
    ) -> Result<Self, ReadError> {
        Self::new_cancellable(db, limits, graph_ids, crate::QueryCancellation::default())
    }

    fn new_cancellable(
        db: &'a DB,
        limits: RdfReadLimits,
        graph_ids: &[String],
        cancellation: crate::QueryCancellation,
    ) -> Result<Self, ReadError> {
        let budget = Budget::new_cancellable(limits, cancellation)?;
        let scope = Arc::new(budget.reserve(4096 + (graph_ids.len() as u64).saturating_mul(256))?);
        let mut view = Self {
            snapshot: db.snapshot(),
            owners: BTreeSet::new(),
            budget,
            _scope: scope,
            terms: 0,
            max_encoded: 0,
            scope_receipt: None,
        };
        for key in ["rdf:term-count", "rdf:max-encoded"] {
            let bytes = view.point(key.as_bytes(), 8)?.ok_or_else(|| {
                view.budget.fail(Error::new(
                    "STORAGE_FAILURE",
                    "RDF read control counter missing",
                ))
            })?;
            let count = bytes
                .as_ref()
                .try_into()
                .map(u64::from_be_bytes)
                .map_err(|_| {
                    view.budget
                        .fail(Error::new("STORAGE_FAILURE", "invalid RDF read counter"))
                })?;
            drop(bytes);
            if key == "rdf:term-count" {
                view.terms = count;
            } else {
                view.max_encoded = count;
            }
        }
        if view.max_encoded > 65536 || (view.terms > 0 && view.max_encoded == 0) {
            return Err(view.budget.fail(Error::new(
                "STORAGE_FAILURE",
                "invalid RDF term allocation bound",
            )));
        }
        for id in graph_ids {
            let term = TermValue::iri(rdf_index::graph_iri(id));
            let owner = view.term_id_by_value(&term)?.ok_or_else(|| {
                view.budget
                    .fail(Error::new("STORAGE_FAILURE", "RDF graph owner missing"))
            })?;
            view.owners.insert(ordinal(owner));
        }
        Ok(view)
    }

    /// Native sequence at which evidence membership and indexed values were captured.
    pub fn snapshot_sequence_number(&self) -> u64 {
        self.snapshot.sequence_number()
    }

    fn point(
        &self,
        key: &[u8],
        maximum: u64,
    ) -> Result<Option<rocksdb::DBPinnableSlice<'_>>, ReadError> {
        self.budget.request(key.len() as u64 + maximum, false)?;
        let bytes = self
            .snapshot
            .get_pinned(key)
            .map_err(|error| self.budget.fail(error.into()))?;
        if bytes
            .as_ref()
            .is_some_and(|bytes| bytes.len() as u64 > maximum)
        {
            return Err(self.budget.fail(Error::new(
                "STORAGE_FAILURE",
                "RDF value exceeds persisted admission bound",
            )));
        }
        Ok(bytes)
    }

    fn lookup(&self, value: &ProjectionTerm) -> Result<Option<GlobalTermId>, ReadError> {
        let limits = rdf_index::term_limits().map_err(|e| self.budget.fail(e))?;
        let bytes = value.to_canonical_json(limits).map_err(|e| {
            self.budget
                .fail(Error::new("INVALID_ARGUMENT", e.to_string()))
        })?;
        let key = rdf_index::reverse_key(&bytes);
        let Some(stored) = self.point(key.as_bytes(), 8)? else {
            return Ok(None);
        };
        let id = stored
            .as_ref()
            .try_into()
            .map(u64::from_be_bytes)
            .map_err(|_| {
                self.budget.fail(Error::new(
                    "STORAGE_FAILURE",
                    "invalid RDF reverse identity",
                ))
            })?;
        let global = global(id).map_err(|e| self.budget.fail(e))?;
        let forward = self.verified_forward(global)?;
        if forward.bytes.as_ref() != bytes.as_slice() {
            return Err(self.budget.fail(Error::new(
                "STORAGE_FAILURE",
                "RDF forward/reverse identity mismatch",
            )));
        }
        Ok(Some(global))
    }

    fn verified_forward(&self, id: GlobalTermId) -> Result<EncodedGuard<'_>, ReadError> {
        let reservation = self.budget.reserve(self.max_encoded + 1024)?;
        let key = rdf_index::term_key(ordinal(id));
        let bytes = self
            .point(key.as_bytes(), self.max_encoded)?
            .ok_or_else(|| {
                self.budget
                    .fail(Error::new("STORAGE_FAILURE", "RDF term missing or foreign"))
            })?;
        let reverse = rdf_index::reverse_key(&bytes);
        let stored = self.point(reverse.as_bytes(), 8)?;
        if stored.as_deref() != Some(ordinal(id).to_be_bytes().as_slice()) {
            return Err(self.budget.fail(Error::new(
                "STORAGE_FAILURE",
                "RDF reverse/forward identity mismatch",
            )));
        }
        Ok(EncodedGuard {
            bytes,
            _reservation: reservation,
        })
    }

    fn rows(
        &self,
        stream: u8,
        plan: usize,
        pattern: [Option<GlobalTermId>; 3],
        graph: GraphMatch<GlobalTermId>,
    ) -> impl Iterator<Item = QuadIds<GlobalTermId>> + '_ {
        let cursor = |plan, graph, union_default| {
            if pattern.iter().all(Option::is_none) && !matches!(graph, GraphMatch::Named(_)) {
                RowCursor::scoped(self, stream, union_default)
            } else {
                RowCursor::Indexed(self.physical_rows(stream, plan, pattern, graph, union_default))
            }
        };
        let first = cursor(plan, graph, matches!(graph, GraphMatch::Default));
        let union = matches!(graph, GraphMatch::Any).then(|| {
            let plan = self.probe_plan(
                pattern[0].is_some(),
                pattern[1].is_some(),
                pattern[2].is_some(),
                GraphMatch::Default,
            );
            cursor(plan.0, GraphMatch::Default, true)
        });
        first.chain(union.into_iter().flatten())
    }

    fn physical_rows(
        &self,
        stream: u8,
        plan: usize,
        pattern: [Option<GlobalTermId>; 3],
        graph: GraphMatch<GlobalTermId>,
        union_default: bool,
    ) -> QuadIter<'_> {
        let (name, order) = rdf_index::ORDERS[plan];
        let mut prefix = format!("rdf:q:{stream}:{name}:").into_bytes();
        let graph_id = match graph {
            GraphMatch::Named(id) => Some(id),
            _ => None,
        };
        let fields = [pattern[0], pattern[1], pattern[2], graph_id];
        for axis in order {
            let Some(id) = fields[axis] else {
                break;
            };
            prefix.extend_from_slice(&ordinal(id).to_be_bytes());
        }
        let reservation = self.budget.reserve(4096);
        let iterator = reservation.as_ref().ok().map(|_| {
            self.snapshot
                .iterator(IteratorMode::From(&prefix, Direction::Forward))
        });
        QuadIter {
            view: self,
            iterator,
            prefix,
            order,
            base_len: format!("rdf:q:{stream}:{name}:").len(),
            pattern,
            graph,
            union_default,
            previous: None,
            _reservation: reservation.ok(),
        }
    }

    fn graph_rows(&self) -> GraphIter<'_> {
        let reservation = self.budget.reserve(4096);
        let iterator = reservation.as_ref().ok().map(|_| {
            self.snapshot
                .iterator(IteratorMode::From(b"rdf:g:", Direction::Forward))
        });
        GraphIter {
            view: self,
            iterator,
            previous: None,
            _reservation: reservation.ok(),
        }
    }
}

struct QuadIter<'a> {
    view: &'a StoredRdfView<'a>,
    iterator: Option<DBIteratorWithThreadMode<'a, DB>>,
    prefix: Vec<u8>,
    order: [usize; 4],
    base_len: usize,
    pattern: [Option<GlobalTermId>; 3],
    graph: GraphMatch<GlobalTermId>,
    previous: Option<QuadIds<GlobalTermId>>,
    union_default: bool,
    _reservation: Option<Reservation>,
}

impl Iterator for QuadIter<'_> {
    type Item = QuadIds<GlobalTermId>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.view.budget.request(128, true).ok()?;
            let item = self.iterator.as_mut()?.next()?;
            let (key, value) = match item {
                Ok(row) => row,
                Err(e) => {
                    self.view.budget.fail(e.into());
                    return None;
                }
            };
            if !key.starts_with(&self.prefix) {
                self.iterator = None;
                return None;
            }
            let suffix = &key[self.base_len..];
            if suffix.len() != 40 || !value.is_empty() {
                self.view.budget.fail(Error::new(
                    "STORAGE_FAILURE",
                    "invalid RDF index row width/value",
                ));
                return None;
            }
            let numbers: [u64; 5] = std::array::from_fn(|index| {
                u64::from_be_bytes(
                    suffix[index * 8..index * 8 + 8]
                        .try_into()
                        .expect("checked index width"),
                )
            });
            if numbers.contains(&0) {
                self.view
                    .budget
                    .fail(Error::new("STORAGE_FAILURE", "zero RDF index identity"));
                return None;
            }
            if !self.view.owners.contains(&numbers[4]) {
                continue;
            }
            let mut fields = [numbers[0]; 4];
            for (position, axis) in self.order.into_iter().enumerate() {
                fields[axis] = numbers[position];
            }
            let ids = fields.map(|id| global(id).expect("checked nonzero index identity"));
            for id in ids {
                self.view.verified_forward(id).ok()?;
            }
            let row = QuadIds {
                s: ids[0],
                p: ids[1],
                o: ids[2],
                g: (!self.union_default).then_some(ids[3]),
            };
            if self.pattern[0].is_some_and(|id| id != row.s)
                || self.pattern[1].is_some_and(|id| id != row.p)
                || self.pattern[2].is_some_and(|id| id != row.o)
                || (!self.union_default && !self.graph.matches(row.g))
            {
                continue;
            }
            if self.previous == Some(row) {
                continue;
            }
            self.previous = Some(row);
            return Some(row);
        }
    }
}

struct GraphIter<'a> {
    view: &'a StoredRdfView<'a>,
    iterator: Option<DBIteratorWithThreadMode<'a, DB>>,
    previous: Option<GlobalTermId>,
    _reservation: Option<Reservation>,
}

impl Iterator for GraphIter<'_> {
    type Item = GlobalTermId;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.view.budget.request(64, true).ok()?;
            let (key, value) = match self.iterator.as_mut()?.next()? {
                Ok(row) => row,
                Err(e) => {
                    self.view.budget.fail(e.into());
                    return None;
                }
            };
            if !key.starts_with(b"rdf:g:") {
                self.iterator = None;
                return None;
            }
            let suffix = &key[6..];
            if suffix.len() != 16 || !value.is_empty() {
                self.view.budget.fail(Error::new(
                    "STORAGE_FAILURE",
                    "invalid RDF graph declaration",
                ));
                return None;
            }
            let [graph, owner] = suffix.as_chunks::<8>().0 else {
                unreachable!("checked graph row width")
            };
            let graph = match global(u64::from_be_bytes(*graph)) {
                Ok(id) => id,
                Err(e) => {
                    self.view.budget.fail(e);
                    return None;
                }
            };
            let owner = u64::from_be_bytes(*owner);
            if owner == 0 {
                self.view
                    .budget
                    .fail(Error::new("STORAGE_FAILURE", "zero RDF graph owner"));
                return None;
            }
            if !self.view.owners.contains(&owner) || self.previous == Some(graph) {
                continue;
            }
            self.view.verified_forward(graph).ok()?;
            self.previous = Some(graph);
            return Some(graph);
        }
    }
}

impl DatasetView for StoredRdfView<'_> {
    type Id = GlobalTermId;
    type ReadError = ReadError;
    type TermGuard<'a>
        = StoredTermGuard<'a>
    where
        Self: 'a;
    type ProbePlan = StoredProbePlan;

    fn read_error(&self) -> Option<ReadError> {
        self.budget.checkpoint().0
    }
    fn reserve_workspace(
        &self,
        bytes: u64,
    ) -> Result<impl WorkspaceReservation<Error = ReadError> + '_, ReadError> {
        self.budget.reserve(bytes)
    }
    fn reserve_owned_workspace(
        &self,
        bytes: u64,
    ) -> Option<Result<OwnedWorkspaceReservation<ReadError>, ReadError>> {
        Some(self.budget.reserve_owned(bytes, &self._scope))
    }
    fn max_owned_term_bytes(&self) -> Option<u64> {
        Some(self.max_encoded.saturating_mul(256).saturating_add(4096))
    }
    fn storage_live_budget(&self) -> Option<u64> {
        Some(self.budget.ceiling())
    }
    fn quads(&self) -> impl Iterator<Item = QuadIds<GlobalTermId>> + '_ {
        self.rows(0, 0, [None; 3], GraphMatch::Any)
    }
    fn resolve(&self, id: GlobalTermId) -> Result<StoredTermGuard<'_>, ReadError> {
        let reservation = self
            .budget
            .reserve(self.max_owned_term_bytes().expect("persisted bound"))?;
        let bytes = self.verified_forward(id)?;
        let limits = rdf_index::term_limits().map_err(|e| self.budget.fail(e))?;
        let value = ProjectionTerm::from_canonical_json(&bytes.bytes, limits).map_err(|e| {
            self.budget
                .fail(Error::new("STORAGE_FAILURE", e.to_string()))
        })?;
        let mut links = [None; 3];
        match &value {
            ProjectionTerm::Literal { datatype, .. } => {
                links[0] = Some(
                    self.lookup(&ProjectionTerm::Iri {
                        value: datatype.clone(),
                    })?
                    .ok_or_else(|| {
                        self.budget.fail(Error::new(
                            "STORAGE_FAILURE",
                            "RDF datatype identity missing",
                        ))
                    })?,
                )
            }
            ProjectionTerm::Triple {
                subject,
                predicate,
                object,
            } => {
                for (index, child) in [subject, predicate, object].into_iter().enumerate() {
                    links[index] = Some(self.lookup(child)?.ok_or_else(|| {
                        self.budget.fail(Error::new(
                            "STORAGE_FAILURE",
                            "RDF triple component missing",
                        ))
                    })?);
                }
            }
            ProjectionTerm::Iri { .. } | ProjectionTerm::Blank { .. } => {}
        }
        Ok(StoredTermGuard {
            value,
            links,
            _reservation: reservation,
            _view: std::marker::PhantomData,
        })
    }
    fn term_id_by_value(&self, value: &TermValue) -> Result<Option<GlobalTermId>, ReadError> {
        let mut remaining = 65536;
        input_capacity(value, 0, &mut remaining).map_err(|e| self.budget.fail(e))?;
        let _reservation = self.budget.reserve((65536 - remaining) * 256 + 4096)?;
        let limits = rdf_index::term_limits().map_err(|e| self.budget.fail(e))?;
        let term = ProjectionTerm::from_term_value(value, limits).map_err(|e| {
            self.budget
                .fail(Error::new("INVALID_ARGUMENT", e.to_string()))
        })?;
        self.lookup(&term)
    }
    fn capabilities(&self) -> RdfStoreCapabilities {
        RdfStoreCapabilities {
            named_graphs: true,
            quoted_triples: true,
            reifiers: true,
            annotations: true,
            ..RdfStoreCapabilities::plain_rdf()
        }
    }
    fn triple_term_nesting_bound(&self) -> Option<usize> {
        Some(16)
    }
    fn probe_plan(
        &self,
        s: bool,
        p: bool,
        o: bool,
        g: GraphMatch<GlobalTermId>,
    ) -> StoredProbePlan {
        let bound = [s, p, o, matches!(g, GraphMatch::Named(_))];
        StoredProbePlan(
            (0..if matches!(g, GraphMatch::Default) {
                3
            } else {
                6
            })
                .max_by_key(|index| {
                    (
                        rdf_index::ORDERS[*index]
                            .1
                            .iter()
                            .take_while(|axis| bound[**axis])
                            .count(),
                        std::cmp::Reverse(*index),
                    )
                })
                .expect("six physical indexes"),
        )
    }
    fn quads_for_pattern(
        &self,
        s: Option<GlobalTermId>,
        p: Option<GlobalTermId>,
        o: Option<GlobalTermId>,
        g: GraphMatch<GlobalTermId>,
    ) -> impl Iterator<Item = QuadIds<GlobalTermId>> + '_ {
        self.rows(
            0,
            self.probe_plan(s.is_some(), p.is_some(), o.is_some(), g).0,
            [s, p, o],
            g,
        )
    }
    fn quads_for_pattern_with_plan(
        &self,
        plan: &StoredProbePlan,
        s: Option<GlobalTermId>,
        p: Option<GlobalTermId>,
        o: Option<GlobalTermId>,
        g: GraphMatch<GlobalTermId>,
    ) -> impl Iterator<Item = QuadIds<GlobalTermId>> + '_ {
        self.rows(0, plan.0, [s, p, o], g)
    }
    fn term_count(&self) -> u64 {
        self.terms
    }
    fn reifier_quads(&self) -> impl Iterator<Item = QuadIds<GlobalTermId>> + '_ {
        self.rows(1, 0, [None; 3], GraphMatch::Any)
    }
    fn annotation_quads(&self) -> impl Iterator<Item = QuadIds<GlobalTermId>> + '_ {
        self.rows(2, 0, [None; 3], GraphMatch::Any)
    }
    fn annotations_of_with_graph(
        &self,
        id: GlobalTermId,
    ) -> impl Iterator<Item = (GlobalTermId, GlobalTermId, Option<GlobalTermId>)> + '_ {
        self.rows(2, 0, [Some(id), None, None], GraphMatch::Any)
            .map(|q| (q.p, q.o, q.g))
    }
    fn named_graphs(&self) -> impl Iterator<Item = GlobalTermId> + '_ {
        self.graph_rows()
    }
}

impl FallibleDatasetView for StoredRdfView<'_> {
    type Error = ReadError;
    type Evidence = RdfReadEvidence;
    fn operation_status(&self) -> ViewOperationStatus<ReadError, RdfReadEvidence> {
        let (error, evidence) = self.budget.checkpoint();
        match error {
            Some(error) => ViewOperationStatus::Failed { error, evidence },
            None => ViewOperationStatus::Ready { evidence },
        }
    }
}

impl Store {
    /// Run a native operation over one concrete selection of retained records.
    /// Capture scope under the serialized owner, then release it before execution.
    /// Snapshot reads cannot observe subsequent ingestion, pin changes or retirement,
    /// and the fail-aware boundary cannot publish a failed read prefix.
    /// One RDF snapshot per store is admitted; concurrent requests refuse explicitly.
    pub fn with_rdf_records<T>(
        &self,
        ids: &[String],
        now: u64,
        limits: RdfReadLimits,
        operation: impl FnOnce(&StoredRdfView<'_>) -> T,
    ) -> Result<T, ReadError> {
        self.with_rdf_record_layers(ids, RdfGraphLayers::ALL, now, limits, operation)
    }

    /// Select physical graph layers through the same retained-record and snapshot owner.
    /// Content readiness is required only when content is selected. Empty layer selection
    /// exposes an empty dataset after validating every referenced record's eligibility.
    pub fn with_rdf_record_layers<T>(
        &self,
        ids: &[String],
        layers: RdfGraphLayers,
        now: u64,
        limits: RdfReadLimits,
        operation: impl FnOnce(&StoredRdfView<'_>) -> T,
    ) -> Result<T, ReadError> {
        if ids.is_empty() || ids.len() > 1000 {
            return Err(Arc::new(Error::new(
                "INVALID_ARGUMENT",
                "RDF selection requires 1..1000 record IDs",
            )));
        }
        let _lease = self.rdf_read_lease()?;
        let writer = self.lock().map_err(Arc::new)?;
        let db = Arc::clone(&writer.db);
        let mut view = StoredRdfView::new(&db, limits, &[])?;
        let mut scope_bytes = 4096 + ids.len() as u64 * 2048;
        Arc::get_mut(&mut view._scope)
            .expect("scope capture precedes owned query admission")
            .resize(scope_bytes)?;
        let catalog_bound = (writer.config.max_artifact_bytes / super::CHUNK + 1)
            .saturating_mul(128)
            .saturating_add(8192) as u64;
        let mut records = Vec::with_capacity(ids.len());
        let mut targets = BTreeSet::new();
        for id in ids {
            if !id.starts_with("record:") || id.len() > 128 {
                return Err(view.budget.fail(Error::new(
                    "INVALID_ARGUMENT",
                    "RDF selection requires bounded record IDs",
                )));
            }
            let _decode = view.budget.reserve(catalog_bound.saturating_mul(64))?;
            let bytes = view.point(id.as_bytes(), catalog_bound)?.ok_or_else(|| {
                view.budget
                    .fail(Error::new("NOT_FOUND", "RDF selected record missing"))
            })?;
            scope_bytes = scope_bytes.saturating_add(bytes.len() as u64 * 4);
            let record = Record::decode(&super::parse(&bytes).map_err(|e| view.budget.fail(e))?)
                .map_err(|e| view.budget.fail(e))?;
            if record.id != *id {
                return Err(view.budget.fail(Error::new(
                    "STORAGE_FAILURE",
                    "RDF record catalog identity mismatch",
                )));
            }
            drop(bytes);
            Arc::get_mut(&mut view._scope)
                .expect("scope capture precedes owned query admission")
                .resize(scope_bytes)?;
            if record.expires_at <= now {
                targets.insert(record.id.clone());
                targets.insert(record.artifact_id.clone());
            }
            records.push(record);
        }
        let protected = if targets.is_empty() {
            BTreeSet::new()
        } else {
            let maximum = writer
                .config
                .max_pin_members
                .saturating_mul(128)
                .saturating_add(4096);
            writer
                .protected_matching(
                    now,
                    |member| targets.contains(member),
                    || {
                        view.budget
                            .request(maximum as u64, false)
                            .and_then(|_| view.budget.reserve((maximum as u64).saturating_mul(64)))
                            .map_err(|error| Error::new(error.code, error.message.clone()))
                    },
                    Some(maximum),
                    None,
                )
                .map_err(|e| view.budget.fail(e))?
        };
        for record in records {
            writer
                .validate_event_record(&record)
                .map_err(|error| view.budget.fail(error))?;
            if !writer.live_record(&record, now, &protected) {
                return Err(view.budget.fail(Error::new(
                    "EVIDENCE_UNAVAILABLE",
                    "RDF selection contains expired evidence",
                )));
            }
            view.include_record(&record, layers, true, &mut RdfScopeCoverage::default())?;
        }
        drop(writer);
        purrdf::dataset_view::checkpointed_drain(&view, operation).map_err(|failure| failure.error)
    }
}
