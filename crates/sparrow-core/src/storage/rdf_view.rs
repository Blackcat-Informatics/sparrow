// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

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
mod text_relations;
use budget::{Budget, Reservation};
pub use explanation::SparqlExplanationItem;
pub use pages::{
    NativePageBody, SparqlExplanationPageBody, SparqlExplanationReadPage, SparqlGraphKind,
    SparqlItem, SparqlPageBody, SparqlReadPage, sparql_term_node_bytes, sparql_term_wire_bytes,
};
pub use query::{ScopedSparqlExplanation, ScopedSparqlResult, SparqlRead};
pub use scope::RdfScopeCoverage;
pub(crate) use scope::{ReadSlot, ReadSlots};
use sparse::RowCursor;
pub use text_relations::{
    STORED_TEXT_CORPUS_PROFILE, STORED_TEXT_RELATION_PROFILE, SparqlTextCapture,
    SparqlTextCoverage, SparqlTextRelations,
};

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
    reverse: &'a rocksdb::ColumnFamily,
    owners: BTreeSet<u64>,
    /// Membership narrower than the whole live store: patterns are evaluated per graph
    /// of `owners`, enumerated from the owner-to-graph index, so cost follows the scope.
    scoped: bool,
    /// While the owner-to-graph index build is pending in this snapshot, the first graph
    /// declaration key it has not copied, and whether that key itself is uncopied. Every
    /// declaration through the build's frontier, and every one written since its fence, has
    /// its `rdf:o:` mirror; the declarations after the frontier are read directly.
    owner_pending: Option<(Vec<u8>, bool)>,
    /// The scope's graphs, enumerated once per view: every graph iteration shares them,
    /// so a query's patterns do not each re-walk the declarations.
    owned_graphs: std::sync::OnceLock<Option<std::sync::Arc<OwnedGraphSet>>>,
    /// The receipt-time fallback facts this view derives for record graphs that lack them.
    fallback: Option<FallbackPending>,
    budget: Arc<Budget>,
    _scope: Arc<Reservation>,
    terms: u64,
    max_encoded: u64,
    scope_receipt: Option<super::Value>,
    text_scope: Option<text_relations::TextScope>,
    /// When set, each graph owner added to the scope raises the read ceilings by this cost
    /// per stored row in its manifest.
    row_allowance: Option<RowAllowance>,
}

/// The receipt-time fallback facts one snapshot derives for selected record graphs that do
/// not store them. The view serves those facts with the stored rows; ingest stores none.
struct FallbackPending {
    /// Dictionary identities of `eventTime` and `eventTimeFallback`, when interned.
    predicates: [Option<u64>; 2],
    /// The first dictionary ordinal the snapshot has not allocated: `rdf:next` holds the
    /// last allocated one, so every stored term's ordinal is below this, and owed terms the
    /// dictionary lacks take ordinals from here.
    next: u64,
    /// Owed terms the dictionary lacks, by ordinal from `next`, with their canonical bytes.
    terms: Vec<(Vec<u8>, ProjectionTerm)>,
    /// Derived fallback facts of selected record graphs that lack them, each in its
    /// record's metadata graph.
    quads: Vec<QuadIds<GlobalTermId>>,
    _reservation: Option<Reservation>,
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
            reverse: rdf_index::reverse_family(db).map_err(|e| budget.fail(e))?,
            owners: BTreeSet::new(),
            scoped: false,
            owner_pending: None,
            owned_graphs: std::sync::OnceLock::new(),
            fallback: None,
            budget,
            _scope: scope,
            terms: 0,
            max_encoded: 0,
            scope_receipt: None,
            text_scope: None,
            row_allowance: None,
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
        view.migration_state()?;
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

    /// Read the owner index checkpoint and arm the fallback derivation from this view's
    /// snapshot.
    fn migration_state(&mut self) -> Result<(), ReadError> {
        let read = |key: &str| {
            self.budget.request(key.len() as u64 + 1024, false)?;
            self.snapshot
                .get(key)
                .map_err(|error| self.budget.fail(error.into()))
        };
        let owner = read(rdf_index::OWNER_GRAPH_UPGRADE)?;
        let next = read("rdf:next")?;
        self.owner_pending = rdf_index::owner_graph_progress(|_| Ok(owner))
            .map_err(|error| self.budget.fail(error))?
            .map(|after| match after {
                None => (rdf_index::graph_key(0, 0), true),
                Some((graph, owner)) => (rdf_index::graph_key(graph, owner), false),
            });
        let mut predicates = [None; 2];
        for (slot, predicate) in predicates
            .iter_mut()
            .zip(super::event_fallback_rdf::event_time_predicates())
        {
            *slot = self.term_id_in(&predicate, false)?.map(ordinal);
        }
        let next = next
            .as_deref()
            .map(<[u8; 8]>::try_from)
            .transpose()
            .ok()
            .flatten()
            .map(u64::from_be_bytes)
            .and_then(|last| last.checked_add(1))
            .ok_or_else(|| {
                self.budget.fail(Error::new(
                    "STORAGE_FAILURE",
                    "RDF dictionary allocation counter missing or invalid",
                ))
            })?;
        self.fallback = Some(FallbackPending {
            predicates,
            next,
            terms: Vec::new(),
            quads: Vec::new(),
            _reservation: None,
        });
        Ok(())
    }

    /// Serve the fallback facts `record`'s metadata graph lacks, derived from the record's
    /// receipt time.
    fn owe_fallback(&mut self, record: &Record) -> Result<(), ReadError> {
        use purrdf::TermFactory;
        let fail = |budget: &Budget, error: Error| budget.fail(error);
        let mut builder = purrdf::RdfDatasetBuilder::new();
        let subject_value = TermValue::iri(rdf_index::entity_iri(&record.id));
        let facts = builder
            .intern_value(&subject_value)
            .map_err(Error::from)
            .and_then(|subject| {
                rdf_index::fallback_metadata(
                    &mut builder,
                    subject,
                    crate::EventPlacement::receipt(record.received_at),
                )
            })
            .and_then(|()| rdf_index::metadata_dataset(builder))
            .map_err(|error| fail(&self.budget, error))?;
        let graph = self
            .term_id_in(&TermValue::iri(rdf_index::graph_iri(&record.id)), false)?
            .ok_or_else(|| {
                fail(
                    &self.budget,
                    Error::new("STORAGE_FAILURE", "record RDF graph owner missing"),
                )
            })?;
        let subject = self.term_id_in(&subject_value, false)?.ok_or_else(|| {
            fail(
                &self.budget,
                Error::new("STORAGE_FAILURE", "record RDF graph subject missing"),
            )
        })?;
        for quad in facts.quads() {
            let [p, o] = [quad.p, quad.o].map(|id| {
                facts
                    .term_value(id)
                    .map_err(|e| fail(&self.budget, Error::new("STORAGE_FAILURE", e.to_string())))
            });
            let p = self.owe_term(&p?)?;
            let o = self.owe_term(&o?)?;
            let pending = self.fallback.as_mut().ok_or_else(|| {
                fail(
                    &self.budget,
                    Error::new("STORAGE_FAILURE", "event fallback state lost"),
                )
            })?;
            match &mut pending._reservation {
                Some(reservation) => reservation.grow(64)?,
                None => pending._reservation = Some(self.budget.reserve(64)?),
            }
            pending.quads.push(QuadIds {
                s: subject,
                p,
                o,
                g: Some(graph),
            });
        }
        Ok(())
    }

    /// The identity of an owed fact's term: its dictionary identity, or an ordinal from the
    /// snapshot's next dictionary ordinal when the dictionary lacks it.
    fn owe_term(&mut self, value: &TermValue) -> Result<GlobalTermId, ReadError> {
        if let Some(id) = self.term_id_in(value, false)? {
            return Ok(id);
        }
        if let TermValue::Literal { datatype, .. } = value {
            self.owe_term(&TermValue::iri(datatype.clone()))?;
        }
        let limits = rdf_index::term_limits().map_err(|e| self.budget.fail(e))?;
        let term = ProjectionTerm::from_term_value(value, limits).map_err(|e| {
            self.budget
                .fail(Error::new("STORAGE_FAILURE", e.to_string()))
        })?;
        let bytes = term.to_canonical_json(limits).map_err(|e| {
            self.budget
                .fail(Error::new("STORAGE_FAILURE", e.to_string()))
        })?;
        let charge = bytes.len() as u64 * 2 + 256;
        let budget = &self.budget;
        let pending = self.fallback.as_mut().ok_or_else(|| {
            budget.fail(Error::new("STORAGE_FAILURE", "event fallback state lost"))
        })?;
        match &mut pending._reservation {
            Some(reservation) => reservation.grow(charge)?,
            None => pending._reservation = Some(budget.reserve(charge)?),
        }
        pending.terms.push((bytes, term));
        global(pending.next + pending.terms.len() as u64 - 1).map_err(|e| budget.fail(e))
    }

    /// The owed fallback facts matching `pattern` and `graph`, with the graph semantics of
    /// the stored rows: a metadata graph is named, and the default graph is their union.
    fn owed_rows(
        &self,
        pattern: [Option<GlobalTermId>; 3],
        graph: GraphMatch<GlobalTermId>,
    ) -> impl Iterator<Item = QuadIds<GlobalTermId>> + '_ {
        self.fallback
            .iter()
            .flat_map(|pending| pending.quads.iter())
            .filter(move |quad| {
                pattern[0].is_none_or(|id| id == quad.s)
                    && pattern[1].is_none_or(|id| id == quad.p)
                    && pattern[2].is_none_or(|id| id == quad.o)
            })
            .flat_map(move |quad| {
                let named = match graph {
                    GraphMatch::Named(id) => quad.g == Some(id),
                    GraphMatch::Any => true,
                    GraphMatch::Default => false,
                };
                let union = matches!(graph, GraphMatch::Any | GraphMatch::Default);
                [
                    named.then_some(*quad),
                    union.then_some(QuadIds { g: None, ..*quad }),
                ]
                .into_iter()
                .flatten()
            })
    }

    /// An owed term by its ordinal, when it is one.
    fn owed_term(&self, id: GlobalTermId) -> Option<&ProjectionTerm> {
        let pending = self.fallback.as_ref()?;
        let index = ordinal(id).checked_sub(pending.next)?;
        pending
            .terms
            .get(usize::try_from(index).ok()?)
            .map(|(_, term)| term)
    }

    /// Whether a record's metadata graph lacks the fallback facts, so the view derives them.
    /// A graph an earlier writer or the retired backfill gave the facts keeps them.
    fn fallback_pending(&self, id: &str) -> Result<bool, ReadError> {
        let Some(pending) = &self.fallback else {
            return Ok(false);
        };
        // Uncharged to the request budget, as the rest of the derivation. Decoding is still
        // reserved.
        let key = format!("rdf:graph:{id}");
        let bytes = self
            .snapshot
            .get_pinned(key.as_bytes())
            .map_err(|error| self.budget.fail(error.into()))?
            .ok_or_else(|| {
                self.budget
                    .fail(Error::new("STORAGE_FAILURE", "record RDF graph missing"))
            })?;
        let _decode = self
            .budget
            .reserve((bytes.len() as u64).saturating_mul(64))?;
        let rows = super::parse(&bytes)
            .and_then(|manifest| rdf_index::manifest_rows(&manifest))
            .map_err(|error| self.budget.fail(error))?;
        drop(bytes);
        let subject = self
            .term_id_in(&TermValue::iri(rdf_index::entity_iri(id)), false)?
            .map(ordinal);
        Ok(!super::event_fallback_rdf::carries_event_time(
            &rows,
            subject,
            pending.predicates,
        ))
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
        self.point_in(None, key, maximum, true)
    }

    /// A record row grows with its record's chunk count. New layouts cap that at
    /// 2048 chunks, but stores laid out earlier may hold larger raw records, so
    /// charge the measured length before the caller reserves decoding work.
    fn catalog_point(&self, key: &[u8]) -> Result<Option<rocksdb::DBPinnableSlice<'_>>, ReadError> {
        let bytes = self
            .snapshot
            .get_pinned(key)
            .map_err(|error| self.budget.fail(error.into()))?;
        self.budget.request(
            (key.len() as u64).saturating_add(bytes.as_ref().map_or(0, |bytes| bytes.len() as u64)),
            false,
        )?;
        Ok(bytes)
    }

    fn reverse_point(
        &self,
        key: &[u8],
        charged: bool,
    ) -> Result<Option<rocksdb::DBPinnableSlice<'_>>, ReadError> {
        self.point_in(Some(self.reverse), key, 8, charged)
    }

    /// One bounded point read. `charged` reads spend the request budget; the reads that
    /// serve what a pending migration owes do not, so a scope near its budget answers
    /// during the migration as it does after it (#158).
    fn point_in(
        &self,
        family: Option<&rocksdb::ColumnFamily>,
        key: &[u8],
        maximum: u64,
        charged: bool,
    ) -> Result<Option<rocksdb::DBPinnableSlice<'_>>, ReadError> {
        if charged {
            self.budget.request(key.len() as u64 + maximum, false)?;
        }
        let bytes = match family {
            Some(family) => self.snapshot.get_pinned_cf(family, key),
            None => self.snapshot.get_pinned(key),
        }
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
        self.lookup_in(value, true)
    }

    /// [`DatasetView::term_id_by_value`], spending the request budget only when `charged`.
    fn term_id_in(
        &self,
        value: &TermValue,
        charged: bool,
    ) -> Result<Option<GlobalTermId>, ReadError> {
        let mut remaining = 65536;
        input_capacity(value, 0, &mut remaining).map_err(|e| self.budget.fail(e))?;
        let _reservation = self.budget.reserve((65536 - remaining) * 256 + 4096)?;
        let limits = rdf_index::term_limits().map_err(|e| self.budget.fail(e))?;
        let term = ProjectionTerm::from_term_value(value, limits).map_err(|e| {
            self.budget
                .fail(Error::new("INVALID_ARGUMENT", e.to_string()))
        })?;
        self.lookup_in(&term, charged)
    }

    /// [`Self::lookup`], spending the request budget only when `charged`.
    fn lookup_in(
        &self,
        value: &ProjectionTerm,
        charged: bool,
    ) -> Result<Option<GlobalTermId>, ReadError> {
        let limits = rdf_index::term_limits().map_err(|e| self.budget.fail(e))?;
        let bytes = value.to_canonical_json(limits).map_err(|e| {
            self.budget
                .fail(Error::new("INVALID_ARGUMENT", e.to_string()))
        })?;
        let key = rdf_index::reverse_key(&bytes);
        let Some(stored) = self.reverse_point(key.as_bytes(), charged)? else {
            let owed = self.fallback.as_ref().and_then(|pending| {
                pending
                    .terms
                    .iter()
                    .position(|(owed, _)| *owed == bytes)
                    .map(|index| pending.next + index as u64)
            });
            return owed
                .map(global)
                .transpose()
                .map_err(|e| self.budget.fail(e));
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
        let forward = self.verified_forward_in(global, charged)?;
        if forward.bytes.as_ref() != bytes.as_slice() {
            return Err(self.budget.fail(Error::new(
                "STORAGE_FAILURE",
                "RDF forward/reverse identity mismatch",
            )));
        }
        Ok(Some(global))
    }

    fn verified_forward(&self, id: GlobalTermId) -> Result<EncodedGuard<'_>, ReadError> {
        self.verified_forward_in(id, true)
    }

    /// [`Self::verified_forward`], spending the request budget only when `charged`.
    fn verified_forward_in(
        &self,
        id: GlobalTermId,
        charged: bool,
    ) -> Result<EncodedGuard<'_>, ReadError> {
        let reservation = self.budget.reserve(self.max_encoded + 1024)?;
        let key = rdf_index::term_key(ordinal(id));
        let bytes = self
            .point_in(None, key.as_bytes(), self.max_encoded, charged)?
            .ok_or_else(|| {
                self.budget
                    .fail(Error::new("STORAGE_FAILURE", "RDF term missing or foreign"))
            })?;
        let reverse = rdf_index::reverse_key(&bytes);
        let stored = self.reverse_point(reverse.as_bytes(), charged)?;
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
        let cursor = |plan: usize, graph, union_default| {
            let unbound = pattern.iter().all(Option::is_none);
            if matches!(graph, GraphMatch::Named(_)) || !(unbound || (self.scoped && plan < 3)) {
                RowCursor::Indexed(self.physical_rows(stream, plan, pattern, graph, union_default))
            } else {
                // Every unbound pattern, and in a scoped view every bound one, merges one
                // graph-leading cursor per in-scope graph in the store-wide plan's order.
                let plan = if unbound { 0 } else { plan };
                RowCursor::scoped(self, stream, plan, pattern, union_default)
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
        let owed = (stream == 0).then(|| self.owed_rows(pattern, graph));
        first
            .chain(union.into_iter().flatten())
            .chain(owed.into_iter().flatten())
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

    /// A scoped view walks its owners' `rdf:o:` rows; the whole live store walks every
    /// `rdf:g:` declaration. Both charge one request and row visit per row read.
    fn graph_rows(&self) -> GraphIter<'_> {
        let reservation = self.budget.reserve(4096);
        let iterator = (reservation.is_ok() && !self.scoped).then(|| {
            self.snapshot
                .iterator(IteratorMode::From(b"rdf:g:", Direction::Forward))
        });
        GraphIter {
            view: self,
            iterator,
            owned: self.scoped.then_some(OwnedGraphs::Pending),
            previous: None,
            _reservation: reservation.ok(),
        }
    }

    /// Distinct graphs declared by this view's owners, in global graph order.
    fn owner_graphs(&self) -> Option<std::sync::Arc<OwnedGraphSet>> {
        self.owned_graphs
            .get_or_init(|| {
                self.enumerate_owner_graphs()
                    .ok()
                    .map(|(graphs, reservation)| {
                        std::sync::Arc::new(OwnedGraphSet {
                            graphs: graphs.into_iter().collect(),
                            _reservation: reservation,
                        })
                    })
            })
            .clone()
    }

    /// One enumeration of the scope's graphs; a failure is recorded in the budget.
    fn enumerate_owner_graphs(&self) -> Result<(BTreeSet<u64>, Reservation), ReadError> {
        let mut reservation = self.budget.reserve(4096)?;
        let mut graphs = BTreeSet::new();
        for owner in &self.owners {
            let mut prefix = rdf_index::OWNER_GRAPH_PREFIX.to_vec();
            prefix.extend_from_slice(&owner.to_be_bytes());
            let mut rows = self
                .snapshot
                .iterator(IteratorMode::From(&prefix, Direction::Forward));
            // Charge each advance, including the terminal probe, before it reads.
            loop {
                self.budget.request(64, true)?;
                let Some(row) = rows.next() else {
                    break;
                };
                let (key, value) = row.map_err(|e| self.budget.fail(e.into()))?;
                if !key.starts_with(&prefix) {
                    break;
                }
                let graph = <[u8; 8]>::try_from(&key[prefix.len()..])
                    .ok()
                    .filter(|_| value.is_empty())
                    .map(u64::from_be_bytes)
                    .ok_or_else(|| {
                        self.budget.fail(Error::new(
                            "STORAGE_FAILURE",
                            "invalid RDF owner graph declaration",
                        ))
                    })?;
                if graph == 0 {
                    return Err(self
                        .budget
                        .fail(Error::new("STORAGE_FAILURE", "zero RDF graph identity")));
                }
                if graphs.insert(graph) {
                    reservation.grow(64)?;
                }
            }
        }
        if let Some((start, inclusive)) = &self.owner_pending {
            self.uncopied_owner_graphs(start, *inclusive, &mut graphs, &mut reservation)?;
        }
        Ok((graphs, reservation))
    }

    /// Add the graphs of this view's owners among the declarations the pending owner index
    /// build has not copied, from `start`. The walk is the build's own remaining work and
    /// shrinks as it advances; it answers exactly instead of refusing, so its rows are not
    /// charged to the request budget (#158). It checks cancellation and the read lease every
    /// 4,096 rows and charges the graphs it keeps.
    fn uncopied_owner_graphs(
        &self,
        start: &[u8],
        inclusive: bool,
        graphs: &mut BTreeSet<u64>,
        reservation: &mut Reservation,
    ) -> Result<(), ReadError> {
        let prefix = &start[..6];
        let rows = self
            .snapshot
            .iterator(IteratorMode::From(start, Direction::Forward));
        for (index, row) in rows.enumerate() {
            if index % 4096 == 0
                && let (Some(error), _) = self.budget.checkpoint()
            {
                return Err(error);
            }
            let (key, value) = row.map_err(|e| self.budget.fail(e.into()))?;
            if !key.starts_with(prefix) {
                break;
            }
            if !inclusive && *key == *start {
                continue;
            }
            let suffix = &key[prefix.len()..];
            let [graph, owner] = suffix
                .as_chunks::<8>()
                .0
                .try_into()
                .ok()
                .filter(|_| suffix.len() == 16 && value.is_empty())
                .ok_or_else(|| {
                    self.budget.fail(Error::new(
                        "STORAGE_FAILURE",
                        "invalid RDF graph declaration",
                    ))
                })?;
            let (graph, owner) = (u64::from_be_bytes(graph), u64::from_be_bytes(owner));
            if graph == 0 || owner == 0 {
                return Err(self
                    .budget
                    .fail(Error::new("STORAGE_FAILURE", "zero RDF graph declaration")));
            }
            if self.owners.contains(&owner) && graphs.insert(graph) {
                reservation.grow(64)?;
            }
        }
        Ok(())
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
    owned: Option<OwnedGraphs>,
    previous: Option<GlobalTermId>,
    _reservation: Option<Reservation>,
}

/// A scoped view's graphs, enumerated on first use so an unused iterator reads nothing.
enum OwnedGraphs {
    Pending,
    Ready {
        set: std::sync::Arc<OwnedGraphSet>,
        next: usize,
    },
}

/// The graphs of a scope in global graph order, with the reservation that charges them.
struct OwnedGraphSet {
    graphs: Vec<u64>,
    _reservation: Reservation,
}

impl GraphIter<'_> {
    fn next_owned(&mut self) -> Option<GlobalTermId> {
        let owned = self.owned.as_mut()?;
        if matches!(owned, OwnedGraphs::Pending) {
            *owned = OwnedGraphs::Ready {
                set: self.view.owner_graphs()?,
                next: 0,
            };
        }
        let OwnedGraphs::Ready { set, next } = owned else {
            unreachable!("owner graphs were enumerated above")
        };
        let graph = *set.graphs.get(*next)?;
        *next += 1;
        let graph = match global(graph) {
            Ok(id) => id,
            Err(e) => {
                self.view.budget.fail(e);
                return None;
            }
        };
        self.view.verified_forward(graph).ok()?;
        Some(graph)
    }
}

impl Iterator for GraphIter<'_> {
    type Item = GlobalTermId;
    fn next(&mut self) -> Option<Self::Item> {
        if self.owned.is_some() {
            return self.next_owned();
        }
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
        let value = match self.owed_term(id) {
            Some(term) => term.clone(),
            None => {
                let bytes = self.verified_forward(id)?;
                let limits = rdf_index::term_limits().map_err(|e| self.budget.fail(e))?;
                ProjectionTerm::from_canonical_json(&bytes.bytes, limits).map_err(|e| {
                    self.budget
                        .fail(Error::new("STORAGE_FAILURE", e.to_string()))
                })?
            }
        };
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
        self.term_id_in(value, true)
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
            + self
                .fallback
                .as_ref()
                .map_or(0, |pending| pending.terms.len() as u64)
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
    /// One query RDF snapshot per store is admitted; a concurrent query refuses explicitly. The
    /// archive has its own slot.
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
        self.with_rdf_scope_ids(
            ReadSlot::Query,
            ids,
            &[],
            layers,
            now,
            limits,
            None,
            |view, _| operation(view),
        )
    }

    /// The same capture with named artifacts' graphs added to the records'. The operation also
    /// receives the artifact ids that have no RDF ownership (retired before the store was
    /// fenced) and so are absent from the view. The selection is not capped at the query bound
    /// of 1,000 (`with_rdf_record_layers` keeps it): the archive reads a whole segment, whose
    /// size its own selection bounds, and each id is charged to the scope's live bytes.
    ///
    /// The writer is held only to pin the snapshot, so a capture of any size does not pause
    /// ingestion. A capture that finds expired evidence runs twice: the first pass names the
    /// expired targets, and the second pins a new snapshot and reads the pins that protect
    /// those targets under one hold of the writer, so the pins and the snapshot are as of the
    /// same moment. Decoding and including then run on that snapshot without the writer, and
    /// a record found expired there that the first pass did not name fails the capture with
    /// the retryable `RESOURCE_LIMIT`, with nothing read.
    /// `allowance` raises the read ceilings by a cost per stored row found in the scope.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn with_rdf_scope_ids<T>(
        &self,
        slot: ReadSlot,
        ids: &[String],
        artifacts: &[String],
        layers: RdfGraphLayers,
        now: u64,
        limits: RdfReadLimits,
        allowance: Option<RowAllowance>,
        operation: impl FnOnce(&StoredRdfView<'_>, &[String]) -> T,
    ) -> Result<T, ReadError> {
        if ids.is_empty() && artifacts.is_empty() {
            return Err(Arc::new(Error::new(
                "INVALID_ARGUMENT",
                "RDF selection requires at least one record or artifact ID",
            )));
        }
        let _lease = self.rdf_read_lease(slot)?;
        let mut operation = Some(operation);
        let mut scope = ScopeCapture {
            ids,
            artifacts,
            layers,
            now,
            limits,
            allowance,
            expired: None,
        };
        let expired = match self.capture_scope(&scope, &mut operation)? {
            Capture::Done(result) => return Ok(result),
            Capture::Expired(expired) => expired,
        };
        scope.expired = Some(&expired);
        match self.capture_scope(&scope, &mut operation)? {
            Capture::Done(result) => Ok(result),
            Capture::Expired(_) => Err(Arc::new(Error::new(
                "STORAGE_FAILURE",
                "RDF scope capture with the expired targets known did not run",
            ))),
        }
    }

    /// One capture and its operation, or the expired targets when the selection holds expired
    /// evidence and `scope.expired` does not yet name them.
    fn capture_scope<T>(
        &self,
        scope: &ScopeCapture<'_>,
        operation: &mut Option<impl FnOnce(&StoredRdfView<'_>, &[String]) -> T>,
    ) -> Result<Capture<T>, ReadError> {
        let ScopeCapture {
            ids,
            artifacts,
            layers,
            now,
            limits,
            allowance,
            expired,
        } = *scope;
        // The one writer hold: pin the snapshot and read the pins protecting the known
        // expired targets, which must be as of the same moment.
        let writer = self.lock().map_err(Arc::new)?;
        let db = Arc::clone(&writer.db);
        let mut view = StoredRdfView::new(&db, limits, &[])?;
        view.scoped = true;
        view.row_allowance = allowance;
        let protected = match expired {
            None => BTreeSet::new(),
            Some(targets) => {
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
                                .and_then(|_| {
                                    view.budget.reserve((maximum as u64).saturating_mul(64))
                                })
                                .map_err(|error| Error::new(error.code, error.message.clone()))
                        },
                        Some(maximum),
                        None,
                    )
                    .map_err(|e| view.budget.fail(e))?
            }
        };
        drop(writer);
        let mut scope_bytes = 4096 + (ids.len() + artifacts.len()) as u64 * 2048;
        Arc::get_mut(&mut view._scope)
            .expect("scope capture precedes owned query admission")
            .resize(scope_bytes)?;
        let mut records = Vec::with_capacity(ids.len());
        let mut targets = BTreeSet::new();
        for id in ids {
            if !id.starts_with("record:") || id.len() > 128 {
                return Err(view.budget.fail(Error::new(
                    "INVALID_ARGUMENT",
                    "RDF selection requires bounded record IDs",
                )));
            }
            let bytes = view.catalog_point(id.as_bytes())?.ok_or_else(|| {
                view.budget
                    .fail(Error::new("NOT_FOUND", "RDF selected record missing"))
            })?;
            let _decode = view
                .budget
                .reserve((bytes.len() as u64).saturating_mul(64))?;
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
                if expired.is_some_and(|known| !known.contains(&record.id)) {
                    // Its pins were not read with this snapshot: neither protected nor
                    // unprotected may be guessed.
                    return Err(view.budget.fail(Error::new(
                        "RESOURCE_LIMIT",
                        "RDF selection changed between capture passes; retry",
                    )));
                }
                targets.insert(record.id.clone());
                targets.insert(record.artifact_id.clone());
            }
            records.push(record);
        }
        if expired.is_none() && !targets.is_empty() {
            return Ok(Capture::Expired(targets));
        }
        for record in records {
            super::source_time::validate_event_record(
                &super::store_counters::Reader::snapshot(&view.snapshot),
                &record,
            )
            .map_err(|error| view.budget.fail(error))?;
            if !super::live_record(&record, now, &protected) {
                return Err(view.budget.fail(Error::new(
                    "EVIDENCE_UNAVAILABLE",
                    "RDF selection contains expired evidence",
                )));
            }
            view.include_record(&record, layers, true, &mut RdfScopeCoverage::default())?;
        }
        let mut absent = Vec::new();
        for id in artifacts {
            if !id.starts_with("artifact:") || id.len() > 128 {
                return Err(view.budget.fail(Error::new(
                    "INVALID_ARGUMENT",
                    "RDF selection requires bounded artifact IDs",
                )));
            }
            if !view.include_artifact_owner(id)? {
                absent.push(id.clone());
            }
        }
        let Some(operation) = operation.take() else {
            return Err(view.budget.fail(Error::new(
                "STORAGE_FAILURE",
                "RDF scope operation was already consumed",
            )));
        };
        purrdf::dataset_view::checkpointed_drain(&view, |view| operation(view, &absent))
            .map(Capture::Done)
            .map_err(|failure| failure.error)
    }
}

/// What one scope capture reads: the selection, its layers and limits, and the expired
/// targets a first pass found (`None` on the first pass). The pins and the snapshot of a
/// capture that names expired targets are read under one hold of the writer.
#[derive(Clone, Copy)]
struct ScopeCapture<'a> {
    ids: &'a [String],
    artifacts: &'a [String],
    layers: RdfGraphLayers,
    now: u64,
    limits: RdfReadLimits,
    allowance: Option<RowAllowance>,
    expired: Option<&'a BTreeSet<String>>,
}

/// The outcome of one capture pass.
enum Capture<T> {
    /// The operation ran.
    Done(T),
    /// The selection holds expired evidence; these record and artifact ids need their pins.
    Expired(BTreeSet<String>),
}

/// Read ceilings added for each stored graph row in a scope, on top of the base limits.
#[derive(Clone, Copy, Debug)]
pub(super) struct RowAllowance {
    /// Requests allowed per stored row.
    pub(super) requests: u64,
    /// Row visits allowed per stored row.
    pub(super) row_visits: u64,
}
