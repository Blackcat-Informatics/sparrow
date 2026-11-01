// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Immutable native results shared with leased, byte-bounded transport pages.

use super::*;
use crate::{
    QueryCancellation,
    storage::catalog_lease::{self, PageBody, Selector},
};
use purrdf::{
    RdfDataset,
    ir::TermId,
    sparql::{FallibleSparqlError, RetainedSparqlResult},
};
use std::{
    ops::{ControlFlow, Range},
    time::Instant,
};

/// Typed statement role within a graph or SELECT auxiliary graph.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SparqlGraphKind {
    /// Explicit named graph, including an empty graph.
    NamedGraph,
    /// Ordinary subject/predicate/object/optional graph.
    Quad,
    /// Reifier/triple term/optional graph.
    Reifier,
    /// Reifier/predicate/object/optional graph.
    Annotation,
}

/// One borrowed native item. IDs are local to the accompanying dataset and must
/// be resolved there before transport; they are never portable wire identities.
#[derive(Debug)]
pub enum SparqlItem<'a> {
    /// Projected variable values, preserving unbound cells and solution bags.
    Solution(&'a [Option<TermValue>]),
    /// Native ASK value.
    Boolean(bool),
    /// One RDF 1.2 statement or declaration, with its immutable native owner.
    Graph {
        /// Native graph owner retaining strings and term structure.
        dataset: &'a RdfDataset,
        /// Whether this is SELECT's auxiliary graph rather than a graph result.
        auxiliary: bool,
        /// Typed statement role, defining the positional term slots.
        kind: SparqlGraphKind,
        /// Native IDs: declaration [graph], quad [s,p,o,g], reifier [r,t,g], annotation [r,p,o,g].
        terms: [Option<TermId>; 4],
    },
}

fn answer(run: &ScopedSparqlResult) -> Option<&RetainedSparqlResult> {
    match &run.outcome {
        Ok(complete) => Some(&complete.result),
        Err(FallibleSparqlError::BudgetExhausted { partial, .. }) => {
            partial.result().map(|partial| partial.result())
        }
        Err(_) => None,
    }
}

fn graph_items(graph: &RdfDataset, auxiliary: bool) -> impl Iterator<Item = SparqlItem<'_>> {
    let row = move |kind, terms| SparqlItem::Graph {
        dataset: graph,
        auxiliary,
        kind,
        terms,
    };
    graph
        .named_graphs()
        .map(move |g| row(SparqlGraphKind::NamedGraph, [Some(g), None, None, None]))
        .chain(graph.quads().map(move |q| {
            row(
                SparqlGraphKind::Quad,
                [Some(q.s), Some(q.p), Some(q.o), q.g],
            )
        }))
        .chain(
            graph
                .reifiers_with_graph()
                .map(move |(r, t, g)| row(SparqlGraphKind::Reifier, [Some(r), Some(t), g, None])),
        )
        .chain(graph.annotations_with_graph().map(move |(r, p, o, g)| {
            row(SparqlGraphKind::Annotation, [Some(r), Some(p), Some(o), g])
        }))
}

fn items(run: &ScopedSparqlResult) -> impl Iterator<Item = SparqlItem<'_>> {
    let result = answer(run);
    let solutions = result
        .and_then(|r| r.solutions())
        .into_iter()
        .flat_map(|(_, rows)| rows.iter().map(|row| SparqlItem::Solution(row)));
    let boolean = result
        .and_then(|r| r.boolean())
        .into_iter()
        .map(SparqlItem::Boolean);
    let graph = result
        .and_then(|r| {
            r.graph()
                .map(|g| (g, false))
                .or_else(|| r.auxiliary_graph().map(|g| (g, true)))
        })
        .into_iter()
        .flat_map(|(g, auxiliary)| graph_items(g, auxiliary));
    solutions.chain(boolean).chain(graph)
}

#[derive(Clone, Copy)]
enum TermInput<'a> {
    Value(&'a TermValue),
    Indexed(&'a RdfDataset, TermId),
}

// Transport admission only. The native visitor owns nested traversal; native
// carriers own RDF semantics. 512 bytes per node covers typed/JSON scaffolding,
// and 6x authored UTF-8 covers worst-case JSON escaping as well as Cap'n Proto.
fn term_wire_bytes(root: TermInput<'_>) -> Result<usize, Error> {
    let mut bytes = 0usize;
    let result = purrdf::ir::visit_nested(root, |node| {
        let (text, children) = match node {
            TermInput::Value(value) => {
                let children = if let TermValue::Triple { s, p, o } = value {
                    Some([
                        TermInput::Value(s),
                        TermInput::Value(p),
                        TermInput::Value(o),
                    ])
                } else {
                    None
                };
                (value.text_payload_bytes(), children)
            }
            TermInput::Indexed(graph, id) => match graph.resolve(id) {
                TermRef::Iri(text) | TermRef::Blank { label: text, .. } => (Some(text.len()), None),
                TermRef::Literal {
                    lexical,
                    datatype,
                    language,
                    ..
                } => {
                    let TermRef::Iri(datatype) = graph.resolve(datatype) else {
                        return ControlFlow::Break(Error::new(
                            "STORAGE_FAILURE",
                            "native result literal datatype is not an IRI",
                        ));
                    };
                    (
                        lexical
                            .len()
                            .checked_add(datatype.len())
                            .and_then(|n| n.checked_add(language.map_or(0, str::len))),
                        None,
                    )
                }
                TermRef::Triple { s, p, o } => (
                    Some(0),
                    Some([
                        TermInput::Indexed(graph, s),
                        TermInput::Indexed(graph, p),
                        TermInput::Indexed(graph, o),
                    ]),
                ),
            },
        };
        let charge = text.and_then(sparql_term_node_bytes);
        let Some(next) = charge
            .and_then(|n| bytes.checked_add(n))
            .filter(|n| *n <= catalog_lease::PAGE_BYTES)
        else {
            return ControlFlow::Break(Error::new(
                "RESOURCE_LIMIT",
                "native RDF term exceeds bounded transport page",
            ));
        };
        bytes = next;
        ControlFlow::Continue(children)
    });
    match result {
        ControlFlow::Continue(()) => Ok(bytes),
        ControlFlow::Break(error) => Err(error),
    }
}

/// Transport charge for one native term node, including worst-case JSON escaping.
/// This is wire capacity, not RDF identity, validation or a scoring law.
pub fn sparql_term_node_bytes(text: usize) -> Option<usize> {
    text.checked_mul(6)?.checked_add(512)
}

/// Price an owned native term with the same law as persisted-result publication.
pub fn sparql_term_wire_bytes(value: &TermValue) -> Result<usize, Error> {
    term_wire_bytes(TermInput::Value(value))
}

fn item_wire_bytes(item: SparqlItem<'_>, variable_bytes: usize) -> Result<usize, Error> {
    let mut bytes = 512usize;
    match item {
        SparqlItem::Solution(row) => {
            // Native SRJ repeats variable names as object keys in every bound
            // solution. Pricing them only in head.vars under-admits long
            // projections, even though the typed wire carries names once.
            bytes = bytes
                .checked_add(variable_bytes)
                .ok_or_else(|| Error::new("RESOURCE_LIMIT", "solution name capacity overflow"))?;
            bytes = bytes
                .checked_add(row.len().saturating_mul(32))
                .ok_or_else(|| Error::new("RESOURCE_LIMIT", "solution cell capacity overflow"))?;
            for value in row.iter().flatten() {
                bytes = bytes
                    .checked_add(term_wire_bytes(TermInput::Value(value))?)
                    .ok_or_else(|| Error::new("RESOURCE_LIMIT", "solution capacity overflow"))?;
            }
        }
        SparqlItem::Boolean(_) => {}
        SparqlItem::Graph { dataset, terms, .. } => {
            for id in terms.into_iter().flatten() {
                bytes = bytes
                    .checked_add(term_wire_bytes(TermInput::Indexed(dataset, id))?)
                    .ok_or_else(|| Error::new("RESOURCE_LIMIT", "SPARQL item capacity overflow"))?;
            }
            // Native extended N-Quads is embedded in the JSON result envelope:
            // quotes, backslashes and control escapes may be escaped twice.
            bytes = bytes.checked_mul(2).ok_or_else(|| {
                Error::new("RESOURCE_LIMIT", "graph serialization capacity overflow")
            })?;
        }
    }
    Ok(bytes)
}

struct Published<Outcome> {
    run: ScopedSparqlResult<Outcome>,
    boundaries: Vec<usize>,
    bytes: usize,
    _admission: OwnedWorkspaceReservation<ReadError>,
}

/// A sealed immutable range sharing the original native result and its admission.
pub struct NativePageBody<Outcome> {
    published: Arc<Published<Outcome>>,
    range: Range<usize>,
}
impl<Outcome> Clone for NativePageBody<Outcome> {
    fn clone(&self) -> Self {
        Self {
            published: self.published.clone(),
            range: self.range.clone(),
        }
    }
}
impl<Outcome> std::fmt::Debug for NativePageBody<Outcome> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SparqlPageBody")
            .field("sequence", &self.published.run.sequence)
            .field("range", &self.range)
            .finish()
    }
}
// Equality is sealed capture/range identity, never a claim of RDF isomorphism.
impl<Outcome> PartialEq for NativePageBody<Outcome> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.published, &other.published) && self.range == other.range
    }
}
impl<Outcome> Eq for NativePageBody<Outcome> {}
impl<Outcome> NativePageBody<Outcome> {
    /// Original retained native outcome and independent coverage.
    pub fn captured(&self) -> &ScopedSparqlResult<Outcome> {
        &self.published.run
    }
}
impl SparqlPageBody {
    /// Borrow this page's native solution, boolean and RDF 1.2 items.
    pub fn items(&self) -> impl Iterator<Item = SparqlItem<'_>> {
        items(self.captured())
            .skip(self.range.start)
            .take(self.range.len())
    }
}
impl<Outcome> PageBody for NativePageBody<Outcome> {
    const PAGE_ITEMS: usize = 1000;
    fn len(&self) -> usize {
        self.published.boundaries.last().copied().unwrap_or(0)
    }
    fn capacity(&self) -> usize {
        self.published.bytes
    }
    fn page(&self, start: usize, end: usize) -> Result<Self, Error> {
        Ok(Self {
            published: self.published.clone(),
            range: start..end,
        })
    }
    fn page_end(&self, start: usize, _: usize) -> Result<usize, Error> {
        let index = self
            .published
            .boundaries
            .binary_search(&start)
            .map_err(|_| {
                Error::new(
                    "INVALID_ARGUMENT",
                    "cursor is not a native result page boundary",
                )
            })?;
        Ok(self
            .published
            .boundaries
            .get(index + 1)
            .copied()
            .unwrap_or(start))
    }
    fn is_boundary(&self, start: usize, _: usize) -> Result<bool, Error> {
        Ok(self.published.boundaries.binary_search(&start).is_ok())
    }
}

#[derive(Clone, PartialEq)]
pub(in crate::storage) struct SparqlSelector;
impl Selector for SparqlSelector {
    fn capacity(&self) -> usize {
        0
    }
}
pub(in crate::storage) type Reads = catalog_lease::Reads<SparqlSelector, NativeBody>;
/// A leased transport page from one immutable native query execution.
pub type SparqlReadPage = catalog_lease::CatalogReadPage<SparqlPageBody>;

impl Store {
    /// Resume a native result without re-evaluating its membership or query.
    pub fn sparql_read_next(
        &self,
        store_id: &str,
        cursor: &str,
        now: u64,
    ) -> Result<SparqlReadPage, Error> {
        if self.store_id()? != store_id {
            return Err(Error::new(
                "STORE_MISMATCH",
                "native result requires its original store",
            ));
        }
        let mut reads = self
            .sparql_reads
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "native query read registry poisoned"))?;
        reads.purge(now);
        let cursor = catalog_lease::Cursor::parse(cursor)?;
        reads
            .resume_captured(&cursor, now)?
            .map(|(_, page)| page.try_map_body(NativeBody::query))
            .transpose()?
            .ok_or_else(|| {
                Error::new(
                    "RESULT_EXPIRED",
                    "native result lease is expired, released or unknown",
                )
            })
    }

    /// Capture and publish byte-bounded native results. Continuation never reruns
    /// membership or evaluation and never renews the original lease.
    pub fn sparql_read(
        &self,
        read: &SparqlRead<'_>,
        now: u64,
        cancellation: &QueryCancellation,
    ) -> Result<SparqlReadPage, ReadError> {
        self.capture_sparql(read, None, now, cancellation)
    }

    pub(in crate::storage) fn capture_sparql(
        &self,
        read: &SparqlRead<'_>,
        correlation: Option<&crate::CorrelationCapture>,
        now: u64,
        cancellation: &QueryCancellation,
    ) -> Result<SparqlReadPage, ReadError> {
        let started = Instant::now();
        let body = self.with_sparql_result(
            (read, correlation),
            now,
            cancellation,
            |view, run| -> Result<_, ReadError> {
                let count = items(&run).count();
                let governors = match &run.outcome {
                    Ok(value) => &value.evidence.governors,
                    Err(error) => &error.evidence().governors,
                };
                let variables = answer(&run)
                    .and_then(|r| r.solutions())
                    .map_or(0, |(names, _)| {
                        names.iter().fold(0usize, |sum, name| {
                            sum.saturating_add(name.len().saturating_mul(6))
                                .saturating_add(64)
                        })
                    });
                let metadata =
                    metadata_bytes(view, &run, governors, run.outcome.as_ref().err(), variables)?;
                let staged = stage_native_page(
                    view,
                    count,
                    read.scope.limit,
                    metadata,
                    items(&run).map(|item| item_wire_bytes(item, variables)),
                )?;
                Ok(NativePageBody::new(run, count, staged))
            },
        )??;
        self.publish_native(read, now, cancellation, started, NativeBody::Query(body))?
            .try_map_body(NativeBody::query)
            .map_err(Arc::new)
    }

    fn publish_native(
        &self,
        read: &SparqlRead<'_>,
        now: u64,
        cancellation: &QueryCancellation,
        started: Instant,
        body: NativeBody,
    ) -> Result<catalog_lease::CatalogReadPage<NativeBody>, ReadError> {
        let id = crate::storage::random_id("sparql-read").map_err(Arc::new)?;
        let snapshot = catalog_lease::Snapshot {
            bytes: 0,
            id,
            store_id: body.store_id().to_owned(),
            selector: SparqlSelector,
            as_of: now,
            expires_at: now.checked_add(catalog_lease::LEASE_MS).ok_or_else(|| {
                Arc::new(Error::new(
                    "INVALID_ARGUMENT",
                    "result lease deadline overflow",
                ))
            })?,
            started,
            page_size: read.scope.limit,
            scan_budget: read.scope.scan_budget,
            scanned: body.scanned(),
            body,
        };
        let mut reads = self.sparql_reads.lock().map_err(|_| {
            Arc::new(Error::new(
                "STORAGE_FAILURE",
                "native query read registry poisoned",
            ))
        })?;
        reads.purge(now);
        reads
            .publish(
                snapshot,
                now,
                catalog_lease::HANDLE_BYTES,
                catalog_lease::HANDLE_BYTES,
                catalog_lease::MAX_HANDLES,
                cancellation,
            )
            .map_err(Arc::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borrowed_graph_items_preserve_all_native_streams_and_empty_graphs() {
        let mut builder = purrdf::ir::RdfDatasetBuilder::new();
        let s = builder.intern_iri("urn:s").unwrap();
        let p = builder.intern_iri("urn:p").unwrap();
        let o = builder.intern_iri("urn:o").unwrap();
        let r = builder
            .intern_blank("witness", purrdf::BlankScope(42))
            .unwrap();
        let g = builder.intern_iri("urn:empty").unwrap();
        builder.declare_named_graph(g).unwrap();
        builder.push_quad(s, p, o, None).unwrap();
        let t = builder.intern_triple(s, p, o).unwrap();
        builder.push_reifier(r, t).unwrap();
        builder.push_annotation(r, p, o).unwrap();
        let graph = builder.freeze().unwrap();
        let rows = graph_items(&graph, true).collect::<Vec<_>>();
        assert_eq!(rows.len(), 4);
        let expected = [
            SparqlGraphKind::NamedGraph,
            SparqlGraphKind::Quad,
            SparqlGraphKind::Reifier,
            SparqlGraphKind::Annotation,
        ];
        for (row, expected) in rows.into_iter().zip(expected) {
            let SparqlItem::Graph {
                dataset,
                auxiliary,
                kind,
                terms,
            } = row
            else {
                panic!("graph row expected");
            };
            assert!(auxiliary);
            assert!(std::ptr::eq(dataset, &*graph));
            assert_eq!(kind, expected);
            if kind == SparqlGraphKind::NamedGraph {
                assert_eq!(terms, [Some(g), None, None, None]);
            }
            if kind == SparqlGraphKind::Reifier {
                assert_eq!(terms, [Some(r), Some(t), None, None]);
            }
        }
        assert!(
            term_wire_bytes(TermInput::Indexed(&graph, t)).unwrap()
                > term_wire_bytes(TermInput::Indexed(&graph, s)).unwrap()
        );
    }
}

type QueryOutcome = purrdf::sparql::FallibleSparqlResult<
    ReadError,
    purrdf::sparql::GovernedEvidence<RdfReadEvidence>,
>;
type ExplanationOutcome = purrdf::sparql::FallibleScopedResult<
    purrdf::sparql::RetainedQueryExplanation,
    ReadError,
    purrdf::sparql::GovernedEvidence<RdfReadEvidence>,
>;

/// Immutable native query answers under the original host admission.
pub type SparqlPageBody = NativePageBody<QueryOutcome>;
/// Immutable measured native explanation under the original host admission.
pub type SparqlExplanationPageBody = NativePageBody<ExplanationOutcome>;
/// A leased transport page from one immutable native explanation.
pub type SparqlExplanationReadPage = catalog_lease::CatalogReadPage<SparqlExplanationPageBody>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::storage) enum NativeBody {
    Query(SparqlPageBody),
    Explanation(SparqlExplanationPageBody),
}

impl NativeBody {
    fn query(self) -> Result<SparqlPageBody, Error> {
        match self {
            Self::Query(body) => Ok(body),
            Self::Explanation(_) => Err(Error::new(
                "INVALID_ARGUMENT",
                "cursor contains an explanation, not query answers",
            )),
        }
    }
    fn explanation(self) -> Result<SparqlExplanationPageBody, Error> {
        match self {
            Self::Explanation(body) => Ok(body),
            Self::Query(_) => Err(Error::new(
                "INVALID_ARGUMENT",
                "cursor contains query answers, not an explanation",
            )),
        }
    }
    fn store_id(&self) -> &str {
        match self {
            Self::Query(body) => &body.captured().store_id,
            Self::Explanation(body) => &body.captured().store_id,
        }
    }
    fn scanned(&self) -> usize {
        match self {
            Self::Query(body) => body.captured().coverage.catalog_entries,
            Self::Explanation(body) => body.captured().coverage.catalog_entries,
        }
    }
    pub(in crate::storage) fn into_result(
        page: catalog_lease::CatalogReadPage<Self>,
    ) -> Result<crate::ResultPage, Error> {
        match &page.body {
            Self::Query(_) => page
                .try_map_body(Self::query)
                .map(crate::ResultPage::Sparql),
            Self::Explanation(_) => page
                .try_map_body(Self::explanation)
                .map(crate::ResultPage::SparqlExplanation),
        }
    }
}

impl PageBody for NativeBody {
    const PAGE_ITEMS: usize = 1000;
    fn len(&self) -> usize {
        match self {
            Self::Query(body) => body.len(),
            Self::Explanation(body) => body.len(),
        }
    }
    fn capacity(&self) -> usize {
        match self {
            Self::Query(body) => body.capacity(),
            Self::Explanation(body) => body.capacity(),
        }
    }
    fn page(&self, start: usize, end: usize) -> Result<Self, Error> {
        match self {
            Self::Query(body) => body.page(start, end).map(Self::Query),
            Self::Explanation(body) => body.page(start, end).map(Self::Explanation),
        }
    }
    fn page_end(&self, start: usize, size: usize) -> Result<usize, Error> {
        match self {
            Self::Query(body) => body.page_end(start, size),
            Self::Explanation(body) => body.page_end(start, size),
        }
    }
    fn is_boundary(&self, start: usize, size: usize) -> Result<bool, Error> {
        match self {
            Self::Query(body) => body.is_boundary(start, size),
            Self::Explanation(body) => body.is_boundary(start, size),
        }
    }
}

impl Store {
    /// Capture measured work once; continuation retains the original ledger and lease.
    /// Explanations and query answers share the native handle/byte admission family.
    pub fn sparql_explanation_read(
        &self,
        read: &SparqlRead<'_>,
        now: u64,
        cancellation: &QueryCancellation,
    ) -> Result<SparqlExplanationReadPage, ReadError> {
        let started = Instant::now();
        let body = self.with_sparql_explanation(
            read,
            now,
            cancellation,
            |view, run| -> Result<_, ReadError> {
                let governors = match &run.outcome {
                    Ok((_, evidence)) => &evidence.governors,
                    Err(error) => &error.evidence().governors,
                };
                let explanation = run.outcome.as_ref().ok().map(|(value, _)| &**value);
                let payload_metadata = explanation
                    .map(super::explanation::metadata_bytes)
                    .transpose()
                    .map_err(|e| view.budget.fail(e))?
                    .unwrap_or(0);
                let metadata = metadata_bytes(
                    view,
                    &run,
                    governors,
                    run.outcome.as_ref().err(),
                    payload_metadata,
                )?;
                let count = super::explanation::items(explanation).count();
                let staged = stage_native_page(
                    view,
                    count,
                    read.scope.limit,
                    metadata,
                    super::explanation::items(explanation).map(super::explanation::item_bytes),
                )?;
                Ok(NativePageBody::new(run, count, staged))
            },
        )??;
        self.publish_native(
            read,
            now,
            cancellation,
            started,
            NativeBody::Explanation(body),
        )?
        .try_map_body(NativeBody::explanation)
        .map_err(Arc::new)
    }
}

impl SparqlExplanationPageBody {
    /// Borrow structured native explanation entries in stable section order.
    pub fn items(&self) -> impl Iterator<Item = super::explanation::SparqlExplanationItem<'_>> {
        super::explanation::items(
            self.captured()
                .outcome
                .as_ref()
                .ok()
                .map(|(value, _)| &**value),
        )
        .skip(self.range.start)
        .take(self.range.len())
    }
}

struct Staged {
    boundaries: Vec<usize>,
    bytes: usize,
    admission: OwnedWorkspaceReservation<ReadError>,
}

impl<Outcome> NativePageBody<Outcome> {
    fn new(run: ScopedSparqlResult<Outcome>, count: usize, staged: Staged) -> Self {
        Self {
            published: Arc::new(Published {
                run,
                boundaries: staged.boundaries,
                bytes: staged.bytes,
                _admission: staged.admission,
            }),
            range: 0..count,
        }
    }
}

fn metadata_bytes<Outcome>(
    view: &StoredRdfView<'_>,
    run: &ScopedSparqlResult<Outcome>,
    governors: &purrdf::sparql::GovernorEvidence,
    error: Option<
        &purrdf::sparql::FallibleSparqlError<
            ReadError,
            purrdf::sparql::GovernedEvidence<RdfReadEvidence>,
        >,
    >,
    payload_metadata: usize,
) -> Result<usize, ReadError> {
    let governor_bytes = governors.retained_bytes().ok_or_else(|| {
        view.budget.fail(Error::new(
            "RESOURCE_LIMIT",
            "native governor receipt capacity overflow",
        ))
    })?;
    let error_bytes = match error {
        Some(FallibleSparqlError::Query { diagnostic, .. }) => {
            diagnostic.diagnostic().retained_bytes().ok_or_else(|| {
                view.budget.fail(Error::new(
                    "RESOURCE_LIMIT",
                    "native diagnostic capacity overflow",
                ))
            })?
        }
        Some(FallibleSparqlError::Operational { error, .. }) => {
            error.message.len().saturating_add(error.code.len())
        }
        Some(FallibleSparqlError::AllocationFailed { construct, .. }) => construct.len(),
        _ => 0,
    };
    let scope_bytes =
        catalog_lease::response_row_bytes(crate::storage::json::write_compact(&run.scope).len());
    let correlation_bytes = run.correlation.as_ref().map_or(0, |capture| {
        catalog_lease::response_row_bytes(
            crate::storage::json::write_compact(&capture.descriptor()).len(),
        )
    });
    let metadata = 8192usize
        .saturating_add(
            run.text
                .as_ref()
                .map_or(0, SparqlTextCapture::transport_bytes),
        )
        .saturating_add(correlation_bytes)
        .saturating_add(scope_bytes)
        .saturating_add(size_of::<Published<Outcome>>())
        .saturating_add(size_of::<catalog_lease::Snapshot<SparqlSelector, NativeBody>>())
        .saturating_add(governor_bytes.saturating_mul(6))
        // Both the typed diagnostic and the human failure summary
        // may carry the same authored text, each JSON-escaped.
        .saturating_add(error_bytes.saturating_mul(12))
        .saturating_add(payload_metadata);
    if metadata > 64 * 1024 {
        return Err(view.budget.fail(Error::new(
            "RESOURCE_LIMIT",
            "native query metadata exceeds 64 KiB page admission",
        )));
    }

    Ok(metadata)
}

fn stage_native_page(
    view: &StoredRdfView<'_>,
    count: usize,
    page_size: usize,
    metadata: usize,
    charges: impl Iterator<Item = Result<usize, Error>>,
) -> Result<Staged, ReadError> {
    let boundary_bytes = count
        .checked_add(2)
        .and_then(|n| n.checked_mul(size_of::<usize>()))
        .and_then(|n| n.checked_add(metadata))
        .ok_or_else(|| {
            view.budget.fail(Error::new(
                "RESOURCE_LIMIT",
                "native result boundary capacity overflow",
            ))
        })?;
    let admission = view
        .reserve_owned_workspace(boundary_bytes as u64)
        .ok_or_else(|| {
            view.budget.fail(Error::new(
                "STORAGE_FAILURE",
                "stored view lacks required owned workspace admission",
            ))
        })??;
    if view.budget.checkpoint().1.live_bytes > catalog_lease::HANDLE_BYTES as u64 {
        return Err(view.budget.fail(Error::new(
            "RESOURCE_LIMIT",
            "native result exceeds retained handle capacity",
        )));
    }
    // Native iterative term visitor may grow a work list. A per-node
    // charge stops before its storage can exceed this fixed scratch.
    let scratch = view.budget.reserve(catalog_lease::PAGE_BYTES as u64)?;
    let mut boundaries = Vec::new();
    boundaries.try_reserve_exact(count + 2).map_err(|_| {
        view.budget.fail(Error::new(
            "RESOURCE_LIMIT",
            "native page boundary allocation refused",
        ))
    })?;
    boundaries.push(0);
    let (mut bytes, mut rows) = (metadata, 0usize);
    for (index, charge) in charges.enumerate() {
        let (error, _) = view.budget.checkpoint();
        if let Some(error) = error {
            return Err(error);
        }
        let charge = charge.map_err(|e| view.budget.fail(e))?;
        if charge.saturating_add(metadata) > catalog_lease::PAGE_BYTES {
            return Err(view.budget.fail(Error::new(
                "RESOURCE_LIMIT",
                "native result item cannot fit a transport page",
            )));
        }
        if rows == page_size || bytes.saturating_add(charge) > catalog_lease::PAGE_BYTES {
            boundaries.push(index);
            bytes = metadata;
            rows = 0;
        }
        bytes += charge;
        rows += 1;
    }
    if count > 0 {
        boundaries.push(count);
    }
    drop(scratch);
    let (error, evidence) = view.budget.checkpoint();
    if let Some(error) = error {
        return Err(error);
    }
    let bytes = usize::try_from(evidence.live_bytes).map_err(|_| {
        view.budget.fail(Error::new(
            "RESOURCE_LIMIT",
            "native result retained capacity overflow",
        ))
    })?;

    Ok(Staged {
        boundaries,
        bytes,
        admission,
    })
}
