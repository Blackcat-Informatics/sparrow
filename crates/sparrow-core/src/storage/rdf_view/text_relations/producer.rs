// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Borrowed posting cursors; native PurRDF owns row and governor semantics.

use super::*;
use purrdf::sparql::{
    AdmittedPfRow, AdmittedVec, BindingPattern, EvalError, ExtensionEnv, IndexGeneration,
    NativeDiagnostic, NativeDiagnosticKind, NativeFnContext, PfArgs, PfArity, PfCursor, PfRow,
    PropertyFunction, PropertyFunctionRegistry, Volatility, WorkspaceAllocation,
    WorkspaceCapability, WorkspaceCursor, WorkspaceTerm,
};
use purrdf_core::datatype::{XSD_BOOLEAN, XSD_DECIMAL, XSD_INTEGER, XSD_STRING};
use purrdf_lex::allocation::SharedText;
use text_index::{CorpusStatistics, PostingReader, PostingWork, RankFailure, RankedPostings};

#[derive(Clone, Copy)]
enum Kind {
    Ranked,
    Spans,
}

impl Kind {
    fn arity(self) -> PfArity {
        PfArity::new(
            1,
            match self {
                Self::Ranked => 5,
                Self::Spans => 8,
            },
        )
    }
}

pub(super) fn environment<'view, 'db>(
    view: &'view StoredRdfView<'db>,
    capture: &SparqlTextCapture,
) -> Result<ExtensionEnv<'view>, ReadError> {
    let text = view.text_scope.as_ref().ok_or_else(|| {
        view.budget.fail(Error::new(
            "STORAGE_FAILURE",
            "stored-text registry requires its captured corpus",
        ))
    })?;
    let bytes = 16384_u64.saturating_add(
        (capture.relations.ranked_iri.len() as u64)
            .saturating_add(capture.relations.spans_iri.len() as u64)
            .saturating_mul(16),
    );
    let configuration = Arc::new(view.budget.reserve(bytes)?);
    let mut registry = PropertyFunctionRegistry::new();
    for (iri, kind) in [
        (&capture.relations.ranked_iri, Kind::Ranked),
        (&capture.relations.spans_iri, Kind::Spans),
    ] {
        registry.register(
            iri.clone(),
            Arc::new(Relation {
                view,
                kind,
                modes: [BindingPattern::from_bound_positions(
                    kind.arity().total(),
                    [1],
                )],
                generation: text.generation.clone(),
                _configuration: configuration.clone(),
            }),
        );
    }
    ExtensionEnv::over_relations(registry).map_err(|error| {
        view.budget.fail(Error::new(
            "NATIVE_QUERY_FAILURE",
            format!("stored-text extension configuration: {error}"),
        ))
    })
}

struct Relation<'view, 'db> {
    view: &'view StoredRdfView<'db>,
    kind: Kind,
    modes: [BindingPattern; 1],
    generation: Arc<str>,
    _configuration: Arc<Reservation>,
}

impl PropertyFunction for Relation<'_, '_> {
    fn volatility(&self) -> Volatility {
        Volatility::Stable
    }
    fn arity(&self) -> PfArity {
        self.kind.arity()
    }
    fn modes(&self) -> &[BindingPattern] {
        &self.modes
    }
    fn rows_per_invocation(&self, _: BindingPattern) -> u64 {
        let text = self
            .view
            .text_scope
            .as_ref()
            .expect("registered text corpus");
        match self.kind {
            Kind::Ranked => text.documents.len() as u64,
            Kind::Spans => text.tokens.min(u128::from(u64::MAX)) as u64,
        }
    }
    purrdf::sparql::resident_property_open!();
    fn open_admitted<'cursor>(
        &self,
        args: &PfArgs<'_>,
        _: Option<u64>,
        workspace: WorkspaceCapability,
    ) -> Result<WorkspaceCursor<'cursor>, EvalError>
    where
        Self: 'cursor,
    {
        let mut bindings = AdmittedVec::with_capacity(self.kind.arity().total(), &workspace)?;
        for value in args.flattened() {
            bindings.push(value.map(|value| workspace.clone_term(value)).transpose()?)?;
        }
        WorkspaceCursor::new(
            Cursor {
                view: self.view,
                kind: self.kind,
                bindings,
                workspace: workspace.clone(),
                generation: self.generation.clone(),
                terms: None,
                _analysis: None,
                ranked: None,
                next_rank: 0,
                reader: None,
                ordinal: 0,
                posting: None,
                visits: 0,
                stopped: false,
            },
            &workspace,
            "stored-text cursor",
        )
    }
}

struct SpanPosting {
    id: SharedText,
    value: crate::storage::Value,
    next: usize,
    _admission: Reservation,
}

struct Cursor<'view, 'db> {
    view: &'view StoredRdfView<'db>,
    kind: Kind,
    bindings: AdmittedVec<Option<WorkspaceTerm>>,
    workspace: WorkspaceCapability,
    generation: Arc<str>,
    terms: Option<Vec<String>>,
    _analysis: Option<WorkspaceAllocation>,
    ranked: Option<RankedPostings>,
    next_rank: usize,
    reader: Option<PostingReader<'view, 'db>>,
    ordinal: usize,
    posting: Option<SpanPosting>,
    visits: usize,
    stopped: bool,
}

fn needle<'a>(
    bindings: &'a [Option<WorkspaceTerm>],
    workspace: &WorkspaceCapability,
) -> Result<&'a str, EvalError> {
    match bindings.get(1).and_then(Option::as_ref).map(|term| &**term) {
        Some(TermValue::Literal {
            lexical_form,
            datatype,
            ..
        }) if matches!(
            datatype.as_str(),
            XSD_STRING
                | purrdf_core::vocab::rdf::LANG_STRING
                | purrdf_core::vocab::rdf::DIR_LANG_STRING
        ) =>
        {
            Ok(lexical_form)
        }
        _ => Err(NativeDiagnostic::error(
            NativeDiagnosticKind::Function,
            "stored-text needle requires a bound string literal",
            workspace,
        )),
    }
}

fn work(
    view: &StoredRdfView<'_>,
    context: Option<&NativeFnContext<'_>>,
    step: PostingWork,
) -> Result<(), RankFailure> {
    match step {
        PostingWork::Probe { bytes } => view
            .budget
            .request(bytes, true)
            .map_err(|_| RankFailure::Native(EvalError::WorkspaceStopped))?,
        PostingWork::Score => {
            if view.budget.checkpoint().0.is_some() {
                return Err(RankFailure::Native(EvalError::WorkspaceStopped));
            }
        }
    }
    if let Some(context) = context {
        context
            .checkpoint()
            .and_then(|()| context.charge_work(1))
            .map_err(|_| RankFailure::Stopped)?;
    }
    Ok(())
}

impl Cursor<'_, '_> {
    fn prepare(&mut self, context: Option<&NativeFnContext<'_>>) -> Result<(), RankFailure> {
        if self.terms.is_some() {
            return Ok(());
        }
        work(self.view, context, PostingWork::Score)?;
        let input = needle(&self.bindings, &self.workspace)?;
        if let Some(context) = context {
            context
                .charge_work(input.len() as u64)
                .map_err(|_| RankFailure::Stopped)?;
        }
        // The public analyzer is opaque to the caller. Its bounded-input working
        // admission remains live through analysis and the retained lexical terms.
        // It is host admission, independently of the evaluator's scratch law.
        let bytes = (input.len() as u64)
            .checked_mul(8192)
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or(EvalError::WorkspaceBoundOverflow)?;
        self._analysis = Some(self.workspace.charge(bytes)?);
        let text = self.view.text_scope.as_ref().expect("captured text corpus");
        let mut terms = text.analyzer.terms(input).map_err(EvalError::from)?;
        terms.sort_unstable();
        terms.dedup();
        if terms.is_empty() {
            return Err(NativeDiagnostic::error(
                NativeDiagnosticKind::Function,
                "stored-text query requires at least one analyzed term",
                &self.workspace,
            )
            .into());
        }
        work(self.view, context, PostingWork::Score)?;
        self.terms = Some(terms);
        Ok(())
    }

    fn prefix(&self, id: &str) -> Result<AdmittedVec<WorkspaceTerm>, EvalError> {
        let document = self
            .view
            .text_scope
            .as_ref()
            .expect("captured text corpus")
            .documents
            .get(id)
            .expect("selected text document");
        let mut cells = AdmittedVec::with_capacity(self.kind.arity().total(), &self.workspace)?;
        cells.push(self.workspace.iri(&document.rdf_iri)?)?;
        cells.push(
            self.workspace
                .clone_term(self.bindings[1].as_ref().expect("validated needle"))?,
        )?;
        cells.push(self.workspace.literal(id, XSD_STRING)?)?;
        Ok(cells)
    }

    fn ranked_row(
        &mut self,
        context: Option<&NativeFnContext<'_>>,
    ) -> Result<Option<AdmittedPfRow>, RankFailure> {
        if self.ranked.is_none() {
            let text = self.view.text_scope.as_ref().expect("captured text corpus");
            let view = self.view;
            self.ranked = Some(text_index::rank_postings(
                &view.snapshot,
                self.terms.as_ref().expect("analyzed query"),
                CorpusStatistics {
                    documents: text.documents.len() as u64,
                    tokens: text.tokens,
                },
                |id| text.documents.get(id).map(|document| document.length),
                text.scan_budget,
                &self.workspace,
                |step| work(view, context, step),
            )?);
        }
        let ranked = self.ranked.as_ref().expect("ranked postings initialized");
        let Some((score, id)) = ranked.scored.get(self.next_rank) else {
            return Ok(None);
        };
        let score = *score;
        let id = id.clone();
        let matched = ranked
            .candidates
            .get(id.as_str())
            .expect("ranked candidate")
            .len() as u64;
        self.next_rank += 1;
        let mut cells = self.prefix(id.as_str())?;
        let _lexical = self.workspace.charge(256)?;
        let lexical = score.value.to_decimal_lexical();
        cells.push(self.workspace.literal(&lexical, XSD_DECIMAL)?)?;
        cells.push(self.workspace.literal(self.next_rank, XSD_INTEGER)?)?;
        cells.push(self.workspace.literal(matched, XSD_INTEGER)?)?;
        Ok(Some(AdmittedPfRow::from_terms(
            cells.into_iter(),
            &self.workspace,
        )?))
    }

    fn span_row(&self, posting: &SpanPosting) -> Result<AdmittedPfRow, RankFailure> {
        let span = &posting.value["spans"]
            .as_array()
            .expect("validated span list")[posting.next];
        let start = crate::storage::integer(span, "start")?;
        let end = crate::storage::integer(span, "end")?;
        let document = &self
            .view
            .text_scope
            .as_ref()
            .expect("captured text corpus")
            .documents[posting.id.as_str()];
        if start > end || end > document.byte_length {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "stored text span exceeds its original record bytes",
            )
            .into());
        }
        let basis = span["basis"]
            .as_str()
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "text span basis missing"))?;
        let path = match (basis, span.get("path")) {
            ("raw_record", Some(crate::storage::Value::Null)) => "",
            ("decoded_json_scalar", Some(path)) => path
                .as_str()
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "decoded text span path missing"))?,
            _ => {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "text span basis/path violates its profile",
                )
                .into());
            }
        };
        let coarse = span["coarse"]
            .as_bool()
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "text span alignment flag missing"))?;
        let mut cells = self.prefix(posting.id.as_str())?;
        cells.push(self.workspace.literal(
            &self.terms.as_ref().expect("analyzed query")[self.ordinal],
            XSD_STRING,
        )?)?;
        cells.push(self.workspace.literal(start, XSD_INTEGER)?)?;
        cells.push(self.workspace.literal(end, XSD_INTEGER)?)?;
        cells.push(self.workspace.literal(basis, XSD_STRING)?)?;
        cells.push(self.workspace.literal(path, XSD_STRING)?)?;
        cells.push(self.workspace.literal(coarse, XSD_BOOLEAN)?)?;
        Ok(AdmittedPfRow::from_terms(
            cells.into_iter(),
            &self.workspace,
        )?)
    }

    fn spans_row(
        &mut self,
        context: Option<&NativeFnContext<'_>>,
    ) -> Result<Option<AdmittedPfRow>, RankFailure> {
        loop {
            work(self.view, context, PostingWork::Score)?;
            if let Some(posting) = self.posting.as_ref() {
                let spans = posting.value["spans"]
                    .as_array()
                    .expect("validated span list");
                if posting.next < spans.len() {
                    let row = self.span_row(posting)?;
                    self.posting.as_mut().expect("loaded text evidence").next += 1;
                    return Ok(Some(row));
                }
            }
            self.posting = None;
            let terms = self.terms.as_ref().expect("analyzed query");
            if self.ordinal == terms.len() {
                return Ok(None);
            }
            if self.reader.is_none() {
                self.reader = Some(PostingReader::new(
                    &self.view.snapshot,
                    &terms[self.ordinal],
                    &self.workspace,
                )?);
            }
            let view = self.view;
            let text = view.text_scope.as_ref().expect("captured text corpus");
            let workspace = &self.workspace;
            let next = self
                .reader
                .as_mut()
                .expect("posting reader initialized")
                .read_next(
                    |id| text.documents.contains_key(id),
                    text.scan_budget,
                    &mut self.visits,
                    |step| work(view, context, step),
                    |id, frequency| Ok((workspace.authored_text(id)?, frequency)),
                )?;
            let Some((id, frequency)) = next else {
                self.reader = None;
                self.ordinal += 1;
                continue;
            };
            let key = text_index::term_key("evidence", &terms[self.ordinal], id.as_str());
            let EncodedGuard {
                bytes,
                _reservation,
            } = view
                .text_catalog(key.as_bytes())
                .map_err(|_| RankFailure::Native(EvalError::WorkspaceStopped))?;
            let value = crate::storage::parse(&bytes)?;
            if value["term"].as_str() != Some(terms[self.ordinal].as_str())
                || crate::storage::integer(&value, "frequency")? != frequency
                || value["spans"].as_array().map(Vec::len) != Some(frequency as usize)
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "text evidence disagrees with its stored posting",
                )
                .into());
            }
            drop(bytes);
            self.posting = Some(SpanPosting {
                id,
                value,
                next: 0,
                _admission: _reservation,
            });
        }
    }

    fn pull_inner(
        &mut self,
        context: Option<&NativeFnContext<'_>>,
    ) -> Result<Option<AdmittedPfRow>, RankFailure> {
        if self.stopped {
            return Ok(None);
        }
        self.prepare(context)?;
        loop {
            work(self.view, context, PostingWork::Score)?;
            let row = match self.kind {
                Kind::Ranked => self.ranked_row(context)?,
                Kind::Spans => self.spans_row(context)?,
            };
            let Some(row) = row else { return Ok(None) };
            let mut accepted = true;
            for (bound, value) in self.bindings.iter().zip(row.cells()) {
                if let Some(bound) = bound
                    && !self.workspace.terms_equal(bound, value)?
                {
                    accepted = false;
                    break;
                }
            }
            if accepted {
                return Ok(Some(row));
            }
        }
    }

    fn pull(
        &mut self,
        context: Option<&NativeFnContext<'_>>,
    ) -> Result<Option<AdmittedPfRow>, EvalError> {
        match self.pull_inner(context) {
            Ok(row) => Ok(row),
            Err(RankFailure::Stopped) => {
                self.stopped = true;
                Ok(None)
            }
            Err(RankFailure::Native(error)) => Err(error),
            Err(RankFailure::Source(error)) => {
                self.view.budget.fail(error);
                Err(EvalError::WorkspaceStopped)
            }
        }
    }
}

impl PfCursor for Cursor<'_, '_> {
    fn next(&mut self) -> Result<Option<PfRow>, EvalError> {
        self.pull(None)?
            .map(AdmittedPfRow::try_into_resident)
            .transpose()
    }
    fn next_metered(&mut self, context: &NativeFnContext<'_>) -> Result<Option<PfRow>, EvalError> {
        self.pull(Some(context))?
            .map(AdmittedPfRow::try_into_resident)
            .transpose()
    }
    fn next_admitted(&mut self) -> Result<Option<AdmittedPfRow>, EvalError> {
        self.pull(None)
    }
    fn next_admitted_metered(
        &mut self,
        context: &NativeFnContext<'_>,
    ) -> Result<Option<AdmittedPfRow>, EvalError> {
        self.pull(Some(context))
    }
    fn generation(&self) -> IndexGeneration {
        IndexGeneration::Declared(self.generation.clone())
    }
}
