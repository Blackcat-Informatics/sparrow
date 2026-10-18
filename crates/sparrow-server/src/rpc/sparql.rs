// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Typed native query publication; no local query parser or certainty classifier.

use super::*;
use purrdf::{
    SparqlRequest, TermValue,
    sparql::{
        FallibleSparqlError, GovernedEvidence, PartialAnswers, QueryGovernors, ResourceDimension,
        RetainedSparqlResult,
    },
};
use sparrow_core::{
    RdfGraphLayers, RdfReadLimits, SparqlGraphKind, SparqlItem, SparqlRead, SparqlReadPage,
};
use std::borrow::Cow;

struct Request {
    scope: QueryScope,
    text: String,
    base: Option<String>,
    layers: RdfGraphLayers,
    bindings: Vec<(String, TermValue)>,
    source: RdfReadLimits,
    governors: QueryGovernors,
}

pub(super) fn query_scope(
    rpc: &Rpc,
    version: u32,
    store: capnp::text::Reader<'_>,
    input: wire::scope::Reader<'_>,
) -> Result<QueryScope, Error> {
    if version != sparrow_client::SCHEMA_VERSION {
        return Err(Error::new(
            "UNSUPPORTED",
            "native queries require the current discovered schema",
        ));
    }
    if store.len() > 128 {
        return Err(invalid("store identity exceeds 128 bytes"));
    }
    rpc.check_store(store.to_str().map_err(invalid)?)?;
    super::scope(input, rpc).map_err(invalid)?
}

fn request(rpc: &Rpc, input: wire::sparql_query::Reader<'_>) -> Result<Request, Error> {
    let scope = query_scope(
        rpc,
        input.get_schema_version(),
        input.get_store_id().map_err(invalid)?,
        input.get_scope().map_err(invalid)?,
    )?;
    let text = input.get_query().map_err(invalid)?;
    if text.len() > 65536 {
        return Err(Error::new("RESOURCE_LIMIT", "SPARQL query exceeds 64 KiB"));
    }
    let text = text.to_str().map_err(invalid)?.to_owned();
    let base = query_text(
        input.get_base_iri().map_err(invalid)?,
        65536,
        "base IRI exceeds 64 KiB",
    )
    .map_err(invalid)?;
    let layer = input.get_layers().map_err(invalid)?;
    let layers = RdfGraphLayers {
        metadata: layer.get_metadata(),
        diagnostic: layer.get_diagnostic(),
        content: layer.get_content(),
    };
    let entries = input.get_bindings().map_err(invalid)?;
    if entries.len() > 64 {
        return Err(Error::new(
            "RESOURCE_LIMIT",
            "SPARQL admits at most 64 typed bindings",
        ));
    }
    let mut names = Vec::new();
    let mut roots = Vec::new();
    for entry in entries {
        let name = entry.get_name().map_err(invalid)?;
        if name.len() > 128 {
            return Err(invalid("binding name exceeds 128 bytes"));
        }
        names.push(name.to_str().map_err(invalid)?.to_owned());
        roots.push(entry.get_term());
    }
    let values = crate::rdf_wire::decode(input.get_terms().map_err(invalid)?, &roots, true)?;
    let bindings = names.into_iter().zip(values).collect();
    let (source, governors) = query_limits(input.get_limits().map_err(invalid)?)?;
    Ok(Request {
        scope,
        text,
        base,
        layers,
        bindings,
        source,
        governors,
    })
}

pub(super) fn query_limits(
    limits: wire::sparql_limits::Reader<'_>,
) -> Result<(RdfReadLimits, QueryGovernors), Error> {
    let source = RdfReadLimits {
        live_bytes: limits.get_live_bytes(),
        requests: limits.get_requests(),
        io_bytes: limits.get_io_bytes(),
        row_visits: limits.get_row_visits(),
        lease_ms: limits.get_source_lease_ms(),
    };
    if source.live_bytes > 134217728
        || source.requests > 100000
        || source.io_bytes > 134217728
        || source.row_visits > 100000
        || source.lease_ms > 60000
        || limits.get_fuel() > 10000000
        || limits.get_answer_rows() > 100000
        || limits.get_intermediate_cells() > 1000000
        || limits.get_scratch_bytes() > 33554432
    {
        return Err(Error::new(
            "RESOURCE_LIMIT",
            "query limits exceed server ceilings; reduce source/engine budgets",
        ));
    }
    let governors = QueryGovernors::METERED
        .with_fuel(limits.get_fuel())
        .with_max_answers(limits.get_answer_rows())
        .with_max_intermediate_cells(limits.get_intermediate_cells())
        .with_max_scratch_bytes(limits.get_scratch_bytes())
        .with_max_remote_requests(0);
    Ok((source, governors))
}

pub(super) enum Operation {
    Query,
    Explain,
}

pub(super) async fn query(
    rpc: &Rpc,
    input: wire::sparql_query::Reader<'_>,
    mut out: wire::result::Builder<'_, wire::sparql_page::Owned>,
    operation: Operation,
) -> capnp::Result<()> {
    let input = match request(rpc, input) {
        Ok(input) => input,
        Err(error) => {
            failure(out, error);
            return Ok(());
        }
    };
    match operation {
        Operation::Query => {
            let reply = rpc
                .query_work(true, move |store, now, cancellation| {
                    store
                        .sparql_read(&input.read(), now, &cancellation)
                        .map_err(|e| Error::new(e.code, e.message.clone()))
                })
                .await;
            deliver(out, reply);
        }
        Operation::Explain => {
            let reply = rpc
                .query_work(true, move |store, now, cancellation| {
                    store
                        .sparql_explanation_read(&input.read(), now, &cancellation)
                        .map_err(|e| Error::new(e.code, e.message.clone()))
                })
                .await;
            match reply {
                Err(error) => failure(out, error),
                Ok(mut reply) => match explanation_page(out.reborrow().init_ok(), &reply.page) {
                    Ok(()) => reply.delivered = true,
                    Err(error) => failure(out, error),
                },
            }
        }
    }
    Ok(())
}

pub(super) fn deliver(
    mut out: wire::result::Builder<'_, wire::sparql_page::Owned>,
    reply: Result<QueryReply<sparrow_core::SparqlPageBody>, Error>,
) {
    match reply {
        Err(error) => failure(out, error),
        Ok(mut reply) => match page(out.reborrow().init_ok(), &reply.page) {
            Ok(()) => reply.delivered = true,
            Err(error) => failure(out, error),
        },
    }
}

fn dimension(value: ResourceDimension) -> wire::GovernorDimension {
    match value {
        ResourceDimension::Fuel => wire::GovernorDimension::Fuel,
        ResourceDimension::AnswerRows => wire::GovernorDimension::AnswerRows,
        ResourceDimension::IntermediateCells => wire::GovernorDimension::IntermediateCells,
        ResourceDimension::ScratchBytes => wire::GovernorDimension::ScratchBytes,
        ResourceDimension::RemoteRequests => wire::GovernorDimension::RemoteRequests,
        ResourceDimension::UdfDepth => wire::GovernorDimension::UdfDepth,
        ResourceDimension::Pages => wire::GovernorDimension::Pages,
        ResourceDimension::Bytes => wire::GovernorDimension::Bytes,
    }
}

fn governors(
    mut out: wire::native_governor_receipt::Builder<'_>,
    value: &purrdf::sparql::GovernorEvidence,
) -> Result<(), Error> {
    use purrdf::sparql::{StopCause, TrippedGovernor};
    out.set_profile(purrdf::sparql::GOVERNOR_PROFILE_ID);
    let mut resources = out
        .reborrow()
        .init_resources(ResourceDimension::COUNT as u32);
    for (index, d) in ResourceDimension::ALL.into_iter().enumerate() {
        let mut use_ = resources.reborrow().get(index as u32);
        use_.set_dimension(dimension(d));
        use_.set_consumed(value.consumed.get(d));
        use_.set_ceiling(value.limits.get(d));
    }
    let mut stop = out.reborrow().init_stop();
    match value.tripped {
        None => stop.set_absent(()),
        Some(TrippedGovernor::Budget {
            dimension: d,
            limit,
            consumed,
        }) => {
            let mut trip = stop.init_budget();
            trip.set_dimension(dimension(d));
            trip.set_limit(limit);
            trip.set_consumed(consumed);
        }
        Some(TrippedGovernor::Refused {
            dimension: d,
            limit,
            estimate,
        }) => {
            let mut trip = stop.init_refused();
            trip.set_dimension(dimension(d));
            trip.set_limit(limit);
            trip.set_estimate(estimate);
        }
        Some(TrippedGovernor::Stopped { cause }) => stop.set_stopped(match cause {
            StopCause::Cancelled => wire::StopCause::Cancelled,
            StopCause::Deadline => wire::StopCause::Deadline,
        }),
        Some(_) => {
            return Err(Error::new(
                "UNSUPPORTED",
                "native governor stop lacks a wire variant",
            ));
        }
    }
    let mut failures = out.reborrow().init_silenced(value.silenced().len() as u32);
    for (index, value) in value.silenced().iter().enumerate() {
        use purrdf::sparql::{SilencedKind as K, SilencedTarget};
        let mut target = failures.reborrow().get(index as u32);
        target.set_target_kind(match &value.target {
            SilencedTarget::Service { .. } => wire::SilencedTargetKind::Service,
            SilencedTarget::Load { .. } => wire::SilencedTargetKind::Load,
            _ => {
                return Err(Error::new(
                    "UNSUPPORTED",
                    "native silent target lacks a wire variant",
                ));
            }
        });
        target.set_target(value.target.name());
        target.set_message(&value.message);
        target.set_kind(match value.kind {
            K::Transport => wire::SilencedFailureKind::Transport,
            K::Decode => wire::SilencedFailureKind::Decode,
            K::Disabled => wire::SilencedFailureKind::Disabled,
            K::Unconfigured => wire::SilencedFailureKind::Unconfigured,
            K::Denied => wire::SilencedFailureKind::Denied,
            K::HostDenied => wire::SilencedFailureKind::HostDenied,
            K::NotAnIri => wire::SilencedFailureKind::NotAnIri,
            K::Fault => wire::SilencedFailureKind::Fault,
            _ => {
                return Err(Error::new(
                    "UNSUPPORTED",
                    "native silent failure lacks a wire variant",
                ));
            }
        });
    }
    let mut errors = out.init_expression_errors(value.expression_errors().len() as u32);
    for (index, (code, count)) in value.expression_errors().iter().enumerate() {
        let mut target = errors.reborrow().get(index as u32);
        target.set_code(code.qname());
        target.set_count(*count);
    }
    Ok(())
}

enum Item {
    Solution(Vec<Option<usize>>),
    Boolean(bool),
    Graph {
        auxiliary: bool,
        kind: SparqlGraphKind,
        terms: [Option<usize>; 4],
    },
}

fn data<'a>(
    mut out: wire::sparql_data::Builder<'_>,
    page: &'a SparqlReadPage,
    result: &'a RetainedSparqlResult,
) -> Result<(), Error> {
    out.set_form(if result.solutions().is_some() {
        wire::SparqlForm::Select
    } else if result.boolean().is_some() {
        wire::SparqlForm::Ask
    } else {
        wire::SparqlForm::Graph
    });
    let variables = result.solutions().map_or(&[][..], |(vars, _)| vars);
    let mut vars = out.reborrow().init_variables(variables.len() as u32);
    for (index, var) in variables.iter().enumerate() {
        vars.set(index as u32, var);
    }
    let mut roots = Vec::<Cow<'a, TermValue>>::new();
    let mut items = Vec::new();
    items
        .try_reserve_exact(page.body.items().count())
        .map_err(|_| Error::new("RESOURCE_LIMIT", "graph page item allocation refused"))?;
    let mut add = |value: Cow<'a, TermValue>| -> Result<usize, Error> {
        roots
            .try_reserve(1)
            .map_err(|_| Error::new("RESOURCE_LIMIT", "graph page term allocation refused"))?;
        let index = roots.len();
        roots.push(value);
        Ok(index)
    };
    for item in page.body.items() {
        items.push(match item {
            SparqlItem::Solution(row) => {
                let mut cells = Vec::new();
                cells.try_reserve_exact(row.len()).map_err(|_| {
                    Error::new("RESOURCE_LIMIT", "solution cell allocation refused")
                })?;
                for value in row {
                    cells.push(value.as_ref().map(|v| add(Cow::Borrowed(v))).transpose()?);
                }
                Item::Solution(cells)
            }
            SparqlItem::Boolean(value) => Item::Boolean(value),
            SparqlItem::Graph {
                dataset,
                auxiliary,
                kind,
                terms,
            } => {
                let mut values = [None; 4];
                for (index, term) in terms.into_iter().enumerate() {
                    values[index] = term
                        .map(|term| add(Cow::Owned(dataset.term_value(term))))
                        .transpose()?;
                }
                Item::Graph {
                    auxiliary,
                    kind,
                    terms: values,
                }
            }
        });
    }
    let refs = roots.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
    let ids = crate::rdf_wire::encode(&refs, |len| out.reborrow().init_terms(len))?;
    let mut rows = out.init_items(items.len() as u32);
    for (index, item) in items.into_iter().enumerate() {
        let mut target = rows.reborrow().get(index as u32);
        let optional = |out: wire::optional_rdf_term::Builder<'_>, value: Option<usize>| {
            sparrow_client::encode_optional(
                out,
                value,
                |mut out, v| out.set_value(ids[v]),
                |mut out| out.set_unbound(()),
            );
        };
        match item {
            Item::Solution(cells) => {
                let mut row = target.init_solution().init_cells(cells.len() as u32);
                for (i, cell) in cells.into_iter().enumerate() {
                    optional(row.reborrow().get(i as u32), cell);
                }
            }
            Item::Boolean(value) => target.set_boolean(value),
            Item::Graph {
                auxiliary,
                kind,
                terms,
            } => {
                let mut graph = target.init_graph();
                graph.set_auxiliary(auxiliary);
                let required = |index: usize| {
                    terms[index].map(|index| ids[index]).ok_or_else(|| {
                        Error::new("STORAGE_FAILURE", "native graph item lacks a required term")
                    })
                };
                match kind {
                    SparqlGraphKind::NamedGraph => graph.set_declaration(required(0)?),
                    SparqlGraphKind::Quad => {
                        let mut row = graph.init_quad();
                        row.set_subject(required(0)?);
                        row.set_predicate(required(1)?);
                        row.set_object(required(2)?);
                        optional(row.init_graph(), terms[3]);
                    }
                    SparqlGraphKind::Reifier => {
                        let mut row = graph.init_reifier();
                        row.set_reifier(required(0)?);
                        row.set_triple(required(1)?);
                        optional(row.init_graph(), terms[2]);
                    }
                    SparqlGraphKind::Annotation => {
                        let mut row = graph.init_annotation();
                        row.set_reifier(required(0)?);
                        row.set_predicate(required(1)?);
                        row.set_object(required(2)?);
                        optional(row.init_graph(), terms[3]);
                    }
                }
            }
        }
    }
    Ok(())
}

pub(super) fn page(
    mut out: wire::sparql_page::Builder<'_>,
    page: &SparqlReadPage,
) -> Result<(), Error> {
    let run = page.body.captured();
    let evidence = match &run.outcome {
        Ok(value) => &value.evidence,
        Err(error) => error.evidence(),
    };
    receipt(
        out.reborrow(),
        page,
        run,
        evidence,
        run.outcome.as_ref().err(),
    )?;
    out.set_operation(wire::NativeReadOperation::Query);
    let mut outcome = out.init_outcome();
    match &run.outcome {
        Ok(value) => data(outcome.init_complete(), page, &value.result)?,
        Err(FallibleSparqlError::BudgetExhausted { partial, .. }) => match partial {
            PartialAnswers::Certain(value) => {
                let mut out = outcome.init_certain();
                out.set_positional_prefix(value.is_positional_prefix());
                data(out.init_data(), page, value.result())?;
            }
            PartialAnswers::AtMost(value) => {
                let mut out = outcome.init_at_most();
                out.set_positional_prefix(value.is_positional_prefix());
                data(out.init_data(), page, value.result())?;
            }
            PartialAnswers::Unknown(barrier) => outcome.set_unknown(barrier.operator()),
        },
        Err(error) => {
            native_failure(outcome.reborrow().init_failure(), error)?;
        }
    }
    Ok(())
}

fn receipt<Body, Outcome>(
    mut out: wire::sparql_page::Builder<'_>,
    page: &sparrow_core::CatalogReadPage<Body>,
    run: &sparrow_core::ScopedSparqlResult<Outcome>,
    evidence: &GovernedEvidence<sparrow_core::RdfReadEvidence>,
    error: Option<
        &FallibleSparqlError<
            sparrow_core::RdfReadError,
            GovernedEvidence<sparrow_core::RdfReadEvidence>,
        >,
    >,
) -> Result<(), Error> {
    match &run.correlation {
        Some(capture) => crate::correlation_wire::encode(
            out.reborrow().init_correlation().init_value(),
            capture,
            &run.store_id,
        )
        .map_err(|error| Error::new("STORAGE_FAILURE", error.to_string()))?,
        None => out.reborrow().init_correlation().set_absent(()),
    }
    query_scope_receipt(out.reborrow().init_scope(), &run.scope)
        .map_err(|error| Error::new("STORAGE_FAILURE", format!("frozen graph scope: {error}")))?;
    match error {
        Some(FallibleSparqlError::Query { diagnostic, .. }) => crate::rdf_wire::diagnostic::encode(
            out.reborrow().init_diagnostic().init_value(),
            diagnostic.diagnostic(),
        )?,
        _ => out.reborrow().init_diagnostic().set_absent(()),
    }
    catalog_receipt(out.reborrow().init_page(), page);
    out.set_sequence(run.sequence);
    let mut layers = out.reborrow().init_layers();
    layers.set_metadata(run.layers.metadata);
    layers.set_diagnostic(run.layers.diagnostic);
    layers.set_content(run.layers.content);
    let c = &run.coverage;
    let mut coverage = out.reborrow().init_coverage();
    coverage.set_visited(c.visited);
    coverage.set_known_selected_records(c.selected_records);
    coverage.set_artifacts_visited(c.artifacts_visited);
    coverage.set_known_selected_artifacts(c.selected_artifacts);
    coverage.set_artifact_diagnostic_unknown(c.artifact_diagnostic_unknown);
    coverage.set_content_ready(c.content_ready);
    coverage.set_content_failed(c.content_failed);
    coverage.set_content_not_applicable(c.content_not_applicable);
    coverage.set_catalog_entries(c.catalog_entries as u64);
    coverage.set_complete(
        run.correlation
            .as_ref()
            .map_or_else(|| c.complete(), |capture| capture.complete(c)),
    );
    coverage.set_event_time_known(c.event_time_known);
    let mut known = coverage
        .reborrow()
        .init_diagnostic_known(sparrow_core::DiagnosticDimension::ALL.len() as u32);
    for (index, dimension) in sparrow_core::DiagnosticDimension::ALL
        .into_iter()
        .enumerate()
    {
        let mut target = known.reborrow().get(index as u32);
        target.set_dimension(crate::diagnostics::wire_dimension(dimension));
        target.set_known(c.diagnostic_known.get(&dimension).copied().unwrap_or(0));
    }
    if c.diagnostics.complete(false) {
        coverage
            .reborrow()
            .init_selected_records()
            .set_known(c.selected_records);
    } else {
        coverage.reborrow().init_selected_records().set_unknown(());
    }
    let mut d = coverage.init_diagnostics();
    d.set_evaluated(c.diagnostics.evaluated);
    d.set_matched(c.diagnostics.matched);
    d.set_excluded(c.diagnostics.excluded);
    d.set_unknown(c.diagnostics.unknown);
    d.set_membership_complete(c.diagnostics.complete(false));
    governors(out.reborrow().init_governors(), &evidence.governors)?;
    let e = &evidence.view;
    let mut target = out.reborrow().init_evidence();
    target.set_requests(e.requests);
    target.set_admitted_bytes(e.admitted_bytes);
    target.set_row_visits(e.row_visits);
    target.set_live_bytes(e.live_bytes);
    target.set_peak_live_bytes(e.peak_live_bytes);
    let mut limits = out.reborrow().init_limits();
    limits.set_source_profile(sparrow_core::RDF_SOURCE_ADMISSION_PROFILE);
    limits.set_page_bytes(256 * 1024);
    limits.set_metadata_bytes(64 * 1024);
    limits.set_result_handles(8);
    limits.set_result_bytes(8 * 1024 * 1024);
    limits.set_result_lease_ms(60000);
    limits.set_scope_catalog_bytes(32 * 1024 * 1024);
    limits.set_scope_receipt_bytes(sparrow_core::QUERY_SCOPE_WORKSPACE_BYTES as u64);
    limits.set_catalog_expansion_factor(64);
    limits.set_query_text_bytes(65536);
    limits.set_binding_bytes(crate::rdf_wire::MAX_BYTES as u64);
    limits.set_binding_nodes(crate::rdf_wire::MAX_NODES as u32);
    let mut source = limits.init_source();
    source.set_live_bytes(run.limits.live_bytes);
    source.set_requests(run.limits.requests);
    source.set_io_bytes(run.limits.io_bytes);
    source.set_row_visits(run.limits.row_visits);
    source.set_source_lease_ms(run.limits.lease_ms);
    source.set_fuel(evidence.governors.limits.get(ResourceDimension::Fuel));
    source.set_answer_rows(evidence.governors.limits.get(ResourceDimension::AnswerRows));
    source.set_intermediate_cells(
        evidence
            .governors
            .limits
            .get(ResourceDimension::IntermediateCells),
    );
    source.set_scratch_bytes(
        evidence
            .governors
            .limits
            .get(ResourceDimension::ScratchBytes),
    );
    Ok(())
}

fn native_failure(
    mut target: wire::failure::Builder<'_>,
    error: &FallibleSparqlError<
        sparrow_core::RdfReadError,
        GovernedEvidence<sparrow_core::RdfReadEvidence>,
    >,
) -> Result<(), Error> {
    target.set_retryable(false);
    match error {
        FallibleSparqlError::BudgetExhausted { .. } => {
            target.set_code("RESOURCE_LIMIT");
            target.set_message(
                "native execution exhausted a governor before a ledger could be published",
            );
        }
        FallibleSparqlError::Query { diagnostic, .. } => {
            target.set_code(&diagnostic.diagnostic().code);
            target.set_message(diagnostic.diagnostic().to_string());
        }
        FallibleSparqlError::Operational { error, .. } => {
            target.set_code(error.code);
            target.set_message(&error.message);
        }
        FallibleSparqlError::AllocationFailed { construct, .. } => {
            target.set_code("RESOURCE_LIMIT");
            target.set_message(construct);
        }
        FallibleSparqlError::ControlFailure { failure, .. } => {
            target.set_code(failure.diagnostic_code());
            target.set_message(failure.to_string());
        }
        _ => {
            return Err(Error::new(
                "UNSUPPORTED",
                "native query failure lacks a wire variant",
            ));
        }
    }
    Ok(())
}

impl Request {
    fn read(&self) -> SparqlRead<'_> {
        SparqlRead {
            scope: &self.scope,
            layers: self.layers,
            request: SparqlRequest {
                query: &self.text,
                base_iri: self.base.as_deref(),
                substitutions: &self.bindings,
            },
            governors: &self.governors,
            limits: self.source,
        }
    }
}

pub(super) fn explanation_page(
    mut out: wire::sparql_page::Builder<'_>,
    page: &sparrow_core::SparqlExplanationReadPage,
) -> Result<(), Error> {
    let run = page.body.captured();
    let evidence = match &run.outcome {
        Ok((_, evidence)) => evidence,
        Err(error) => error.evidence(),
    };
    receipt(
        out.reborrow(),
        page,
        run,
        evidence,
        run.outcome.as_ref().err(),
    )?;
    out.set_operation(wire::NativeReadOperation::Explain);
    match &run.outcome {
        Err(error) => native_failure(out.init_outcome().init_failure(), error)?,
        Ok((value, _)) => {
            let mut data = out.init_outcome().init_explanation();
            let profile = value.profile();
            data.set_profile(profile.id);
            data.set_version(profile.version);
            data.set_digest(&profile.digest);
            data.set_stop_poll_fuel(profile.stop_poll_fuel);
            let mut schedule = data
                .reborrow()
                .init_schedule(purrdf::sparql::CHARGE_SCHEDULE.len() as u32);
            for (index, (label, cost)) in purrdf::sparql::CHARGE_SCHEDULE.into_iter().enumerate() {
                let mut row = schedule.reborrow().get(index as u32);
                row.set_label(label);
                row.set_cost(cost);
            }
            let indexes = value.statement_indexes();
            let mut target = data.reborrow().init_indexes();
            target.set_reifier_targets(indexes.reifier_targets);
            target.set_reifier_components(indexes.reifier_components);
            target.set_annotation_values(indexes.annotation_values);
            // Core validates that QueryOptions::default injected no registry.
            data.reborrow().init_relations(0);
            data.reborrow().init_aggregates(0);
            let mut items = data.init_items(page.body.items().count() as u32);
            for (index, item) in page.body.items().enumerate() {
                explanation_item(items.reborrow().get(index as u32), item);
            }
        }
    }
    Ok(())
}

fn explanation_item(
    mut out: wire::native_explanation_item::Builder<'_>,
    item: sparrow_core::SparqlExplanationItem<'_>,
) {
    use sparrow_core::SparqlExplanationItem as I;
    use wire::{optional_limit::Builder as Limit, optional_text::Builder as Text};
    match item {
        I::JoinOrder(text) => out.set_join_order(text),
        I::Statistic(text) => out.set_statistic(text),
        I::Node(node) => {
            let mut out = out.init_node();
            out.set_ordinal(node.ordinal as u64);
            out.set_depth(node.depth as u64);
            out.set_label(node.label);
            out.set_fuel_total(node.fuel_total());
            out.set_rows(node.rows);
            out.set_cells(node.cells);
            let mut fuel = out.reborrow().init_fuel(node.fuel.len() as u32);
            for (index, units) in node.fuel.into_iter().enumerate() {
                fuel.set(index as u32, units);
            }
            match &node.estimate {
                None => out.init_estimate().set_absent(()),
                Some(value) => {
                    let mut out = out.init_estimate().init_value();
                    out.set_rows(value.rows);
                    out.set_peak_rows(value.peak_rows);
                    out.set_columns(value.columns);
                    out.set_peak_cells(value.peak_cells());
                }
            }
        }
        I::Shape(shape) => {
            let mut out = out.init_shape();
            explanation_optional(
                out.reborrow().init_node(),
                shape.node.map(|n| n as u64),
                Limit::set_value,
                Limit::set_absent,
            );
            explanation_optional(
                out.reborrow().init_operator(),
                shape.operator,
                Text::set_value,
                Text::set_absent,
            );
            explanation_optional(
                out.reborrow().init_provider(),
                shape.provider,
                Text::set_value,
                Text::set_absent,
            );
            explanation_optional(
                out.reborrow().init_registry(),
                shape.registry.map(|id| id.as_u64()),
                Limit::set_value,
                Limit::set_absent,
            );
            out.set_source_visits(shape.work.source_visits);
            out.set_transitions(shape.work.transitions);
            out.set_output_rows(shape.work.output_rows);
            match shape.estimate.access {
                None => out.init_estimate().set_absent(()),
                Some(cost) => {
                    let mut out = out.init_estimate().init_value();
                    out.set_work(cost.work());
                    out.set_rows(cost.rows());
                    explanation_optional(
                        out.reborrow().init_pages(),
                        cost.pages(),
                        Limit::set_value,
                        Limit::set_absent,
                    );
                    explanation_optional(
                        out.reborrow().init_cardinality_fallback(),
                        cost.cardinality_fallback(),
                        Limit::set_value,
                        Limit::set_absent,
                    );
                    out.set_residency(match cost.residency() {
                        None => wire::NativeReadResidency::Absent,
                        Some(purrdf::dataset_view::ReadResidency::Resident) => {
                            wire::NativeReadResidency::Resident
                        }
                        Some(purrdf::dataset_view::ReadResidency::NonResident) => {
                            wire::NativeReadResidency::NonResident
                        }
                        Some(purrdf::dataset_view::ReadResidency::Mixed) => {
                            wire::NativeReadResidency::Mixed
                        }
                    });
                }
            }
        }
    }
}

fn explanation_optional<B, T>(
    mut out: B,
    value: Option<T>,
    present: impl FnOnce(&mut B, T),
    absent: impl FnOnce(&mut B, ()),
) {
    match value {
        Some(value) => present(&mut out, value),
        None => absent(&mut out, ()),
    }
}
