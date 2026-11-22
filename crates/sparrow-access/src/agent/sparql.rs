// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Native result rendering shared by query and cursor-only continuation.

use super::{Result, failure, wire};
use purrdf::{SparqlResult, TermFactory, ir::RdfDatasetBuilder};
use purrdf_lex::json::{self, Object, Value};

pub(crate) struct Input {
    explain: bool,
    controls: Controls,
    query: String,
    base: Option<String>,
    layers: [bool; 3],
    bindings: Vec<(String, purrdf::TermValue)>,
    text_relations: Option<sparrow_core::SparqlTextRelations>,
}

pub(super) struct Controls {
    pub store: String,
    pub options: super::Options,
    limits: [Option<u64>; 9],
}

const LIMIT_NAMES: [&str; 9] = [
    "fuel",
    "answer_rows",
    "intermediate_cells",
    "scratch_bytes",
    "live_bytes",
    "requests",
    "io_bytes",
    "row_visits",
    "source_lease_ms",
];

impl Input {
    pub(super) fn decode(value: &Value) -> Result<Self> {
        use json::record::{FromJson, Record};
        let invalid = |e: json::record::DecodeError| super::invalid(e.to_string());
        let decode = || -> std::result::Result<Self, json::record::DecodeError> {
            let mut fields = Record::new(value, "SPARQL request")?;
            let controls = Controls::decode(&mut fields)?;
            let query = fields.required("query")?;
            let base = fields.optional("base_iri")?;
            let mut layers = [true; 3];
            if let Some(value) = fields.raw("layers")? {
                let mut layer = Record::new(value, "RDF layers")?;
                for (index, name) in ["metadata", "diagnostic", "content"]
                    .into_iter()
                    .enumerate()
                {
                    if let Some(value) = layer.optional::<bool>(name)? {
                        layers[index] = value;
                    }
                }
                layer.deny_unknown()?;
            }
            let text_relations = match fields.raw("text_relations")? {
                None => None,
                Some(value) => {
                    let mut relation = Record::new(value, "stored-text relations")?;
                    let ranked_iri: String = relation.required("ranked_iri")?;
                    let spans_iri: String = relation.required("spans_iri")?;
                    relation.deny_unknown()?;
                    if ranked_iri.len() > sparrow_core::TEXT_RELATION_IRI_BYTES
                        || spans_iri.len() > sparrow_core::TEXT_RELATION_IRI_BYTES
                    {
                        return Err(json::record::DecodeError::custom(
                            "text relation IRI exceeds 1024 UTF-8 bytes",
                        ));
                    }
                    Some(sparrow_core::SparqlTextRelations {
                        ranked_iri,
                        spans_iri,
                    })
                }
            };
            let mut bindings = Vec::new();
            if let Some(value) = fields.raw("bindings")? {
                let values = Vec::<Value>::from_json(value)?;
                if values.len() > 64 {
                    return Err(json::record::DecodeError::custom(
                        "at most 64 bindings are admitted",
                    ));
                }
                let limits = purrdf::projections::ProjectionLimits::new(
                    1,
                    crate::codec::RDF_MAX_BYTES,
                    crate::codec::RDF_MAX_BYTES,
                    crate::codec::RDF_MAX_BYTES,
                    16,
                )
                .map_err(json::record::DecodeError::custom)?;
                for value in values {
                    let mut entry = Record::new(&value, "typed binding")?;
                    let name: String = entry.required("name")?;
                    if name.len() > 128 {
                        return Err(json::record::DecodeError::custom(
                            "binding name exceeds 128 bytes",
                        ));
                    }
                    let term = entry.raw("term")?.ok_or_else(|| {
                        json::record::DecodeError::custom(
                            "binding requires a canonical PurRDF term",
                        )
                    })?;
                    // A tool argument is a JSON object, not a canonical byte
                    // artifact. Native typed decoding and validation preserve
                    // term identity independently of member order/whitespace.
                    let term = purrdf::projections::ProjectionTerm::from_json(term)?;
                    bindings.push((
                        name,
                        term.to_term_value(limits)
                            .map_err(json::record::DecodeError::custom)?,
                    ));
                    entry.deny_unknown()?;
                }
            }
            fields.deny_unknown()?;
            Ok(Self {
                explain: false,
                controls,
                query,
                base,
                layers,
                bindings,
                text_relations,
            })
        };
        decode().map_err(invalid)
    }

    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        self.controls.require(client).await?;
        let mut message = capnp::message::Builder::new_default();
        let mut query = message.init_root::<wire::sparql_query::Builder<'_>>();
        query.set_store_id(&self.controls.store);
        query.set_schema_version(sparrow_client::SCHEMA_VERSION);
        query.set_query(&self.query);
        super::optional(query.reborrow().init_base_iri(), &self.base);
        super::fill_scope(query.reborrow().init_scope(), &self.controls.options)?;
        let mut layers = query.reborrow().init_layers();
        layers.set_metadata(self.layers[0]);
        layers.set_diagnostic(self.layers[1]);
        layers.set_content(self.layers[2]);
        match &self.text_relations {
            Some(value) => crate::codec::encode_text_relations(
                query.reborrow().init_text_relations().init_value(),
                value,
            ),
            None => query.reborrow().init_text_relations().set_absent(()),
        }
        let refs = self
            .bindings
            .iter()
            .map(|(_, term)| term)
            .collect::<Vec<_>>();
        let ids = crate::codec::encode_rdf(&refs, |len| query.reborrow().init_terms(len))
            .map_err(|e| super::invalid(e.to_string()))?;
        let mut bindings = query.reborrow().init_bindings(self.bindings.len() as u32);
        for (index, (name, _)) in self.bindings.iter().enumerate() {
            let mut binding = bindings.reborrow().get(index as u32);
            binding.set_name(name);
            binding.set_term(ids[index]);
        }
        self.controls.fill_limits(query.init_limits());
        let query = message.get_root_as_reader::<wire::sparql_query::Reader<'_>>()?;
        if self.explain {
            let mut call = client.sparql_explain_request();
            call.get().set_query(query)?;
            let response = call.send().promise.await?;
            render_reply(response.get()?.get_result()?)
        } else {
            let mut call = client.sparql_query_request();
            call.get().set_query(query)?;
            let response = call.send().promise.await?;
            render_reply(response.get()?.get_result()?)
        }
    }

    pub(super) fn explanation(mut self) -> Self {
        self.explain = true;
        self
    }
}

fn render_reply(value: wire::result::Reader<'_, wire::sparql_page::Owned>) -> Result<Value> {
    match value.which()? {
        wire::result::Ok(value) => render(value?),
        wire::result::Error(error) => Err(failure(error?)),
    }
}

impl Controls {
    pub(super) fn decode(
        fields: &mut json::record::Record<'_>,
    ) -> std::result::Result<Self, json::record::DecodeError> {
        let store = fields.required("store_id")?;
        let mut options = super::Options::default();
        if let Some(value) = fields.raw("scope")? {
            options.selection = Box::new(super::selection::decode(value)?);
        }
        options.source = fields.optional("source")?;
        for (name, target) in [
            ("received_from", &mut options.from),
            ("received_before", &mut options.before),
        ] {
            if let Some(value) = fields.optional::<String>(name)? {
                *target = sparrow_core::parse_instant(&value)
                    .map_err(json::record::DecodeError::custom)?;
            }
        }
        if let Some(value) = fields.optional::<u32>("page_size")? {
            options.limit = value;
        }
        if let Some(value) = fields.optional::<u32>("scan_budget")? {
            options.scan_budget = value;
        }
        options.require_complete_index =
            fields.optional("require_complete_index")?.unwrap_or(false);
        options
            .validate()
            .map_err(json::record::DecodeError::custom)?;
        let mut limits = [None; 9];
        if let Some(value) = fields.raw("limits")? {
            let mut input = json::record::Record::new(value, "native query limits")?;
            for (index, name) in LIMIT_NAMES.into_iter().enumerate() {
                limits[index] = input.raw(name)?.map(super::decimal_u64).transpose()?;
            }
            input.deny_unknown()?;
        }
        Ok(Self {
            store,
            options,
            limits,
        })
    }

    pub(super) async fn require(&self, client: &wire::sparrow::Client) -> Result<()> {
        self.options.selection.require_supported(client).await?;
        sparrow_client::Discovery::fetch(client).await?;
        Ok(())
    }

    pub(super) fn fill_limits(&self, mut limits: wire::sparql_limits::Builder<'_>) {
        for (index, value) in self.limits.into_iter().enumerate() {
            if let Some(value) = value {
                match index {
                    0 => limits.set_fuel(value),
                    1 => limits.set_answer_rows(value),
                    2 => limits.set_intermediate_cells(value),
                    3 => limits.set_scratch_bytes(value),
                    4 => limits.set_live_bytes(value),
                    5 => limits.set_requests(value),
                    6 => limits.set_io_bytes(value),
                    7 => limits.set_row_visits(value),
                    8 => limits.set_source_lease_ms(value),
                    _ => unreachable!(),
                }
            }
        }
    }
}

pub(super) fn schema() -> Value {
    let string = || Value::from(Object::new().with("type", "string"));
    let mut limits = Object::new();
    for name in LIMIT_NAMES {
        limits = limits.with(
            name,
            Object::new()
                .with("type", "string")
                .with("pattern", "^[0-9]+$"),
        );
    }
    let mut properties = Object::new()
        .with("store_id", string())
        .with("query", string())
        .with("base_iri", string())
        .with("text_relations", Object::new()
            .with("type", "object")
            .with("additionalProperties", false)
            .with("required", vec!["ranked_iri", "spans_iri"])
            .with("description", "Select both absolute IRIs. Call ranked_iri as ?doc <iri> (needle ?record_id ?score ?rank ?matched), or spans_iri as ?doc <iri> (needle ?record_id ?term ?start ?end ?basis ?path ?coarse). The bound string needle uses native lexical analysis and matches any analyzed term. Scores and 1-based ranks use the captured covered corpus, including bound-document calls. Span [start,end) offsets are record-local bytes; decoded scalar spans can be coarse. record_id is accepted by bytes_read; doc joins stored metadata.")
            .with("properties", Object::new()
                .with("ranked_iri", Object::new().with("type", "string").with("maxLength", 1024_u32))
                .with("spans_iri", Object::new().with("type", "string").with("maxLength", 1024_u32))))
        .with("scope", super::selection::schema())
        .with("source", string())
        .with("received_from", string())
        .with("received_before", string())
        .with(
            "require_complete_index",
            Object::new().with("type", "boolean"),
        )
        .with(
            "limits",
            Object::new()
                .with("type", "object")
                .with("additionalProperties", false)
                .with("properties", limits),
        )
        .with(
            "layers",
            Object::new()
                .with("type", "object")
                .with("additionalProperties", false)
                .with(
                    "properties",
                    Object::new()
                        .with("metadata", Object::new().with("type", "boolean"))
                        .with("diagnostic", Object::new().with("type", "boolean"))
                        .with("content", Object::new().with("type", "boolean")),
                ),
        )
        .with(
            "bindings",
            Object::new()
                .with("type", "array")
                .with("maxItems", 64_u32)
                .with(
                    "items",
                    Object::new()
                        .with("type", "object")
                        .with("additionalProperties", false)
                        .with("required", vec!["name", "term"])
                        .with(
                            "properties",
                            Object::new()
                                .with("name", string())
                                .with("term", Object::new().with("type", "object").with("description", "PurRDF projection term, tagged by kind: iri, blank, literal or triple. Example: {\"kind\":\"iri\",\"value\":\"urn:example\"}. Native validation checks datatype/language/direction and triple positions.")),
                        ),
                ),
        );
    for (name, maximum) in [("page_size", 1000_u32), ("scan_budget", 100000)] {
        properties = properties.with(
            name,
            Object::new()
                .with("type", "integer")
                .with("minimum", 1_u32)
                .with("maximum", maximum),
        );
    }
    Object::new()
        .with("type", "object")
        .with("additionalProperties", false)
        .with("required", vec!["store_id", "query"])
        .with("properties", properties)
        .into()
}

fn protocol(error: impl std::fmt::Display) -> super::AgentError {
    super::AgentError {
        code: "PROTOCOL_FAILURE".into(),
        message: error.to_string(),
        retryable: false,
    }
}
fn optional(value: wire::optional_rdf_term::Reader<'_>) -> Result<Option<u32>> {
    Ok(match value.which()? {
        wire::optional_rdf_term::Unbound(()) => None,
        wire::optional_rdf_term::Value(id) => Some(id),
    })
}

enum Shape {
    Solution(Vec<Option<u32>>),
    Boolean(bool),
    Graph {
        auxiliary: bool,
        kind: sparrow_core::SparqlGraphKind,
        terms: Vec<Option<u32>>,
    },
}
fn shape(value: wire::sparql_item::Reader<'_>) -> Result<Shape> {
    use sparrow_core::SparqlGraphKind as K;
    Ok(match value.which()? {
        wire::sparql_item::Solution(value) => Shape::Solution(
            value?
                .get_cells()?
                .iter()
                .map(optional)
                .collect::<Result<Vec<_>>>()?,
        ),
        wire::sparql_item::Boolean(value) => Shape::Boolean(value),
        wire::sparql_item::Graph(value) => {
            let value = value?;
            let auxiliary = value.get_auxiliary();
            let (kind, terms) = match value.which()? {
                wire::sparql_graph_item::Declaration(id) => (K::NamedGraph, vec![Some(id)]),
                wire::sparql_graph_item::Quad(row) => {
                    let row = row?;
                    (
                        K::Quad,
                        vec![
                            Some(row.get_subject()),
                            Some(row.get_predicate()),
                            Some(row.get_object()),
                            optional(row.get_graph()?)?,
                        ],
                    )
                }
                wire::sparql_graph_item::Reifier(row) => {
                    let row = row?;
                    (
                        K::Reifier,
                        vec![
                            Some(row.get_reifier()),
                            Some(row.get_triple()),
                            optional(row.get_graph()?)?,
                        ],
                    )
                }
                wire::sparql_graph_item::Annotation(row) => {
                    let row = row?;
                    (
                        K::Annotation,
                        vec![
                            Some(row.get_reifier()),
                            Some(row.get_predicate()),
                            Some(row.get_object()),
                            optional(row.get_graph()?)?,
                        ],
                    )
                }
            };
            Shape::Graph {
                auxiliary,
                kind,
                terms,
            }
        }
    })
}

fn serialize(result: &SparqlResult) -> Result<Value> {
    let output = purrdf::sparql::to_json(result, &Default::default(), None).map_err(protocol)?;
    if output.provenance_dropped {
        return Err(protocol("native result serializer dropped provenance"));
    }
    if output.bytes.len() > crate::codec::RDF_MAX_BYTES {
        return Err(protocol("native result JSON exceeds admitted page bytes"));
    }
    // Native SRJ nests a term's type/value objects in addition to its triple
    // children. Bound this by the already admitted forest, rather than a
    // generic JSON default that rejects valid RDF transport items.
    json::read_slice(
        &output.bytes,
        json::Limits {
            max_depth: 2 * crate::codec::RDF_MAX_NODES + 16,
            max_values: crate::codec::RDF_MAX_BYTES as u64,
            max_string_bytes: crate::codec::RDF_MAX_BYTES,
            unique_members: false,
        },
    )
    .map_err(protocol)
}

fn declarations(graph: &purrdf::RdfDataset) -> Result<Value> {
    let limits = purrdf::projections::ProjectionLimits::new(1, 262144, 262144, 262144, 16)
        .map_err(protocol)?;
    graph
        .named_graphs()
        .map(|id| {
            let term =
                purrdf::projections::ProjectionTerm::from_term_value(&graph.term_value(id), limits)
                    .map_err(protocol)?;
            json::read_slice(
                &term.to_canonical_json(limits).map_err(protocol)?,
                json::Limits::default(),
            )
            .map_err(protocol)
        })
        .collect::<Result<Vec<_>>>()
        .map(Value::array)
}

fn data(value: wire::sparql_data::Reader<'_>) -> Result<Value> {
    let source = value.get_items()?;
    if source.len() > 1000 {
        return Err(protocol("native page exceeds its item ceiling"));
    }
    let shapes = source.iter().map(shape).collect::<Result<Vec<_>>>()?;
    let mut roots = Vec::new();
    for shape in &shapes {
        match shape {
            Shape::Solution(terms) | Shape::Graph { terms, .. } => {
                roots.extend(terms.iter().flatten().copied())
            }
            Shape::Boolean(_) => {}
        }
    }
    let decoded = crate::codec::decode_rdf(value.get_terms()?, &roots, false).map_err(protocol)?;
    let mut values = decoded.into_iter();
    let mut primary = RdfDatasetBuilder::new();
    let mut auxiliary = RdfDatasetBuilder::new();
    let mut rows = Vec::new();
    let mut boolean = None;
    let variables = value
        .get_variables()?
        .iter()
        .map(|v| Ok(v?.to_str()?.to_owned()))
        .collect::<Result<Vec<_>>>()?;
    if value.get_form()? != wire::SparqlForm::Select && !variables.is_empty() {
        return Err(protocol("a non-SELECT page carries projection variables"));
    }
    for shape in shapes {
        match shape {
            Shape::Solution(cells) => {
                if cells.len() != variables.len() || value.get_form()? != wire::SparqlForm::Select {
                    return Err(protocol(
                        "solution width/form disagrees with native projection",
                    ));
                }
                rows.push(
                    cells
                        .into_iter()
                        .map(|cell| {
                            cell.map(|_| {
                                values
                                    .next()
                                    .ok_or_else(|| protocol("native term forest ended early"))
                            })
                            .transpose()
                        })
                        .collect::<Result<Vec<_>>>()?,
                );
            }
            Shape::Boolean(value_) => {
                if boolean.replace(value_).is_some() || value.get_form()? != wire::SparqlForm::Ask {
                    return Err(protocol("ASK page carries inconsistent values"));
                }
            }
            Shape::Graph {
                auxiliary: aux,
                kind,
                terms,
            } => {
                if !matches!(
                    (value.get_form()?, aux),
                    (wire::SparqlForm::Select, true) | (wire::SparqlForm::Graph, false)
                ) {
                    return Err(protocol("native graph role disagrees with its result form"));
                }
                let builder = if aux { &mut auxiliary } else { &mut primary };
                let ids = terms
                    .into_iter()
                    .map(|term| {
                        term.map(|_| {
                            let value = values
                                .next()
                                .ok_or_else(|| protocol("native graph term forest ended early"))?;
                            builder.intern_value(&value).map_err(protocol)
                        })
                        .transpose()
                    })
                    .collect::<Result<Vec<_>>>()?;
                let required = |index: usize| {
                    ids.get(index)
                        .copied()
                        .flatten()
                        .ok_or_else(|| protocol("native graph item lacks a required term"))
                };
                match kind {
                    sparrow_core::SparqlGraphKind::NamedGraph => {
                        builder.declare_named_graph(required(0)?)
                    }
                    sparrow_core::SparqlGraphKind::Quad => {
                        builder.push_quad(required(0)?, required(1)?, required(2)?, ids[3])
                    }
                    sparrow_core::SparqlGraphKind::Reifier => {
                        builder.push_reifier_in_graph(required(0)?, required(1)?, ids[2])
                    }
                    sparrow_core::SparqlGraphKind::Annotation => builder.push_annotation_in_graph(
                        required(0)?,
                        required(1)?,
                        required(2)?,
                        ids[3],
                    ),
                }
                .map_err(protocol)?;
            }
        }
    }
    let primary = primary.freeze().map_err(protocol)?;
    let auxiliary = auxiliary.freeze().map_err(protocol)?;
    let primary_declarations = declarations(&primary)?;
    let aux_declarations = declarations(&auxiliary)?;
    let auxiliary_result = serialize(&SparqlResult::Graph(auxiliary.clone().into()))?;
    let result = match value.get_form()? {
        wire::SparqlForm::Select => SparqlResult::Solutions {
            variables,
            rows,
            aux: auxiliary.into(),
        },
        wire::SparqlForm::Ask => {
            SparqlResult::Boolean(boolean.ok_or_else(|| protocol("ASK data has no value"))?)
        }
        wire::SparqlForm::Graph => SparqlResult::Graph(primary.into()),
    };
    Ok(Object::new()
        .with("native_result", serialize(&result)?)
        .with("named_graphs", primary_declarations)
        .with(
            "auxiliary_graph",
            Object::new()
                .with("native_result", auxiliary_result)
                .with("named_graphs", aux_declarations),
        )
        .into())
}

fn governor(value: wire::native_governor_receipt::Reader<'_>) -> Result<Value> {
    let resources = value
        .get_resources()?
        .iter()
        .map(|v| {
            Ok(Value::from(
                Object::new()
                    .with("dimension", dimension(v.get_dimension()?))
                    .with("consumed", v.get_consumed().to_string())
                    .with("ceiling", v.get_ceiling().to_string()),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let stop = match value.get_stop()?.which()? {
        wire::governor_stop::Absent(()) => Value::Null,
        wire::governor_stop::Budget(v) => {
            let v = v?;
            Object::new()
                .with("kind", "budget")
                .with("dimension", dimension(v.get_dimension()?))
                .with("limit", v.get_limit().to_string())
                .with("consumed", v.get_consumed().to_string())
                .into()
        }
        wire::governor_stop::Refused(v) => {
            let v = v?;
            Object::new()
                .with("kind", "refused")
                .with("dimension", dimension(v.get_dimension()?))
                .with("limit", v.get_limit().to_string())
                .with("estimate", v.get_estimate().to_string())
                .into()
        }
        wire::governor_stop::Stopped(v) => Object::new()
            .with("kind", "stopped")
            .with(
                "cause",
                match v? {
                    wire::StopCause::Cancelled => "cancelled",
                    wire::StopCause::Deadline => "deadline",
                },
            )
            .into(),
    };
    let silenced = value
        .get_silenced()?
        .iter()
        .map(|v| {
            Ok(Value::from(
                Object::new()
                    .with(
                        "target_kind",
                        match v.get_target_kind()? {
                            wire::SilencedTargetKind::Service => "service",
                            wire::SilencedTargetKind::Load => "load",
                        },
                    )
                    .with("target", v.get_target()?.to_str()?)
                    .with(
                        "kind",
                        match v.get_kind()? {
                            wire::SilencedFailureKind::Transport => "transport",
                            wire::SilencedFailureKind::Decode => "decode",
                            wire::SilencedFailureKind::Disabled => "disabled",
                            wire::SilencedFailureKind::Unconfigured => "unconfigured",
                            wire::SilencedFailureKind::Denied => "denied",
                            wire::SilencedFailureKind::HostDenied => "host_denied",
                            wire::SilencedFailureKind::NotAnIri => "not_an_iri",
                            wire::SilencedFailureKind::Fault => "fault",
                        },
                    )
                    .with("message", v.get_message()?.to_str()?),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let errors = value
        .get_expression_errors()?
        .iter()
        .map(|v| {
            Ok(Value::from(
                Object::new()
                    .with("code", v.get_code()?.to_str()?)
                    .with("count", v.get_count().to_string()),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Object::new()
        .with("profile", value.get_profile()?.to_str()?)
        .with("resources", Value::array(resources))
        .with("stop", stop)
        .with("silenced", Value::array(silenced))
        .with("expression_errors", Value::array(errors))
        .into())
}

fn dimension(value: wire::GovernorDimension) -> &'static str {
    match value {
        wire::GovernorDimension::Fuel => "fuel",
        wire::GovernorDimension::AnswerRows => "answer_rows",
        wire::GovernorDimension::IntermediateCells => "intermediate_cells",
        wire::GovernorDimension::ScratchBytes => "scratch_bytes",
        wire::GovernorDimension::RemoteRequests => "remote_requests",
        wire::GovernorDimension::UdfDepth => "udf_depth",
        wire::GovernorDimension::Pages => "pages",
        wire::GovernorDimension::Bytes => "bytes",
    }
}

pub(super) fn render(value: wire::sparql_page::Reader<'_>) -> Result<Value> {
    let host = value.get_limits()?;
    let source_profile = host.get_source_profile()?.to_str()?;
    if source_profile != sparrow_core::RDF_SOURCE_ADMISSION_PROFILE
        || host.get_scope_receipt_bytes() != sparrow_core::QUERY_SCOPE_WORKSPACE_BYTES as u64
    {
        return Err(protocol("unsupported RDF source admission profile"));
    }
    let c = value.get_coverage()?;
    let selected = c.get_known_selected_records();
    let unknown = |known: u64| {
        selected
            .checked_sub(known)
            .ok_or_else(|| protocol("interpretation counts exceed selected records"))
    };
    let mut diagnostic_known = Object::new();
    let mut seen = std::collections::BTreeSet::new();
    for row in c.get_diagnostic_known()? {
        let dimension = crate::codec::diagnostic_dimension(row.get_dimension()?);
        if !seen.insert(dimension) {
            return Err(protocol("duplicate diagnostic interpretation counter"));
        }
        diagnostic_known.insert(
            dimension.name(),
            Object::new()
                .with("known", row.get_known().to_string())
                .with("unknown", unknown(row.get_known())?.to_string()),
        );
    }
    if seen.len() != sparrow_core::DiagnosticDimension::ALL.len() {
        return Err(protocol("missing diagnostic interpretation counters"));
    }
    let interpretations = Object::new()
        .with(
            "event_time",
            Object::new()
                .with("known", c.get_event_time_known().to_string())
                .with("unknown", unknown(c.get_event_time_known())?.to_string())
                // Every unknown source time is placed on the event timeline at receipt time.
                .with("fallback", unknown(c.get_event_time_known())?.to_string()),
        )
        .with("diagnostics", diagnostic_known);
    let correlation = match value.get_correlation()?.which()? {
        wire::optional_correlation_capture::Absent(()) => None,
        wire::optional_correlation_capture::Value(capture) => Some(
            crate::codec::decode_correlation(capture?, value.get_page()?.get_store_id()?.to_str()?)
                .map_err(|error| protocol(error.to_string()))?,
        ),
    };
    let d = c.get_diagnostics()?;
    let text =
        crate::text_wire::render_capture(value.get_text()?, selected, d.get_membership_complete())?;
    let coverage = Object::new()
        .with("visited", c.get_visited().to_string())
        .with("artifacts_visited", c.get_artifacts_visited().to_string())
        .with(
            "known_selected_artifacts",
            c.get_known_selected_artifacts().to_string(),
        )
        .with(
            "artifact_diagnostic_unknown",
            c.get_artifact_diagnostic_unknown().to_string(),
        )
        .with("catalog_entries", c.get_catalog_entries().to_string())
        .with(
            "selected_records",
            super::render_count(c.get_selected_records()?)?,
        )
        .with(
            "known_selected_records",
            c.get_known_selected_records().to_string(),
        )
        .with("content_ready", c.get_content_ready().to_string())
        .with("content_failed", c.get_content_failed().to_string())
        .with(
            "content_not_applicable",
            c.get_content_not_applicable().to_string(),
        )
        .with("complete", c.get_complete())
        .with(
            "event_fallback_pending",
            c.get_event_fallback_pending().to_string(),
        )
        .with(
            "diagnostic_scope",
            Object::new()
                .with("evaluated", d.get_evaluated().to_string())
                .with("matched", d.get_matched().to_string())
                .with("excluded", d.get_excluded().to_string())
                .with("unknown", d.get_unknown().to_string())
                .with("membership_complete", d.get_membership_complete()),
        );
    let g = governor(value.get_governors()?)?;
    let stop = g["stop"].clone();
    let e = value.get_evidence()?;
    let limits = host.get_source()?;
    let layers = value.get_layers()?;
    let mut result = super::catalog::page(value.get_page()?)?
        .with(
            "scope",
            super::query_receipt::scope_receipt(
                value.get_scope()?,
                value.get_page()?.get_as_of()?.get_unix_millis(),
            )?,
        )
        .with(
            "operation",
            if value.get_operation()? == wire::NativeReadOperation::Explain {
                "sparql_explain"
            } else if correlation.is_some() {
                "correlate"
            } else {
                "sparql_query"
            },
        )
        .with(
            "correlation",
            correlation.map_or(Value::Null, |capture| capture.descriptor()),
        )
        .with("interpretations", interpretations)
        .with("sequence", value.get_sequence().to_string())
        .with("coverage", coverage)
        .with("text_corpus", text)
        .with("native_governor", g)
        .with("stop_reason", stop)
        .with(
            "profile_ids",
            vec![purrdf::sparql::GOVERNOR_PROFILE_ID, source_profile],
        )
        .with(
            "layers",
            Object::new()
                .with("metadata", layers.get_metadata())
                .with("diagnostic", layers.get_diagnostic())
                .with("content", layers.get_content()),
        )
        .with(
            "source_evidence",
            Object::new()
                .with("requests", e.get_requests().to_string())
                .with("admitted_bytes", e.get_admitted_bytes().to_string())
                .with("row_visits", e.get_row_visits().to_string())
                .with("live_bytes", e.get_live_bytes().to_string())
                .with("peak_live_bytes", e.get_peak_live_bytes().to_string()),
        )
        .with(
            "limits",
            Object::new()
                .with("source_profile", source_profile)
                .with("page_size", value.get_page()?.get_page_size())
                .with("scan_budget", value.get_page()?.get_scan_budget())
                .with("page_bytes", host.get_page_bytes().to_string())
                .with("metadata_bytes", host.get_metadata_bytes().to_string())
                .with("result_handles", host.get_result_handles().to_string())
                .with("result_bytes", host.get_result_bytes().to_string())
                .with("result_lease_ms", host.get_result_lease_ms().to_string())
                .with(
                    "scope_catalog_bytes",
                    host.get_scope_catalog_bytes().to_string(),
                )
                .with(
                    "scope_receipt_bytes",
                    host.get_scope_receipt_bytes().to_string(),
                )
                .with(
                    "catalog_expansion_factor",
                    host.get_catalog_expansion_factor().to_string(),
                )
                .with("query_text_bytes", host.get_query_text_bytes().to_string())
                .with("binding_bytes", host.get_binding_bytes().to_string())
                .with("binding_nodes", host.get_binding_nodes())
                .with("fuel", limits.get_fuel().to_string())
                .with("answer_rows", limits.get_answer_rows().to_string())
                .with(
                    "intermediate_cells",
                    limits.get_intermediate_cells().to_string(),
                )
                .with("scratch_bytes", limits.get_scratch_bytes().to_string())
                .with("live_bytes", limits.get_live_bytes().to_string())
                .with("requests", limits.get_requests().to_string())
                .with("io_bytes", limits.get_io_bytes().to_string())
                .with("row_visits", limits.get_row_visits().to_string())
                .with("source_lease_ms", limits.get_source_lease_ms().to_string()),
        );
    if let wire::optional_native_diagnostic::Value(diagnostic) = value.get_diagnostic()?.which()? {
        result = result.with(
            "diagnostic",
            crate::codec::render_native_diagnostic(diagnostic?)?,
        );
    }
    let explain = value.get_operation()? == wire::NativeReadOperation::Explain;
    let outcome = value.get_outcome()?.which()?;
    let valid_operation = match outcome {
        wire::sparql_outcome::Explanation(_) => explain,
        wire::sparql_outcome::Failure(_) => true,
        _ => !explain,
    };
    if !valid_operation {
        return Err(protocol("native operation disagrees with its outcome"));
    }
    match outcome {
        wire::sparql_outcome::Explanation(v) => {
            result = result
                .with(
                    "execution_status",
                    if matches!(
                        value.get_governors()?.get_stop()?.which()?,
                        wire::governor_stop::Absent(())
                    ) {
                        "measured"
                    } else {
                        "stopped"
                    },
                )
                .with("explanation", super::explanation::render(v?)?);
        }
        wire::sparql_outcome::Complete(v) => {
            result = result
                .with("execution_status", "complete")
                .with("answer_certainty", "exact")
                .with("rows", data(v?)?);
        }
        wire::sparql_outcome::Certain(v) => {
            let v = v?;
            result = result
                .with("execution_status", "partial")
                .with("answer_certainty", "certain")
                .with("positional_prefix", v.get_positional_prefix())
                .with("rows", data(v.get_data()?)?);
        }
        wire::sparql_outcome::AtMost(v) => {
            let v = v?;
            result = result
                .with("execution_status", "partial")
                .with("answer_certainty", "at_most")
                .with("positional_prefix", v.get_positional_prefix())
                .with("candidates", data(v.get_data()?)?);
        }
        wire::sparql_outcome::Unknown(barrier) => {
            result = result
                .with("execution_status", "partial")
                .with("answer_certainty", "unknown")
                .with("barrier", barrier?.to_str()?);
        }
        wire::sparql_outcome::Failure(error) => {
            let error = failure(error?);
            result = result
                .with("execution_status", "failed")
                .with("error", error.json());
            if !explain {
                result = result.with("answer_certainty", "unknown");
            }
        }
    }
    let result: Value = result.into();
    if json::write_compact(&result).len() > 256 * 1024 {
        return Err(protocol(
            "native result page exceeds the 256 KiB rendered envelope ceiling",
        ));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use purrdf::{BlankScope, TermValue, ir::TermBox};

    #[test]
    fn missing_or_unknown_source_profiles_refuse_instead_of_claiming_current_admission() {
        for profile in ["", "unknown-source-law"] {
            let mut message = capnp::message::Builder::new_default();
            let mut page = message.init_root::<wire::sparql_page::Builder<'_>>();
            page.reborrow().init_limits().set_source_profile(profile);
            let error = render(page.into_reader()).unwrap_err();
            assert_eq!(error.code, "PROTOCOL_FAILURE");
            assert!(
                error
                    .message
                    .contains("unsupported RDF source admission profile")
            );
        }
    }

    #[test]
    fn typed_binding_objects_use_native_decoding_independent_of_member_order() {
        let input: Value = Object::new()
            .with("store_id", "store:test")
            .with("query", "SELECT ?x WHERE {}")
            .with(
                "bindings",
                Value::array([Object::new().with("name", "x").with(
                    "term",
                    Object::new().with("value", "urn:x").with("kind", "iri"),
                )]),
            )
            .into();
        let decoded = Input::decode(&input).unwrap();
        assert_eq!(
            decoded.bindings,
            vec![("x".into(), TermValue::iri("urn:x"))]
        );
    }

    #[test]
    fn native_graph_json_preserves_empty_declarations_reifiers_and_annotations() {
        let literal = TermValue::Literal {
            lexical_form: "مرحبا\n\"\\".into(),
            datatype: "http://www.w3.org/1999/02/22-rdf-syntax-ns#dirLangString".into(),
            language: Some("ar".into()),
            direction: Some(purrdf::RdfTextDirection::Rtl),
        };
        let values = [
            TermValue::iri("urn:s"),
            TermValue::iri("urn:p"),
            literal.clone(),
            TermValue::iri("urn:g"),
            TermValue::iri("urn:empty"),
            TermValue::Blank {
                label: "reifier".into(),
                scope: BlankScope(37),
            },
            TermValue::Triple {
                s: TermBox::new(TermValue::iri("urn:s")),
                p: TermBox::new(TermValue::iri("urn:p")),
                o: TermBox::new(literal),
            },
        ];
        let mut message = capnp::message::Builder::new_default();
        let mut wire = message.init_root::<wire::sparql_data::Builder<'_>>();
        wire.set_form(wire::SparqlForm::Graph);
        let refs = values.iter().collect::<Vec<_>>();
        let ids = crate::codec::encode_rdf(&refs, |n| wire.reborrow().init_terms(n)).unwrap();
        let mut items = wire.reborrow().init_items(5);
        items.reborrow().get(0).init_graph().set_declaration(ids[3]);
        items.reborrow().get(1).init_graph().set_declaration(ids[4]);
        let mut q = items.reborrow().get(2).init_graph().init_quad();
        q.set_subject(ids[0]);
        q.set_predicate(ids[1]);
        q.set_object(ids[2]);
        q.init_graph().set_value(ids[3]);
        let mut r = items.reborrow().get(3).init_graph().init_reifier();
        r.set_reifier(ids[5]);
        r.set_triple(ids[6]);
        r.init_graph().set_value(ids[3]);
        let mut a = items.get(4).init_graph().init_annotation();
        a.set_reifier(ids[5]);
        a.set_predicate(ids[1]);
        a.set_object(ids[2]);
        a.init_graph().set_value(ids[3]);
        let mut expected = RdfDatasetBuilder::new();
        let terms = values
            .iter()
            .map(|v| expected.intern_value(v).unwrap())
            .collect::<Vec<_>>();
        expected.declare_named_graph(terms[3]).unwrap();
        expected.declare_named_graph(terms[4]).unwrap();
        expected
            .push_quad(terms[0], terms[1], terms[2], Some(terms[3]))
            .unwrap();
        expected
            .push_reifier_in_graph(terms[5], terms[6], Some(terms[3]))
            .unwrap();
        expected
            .push_annotation_in_graph(terms[5], terms[1], terms[2], Some(terms[3]))
            .unwrap();
        let expected = expected.freeze().unwrap();
        let output = data(wire.reborrow().into_reader()).unwrap();
        assert_eq!(
            output["native_result"],
            serialize(&SparqlResult::Graph(expected.clone().into())).unwrap()
        );
        assert_eq!(output["named_graphs"], declarations(&expected).unwrap());
        assert_eq!(output["named_graphs"].as_array().unwrap().len(), 2);
        wire.set_form(wire::SparqlForm::Select);
        assert!(data(wire.into_reader()).is_err());
    }

    #[test]
    fn native_select_json_keeps_admitted_nested_terms_and_unbound_cells() {
        let mut message = capnp::message::Builder::new_default();
        let mut wire = message.init_root::<wire::sparql_data::Builder<'_>>();
        wire.set_form(wire::SparqlForm::Select);
        let mut variables = wire.reborrow().init_variables(2);
        variables.set(0, "x");
        variables.set(1, "missing");
        // 241 native nodes fit the 512-node/256-KiB transport admission.
        // Native SRJ adds two JSON objects per nested triple, exceeding an
        // unrelated 128-container JSON reader default without exceeding RDF
        // transport admission. Egress is independent of binding-input depth.
        let mut value = TermValue::iri("urn:x");
        for _ in 0..80 {
            value = TermValue::Triple {
                s: TermBox::new(value),
                p: TermBox::new(TermValue::iri("urn:p")),
                o: TermBox::new(TermValue::iri("urn:o")),
            };
        }
        let ids = crate::codec::encode_rdf(&[&value], |n| wire.reborrow().init_terms(n)).unwrap();
        let mut cells = wire
            .reborrow()
            .init_items(1)
            .get(0)
            .init_solution()
            .init_cells(2);
        cells.reborrow().get(0).set_value(ids[0]);
        cells.get(1).set_unbound(());
        let result = data(wire.into_reader()).unwrap();
        let rows = result["native_result"]["results"]["bindings"]
            .as_array()
            .unwrap();
        assert_eq!(rows.len(), 1);
        let parsed =
            purrdf::sparql::from_json(json::write_compact(&result["native_result"]).as_bytes())
                .unwrap();
        assert_eq!(parsed.rows, vec![vec![Some(value), None]]);
        assert!(rows[0].get("missing").is_none());
    }
}
