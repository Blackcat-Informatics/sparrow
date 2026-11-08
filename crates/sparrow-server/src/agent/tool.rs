// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Closed, typed tool inputs sharing the CLI's generated-client operations.

use super::Result;
use super::{
    Command, Options, context, failure, ingest, invalid, parse_kind, pin, read, status, wire,
};
use purrdf_lex::json::{
    Object, Value,
    record::{DecodeError, FromJson, Record},
};

mod catalog;
pub(crate) use catalog::{definitions, known};

pub(crate) enum Operation {
    Status,
    Query(Command),
    Metadata {
        store: String,
        id: String,
        artifact: bool,
    },
    Bytes {
        store: String,
        id: String,
        offset: u64,
        length: u32,
        encoding: String,
    },
    Renew(pin::Renewal),
    PinRead(pin::Read),
}

impl Operation {
    pub(crate) fn parse(name: &str, arguments: &Value) -> Result<Self> {
        if name == "correlate" {
            return Ok(Self::Query(Command::Correlate(Box::new(
                super::correlation::Input::decode(arguments)?,
            ))));
        }
        if name == "sparql_query" || name == "sparql_explain" {
            let input = super::sparql::Input::decode(arguments)?;
            return Ok(Self::Query(Command::Sparql(Box::new(
                if name == "sparql_explain" {
                    input.explanation()
                } else {
                    input
                },
            ))));
        }
        let decode = || -> std::result::Result<Self, DecodeError> {
            let mut fields = Record::new(arguments, "Sparrow tool arguments")?;
            let result = match name {
                "retention_metrics" => {
                    let defaults = sparrow_client::RetentionReadLimits::default();
                    Self::Query(Command::Retention(super::retention::Input {
                        store: fields.required("store_id")?,
                        limits: sparrow_client::RetentionReadLimits {
                            max_visits: fields
                                .raw("max_visits")?
                                .map(u32::from_json)
                                .transpose()?
                                .unwrap_or(defaults.max_visits),
                            catalog_bytes: fields
                                .raw("catalog_bytes")?
                                .map(u32::from_json)
                                .transpose()?
                                .unwrap_or(defaults.catalog_bytes),
                            payload_bytes: fields
                                .raw("payload_bytes")?
                                .map(u32::from_json)
                                .transpose()?
                                .unwrap_or(defaults.payload_bytes),
                        },
                    }))
                }
                "ingest_get" => {
                    let request = fields
                        .raw("request_id")?
                        .map(String::from_json)
                        .transpose()?;
                    let import = fields
                        .raw("import_id")?
                        .map(String::from_json)
                        .transpose()?;
                    let handoff = fields
                        .raw("handoff_request_id")?
                        .map(String::from_json)
                        .transpose()?;
                    let selector = match (request, import, handoff) {
                        (Some(id), None, None) => sparrow_client::IngestSelector::Request(id),
                        (None, Some(id), None) => sparrow_client::IngestSelector::Import(id),
                        (None, None, Some(id)) => sparrow_client::IngestSelector::Handoff(id),
                        _ => {
                            return Err(DecodeError::custom(
                                "ingest_get requires exactly one of request_id, import_id or handoff_request_id",
                            ));
                        }
                    };
                    Self::Query(Command::IngestStatus(super::ingest_status::Input {
                        store: fields.required("store_id")?,
                        selector,
                        limits: sparrow_client::IngestReadLimits {
                            scan_budget: fields
                                .raw("scan_budget")?
                                .map(u32::from_json)
                                .transpose()?
                                .unwrap_or(10000),
                            catalog_bytes: fields
                                .raw("catalog_bytes")?
                                .map(u32::from_json)
                                .transpose()?
                                .unwrap_or(33554432),
                        },
                    }))
                }
                "file_finish" | "file_finish_get" => {
                    let observed: Value = fields.required("observed")?;
                    let intent = sparrow_client::TailFinishIntent {
                        store_id: fields.required("store_id")?,
                        request_id: fields.required("request_id")?,
                        generation_id: fields.required("generation_id")?,
                        watch_revision: fields
                            .required_with("watch_revision", super::decimal_u64)?,
                        observed: file_observation(&observed)?,
                    };
                    Self::Query(Command::TailFinish(super::tail_finish::Input::Finish {
                        intent,
                        read_only: name == "file_finish_get",
                    }))
                }
                "watch_handoff" | "watch_handoff_get" => {
                    let observed: Value = fields.required("observed")?;
                    let intent = sparrow_client::CompletedHandoff {
                        store_id: fields.required("store_id")?,
                        request_id: fields.required("request_id")?,
                        watch: fields.required("watch")?,
                        watch_revision: fields
                            .required_with("watch_revision", super::decimal_u64)?,
                        relative_path: fields.required_with("relative_path_hex", |value| {
                            let text = String::from_json(value)?;
                            if text.len() > 4096 {
                                return Err(DecodeError::custom(
                                    "handoff raw path exceeds 2048 bytes",
                                ));
                            }
                            purrdf_hash::hex::decode_canonical(&text).map_err(DecodeError::custom)
                        })?,
                        generation: file_observation(&observed)?,
                        tail_generation_id: fields.optional("tail_generation_id")?,
                    };
                    Self::Query(Command::Watch(super::watch::Input::Handoff {
                        intent,
                        read_only: name == "watch_handoff_get",
                    }))
                }
                "watch_claim_retire" => {
                    Self::Query(Command::Watch(super::watch::Input::RetireClaim {
                        store: Some(fields.required("store_id")?),
                        watch: fields.required("watch")?,
                        claim: fields.required("claim_id")?,
                        reason: fields.required("reason")?,
                    }))
                }
                "file_generation_get" => Self::Query(Command::Tail(super::tail::Input {
                    store: fields.required("store_id")?,
                    id: fields.required("generation_id")?,
                })),
                "syslog_listener_get" => Self::Query(Command::Syslog(super::syslog::Input {
                    store: fields.required("store_id")?,
                    source: fields.required("source")?,
                    id: fields.required("listener_id")?,
                })),
                "producer_stream_get" => Self::Query(Command::Producer(super::producer::Input {
                    store: fields.required("store_id")?,
                    id: fields.required("stream_id")?,
                })),
                "receiver_window_get" => Self::Query(Command::Receiver(super::receiver::Input {
                    store: fields.required("store_id")?,
                    id: fields.required("window_id")?,
                })),
                "status" => Self::Status,
                "flush" => Self::Query(Command::Flush(sparrow_client::DurabilityWatermark {
                    store_id: fields.required("store_id")?,
                    session_id: fields.required("session_id")?,
                    sequence: fields.required_with("sequence", super::decimal_u64)?,
                })),
                "diagnostic_define" | "diagnostic_get" => {
                    Self::Query(Command::Diagnostic(super::diagnostic::Input {
                        source: fields.required("source")?,
                        mappings: if name == "diagnostic_define" {
                            Some(super::diagnostic::decode_mappings(
                                fields
                                    .raw("mappings")?
                                    .ok_or_else(|| DecodeError::custom("mappings is required"))?,
                            )?)
                        } else {
                            None
                        },
                    }))
                }
                "source_time_define" | "source_time_get" => {
                    let definition = if name == "source_time_define" {
                        Some(super::source_time::definition(&mut fields)?)
                    } else {
                        None
                    };
                    Self::Query(Command::SourceTime(super::source_time::Input {
                        source: fields.required("source")?,
                        definition,
                    }))
                }
                "result_next" => Self::Query(Command::Result(super::result::Input {
                    store: fields.required("store_id")?,
                    cursor: fields.required("cursor")?,
                })),
                "result_release" => {
                    Self::Query(Command::ReleaseResult(super::result::ReleaseInput {
                        store: fields.required("store_id")?,
                        snapshot: fields.required("snapshot_id")?,
                    }))
                }
                "rdf_inspect" => Self::Query(Command::Rdf {
                    store: fields.required("store_id")?,
                    id: fields.required("id")?,
                }),
                "capabilities" => Self::Query(Command::Capabilities),
                "sources_list" => {
                    let mut query = sparrow_core::SourceRead::default();
                    query.selector.source = fields.optional("source")?;
                    query.cursor = fields.optional("cursor")?;
                    query.page_size = fields
                        .raw("page_size")?
                        .map(u32::from_json)
                        .transpose()?
                        .unwrap_or(200) as usize;
                    query.scan_budget = fields
                        .raw("scan_budget")?
                        .map(u32::from_json)
                        .transpose()?
                        .unwrap_or(10000) as usize;
                    Self::Query(Command::Sources(super::sources::Input {
                        store: fields.required("store_id")?,
                        query,
                    }))
                }
                "artifacts_list" => {
                    let mut query = sparrow_core::ArtifactRead::default();
                    query.selector.require_complete_index =
                        fields.optional("require_complete_index")?.unwrap_or(false);
                    query.selector.source = fields.optional("source")?;
                    query.cursor = fields.optional("cursor")?;
                    let from: Option<String> = fields.optional("received_from")?;
                    let before: Option<String> = fields.optional("received_before")?;
                    if let Some(value) = from {
                        query.selector.received_from =
                            sparrow_core::parse_instant(&value).map_err(DecodeError::custom)?;
                    }
                    if let Some(value) = before {
                        query.selector.received_before =
                            sparrow_core::parse_instant(&value).map_err(DecodeError::custom)?;
                    }
                    query.page_size = fields
                        .raw("page_size")?
                        .map(u32::from_json)
                        .transpose()?
                        .unwrap_or(200) as usize;
                    query.scan_budget = fields
                        .raw("scan_budget")?
                        .map(u32::from_json)
                        .transpose()?
                        .unwrap_or(10000) as usize;
                    Self::Query(Command::Artifacts(super::artifacts::Input {
                        store: fields.required("store_id")?,
                        query,
                        selection: Box::new(
                            fields
                                .raw("scope")?
                                .map(super::selection::decode)
                                .transpose()?
                                .unwrap_or_default(),
                        ),
                    }))
                }

                "keys_list" | "values_search" | "text_search" | "records_context"
                | "records_search" => {
                    let mut options = Options {
                        selection: Box::new(
                            fields
                                .raw("scope")?
                                .map(super::selection::decode)
                                .transpose()?
                                .unwrap_or_default(),
                        ),
                        source: fields.optional("source")?,
                        ..Options::default()
                    };
                    let from: Option<String> = fields.optional("received_from")?;
                    let before: Option<String> = fields.optional("received_before")?;
                    if let Some(value) = from {
                        options.from =
                            sparrow_core::parse_instant(&value).map_err(DecodeError::custom)?;
                    }
                    if let Some(value) = before {
                        options.before =
                            sparrow_core::parse_instant(&value).map_err(DecodeError::custom)?;
                    }
                    let limit = fields.raw("limit")?.map(u32::from_json).transpose()?;
                    let paged = [
                        "records_search",
                        "text_search",
                        "keys_list",
                        "values_search",
                    ]
                    .contains(&name);
                    let page_size = if paged {
                        fields.raw("page_size")?.map(u32::from_json).transpose()?
                    } else {
                        None
                    };
                    if limit.is_some() && page_size.is_some() {
                        return Err(DecodeError::custom(
                            "use either page_size or limit, not both",
                        ));
                    }
                    options.limit = page_size.or(limit).unwrap_or(options.limit);
                    if paged {
                        options.cursor = fields.optional("cursor")?;
                    }
                    options.scan_budget = fields
                        .raw("scan_budget")?
                        .map(u32::from_json)
                        .transpose()?
                        .unwrap_or(options.scan_budget);
                    if ["keys_list", "values_search"].contains(&name) {
                        options.path = fields.optional("path")?;
                    }
                    if name == "values_search" {
                        let kind: Option<String> = fields.optional("kind")?;
                        options.kind = kind.as_deref().map(parse_kind).transpose()?;
                        options.lexical = fields.optional("lexical")?;
                        options.decoded = fields.optional("decoded")?;
                    }
                    options.validate().map_err(DecodeError::custom)?;
                    options.require_complete_index = fields
                        .raw("require_complete_index")?
                        .map(bool::from_json)
                        .transpose()?
                        .unwrap_or(false);
                    if name == "records_search" {
                        let input = super::records::Input {
                            group_by_artifact: fields
                                .raw("group_by_artifact")?
                                .map(bool::from_json)
                                .transpose()?
                                .unwrap_or(false),
                            event_interval: super::records::interval(
                                fields.optional("event_from")?,
                                fields.optional("event_before")?,
                            )
                            .map_err(DecodeError::custom)?,
                            text: fields.optional("text")?,
                            fields: fields
                                .raw("fields")?
                                .map(super::records::decode_fields)
                                .transpose()?
                                .unwrap_or_default(),
                            options,
                        };
                        fields.deny_unknown()?;
                        return Ok(Self::Query(Command::Records(input)));
                    }
                    if name == "records_context" {
                        let ordering: Option<String> =
                            fields.raw("ordering")?.map(String::from_json).transpose()?;
                        if ordering
                            .as_deref()
                            .is_some_and(|s| s != "received_at_record_id")
                        {
                            return Err(DecodeError::custom(
                                "context ordering must be received_at_record_id",
                            ));
                        }
                        let input = context::Input {
                            store: fields.required("store_id")?,
                            id: fields.required("id")?,
                            before: fields
                                .raw("before")?
                                .map(u32::from_json)
                                .transpose()?
                                .unwrap_or(10),
                            after: fields
                                .raw("after")?
                                .map(u32::from_json)
                                .transpose()?
                                .unwrap_or(10),
                            options,
                        };
                        input.validate().map_err(DecodeError::custom)?;
                        fields.deny_unknown()?;
                        return Ok(Self::Query(Command::Context(input)));
                    }
                    let text = if name == "text_search" {
                        Some(fields.required("text")?)
                    } else {
                        None
                    };
                    Self::Query(Command::Query {
                        values: name == "values_search",
                        text,
                        options,
                    })
                }
                "artifact_get" | "record_get" => Self::Metadata {
                    store: fields.required("store_id")?,
                    id: fields.required("id")?,
                    artifact: name == "artifact_get",
                },
                "bytes_read" => Self::Bytes {
                    store: fields.required("store_id")?,
                    id: fields.required("id")?,
                    offset: fields.required_with("offset", super::decimal_u64)?,
                    length: fields.required("length")?,
                    encoding: fields
                        .raw("encoding")?
                        .map(String::from_json)
                        .transpose()?
                        .unwrap_or_else(|| "base64".into()),
                },
                "pin" => Self::Query(Command::PinMutation(pin::Mutation::Protect {
                    request: fields.required("request_id")?,
                    store: fields.required("store_id")?,
                    name: fields.required("name")?,
                    targets: fields.required("targets")?,
                    days: fields.required("days")?,
                })),
                "pin_release" => Self::Query(Command::PinMutation(pin::Mutation::Release {
                    request: fields.required("request_id")?,
                    store: fields.required("store_id")?,
                    pin: fields.required("pin_id")?,
                })),
                "pin_renew" => Self::Renew(pin::Renewal {
                    request: fields.required("request_id")?,
                    store: fields.required("store_id")?,
                    id: fields.required("pin_id")?,
                    days: fields.required("days")?,
                    expected_revision: fields
                        .raw("expected_revision")?
                        .map(|value| {
                            let text = String::from_json(value)?;
                            pin::revision(&text).map_err(DecodeError::custom)
                        })
                        .transpose()?,
                }),
                "pins_list" | "pin_get" => {
                    let selector = if name == "pin_get" {
                        match (
                            fields.optional::<String>("pin_id")?,
                            fields.optional::<String>("name")?,
                        ) {
                            (Some(id), None) => sparrow_core::PinSelector::Id(id),
                            (None, Some(name)) => sparrow_core::PinSelector::Name(name),
                            _ => {
                                return Err(DecodeError::custom(
                                    "pin_get requires exactly one pin_id or name",
                                ));
                            }
                        }
                    } else {
                        sparrow_core::PinSelector::Active
                    };
                    Self::PinRead(pin::Read {
                        store: fields.required("store_id")?,
                        selector,
                        cursor: fields.optional("cursor")?,
                        page_size: fields
                            .raw("page_size")?
                            .map(u32::from_json)
                            .transpose()?
                            .unwrap_or(200),
                        scan_budget: fields
                            .raw("scan_budget")?
                            .map(u32::from_json)
                            .transpose()?
                            .unwrap_or(10000),
                    })
                }
                _ => return Err(DecodeError::custom(format!("unknown tool: {name}"))),
            };
            fields.deny_unknown()?;
            Ok(result)
        };
        decode().map_err(|e| invalid(e.to_string()))
    }

    pub(crate) async fn execute(self, connection: &sparrow_client::Client) -> Result<Value> {
        let client = connection.capability();
        match self {
            Self::Status => status::execute(client).await,
            Self::Query(command) => command
                .execute(connection)
                .await?
                .ok_or_else(|| invalid("query produced no result")),
            Self::Renew(input) => input.execute(client).await,
            Self::PinRead(input) => input.execute(client).await,
            Self::Metadata {
                store,
                id,
                artifact,
            } => {
                if artifact {
                    let mut request = client.artifact_get_request();
                    let mut input = request.get().init_artifact();
                    input.set_store_id(&store);
                    input.set_id(&id);
                    let response = request.send().promise.await?;
                    match response.get()?.get_result()?.which()? {
                        wire::result::Ok(value) => ingest::render(value?),
                        wire::result::Error(error) => Err(failure(error?)),
                    }
                } else {
                    let mut request = client.record_get_request();
                    let mut input = request.get().init_record();
                    input.set_store_id(&store);
                    input.set_id(&id);
                    let response = request.send().promise.await?;
                    let value = match response.get()?.get_result()?.which()? {
                        wire::result::Ok(value) => value?,
                        wire::result::Error(error) => return Err(failure(error?)),
                    };
                    context::render_record(value)
                }
            }
            Self::Bytes {
                store,
                id,
                offset,
                length,
                encoding,
            } => {
                if !["base64", "utf8"].contains(&encoding.as_str()) {
                    return Err(invalid(
                        "INVALID_ARGUMENT: encoding requires base64 or utf8",
                    ));
                }
                let (store, bytes) = read::fetch(client, &store, &id, offset, length).await?;
                let data = if encoding == "base64" {
                    purrdf::xsd::canonical_base64(&bytes)
                } else {
                    String::from_utf8(bytes.clone()).map_err(|e| {
                        invalid(format!(
                            "INVALID_ARGUMENT: requested bytes are not UTF-8: {e}"
                        ))
                    })?
                };
                Ok(Object::new()
                    .with("store_id", store)
                    .with("evidence_id", id)
                    .with("offset", offset.to_string())
                    .with("byte_length", bytes.len().to_string())
                    .with("encoding", encoding)
                    .with("data", data)
                    .into())
            }
        }
    }
}

fn file_observation(
    value: &Value,
) -> std::result::Result<sparrow_client::CompletedFileIdentity, DecodeError> {
    let mut fields = Record::new(value, "final file observation")?;
    let observed = sparrow_client::CompletedFileIdentity {
        device: fields.required_with("device", super::decimal_u64)?,
        inode: fields.required_with("inode", super::decimal_u64)?,
        length: fields.required_with("byte_length", super::decimal_u64)?,
        modified_seconds: fields.required("modified_seconds")?,
        modified_nanos: fields.required("modified_nanos")?,
        changed_seconds: fields.required("changed_seconds")?,
        changed_nanos: fields.required("changed_nanos")?,
    };
    fields.deny_unknown()?;
    Ok(observed)
}
