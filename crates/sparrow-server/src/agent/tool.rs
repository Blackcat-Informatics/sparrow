// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Closed, typed tool inputs sharing the CLI's generated-client operations.

use super::Result;
use super::{
    Command, Options, context, evidence, failure, ingest, instant, invalid, parse_kind, pin, read,
    status, wire,
};
use purrdf_lex::json::{
    Object, Value,
    record::{DecodeError, FromJson, Record},
};

mod catalog;
pub(crate) use catalog::{definitions, known};

pub(crate) enum Operation {
    Status,
    Capabilities,
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
    Pin {
        request: String,
        name: String,
        store: String,
        targets: Vec<String>,
        days: u32,
    },
    Release {
        request: String,
        store: String,
        pin: String,
    },
    Renew(pin::Renewal),
    PinRead(pin::Read),
}

impl Operation {
    pub(crate) fn parse(name: &str, arguments: &Value) -> Result<Self> {
        let decode = || -> std::result::Result<Self, DecodeError> {
            let mut fields = Record::new(arguments, "Sparrow tool arguments")?;
            let result = match name {
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
                        let representation: String = fields.required("representation")?;
                        Some((
                            fields.required("path")?,
                            super::source_time::representation(&representation)?,
                        ))
                    } else {
                        None
                    };
                    Self::Query(Command::SourceTime(super::source_time::Input {
                        source: fields.required("source")?,
                        definition,
                    }))
                }
                "rdf_inspect" => Self::Query(Command::Rdf {
                    store: fields.required("store_id")?,
                    id: fields.required("id")?,
                }),
                "capabilities" => Self::Capabilities,
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
                    let paged = ["records_search", "text_search"].contains(&name);
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
                    if name == "values_search" {
                        options.path = fields.optional("path")?;
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
                "pin" => Self::Pin {
                    request: fields.required("request_id")?,
                    store: fields.required("store_id")?,
                    name: fields.required("name")?,
                    targets: fields.required("targets")?,
                    days: fields.required("days")?,
                },
                "pin_release" => Self::Release {
                    request: fields.required("request_id")?,
                    store: fields.required("store_id")?,
                    pin: fields.required("pin_id")?,
                },
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

    pub(crate) async fn execute(self, client: &wire::sparrow::Client) -> Result<Value> {
        match self {
            Self::Status => status::execute(client).await,
            Self::Capabilities => Ok(Object::new()
                .with("status", status::execute(client).await?)
                .with("tools", definitions())
                .with("transport", "capnp-unix")
                .with("retention_days", 30_u64)
                .with("time_basis", "received_at")
                .with(
                    "records_time_bases",
                    Value::array(["received_at", "event_time"]),
                )
                .with("mcp_protocol", "2025-11-25")
                .into()),
            Self::Query(command) => command
                .execute(client)
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
            Self::Pin {
                request,
                store,
                name,
                targets,
                days,
            } => {
                if targets.len() > 10000 {
                    return Err(invalid(
                        "RESOURCE_LIMIT: pin tool permits at most 10,000 targets",
                    ));
                }
                let mut call = client.pin_request();
                let mut input = call.get().init_request();
                input.set_request_id(&request);
                input.set_name(&name);
                input.set_days(days);
                let mut list = input.init_targets(targets.len() as u32);
                for (index, id) in targets.iter().enumerate() {
                    evidence(list.reborrow().get(index as u32), &store, id)?;
                }
                let response = call.send().promise.await?;
                let value = match response.get()?.get_result()?.which()? {
                    wire::result::Ok(value) => value?,
                    wire::result::Error(error) => return Err(failure(error?)),
                };
                pin::render(value)
            }
            Self::Release {
                request,
                store,
                pin,
            } => {
                let mut call = client.pin_release_request();
                let mut input = call.get().init_request();
                input.set_request_id(&request);
                input.set_store_id(&store);
                input.set_pin_id(&pin);
                let response = call.send().promise.await?;
                let value = match response.get()?.get_result()?.which()? {
                    wire::result::Ok(value) => value?,
                    wire::result::Error(error) => return Err(failure(error?)),
                };
                Ok(Object::new()
                    .with("store_id", value.get_store_id()?.to_str()?)
                    .with("request_id", value.get_request_id()?.to_str()?)
                    .with("pin_id", value.get_pin_id()?.to_str()?)
                    .with("released_at", instant(value.get_released_at()?)?)
                    .into())
            }
        }
    }
}
