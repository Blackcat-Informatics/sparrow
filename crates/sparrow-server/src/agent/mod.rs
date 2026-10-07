// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Shared agent operations and canonical result rendering for CLI and MCP.

use crate::sparrow_capnp as wire;
use purrdf_lex::json::{Object, Value};

mod context;
mod durability;
mod error;
mod ingest;
mod pin;
mod rdf;
mod read;
mod records;
mod search;
mod source_time;
mod status;
pub use error::AgentError;
/// Shared result type preserving typed failures across CLI and MCP.
pub type AgentResult<T> = std::result::Result<T, AgentError>;
type Result<T> = AgentResult<T>;
pub(crate) mod tool;

pub(super) const USAGE: &str = concat!(
    "usage: sparrow SOCKET_PATH status | rdf STORE_ID RECORD_ID | ingest REQUEST_ID SOURCE FORMAT FILE | flush STORE_ID SESSION_ID SEQUENCE",
    " | bytes STORE_ID EVIDENCE_ID OFFSET LENGTH OUTPUT_FILE | pin-renew REQUEST_ID STORE_ID PIN_ID DAYS [EXPECTED_REVISION]",
    " | pins STORE_ID [--cursor CURSOR] [--page-size N] [--scan-budget N]",
    " | pin-get STORE_ID (--id PIN_ID | --name NAME) [--cursor CURSOR] [--page-size N] [--scan-budget N]",
    " | source-time SOURCE POINTER REPRESENTATION | source-time-get SOURCE | records [--event-from RFC3339 --event-before RFC3339] [--text TEXT] [--where-file FILE] [--require-complete-index true|false] | context STORE_ID RECORD_ID [--before N] [--after N] | text QUERY | keys | values",
    " [--source TEXT] [--received-from RFC3339] [--received-before RFC3339] [--limit N] [--scan-budget N];",
    " values also accepts --path POINTER --kind KIND --lexical TEXT --decoded TEXT"
);

/// Execute a CLI command through the same typed client and renderer as MCP.
pub async fn run_cli(client: &wire::sparrow::Client, args: &[String]) -> Result<Value> {
    match Command::parse(args)?.execute(client).await? {
        Some(output) => Ok(output),
        None => status::execute(client).await,
    }
}

pub(super) enum Command {
    Flush(sparrow_client::DurabilityWatermark),
    Status,
    SourceTime(source_time::Input),
    Records(records::Input),
    Ingest(ingest::Input),
    Bytes(read::Input),
    Context(context::Input),
    Renew(pin::Renewal),
    PinRead(pin::Read),
    Rdf {
        store: String,
        id: String,
    },
    Query {
        values: bool,
        text: Option<String>,
        options: Options,
    },
}

pub(super) struct Options {
    source: Option<String>,
    from: u64,
    before: u64,
    limit: u32,
    scan_budget: u32,
    path: Option<String>,
    kind: Option<wire::JsonKind>,
    lexical: Option<String>,
    decoded: Option<String>,
}

fn invalid(message: impl Into<String>) -> AgentError {
    AgentError {
        code: "INVALID_ARGUMENT".into(),
        message: message.into(),
        retryable: false,
    }
}

fn decimal_u64(value: &Value) -> std::result::Result<u64, purrdf_lex::json::record::DecodeError> {
    value
        .as_str()
        .filter(|v| !v.is_empty() && v.as_bytes().iter().all(u8::is_ascii_digit))
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| {
            purrdf_lex::json::record::DecodeError::invalid_value(value, "decimal UInt64 string")
        })
}

fn evidence(mut target: wire::evidence::Builder<'_>, store: &str, id: &str) -> Result<()> {
    target.set_store_id(store);
    if id.starts_with("artifact:") {
        target.set_artifact(id);
    } else if id.starts_with("record:") {
        target.set_record(id);
    } else {
        return Err(invalid("evidence ID requires artifact: or record: prefix"));
    }
    Ok(())
}

impl Default for Options {
    fn default() -> Self {
        Self {
            source: None,
            from: 0,
            before: u64::MAX,
            limit: 100,
            scan_budget: 10000,
            path: None,
            kind: None,
            lexical: None,
            decoded: None,
        }
    }
}

impl Options {
    fn validate(&self) -> Result<()> {
        if self.from >= self.before
            || !(1..=1000).contains(&self.limit)
            || !(1..=100000).contains(&self.scan_budget)
        {
            return Err(invalid(
                "require from < before, limit 1..1000 and scan-budget 1..100000",
            ));
        }
        Ok(())
    }
}

fn parse_kind(
    value: &str,
) -> std::result::Result<wire::JsonKind, purrdf_lex::json::record::DecodeError> {
    match value {
        "string" => Ok(wire::JsonKind::String),
        "number" => Ok(wire::JsonKind::Number),
        "true" => Ok(wire::JsonKind::BooleanTrue),
        "false" => Ok(wire::JsonKind::BooleanFalse),
        "null" => Ok(wire::JsonKind::Null),
        "array" => Ok(wire::JsonKind::Array),
        "object" => Ok(wire::JsonKind::Object),
        _ => Err(purrdf_lex::json::record::DecodeError::unknown_variant(
            value,
            &[
                "string", "number", "true", "false", "null", "array", "object",
            ],
        )),
    }
}

impl Command {
    pub(super) fn parse(args: &[String]) -> Result<Self> {
        if args.first().is_some_and(|arg| arg == "flush") {
            if args.len() != 4 {
                return Err(invalid("flush requires STORE_ID SESSION_ID SEQUENCE"));
            }
            return Ok(Self::Flush(sparrow_client::DurabilityWatermark {
                store_id: args[1].clone(),
                session_id: args[2].clone(),
                sequence: decimal_u64(&Value::from(args[3].clone()))
                    .map_err(|error| invalid(error.to_string()))?,
            }));
        }
        if args == ["status"] {
            return Ok(Self::Status);
        }
        if args
            .first()
            .is_some_and(|arg| arg == "source-time" || arg == "source-time-get")
        {
            let define = args[0] == "source-time";
            if args.len() != if define { 4 } else { 2 } {
                return Err(invalid(
                    "source-time requires SOURCE POINTER REPRESENTATION; source-time-get requires SOURCE",
                ));
            }
            let definition = if define {
                Some((
                    args[2].clone(),
                    source_time::representation(&args[3]).map_err(|e| invalid(e.to_string()))?,
                ))
            } else {
                None
            };
            return Ok(Self::SourceTime(source_time::Input {
                source: args[1].clone(),
                definition,
            }));
        }
        if args.first().map(String::as_str) == Some("rdf") {
            if args.len() != 3 {
                return Err(invalid("rdf requires STORE_ID RECORD_ID"));
            }
            return Ok(Self::Rdf {
                store: args[1].clone(),
                id: args[2].clone(),
            });
        }
        if args.first().map(String::as_str) == Some("ingest") {
            return Ok(Self::Ingest(ingest::Input::parse(&args[1..])?));
        }
        if args.first().map(String::as_str) == Some("bytes") {
            return Ok(Self::Bytes(read::Input::parse(&args[1..])?));
        }
        if args.first().map(String::as_str) == Some("pin-renew") {
            return Ok(Self::Renew(pin::Renewal::parse(&args[1..])?));
        }
        if matches!(args.first().map(String::as_str), Some("pins" | "pin-get")) {
            return Ok(Self::PinRead(pin::Read::parse(
                &args[1..],
                args[0] == "pin-get",
            )?));
        }
        let values = match args.first().map(String::as_str) {
            Some("keys") => false,
            Some("values") => true,
            Some("text") => false,
            Some("context") => false,
            Some("records") => false,
            _ => return Err(invalid(USAGE)),
        };
        let text = if args[0] == "text" {
            Some(
                args.get(1)
                    .ok_or_else(|| invalid("text requires a query string"))?
                    .clone(),
            )
        } else {
            None
        };
        let records = args[0] == "records";
        let mut records_text = None;
        let mut event_from = None;
        let mut event_before = None;
        let mut record_fields = Vec::new();
        let mut require_complete = false;
        let mut options = Options::default();
        let context = args[0] == "context";
        if context && args.len() < 3 {
            return Err(invalid("context requires STORE_ID RECORD_ID"));
        }
        let mut before = 10;
        let mut after = 10;
        let mut seen = std::collections::BTreeSet::new();
        let mut fields = args[if context {
            3
        } else if text.is_some() {
            2
        } else {
            1
        }..]
            .iter();
        while let Some(flag) = fields.next() {
            if !seen.insert(flag) {
                return Err(invalid(format!("duplicate option: {flag}")));
            }
            let value = fields
                .next()
                .ok_or_else(|| invalid(format!("missing value for {flag}")))?;
            match flag.as_str() {
                "--event-from" if records => event_from = Some(value.clone()),
                "--event-before" if records => event_before = Some(value.clone()),
                "--text" if records => records_text = Some(value.clone()),
                "--where-file" if records => record_fields = records::read_fields(value)?,
                "--require-complete-index" if records => {
                    require_complete = value
                        .parse()
                        .map_err(|_| invalid("require-complete-index requires true or false"))?
                }
                "--source" => options.source = Some(value.clone()),
                "--received-from" => {
                    options.from =
                        sparrow_core::parse_instant(value).map_err(|e| invalid(e.to_string()))?
                }
                "--received-before" => {
                    options.before =
                        sparrow_core::parse_instant(value).map_err(|e| invalid(e.to_string()))?
                }
                "--limit" => {
                    options.limit = value
                        .parse()
                        .map_err(|_| invalid("--limit requires an unsigned integer"))?
                }
                "--scan-budget" => {
                    options.scan_budget = value
                        .parse()
                        .map_err(|_| invalid("--scan-budget requires an unsigned integer"))?
                }
                "--path" if values => options.path = Some(value.clone()),
                "--kind" if values => {
                    options.kind = Some(parse_kind(value).map_err(|e| invalid(e.to_string()))?)
                }
                "--lexical" if values => options.lexical = Some(value.clone()),
                "--decoded" if values => options.decoded = Some(value.clone()),
                "--before" if context => {
                    before = value
                        .parse()
                        .map_err(|_| invalid("--before requires an unsigned integer"))?
                }
                "--after" if context => {
                    after = value
                        .parse()
                        .map_err(|_| invalid("--after requires an unsigned integer"))?
                }
                "--ordering" if context && value == "received_at_record_id" => {}
                _ => return Err(invalid(format!("unknown option for {}: {flag}", args[0]))),
            }
        }
        options.validate()?;
        if records {
            return Ok(Self::Records(records::Input {
                options,
                text: records_text,
                event_interval: records::interval(event_from, event_before)?,
                fields: record_fields,
                require_complete,
            }));
        }
        if context {
            let input = context::Input {
                store: args[1].clone(),
                id: args[2].clone(),
                options,
                before,
                after,
            };
            input.validate()?;
            return Ok(Self::Context(input));
        }
        Ok(Self::Query {
            values,
            text,
            options,
        })
    }

    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Option<Value>> {
        if let Self::Flush(through) = self {
            return Ok(Some(durability::execute(client, through).await?));
        }
        if let Self::SourceTime(input) = self {
            return Ok(Some(input.execute(client).await?));
        }
        if let Self::Records(input) = self {
            return Ok(Some(input.execute(client).await?));
        }
        if let Self::Ingest(input) = self {
            return Ok(Some(input.execute(client).await?));
        }
        if let Self::Bytes(input) = self {
            return Ok(Some(input.execute(client).await?));
        }
        if let Self::Context(input) = self {
            return Ok(Some(input.execute(client).await?));
        }
        if let Self::Renew(input) = self {
            return Ok(Some(input.execute(client).await?));
        }
        if let Self::PinRead(input) = self {
            return Ok(Some(input.execute(client).await?));
        }
        if let Self::Rdf { store, id } = self {
            return Ok(Some(rdf::inspect(client, store, id).await?));
        }
        let Self::Query {
            values,
            text,
            options,
        } = self
        else {
            return Ok(None);
        };
        if let Some(text) = text {
            return Ok(Some(search::execute(client, options, text).await?));
        }
        if *values {
            let mut request = client.values_search_request();
            let mut query = request.get().init_query();
            fill_scope(query.reborrow().init_scope(), options);
            optional(query.reborrow().init_path(), &options.path);
            optional(query.reborrow().init_lexical(), &options.lexical);
            optional(query.reborrow().init_decoded(), &options.decoded);
            let mut kind = query.init_kind();
            match options.kind {
                Some(value) => kind.set_value(value),
                None => kind.set_absent(()),
            }
            let response = request.send().promise.await?;
            let result = response.get()?.get_result()?;
            let found = match result.which()? {
                wire::result::Ok(value) => value?,
                wire::result::Error(error) => return Err(failure(error?)),
            };
            let rows = found
                .get_rows()?
                .iter()
                .map(|row| {
                    Ok(Value::from(
                        Object::new()
                            .with("record_id", row.get_record_id()?.to_str()?)
                            .with("artifact_id", row.get_artifact_id()?.to_str()?)
                            .with("source", row.get_source()?.to_str()?)
                            .with("received_at", instant(row.get_received_at()?)?)
                            .with("source_start", row.get_source_start().to_string())
                            .with("source_end", row.get_source_end().to_string())
                            .with("occurrence", row.get_occurrence().to_string())
                            .with("path", row.get_path()?.to_str()?)
                            .with("kind", kind_name(row.get_kind()?))
                            .with("lexical", optional_value(row.get_lexical()?)?)
                            .with("decoded", optional_value(row.get_decoded()?)?),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Some(
                Object::new()
                    .with("receipt", receipt(found.get_receipt()?)?)
                    .with("rows", Value::array(rows))
                    .into(),
            ))
        } else {
            let mut request = client.keys_list_request();
            fill_scope(request.get().init_scope(), options);
            let response = request.send().promise.await?;
            let result = response.get()?.get_result()?;
            let found = match result.which()? {
                wire::result::Ok(value) => value?,
                wire::result::Error(error) => return Err(failure(error?)),
            };
            let rows = found
                .get_rows()?
                .iter()
                .map(|row| {
                    Ok(Value::from(
                        Object::new()
                            .with("path", row.get_path()?.to_str()?)
                            .with("kind", kind_name(row.get_kind()?))
                            .with("occurrences", row.get_occurrences().to_string())
                            .with("records", row.get_records().to_string()),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Some(
                Object::new()
                    .with("receipt", receipt(found.get_receipt()?)?)
                    .with("rows", Value::array(rows))
                    .into(),
            ))
        }
    }
}

fn fill_scope(mut scope: wire::scope::Builder<'_>, options: &Options) {
    optional(scope.reborrow().init_source(), &options.source);
    scope
        .reborrow()
        .init_received_from()
        .set_unix_millis(options.from);
    scope
        .reborrow()
        .init_received_before()
        .set_unix_millis(options.before);
    scope.set_limit(options.limit);
    scope.set_scan_budget(options.scan_budget);
}

fn optional(field: wire::optional_text::Builder<'_>, value: &Option<String>) {
    sparrow_client::encode_optional(
        field,
        value.as_deref(),
        |mut field, value| field.set_value(value),
        |mut field| field.set_absent(()),
    );
}

fn optional_value(field: wire::optional_text::Reader<'_>) -> Result<Value> {
    match field.which()? {
        wire::optional_text::Absent(()) => Ok(Value::Null),
        wire::optional_text::Value(value) => Ok(value?.to_str()?.into()),
    }
}

fn failure(error: wire::failure::Reader<'_>) -> AgentError {
    sparrow_client::Error::domain(error).into()
}

fn instant(value: wire::instant::Reader<'_>) -> Result<String> {
    sparrow_core::format_instant(value.get_unix_millis()).map_err(|e| invalid(e.to_string()))
}

fn kind_name(kind: wire::JsonKind) -> &'static str {
    match kind {
        wire::JsonKind::String => "string",
        wire::JsonKind::Number => "number",
        wire::JsonKind::BooleanTrue => "true",
        wire::JsonKind::BooleanFalse => "false",
        wire::JsonKind::Null => "null",
        wire::JsonKind::Array => "array",
        wire::JsonKind::Object => "object",
    }
}

fn receipt(value: wire::receipt::Reader<'_>) -> Result<Value> {
    let coverage = value.get_coverage()?;
    Ok(Object::new()
        .with("store_id", value.get_store_id()?.to_str()?)
        .with("membership_at", instant(value.get_membership_at()?)?)
        .with(
            "execution",
            match value.get_execution()? {
                wire::Execution::Complete => "complete",
                wire::Execution::Limited => "limited",
            },
        )
        .with(
            "certainty",
            match value.get_certainty()? {
                wire::Certainty::Exact => "exact",
                wire::Certainty::Incomplete => "incomplete",
            },
        )
        .with(
            "coverage",
            Object::new()
                .with("visited", coverage.get_visited().to_string())
                .with(
                    "eligible_records",
                    coverage.get_eligible_records().to_string(),
                )
                .with(
                    "failed_json_records",
                    coverage.get_failed_json_records().to_string(),
                )
                .with(
                    "non_json_records",
                    coverage.get_non_json_records().to_string(),
                )
                .with(
                    "decoding_failures",
                    coverage.get_decoding_failures().to_string(),
                ),
        )
        .into())
}
