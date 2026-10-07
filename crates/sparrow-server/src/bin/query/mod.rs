// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use purrdf_lex::json::{Object, Value};
use sparrow_server::sparrow_capnp as wire;

mod ingest;
mod read;
mod search;

pub(super) const USAGE: &str = "usage: sparrow SOCKET_PATH status | ingest REQUEST_ID SOURCE FORMAT FILE | bytes STORE_ID EVIDENCE_ID OFFSET LENGTH OUTPUT_FILE | keys | values [--source TEXT] [--received-from RFC3339] [--received-before RFC3339] [--limit N] [--scan-budget N]; values also accepts --path POINTER --kind KIND --lexical TEXT --decoded TEXT";

pub(super) enum Command {
    Status,
    Ingest(ingest::Input),
    Bytes(read::Input),
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

fn invalid(message: impl Into<String>) -> capnp::Error {
    capnp::Error::failed(message.into())
}

impl Command {
    pub(super) fn parse(args: &[String]) -> capnp::Result<Self> {
        if args == ["status"] {
            return Ok(Self::Status);
        }
        if args.first().map(String::as_str) == Some("ingest") {
            return Ok(Self::Ingest(ingest::Input::parse(&args[1..])?));
        }
        if args.first().map(String::as_str) == Some("bytes") {
            return Ok(Self::Bytes(read::Input::parse(&args[1..])?));
        }
        let values = match args.first().map(String::as_str) {
            Some("keys") => false,
            Some("values") => true,
            Some("text") => false,
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
        let mut options = Options {
            source: None,
            from: 0,
            before: u64::MAX,
            limit: 100,
            scan_budget: 10000,
            path: None,
            kind: None,
            lexical: None,
            decoded: None,
        };
        let mut seen = std::collections::BTreeSet::new();
        let mut fields = args[if text.is_some() { 2 } else { 1 }..].iter();
        while let Some(flag) = fields.next() {
            if !seen.insert(flag) {
                return Err(invalid(format!("duplicate option: {flag}")));
            }
            let value = fields
                .next()
                .ok_or_else(|| invalid(format!("missing value for {flag}")))?;
            match flag.as_str() {
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
                    options.kind = Some(match value.as_str() {
                        "string" => wire::JsonKind::String,
                        "number" => wire::JsonKind::Number,
                        "true" => wire::JsonKind::BooleanTrue,
                        "false" => wire::JsonKind::BooleanFalse,
                        "null" => wire::JsonKind::Null,
                        "array" => wire::JsonKind::Array,
                        "object" => wire::JsonKind::Object,
                        _ => {
                            return Err(invalid(
                                "--kind requires string, number, true, false, null, array or object",
                            ));
                        }
                    })
                }
                "--lexical" if values => options.lexical = Some(value.clone()),
                "--decoded" if values => options.decoded = Some(value.clone()),
                _ => return Err(invalid(format!("unknown option for {}: {flag}", args[0]))),
            }
        }
        if options.from >= options.before
            || !(1..=1000).contains(&options.limit)
            || !(1..=100000).contains(&options.scan_budget)
        {
            return Err(invalid(
                "require from < before, limit 1..1000 and scan-budget 1..100000",
            ));
        }
        Ok(Self::Query {
            values,
            text,
            options,
        })
    }

    pub(super) async fn execute(
        &self,
        client: &wire::sparrow::Client,
    ) -> capnp::Result<Option<Value>> {
        if let Self::Ingest(input) = self {
            return Ok(Some(input.execute(client).await?));
        }
        if let Self::Bytes(input) = self {
            return Ok(Some(input.execute(client).await?));
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
                .collect::<capnp::Result<Vec<_>>>()?;
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
                .collect::<capnp::Result<Vec<_>>>()?;
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

fn optional(mut field: wire::optional_text::Builder<'_>, value: &Option<String>) {
    match value {
        Some(value) => field.set_value(value),
        None => field.set_absent(()),
    }
}

fn optional_value(field: wire::optional_text::Reader<'_>) -> capnp::Result<Value> {
    match field.which()? {
        wire::optional_text::Absent(()) => Ok(Value::Null),
        wire::optional_text::Value(value) => Ok(value?.to_str()?.into()),
    }
}

fn failure(error: wire::failure::Reader<'_>) -> capnp::Error {
    match (
        error.get_code().and_then(|t| Ok(t.to_str()?.to_owned())),
        error.get_message().and_then(|t| Ok(t.to_str()?.to_owned())),
    ) {
        (Ok(code), Ok(message)) => invalid(format!(
            "{code}: {message}; retryable={}",
            error.get_retryable()
        )),
        (Err(error), _) | (_, Err(error)) => error,
    }
}

fn instant(value: wire::instant::Reader<'_>) -> capnp::Result<String> {
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

fn receipt(value: wire::receipt::Reader<'_>) -> capnp::Result<Value> {
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
