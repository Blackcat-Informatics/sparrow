// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Schema and annotation registry for implemented operations only.

use purrdf_lex::json::{Object, Value};

const TOOLS: &[(&str, &str)] = &[
    (
        "correlate",
        "Run fixed native governed correlation recipes over one immutable scope: equal declared trace/span values, shared sources or co-occurrence in an explicit receipt/event-time interval. Returns both supporting record IDs, source byte spans and the original native certainty/coverage receipt. Equal identifiers and temporal co-occurrence do not establish causality or global span identity. Missing interpretations degrade coverage; strict mode refuses them. Resume with result_next without recapturing seeds.",
    ),
    (
        "sparql_query",
        "Execute native read-only SELECT, ASK, CONSTRUCT or DESCRIBE over an admitted immutable retained record scope. Typed bindings use PurRDF canonical term JSON. Native complete/certain/at-most/unknown certificates and source coverage remain separate from paging. Remote requests are disabled. Resume using result_next; reading never renews the one-minute result lease.",
    ),
    (
        "retention_metrics",
        "Read a complete admitted logical raw-payload account: oldest retained receipt, effective window, unique live/pin-held/retirable/private-owner bytes. Counts share one catalog instant. No maintenance, pin renewal or physical reclamation is performed or certified. Admission and integrity failures return no partial account.",
    ),
    (
        "ingest_get",
        "Read one bounded catalog instant for an original upload/direct request, opaque import or producer handoff: staging, preparation, native index coverage, original acceptance and synchronized source removal. Exactly one selector is required. This read never advances intake, renews evidence or flushes buffered writes.",
    ),
    (
        "result_release",
        "Release a captured result replay lease and its memory; future continuations expire. Does not release evidence pins or remove stored records.",
    ),
    (
        "watch_handoff",
        "Admit an immutable completed source for durable claim and deletion; a tail target acknowledges its original producer-finished checkpoint without reimporting records.",
    ),
    (
        "watch_handoff_get",
        "Read original source retirement progress using the exact saved handoff intent; only removed confirms synchronized deletion.",
    ),
    (
        "file_finish",
        "Durably admit an immutable producer assertion for a held file generation, then drain through its final size; source files remain retained and admission alone is not completion.",
    ),
    (
        "file_finish_get",
        "Read original completion progress using the unchanged admitted intent; retries never recapture files or refresh acceptance clocks.",
    ),
    (
        "file_generation_get",
        "Read a durable file-generation checkpoint and original raw path/namespace declaration; accepted prefixes do not certify whole-file capture or producer continuity.",
    ),
    (
        "syslog_listener_get",
        "Read a currently configured syslog listener under its exact store/source: framing, bounds, binding and process-scoped observed counters. Continuity and UDP kernel loss remain unknown; counters reset on restart.",
    ),
    (
        "producer_stream_get",
        "Read a finite accepted producer window, exact aggregate unobserved positions and bounded recent gaps; prefix and trailing continuity remain unknown.",
    ),
    (
        "receiver_window_get",
        "Read durable syslog receiver commit order and known local discard counts/ranges. Sender, kernel/network and pre-commit loss remain unknown; summary retention is independent of pins.",
    ),
    (
        "result_next",
        "Resume the captured operation, scope and limits from an immutable cursor; reading never renews its lease or evidence retention.",
    ),
    (
        "sources_list",
        "Page an immutable capture of exact intake keys, retained occurrence counts, mapping IDs, producer and receiver windows, file-generation checkpoint IDs and configured watch/syslog listener bindings; continuity remains explicitly unknown.",
    ),
    (
        "artifacts_list",
        "Page an immutable capture of eligible complete artifacts and surviving parent provenance; original receipt-time filters refuse reclaimed parents with unknown origin metadata.",
    ),
    (
        "diagnostic_define",
        "Durably bind immutable explicit JSON pointer mappings for service, host, severity and trace/span IDs; existing records are never reinterpreted.",
    ),
    (
        "diagnostic_get",
        "Read an immutable diagnostic source mapping or explicit absence.",
    ),
    (
        "flush",
        "Synchronize through a specified current-session acceptance fence; earlier sessions and future sequences are refused.",
    ),
    (
        "source_time_define",
        "Durably bind an immutable source timestamp pointer and representation for future ingestions; identical retries are idempotent and conflicting mappings are refused.",
    ),
    (
        "source_time_get",
        "Read a declared source timestamp mapping or explicit absence.",
    ),
    (
        "records_search",
        "Intersect field predicates within one record before optional native text ranking; explicit acceptance-time scope and native parent bindings.",
    ),
    (
        "pins_list",
        "List active collection metadata in bounded pages over one immutable 60-second catalog capture; cursors never renew protection.",
    ),
    (
        "pin_get",
        "Resolve exactly one collection name or stable ID and page its captured direct members/revision; later pages preserve that capture after mutations.",
    ),
    (
        "capabilities",
        "Discover running store identity, limits and implemented tools.",
    ),
    (
        "status",
        "Read current storage identity, limits and counters.",
    ),
    (
        "keys_list",
        "Discover JSON pointers and kinds in an acceptance-time scope, optionally at one exact RFC 6901 path; preserve duplicate occurrences, record presence and coverage.",
    ),
    (
        "values_search",
        "Find exact JSON pointer/kind/lexical/decoded values; predicates intersect on the same occurrence.",
    ),
    (
        "text_search",
        "Rank persisted native lexical postings matching any query term; returns ranking corpus, source spans and coverage.",
    ),
    (
        "artifact_get",
        "Read a concrete complete artifact's metadata; retained records do not substitute for an expired artifact.",
    ),
    (
        "records_context",
        "Fetch retained same-source neighbors ordered by acceptance time and record ID; reports observed retirement gaps and unknown producer continuity.",
    ),
    (
        "rdf_inspect",
        "Inspect persisted native RDF projection state, graph references and distinct stream row counts for a retained record.",
    ),
    (
        "record_get",
        "Read one concrete record's metadata and artifact-relative byte bounds.",
    ),
    (
        "bytes_read",
        "Read at most 1 MiB of exact artifact/record bytes or a completed file-generation range; whole-file reads require the entire retained cover.",
    ),
    (
        "pin",
        "Durably protect concrete artifact/record IDs for a finite duration, using a stable retry identity.",
    ),
    (
        "pin_renew",
        "Durably extend a stable active pin without changing members; optional decimal revision guards concurrent changes and retries preserve the original receipt.",
    ),
    (
        "pin_release",
        "Durably release only the named stable pin identity; other protection remains active.",
    ),
];

pub(crate) fn known(name: &str) -> bool {
    TOOLS.iter().any(|(tool, _)| *tool == name)
}

fn string() -> Value {
    Object::new().with("type", "string").into()
}
fn integer(maximum: u64) -> Value {
    Object::new()
        .with("type", "integer")
        .with("minimum", 1_u64)
        .with("maximum", maximum)
        .into()
}

pub(crate) fn definitions() -> Value {
    Value::array(TOOLS.iter().map(|(name, description)| -> Value {
        if *name == "sparql_query" || *name == "correlate" {
            return Object::new()
                .with("name", *name)
                .with("description", *description)
                .with(
                    "inputSchema",
                    if *name == "correlate" {
                        super::super::correlation::schema()
                    } else {
                        super::super::sparql::schema()
                    },
                )
                .with("outputSchema", Object::new().with("type", "object"))
                .with(
                    "annotations",
                    Object::new()
                        .with("readOnlyHint", true)
                        .with("destructiveHint", false)
                        .with("idempotentHint", true)
                        .with("openWorldHint", false),
                )
                .into();
        }
        let mut properties = Object::new();
        let mut required = Vec::new();
        if [
            "file_finish",
            "file_finish_get",
            "watch_handoff",
            "watch_handoff_get",
        ]
        .contains(name)
        {
            let seconds: Value = Object::new().with("type", "integer").into();
            let nanos: Value = Object::new()
                .with("type", "integer")
                .with("minimum", 0_u32)
                .with("maximum", 999_999_999_u32)
                .into();
            properties = properties
                .with("store_id", string())
                .with("request_id", string())
                .with("watch_revision", string())
                .with(
                    "observed",
                    Object::new()
                        .with("type", "object")
                        .with("additionalProperties", false)
                        .with(
                            "required",
                            vec![
                                "device",
                                "inode",
                                "byte_length",
                                "modified_seconds",
                                "modified_nanos",
                                "changed_seconds",
                                "changed_nanos",
                            ],
                        )
                        .with(
                            "properties",
                            Object::new()
                                .with("device", string())
                                .with("inode", string())
                                .with("byte_length", string())
                                .with("modified_seconds", seconds.clone())
                                .with("modified_nanos", nanos.clone())
                                .with("changed_seconds", seconds)
                                .with("changed_nanos", nanos),
                        ),
                );
            required.extend(["store_id", "request_id", "watch_revision", "observed"]);
            if ["file_finish", "file_finish_get"].contains(name) {
                properties = properties.with("generation_id", string());
                required.push("generation_id");
            } else {
                properties = properties
                    .with("watch", string())
                    .with("relative_path_hex", string())
                    .with("tail_generation_id", string());
                required.extend(["watch", "relative_path_hex"]);
            }
        }
        if *name == "retention_metrics" {
            properties = properties
                .with("store_id", string())
                .with("max_visits", integer(100000))
                .with("catalog_bytes", integer(33554432))
                .with("payload_bytes", integer(1073741824));
            required.push("store_id");
        }
        if *name == "ingest_get" {
            properties = properties
                .with("store_id", string())
                .with("import_id", string())
                .with("request_id", string())
                .with("handoff_request_id", string())
                .with("scan_budget", integer(100000))
                .with("catalog_bytes", integer(33554432));
            required.push("store_id");
        }
        if *name == "file_generation_get" {
            properties = properties
                .with("store_id", string())
                .with("generation_id", string());
            required.extend(["store_id", "generation_id"]);
        }
        if *name == "syslog_listener_get" {
            properties = properties
                .with("store_id", string())
                .with("source", string())
                .with("listener_id", string());
            required.extend(["store_id", "source", "listener_id"]);
        }
        if *name == "producer_stream_get" {
            properties = properties
                .with("store_id", string())
                .with("stream_id", string());
            required.extend(["store_id", "stream_id"]);
        }
        if *name == "receiver_window_get" {
            properties = properties
                .with("store_id", string())
                .with("window_id", string());
            required.extend(["store_id", "window_id"]);
        }
        if *name == "result_next" {
            properties = properties
                .with("store_id", string())
                .with("cursor", string());
            required.extend(["store_id", "cursor"]);
        }
        if *name == "result_release" {
            properties = properties
                .with("store_id", string())
                .with("snapshot_id", string());
            required.extend(["store_id", "snapshot_id"]);
        }
        if ["artifacts_list", "sources_list"].contains(name) {
            properties = properties
                .with("store_id", string())
                .with("page_size", integer(1000))
                .with("scan_budget", integer(100000));
            let fields: &[&str] = if *name == "artifacts_list" {
                &["source", "received_from", "received_before", "cursor"]
            } else {
                &["source", "cursor"]
            };
            for &field in fields {
                properties = properties.with(
                    field,
                    Object::new().with("type", Value::array(["string", "null"])),
                );
            }
            if *name == "artifacts_list" {
                properties = properties
                    .with("scope", super::super::selection::schema())
                    .with(
                        "require_complete_index",
                        Object::new().with("type", "boolean"),
                    );
            }
            required.push("store_id");
        }
        if *name == "flush" {
            properties = properties
                .with("store_id", string())
                .with("session_id", string())
                .with("sequence", string());
            required.extend(["store_id", "session_id", "sequence"]);
        }
        if [
            "keys_list",
            "values_search",
            "text_search",
            "records_search",
            "records_context",
        ]
        .contains(name)
        {
            for field in ["source", "received_from", "received_before"] {
                properties = properties.with(
                    field,
                    Object::new().with("type", Value::array(["string", "null"])),
                );
            }
            properties = properties
                .with("scope", super::super::selection::schema())
                .with(
                    "require_complete_index",
                    Object::new().with("type", "boolean"),
                )
                .with("limit", integer(1000))
                .with("scan_budget", integer(100000));
        }
        if [
            "records_search",
            "text_search",
            "keys_list",
            "values_search",
        ]
        .contains(name)
        {
            properties = properties.with("page_size", integer(1000)).with(
                "cursor",
                Object::new().with("type", Value::array(["string", "null"])),
            );
        }
        if *name == "keys_list" {
            properties = properties.with(
                "path",
                Object::new()
                    .with("type", Value::array(["string", "null"]))
                    .with("maxLength", sparrow_core::JSON_QUERY_PATH_BYTES as u64),
            );
        }
        if *name == "values_search" {
            for field in ["path", "kind", "lexical", "decoded"] {
                properties = properties.with(
                    field,
                    Object::new().with("type", Value::array(["string", "null"])),
                );
            }
        }
        if ["diagnostic_define", "diagnostic_get"].contains(name) {
            properties = properties.with("source", string());
            required.push("source");
            if *name == "diagnostic_define" {
                let mut mapping = Object::new();
                for field in ["service", "host", "severity", "trace_id", "span_id"] {
                    mapping = mapping.with(field, string());
                }
                properties = properties.with(
                    "mappings",
                    Object::new()
                        .with("type", "object")
                        .with("additionalProperties", false)
                        .with("minProperties", 1_u64)
                        .with("properties", mapping),
                );
                required.push("mappings");
            }
        }
        if ["source_time_define", "source_time_get"].contains(name) {
            properties = properties.with("source", string());
            required.push("source");
            if *name == "source_time_define" {
                properties = properties.with("path", string()).with(
                    "representation",
                    Object::new().with("type", "string").with(
                        "enum",
                        Value::array(["rfc3339", "unix_millis", "unix_seconds"]),
                    ),
                );
                required.extend(["path", "representation"]);
            }
        }
        if *name == "records_search" {
            properties =
                properties.with("group_by_artifact", Object::new().with("type", "boolean"));
            for field in ["event_from", "event_before"] {
                properties = properties.with(
                    field,
                    Object::new().with("type", Value::array(["string", "null"])),
                );
            }
            let mut field_properties = Object::new();
            for field in [
                "path",
                "kind",
                "lexical",
                "decoded",
                "parent_binding",
                "parent_path",
            ] {
                field_properties = field_properties.with(
                    field,
                    Object::new().with("type", Value::array(["string", "null"])),
                );
            }
            properties = properties
                .with(
                    "text",
                    Object::new().with("type", Value::array(["string", "null"])),
                )
                .with(
                    "fields",
                    Object::new()
                        .with("type", "array")
                        .with("maxItems", 32_u64)
                        .with(
                            "items",
                            Object::new()
                                .with("type", "object")
                                .with("additionalProperties", false)
                                .with("properties", field_properties),
                        ),
                );
        }
        if *name == "text_search" {
            properties = properties.with("text", string());
            required.push("text");
        }
        if [
            "artifact_get",
            "record_get",
            "bytes_read",
            "pin",
            "pin_release",
            "pin_renew",
            "pin_get",
            "pins_list",
            "records_context",
            "rdf_inspect",
        ]
        .contains(name)
        {
            properties = properties.with("store_id", string());
            required.push("store_id");
        }
        if [
            "artifact_get",
            "record_get",
            "bytes_read",
            "records_context",
            "rdf_inspect",
        ]
        .contains(name)
        {
            properties = properties.with("id", string());
            required.push("id");
        }
        if *name == "records_context" {
            let count = Object::new()
                .with("type", "integer")
                .with("minimum", 0_u64)
                .with("maximum", 999_u64);
            properties = properties
                .with("before", count.clone())
                .with("after", count)
                .with(
                    "ordering",
                    Object::new()
                        .with("type", "string")
                        .with("enum", Value::array(["received_at_record_id"])),
                );
        }
        if *name == "bytes_read" {
            properties = properties
                .with(
                    "offset",
                    Object::new()
                        .with("type", "string")
                        .with("pattern", "^[0-9]+$"),
                )
                .with(
                    "length",
                    Object::new()
                        .with("type", "integer")
                        .with("minimum", 0_u64)
                        .with("maximum", 1048576_u64),
                )
                .with(
                    "encoding",
                    Object::new()
                        .with("type", "string")
                        .with("enum", Value::array(["base64", "utf8"])),
                );
            required.extend(["offset", "length"]);
        }
        if ["pin", "pin_release", "pin_renew"].contains(name) {
            properties = properties.with("request_id", string());
            required.push("request_id");
        }
        if *name == "pin" {
            properties = properties
                .with("name", string())
                .with("days", integer(u64::from(u32::MAX)))
                .with(
                    "targets",
                    Object::new()
                        .with("type", "array")
                        .with("items", string())
                        .with("minItems", 1_u64)
                        .with("maxItems", 10000_u64),
                );
            required.extend(["name", "days", "targets"]);
        }
        if ["pin_release", "pin_renew"].contains(name) {
            properties = properties.with("pin_id", string());
            required.push("pin_id");
        }
        if *name == "pin_renew" {
            properties = properties.with("days", integer(u64::from(u32::MAX))).with(
                "expected_revision",
                Object::new()
                    .with("type", "string")
                    .with("pattern", "^[0-9]+$"),
            );
            required.push("days");
        }
        if ["pin_get", "pins_list"].contains(name) {
            properties = properties
                .with(
                    "cursor",
                    Object::new().with("type", Value::array(["string", "null"])),
                )
                .with("page_size", integer(1000))
                .with("scan_budget", integer(100000));
        }
        if *name == "pin_get" {
            properties = properties.with("pin_id", string()).with("name", string());
        }
        let mut schema = Object::new()
            .with("type", "object")
            .with("properties", properties)
            .with("required", Value::array(required))
            .with("additionalProperties", false);
        if *name == "pin_get" {
            schema = schema.with(
                "oneOf",
                Value::array([
                    Value::from(Object::new().with("required", Value::array(["pin_id"]))),
                    Value::from(Object::new().with("required", Value::array(["name"]))),
                ]),
            );
        }
        if *name == "ingest_get" {
            schema = schema.with(
                "oneOf",
                Value::array([
                    Value::from(Object::new().with("required", Value::array(["import_id"]))),
                    Value::from(Object::new().with("required", Value::array(["request_id"]))),
                    Value::from(
                        Object::new().with("required", Value::array(["handoff_request_id"])),
                    ),
                ]),
            );
        }
        Object::new()
            .with("name", *name)
            .with("description", *description)
            .with("inputSchema", schema)
            .with("outputSchema", Object::new().with("type", "object"))
            .with(
                "annotations",
                Object::new()
                    .with(
                        "readOnlyHint",
                        ![
                            "pin",
                            "pin_release",
                            "result_release",
                            "pin_renew",
                            "source_time_define",
                            "diagnostic_define",
                            "flush",
                            "file_finish",
                            "watch_handoff",
                        ]
                        .contains(name),
                    )
                    .with(
                        "destructiveHint",
                        ["pin_release", "watch_handoff", "result_release"].contains(name),
                    )
                    .with("idempotentHint", true)
                    .with("openWorldHint", false),
            )
            .into()
    }))
}
