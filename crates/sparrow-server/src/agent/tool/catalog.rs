// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Schema and annotation registry for implemented operations only.

use purrdf_lex::json::{Object, Value};

const TOOLS: &[(&str, &str)] = &[
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
        "Discover JSON pointers and kinds in an acceptance-time scope, preserving duplicate occurrences and coverage.",
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
        "record_get",
        "Read one concrete record's metadata and artifact-relative byte bounds.",
    ),
    (
        "bytes_read",
        "Read at most 1 MiB of exact original bytes; offset is relative to the selected evidence.",
    ),
    (
        "pin",
        "Durably protect concrete artifact/record IDs for a finite duration, using a stable retry identity.",
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
        let mut properties = Object::new();
        let mut required = Vec::new();
        if ["keys_list", "values_search", "text_search"].contains(name) {
            for field in ["source", "received_from", "received_before"] {
                properties = properties.with(
                    field,
                    Object::new().with("type", Value::array(["string", "null"])),
                );
            }
            properties = properties
                .with("limit", integer(1000))
                .with("scan_budget", integer(100000));
        }
        if *name == "values_search" {
            for field in ["path", "kind", "lexical", "decoded"] {
                properties = properties.with(
                    field,
                    Object::new().with("type", Value::array(["string", "null"])),
                );
            }
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
        ]
        .contains(name)
        {
            properties = properties.with("store_id", string());
            required.push("store_id");
        }
        if ["artifact_get", "record_get", "bytes_read"].contains(name) {
            properties = properties.with("id", string());
            required.push("id");
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
        if ["pin", "pin_release"].contains(name) {
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
        if *name == "pin_release" {
            properties = properties.with("pin_id", string());
            required.push("pin_id");
        }
        Object::new()
            .with("name", *name)
            .with("description", *description)
            .with(
                "inputSchema",
                Object::new()
                    .with("type", "object")
                    .with("properties", properties)
                    .with("required", Value::array(required))
                    .with("additionalProperties", false),
            )
            .with("outputSchema", Object::new().with("type", "object"))
            .with(
                "annotations",
                Object::new()
                    .with("readOnlyHint", !["pin", "pin_release"].contains(name))
                    .with("destructiveHint", *name == "pin_release")
                    .with("idempotentHint", true)
                    .with("openWorldHint", false),
            )
            .into()
    }))
}
