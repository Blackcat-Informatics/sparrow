// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Render tail generation and checkpoint responses for command output.

use crate::sparrow_capnp as wire;
use purrdf_lex::json::{Object, Value};

fn render_declaration(value: wire::tail_declaration::Reader<'_>) -> crate::AgentResult<Value> {
    let format = match value.get_format()? {
        wire::Format::Log => "log",
        wire::Format::Jsonl => "jsonl",
        _ => {
            return Err(
                sparrow_client::Error::Protocol("file generation has a non-line format").into(),
            );
        }
    };
    Ok(Object::new()
        .with("request_id", value.get_request_id()?.to_str()?)
        .with("watch", value.get_watch()?.to_str()?)
        .with("watch_revision", value.get_watch_revision().to_string())
        .with(
            "relative_path_hex",
            purrdf_hash::hex::encode(value.get_relative_path()?),
        )
        .with("source", value.get_source()?.to_str()?)
        .with("format", format)
        .with("device", value.get_device().to_string())
        .with("inode", value.get_inode().to_string())
        .with("root_device", value.get_root_device().to_string())
        .with("root_inode", value.get_root_inode().to_string())
        .with("parent_device", value.get_parent_device().to_string())
        .with("parent_inode", value.get_parent_inode().to_string())
        .into())
}

pub(crate) fn render_position(
    value: wire::optional_tail_position::Reader<'_>,
) -> crate::AgentResult<Value> {
    Ok(match value.which()? {
        wire::optional_tail_position::Absent(()) => Value::Null,
        wire::optional_tail_position::Value(value) => {
            let value = value?;
            Object::new()
                .with("generation_id", value.get_generation_id()?.to_str()?)
                .with("declaration", render_declaration(value.get_declaration()?)?)
                .with("source_start", value.get_source_start().to_string())
                .with("source_end", value.get_source_end().to_string())
                .into()
        }
    })
}

pub(crate) fn render_generation(
    value: wire::tail_generation::Reader<'_>,
) -> crate::AgentResult<Value> {
    let expires = match value.get_replacement_eligible_at()?.which()? {
        wire::optional_instant::Absent(()) => Value::Null,
        wire::optional_instant::Value(at) => super::agent::instant(at?)?.into(),
    };
    Ok(Object::new()
        .with("store_id", value.get_store_id()?.to_str()?)
        .with("generation_id", value.get_id()?.to_str()?)
        .with("declaration", render_declaration(value.get_declaration()?)?)
        .with(
            "started_at",
            super::agent::instant(value.get_started_at()?)?,
        )
        .with("offset", value.get_offset().to_string())
        .with("records", value.get_records().to_string())
        .with("complete", value.get_complete())
        .with("gap", super::agent::optional_value(value.get_gap()?)?)
        .with("replacement_eligible_at", expires)
        .with("continuity_unknown", value.get_continuity_unknown())
        .with("finish", render_finish(value.get_finish()?)?)
        .into())
}

fn render_finish(value: wire::optional_tail_finish::Reader<'_>) -> crate::AgentResult<Value> {
    let finish = match value.which()? {
        wire::optional_tail_finish::Absent(()) => return Ok(Value::Null),
        wire::optional_tail_finish::Value(finish) => finish?,
    };
    let intent = sparrow_client::TailFinishIntent::decode(finish.get_request()?)?;
    let observed = intent.observed;
    let finished_at = match finish.get_finished_at()?.which()? {
        wire::optional_instant::Absent(()) => Value::Null,
        wire::optional_instant::Value(at) => super::agent::instant(at?)?.into(),
    };
    Ok(Object::new()
        .with("request_id", intent.request_id)
        .with("generation_id", intent.generation_id)
        .with("watch_revision", intent.watch_revision.to_string())
        .with(
            "admitted_at",
            super::agent::instant(finish.get_admitted_at()?)?,
        )
        .with("finished_at", finished_at)
        .with(
            "observed",
            Object::new()
                .with("device", observed.device.to_string())
                .with("inode", observed.inode.to_string())
                .with("byte_length", observed.length.to_string())
                .with("modified_seconds", observed.modified_seconds)
                .with("modified_nanos", observed.modified_nanos)
                .with("changed_seconds", observed.changed_seconds)
                .with("changed_nanos", observed.changed_nanos),
        )
        .into())
}
