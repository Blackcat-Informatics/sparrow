// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One typed file provenance adapter and agent rendering home.

use crate::sparrow_capnp as wire;
use purrdf_lex::json::{Object, Value};

pub(crate) fn intent(
    value: &sparrow_client::TailFinishIntent,
) -> Result<sparrow_core::TailFinishIntent, sparrow_core::Error> {
    Ok(sparrow_core::TailFinishIntent {
        request_id: value.request_id.clone(),
        generation_id: value.generation_id.clone(),
        watch_revision: value.watch_revision,
        observed: crate::consumption::generation(value.observed)
            .map_err(|error| sparrow_core::Error::new("INVALID_ARGUMENT", error.to_string()))?,
    })
}

fn finish_request(
    value: &sparrow_core::TailFinishIntent,
    store: &str,
) -> sparrow_client::TailFinishIntent {
    sparrow_client::TailFinishIntent {
        store_id: store.into(),
        request_id: value.request_id.clone(),
        generation_id: value.generation_id.clone(),
        watch_revision: value.watch_revision,
        observed: crate::consumption::identity(&value.observed),
    }
}

fn declaration(
    mut out: wire::tail_declaration::Builder<'_>,
    value: &sparrow_core::TailStart,
) -> capnp::Result<()> {
    out.set_request_id(&value.request_id);
    out.set_watch(&value.watch);
    out.set_watch_revision(value.watch_revision);
    out.set_relative_path(&value.path);
    out.set_source(&value.source);
    out.set_format(match value.format.as_str() {
        "log" => wire::Format::Log,
        "jsonl" => wire::Format::Jsonl,
        _ => {
            return Err(capnp::Error::failed(
                "file generation has a non-line format".into(),
            ));
        }
    });
    out.set_device(value.device);
    out.set_inode(value.inode);
    out.set_root_device(value.directories.root_device);
    out.set_root_inode(value.directories.root_inode);
    out.set_parent_device(value.directories.parent_device);
    out.set_parent_inode(value.directories.parent_inode);
    Ok(())
}

pub(crate) fn position(
    mut out: wire::optional_tail_position::Builder<'_>,
    value: Option<&sparrow_core::TailPosition>,
) -> capnp::Result<()> {
    match value {
        None => out.set_absent(()),
        Some(value) => {
            let mut out = out.reborrow().init_value();
            out.set_generation_id(&value.generation_id);
            out.set_source_start(value.source_start);
            out.set_source_end(value.source_end);
            declaration(out.init_declaration(), &value.declaration)?;
        }
    }
    Ok(())
}

pub(crate) fn generation(
    mut out: wire::tail_generation::Builder<'_>,
    value: &sparrow_core::TailGeneration,
    store: &str,
) -> capnp::Result<()> {
    out.set_store_id(store);
    out.set_id(&value.id);
    declaration(out.reborrow().init_declaration(), &value.declaration)?;
    out.reborrow()
        .init_started_at()
        .set_unix_millis(value.started_at);
    out.set_offset(value.offset);
    out.set_records(value.records);
    out.set_complete(value.complete);
    match &value.gap {
        Some(reason) => out.reborrow().init_gap().set_value(reason),
        None => out.reborrow().init_gap().set_absent(()),
    }
    match value.expires_at {
        Some(at) => out
            .reborrow()
            .init_replacement_eligible_at()
            .init_value()
            .set_unix_millis(at),
        None => out.reborrow().init_replacement_eligible_at().set_absent(()),
    }
    out.set_continuity_unknown(true);
    match &value.finish {
        None => out.reborrow().init_finish().set_absent(()),
        Some(finish) => {
            let mut out = out.reborrow().init_finish().init_value();
            finish_request(&finish.intent, store)
                .encode(out.reborrow().init_request())
                .map_err(|error| capnp::Error::failed(error.to_string()))?;
            out.reborrow()
                .init_admitted_at()
                .set_unix_millis(finish.admitted_at);
            match finish.finished_at {
                None => out.init_finished_at().set_absent(()),
                Some(at) => out.init_finished_at().init_value().set_unix_millis(at),
            }
        }
    }
    Ok(())
}

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
