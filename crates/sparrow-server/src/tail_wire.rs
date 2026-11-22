// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Encode and decode typed file provenance.

use crate::sparrow_capnp as wire;

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
