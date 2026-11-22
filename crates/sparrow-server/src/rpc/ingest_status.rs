// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Typed projection of the single-lock ingestion status; no adapter accounting.

use super::{stored_format, wire};

fn counts(mut out: wire::ingest_index_counts::Builder<'_>, value: sparrow_core::IngestIndexCounts) {
    out.set_complete(value.complete);
    out.set_failed(value.failed);
    out.set_incomplete(value.incomplete);
    out.set_not_applicable(value.not_applicable);
    out.set_unavailable(value.unavailable);
    out.set_pending(value.pending);
}

pub(super) fn encode(
    mut out: wire::ingest_status::Builder<'_>,
    value: sparrow_core::IngestStatus,
) -> capnp::Result<()> {
    out.set_store_id(&value.store_id);
    let selector = match value.selector {
        sparrow_core::IngestSelector::Request(id) => sparrow_client::IngestSelector::Request(id),
        sparrow_core::IngestSelector::Import(id) => sparrow_client::IngestSelector::Import(id),
        sparrow_core::IngestSelector::Handoff(id) => sparrow_client::IngestSelector::Handoff(id),
    };
    selector
        .encode(out.reborrow().init_selector())
        .map_err(|e| capnp::Error::failed(e.to_string()))?;
    out.reborrow().init_as_of().set_unix_millis(value.as_of);
    out.set_request_id(&value.request_id);
    if let Some(id) = &value.import_id {
        out.reborrow().init_import_id().set_value(id);
    }
    if let Some(manifest) = value.staging {
        super::import_progress(
            out.reborrow().init_staging().init_value(),
            manifest,
            &value.store_id,
        )?;
    }
    if let Some(preparation) = value.preparation {
        super::import_preparation(
            out.reborrow().init_preparation().init_value(),
            preparation,
            &value.store_id,
        );
    }
    if let Some(artifact) = value.accepted {
        let mut acceptance = out.reborrow().init_accepted().init_value();
        if value.ordered_set {
            acceptance.set_kind(wire::IngestAcceptanceKind::OrderedSet);
        }
        acceptance.set_artifact_id(&artifact.id);
        acceptance.set_source(&artifact.source);
        acceptance.set_format(stored_format(&artifact.format)?);
        acceptance
            .reborrow()
            .init_received_at()
            .set_unix_millis(artifact.received_at);
        acceptance
            .reborrow()
            .init_expires_at()
            .set_unix_millis(artifact.expires_at);
        acceptance.set_byte_length(artifact.byte_length);
        acceptance.set_payload_id(&artifact.payload_id);
        acceptance.set_record_count(artifact.record_ids.len() as u64);
        if let Some(fence) = artifact.buffered {
            acceptance.set_durability(wire::Durability::Buffered);
            super::durability_watermark(acceptance.init_buffered_through().init_value(), fence)?;
        }
    }
    if let Some(tail) = value.tail {
        crate::tail_wire::generation(
            out.reborrow().init_tail().init_value(),
            &tail,
            &value.store_id,
        )?;
    }
    out.set_deletion(match value.deletion {
        sparrow_core::IngestDeletion::NotManaged => wire::IngestDeletion::NotManaged,
        sparrow_core::IngestDeletion::Retired => wire::IngestDeletion::Retired,
        sparrow_core::IngestDeletion::Reserved => wire::IngestDeletion::Reserved,
        sparrow_core::IngestDeletion::Claimed => wire::IngestDeletion::Claimed,
        sparrow_core::IngestDeletion::Published => wire::IngestDeletion::Published,
        sparrow_core::IngestDeletion::Removed => wire::IngestDeletion::Removed,
    });
    if let Some(at) = value.removed_at {
        out.reborrow()
            .init_removed_at()
            .init_value()
            .set_unix_millis(at);
    }
    if let Some(journal) = value.consumption {
        let mut ownership = out.reborrow().init_ownership().init_value();
        ownership.set_watch(&journal.watch);
        ownership.set_relative_path(&journal.relative_path);
        ownership.set_source(&journal.source);
        ownership.set_format(stored_format(&journal.format)?);
        crate::consumption::identity(&journal.observed)
            .encode(ownership.reborrow().init_observed())
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        if let Some(claimed) = journal.claimed {
            crate::consumption::identity(&claimed)
                .encode(ownership.init_claimed().init_value())
                .map_err(|e| capnp::Error::failed(e.to_string()))?;
        }
    }
    if let Some(count) = value.total_records {
        out.reborrow().init_total_records().set_known(count);
    }
    out.set_inspected_records(value.inspected_records);
    counts(out.reborrow().init_json(), value.json);
    counts(out.reborrow().init_rdf(), value.rdf);
    counts(out.init_text(), value.text);
    Ok(())
}
