// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Shared CLI/MCP rendering of the certified combined ingestion read.

use super::{Result, instant, invalid, optional_value, wire};
use purrdf_lex::json::{Object, Value};

pub(crate) struct Input {
    pub store: String,
    pub selector: sparrow_client::IngestSelector,
    pub limits: sparrow_client::IngestReadLimits,
}

impl Input {
    pub(super) fn parse(args: &[String]) -> Result<Self> {
        let store = args
            .first()
            .ok_or_else(|| invalid("ingest-get requires STORE_ID and one selector"))?
            .clone();
        let mut selector = None;
        let mut limits = sparrow_client::IngestReadLimits::default();
        let mut fields = args[1..].iter();
        while let Some(flag) = fields.next() {
            if !flag.starts_with("--") {
                if selector
                    .replace(sparrow_client::IngestSelector::Import(flag.clone()))
                    .is_some()
                {
                    return Err(invalid("ingest-get requires exactly one selector"));
                }
                continue;
            }
            let value = fields
                .next()
                .ok_or_else(|| invalid(format!("missing value for {flag}")))?;
            let selected = match flag.as_str() {
                "--request-id" => Some(sparrow_client::IngestSelector::Request(value.clone())),
                "--import-id" => Some(sparrow_client::IngestSelector::Import(value.clone())),
                "--handoff-request-id" => {
                    Some(sparrow_client::IngestSelector::Handoff(value.clone()))
                }
                "--scan-budget" => {
                    limits.scan_budget =
                        value.parse::<u32>().map_err(|e| invalid(e.to_string()))?;
                    None
                }
                "--catalog-bytes" => {
                    limits.catalog_bytes =
                        value.parse::<u32>().map_err(|e| invalid(e.to_string()))?;
                    None
                }
                _ => return Err(invalid(format!("unknown ingestion status option: {flag}"))),
            };
            if let Some(selected) = selected
                && selector.replace(selected).is_some()
            {
                return Err(invalid("ingest-get requires exactly one selector"));
            }
        }
        let selector =
            selector.ok_or_else(|| invalid("ingest-get requires exactly one selector"))?;
        selector.validate()?;
        Ok(Self {
            store,
            selector,
            limits,
        })
    }

    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        let certified =
            sparrow_client::get_ingest_status(client, &self.store, &self.selector, self.limits)
                .await?;
        let p = certified.reader()?;
        let selector = match &self.selector {
            sparrow_client::IngestSelector::Request(id) => {
                Object::new().with("request_id", id.clone())
            }
            sparrow_client::IngestSelector::Import(id) => {
                Object::new().with("import_id", id.clone())
            }
            sparrow_client::IngestSelector::Handoff(id) => {
                Object::new().with("handoff_request_id", id.clone())
            }
        };
        let staging = match p.get_staging()?.which()? {
            wire::optional_import_progress::Absent(()) => Value::Null,
            wire::optional_import_progress::Value(s) => {
                let s = s?;
                Object::new()
                    .with("source", s.get_source()?.to_str()?)
                    .with("format", sparrow_client::format_label(s.get_format()?))
                    .with("byte_length", s.get_byte_length().to_string())
                    .with("payload_id", s.get_payload_id()?.to_str()?)
                    .with("received_bytes", s.get_received_bytes().to_string())
                    .with("expires_at", instant(s.get_expires_at()?)?)
                    .with("sealed", s.get_sealed())
                    .with("layout_complete", s.get_layout_complete())
                    .with("layout_records", s.get_layout_records().to_string())
                    .with(
                        "published_artifact",
                        optional_value(s.get_published_artifact()?)?,
                    )
                    .into()
            }
        };
        let preparation = match p.get_preparation()?.which()? {
            wire::optional_import_preparation::Absent(()) => Value::Null,
            wire::optional_import_preparation::Value(r) => {
                let r = r?;
                let raw = match r.get_raw()?.which()? {
                    wire::optional_raw_preparation::Absent(()) => Value::Null,
                    wire::optional_raw_preparation::Value(raw) => {
                        let raw = raw?;
                        Object::new()
                            .with("record_index", raw.get_record_index().to_string())
                            .with(
                                "reserved_record_id",
                                raw.get_reserved_record_id()?.to_str()?,
                            )
                            .with("prepared_chunks", raw.get_prepared_chunks().to_string())
                            .with("total_chunks", raw.get_total_chunks().to_string())
                            .into()
                    }
                };
                Object::new()
                    .with(
                        "reserved_artifact_id",
                        r.get_reserved_artifact_id()?.to_str()?,
                    )
                    .with("prepared_records", r.get_prepared_records().to_string())
                    .with("total_records", r.get_total_records().to_string())
                    .with("received_at", instant(r.get_received_at()?)?)
                    .with("expires_at", instant(r.get_expires_at()?)?)
                    .with(
                        "source_time_profile",
                        optional_value(r.get_source_time_profile()?)?,
                    )
                    .with(
                        "diagnostic_profile",
                        optional_value(r.get_diagnostic_profile()?)?,
                    )
                    .with("raw", raw)
                    .into()
            }
        };
        let accepted = match p.get_accepted()?.which()? {
            wire::optional_ingest_acceptance::Absent(()) => Value::Null,
            wire::optional_ingest_acceptance::Value(a) => {
                let a = a?;
                let buffered = match a.get_buffered_through()?.which()? {
                    wire::optional_durability_watermark::Absent(()) => Value::Null,
                    wire::optional_durability_watermark::Value(f) => super::durability::watermark(
                        &sparrow_client::DurabilityWatermark::decode(f?)?,
                    ),
                };
                Object::new()
                    .with("artifact_id", a.get_artifact_id()?.to_str()?)
                    .with(
                        "kind",
                        match a.get_kind()? {
                            wire::IngestAcceptanceKind::Artifact => "artifact",
                            wire::IngestAcceptanceKind::OrderedSet => "ordered_set",
                        },
                    )
                    .with("source", a.get_source()?.to_str()?)
                    .with("format", sparrow_client::format_label(a.get_format()?))
                    .with("received_at", instant(a.get_received_at()?)?)
                    .with("expires_at", instant(a.get_expires_at()?)?)
                    .with("byte_length", a.get_byte_length().to_string())
                    .with("payload_id", a.get_payload_id()?.to_str()?)
                    .with("record_count", a.get_record_count().to_string())
                    .with(
                        "durability",
                        match a.get_durability()? {
                            wire::Durability::Buffered => "buffered",
                            wire::Durability::Durable => "durable",
                        },
                    )
                    .with("buffered_through", buffered)
                    .into()
            }
        };
        let ownership = match p.get_ownership()?.which()? {
            wire::optional_ingest_ownership::Absent(()) => Value::Null,
            wire::optional_ingest_ownership::Value(o) => {
                let o = o?;
                let claimed = match o.get_claimed()?.which()? {
                    wire::optional_file_generation::Absent(()) => Value::Null,
                    wire::optional_file_generation::Value(g) => observation(g?)?,
                };
                Object::new()
                    .with("watch", o.get_watch()?.to_str()?)
                    .with(
                        "relative_path_hex",
                        purrdf_hash::hex::encode(o.get_relative_path()?),
                    )
                    .with("source", o.get_source()?.to_str()?)
                    .with("format", sparrow_client::format_label(o.get_format()?))
                    .with("observed", observation(o.get_observed()?)?)
                    .with("claimed", claimed)
                    .into()
            }
        };
        let tail = match p.get_tail()?.which()? {
            wire::optional_tail_generation::Absent(()) => Value::Null,
            wire::optional_tail_generation::Value(t) => crate::tail_wire::render_generation(t?)?,
        };
        let removed = match p.get_removed_at()?.which()? {
            wire::optional_instant::Absent(()) => Value::Null,
            wire::optional_instant::Value(at) => instant(at?)?.into(),
        };
        let total = match p.get_total_records()?.which()? {
            wire::optional_record_count::Unknown(()) => Value::Null,
            wire::optional_record_count::Known(n) => n.to_string().into(),
        };
        Ok(Object::new()
            .with("store_id", p.get_store_id()?.to_str()?)
            .with("selector", selector)
            .with("as_of", instant(p.get_as_of()?)?)
            .with("request_id", p.get_request_id()?.to_str()?)
            .with("import_id", optional_value(p.get_import_id()?)?)
            .with("staging", staging)
            .with("preparation", preparation)
            .with("accepted", accepted)
            .with("tail", tail)
            .with("ownership", ownership)
            .with(
                "deletion",
                match p.get_deletion()? {
                    wire::IngestDeletion::NotManaged => "not_managed",
                    wire::IngestDeletion::Reserved => "reserved",
                    wire::IngestDeletion::Claimed => "claimed",
                    wire::IngestDeletion::Published => "published",
                    wire::IngestDeletion::Removed => "removed",
                    wire::IngestDeletion::Retired => "retired_unrecoverable",
                },
            )
            .with("removed_at", removed)
            .with("total_records", total)
            .with("inspected_records", p.get_inspected_records().to_string())
            .with("json", counts(p.get_json()?))
            .with("rdf", counts(p.get_rdf()?))
            .with("text", counts(p.get_text()?))
            .into())
    }
}

fn counts(c: wire::ingest_index_counts::Reader<'_>) -> Value {
    Object::new()
        .with("complete", c.get_complete().to_string())
        .with("failed", c.get_failed().to_string())
        .with("incomplete", c.get_incomplete().to_string())
        .with("not_applicable", c.get_not_applicable().to_string())
        .with("unavailable", c.get_unavailable().to_string())
        .with("pending", c.get_pending().to_string())
        .into()
}

fn observation(value: wire::file_generation::Reader<'_>) -> Result<Value> {
    let g = sparrow_client::CompletedFileIdentity::decode(value)?;
    Ok(Object::new()
        .with("device", g.device.to_string())
        .with("inode", g.inode.to_string())
        .with("byte_length", g.length.to_string())
        .with("modified_seconds", g.modified_seconds.to_string())
        .with("modified_nanos", g.modified_nanos as u64)
        .with("changed_seconds", g.changed_seconds.to_string())
        .with("changed_nanos", g.changed_nanos as u64)
        .into())
}
