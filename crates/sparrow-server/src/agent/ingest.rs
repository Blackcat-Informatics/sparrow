// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{Result, invalid, wire};
use purrdf_lex::json::{Object, Value};
use std::path::PathBuf;

pub(crate) struct Input {
    request: String,
    source: String,
    format: wire::Format,
    path: PathBuf,
}

impl Input {
    pub(super) fn parse(args: &[String]) -> Result<Self> {
        if args.len() != 4 {
            return Err(invalid("usage: ingest REQUEST_ID SOURCE FORMAT FILE"));
        }
        Ok(Self {
            request: args[0].clone(),
            source: args[1].clone(),
            format: sparrow_client::parse_format(&args[2]).map_err(|e| invalid(e.to_string()))?,
            path: args[3].clone().into(),
        })
    }

    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        render_receipt(
            sparrow_client::import_file(
                client,
                &self.request,
                &self.source,
                self.format,
                &self.path,
            )
            .await?,
        )
    }
}

pub(super) fn render(artifact: wire::artifact::Reader<'_>) -> Result<Value> {
    render_receipt(sparrow_client::ArtifactReceipt::decode(artifact)?)
}

fn render_receipt(artifact: sparrow_client::ArtifactReceipt) -> Result<Value> {
    let records = artifact
        .record_ids
        .into_iter()
        .map(Value::from)
        .collect::<Vec<_>>();
    Ok(Object::new()
        .with("store_id", artifact.store_id)
        .with("artifact_id", artifact.id)
        .with("source", artifact.source)
        .with("format", sparrow_client::format_label(artifact.format))
        .with(
            "received_at",
            sparrow_core::format_instant(artifact.received_at)
                .map_err(|e| invalid(e.to_string()))?,
        )
        .with(
            "expires_at",
            sparrow_core::format_instant(artifact.expires_at)
                .map_err(|e| invalid(e.to_string()))?,
        )
        .with("byte_length", artifact.byte_length.to_string())
        .with("payload_id", artifact.payload_id)
        .with("record_ids", Value::array(records))
        .with(
            "durability",
            match artifact.durability {
                wire::Durability::Buffered => "buffered",
                wire::Durability::Durable => "durable",
            },
        )
        .into())
}
