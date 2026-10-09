// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One CLI/MCP read and renderer for certified logical retention accounting.

use super::{Result, instant, invalid, wire};
use purrdf_lex::json::{Object, Value};

pub(crate) struct Input {
    pub store: String,
    pub limits: sparrow_client::RetentionReadLimits,
}

impl Input {
    pub(super) fn parse(args: &[String]) -> Result<Self> {
        let store = args
            .first()
            .ok_or_else(|| invalid("retention-metrics requires STORE_ID"))?
            .clone();
        let mut limits = sparrow_client::RetentionReadLimits::default();
        let mut seen = 0;
        let mut fields = args[1..].iter();
        while let Some(flag) = fields.next() {
            let (bit, target) = match flag.as_str() {
                "--max-visits" => (1, &mut limits.max_visits),
                "--catalog-bytes" => (2, &mut limits.catalog_bytes),
                "--payload-bytes" => (4, &mut limits.payload_bytes),
                _ => return Err(invalid(format!("unknown retention metrics option: {flag}"))),
            };
            if seen & bit != 0 {
                return Err(invalid(format!(
                    "repeated retention metrics option: {flag}"
                )));
            }
            seen |= bit;
            *target = fields
                .next()
                .ok_or_else(|| invalid(format!("missing value for {flag}")))?
                .parse::<u32>()
                .map_err(|e| invalid(e.to_string()))?;
        }
        limits.validate()?;
        Ok(Self { store, limits })
    }

    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        let certified =
            sparrow_client::get_retention_metrics(client, &self.store, self.limits).await?;
        let value = certified.reader()?;
        let oldest = match value.get_oldest_received_at()?.which()? {
            wire::optional_instant::Absent(()) => Value::Null,
            wire::optional_instant::Value(clock) => instant(clock?)?.into(),
        };
        Ok(Object::new()
            .with("store_id", value.get_store_id()?.to_str()?)
            .with("as_of", instant(value.get_as_of()?)?)
            .with("live_records", value.get_live_records().to_string())
            .with("live_artifacts", value.get_live_artifacts().to_string())
            .with("pinned_records", value.get_pinned_records().to_string())
            .with("pinned_artifacts", value.get_pinned_artifacts().to_string())
            .with("oldest_received_at", oldest)
            .with(
                "retained_window_ms",
                value.get_retained_window_ms().to_string(),
            )
            .with(
                "owned_payload_bytes",
                value.get_owned_payload_bytes().to_string(),
            )
            .with(
                "live_payload_bytes",
                value.get_live_payload_bytes().to_string(),
            )
            .with(
                "pin_held_payload_bytes",
                value.get_pin_held_payload_bytes().to_string(),
            )
            .with(
                "retirable_payload_bytes",
                value.get_retirable_payload_bytes().to_string(),
            )
            .with(
                "unclassified_payload_bytes",
                value.get_unclassified_payload_bytes().to_string(),
            )
            .with("payload_chunks", value.get_payload_chunks().to_string())
            .with("visits", value.get_visits().to_string())
            .into())
    }
}
