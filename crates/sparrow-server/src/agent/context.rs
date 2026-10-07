// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Shared typed context operation and record metadata rendering.

use super::{Options, Result, failure, fill_scope, instant, invalid, receipt, wire};
use purrdf_lex::json::{Object, Value};

pub(crate) struct Input {
    pub store: String,
    pub id: String,
    pub options: Options,
    pub before: u32,
    pub after: u32,
}

impl Input {
    pub(super) fn validate(&self) -> Result<()> {
        self.options.validate()?;
        if !self.id.starts_with("record:")
            || u64::from(self.before) + u64::from(self.after) >= u64::from(self.options.limit)
        {
            return Err(invalid(
                "context requires a record ID and before + after + 1 <= limit",
            ));
        }
        Ok(())
    }

    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        self.validate()?;
        let mut request = client.records_context_request();
        let mut query = request.get().init_query();
        let mut anchor = query.reborrow().init_anchor();
        anchor.set_store_id(&self.store);
        anchor.set_id(&self.id);
        fill_scope(query.reborrow().init_scope(), &self.options);
        query.set_ordering(wire::ContextOrder::ReceivedAtRecordId);
        query.set_before(self.before);
        query.set_after(self.after);
        let response = request.send().promise.await?;
        let value = match response.get()?.get_result()?.which()? {
            wire::result::Ok(value) => value?,
            wire::result::Error(error) => return Err(failure(error?)),
        };
        let ordering = match value.get_ordering()? {
            wire::ContextOrder::ReceivedAtRecordId => "received_at_record_id",
        };
        Ok(Object::new()
            .with("receipt", receipt(value.get_receipt()?)?)
            .with("ordering", ordering)
            .with("continuity_unknown", value.get_continuity_unknown())
            .with("anchor", render_record(value.get_anchor()?)?)
            .with("before", render_side(value.get_before()?)?)
            .with("after", render_side(value.get_after()?)?)
            .into())
    }
}

pub(super) fn render_record(value: wire::record::Reader<'_>) -> Result<Value> {
    Ok(Object::new()
        .with("store_id", value.get_store_id()?.to_str()?)
        .with("record_id", value.get_id()?.to_str()?)
        .with("artifact_id", value.get_artifact_id()?.to_str()?)
        .with("source", value.get_source()?.to_str()?)
        .with("received_at", instant(value.get_received_at()?)?)
        .with("expires_at", instant(value.get_expires_at()?)?)
        .with("byte_start", value.get_byte_start().to_string())
        .with("byte_end", value.get_byte_end().to_string())
        .with(
            "event_time",
            super::source_time::event(value.get_event_time()?)?,
        )
        .into())
}

fn render_side(value: wire::context_side::Reader<'_>) -> Result<Value> {
    Ok(Object::new()
        .with("visited", value.get_visited().to_string())
        .with("retired", value.get_retired().to_string())
        .with(
            "frontier",
            match value.get_frontier()? {
                wire::ContextFrontier::RequestedCount => "requested_count",
                wire::ContextFrontier::ScopeBoundary => "scope_boundary",
                wire::ContextFrontier::ScanBudget => "scan_budget",
            },
        )
        .with(
            "rows",
            Value::array(
                value
                    .get_rows()?
                    .iter()
                    .map(render_record)
                    .collect::<Result<Vec<_>>>()?,
            ),
        )
        .into())
}
