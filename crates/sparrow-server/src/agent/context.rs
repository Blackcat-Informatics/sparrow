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
        fill_scope(query.reborrow().init_scope(), &self.options)?;
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
            .with("receipt", capture_receipt(value)?)
            .with("ordering", ordering)
            .with("continuity_unknown", value.get_continuity_unknown())
            .with("anchor", render_record(value.get_anchor()?)?)
            .with("before", render_side(value.get_before()?)?)
            .with("after", render_side(value.get_after()?)?)
            .into())
    }
}

pub(super) fn render_record(value: wire::record::Reader<'_>) -> Result<Value> {
    let receiver = match value.get_receiver()?.which()? {
        wire::optional_receiver_position::Absent(()) => Value::Null,
        wire::optional_receiver_position::Value(position) => {
            let position = position?;
            Object::new()
                .with("window_id", position.get_window_id()?.to_str()?)
                .with("sequence", position.get_sequence().to_string())
                .into()
        }
    };
    let producer = match value.get_producer()?.which()? {
        wire::optional_producer_position::Absent(()) => Value::Null,
        wire::optional_producer_position::Value(position) => {
            let position = position?;
            Object::new()
                .with("stream_id", position.get_stream_id()?.to_str()?)
                .with("sequence", position.get_sequence().to_string())
                .into()
        }
    };
    Ok(Object::new()
        .with(
            "tail",
            crate::tail_wire::render_position(value.get_tail()?)?,
        )
        .with("syslog", crate::syslog_wire::render(value.get_syslog()?)?)
        .with("producer", producer)
        .with("receiver", receiver)
        .with("store_id", value.get_store_id()?.to_str()?)
        .with("record_id", value.get_id()?.to_str()?)
        .with("artifact_id", value.get_artifact_id()?.to_str()?)
        .with("source", value.get_source()?.to_str()?)
        .with("received_at", instant(value.get_received_at()?)?)
        .with("expires_at", instant(value.get_expires_at()?)?)
        .with("byte_start", value.get_byte_start().to_string())
        .with("byte_end", value.get_byte_end().to_string())
        .with(
            "diagnostics",
            crate::diagnostics::render_snapshot(value.get_diagnostics()?)?,
        )
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

fn capture_receipt(value: wire::context::Reader<'_>) -> Result<Value> {
    let capture = value.get_capture()?;
    let mut result = receipt(value.get_receipt()?)?;
    let out = result
        .as_object_mut()
        .ok_or_else(|| invalid("context receipt must be an object"))?;
    let limited = value.get_before()?.get_frontier()? == wire::ContextFrontier::ScanBudget
        || value.get_after()?.get_frontier()? == wire::ContextFrontier::ScanBudget;
    out.insert("snapshot_id", capture.get_snapshot_id()?.to_str()?);
    out.insert("as_of", instant(capture.get_as_of()?)?);
    out.insert(
        "execution_status",
        if limited { "partial" } else { "complete" },
    );
    out.insert(
        "stop_reason",
        if limited {
            "direction_scan_budget".into()
        } else {
            Value::Null
        },
    );
    out.insert(
        "native_governor",
        super::optional_value(capture.get_native_governor()?)?,
    );
    out.insert("has_more", false);
    out.insert("next_cursor", Value::Null);
    out.insert(
        "profile_ids",
        Value::array(vec![
            Object::new()
                .with("role", "host_admission")
                .with("id", capture.get_host_profile()?.to_str()?),
        ]),
    );
    let limits = capture.get_limits()?;
    out.insert(
        "limits",
        Object::new()
            .with(
                "direction_visits",
                limits.get_direction_visits().to_string(),
            )
            .with("catalog_bytes", limits.get_catalog_bytes().to_string())
            .with("response_bytes", limits.get_response_bytes().to_string())
            .with("output_records", u64::from(limits.get_output_records()))
            .with("requested_before", u64::from(limits.get_requested_before()))
            .with("requested_after", u64::from(limits.get_requested_after()))
            .with(
                "membership_member_visits",
                limits.get_membership_member_visits().to_string(),
            )
            .with(
                "membership_record_visits",
                limits.get_membership_record_visits().to_string(),
            ),
    );
    Ok(result)
}
