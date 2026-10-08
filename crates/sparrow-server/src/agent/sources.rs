// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One source-list client and renderer shared by CLI and MCP.

use super::{Result, failure, invalid, optional, optional_value, wire};
use purrdf_lex::json::{Object, Value};

pub(crate) struct Input {
    pub store: String,
    pub query: sparrow_core::SourceRead,
}
impl Input {
    pub(super) fn parse(args: &[String]) -> Result<Self> {
        let store = args
            .first()
            .ok_or_else(|| invalid("sources requires STORE_ID"))?
            .clone();
        let mut query = sparrow_core::SourceRead::default();
        let mut fields = args[1..].iter();
        while let Some(flag) = fields.next() {
            let value = fields
                .next()
                .ok_or_else(|| invalid(format!("missing value for {flag}")))?;
            if !super::catalog::option(
                flag,
                value,
                &mut query.selector.source,
                &mut query.cursor,
                &mut query.page_size,
                &mut query.scan_budget,
            )? {
                return Err(invalid(format!("unknown source list option: {flag}")));
            }
        }
        Ok(Self { store, query })
    }
    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        sparrow_client::Discovery::fetch(client).await?;
        let mut request = client.sources_list_request();
        let mut query = request.get().init_query();
        query.set_store_id(&self.store);
        query.set_schema_version(sparrow_client::SCHEMA_VERSION);
        optional(query.reborrow().init_source(), &self.query.selector.source);
        optional(query.reborrow().init_cursor(), &self.query.cursor);
        query.set_page_size(
            u32::try_from(self.query.page_size).map_err(|_| invalid("page-size exceeds UInt32"))?,
        );
        query.set_scan_budget(
            u32::try_from(self.query.scan_budget)
                .map_err(|_| invalid("scan-budget exceeds UInt32"))?,
        );
        let response = request.send().promise.await?;
        let page = match response.get()?.get_result()?.which()? {
            wire::result::Ok(value) => value?,
            wire::result::Error(error) => return Err(failure(error?)),
        };
        render(page)
    }
}

pub(super) fn render(page: wire::source_list_page::Reader<'_>) -> Result<Value> {
    let mut rows = Vec::new();
    for row in page.get_rows()? {
        let mut watches = Vec::new();
        for watch in row.get_watches()? {
            watches.push(Value::from(
                Object::new()
                    .with("watch_id", watch.get_watch_id()?.to_str()?)
                    .with("revision", watch.get_revision().to_string())
                    .with("enabled", watch.get_enabled()),
            ));
        }
        rows.push(Value::from(
            Object::new()
                .with(
                    "syslog_listener_ids",
                    row.get_syslog_listener_ids()?
                        .iter()
                        .map(|id| Ok(id?.to_str()?.to_owned()))
                        .collect::<Result<Vec<_>>>()?,
                )
                .with(
                    "tail_generation_ids",
                    row.get_tail_generation_ids()?
                        .iter()
                        .map(|id| Ok(id?.to_str()?.to_owned()))
                        .collect::<Result<Vec<_>>>()?,
                )
                .with(
                    "producer_stream_ids",
                    row.get_producer_stream_ids()?
                        .iter()
                        .map(|id| Ok(id?.to_str()?.to_owned()))
                        .collect::<Result<Vec<_>>>()?,
                )
                .with("source_id", row.get_source_id()?.to_str()?)
                .with("rdf_iri", row.get_rdf_iri()?.to_str()?)
                .with(
                    "complete_artifacts",
                    row.get_complete_artifacts().to_string(),
                )
                .with("fragment_parents", row.get_fragment_parents().to_string())
                .with("retained_records", row.get_retained_records().to_string())
                .with(
                    "time_profile_id",
                    optional_value(row.get_time_profile_id()?)?,
                )
                .with(
                    "diagnostic_profile_id",
                    optional_value(row.get_diagnostic_profile_id()?)?,
                )
                .with("continuity_unknown", row.get_continuity_unknown())
                .with("watches", Value::array(watches)),
        ));
    }
    Ok(super::catalog::receipt(page.get_receipt()?)?
        .with("operation", "sources_list")
        .with("rows", Value::array(rows))
        .with("profile_ids", Value::array(["sparrow-source-catalog-v1"]))
        .into())
}
