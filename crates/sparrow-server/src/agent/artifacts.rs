// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One artifact-list client and renderer shared by CLI and MCP.

use super::{Result, failure, instant, invalid, optional, wire};
use purrdf_lex::json::{Object, Value};

pub(crate) struct Input {
    pub store: String,
    pub query: sparrow_core::ArtifactRead,
    pub(super) selection: Box<super::selection::Input>,
}

impl Input {
    pub(super) fn parse(args: &[String]) -> Result<Self> {
        let store = args
            .first()
            .ok_or_else(|| invalid("artifacts requires STORE_ID"))?
            .clone();
        let mut query = sparrow_core::ArtifactRead::default();
        let mut selection = Box::<super::selection::Input>::default();
        let mut fields = args[1..].iter();
        while let Some(flag) = fields.next() {
            if flag == "--require-complete-index" {
                query.selector.require_complete_index = true;
                continue;
            }
            let value = fields
                .next()
                .ok_or_else(|| invalid(format!("missing value for {flag}")))?;
            if super::catalog::option(
                flag,
                value,
                &mut query.selector.source,
                &mut query.cursor,
                &mut query.page_size,
                &mut query.scan_budget,
            )? {
                continue;
            }
            match flag.as_str() {
                "--scope-file" => {
                    selection = Box::new(
                        super::selection::decode(&super::records::read_control_file(value, 8)?)
                            .map_err(|error| invalid(error.to_string()))?,
                    )
                }
                "--received-from" => {
                    query.selector.received_from =
                        sparrow_core::parse_instant(value).map_err(|e| invalid(e.to_string()))?
                }
                "--received-before" => {
                    query.selector.received_before =
                        sparrow_core::parse_instant(value).map_err(|e| invalid(e.to_string()))?
                }
                _ => return Err(invalid(format!("unknown artifact list option: {flag}"))),
            }
        }
        Ok(Self {
            store,
            query,
            selection,
        })
    }

    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        sparrow_client::Discovery::fetch(client).await?;
        let mut request = client.artifacts_list_request();
        let mut query = request.get().init_query();
        query.set_store_id(&self.store);
        query.set_schema_version(sparrow_client::SCHEMA_VERSION);
        let mut scope = query.reborrow().init_scope();
        scope.set_schema_version(sparrow_client::SCHEMA_VERSION);
        scope
            .reborrow()
            .init_received_before()
            .set_unix_millis(u64::MAX);
        scope.set_require_complete_index(self.query.selector.require_complete_index);
        self.selection.encode(scope)?;
        optional(query.reborrow().init_source(), &self.query.selector.source);
        optional(query.reborrow().init_cursor(), &self.query.cursor);
        let mut received = query.reborrow().init_received();
        if self.query.selector.received_from == 0 && self.query.selector.received_before == u64::MAX
        {
            received.set_unfiltered(());
        } else {
            let mut interval = received.init_interval();
            interval
                .reborrow()
                .init_from()
                .set_unix_millis(self.query.selector.received_from);
            interval
                .init_before()
                .set_unix_millis(self.query.selector.received_before);
        }
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

pub(super) fn render(page: wire::artifact_list_page::Reader<'_>) -> Result<Value> {
    let mut rows = Vec::new();
    for row in page.get_rows()? {
        let origin = match row.get_origin()?.which()? {
            wire::optional_artifact_origin::Absent(()) => Value::Null,
            wire::optional_artifact_origin::Value(value) => {
                let value = value?;
                Object::new()
                    .with("format", sparrow_client::format_label(value.get_format()?))
                    .with("received_at", instant(value.get_received_at()?)?)
                    .with("expires_at", instant(value.get_expires_at()?)?)
                    .with("byte_length", value.get_byte_length().to_string())
                    .with("original_records", value.get_original_records().to_string())
                    .into()
            }
        };
        rows.push(Value::from(
            Object::new()
                .with("artifact_id", row.get_id()?.to_str()?)
                .with("source", row.get_source()?.to_str()?)
                .with("origin", origin)
                .with(
                    "complete_bytes_available",
                    row.get_complete_bytes_available(),
                )
                .with("retained_records", row.get_retained_records().to_string())
                .with(
                    "coverage",
                    Value::from(super::render_membership_coverage(
                        row.get_coverage()?,
                        row.get_matching_records()?,
                    )?),
                ),
        ));
    }
    let coverage = page.get_coverage()?;
    let complete = coverage.get_membership_complete();
    Ok(super::catalog::receipt(page.get_receipt()?)?
        .with(
            "answer_certainty",
            if complete { "exact" } else { "incomplete" },
        )
        .with(
            "known_matching_artifacts",
            page.get_receipt()?.get_total_items().to_string(),
        )
        .with(
            "matching_artifacts",
            if complete {
                Value::from(page.get_receipt()?.get_total_items().to_string())
            } else {
                Value::Null
            },
        )
        .with(
            "coverage",
            Value::from(
                super::render_membership_coverage(coverage, page.get_matching_records()?)?
                    .with("catalog_complete", true)
                    .with("scanned", page.get_receipt()?.get_scanned().to_string()),
            ),
        )
        .with("operation", "artifacts_list")
        .with("rows", Value::array(rows))
        .with("profile_ids", Value::array(["sparrow-artifact-catalog-v2"]))
        .into())
}
