// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One generated-client catalog read and renderer for CLI and MCP.

use super::super::{Result, failure, instant, invalid, optional, optional_value, wire};
use purrdf_lex::json::{Object, Value};
use sparrow_core::PinSelector;

pub(crate) struct Read {
    pub store: String,
    pub selector: PinSelector,
    pub cursor: Option<String>,
    pub page_size: u32,
    pub scan_budget: u32,
}

impl Read {
    pub(crate) fn parse(args: &[String], get: bool) -> Result<Self> {
        let mut input = Self {
            store: args
                .first()
                .ok_or_else(|| invalid("pin read requires STORE_ID"))?
                .clone(),
            selector: PinSelector::Active,
            cursor: None,
            page_size: 200,
            scan_budget: 10000,
        };
        let mut seen = std::collections::BTreeSet::new();
        let mut fields = args[1..].iter();
        let mut selector = None;
        while let Some(flag) = fields.next() {
            if !seen.insert(flag) {
                return Err(invalid(format!("duplicate option: {flag}")));
            }
            let value = fields
                .next()
                .ok_or_else(|| invalid(format!("missing value for {flag}")))?;
            match flag.as_str() {
                "--id" | "--name" if get => {
                    if selector.is_some() {
                        return Err(invalid("pin-get requires exactly one --id or --name"));
                    }
                    selector = Some(if flag == "--id" {
                        PinSelector::Id(value.clone())
                    } else {
                        PinSelector::Name(value.clone())
                    });
                }
                "--cursor" => input.cursor = Some(value.clone()),
                "--page-size" => {
                    input.page_size = value
                        .parse()
                        .map_err(|_| invalid("--page-size requires a UInt32 integer"))?
                }
                "--scan-budget" => {
                    input.scan_budget = value
                        .parse()
                        .map_err(|_| invalid("--scan-budget requires a UInt32 integer"))?
                }
                _ => return Err(invalid(format!("unknown pin read option: {flag}"))),
            }
        }
        if get {
            input.selector =
                selector.ok_or_else(|| invalid("pin-get requires exactly one --id or --name"))?;
        }
        Ok(input)
    }

    pub(crate) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        let mut request = client.pin_read_request();
        let mut query = request.get().init_query();
        query.set_store_id(&self.store);
        query.set_page_size(self.page_size);
        query.set_scan_budget(self.scan_budget);
        optional(query.reborrow().init_cursor(), &self.cursor);
        let mut selector = query.init_selector();
        match &self.selector {
            PinSelector::Active => selector.set_active(()),
            PinSelector::Id(value) => selector.set_id(value),
            PinSelector::Name(value) => selector.set_name(value),
        }
        let response = request.send().promise.await?;
        let page = match response.get()?.get_result()?.which()? {
            wire::result::Ok(value) => value?,
            wire::result::Error(error) => return Err(failure(error?)),
        };
        let receipt = page.get_receipt()?;
        let next = optional_value(receipt.get_next_cursor()?)?;
        let mut out = Object::new()
            .with("store_id", receipt.get_store_id()?.to_str()?)
            .with("snapshot_id", receipt.get_snapshot_id()?.to_str()?)
            .with("as_of", instant(receipt.get_as_of()?)?)
            .with(
                "lease_expires_at",
                instant(receipt.get_lease_expires_at()?)?,
            )
            .with("total_items", receipt.get_total_items().to_string())
            .with("page_start", receipt.get_page_start().to_string())
            .with("has_more", next != Value::Null)
            .with("next_cursor", next)
            .with("execution_status", "complete")
            .with("answer_certainty", "exact")
            .with(
                "coverage",
                Object::new()
                    .with("catalog_complete", true)
                    .with("scanned", receipt.get_scanned().to_string()),
            )
            .with(
                "limits",
                Object::new()
                    .with("page_size", u64::from(receipt.get_page_size()))
                    .with("scan_budget", u64::from(receipt.get_scan_budget()))
                    .with("page_bytes", sparrow_core::PIN_READ_PAGE_BYTES as u64),
            )
            .with("stop_reason", Value::Null)
            .with("profile_ids", Value::array(["sparrow-pin-catalog-v1"]));
        match page.which()? {
            wire::pin_read_page::Collections(rows) => {
                let rows = rows?.iter().map(summary).collect::<Result<Vec<_>>>()?;
                out = out
                    .with("operation", "pins_list")
                    .with("rows", Value::array(rows));
            }
            wire::pin_read_page::Members(body) => {
                let body = body?;
                let members = super::member_ids(body.get_members()?)?;
                out = out
                    .with("operation", "pin_get")
                    .with("pin", summary(body.get_pin()?)?)
                    .with("members", members);
            }
        }
        Ok(out.into())
    }
}

fn summary(value: wire::pin_summary::Reader<'_>) -> Result<Value> {
    Ok(Object::new()
        .with("pin_id", value.get_id()?.to_str()?)
        .with("name", value.get_name()?.to_str()?)
        .with("expires_at", instant(value.get_expires_at()?)?)
        .with("revision", value.get_revision().to_string())
        .with(
            "direct_member_count",
            value.get_direct_member_count().to_string(),
        )
        .into())
}
