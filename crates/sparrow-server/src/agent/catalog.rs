// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Shared rendering of immutable catalog receipts.

use super::{Result, instant, optional_value, wire};
use purrdf_lex::json::{Object, Value};

pub(super) fn receipt(value: wire::catalog_read_receipt::Reader<'_>) -> Result<Object> {
    let next = optional_value(value.get_next_cursor()?)?;
    Ok(Object::new()
        .with("store_id", value.get_store_id()?.to_str()?)
        .with("snapshot_id", value.get_snapshot_id()?.to_str()?)
        .with("as_of", instant(value.get_as_of()?)?)
        .with("lease_expires_at", instant(value.get_lease_expires_at()?)?)
        .with("total_items", value.get_total_items().to_string())
        .with("page_start", value.get_page_start().to_string())
        .with("has_more", next != Value::Null)
        .with("next_cursor", next)
        .with("execution_status", "complete")
        .with("answer_certainty", "exact")
        .with(
            "coverage",
            Object::new()
                .with("catalog_complete", true)
                .with("scanned", value.get_scanned().to_string()),
        )
        .with(
            "limits",
            Object::new()
                .with("page_size", u64::from(value.get_page_size()))
                .with("scan_budget", u64::from(value.get_scan_budget()))
                .with("page_bytes", sparrow_core::PIN_READ_PAGE_BYTES as u64),
        )
        .with("stop_reason", Value::Null))
}

/// Pagination metadata is separate from the query's semantic certainty and coverage.
pub(super) fn page(value: wire::catalog_read_receipt::Reader<'_>) -> Result<Object> {
    let mut page = receipt(value)?;
    page.remove("execution_status");
    page.remove("answer_certainty");
    page.remove("coverage");
    Ok(page)
}

/// Parse the shared CLI options for bounded catalog captures.
pub(super) fn option(
    flag: &str,
    value: &str,
    source: &mut Option<String>,
    cursor: &mut Option<String>,
    page_size: &mut usize,
    scan_budget: &mut usize,
) -> Result<bool> {
    match flag {
        "--source" => *source = Some(value.to_owned()),
        "--cursor" => *cursor = Some(value.to_owned()),
        "--page-size" => {
            *page_size = value
                .parse()
                .map_err(|_| super::invalid("page-size requires an integer"))?
        }
        "--scan-budget" => {
            *scan_budget = value
                .parse()
                .map_err(|_| super::invalid("scan-budget requires an integer"))?
        }
        _ => return Ok(false),
    }
    Ok(true)
}
