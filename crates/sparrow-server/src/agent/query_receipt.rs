// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Frozen capture provenance, preserving the original semantic certainty and coverage.

use super::{Result, instant, optional_value, wire};
use purrdf_lex::json::{Object, Value};

fn limit(value: wire::optional_limit::Reader<'_>) -> Result<Value> {
    Ok(match value.which()? {
        wire::optional_limit::Absent(()) => Value::Null,
        wire::optional_limit::Value(value) => value.to_string().into(),
    })
}

pub(super) fn apply(out: Object, capture: wire::query_capture::Reader<'_>) -> Result<Object> {
    let page = capture.get_page()?;
    let profiles = capture
        .get_profile_ids()?
        .iter()
        .map(|profile| {
            Ok(Value::from(
                Object::new()
                    .with(
                        "role",
                        match profile.get_role()? {
                            wire::QueryProfileRole::JsonProjection => "json_projection",
                            wire::QueryProfileRole::HostAdmission => "host_admission",
                            wire::QueryProfileRole::TextAnalyzer => "text_analyzer",
                            wire::QueryProfileRole::TextRanking => "text_ranking",
                            wire::QueryProfileRole::TextCorpus => "text_corpus",
                        },
                    )
                    .with("id", profile.get_id()?.to_str()?),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let limits = capture.get_limits()?;
    Ok(out
        .with("snapshot_id", page.get_snapshot_id()?.to_str()?)
        .with("as_of", instant(page.get_as_of()?)?)
        .with("lease_expires_at", instant(page.get_lease_expires_at()?)?)
        .with("execution_status", "complete")
        .with("stop_reason", Value::Null)
        .with("profile_ids", Value::array(profiles))
        .with(
            "native_governor",
            optional_value(capture.get_native_governor()?)?,
        )
        .with(
            "limits",
            Object::new()
                .with("page_size", u64::from(page.get_page_size()))
                .with(
                    "candidate_visits",
                    limits.get_candidate_visits().to_string(),
                )
                .with("json_value_visits", limit(limits.get_json_value_visits()?)?)
                .with("field_visits", limit(limits.get_field_visits()?)?)
                .with("posting_visits", limit(limits.get_posting_visits()?)?)
                .with(
                    "text_membership_bytes",
                    limit(limits.get_text_membership_bytes()?)?,
                )
                .with("query_text_bytes", limit(limits.get_query_text_bytes()?)?)
                .with("query_terms", limit(limits.get_query_terms()?)?)
                .with("field_constraints", limit(limits.get_field_constraints()?)?)
                .with(
                    "compiled_field_bytes",
                    limit(limits.get_compiled_field_bytes()?)?,
                )
                .with(
                    "membership_member_visits",
                    limits.get_membership_member_visits().to_string(),
                )
                .with(
                    "membership_record_visits",
                    limits.get_membership_record_visits().to_string(),
                )
                .with("capture_items", limits.get_capture_items().to_string())
                .with("capture_bytes", limits.get_capture_bytes().to_string())
                .with("row_bytes", limits.get_row_bytes().to_string())
                .with("metadata_bytes", limits.get_metadata_bytes().to_string())
                .with("page_bytes", limits.get_page_bytes().to_string())
                .with("result_handles", limits.get_result_handles().to_string())
                .with("result_bytes", limits.get_result_bytes().to_string())
                .with("lease_ms", limits.get_lease_ms().to_string()),
        ))
}
