// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Frozen capture provenance, preserving the original semantic certainty and coverage.

use super::{Result, instant, optional_value, render_count, wire};
use purrdf_lex::json::{Object, Value};

pub(super) fn scope_receipt(
    scope: wire::query_scope_receipt::Reader<'_>,
    as_of: u64,
) -> Result<Value> {
    let profile = scope.get_profile()?.to_str()?;
    let id = scope.get_id()?.to_str()?;
    let selection = scope.get_selection()?;
    let pin = scope.get_pin()?.which()?;
    let direct = render_count(scope.get_direct_members()?)?;
    let expanded = render_count(scope.get_expanded_records()?)?;
    let live = selection == wire::ScopeSelectionKind::Live;
    let pin_selected = selection == wire::ScopeSelectionKind::Pin;
    if profile != sparrow_core::QUERY_SCOPE_PROFILE
        || !id
            .strip_prefix("scope:")
            .is_some_and(|hash| purrdf_hash::hex::decode_32_canonical(hash).is_some())
        || direct.is_null() != live
        || expanded.is_null() != live
        || matches!(&pin, wire::optional_captured_pin::Value(_)) != pin_selected
        || scope.get_received_from_ms() >= scope.get_received_before_ms()
        || scope.get_source_alternatives() > 128
        || scope.get_diagnostic_dimensions() > 5
    {
        return Err(sparrow_client::Error::Protocol("invalid frozen scope receipt").into());
    }
    let pin = match pin {
        wire::optional_captured_pin::Absent(()) => Value::Null,
        wire::optional_captured_pin::Value(pin) => {
            let pin = pin?;
            let id = pin.get_id()?.to_str()?;
            if !id.starts_with("pin:")
                || id.len() <= 4
                || id.len() > 128
                || pin.get_revision() == 0
                || pin.get_expires_at_ms() <= as_of
            {
                return Err(sparrow_client::Error::Protocol("invalid frozen pin receipt").into());
            }
            Object::new()
                .with("id", id)
                .with("revision", pin.get_revision().to_string())
                .with("expires_at_ms", pin.get_expires_at_ms().to_string())
                .into()
        }
    };
    Ok(Object::new()
        .with("profile", profile)
        .with("id", id)
        .with(
            "selection",
            match selection {
                wire::ScopeSelectionKind::Live => "live",
                wire::ScopeSelectionKind::Evidence => "evidence",
                wire::ScopeSelectionKind::Pin => "pin",
            },
        )
        .with("pin", pin)
        .with("direct_members", direct)
        .with("expanded_records", expanded)
        .with("has_exact_source", scope.get_has_exact_source())
        .with(
            "source_alternatives",
            scope.get_source_alternatives().to_string(),
        )
        .with(
            "diagnostic_dimensions",
            scope.get_diagnostic_dimensions().to_string(),
        )
        .with("received_from_ms", scope.get_received_from_ms().to_string())
        .with(
            "received_before_ms",
            scope.get_received_before_ms().to_string(),
        )
        .with("require_complete_index", scope.get_require_complete_index())
        .into())
}

pub(super) fn limit(value: wire::optional_limit::Reader<'_>) -> Result<Value> {
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
        .with(
            "scope",
            scope_receipt(capture.get_scope()?, page.get_as_of()?.get_unix_millis())?,
        )
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
                .with("query_path_bytes", limit(limits.get_query_path_bytes()?)?)
                .with(
                    "key_aggregation_bytes",
                    limit(limits.get_key_aggregation_bytes()?)?,
                )
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_decoder_refuses_missing_or_incoherent_required_evidence() {
        let mut message = capnp::message::Builder::new_default();
        let mut scope = message.init_root::<wire::query_scope_receipt::Builder<'_>>();
        assert_eq!(
            scope_receipt(scope.reborrow_as_reader(), 0)
                .unwrap_err()
                .code,
            "PROTOCOL_FAILURE"
        );
        scope.set_profile(sparrow_core::QUERY_SCOPE_PROFILE);
        scope.set_id(format!("scope:{}", "0".repeat(64)));
        scope.set_received_before_ms(u64::MAX);
        let live = scope_receipt(scope.reborrow_as_reader(), 0).unwrap();
        assert!(live["direct_members"].is_null());
        assert_eq!(
            live["received_before_ms"].as_str(),
            Some(u64::MAX.to_string().as_str())
        );
        scope.set_selection(wire::ScopeSelectionKind::Evidence);
        assert!(scope_receipt(scope.reborrow_as_reader(), 0).is_err());
        scope.reborrow().init_direct_members().set_known(1);
        scope.reborrow().init_expanded_records().set_known(0);
        assert!(scope_receipt(scope.reborrow_as_reader(), 0).is_ok());
        scope.set_selection(wire::ScopeSelectionKind::Pin);
        assert!(scope_receipt(scope.reborrow_as_reader(), 0).is_err());
        {
            let mut pin = scope.reborrow().init_pin().init_value();
            pin.set_id("pin:captured");
            pin.set_revision(7);
            pin.set_expires_at_ms(100);
        }
        let pin = scope_receipt(scope.reborrow_as_reader(), 0).unwrap();
        assert_eq!(pin["pin"]["revision"].as_str(), Some("7"));
        match scope.reborrow().get_pin().unwrap().which().unwrap() {
            wire::optional_captured_pin::Value(pin) => pin.unwrap().set_revision(0),
            _ => panic!("missing captured pin"),
        }
        assert!(scope_receipt(scope.reborrow_as_reader(), 0).is_err());
        scope.set_profile("future-profile");
        assert!(scope_receipt(scope.reborrow_as_reader(), 0).is_err());
    }
}
