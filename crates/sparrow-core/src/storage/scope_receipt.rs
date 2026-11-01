// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Compact logical scope identity, captured with the resolved selection.

use super::{Error, Value, query_membership::SelectionMembers};
use crate::{QueryAlternatives, QueryScope, QuerySelection};
use purrdf_lex::json::Object;
use std::collections::BTreeSet;

/// Logical selector fingerprint and compact resolved membership receipt format.
pub const QUERY_SCOPE_PROFILE: &str = "sparrow-scope-receipt/1";

/// Conservative receipt/hash workspace charged by context and native graph capture.
pub const QUERY_SCOPE_WORKSPACE_BYTES: usize = 64 * 1024;

pub(super) fn capture(
    store: &str,
    scope: &QueryScope,
    members: Option<&SelectionMembers>,
) -> Result<Value, Error> {
    let mut digest = purrdf_hash::blake3::Hasher::new();
    let mut part = |bytes: &[u8]| {
        purrdf_hash::frame::frame_le_with(bytes, &mut |chunk| {
            digest.update(chunk);
        });
    };
    part(QUERY_SCOPE_PROFILE.as_bytes());
    part(store.as_bytes());
    let kind = match &scope.selection {
        QuerySelection::Live => "live",
        QuerySelection::Evidence(_) => "evidence",
        QuerySelection::Pin { .. } => "pin",
    };
    part(kind.as_bytes());
    let pin = match (&scope.selection, members.and_then(|m| m.pin.as_ref())) {
        (QuerySelection::Pin { id, .. }, Some(pin)) if *id == pin.id => {
            part(id.as_bytes());
            part(&pin.revision.to_le_bytes());
            Object::new()
                .with("id", pin.id.clone())
                .with("revision", pin.revision)
                .with("expires_at_ms", pin.expires_at)
                .into()
        }
        (QuerySelection::Pin { .. }, _) | (_, Some(_)) => {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "scope pin capture disagrees with selection",
            ));
        }
        _ => Value::Null,
    };
    if let QuerySelection::Evidence(ids) = &scope.selection {
        let ids = ids.iter().collect::<BTreeSet<_>>();
        part(&(ids.len() as u64).to_le_bytes());
        for id in ids {
            part(id.as_bytes());
        }
    }
    part(&[u8::from(scope.source.is_some())]);
    if let Some(source) = &scope.source {
        part(source.as_bytes());
    }
    // OR alternatives are sets. Frame counts as well as values to preserve
    // dimension boundaries without allocating a serialized selector.
    let mut alternatives = |values: Option<&QueryAlternatives>| {
        let values = values
            .into_iter()
            .flat_map(QueryAlternatives::ids)
            .collect::<BTreeSet<_>>();
        part(&(values.len() as u64).to_le_bytes());
        for value in &values {
            part(value.as_bytes());
        }
        values.len()
    };
    let source_count = alternatives(scope.sources.as_ref());
    for dimension in crate::DiagnosticDimension::ALL {
        alternatives(scope.diagnostics.get(&dimension));
    }
    part(&scope.received_from.to_le_bytes());
    part(&scope.received_before.to_le_bytes());
    part(&[u8::from(scope.require_complete_index)]);
    let identity = format!(
        "scope:{}",
        purrdf_hash::hex::encode(digest.finalize().as_bytes())
    );
    Ok(Object::new()
        .with("profile", QUERY_SCOPE_PROFILE)
        .with("id", identity)
        .with("selection", kind)
        .with("pin", pin)
        .with(
            "direct_members",
            members.map_or(Value::Null, |m| Value::from(m.direct_members.to_string())),
        )
        .with(
            "expanded_records",
            members.map_or(Value::Null, |m| Value::from(m.records.len().to_string())),
        )
        .with("has_exact_source", scope.source.is_some())
        .with("source_alternatives", source_count as u64)
        .with("diagnostic_dimensions", scope.diagnostics.len() as u64)
        .with("received_from_ms", scope.received_from)
        .with("received_before_ms", scope.received_before)
        .with("require_complete_index", scope.require_complete_index)
        .into())
}
