// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Shared inspection of persisted native RDF state, without a resident replay.

use super::{Result, failure, instant, optional_value, wire};
use purrdf_lex::json::{Object, Value};

pub(super) async fn inspect(
    client: &wire::sparrow::Client,
    store: &str,
    id: &str,
) -> Result<Value> {
    let mut request = client.rdf_inspect_request();
    let mut input = request.get().init_record();
    input.set_store_id(store);
    input.set_id(id);
    let response = request.send().promise.await?;
    let found = match response.get()?.get_result()?.which()? {
        wire::result::Ok(value) => value?,
        wire::result::Error(error) => return Err(failure(error?)),
    };
    Ok(Object::new()
        .with("store_id", found.get_store_id()?.to_str()?)
        .with("record_id", found.get_record_id()?.to_str()?)
        .with("membership_at", instant(found.get_membership_at()?)?)
        .with("metadata_graph", found.get_metadata_graph()?.to_str()?)
        .with(
            "metadata_primary_rows",
            found.get_metadata_primary_rows().to_string(),
        )
        .with(
            "state",
            match found.get_state()? {
                wire::ProjectionState::Complete => "complete",
                wire::ProjectionState::Failed => "failed",
                wire::ProjectionState::NotApplicable => "not_applicable",
            },
        )
        .with("content_graph", optional_value(found.get_content_graph()?)?)
        .with("diagnostic", optional_value(found.get_diagnostic()?)?)
        .with("primary_rows", found.get_primary_rows().to_string())
        .with("reifier_rows", found.get_reifier_rows().to_string())
        .with("annotation_rows", found.get_annotation_rows().to_string())
        .into())
}
