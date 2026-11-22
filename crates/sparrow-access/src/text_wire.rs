// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Render and validate stored-text evidence in query results.

use crate::sparrow_capnp as wire;
use purrdf_lex::json::{Object, Value};

pub(crate) fn render_capture(
    value: wire::optional_stored_text_capture::Reader<'_>,
    selected: u64,
    membership_known: bool,
) -> crate::AgentResult<Value> {
    let invalid = || crate::agent::invalid("incoherent stored-text corpus receipt");
    let value = match value.which()? {
        wire::optional_stored_text_capture::Absent(()) => return Ok(Value::Null),
        wire::optional_stored_text_capture::Value(value) => value?,
    };
    if value.get_profile()?.to_str()? != sparrow_core::STORED_TEXT_RELATION_PROFILE
        || value.get_corpus_profile()?.to_str()? != sparrow_core::STORED_TEXT_CORPUS_PROFILE
        || value.get_ranking_profile()?.to_str()? != purrdf::text::RANKING_PROFILE_ID
    {
        return Err(invalid());
    }
    let relations = value.get_relations()?;
    let ranked = relations.get_ranked_iri()?.to_str()?;
    let spans = relations.get_spans_iri()?.to_str()?;
    let coverage = value.get_coverage()?;
    let ready = coverage.get_ready();
    let failed = coverage.get_failed_or_incomplete();
    let not_applicable = coverage.get_not_applicable();
    let tokens = sparrow_client::decode_wide_integer(value.get_tokens()?);
    let complete = membership_known && failed == 0 && not_applicable == 0;
    if ranked.is_empty()
        || spans.is_empty()
        || ranked == spans
        || ranked.len() > sparrow_core::TEXT_RELATION_IRI_BYTES
        || spans.len() > sparrow_core::TEXT_RELATION_IRI_BYTES
        || ready
            .checked_add(failed)
            .and_then(|count| count.checked_add(not_applicable))
            != Some(selected)
        || value.get_documents() > ready.saturating_add(failed)
        || tokens < u128::from(value.get_documents())
        || (value.get_documents() == 0 && tokens != 0)
        || value.get_membership_complete() != complete
    {
        return Err(invalid());
    }
    Ok(Object::new()
        .with("profile", value.get_profile()?.to_str()?)
        .with(
            "relations",
            Object::new()
                .with("ranked_iri", ranked)
                .with("spans_iri", spans),
        )
        .with("corpus_profile", value.get_corpus_profile()?.to_str()?)
        .with("ranking_profile", value.get_ranking_profile()?.to_str()?)
        .with("analyzer", value.get_analyzer()?.to_str()?)
        .with("corpus_id", value.get_corpus_id()?.to_str()?)
        .with("documents", value.get_documents().to_string())
        .with("tokens", tokens.to_string())
        .with("membership_complete", complete)
        .with(
            "coverage",
            Object::new()
                .with("ready", ready.to_string())
                .with("failed_or_incomplete", failed.to_string())
                .with("not_applicable", not_applicable.to_string()),
        )
        .into())
}
