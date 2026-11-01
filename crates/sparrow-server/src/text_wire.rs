// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Typed stored-text request and corpus receipts shared by all agent transports.

use crate::sparrow_capnp as wire;
use purrdf_lex::json::{Object, Value};
use sparrow_core::{Error, RdfScopeCoverage, SparqlTextCapture, SparqlTextRelations};

pub(crate) fn decode_relations(
    value: wire::optional_stored_text_relations::Reader<'_>,
) -> Result<Option<SparqlTextRelations>, Error> {
    let invalid = |error: capnp::Error| Error::new("INVALID_ARGUMENT", error.to_string());
    match value
        .which()
        .map_err(|error| Error::new("INVALID_ARGUMENT", error.to_string()))?
    {
        wire::optional_stored_text_relations::Absent(()) => Ok(None),
        wire::optional_stored_text_relations::Value(value) => {
            let value = value.map_err(invalid)?;
            let ranked = value.get_ranked_iri().map_err(invalid)?;
            let spans = value.get_spans_iri().map_err(invalid)?;
            if ranked.len() > 1024 || spans.len() > 1024 {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "text relation IRI exceeds 1024 bytes",
                ));
            }
            Ok(Some(SparqlTextRelations {
                ranked_iri: ranked
                    .to_str()
                    .map_err(|error| Error::new("INVALID_ARGUMENT", error.to_string()))?
                    .to_owned(),
                spans_iri: spans
                    .to_str()
                    .map_err(|error| Error::new("INVALID_ARGUMENT", error.to_string()))?
                    .to_owned(),
            }))
        }
    }
}

pub(crate) fn encode_relations(
    mut out: wire::stored_text_relations::Builder<'_>,
    value: &SparqlTextRelations,
) {
    out.set_ranked_iri(&value.ranked_iri);
    out.set_spans_iri(&value.spans_iri);
}

pub(crate) fn encode_capture(
    mut out: wire::optional_stored_text_capture::Builder<'_>,
    value: Option<&SparqlTextCapture>,
    scope: &RdfScopeCoverage,
) {
    let Some(value) = value else {
        out.set_absent(());
        return;
    };
    let mut out = out.init_value();
    encode_relations(out.reborrow().init_relations(), &value.relations);
    out.set_profile(sparrow_core::STORED_TEXT_RELATION_PROFILE);
    out.set_corpus_profile(sparrow_core::STORED_TEXT_CORPUS_PROFILE);
    out.set_ranking_profile(purrdf::text::RANKING_PROFILE_ID);
    out.set_analyzer(&value.analyzer_id);
    out.set_corpus_id(&value.corpus_id);
    out.set_documents(value.documents);
    sparrow_client::encode_wide_integer(out.reborrow().init_tokens(), value.tokens);
    out.set_membership_complete(value.membership_complete(scope));
    let mut coverage = out.init_coverage();
    coverage.set_ready(value.coverage.ready);
    coverage.set_failed_or_incomplete(value.coverage.failed);
    coverage.set_not_applicable(value.coverage.not_applicable);
}

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
        || ranked.len() > 1024
        || spans.len() > 1024
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
