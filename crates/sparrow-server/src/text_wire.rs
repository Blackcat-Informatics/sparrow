// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Typed stored-text request and corpus receipts shared by all agent transports.

use crate::sparrow_capnp as wire;
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
            if ranked.len() > sparrow_core::TEXT_RELATION_IRI_BYTES
                || spans.len() > sparrow_core::TEXT_RELATION_IRI_BYTES
            {
                return Err(Error::input_bound("text relation IRI exceeds 1024 bytes"));
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

/// Encode stored-text relation identifiers.
pub fn encode_relations(
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
