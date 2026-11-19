// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Stored search facts derived exclusively from the native ordered JSON model.

use purrdf::json::{self, SourceDocument};
use purrdf_lex::json::{Object, Value};

use crate::Error;

pub(crate) type NativeGraph = Result<std::sync::Arc<purrdf::RdfDataset>, Error>;

/// Maximum materialized source passed to native document projection.
pub(crate) const RECORD_BYTES: u64 = 16 * 1024 * 1024;

/// The exact native ordered-JSON profile used for stored occurrence and RDF facts.
pub fn json_profile() -> Result<json::Profile, Error> {
    let vocabulary = json::Vocabulary::standard()
        .map_err(|e| Error::new("PROJECTION_FAILURE", e.to_string()))?;
    json::Profile::new(
        "sparrow-json-occurrences",
        1,
        vocabulary,
        json::Bounds {
            max_source_bytes: RECORD_BYTES,
            max_values: 65536,
            max_depth: 128,
            max_pointer_bytes: 2 * 1024 * 1024,
        },
    )
    .map_err(|e| Error::new("PROJECTION_FAILURE", e.to_string()))
}

pub(crate) fn json_facts(
    bytes: Option<&[u8]>,
    byte_length: u64,
    content_id: &str,
    profile: &json::Profile,
) -> Result<(Value, NativeGraph), Error> {
    let source = format!("urn:sparrow:payload:{content_id}");
    let result = if byte_length > profile.bounds().max_source_bytes {
        Err(json::JsonError::Limit {
            resource: "source bytes",
            limit: profile.bounds().max_source_bytes,
        })
    } else {
        let bytes = bytes.ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "JSON source missing within its materialization bound",
            )
        })?;
        json::analyze(SourceDocument { id: &source, bytes }, profile)
    };
    let mut envelope = Object::new()
        .with("profile", profile.identity().to_hex())
        .with("source", source.clone());
    let graph;
    match result {
        Err(e) => {
            graph = Err(Error::new("PROJECTION_FAILURE", e.to_string()));
            envelope = envelope
                .with("state", "failed")
                .with("diagnostic", e.to_string())
                .with("values", Value::array(Vec::<Value>::new()));
        }
        Ok(document) => {
            graph = json::project(&document, profile)
                .map_err(|e| Error::new("PROJECTION_FAILURE", e.to_string()));
            let mut values = Vec::with_capacity(document.values().len());
            let mut decoding_failures = 0_u64;
            for (index, value) in document.values().iter().enumerate() {
                let span = value.span();
                let mut fact = Object::new()
                    .with("occurrence", index.to_string())
                    .with("path", value.path())
                    .with("kind", value.kind().name())
                    .with("start", span.start.to_string())
                    .with("end", span.end.to_string())
                    .with("ordinal", value.ordinal().to_string())
                    .with(
                        "parent",
                        value
                            .parent()
                            .map(|p| Value::from(p.to_string()))
                            .unwrap_or(Value::Null),
                    );
                if value.kind().is_scalar() {
                    let lexical = &document.text()[span];
                    fact = fact.with("lexical", lexical);
                    if value.kind() == json::Kind::String {
                        match purrdf_lex::json_escape::unescape(lexical) {
                            Ok(decoded) => fact = fact.with("decoded", decoded.as_ref()),
                            Err(e) => {
                                decoding_failures += 1;
                                fact = fact.with("decoding_error", e.to_string());
                            }
                        }
                    }
                }
                values.push(Value::from(fact));
            }
            envelope = envelope
                .with("state", "complete")
                .with("document_id", document.id())
                .with("decoding_failures", decoding_failures.to_string())
                .with("values", Value::array(values));
        }
    }
    Ok((envelope.into(), graph))
}

pub(crate) fn markdown_profile() -> Result<purrdf::markdown::Profile, Error> {
    Ok(purrdf::markdown::Profile::new(
        "sparrow-markdown",
        1,
        purrdf::markdown::Vocabulary::standard()
            .map_err(|e| Error::new("PROJECTION_FAILURE", e.to_string()))?,
    ))
}

pub(crate) fn markdown_graph(
    bytes: Option<&[u8]>,
    byte_length: u64,
    digest: &str,
    profile: &purrdf::markdown::Profile,
) -> Result<std::sync::Arc<purrdf::RdfDataset>, Error> {
    if byte_length > RECORD_BYTES {
        return Err(Error::new(
            "RESOURCE_LIMIT",
            "native Markdown source exceeds 16 MiB materialization bound",
        ));
    }
    let bytes = bytes.ok_or_else(|| {
        Error::new(
            "STORAGE_FAILURE",
            "Markdown source missing within its materialization bound",
        )
    })?;
    let source = format!("urn:sparrow:payload:{digest}");
    let claims = purrdf::markdown::slice_markdown(
        &purrdf::markdown::SourceDocument { id: &source, bytes },
        profile,
    )
    .map_err(|e| Error::new("PROJECTION_FAILURE", e.to_string()))?;
    let mut text = String::new();
    for claim in claims {
        if text.len().saturating_add(claim.turtle.len()) > 16 * 1024 * 1024 {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "native Markdown RDF exceeds 16 MiB",
            ));
        }
        text.push_str(&claim.turtle);
    }
    purrdf::dataset_from_bytes(text.as_bytes(), purrdf::NativeRdfFormat::Turtle)
        .map_err(|e| Error::new("PROJECTION_FAILURE", format!("parse error: {e}")))
}
