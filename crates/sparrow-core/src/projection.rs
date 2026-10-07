// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Stored search facts derived exclusively from the native ordered JSON model.

use purrdf::json::{self, SourceDocument};
use purrdf_lex::json::{Object, Value};

use crate::Error;

pub(crate) fn json_profile() -> Result<json::Profile, Error> {
    let vocabulary = json::Vocabulary::standard().map_err(|e| Error {
        code: "PROJECTION_FAILURE",
        message: e.to_string(),
    })?;
    json::Profile::new(
        "sparrow-json-occurrences",
        1,
        vocabulary,
        json::Bounds {
            max_source_bytes: 16 * 1024 * 1024,
            max_values: 65536,
            max_depth: 128,
            max_pointer_bytes: 2 * 1024 * 1024,
        },
    )
    .map_err(|e| Error {
        code: "PROJECTION_FAILURE",
        message: e.to_string(),
    })
}

pub(crate) fn json_facts(
    bytes: &[u8],
    content_id: &str,
    profile: &json::Profile,
) -> Result<Value, Error> {
    let source = format!("urn:sparrow:payload:{content_id}");
    let result = json::analyze(SourceDocument { id: &source, bytes }, profile);
    let mut envelope = Object::new()
        .with("profile", profile.identity().to_hex())
        .with("source", source.clone());
    match result {
        Err(e) => {
            envelope = envelope
                .with("state", "failed")
                .with("diagnostic", e.to_string())
                .with("values", Value::array(Vec::<Value>::new()));
        }
        Ok(document) => {
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
    Ok(envelope.into())
}
