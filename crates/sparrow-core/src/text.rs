// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Native text analysis with explicit, fingerprinted artifact inputs.

use crate::Error;
use purrdf::text::{Analyzer, AnalyzerProfile};
use purrdf_lex::json::{Object, Value};
use std::{collections::BTreeMap, io::Read, path::Path};

pub(crate) const PROFILE: &str = "sparrow-record-raw-and-decoded-v1";

/// Resolve PurRDF's standard law from exactly five supplied CBOR lexicons.
/// Missing, oversized, corrupt or drifted artifacts are hard failures.
pub fn resolve_text_analyzer(directory: impl AsRef<Path>) -> Result<Analyzer, Error> {
    let mut paths = std::fs::read_dir(directory.as_ref())
        .map_err(|e| {
            Error::new(
                "INVALID_ARGUMENT",
                format!("text lexicons {}: {e}", directory.as_ref().display()),
            )
        })?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Error::new("INVALID_ARGUMENT", e.to_string()))?;
    paths.retain(|path| path.extension().is_some_and(|ext| ext == "cbor"));
    paths.sort();
    if paths.len() != 5 {
        return Err(Error::new(
            "INVALID_ARGUMENT",
            "text_lexicon_dir requires exactly five standard CBOR artifacts",
        ));
    }
    let mut artifacts = Vec::new();
    for path in paths {
        let mut bytes = Vec::new();
        std::fs::File::open(&path)
            .and_then(|file| file.take(16 * 1024 * 1024 + 1).read_to_end(&mut bytes))
            .map_err(|e| {
                Error::new(
                    "INVALID_ARGUMENT",
                    format!("text lexicon {}: {e}", path.display()),
                )
            })?;
        if bytes.len() > 16 * 1024 * 1024 {
            return Err(Error::new("RESOURCE_LIMIT", "text lexicon exceeds 16 MiB"));
        }
        artifacts.push(bytes);
    }
    Analyzer::resolve(
        AnalyzerProfile::standard(),
        &artifacts.iter().map(Vec::as_slice).collect::<Vec<_>>(),
    )
    .map_err(|e| Error::new("INVALID_ARGUMENT", format!("standard text analyzer: {e}")))
}

pub(crate) fn facts(
    analyzer: &Analyzer,
    bytes: Option<&[u8]>,
    byte_length: u64,
    json: Option<&Value>,
    opaque: bool,
) -> Result<Value, Error> {
    let mut envelope = Object::new().with("profile", PROFILE).with(
        "analyzer",
        purrdf_hash::hex::encode(&analyzer.fingerprint()),
    );
    if opaque {
        return Ok(envelope
            .with("state", "not_applicable")
            .with("length", "0")
            .with("terms", Value::array(Vec::<Value>::new()))
            .into());
    }
    let result = if byte_length > 1024 * 1024 {
        Err(Error::new(
            "RESOURCE_LIMIT",
            "aligned text record exceeds 1 MiB profile bound",
        ))
    } else {
        let bytes = bytes.ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "text source missing within its materialization bound",
            )
        })?;
        analyze(analyzer, bytes, json)
    };
    Ok(match result {
        Ok((length, terms, incomplete)) => envelope
            .with("state", if incomplete { "incomplete" } else { "complete" })
            .with("length", length.to_string())
            .with("terms", Value::array(terms))
            .into(),
        Err(error) => {
            envelope = envelope
                .with("state", "failed")
                .with("diagnostic", error.to_string());
            envelope
                .with("length", "0")
                .with("terms", Value::array(Vec::<Value>::new()))
                .into()
        }
    })
}

fn analyze(
    analyzer: &Analyzer,
    bytes: &[u8],
    json: Option<&Value>,
) -> Result<(u64, Vec<Value>, bool), Error> {
    let raw =
        std::str::from_utf8(bytes).map_err(|e| Error::new("PROJECTION_FAILURE", e.to_string()))?;
    let mut terms = BTreeMap::<String, Vec<Value>>::new();
    let mut length = 0_u64;
    append(analyzer, raw, None, None, &mut terms, &mut length)?;
    let mut incomplete = false;
    if let Some(json) = json {
        incomplete = json["state"].as_str() != Some("complete");
        for fact in json["values"]
            .as_array()
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "JSON facts lack values"))?
        {
            if let Some(decoded) = fact.get("decoded").and_then(Value::as_str) {
                let start = fact["start"]
                    .as_str()
                    .and_then(|v| v.parse().ok())
                    .ok_or_else(|| Error::new("STORAGE_FAILURE", "JSON span missing"))?;
                let end = fact["end"]
                    .as_str()
                    .and_then(|v| v.parse().ok())
                    .ok_or_else(|| Error::new("STORAGE_FAILURE", "JSON span missing"))?;
                append(
                    analyzer,
                    decoded,
                    Some((start, end)),
                    fact["path"].as_str(),
                    &mut terms,
                    &mut length,
                )?;
            } else if fact.get("decoding_error").is_some() {
                incomplete = true;
            }
        }
    }
    let rows = terms
        .into_iter()
        .map(|(term, spans)| {
            Object::new()
                .with("term", term)
                .with("frequency", spans.len().to_string())
                .with("spans", Value::array(spans))
                .into()
        })
        .collect();
    Ok((length, rows, incomplete))
}

fn append(
    analyzer: &Analyzer,
    input: &str,
    scalar: Option<(u64, u64)>,
    path: Option<&str>,
    terms: &mut BTreeMap<String, Vec<Value>>,
    length: &mut u64,
) -> Result<(), Error> {
    let analysis = analyzer
        .projections(input)
        .map_err(|e| Error::new("PROJECTION_FAILURE", e.to_string()))?;
    for projection in analysis.lexical {
        if *length == 16384 {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "text record exceeds 16,384 token occurrences",
            ));
        }
        *length += 1;
        let (start, end) = scalar.unwrap_or((
            projection.highlight.start as u64,
            projection.highlight.end as u64,
        ));
        let sources = if scalar.is_some() {
            vec![(start, end)]
        } else {
            projection
                .sources
                .iter()
                .map(|span| (span.start as u64, span.end as u64))
                .collect()
        };
        let sources = sources
            .into_iter()
            .map(|(start, end)| {
                Value::from(
                    Object::new()
                        .with("start", start.to_string())
                        .with("end", end.to_string()),
                )
            })
            .collect::<Vec<_>>();
        terms.entry(projection.text).or_default().push(
            Object::new()
                .with("start", start.to_string())
                .with("end", end.to_string())
                .with(
                    "basis",
                    if scalar.is_some() {
                        "decoded_json_scalar"
                    } else {
                        "raw_record"
                    },
                )
                .with("coarse", scalar.is_some() || projection.coarse)
                .with("path", path.map(Value::from).unwrap_or(Value::Null))
                .with("contributors", Value::array(sources))
                .into(),
        );
    }
    Ok(())
}
