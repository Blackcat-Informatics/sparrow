// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! `sparrow-archive list|verify|index DIR`, `extract DIR ARTIFACT_ID` and `find DIR PAYLOAD_ID`.
//! Every command reads the archive and never opens a live store; the only write is `index`,
//! which writes `DIR/payload.index`. `list` and `verify` print one JSON line per segment on
//! stdout; `extract` prints one artifact's bytes, and only after they are verified; `find`
//! prints one JSON line naming the artifacts that hold the payload, and whether any does. `find`
//! uses `payload.index` when it is valid and current, and otherwise scans every segment, as the
//! format document states. Diagnostics go to stderr, and any failure exits nonzero.

use std::io::Write;
use std::path::Path;
use std::process::ExitCode;

use purrdf_lex::json::{self, Value};

use sparrow_archive::{
    ArchiveError, Method, PayloadId, Segment, Summary, build_index, extract, list_segments,
    lookup_payload, store_binding,
};

const USAGE: &str =
    "usage: sparrow-archive list|verify|index DIR | extract DIR ARTIFACT_ID | find DIR PAYLOAD_ID";

/// One output line: `members` as a compact JSON object, quoted by the supplier's writer so
/// a path or an error detail with a quote, a backslash or a control character stays valid.
fn line(members: Vec<(&str, Value)>) -> String {
    json::write_compact(&Value::object(members))
}

/// The record, artifact and blob counts of a decoded segment, as object members.
fn counts(summary: Summary) -> [(&'static str, Value); 3] {
    [
        ("records", summary.records.into()),
        ("artifacts", summary.artifacts.into()),
        ("blobs", summary.blobs.into()),
    ]
}

/// The segment's artifacts, so a person can find an artifact id, or match content by its
/// payload id, and extract it.
fn entries(segment: &Segment) -> Value {
    Value::array(segment.artifacts().iter().map(|artifact| {
        Value::object([
            ("id", Value::from(artifact.id.as_str())),
            ("source", Value::from(artifact.source.as_str())),
            ("received_at", Value::from(artifact.received_at)),
            ("byte_length", Value::from(artifact.byte_length)),
            ("payload_id", Value::from(artifact.payload_id.as_str())),
        ])
    }))
}

fn open(path: &Path) -> Result<(Segment, Summary), ArchiveError> {
    let segment = Segment::open(path)?;
    let summary = segment.summary();
    Ok((segment, summary))
}

fn emit(line: &str) -> Result<(), ExitCode> {
    writeln!(std::io::stdout(), "{line}").map_err(|error| {
        let _ = writeln!(std::io::stderr(), "sparrow-archive: stdout: {error}");
        ExitCode::from(2)
    })
}

/// Why a segment does not belong to the bound store: its directory is not the store's
/// identity. With no `STORE_ID`, the root is unbound and nothing is refused on that ground.
fn binding_refusal(path: &Path, store: Option<&str>) -> Option<String> {
    let store = store?;
    let directory = path.parent()?.file_name()?.to_str().unwrap_or_default();
    (directory != store)
        .then(|| format!("segment is in directory {directory:?} but STORE_ID names {store}"))
}

fn run(command: &str, root: &Path) -> Result<ExitCode, ExitCode> {
    let segments = list_segments(root).map_err(|error| {
        let _ = writeln!(std::io::stderr(), "sparrow-archive: {error}");
        ExitCode::FAILURE
    })?;
    let store = if command == "verify" {
        store_binding(root).map_err(|cause| {
            let _ = writeln!(std::io::stderr(), "sparrow-archive: {cause}");
            ExitCode::FAILURE
        })?
    } else {
        None
    };
    let mut corrupt = 0usize;
    let mut duplicates = 0usize;
    let mut first_of: std::collections::HashMap<String, std::path::PathBuf> =
        std::collections::HashMap::new();
    for path in &segments {
        let name = Value::from(path.display().to_string().as_str());
        if command == "verify" {
            match open(path) {
                Ok((segment, summary)) => {
                    if let Some(refusal) = binding_refusal(path, store.as_deref()) {
                        corrupt += 1;
                        emit(&line(vec![
                            ("path", name),
                            ("ok", false.into()),
                            ("error", Value::from(refusal.as_str())),
                        ]))?;
                    } else if let Some(original) = first_of.get(segment.digest()) {
                        duplicates += 1;
                        let mut members = vec![
                            ("path", name),
                            ("ok", true.into()),
                            (
                                "duplicate_of",
                                Value::from(original.display().to_string().as_str()),
                            ),
                        ];
                        members.extend(counts(summary));
                        emit(&line(members))?;
                    } else {
                        first_of.insert(segment.digest().to_owned(), path.clone());
                        let mut members = vec![("path", name), ("ok", true.into())];
                        members.extend(counts(summary));
                        emit(&line(members))?;
                    }
                }
                Err(error) => {
                    corrupt += 1;
                    emit(&line(vec![
                        ("path", name),
                        ("ok", false.into()),
                        ("error", Value::from(error.to_string().as_str())),
                    ]))?;
                }
            }
            continue;
        }
        match open(path) {
            Ok((segment, summary)) => {
                let mut members = vec![
                    ("path", name),
                    (
                        "store_dir",
                        Value::from(
                            path.parent()
                                .and_then(Path::file_name)
                                .and_then(|n| n.to_str())
                                .unwrap_or_default(),
                        ),
                    ),
                ];
                members.extend(counts(summary));
                members.push(("entries", entries(&segment)));
                emit(&line(members))?;
            }
            Err(error) => {
                let _ = writeln!(std::io::stderr(), "sparrow-archive: {error}");
                return Ok(ExitCode::FAILURE);
            }
        }
    }
    if command == "verify" {
        emit(&line(vec![
            ("segments", segments.len().into()),
            ("ok", (segments.len() - corrupt).into()),
            ("corrupt", corrupt.into()),
            ("duplicates", duplicates.into()),
        ]))?;
    }
    Ok(if corrupt == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

/// Write one artifact's verified bytes to stdout. Nothing reaches stdout before the whole
/// artifact has been read from every segment and checked.
fn take(root: &Path, artifact_id: &str) -> ExitCode {
    let fail = |error: &dyn std::fmt::Display| {
        let _ = writeln!(std::io::stderr(), "sparrow-archive: {error}");
        ExitCode::FAILURE
    };
    let bytes = match list_segments(root).and_then(|paths| extract(&paths, artifact_id)) {
        Ok(bytes) => bytes,
        Err(error) => return fail(&error),
    };
    let mut out = std::io::stdout().lock();
    match out.write_all(&bytes).and_then(|()| out.flush()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let _ = writeln!(std::io::stderr(), "sparrow-archive: stdout: {error}");
            ExitCode::from(2)
        }
    }
}

/// One JSON line: whether any segment holds the payload, each artifact that does with the file
/// whose metadata names it, and how the answer was reached (`lookup` is `index` or `scan`, with
/// `reason` when the index was not used). An absent payload is a normal answer, not a failure.
/// A damaged segment fails the call by name, wherever the payload is.
fn find(root: &Path, payload_id: &str) -> ExitCode {
    let payload_id = match PayloadId::parse(payload_id) {
        Ok(id) => id,
        Err(error) => {
            let _ = writeln!(std::io::stderr(), "sparrow-archive: {error}");
            return ExitCode::FAILURE;
        }
    };
    let lookup = match lookup_payload(root, &payload_id) {
        Ok(lookup) => lookup,
        Err(error) => {
            let _ = writeln!(std::io::stderr(), "sparrow-archive: {error}");
            return ExitCode::FAILURE;
        }
    };
    let found = !lookup.matches.is_empty();
    let matches = Value::array(lookup.matches.iter().map(|hit| {
        Value::object([
            ("artifact_id", Value::from(hit.artifact_id.as_str())),
            ("path", Value::from(hit.path.display().to_string().as_str())),
        ])
    }));
    let mut members = vec![
        ("payload_id", Value::from(payload_id.as_str())),
        ("found", found.into()),
        ("matches", matches),
    ];
    match &lookup.method {
        Method::Index { decoded } => {
            members.push(("lookup", Value::from("index")));
            members.push(("decoded", Value::from(*decoded as u64)));
        }
        Method::Scan { reason } => {
            members.push(("lookup", Value::from("scan")));
            members.push(("reason", Value::from(reason.as_str())));
        }
    }
    match emit(&line(members)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(code) => code,
    }
}

/// One JSON line: the segments and entries the payload index now covers. The index is written
/// only when every segment decodes; a damaged segment refuses the build by name.
fn index(root: &Path) -> ExitCode {
    match build_index(root) {
        Ok(summary) => {
            let report = line(vec![
                ("index", Value::from(sparrow_archive::INDEX_FILE)),
                ("segments", Value::from(summary.segments as u64)),
                ("entries", Value::from(summary.entries as u64)),
            ]);
            match emit(&report) {
                Ok(()) => ExitCode::SUCCESS,
                Err(code) => code,
            }
        }
        Err(error) => {
            let _ = writeln!(std::io::stderr(), "sparrow-archive: {error}");
            ExitCode::FAILURE
        }
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let [command, root, artifact_id] = args.as_slice()
        && command == "extract"
    {
        return take(Path::new(root), artifact_id);
    }
    if let [command, root, payload_id] = args.as_slice()
        && command == "find"
    {
        return find(Path::new(root), payload_id);
    }
    if let [command, root] = args.as_slice()
        && command == "index"
    {
        return index(Path::new(root));
    }
    let [command, root] = args.as_slice() else {
        let _ = writeln!(std::io::stderr(), "{USAGE}");
        return ExitCode::from(2);
    };
    if command != "list" && command != "verify" {
        let _ = writeln!(std::io::stderr(), "{USAGE}");
        return ExitCode::from(2);
    }
    match run(command, Path::new(root)) {
        Ok(code) | Err(code) => code,
    }
}
