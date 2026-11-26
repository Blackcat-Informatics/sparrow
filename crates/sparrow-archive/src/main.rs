// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! `sparrow-archive list|verify DIR` and `extract DIR ARTIFACT_ID`: offline, read-only.
//! `list` and `verify` print one JSON line per segment on stdout; `extract` prints one
//! artifact's bytes, and only after they are verified. Diagnostics go to stderr, and any
//! failure exits nonzero.

use std::io::Write;
use std::path::Path;
use std::process::ExitCode;

use purrdf_lex::json::{self, Value};

use sparrow_archive::{ArchiveError, Segment, Summary, extract, list_segments};

const USAGE: &str = "usage: sparrow-archive list|verify DIR | extract DIR ARTIFACT_ID";

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

fn run(command: &str, root: &Path) -> Result<ExitCode, ExitCode> {
    let segments = list_segments(root).map_err(|error| {
        let _ = writeln!(std::io::stderr(), "sparrow-archive: {error}");
        ExitCode::FAILURE
    })?;
    let mut corrupt = 0usize;
    for path in &segments {
        let name = Value::from(path.display().to_string().as_str());
        match (command, open(path)) {
            ("list", Ok((segment, summary))) => {
                let mut members = vec![("path", name)];
                members.extend(counts(summary));
                members.push(("entries", entries(&segment)));
                emit(&line(members))?;
            }
            ("verify", Ok((_, summary))) => {
                let mut members = vec![("path", name), ("ok", true.into())];
                members.extend(counts(summary));
                emit(&line(members))?;
            }
            ("verify", Err(error)) => {
                corrupt += 1;
                emit(&line(vec![
                    ("path", name),
                    ("ok", false.into()),
                    ("error", Value::from(error.to_string().as_str())),
                ]))?;
            }
            (_, Err(error)) => {
                let _ = writeln!(std::io::stderr(), "sparrow-archive: {error}");
                return Ok(ExitCode::FAILURE);
            }
            _ => return Err(ExitCode::from(2)),
        }
    }
    if command == "verify" {
        emit(&line(vec![
            ("segments", segments.len().into()),
            ("ok", (segments.len() - corrupt).into()),
            ("corrupt", corrupt.into()),
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

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let [command, root, artifact_id] = args.as_slice()
        && command == "extract"
    {
        return take(Path::new(root), artifact_id);
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
