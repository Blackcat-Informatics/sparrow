// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! `sparrow-archive list|verify DIR` and `extract DIR ARTIFACT_ID`: offline, read-only.
//! `list` and `verify` print one JSON line per segment on stdout; `extract` prints one
//! artifact's bytes, and only after they are verified. Diagnostics go to stderr, and any
//! failure exits nonzero.

use std::io::Write;
use std::path::Path;
use std::process::ExitCode;

use sparrow_archive::{ArchiveError, Segment, Summary, extract, list_segments};

const USAGE: &str = "usage: sparrow-archive list|verify DIR | extract DIR ARTIFACT_ID";

fn quote(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn counts(summary: Summary) -> String {
    format!(
        "\"records\":{},\"artifacts\":{},\"blobs\":{}",
        summary.records, summary.artifacts, summary.blobs
    )
}

/// The segment's artifacts, so a person can find an artifact id and extract it.
fn entries(segment: &Segment) -> String {
    let items: Vec<String> = segment
        .artifacts()
        .iter()
        .map(|artifact| {
            format!(
                "{{\"id\":{},\"source\":{},\"received_at\":{},\"byte_length\":{}}}",
                quote(&artifact.id),
                quote(&artifact.source),
                artifact.received_at,
                artifact.byte_length
            )
        })
        .collect();
    format!("[{}]", items.join(","))
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
        let name = quote(&path.display().to_string());
        match (command, open(path)) {
            ("list", Ok((segment, summary))) => {
                emit(&format!(
                    "{{\"path\":{name},{},\"entries\":{}}}",
                    counts(summary),
                    entries(&segment)
                ))?;
            }
            ("verify", Ok((_, summary))) => {
                emit(&format!(
                    "{{\"path\":{name},\"ok\":true,{}}}",
                    counts(summary)
                ))?;
            }
            ("verify", Err(error)) => {
                corrupt += 1;
                emit(&format!(
                    "{{\"path\":{name},\"ok\":false,\"error\":{}}}",
                    quote(&error.to_string())
                ))?;
            }
            (_, Err(error)) => {
                let _ = writeln!(std::io::stderr(), "sparrow-archive: {error}");
                return Ok(ExitCode::FAILURE);
            }
            _ => return Err(ExitCode::from(2)),
        }
    }
    if command == "verify" {
        emit(&format!(
            "{{\"segments\":{},\"ok\":{},\"corrupt\":{corrupt}}}",
            segments.len(),
            segments.len() - corrupt
        ))?;
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
