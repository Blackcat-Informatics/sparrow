// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! `sparrow-archive list|verify DIR`: offline, read-only. One JSON line per segment on
//! stdout, diagnostics on stderr, and a nonzero exit on any failure.

use std::io::Write;
use std::path::Path;
use std::process::ExitCode;

use sparrow_archive::{ArchiveError, Segment, Summary, list_segments};

const USAGE: &str = "usage: sparrow-archive list|verify DIR";

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
            ("list", Ok((_, summary))) => {
                emit(&format!("{{\"path\":{name},{}}}", counts(summary)))?;
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

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
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
