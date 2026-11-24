// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Convert a completed systemd Journal Export Format stream to ordinary JSON Lines.

use super::{Result, diagnostic, invalid, wire};
use purrdf_lex::json::{Object, Value, write_compact};
use sparrow_client::{Client, CompletedFileIdentity, Error, open_completed_file};
use sparrow_core::DiagnosticDimension;
use std::{
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

const ENTRY_BYTES: usize = 1024 * 1024;
const FIELD_COUNT: usize = 1024;
const FIELD_NAME_BYTES: usize = 64;

pub(super) async fn import(
    client: &Client,
    request: &str,
    source: &str,
    path: PathBuf,
) -> Result<sparrow_client::ArtifactReceipt> {
    if request.is_empty() || request.len() > 128 || source.is_empty() || source.len() > 512 {
        return Err(invalid(
            "require bounded nonempty request and source labels",
        ));
    }
    let discovery = client.discovery();
    let limits = stage_limits(discovery)?;
    let mut staged = tokio::task::spawn_blocking(move || stage(&path, limits))
        .await
        .map_err(|error| Error::Io(io::Error::other(error)))??;
    diagnostic::Input {
        source: source.to_owned(),
        mappings: Some(
            [
                (
                    DiagnosticDimension::Service,
                    "/diagnostic/service".to_owned(),
                ),
                (DiagnosticDimension::Host, "/diagnostic/host".to_owned()),
                (
                    DiagnosticDimension::Severity,
                    "/diagnostic/severity".to_owned(),
                ),
            ]
            .into(),
        ),
    }
    .execute(client.capability())
    .await?;
    Ok(sparrow_client::import_open_file(
        client.capability(),
        request,
        source,
        wire::Format::Jsonl,
        &mut staged,
    )
    .await?)
}

struct Limits {
    file: u64,
    record: u64,
    records: u64,
}

fn stage_limits(discovery: &sparrow_client::Discovery) -> Result<Limits> {
    if discovery.max_import_bytes == 0 || discovery.raw_record_bytes == 0 {
        return Err(
            Error::Unsupported("journal import requires advertised staged import limits").into(),
        );
    }
    Ok(Limits {
        file: discovery.max_import_bytes,
        record: discovery.raw_record_bytes,
        records: discovery.max_records_per_artifact,
    })
}

fn stage(path: &Path, limits: Limits) -> Result<File> {
    let input = open_completed_file(path)?;
    let identity = CompletedFileIdentity::capture(&input.metadata().map_err(Error::Io)?);
    let mut reader = BufReader::new(&input);
    if reader
        .fill_buf()
        .map_err(Error::Io)?
        .starts_with(b"LPKSHHRH")
    {
        return Err(invalid(
            "native journal file requires journalctl --file FILE --output=export",
        ));
    }
    // The upload path reads by offset, so the flushed file can retain its cursor.
    let mut output = tempfile::tempfile().map_err(Error::Io)?;
    let mut writer = BufWriter::new(&mut output);
    let mut written = 0;
    let mut number = 1;
    while let Some(fields) = entry(&mut reader, number)? {
        let record = write_compact(&render(fields));
        let bytes = record.len() as u64 + 1;
        if number > limits.records || bytes > limits.record || bytes > limits.file - written {
            return Err(invalid(format!(
                "journal entry {number} exceeds a staged import limit; split the export at entry boundaries"
            )));
        }
        writer.write_all(record.as_bytes()).map_err(Error::Io)?;
        writer.write_all(b"\n").map_err(Error::Io)?;
        written += bytes;
        number += 1;
    }
    if number == 1 {
        return Err(invalid("journal export has no entries"));
    }
    identity.require(&input)?;
    writer.flush().map_err(Error::Io)?;
    drop(writer);
    Ok(output)
}

struct Field {
    name: String,
    value: Vec<u8>,
}

fn entry(reader: &mut impl BufRead, number: u64) -> Result<Option<Vec<Field>>> {
    let malformed = |reason: &str| invalid(format!("journal entry {number}: {reason}"));
    let mut fields = Vec::new();
    let mut remaining = ENTRY_BYTES;
    loop {
        let mut line = Vec::new();
        let length = (&mut *reader)
            .take(remaining as u64)
            .read_until(b'\n', &mut line)
            .map_err(Error::Io)?;
        if length == 0 && fields.is_empty() {
            return Ok(None);
        }
        if line.pop() != Some(b'\n') {
            return Err(malformed("incomplete entry or entry larger than 1 MiB"));
        }
        remaining -= length;
        if line.is_empty() {
            return if fields.is_empty() {
                Err(malformed("empty entry"))
            } else {
                Ok(Some(fields))
            };
        }
        if fields.len() == FIELD_COUNT {
            return Err(malformed("entry exceeds 1024 fields"));
        }
        let (name, value) = if let Some(at) = line.iter().position(|byte| *byte == b'=') {
            let value = line.split_off(at + 1);
            line.truncate(at);
            (line, Some(value))
        } else {
            (line, None)
        };
        if name.is_empty()
            || name.len() > FIELD_NAME_BYTES
            || !name
                .iter()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || *byte == b'_')
        {
            return Err(malformed("invalid export field name"));
        }
        let name = String::from_utf8(name).map_err(|_| malformed("invalid export field name"))?;
        let value = match value {
            Some(value) => value,
            None => binary_field(reader, &mut remaining, &malformed)?,
        };
        fields.push(Field { name, value });
    }
}

fn binary_field(
    reader: &mut impl Read,
    remaining: &mut usize,
    malformed: &impl Fn(&str) -> super::AgentError,
) -> Result<Vec<u8>> {
    let limit = remaining
        .checked_sub(9)
        .ok_or_else(|| malformed("entry exceeds 1 MiB"))?;
    let mut size = [0; 8];
    read_exact(reader, &mut size, malformed)?;
    let size = usize::try_from(u64::from_le_bytes(size))
        .ok()
        .filter(|size| *size <= limit)
        .ok_or_else(|| malformed("binary field exceeds the 1 MiB entry limit"))?;
    let mut value = vec![0; size];
    read_exact(reader, &mut value, malformed)?;
    let mut delimiter = [0; 1];
    read_exact(reader, &mut delimiter, malformed)?;
    if delimiter[0] != b'\n' {
        return Err(malformed("binary field requires a newline terminator"));
    }
    *remaining = limit - size;
    Ok(value)
}

fn read_exact(
    reader: &mut impl Read,
    bytes: &mut [u8],
    malformed: &impl Fn(&str) -> super::AgentError,
) -> Result<()> {
    reader.read_exact(bytes).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            malformed("truncated binary field")
        } else {
            Error::Io(error).into()
        }
    })
}

fn singleton<'a>(fields: &'a [Field], name: &str) -> Option<&'a str> {
    let mut matched = fields.iter().filter(|field| field.name == name);
    let field = matched.next()?;
    if matched.next().is_some() || field.value.is_empty() || field.value.len() > 512 {
        return None;
    }
    std::str::from_utf8(&field.value).ok()
}

fn render(fields: Vec<Field>) -> Value {
    let mut diagnostics = Object::new();
    for (key, value) in [
        (
            "service",
            singleton(&fields, "_SYSTEMD_UNIT").or_else(|| singleton(&fields, "SYSLOG_IDENTIFIER")),
        ),
        ("host", singleton(&fields, "_HOSTNAME")),
        (
            "severity",
            singleton(&fields, "PRIORITY")
                .filter(|value| matches!(value.as_bytes(), [b'0'..=b'7'])),
        ),
    ] {
        if let Some(value) = value {
            diagnostics = diagnostics.with(key, value);
        }
    }
    let fields = fields.into_iter().map(|field| {
        let value = Object::new().with("name", field.name);
        match String::from_utf8(field.value) {
            Ok(text) => value.with("text", text),
            Err(error) => value.with("base64", purrdf::xsd::canonical_base64(&error.into_bytes())),
        }
    });
    Object::new()
        .with("diagnostic", diagnostics)
        .with("journal_fields", Value::array(fields))
        .into()
}
