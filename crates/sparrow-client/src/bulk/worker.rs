// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use super::{
    RangeReceipt, Shared,
    disk::{self, Blocks, Pending, State},
};
use crate::{
    Client, Durability, Error, Expect, Format, IMPORT_CHUNK_BYTES, format_label,
    imports::{self, Checkpoint, StagedInput},
    parse_format,
    parts::{self, CutSource, PartLimits, PartSpan},
    receipt::ExpectedArtifact,
};
use std::{io, path::Path, time::Duration};

/// Bounds local receipt files, manifest working memory and one window's retry work.
const WINDOW: u32 = 8;

enum Body {
    Data(Blocks, u64),
    Manifest(Vec<u8>),
}

struct Input<'a> {
    body: Body,
    length: u64,
    digest: Option<String>,
    shared: &'a Shared,
}

impl StagedInput for Input<'_> {
    fn length(&self) -> u64 {
        self.length
    }
    fn unchanged(&self) -> Result<(), Error> {
        Ok(())
    }
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<(), Error> {
        match &self.body {
            Body::Data(blocks, start) => blocks.read_at(buf, start + offset),
            Body::Manifest(bytes) => CutSource::read_at(bytes.as_slice(), buf, offset),
        }
    }
    fn payload_hint(&self) -> Option<&str> {
        self.digest.as_deref()
    }
    fn before_publish(&self, store: &str, import: &str) -> Result<(), Error> {
        let mut j = self.shared.lock()?;
        if j.state.store.as_deref() != Some(store) || j.state.pending.is_none() {
            return Err(Error::Protocol("publication changed bulk store or range"));
        }
        j.change(|s| {
            if let Some(part) = &mut s.pending {
                part.import = Some(import.into());
            }
        })?;
        Ok(())
    }
}

fn set_id(state: &State) -> String {
    format!(
        "bulk:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(state.id.as_bytes()).as_bytes())
    )
}

struct Page {
    bytes: Vec<u8>,
    end: u32,
    offset: u64,
    final_page: bool,
}

fn manifest(
    dir: &Path,
    s: &State,
    limits: &PartLimits,
    mut end: u32,
    mut final_page: bool,
    sealed: bool,
) -> Result<Page, Error> {
    use purrdf_lex::json::{self, Object, Value};
    if end < s.page_start || end > s.count || end - s.page_start > WINDOW {
        return Err(Error::Protocol(
            "bulk manifest window outside receipt ledger",
        ));
    }
    let mut offset = s.page_offset;
    let mut rows = Vec::new();
    for index in s.page_start..end {
        let r: RangeReceipt = disk::read_meta(&dir.join(disk::receipt_name(index)))?;
        if r.index != index
            || r.start != offset
            || r.end < r.start
            || s.store.as_ref() != Some(&r.store_id)
        {
            return Err(Error::Protocol(
                "bulk range ledger is not contiguous on its bound store",
            ));
        }
        rows.push((
            PartSpan {
                index,
                start: r.start,
                length: r.end - r.start,
                format: parse_format(&r.format)?,
            },
            r.artifact_id,
        ));
        offset = r.end;
    }
    let format = parse_format(&s.format)?;
    loop {
        let bytes = if final_page && s.previous.is_none() {
            parts::manifest_body(&set_id(s), &s.source, format, offset, &rows)
        } else {
            let value: Value = Object::new()
                .with("schema", "sparrow.bulk-window/1")
                .with("set", set_id(s))
                .with("source", s.source.clone())
                .with("format", s.format.clone())
                .with(
                    "previous",
                    s.previous.clone().map_or(Value::Null, Value::from),
                )
                .with("first_index", u64::from(s.page_start))
                .with("start", s.page_offset)
                .with("end", offset)
                .with("complete", final_page)
                .with(
                    "parts",
                    Value::array(rows.iter().map(|(span, id)| parts::manifest_part(span, id))),
                )
                .into();
            json::write_compact(&value).into_bytes()
        };
        let length = bytes.len() as u64;
        if sealed
            || (length <= limits.raw_split_bytes
                && parts::is_one_part(bytes.as_slice(), length, Format::Json, limits)?)
        {
            return Ok(Page {
                bytes,
                end,
                offset,
                final_page,
            });
        }
        if rows.len() <= 1 {
            return Err(Error::InvalidArgument(
                "store limits cannot admit one bulk manifest window; retained receipts are available locally",
            ));
        }
        let (last, _) = rows
            .pop()
            .ok_or(Error::Protocol("empty bulk manifest window"))?;
        offset = last.start;
        end -= 1;
        final_page = false;
    }
}

fn part(
    state: &State,
    blocks: &Blocks,
    limits: &PartLimits,
    range: u64,
) -> Result<Option<PartSpan>, Error> {
    let start = state.confirmed;
    let format = parse_format(&state.format)?;
    if start == 0
        && state.eof
        && (state.staged == 0
            || (state.file.is_some() || state.staged <= range)
                && parts::is_one_part(blocks, state.staged, format, limits)?)
    {
        return Ok(Some(PartSpan {
            index: state.count,
            start,
            length: state.staged,
            format,
        }));
    }
    if start == state.staged {
        return Ok(None);
    }
    // A short read is a prefix. Only a real EOF, or the last complete newline, can end a
    // structured record. One virtual lookahead byte prevents next_cut treating a full
    // range of unfinished text as the original input's last record.
    let mut limits = *limits;
    limits.max_bytes = limits.max_bytes.min(range);
    let length = if state.eof {
        state.staged
    } else if matches!(format, Format::Log | Format::Jsonl) {
        if state.boundary > start {
            state.boundary
        } else if state.staged - start >= limits.max_bytes {
            start + limits.max_bytes + 1
        } else {
            return Ok(None);
        }
    } else {
        state.staged
    };
    parts::next_cut(blocks, length, start, state.count, format, &limits).map(Some)
}

async fn advance(shared: &Shared, client: &Client) -> Result<bool, Error> {
    let (s, blocks, dir) = {
        let j = shared.lock()?;
        (j.state.clone(), j.blocks(), j.dir.clone())
    };
    let finishing = s.eof && s.confirmed == s.staged && s.count > 0;
    if finishing && s.count == 1 {
        let r: RangeReceipt = disk::read_meta(&dir.join(disk::receipt_name(0)))?;
        let mut j = shared.lock()?;
        j.complete(r.artifact_id)?;
        shared.changed(&j.state);
        return Ok(true);
    }
    let set = set_id(&s);
    let limits = PartLimits::from_discovery(
        client.discovery(),
        sparrow_limits::SYSLOG_MESSAGE_BYTES as u64,
    )
    .map_err(|_| {
        Error::Unsupported("bulk splitting requires discovered import and retirement limits")
    })?;
    let page = match &s.pending {
        Some(p) => p
            .page_end
            .map(|end| manifest(&dir, &s, &limits, end, p.final_page, true))
            .transpose()?,
        None if finishing || s.count - s.page_start >= WINDOW => {
            Some(manifest(&dir, &s, &limits, s.count, finishing, false)?)
        }
        None => None,
    };
    let page_result = page.as_ref().map(|p| (p.end, p.offset, p.final_page));
    let (body, format, length, request) = if let Some(page) = page {
        let length = page.bytes.len() as u64;
        (
            Body::Manifest(page.bytes),
            Format::Json,
            length,
            format!("{set}.window{:010}", s.page_start),
        )
    } else {
        if s.count == u32::MAX {
            return Err(Error::InvalidArgument(
                "bulk job exhausted its range ordinals",
            ));
        }
        let (format, length) = match &s.pending {
            Some(p) => (parse_format(&p.format)?, p.length),
            None => {
                let Some(span) = part(&s, &blocks, &limits, shared.config.range_bytes)? else {
                    return Ok(false);
                };
                (span.format, span.length)
            }
        };
        (
            Body::Data(blocks, s.confirmed),
            format,
            length,
            parts::part_request(&set, s.count),
        )
    };
    let mut input = Input {
        body,
        length,
        digest: None,
        shared,
    };
    let digest = imports::payload_id(&input, &mut vec![0; IMPORT_CHUNK_BYTES])?;
    input.digest = Some(digest.clone());
    if let Some(p) = &s.pending {
        if p.digest != digest
            || p.length != length
            || p.page_end != page_result.map(|r| r.0)
            || p.format != format_label(format)
        {
            return Err(Error::Protocol(
                "staged bulk bytes or request changed after sealing",
            ));
        }
    } else {
        let mut j = shared.lock()?;
        j.change(|s| {
            s.pending = Some(Pending {
                length,
                format: format_label(format).into(),
                digest: digest.clone(),
                page_end: page_result.map(|r| r.0),
                final_page: page_result.is_some_and(|r| r.2),
                import: None,
            })
        })?;
    }
    let receipt = if let Some(id) = s.pending.as_ref().and_then(|p| p.import.as_ref()) {
        // An earlier publication may already have committed. Publishing only its original
        // native import can retrieve or finish it; it cannot begin a replacement journal.
        let expected = ExpectedArtifact {
            discovery: client.discovery(),
            source: &s.source,
            format,
            byte_length: length,
            payload_id: digest,
            durability: Durability::Durable,
            record_count: None,
        };
        imports::publish(
            client.capability(),
            &client.discovery().store_id,
            id,
            &expected,
        )
        .await?
    } else {
        imports::upload(
            client.capability(),
            client.discovery(),
            &request,
            &s.source,
            format,
            &input,
            &mut Checkpoint::reporting(Some(shared.upload.clone()), s.count),
        )
        .await?
    };
    let mut j = shared.lock()?;
    if let Some((end, offset, final_page)) = page_result {
        if final_page {
            j.complete(receipt.id)?;
        } else {
            j.window(receipt.id, end, offset)?;
        }
    } else {
        let range = RangeReceipt {
            index: s.count,
            start: s.confirmed,
            end: s.confirmed + length,
            format: format_label(format).into(),
            store_id: receipt.store_id,
            artifact_id: receipt.id,
            payload_id: receipt.payload_id,
        };
        j.certify(&range)?;
    }
    shared.status.send_modify(|p| p.diagnostic = None);
    shared.changed(&j.state);
    Ok(true)
}

fn retry(error: &Error, connecting: bool) -> bool {
    if error.retryable() || (connecting && crate::delivery::reconnectable(error)) {
        return true;
    }
    match error {
        Error::Io(e) => e.kind() == io::ErrorKind::TimedOut,
        Error::Domain { code, .. } => matches!(
            code.as_str(),
            "READ_ONLY" | "UNAUTHENTICATED" | "PERMISSION_DENIED" | "INSTANCE_MISMATCH"
        ),
        _ => false,
    }
}

pub(super) async fn run(shared: &Shared) -> Result<(), Error> {
    let mut connection = None;
    let mut delay = Duration::from_millis(100);
    loop {
        let state = shared.lock()?.state.clone();
        if state.completed.is_some() {
            return Ok(());
        }
        if !state.eof
            && state.count - state.page_start < WINDOW
            && (state.staged == state.confirmed
                || state
                    .file
                    .is_some_and(|file| file.length <= shared.config.staged_bytes))
        {
            connection = None;
            shared.wake.notified().await;
            continue;
        }
        let attempt = async {
            if connection.is_none() {
                let connected = match &shared.config.expect {
                    Some(expected) => {
                        Client::connect_producer_expecting(&shared.config.endpoint, expected)
                            .await?
                    }
                    None => Client::connect_producer(&shared.config.endpoint).await?,
                };
                if let Some(store) = state.store {
                    Expect::store(store).require(connected.discovery())?;
                }
                if shared.lock()?.state.store.is_none() {
                    shared
                        .lock()?
                        .change(|s| s.store = Some(connected.discovery().store_id.clone()))?;
                }
                connection = Some(connected);
            }
            advance(
                shared,
                connection
                    .as_ref()
                    .ok_or(Error::Protocol("bulk connection missing"))?,
            )
            .await
        };
        let result = tokio::time::timeout(Duration::from_secs(30), attempt)
            .await
            .unwrap_or_else(|_| {
                Err(Error::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "bulk RPC attempt timed out",
                )))
            });
        match result {
            Ok(true) => delay = Duration::from_millis(100),
            Ok(false) => {
                connection = None;
                shared.wake.notified().await;
            }
            Err(e) => {
                shared
                    .status
                    .send_modify(|p| p.diagnostic = Some(e.to_string()));
                let draft_expired = matches!(&e,Error::Domain{code,..} if code=="IMPORT_EXPIRED")
                    && shared
                        .lock()?
                        .state
                        .pending
                        .as_ref()
                        .is_some_and(|p| p.import.is_none());
                if !draft_expired && !retry(&e, connection.is_none()) {
                    return Err(e);
                }
                connection = None;
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
        }
    }
}
