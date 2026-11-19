// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The part cut and part driver for a completed file above the import bound (#280).
//!
//! A file is cut lazily, one part at a time, so that every part satisfies the three limits the
//! daemon advertises and enforces: the import bound, the record limit and the joint retirement
//! bound. Each part is an ordinary staged import under `consumeset:{H}.part{k:010}`; the
//! manifest `consumeset:{H}.manifest` lists the parts in order and alone defines the set.

use crate::imports::{Checkpoint, StagedInput, payload_id, upload};
use crate::ingest_status::PartGate;
use crate::{
    ArtifactReceipt, Client, CompletedFileIdentity, Discovery, Error, Format, PartDraft, PartRead,
    PartRecord,
};
use purrdf_lex::json::{self, Object, Value};
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

/// The daemon's chunk unit; a record of `n` bytes costs `ceil(n / CHUNK)` chunks.
const CHUNK: u64 = crate::IMPORT_CHUNK_BYTES as u64;

/// The limits a part must satisfy, all read from the daemon's discovery (#280).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartLimits {
    /// Largest declared input the daemon admits.
    pub max_bytes: u64,
    /// Longest record the daemon's layout keeps whole.
    pub raw_split_bytes: u64,
    /// Most records one artifact may hold.
    pub max_records: u64,
    /// What one artifact's chunk and record charges may sum to.
    pub retire_row_budget: u64,
    /// Charge of one chunk.
    pub retire_chunk_charge: u64,
    /// Charge of one record.
    pub retire_record_charge: u64,
    /// Largest syslog artifact the daemon stores as one packet. A syslog file above it is cut
    /// into opaque parts of at most this many bytes. The daemon's constant, supplied by the
    /// caller; discovery does not advertise it.
    pub syslog_packet_bytes: u64,
}

/// Which advertised values a daemon did not send, so no split can be planned.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MissingLimits {
    /// `maxImportBytes` is zero.
    pub max_import_bytes: bool,
    /// `rawRecordBytes` is zero.
    pub raw_record_bytes: bool,
    /// One of the retirement numbers is zero.
    pub retirement: bool,
}

impl MissingLimits {
    /// The wire names of the missing values, for an attempt text.
    pub fn names(&self) -> String {
        let mut names = Vec::new();
        if self.max_import_bytes {
            names.push("maxImportBytes");
        }
        if self.raw_record_bytes {
            names.push("rawRecordBytes");
        }
        if self.retirement {
            names.push(
                "the retirement numbers (retireRowBudget, retireChunkCharge, retireRecordCharge)",
            );
        }
        names.join(" and ")
    }
}

impl PartLimits {
    /// The limits when every advertised value is nonzero; otherwise which are missing. A
    /// daemon that does not send them keeps the single path as before, and only a split is
    /// refused. `syslog_packet_bytes` is the daemon's syslog packet limit, which the caller
    /// names because discovery does not carry it.
    pub fn from_discovery(
        discovery: &Discovery,
        syslog_packet_bytes: u64,
    ) -> Result<Self, MissingLimits> {
        let missing = MissingLimits {
            max_import_bytes: discovery.max_import_bytes == 0,
            raw_record_bytes: discovery.raw_record_bytes == 0,
            retirement: discovery.retire_row_budget == 0
                || discovery.retire_chunk_charge == 0
                || discovery.retire_record_charge == 0,
        };
        if missing != MissingLimits::default() {
            return Err(missing);
        }
        Ok(Self {
            max_bytes: discovery.max_import_bytes,
            raw_split_bytes: discovery.raw_record_bytes,
            max_records: discovery.max_records_per_artifact,
            retire_row_budget: discovery.retire_row_budget,
            retire_chunk_charge: discovery.retire_chunk_charge,
            retire_record_charge: discovery.retire_record_charge,
            syslog_packet_bytes,
        })
    }

    /// Whether `chunks` and `records` fit the record limit and the joint retirement bound,
    /// with the daemon's own inequality, computed without overflow.
    fn fits(&self, chunks: u64, records: u64) -> bool {
        records <= self.max_records
            && u128::from(self.retire_chunk_charge) * u128::from(chunks)
                + u128::from(self.retire_record_charge) * u128::from(records)
                <= u128::from(self.retire_row_budget)
    }

    /// Whether one raw record unit of `length` bytes, stored opaque, fits.
    fn fits_raw(&self, length: u64) -> bool {
        length <= self.max_bytes
            && self.fits(
                length.div_ceil(CHUNK),
                records_of(length, self.raw_split_bytes),
            )
    }
}

/// Records the daemon's layout can emit for one record of `length` bytes: one at or under the
/// raw split, and above it an upper bound for the raw pieces. The daemon's piece is the split
/// less the 16 MiB projected record bound, at least half the split, so counting pieces of half
/// the split is never below the daemon's count.
pub fn records_of(length: u64, split: u64) -> u64 {
    if length <= split {
        1
    } else {
        1 + (length - split).div_ceil((split / 2).max(1))
    }
}

/// One planned part: its byte range of the source and the format it is stored under.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartSpan {
    /// Zero-based part ordinal.
    pub index: u32,
    /// First source byte.
    pub start: u64,
    /// Byte length.
    pub length: u64,
    /// Stored format: the original line format for whole lines, otherwise opaque, so nothing
    /// parses a fragment.
    pub format: Format,
}

/// Positional reads of the bytes a cut is planned over.
pub trait CutSource {
    /// Fill `buffer` from `offset`; the range lies within the source.
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<(), Error>;
}

impl CutSource for [u8] {
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<(), Error> {
        let start = usize::try_from(offset)
            .map_err(|_| Error::InvalidArgument("cut offset exceeds memory"))?;
        let bytes = self
            .get(start..start + buffer.len())
            .ok_or(Error::InvalidArgument("cut range lies outside its source"))?;
        buffer.copy_from_slice(bytes);
        Ok(())
    }
}

impl CutSource for File {
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<(), Error> {
        Ok(self.read_exact_at(buffer, offset)?)
    }
}

fn line_format(format: Format) -> bool {
    matches!(format, Format::Jsonl | Format::Log)
}

/// Refusal of a part no cut can make fit, before any byte of it is uploaded. It names the
/// advertised values that admit no byte, and says the claim is pending, not refused.
fn unplannable(index: u32, limits: &PartLimits) -> Error {
    let mut failing = Vec::new();
    if limits.max_bytes == 0 {
        failing.push("maxImportBytes");
    }
    if limits.max_records == 0 {
        failing.push("maxRecordsPerArtifact");
    }
    if u128::from(limits.retire_chunk_charge) + u128::from(limits.retire_record_charge)
        > u128::from(limits.retire_row_budget)
    {
        failing.push(
            "the retirement numbers (retireRowBudget, retireChunkCharge, retireRecordCharge)",
        );
    }
    Error::Domain {
        code: "RESOURCE_LIMIT".into(),
        message: format!(
            "part {index} cannot fit one byte under the daemon's advertised {}; no byte was uploaded; the claim is pending; the daemon's configuration sets these values; the claim continues by itself when a connection advertises a usable value",
            failing.join(" and ")
        ),
        retryable: false,
        class: crate::FailureClass::Unclassified,
    }
}

/// The attempt's refusal to cut part `index` while the daemon advertises no usable limits.
fn missing_pending(index: u32, missing: MissingLimits) -> Error {
    Error::Domain {
        code: "RESOURCE_LIMIT".into(),
        message: format!(
            "part {index} cannot be cut: the daemon did not advertise {}; no byte was uploaded; the claim is pending until the daemon advertises it, which is the daemon's configuration; the claim continues by itself when it does",
            missing.names()
        ),
        retryable: false,
        class: crate::FailureClass::Unclassified,
    }
}

/// The end of the line that contains `start`, scanning at most to `limit`: one past its
/// newline, or `limit` when no newline lies before it.
fn line_end(
    source: &(impl CutSource + ?Sized),
    start: u64,
    limit: u64,
    buffer: &mut [u8],
) -> Result<u64, Error> {
    let mut at = start;
    while at < limit {
        let count = (limit - at).min(buffer.len() as u64) as usize;
        source.read_at(&mut buffer[..count], at)?;
        if let Some(index) = buffer[..count].iter().position(|byte| *byte == b'\n') {
            return Ok(at + index as u64 + 1);
        }
        at += count as u64;
    }
    Ok(limit)
}

/// The longest raw piece from `start`, at most `cap` bytes, that fits stored opaque.
fn raw_piece(limits: &PartLimits, start: u64, cap: u64, index: u32) -> Result<PartSpan, Error> {
    if !limits.fits_raw(1) || cap == 0 {
        return Err(unplannable(index, limits));
    }
    // Every charge grows with the length, so the fitting lengths are a prefix of 1..=cap.
    let (mut low, mut high) = (1, cap);
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if limits.fits_raw(middle) {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    Ok(PartSpan {
        index,
        start,
        length: low,
        format: Format::Opaque,
    })
}

/// Plan part `index`, which begins at `start` of a source of `length` bytes in `format`.
///
/// A line-format part that begins on a line boundary takes whole lines while all three limits
/// hold, counting each line as the daemon charges it: `records_of(line)` records and
/// `ceil(line / 65536)` chunks, the terminator included. A line that cannot fit alone is cut
/// raw, and the rest of that line forms the next part, stored opaque, up to its end. Every
/// other format is cut into raw parts stored opaque. A syslog part is also at most the syslog
/// packet limit, since the daemon stores one syslog artifact as one packet. A part that fails
/// here is never uploaded.
pub fn next_cut(
    source: &(impl CutSource + ?Sized),
    length: u64,
    start: u64,
    index: u32,
    format: Format,
    limits: &PartLimits,
) -> Result<PartSpan, Error> {
    if start >= length {
        return Err(Error::InvalidArgument("a part begins inside its source"));
    }
    let reach = match format {
        Format::Syslog => limits.max_bytes.min(limits.syslog_packet_bytes),
        _ => limits.max_bytes,
    };
    let window = length.min(start.saturating_add(reach));
    let mut buffer = vec![0; crate::IMPORT_CHUNK_BYTES];
    if !line_format(format) {
        return raw_piece(limits, start, window - start, index);
    }
    let on_boundary = start == 0 || {
        let mut previous = [0];
        source.read_at(&mut previous, start - 1)?;
        previous[0] == b'\n'
    };
    if !on_boundary {
        // The rest of a line an earlier part cut raw: up to that line's end, stored opaque.
        let end = line_end(source, start, window, &mut buffer)?;
        return raw_piece(limits, start, end - start, index);
    }
    let (mut line_start, mut chunks, mut records, mut cut) = (start, 0_u64, 0_u64, start);
    let mut accept = |end: u64, line_start: &mut u64, cut: &mut u64| {
        let line = end - *line_start;
        let next_chunks = chunks + line.div_ceil(CHUNK);
        let next_records = records + records_of(line, limits.raw_split_bytes);
        if !limits.fits(next_chunks, next_records) {
            return false;
        }
        (chunks, records, *line_start, *cut) = (next_chunks, next_records, end, end);
        true
    };
    let mut at = start;
    'scan: while at < window {
        let count = (window - at).min(buffer.len() as u64) as usize;
        source.read_at(&mut buffer[..count], at)?;
        for index in buffer[..count]
            .iter()
            .enumerate()
            .filter(|(_, byte)| **byte == b'\n')
            .map(|(index, _)| index)
        {
            if !accept(at + index as u64 + 1, &mut line_start, &mut cut) {
                break 'scan;
            }
        }
        at += count as u64;
    }
    // The source's last line need not end in a newline; the daemon lays it out as a record.
    if at == length && line_start < length && cut == line_start {
        accept(length, &mut line_start, &mut cut);
    }
    if cut > start {
        return Ok(PartSpan {
            index,
            start,
            length: cut - start,
            format,
        });
    }
    // Not even the first line fits: cut it raw.
    let end = line_end(source, start, window, &mut buffer)?;
    raw_piece(limits, start, end - start, index)
}

/// Whether `span`, cut earlier, is still admitted by `limits`: within the byte bound (and the
/// syslog packet bound), and, counting each whole line as the daemon charges it, within the
/// record limit and the joint retirement bound. A span stored opaque is one raw unit.
pub(crate) fn admits(
    source: &(impl CutSource + ?Sized),
    span: &PartSpan,
    limits: &PartLimits,
) -> Result<bool, Error> {
    let reach = match span.format {
        Format::Syslog => limits.max_bytes.min(limits.syslog_packet_bytes),
        _ => limits.max_bytes,
    };
    if span.length == 0 || span.length > reach {
        return Ok(false);
    }
    if !line_format(span.format) {
        return Ok(limits.fits_raw(span.length));
    }
    let end = span.start + span.length;
    let mut buffer = vec![0; crate::IMPORT_CHUNK_BYTES];
    let (mut chunks, mut records, mut line_start) = (0_u64, 0_u64, span.start);
    while line_start < end {
        let line_end = line_end(source, line_start, end, &mut buffer)?;
        let line = line_end - line_start;
        chunks += line.div_ceil(CHUNK);
        records += records_of(line, limits.raw_split_bytes);
        if !limits.fits(chunks, records) {
            return Ok(false);
        }
        line_start = line_end;
    }
    Ok(true)
}

/// Whether a source of `length` bytes is a single part, so it takes the single path. A
/// source no denser than one record per byte that fits is decided without reading it. A
/// syslog source above the packet limit is never one part: the daemon refuses it as one packet.
pub fn is_one_part(
    source: &(impl CutSource + ?Sized),
    length: u64,
    format: Format,
    limits: &PartLimits,
) -> Result<bool, Error> {
    if length == 0 {
        return Ok(true);
    }
    if length > limits.max_bytes {
        return Ok(false);
    }
    if format == Format::Syslog && length > limits.syslog_packet_bytes {
        return Ok(false);
    }
    if limits.fits(length, length) {
        return Ok(true);
    }
    Ok(next_cut(source, length, 0, 0, format, limits)?.length == length)
}

/// Durable progress of one claim's upload, ordered as it advances: completed parts, then
/// received bytes of the current part, its seal and layout stage, prepared records and raw
/// chunks of the record being prepared.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProgressMark {
    /// Parts published.
    pub parts_done: u32,
    /// Durably received bytes of the current upload.
    pub bytes: u64,
    /// Seal and layout stage of the current upload.
    pub stage: u8,
    /// Records prepared.
    pub prepared: u64,
    /// Raw chunks prepared of the record being prepared.
    pub raw_chunks: u64,
}

/// A claim's progress, shared with the worker so it survives an attempt dropped at a
/// deadline; in memory only.
#[derive(Debug, Default)]
pub struct PartsProgress {
    mark: Mutex<ProgressMark>,
    parts_total: AtomicU32,
    begun_again: AtomicU32,
}

impl PartsProgress {
    /// The highest mark reached in this attempt.
    pub fn mark(&self) -> ProgressMark {
        *self
            .mark
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Start an attempt: the mark is the attempt's own, so a part begun again after
    /// reclamation shows lower progress until it passes the old high-water mark.
    pub fn restart(&self) {
        *self
            .mark
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = ProgressMark::default();
    }

    /// Parts known once the last part is planned; zero while the cut has not reached the end.
    pub fn parts_total(&self) -> u32 {
        self.parts_total.load(Ordering::Relaxed)
    }

    /// Parts that began again after their lapsed staging was reclaimed, in this process.
    pub fn begun_again(&self) -> u32 {
        self.begun_again.load(Ordering::Relaxed)
    }

    /// The part counts that lead a recorded attempt text.
    pub fn summary(&self) -> String {
        let total = match self.parts_total() {
            0 => "?".to_owned(),
            total => total.to_string(),
        };
        format!(
            "parts {} of {total}, {} begun again",
            self.mark().parts_done,
            self.begun_again()
        )
    }

    pub(crate) fn raise(&self, parts_done: u32, progress: (u64, u8, u64, u64)) {
        let next = ProgressMark {
            parts_done,
            bytes: progress.0,
            stage: progress.1,
            prepared: progress.2,
            raw_chunks: progress.3,
        };
        let mut mark = self
            .mark
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *mark = (*mark).max(next);
    }
}

/// A planned part and its payload digest, kept so a retried attempt does not hash it again.
#[derive(Clone, Debug)]
struct PartState {
    span: PartSpan,
    payload: String,
}

/// One claim's planned part spans and payload digests, kept across attempts in memory so a
/// part is never hashed twice while the worker runs. Which parts are published is never kept
/// here: every attempt reads it from the daemon ([`recorded_prefix`]).
#[derive(Debug, Default)]
pub struct ClaimParts {
    state: Mutex<Option<(CompletedFileIdentity, Vec<PartState>)>>,
    /// The part whose cancelled or lapsed staging is awaited; its next begin is a begin again.
    waiting: Mutex<Option<u32>>,
}

impl ClaimParts {
    fn payload(&self, index: u32, span: &PartSpan) -> Option<String> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|(_, list)| list.get(index as usize))
            .filter(|part| part.span == *span)
            .map(|part| part.payload.clone())
    }

    fn remember(&self, index: u32, span: PartSpan, payload: String) {
        if let Some((_, list)) = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_mut()
        {
            let part = PartState { span, payload };
            if let Some(slot) = list.get_mut(index as usize) {
                *slot = part;
            } else if list.len() == index as usize {
                list.push(part);
            }
        }
    }

    fn wait_for(&self, index: u32) {
        *self
            .waiting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(index);
    }

    /// Whether `index` was awaited; clears the wait.
    fn take_wait(&self, index: u32) -> bool {
        let mut waiting = self
            .waiting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *waiting == Some(index) {
            *waiting = None;
            true
        } else {
            false
        }
    }
}

/// One part of a completed file, read positionally, bound to the whole file's identity.
struct PartInput<'a> {
    file: &'a File,
    identity: CompletedFileIdentity,
    start: u64,
    length: u64,
    payload: Option<&'a str>,
}

impl StagedInput for PartInput<'_> {
    fn length(&self) -> u64 {
        self.length
    }
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<(), Error> {
        Ok(self.file.read_exact_at(buffer, self.start + offset)?)
    }
    fn unchanged(&self) -> Result<(), Error> {
        self.identity.require(self.file)
    }
    fn payload_hint(&self) -> Option<&str> {
        self.payload
    }
}

/// How a part attempt waits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartWait {
    /// The part's staging lease lapsed: it waits for reclamation and then begins again from
    /// byte zero under the same request id, publishing under a new artifact id.
    AwaitingReclaim {
        /// The part ordinal.
        part: u32,
    },
}

/// How part `part` waits after `error`, if it is a wait rather than an ordinary failure.
///
/// `IMPORT_EXPIRED` is begin-again-after-reclaim and never permanent, although the daemon sends
/// it with `retryable = false` (`sparrow-server/src/rpc.rs`, `failure()`, which sends
/// `Error::is_retryable`: only `STORAGE_PRESSURE`, `SHUTTING_DOWN` and occupancy
/// `RESOURCE_LIMIT` are retryable). The daemon raises it when a
/// part's lease ended (`ImportManifest::require_live`, `sparrow-core/src/storage/import.rs`);
/// once reclamation deletes that row, a begin under the same request id starts afresh
/// (`begin_import`, which finds no row and no live receipt). The caller keeps its ordinary
/// backoff; nothing is cancelled and the source stays.
pub fn part_wait(part: u32, error: &Error) -> Option<PartWait> {
    matches!(error, Error::Domain { code, .. } if code == "IMPORT_EXPIRED")
        .then_some(PartWait::AwaitingReclaim { part })
}

/// The set request id of claim request `consume:{H}`: `consumeset:{H}`.
pub fn set_request(claim_request: &str) -> Result<String, Error> {
    claim_request
        .strip_prefix("consume:")
        .map(|hash| format!("consumeset:{hash}"))
        .ok_or(Error::InvalidArgument(
            "a part set belongs to a consumption claim",
        ))
}

/// The request id of part `index` of `set`.
pub fn part_request(set: &str, index: u32) -> String {
    format!("{set}.part{index:010}")
}

/// The request id of `set`'s manifest.
pub fn manifest_request(set: &str) -> String {
    format!("{set}.manifest")
}

/// The `sparrow.parts/1` manifest body: the original source label, format and length, and
/// each part's range, stored format and artifact in order.
pub fn manifest_body(
    set: &str,
    source: &str,
    format: Format,
    byte_length: u64,
    parts: &[(PartSpan, String)],
) -> Vec<u8> {
    let parts = parts
        .iter()
        .map(|(span, artifact)| {
            Value::from(
                Object::new()
                    .with("index", u64::from(span.index))
                    .with("start", span.start)
                    .with("length", span.length)
                    .with("format", crate::format_label(span.format))
                    .with("artifact", artifact.clone()),
            )
        })
        .collect::<Vec<_>>();
    let body: Value = Object::new()
        .with("schema", "sparrow.parts/1")
        .with("set", set)
        .with("source", source)
        .with("format", crate::format_label(format))
        .with("byte_length", byte_length)
        .with("parts", Value::array(parts))
        .into();
    json::write_compact(&body).into_bytes()
}

/// What the first index with no kept receipt says (#285).
#[derive(Debug)]
pub enum Next {
    /// Nothing of that part was begun: it is cut under the current limits.
    Fresh,
    /// A live draft is held for it: it continues only if the current limits still admit it.
    Draft(PartDraft),
    /// A cancelled or lapsed draft is not reclaimed yet: the part waits for reclamation.
    Expired,
}

/// A claim's recorded set prefix and what follows it (#285).
#[derive(Debug)]
pub struct Resume {
    /// The parts with a kept receipt from index 0, each `Stored` or `Evicted`.
    pub parts: Vec<PartRead>,
    /// The state of the first part without one.
    pub next: Next,
}

impl Resume {
    /// A claim with nothing recorded and nothing begun.
    #[must_use]
    pub fn fresh() -> Self {
        Self {
            parts: Vec::new(),
            next: Next::Fresh,
        }
    }
}

/// The decision of the part walk. Only `Measure` lets the caller take the single path (#285).
#[derive(Debug)]
pub enum Frontier {
    /// Index 0 has no receipt and no draft: measure the file against the limits.
    Measure,
    /// The set is begun: continue it from its recorded prefix.
    Recorded(Resume),
    /// A read failed or the state is unusable now. Nothing was uploaded; the claim is pending
    /// and the next attempt reads again.
    Pending(Error),
}

/// Why the walk stopped at index `parts.len()`.
#[derive(Debug)]
pub(crate) enum WalkStop {
    /// The receipt read failed.
    Receipt(Error),
    /// The part is being evicted: some record of it is still stored.
    Evicting,
    /// The part has no kept receipt; its status read gave this.
    Gate(Result<PartGate, Error>),
}

fn unimplemented(error: &Error) -> bool {
    matches!(error, Error::Transport(error) if error.kind == capnp::ErrorKind::Unimplemented)
}

fn pending(message: String) -> Error {
    Error::Domain {
        code: "CLAIM_PENDING".into(),
        message,
        retryable: true,
        class: crate::FailureClass::Unclassified,
    }
}

fn read_pending(index: usize, read: &str, error: &Error) -> Error {
    if unimplemented(error) {
        return pending(format!(
            "the daemon does not implement ingestPartReceipt; part {index} is pending until it does; nothing was uploaded"
        ));
    }
    pending(format!(
        "part {index} state could not be read ({read}: {error}); nothing was uploaded; the claim is pending and retries"
    ))
}

/// The one decision on a walk's reads: which of the claim's paths its recorded state admits.
/// `parts` are the non-absent receipts from index 0. Every input maps to exactly one variant,
/// and only a clean absence at index 0 (no receipt, then no draft) maps to `Measure`.
pub(crate) fn decide_frontier(parts: Vec<PartRead>, stop: WalkStop) -> Frontier {
    let index = parts.len();
    let gate = match stop {
        WalkStop::Receipt(error) => {
            return Frontier::Pending(read_pending(index, "receipt read", &error));
        }
        WalkStop::Evicting => {
            return Frontier::Pending(pending(format!(
                "part {index} is being evicted and is sent again once its last stored record is retired; nothing was uploaded; the claim is pending and retries"
            )));
        }
        WalkStop::Gate(Err(error)) => {
            return Frontier::Pending(read_pending(index, "status read", &error));
        }
        WalkStop::Gate(Ok(gate)) => gate,
    };
    let next = match gate {
        PartGate::NotFound if parts.is_empty() => return Frontier::Measure,
        PartGate::NotFound => Next::Fresh,
        PartGate::Draft(draft) => Next::Draft(draft),
        PartGate::Expired => Next::Expired,
        PartGate::Other(state) => {
            return Frontier::Pending(pending(format!(
                "part {index} has no kept receipt but its status is {state}; nothing was uploaded; the claim is pending and retries"
            )));
        }
    };
    Frontier::Recorded(Resume { parts, next })
}

/// Read claim `claim_request`'s recorded part prefix from the daemon: every kept receipt from
/// index 0, then the status of the first part without one. It runs on every attempt before any
/// begin, because a cached answer could hide an eviction between attempts.
pub async fn recorded_prefix(client: &Client, claim_request: &str) -> Result<Frontier, Error> {
    let set = set_request(claim_request)?;
    let store = client.discovery().store_id.clone();
    let mut parts = Vec::new();
    loop {
        let request = part_request(
            &set,
            u32::try_from(parts.len()).map_err(|_| {
                Error::Protocol("a part set cannot have more parts than a part ordinal can number")
            })?,
        );
        let stop = match crate::get_ingest_part_receipt(client.capability(), &store, &request).await
        {
            Err(error) => WalkStop::Receipt(error),
            Ok(PartRead::Evicting(_)) => WalkStop::Evicting,
            Ok(read @ (PartRead::Stored(_) | PartRead::Evicted(_))) => {
                parts.push(read);
                continue;
            }
            Ok(PartRead::Absent) => WalkStop::Gate(
                crate::ingest_status::get_part_gate(client.capability(), &store, &request).await,
            ),
        };
        return Ok(decide_frontier(parts, stop));
    }
}

/// The spans of a recorded prefix, each with the artifact id of a `Stored` part. Starts are the
/// sums of the recorded lengths: the set is cut from byte 0 with no gap. A prefix longer than
/// the file is a named storage failure and nothing is uploaded.
fn recorded_spans(
    set: &str,
    recorded: &[PartRead],
    length: u64,
) -> Result<Vec<(PartSpan, Option<String>)>, Error> {
    let mut start = 0_u64;
    let mut spans = Vec::with_capacity(recorded.len());
    for (index, read) in recorded.iter().enumerate() {
        let (record, stored) = match read {
            PartRead::Stored(record) => (record, true),
            PartRead::Evicted(record) => (record, false),
            PartRead::Absent | PartRead::Evicting(_) => {
                return Err(Error::Protocol("a recorded part is stored or evicted"));
            }
        };
        let PartRecord {
            artifact_id,
            byte_length,
            format,
        } = record;
        let end = start.checked_add(*byte_length).filter(|end| *end <= length);
        let (Some(end), true) = (end, *byte_length > 0) else {
            return Err(Error::Domain {
                code: "STORAGE_FAILURE".into(),
                message: format!(
                    "recorded part {index} of {set} covers bytes from {start} for {byte_length}, which a source of {length} bytes does not hold; nothing was uploaded"
                ),
                retryable: false,
                class: crate::FailureClass::Unclassified,
            });
        };
        let index = u32::try_from(index)
            .map_err(|_| Error::Protocol("a part ordinal exceeds its number"))?;
        spans.push((
            PartSpan {
                index,
                start,
                length: *byte_length,
                format: *format,
            },
            stored.then(|| artifact_id.clone()),
        ));
        start = end;
    }
    Ok(spans)
}

/// The span of the draft held at the frontier, if the current limits still admit it and its
/// payload is the file's bytes at that span. Otherwise the draft is cancelled and the part
/// waits for its reclamation: it is cut again under the current limits at the same index.
async fn continue_draft(
    client: &Client,
    parts: &ClaimParts,
    file: &File,
    identity: CompletedFileIdentity,
    limits: Result<&PartLimits, MissingLimits>,
    (index, start): (u32, u64),
    draft: &PartDraft,
) -> Result<(PartSpan, String), Error> {
    let limits = limits.map_err(|missing| missing_pending(index, missing))?;
    let span = PartSpan {
        index,
        start,
        length: draft.byte_length,
        format: draft.format,
    };
    let admitted =
        start.saturating_add(span.length) <= identity.length && admits(file, &span, limits)?;
    let mut buffer = vec![0; crate::IMPORT_CHUNK_BYTES];
    let payload = if admitted {
        let input = PartInput {
            file,
            identity,
            start,
            length: span.length,
            payload: None,
        };
        Some(payload_id(&input, &mut buffer)?)
    } else {
        None
    };
    if let Some(payload) = payload.filter(|payload| *payload == draft.payload_id) {
        return Ok((span, payload));
    }
    crate::imports::cancel(
        &client.endpoint,
        client.expectation(),
        &client.discovery().store_id,
        &draft.import_id,
    )
    .await?;
    parts.wait_for(index);
    Err(pending_reclaim(index))
}

fn pending_reclaim(index: u32) -> Error {
    Error::Domain {
        code: "IMPORT_EXPIRED".into(),
        message: format!(
            "part {index} waits for reclamation of its cancelled draft, then is cut again under the current limits"
        ),
        retryable: false,
        class: crate::FailureClass::Unclassified,
    }
}

/// Upload claim `claim_request`'s completed `file` as ordered parts, returning each part's span
/// and artifact id in order for [`ingest_manifest`]. `resume` is what [`recorded_prefix`] read
/// on this attempt: a part `Stored` is listed without a read, a hash or a begin; a part `Evicted`
/// is sent again on its recorded span under the same request id, because later parts start
/// where it ends; the rest are cut under the current `limits` and uploaded in order. A frontier
/// draft the limits no longer admit is cancelled and waited for. `limits` are read only when a
/// span must be cut. Progress is reported to `progress`.
#[expect(
    clippy::too_many_arguments,
    reason = "one claim's identity, its file, its limits, its cache, its progress sink and its resume point"
)]
pub async fn upload_parts(
    client: &Client,
    claim_request: &str,
    source: &str,
    format: Format,
    file: &File,
    limits: Result<&PartLimits, MissingLimits>,
    parts: &ClaimParts,
    progress: &Arc<PartsProgress>,
    resume: &Resume,
) -> Result<Vec<(PartSpan, String)>, Error> {
    let set = set_request(claim_request)?;
    let identity = CompletedFileIdentity::capture(&file.metadata()?);
    let length = identity.length;
    {
        let mut state = parts
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.as_ref().is_none_or(|(known, _)| *known != identity) {
            *state = Some((identity, Vec::new()));
        }
    }
    let recorded = recorded_spans(&set, &resume.parts, length)?;
    let frontier = u32::try_from(recorded.len())
        .map_err(|_| Error::Protocol("a part ordinal exceeds its number"))?;
    let frontier_start = recorded
        .last()
        .map_or(0, |(span, _)| span.start + span.length);
    let mut held = None;
    if frontier_start < length {
        match &resume.next {
            Next::Fresh => {}
            Next::Expired => {
                parts.wait_for(frontier);
                return Err(pending_reclaim(frontier));
            }
            Next::Draft(draft) => {
                held = Some(
                    continue_draft(
                        client,
                        parts,
                        file,
                        identity,
                        limits,
                        (frontier, frontier_start),
                        draft,
                    )
                    .await?,
                );
            }
        }
    }
    let mut buffer = vec![0; crate::IMPORT_CHUNK_BYTES];
    let mut index = 0_u32;
    let mut start = 0_u64;
    let mut listed = Vec::new();
    while start < length {
        let mut known = None;
        let span = if let Some((span, stored)) = recorded.get(index as usize) {
            known = stored.clone();
            *span
        } else if let Some((span, payload)) = held.take().filter(|_| index == frontier) {
            parts.remember(index, span, payload);
            span
        } else {
            let limits = limits.map_err(|missing| missing_pending(index, missing))?;
            next_cut(file, length, start, index, format, limits)?
        };
        if span.start + span.length == length {
            progress.parts_total.store(index + 1, Ordering::Relaxed);
        }
        let id = match known {
            Some(id) => id,
            None => {
                let mut input = PartInput {
                    file,
                    identity,
                    start: span.start,
                    length: span.length,
                    payload: None,
                };
                let payload = match parts.payload(index, &span) {
                    Some(payload) => payload,
                    None => payload_id(&input, &mut buffer)?,
                };
                parts.remember(index, span, payload.clone());
                input.payload = Some(&payload);
                if parts.take_wait(index) {
                    progress.begun_again.fetch_add(1, Ordering::Relaxed);
                }
                let result = upload(
                    client.capability(),
                    client.discovery(),
                    &part_request(&set, index),
                    source,
                    span.format,
                    &input,
                    &mut Checkpoint::reporting(Some(Arc::clone(progress)), index),
                )
                .await;
                match result {
                    Ok(receipt) => receipt.id,
                    Err(error) => {
                        let wait = part_wait(index, &error);
                        if wait.is_some() {
                            parts.wait_for(index);
                        }
                        return Err(match (wait, error) {
                            (
                                Some(PartWait::AwaitingReclaim { part }),
                                Error::Domain { message, .. },
                            ) => Error::Domain {
                                code: "IMPORT_EXPIRED".into(),
                                message: format!(
                                    "part {part} waits for reclamation of its lapsed staging, then begins again under the same request id: {message}"
                                ),
                                retryable: false,
                                class: crate::FailureClass::Unclassified,
                            },
                            (_, error) => error,
                        });
                    }
                }
            }
        };
        progress.raise(index + 1, (0, 0, 0, 0));
        listed.push((span, id));
        start = span.start + span.length;
        index += 1;
    }
    identity.require(file)?;
    Ok(listed)
}

/// Ingest the manifest of claim `claim_request`'s part set, listing `parts` in order, durably
/// under `consumeset:{H}.manifest`; its receipt is the set's publication proof.
pub async fn ingest_manifest(
    client: &Client,
    claim_request: &str,
    source: &str,
    format: Format,
    byte_length: u64,
    parts: &[(PartSpan, String)],
) -> Result<ArtifactReceipt, Error> {
    let set = set_request(claim_request)?;
    let body = manifest_body(&set, source, format, byte_length, parts);
    client
        .ingest(&manifest_request(&set), source, Format::Json, &body)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Production charges with a small bound so short inputs reach every limit.
    fn limits(max_bytes: u64, max_records: u64, retire_row_budget: u64) -> PartLimits {
        PartLimits {
            max_bytes,
            raw_split_bytes: 128 * 1024 * 1024,
            max_records,
            retire_row_budget,
            retire_chunk_charge: 134,
            retire_record_charge: 84,
            syslog_packet_bytes: u64::MAX,
        }
    }

    fn cut_all(input: &[u8], format: Format, limits: &PartLimits) -> Vec<PartSpan> {
        let mut parts = Vec::new();
        let mut start = 0;
        while start < input.len() as u64 {
            let span = next_cut(
                input,
                input.len() as u64,
                start,
                parts.len() as u32,
                format,
                limits,
            )
            .unwrap();
            assert_eq!(span.start, start, "parts are contiguous and in order");
            assert!(span.length > 0 && span.length <= limits.max_bytes);
            start += span.length;
            parts.push(span);
        }
        parts
    }

    fn lines(count: usize, width: usize) -> Vec<u8> {
        (0..count)
            .flat_map(|index| {
                let mut line = format!("{index:0width$}").into_bytes();
                line.truncate(width - 1);
                line.push(b'\n');
                line
            })
            .collect()
    }

    /// INVARIANT (#280): a line file is cut on line boundaries into parts that cover it in
    /// order and stay within the byte bound and the record limit; a file within every limit
    /// is one part.
    #[test]
    fn line_parts_cover_the_input_in_order_within_all_limits() {
        let input = lines(100, 10);
        let budget = u64::MAX / 4;
        for (name, limits, parts) in [
            ("within all limits", limits(10_000, 1_000, budget), 1),
            (
                "above the record limit, within the byte bound",
                limits(10_000, 30, budget),
                4,
            ),
            ("above the byte bound", limits(250, 1_000, budget), 4),
        ] {
            let cut = cut_all(&input, Format::Log, &limits);
            assert_eq!(cut.len(), parts, "{name}");
            for span in &cut {
                let bytes = &input[span.start as usize..(span.start + span.length) as usize];
                assert_eq!(bytes.last(), Some(&b'\n'), "{name}: whole lines");
                assert_eq!(span.format, Format::Log, "{name}");
                assert!(bytes.iter().filter(|b| **b == b'\n').count() as u64 <= limits.max_records);
            }
            assert_eq!(
                is_one_part(&input[..], input.len() as u64, Format::Log, &limits).unwrap(),
                parts == 1,
                "{name}"
            );
        }
    }

    /// INVARIANT (#280): a line longer than the byte bound is cut raw; the pieces that begin
    /// or end inside it are stored opaque, and the lines after it keep their format.
    #[test]
    fn over_long_line_is_cut_raw_and_counted() {
        let mut input = b"short\n".to_vec();
        input.extend(std::iter::repeat_n(b'x', 250));
        input.push(b'\n');
        input.extend_from_slice(b"after\n");
        let limits = limits(100, 1_000, u64::MAX / 4);
        let cut = cut_all(&input, Format::Jsonl, &limits);
        let formats: Vec<_> = cut.iter().map(|span| span.format).collect();
        assert_eq!(
            formats,
            [
                Format::Jsonl,
                Format::Opaque,
                Format::Opaque,
                Format::Opaque,
                Format::Jsonl
            ]
        );
        assert_eq!(cut[1].start, 6, "the long line begins its own part");
        assert_eq!(
            cut[4].start,
            input.len() as u64 - 6,
            "the next line starts a line part"
        );
    }

    /// INVARIANT (#280, OR-B): every non-line format is taken, cut into raw parts stored
    /// opaque that concatenate to the input, each but the last at the byte bound.
    #[test]
    fn non_line_formats_are_cut_raw_and_stored_opaque() {
        let input: Vec<u8> = (0..1_000u32).map(|n| (n % 251) as u8).collect();
        let limits = limits(300, 1_000, u64::MAX / 4);
        for format in [
            Format::Json,
            Format::Markdown,
            Format::Syslog,
            Format::Opaque,
        ] {
            let cut = cut_all(&input, format, &limits);
            assert_eq!(cut.len(), 4, "{format:?}");
            assert!(cut.iter().all(|span| span.format == Format::Opaque));
            assert!(cut[..3].iter().all(|span| span.length == 300));
        }
        let records = lines(10, 10);
        assert!(
            cut_all(&records, Format::Jsonl, &limits)
                .iter()
                .all(|span| span.format == Format::Jsonl)
        );
    }

    /// INVARIANT (#280, F2): a dense short-line file is cut so that every part satisfies the
    /// daemon's joint retirement inequality, charging each line its own chunks (not the
    /// part's bytes over 64 KiB); a smaller budget makes smaller parts.
    #[test]
    fn dense_short_line_file_is_cut_inside_the_joint_retirement_bound() {
        let input = lines(1_000, 100);
        // 1,000 lines of 100 bytes: one part would charge 1,000 chunks, not 2.
        let whole = 134 * 1_000 + 84 * 1_000;
        for budget in [whole / 2, whole / 4] {
            let limits = limits(u64::MAX, 100_000, budget);
            let cut = cut_all(&input, Format::Jsonl, &limits);
            for span in &cut {
                let part = &input[span.start as usize..(span.start + span.length) as usize];
                let line_lengths: Vec<u64> = part
                    .split_inclusive(|b| *b == b'\n')
                    .map(|line| line.len() as u64)
                    .collect();
                let chunks: u64 = line_lengths.iter().map(|len| len.div_ceil(65_536)).sum();
                let records = line_lengths.len() as u64;
                assert!(134 * chunks + 84 * records <= budget);
            }
            assert_eq!(cut.len() as u64, whole.div_ceil(budget), "budget {budget}");
        }
    }

    /// INVARIANT (#280, OR-F, F5): discovery keeps a zero value and plans no split, naming
    /// exactly the advertised values that are missing.
    #[test]
    fn discovery_keeps_a_zero_bound_and_plans_no_split() {
        let mut discovery = Discovery {
            durability_session: "session".into(),
            max_buffered_writes: 1,
            max_buffered_bytes: 1,
            flush_interval_ms: 1,
            store_id: "store:1".into(),
            schema_version: crate::SCHEMA_VERSION,
            max_inline_artifact_bytes: 1,
            max_records_per_artifact: 1,
            text_analyzer: "a".into(),
            text_profile: "t".into(),
            rdf_profile: "r".into(),
            max_event_batch_bytes: 1,
            max_event_batch_events: 1,
            max_import_bytes: 1 << 33,
            raw_record_bytes: 0,
            retire_row_budget: 0,
            retire_chunk_charge: 0,
            retire_record_charge: 0,
            instance_role: crate::sparrow_capnp::InstanceRole::Unspecified,
        };
        // A daemon at the previous release sends the import bound and none of the rest.
        let missing = PartLimits::from_discovery(&discovery, 65_536).unwrap_err();
        assert!(!missing.max_import_bytes && missing.raw_record_bytes && missing.retirement);
        assert!(missing.names().contains("rawRecordBytes"));
        assert!(missing.names().contains("retireRowBudget"));
        assert!(!missing.names().contains("maxImportBytes"));
        discovery.max_import_bytes = 0;
        let missing = PartLimits::from_discovery(&discovery, 65_536).unwrap_err();
        assert!(missing.names().contains("maxImportBytes"));
        (discovery.max_import_bytes, discovery.raw_record_bytes) = (1 << 33, 1 << 27);
        (
            discovery.retire_row_budget,
            discovery.retire_chunk_charge,
            discovery.retire_record_charge,
        ) = (1, 134, 84);
        assert!(PartLimits::from_discovery(&discovery, 65_536).is_ok());
    }

    /// INVARIANT (#280): a part whose staging lapsed waits for reclamation and begins again;
    /// IMPORT_EXPIRED is never treated as a permanent failure of the set.
    #[test]
    fn import_expired_waits_for_reclaim_and_is_not_permanent() {
        let expired = Error::Domain {
            code: "IMPORT_EXPIRED".into(),
            message: "staging lease ended".into(),
            retryable: false,
            class: crate::FailureClass::Unclassified,
        };
        assert_eq!(
            part_wait(3, &expired),
            Some(PartWait::AwaitingReclaim { part: 3 })
        );
        let conflict = Error::Domain {
            code: "CONFLICT".into(),
            message: "different input".into(),
            retryable: false,
            class: crate::FailureClass::Unclassified,
        };
        assert_eq!(part_wait(3, &conflict), None);
    }

    /// INVARIANT (#280): progress rises through seal, layout and preparation, where received
    /// bytes no longer move, so an attempt that only prepares still counts as progress.
    #[test]
    fn progress_tuple_rises_through_seal_layout_and_prepare() {
        let sink = Arc::new(PartsProgress::default());
        let mut checkpoint = Checkpoint::reporting(Some(Arc::clone(&sink)), 2);
        let mut previous = sink.mark();
        for step in [
            (100, 0, 0, 0),
            (100, 1, 0, 0),
            (100, 2, 0, 0),
            (100, 2, 0, 5),
            (100, 2, 1, 0),
        ] {
            checkpoint.raise_for_tests(step);
            let mark = sink.mark();
            assert!(mark > previous, "{step:?}");
            assert_eq!(mark.parts_done, 2);
            previous = mark;
        }
    }

    fn domain(code: &str) -> Error {
        Error::Domain {
            code: code.into(),
            message: format!("{code} from the daemon"),
            retryable: false,
            class: crate::FailureClass::Unclassified,
        }
    }

    fn stored(artifact: &str, byte_length: u64, format: Format) -> PartRead {
        PartRead::Stored(PartRecord {
            artifact_id: artifact.into(),
            byte_length,
            format,
        })
    }

    /// INVARIANT (#285): a span cut earlier is admitted only while each of the three limits
    /// that cut it still holds, counted as the daemon charges it: the byte bound, the record
    /// limit and the joint retirement bound, exactly at each boundary. A span stored opaque
    /// is one raw unit.
    #[test]
    fn a_span_is_admitted_only_while_the_limits_that_cut_it_still_hold() {
        let input = lines(40, 16);
        let span = PartSpan {
            index: 0,
            start: 0,
            length: input.len() as u64,
            format: Format::Log,
        };
        let need = (134 + 84) * 40;
        let admitted = |limits: PartLimits| admits(&input[..], &span, &limits).unwrap();
        assert!(admitted(limits(span.length, 40, need)));
        assert!(!admitted(limits(span.length - 1, 40, need)), "byte bound");
        assert!(!admitted(limits(span.length, 39, need)), "record limit");
        assert!(
            !admitted(limits(span.length, 40, need - 1)),
            "retirement bound"
        );
        let raw = PartSpan {
            format: Format::Opaque,
            length: 100,
            ..span
        };
        assert!(admits(&input[..], &raw, &limits(100, 1, 134 + 84)).unwrap());
        assert!(!admits(&input[..], &raw, &limits(99, 1, 134 + 84)).unwrap());
    }

    /// INVARIANT (#285): after a recorded prefix, which may end inside a line, the remainder
    /// is cut from the prefix's end under the current limits and the parts cover the source
    /// exactly once, in order, with no gap and no overlap.
    #[test]
    fn a_remainder_after_a_recorded_prefix_covers_the_source_exactly_once() {
        let input = lines(30, 16);
        let length = input.len() as u64;
        let recorded = [stored("artifact:a", 40, Format::Opaque)];
        let prefix = recorded_spans("consumeset:x", &recorded, length).unwrap();
        let mut start = prefix[0].0.start + prefix[0].0.length;
        let mut covered = vec![(0, start)];
        let current = limits(64, 1000, 1 << 30);
        let mut index = 1;
        while start < length {
            let span = next_cut(&input[..], length, start, index, Format::Log, &current).unwrap();
            assert_eq!(span.start, start);
            if index == 1 {
                assert_eq!(
                    span.format,
                    Format::Opaque,
                    "the rest of a cut line is opaque"
                );
                assert_eq!(span.start + span.length, 48, "it ends at that line's end");
            }
            covered.push((span.start, span.start + span.length));
            start += span.length;
            index += 1;
        }
        assert_eq!(covered.first().map(|part| part.0), Some(0));
        assert_eq!(covered.last().map(|part| part.1), Some(length));
        assert!(covered.windows(2).all(|pair| pair[0].1 == pair[1].0));
    }

    /// INVARIANT (#285): a limit that fits no byte uploads nothing and leaves the claim pending
    /// with a text naming the failing advertised values and the way out; a later connection
    /// that advertises a usable limit cuts the whole source.
    #[test]
    fn a_limit_below_one_byte_leaves_the_claim_pending_until_a_usable_limit_is_advertised() {
        let input = lines(10, 16);
        let length = input.len() as u64;
        for (bound, name) in [
            (limits(0, 100, 1 << 30), "maxImportBytes"),
            (limits(1 << 20, 100, 100), "the retirement numbers"),
        ] {
            let Err(Error::Domain { message, .. }) =
                next_cut(&input[..], length, 0, 0, Format::Log, &bound)
            else {
                panic!("a limit that fits no byte plans no part");
            };
            for text in [
                name,
                "no byte was uploaded",
                "the claim is pending",
                "continues by itself when a connection advertises a usable value",
            ] {
                assert!(message.contains(text), "{text}: {message}");
            }
        }
        let usable = limits(64, 100, 1 << 30);
        let parts = cut_all(&input, Format::Log, &usable);
        assert_eq!(parts.iter().map(|part| part.length).sum::<u64>(), length);
    }

    /// INVARIANT (#285): only a clean absence at index 0 (no receipt, then no draft) admits the
    /// single path. Every other result of either read is a named pending attempt that uploads
    /// nothing, and a daemon without the part read is a named pending state, never a failure.
    #[test]
    fn only_a_clean_index_zero_absence_admits_the_single_path() {
        let unimplemented = Error::Transport(capnp::Error::unimplemented("no such method".into()));
        let disconnected = Error::Transport(capnp::Error::disconnected("gone".into()));
        let draft = || {
            PartGate::Draft(PartDraft {
                import_id: "import:x".into(),
                byte_length: 1,
                payload_id: "blake3:x".into(),
                format: Format::Log,
            })
        };
        let one = || vec![stored("artifact:a", 8, Format::Log)];
        let rows: Vec<(&str, Vec<PartRead>, WalkStop, &str)> = vec![
            (
                "clean absence",
                vec![],
                WalkStop::Gate(PartGate::from_error(domain("NOT_FOUND"))),
                "measure",
            ),
            (
                "draft at 0",
                vec![],
                WalkStop::Gate(Ok(draft())),
                "recorded 0 draft",
            ),
            (
                "expired at 0",
                vec![],
                WalkStop::Gate(PartGate::from_error(domain("IMPORT_EXPIRED"))),
                "recorded 0 expired",
            ),
            (
                "receipt RESOURCE_LIMIT",
                vec![],
                WalkStop::Receipt(domain("RESOURCE_LIMIT")),
                "pending part 0 state could not be read (receipt read",
            ),
            (
                "receipt STORAGE_FAILURE",
                vec![],
                WalkStop::Receipt(domain("STORAGE_FAILURE")),
                "pending part 0 state could not be read (receipt read",
            ),
            (
                "receipt unimplemented",
                vec![],
                WalkStop::Receipt(unimplemented),
                "pending the daemon does not implement ingestPartReceipt; part 0 is pending until it does; nothing was uploaded",
            ),
            (
                "receipt disconnected",
                vec![],
                WalkStop::Receipt(disconnected),
                "pending part 0 state could not be read (receipt read",
            ),
            (
                "status STORAGE_FAILURE",
                vec![],
                WalkStop::Gate(PartGate::from_error(domain("STORAGE_FAILURE"))),
                "pending part 0 state could not be read (status read",
            ),
            (
                "status RESOURCE_LIMIT",
                vec![],
                WalkStop::Gate(PartGate::from_error(domain("RESOURCE_LIMIT"))),
                "pending part 0 state could not be read (status read",
            ),
            (
                "accepted, not a draft",
                vec![],
                WalkStop::Gate(Ok(PartGate::Other("accepted".into()))),
                "pending part 0 has no kept receipt but its status is accepted",
            ),
            (
                "next part never begun",
                one(),
                WalkStop::Gate(PartGate::from_error(domain("NOT_FOUND"))),
                "recorded 1 fresh",
            ),
            (
                "draft at the frontier",
                one(),
                WalkStop::Gate(Ok(draft())),
                "recorded 1 draft",
            ),
            (
                "expired at the frontier",
                one(),
                WalkStop::Gate(PartGate::from_error(domain("IMPORT_EXPIRED"))),
                "recorded 1 expired",
            ),
            (
                "frontier status fails",
                one(),
                WalkStop::Gate(PartGate::from_error(domain("STORAGE_FAILURE"))),
                "pending part 1 state could not be read (status read",
            ),
            (
                "part being evicted",
                one(),
                WalkStop::Evicting,
                "pending part 1 is being evicted",
            ),
        ];
        for (label, parts, stop, expected) in rows {
            let seen = match decide_frontier(parts, stop) {
                Frontier::Measure => "measure".to_owned(),
                Frontier::Recorded(resume) => format!(
                    "recorded {} {}",
                    resume.parts.len(),
                    match resume.next {
                        Next::Fresh => "fresh",
                        Next::Draft(_) => "draft",
                        Next::Expired => "expired",
                    }
                ),
                Frontier::Pending(Error::Domain { message, .. }) => format!("pending {message}"),
                Frontier::Pending(other) => {
                    panic!("{label}: pending is a named domain error: {other}")
                }
            };
            if expected.starts_with("pending") {
                assert!(seen.starts_with(expected), "{label}: {seen}");
                assert!(seen.contains("nothing was uploaded"), "{label}: {seen}");
            } else {
                assert_eq!(seen, expected, "{label}");
            }
        }
    }

    /// INVARIANT (#285): a recorded prefix longer than the file is a named storage failure and
    /// nothing is uploaded; a prefix that ends exactly at the file's end is the whole set.
    #[test]
    fn a_recorded_prefix_longer_than_the_file_is_a_named_storage_failure() {
        let recorded = [
            stored("artifact:a", 40, Format::Log),
            PartRead::Evicted(PartRecord {
                artifact_id: "artifact:b".into(),
                byte_length: 40,
                format: Format::Log,
            }),
        ];
        let Err(Error::Domain { code, message, .. }) =
            recorded_spans("consumeset:abc", &recorded, 60)
        else {
            panic!("a prefix longer than the file is refused");
        };
        assert_eq!(code, "STORAGE_FAILURE");
        assert!(
            message.contains("recorded part 1 of consumeset:abc"),
            "{message}"
        );
        let whole = recorded_spans("consumeset:abc", &recorded, 80).unwrap();
        assert_eq!(whole.len(), 2);
        assert!(whole[1].1.is_none(), "an evicted part is sent again");
    }
}
