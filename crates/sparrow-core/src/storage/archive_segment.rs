// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Archive segment selection and preparation, in memory.
//!
//! A segment covers one contiguous run of the eviction order after the archive watermark.
//! Selection reads only the catalog under the writer lock; the covered records' bytes are
//! then read and handed to the one encoder (`archive_encode`), which returns named byte
//! buffers. Nothing here knows the buffers' format and nothing does archive I/O: the caller
//! writes the buffers, verifies them, and only then calls `Store::archive_commit` with the
//! segment identity.

use super::archive::{ArchiveIntent, corrupt};
use super::{Artifact, Error, Record, Store, Writer, archive_encode, blake3, hex, record_bytes};
use purrdf_hash::frame::frame_le;
use rocksdb::{Direction, IteratorMode};

/// The horizon never trails `now` by less than this (archive format, section 5).
const HORIZON_FLOOR_MS: u64 = 10 * 60 * 1000;
const DAY_MS: u64 = 24 * 60 * 60 * 1000;
/// Records above this are held: neither archived nor deleted. The GTS writer holds the whole
/// segment resident and costs about three times a record while it writes; 128 MiB is the size
/// measured (`.stage/archive/probe/result.md`). It is a memory limit that relaxes when PurRDF
/// issue 1257 (a streaming writer) lands. The format for payloads above it is undecided and
/// with the operator.
const HOLD_BYTES: u64 = 128 * 1024 * 1024;
const FORMAT: &[u8] = b"sparrow-archive/1";

/// One encoded segment, ready to be written to the archive and verified.
///
/// Sparrow assumes nothing about the buffers' format: each is a file name and its bytes,
/// and the segment identity covers them as written.
#[derive(Clone, Debug)]
pub struct PreparedSegment {
    /// The segment's files in write order: file name, then bytes.
    pub files: Vec<(String, Vec<u8>)>,
    /// First covered eviction position, `{received_at:020}:{id}`.
    pub from: String,
    /// Last covered eviction position, inclusive.
    pub through: String,
    /// BLAKE3 identity over the format name, store, range and file digests.
    pub segment_id: [u8; 32],
    /// Record index entries covered.
    pub records: u64,
    /// Recordless artifact index entries covered.
    pub artifacts: u64,
    /// Covered records whose bytes were retired before the store was fenced; their
    /// metadata cannot be read, so they are counted here and absent from the files.
    pub retired_records: u64,
    /// Artifacts of covered entries with no RDF graph (retired before the fence), absent from
    /// the files. Every other covered record and artifact has its graph in the file.
    pub retired_artifacts: u64,
    /// Present when the segment is shorter than the range first selected because encoding
    /// failed and the range was halved; absent otherwise.
    pub shortened: Option<Shortened>,
}

/// How a prepare came to cover fewer entries than it first selected.
///
/// The entries left out stay after the watermark and are prepared by a later call, so
/// nothing is lost, but a caller that sees a short segment can tell why.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Shortened {
    /// Records and artifacts in the range first selected.
    pub selected: u64,
    /// Records and artifacts in the segment returned.
    pub kept: u64,
    /// Code of the first encode error that caused a halving, such as `RESULT_EXPIRED`.
    pub code: &'static str,
}

impl PreparedSegment {
    /// The segment's directory name under `<store_id>/`: `<from>-<through>`.
    #[must_use]
    pub fn name(&self) -> String {
        format!("{}-{}", self.from, self.through)
    }

    /// Lowercase hexadecimal segment identity.
    #[must_use]
    pub fn id_hex(&self) -> String {
        hex::encode(&self.segment_id)
    }
}

/// The covered range, chosen from the catalog alone.
struct Selection {
    first: String,
    last: String,
    records: u64,
    artifacts: u64,
    /// Records whose bytes and graphs are encoded.
    encoded: Vec<Record>,
    retired: u64,
    /// Artifacts whose graphs go in the file: recordless entries and the parents of encoded
    /// records.
    artifact_ids: std::collections::BTreeSet<String>,
}

/// Why a record at the head of the unarchived range is held.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HoldReason {
    /// Its bytes exceed what the GTS writer can carry (`HOLD_BYTES`).
    TooLarge,
    /// Its RDF content projection failed at ingest, so its evidence cannot be read whole.
    ProjectionFailed,
}

/// Whether a record must be held rather than archived.
fn hold_reason(writer: &Writer, record: &Record, length: u64) -> Result<Option<HoldReason>, Error> {
    if length > HOLD_BYTES {
        return Ok(Some(HoldReason::TooLarge));
    }
    if record.evicted || record.retired || record.rdf_projection_id.is_empty() {
        return Ok(None);
    }
    let key = format!("rdf:state:{}", record.rdf_projection_id);
    let Some(bytes) = writer.db.get(key)? else {
        return Err(corrupt("RDF projection state is missing"));
    };
    let state = super::parse(&bytes)?;
    let state: String = super::read_field(&state, "state")?;
    Ok((state == "failed").then_some(HoldReason::ProjectionFailed))
}

/// Whether an entry is due: by age once `start_fraction` of its window has passed, or
/// regardless of age when the store is at the size trigger.
fn due(now: u64, received_at: u64, expires_at: u64, fraction: f64, size_due: bool) -> bool {
    let window = expires_at.saturating_sub(received_at);
    size_due || now >= received_at.saturating_add((window as f64 * fraction) as u64)
}

/// Bytes a record contributes to a segment; a record retired before the fence has none.
fn encodable_length(record: &Record) -> u64 {
    if record.evicted || record.retired {
        0
    } else {
        record.byte_end.saturating_sub(record.byte_start)
    }
}

/// A record that cannot be archived at the head of the unarchived range. It is
/// neither archived nor deleted, and nothing behind it can be archived until it is resolved.
///
/// Only the head of the range is examined: a scan of every record's length would read the
/// whole unarchived catalog on every pass. The count of held records and the bytes waiting
/// behind the first one are therefore not known, and are not reported.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchiveHold {
    /// The held record.
    pub record_id: String,
    /// Its size in bytes, from the catalog.
    pub bytes: u64,
    /// Its eviction position, the first unarchived one.
    pub position: String,
    /// Why it is held.
    pub reason: HoldReason,
}

/// Index entries read under one hold of the writer. The selection takes the writer again for
/// the next batch, deferring first to waiting writers, so ingestion waits for at most one
/// batch however long the segment is. The unarchived range only grows at its end while a
/// segment is selected: its entries are held until a segment covers them, the one archive
/// worker is the only mover of the watermark, and the horizon keeps the selection behind
/// buffered publication. `archive_intend` re-counts the range under one hold regardless.
const SELECT_BATCH: usize = 256;

fn select(
    store: &Store,
    now: u64,
    intent: Option<&ArchiveIntent>,
    cap: Option<u64>,
) -> Result<Option<Selection>, Error> {
    let (policy, through, size_due, horizon) = {
        let writer = store.lock()?;
        let policy = writer.config.archive.clone().ok_or_else(|| {
            Error::new("UNSUPPORTED", "archiving is not configured for this store")
        })?;
        let through = writer
            .archive
            .as_ref()
            .and_then(|state| state.borrow().through.clone());
        let size_due = (writer.linked_physical_bytes()? as f64)
            >= policy.start_fraction
                * writer
                    .config
                    .disk_budget
                    .saturating_sub(writer.config.maintenance_reserve) as f64;
        let horizon = now.saturating_sub(
            HORIZON_FLOOR_MS.max(u64::from(writer.config.buffered_flush_interval_ms)),
        );
        (policy, through, size_due, horizon)
    };
    let entry_cap = cap.unwrap_or(policy.max_segment_records);
    let mut selection = Selection {
        first: String::new(),
        last: String::new(),
        records: 0,
        artifacts: 0,
        encoded: Vec::new(),
        retired: 0,
        artifact_ids: std::collections::BTreeSet::new(),
    };
    let mut payload = 0_u64;
    let mut first_day = 0;
    // Where the next batch resumes: the last entry handled, which begins as the watermark.
    let mut floor = through.clone().map(String::into_bytes);
    'scan: loop {
        let writer = store.lock_page()?;
        let mut handled = 0;
        for entry in writer.eviction_entries(floor.as_deref()) {
            let (key, _) = entry?;
            if handled == SELECT_BATCH {
                continue 'scan;
            }
            handled += 1;
            let (empty, position) = match key.strip_prefix(super::recordless::PREFIX) {
                Some(position) => (true, position),
                None => (false, &key[b"time:".len()..]),
            };
            if floor.as_deref().is_some_and(|floor| position <= floor) {
                continue;
            }
            floor = Some(position.to_vec());
            let position =
                std::str::from_utf8(position).map_err(|_| corrupt("position is not UTF-8"))?;
            let (Some(stamp), Some(id)) = (position.get(..20), position.get(21..)) else {
                return Err(corrupt("position is malformed"));
            };
            let received_at: u64 = stamp
                .parse()
                .map_err(|_| corrupt("position is malformed"))?;
            let count = selection.records + selection.artifacts;
            let rebuilding = intent.is_some();
            if !rebuilding {
                if count == entry_cap || received_at > horizon {
                    break 'scan;
                }
                if count > 0 && received_at / DAY_MS != first_day {
                    break 'scan;
                }
            }
            let (expires_at, length, record) = if empty {
                let artifact: Artifact = writer.get(id)?;
                (artifact.expires_at, 0, None)
            } else {
                let record: Record = writer.get(id)?;
                let length = encodable_length(&record);
                (record.expires_at, length, Some(record))
            };
            if !rebuilding
                && !due(
                    now,
                    received_at,
                    expires_at,
                    policy.start_fraction,
                    size_due,
                )
            {
                break 'scan;
            }
            if let Some(record) = &record
                && let Some(reason) = hold_reason(&writer, record, length)?
            {
                if count == 0 {
                    // Neither archived nor deleted: the watermark never passes it.
                    tracing::warn!(
                        record = id,
                        bytes = length,
                        reason = ?reason,
                        "archive held: record cannot be archived; it is not archived and not deleted"
                    );
                    return Ok(None);
                }
                if !rebuilding {
                    break 'scan;
                }
                return Err(corrupt("intent covers a held record"));
            }
            let oversized = length > policy.max_segment_bytes;
            if !rebuilding
                && count > 0
                && (oversized || payload + length > policy.max_segment_bytes)
            {
                break 'scan;
            }
            if count == 0 {
                selection.first = position.to_owned();
                first_day = received_at / DAY_MS;
            }
            position.clone_into(&mut selection.last);
            payload += length;
            match record {
                None => {
                    selection.artifacts += 1;
                    selection.artifact_ids.insert(id.to_owned());
                }
                Some(record) => {
                    selection.records += 1;
                    if record.evicted || record.retired {
                        selection.retired += 1;
                    } else {
                        selection.artifact_ids.insert(record.artifact_id.clone());
                        selection.encoded.push(record);
                    }
                }
            }
            if intent.is_some_and(|intent| position == intent.last) {
                break 'scan;
            }
            if oversized {
                break 'scan;
            }
        }
        break 'scan;
    }
    if selection.records + selection.artifacts == 0 {
        return Ok(None);
    }
    // The watermark passes every artifact positioned in the range, with or without records
    // in it: an artifact sorts before its own records, so a segment can end between them.
    // Its graph must be in this segment or the gate would let it retire unarchived.
    let prefix = super::artifact_rdf::TIME.as_bytes();
    let mut start = [prefix, through.as_deref().unwrap_or_default().as_bytes()].concat();
    'artifacts: loop {
        let writer = store.lock_page()?;
        let mut handled = 0;
        for row in writer
            .db
            .iterator(IteratorMode::From(&start, Direction::Forward))
        {
            let (key, _) = row?;
            let Some(position) = key.strip_prefix(prefix) else {
                break 'artifacts;
            };
            if position > selection.last.as_bytes() {
                break 'artifacts;
            }
            if handled == SELECT_BATCH {
                start = key.to_vec();
                continue 'artifacts;
            }
            handled += 1;
            if through
                .as_deref()
                .is_some_and(|through| position <= through.as_bytes())
            {
                continue;
            }
            let id = std::str::from_utf8(position)
                .ok()
                .and_then(|position| position.get(21..))
                .ok_or_else(|| corrupt("artifact index position is malformed"))?;
            selection.artifact_ids.insert(id.to_owned());
        }
        break;
    }
    Ok(Some(selection))
}

/// Segment identity (archive format, section 3). Sparrow writes no quarantine files yet,
/// so that count is zero; the identity covers the files as written, whatever their codec.
fn segment_id(store_id: &str, first: &str, last: &str, files: &[(String, Vec<u8>)]) -> [u8; 32] {
    let mut framed = Vec::new();
    for part in [
        FORMAT,
        store_id.as_bytes(),
        first.as_bytes(),
        last.as_bytes(),
    ] {
        frame_le(&mut framed, part);
    }
    for (name, bytes) in files {
        frame_le(&mut framed, name.as_bytes());
        frame_le(&mut framed, blake3::hash(bytes).as_bytes());
    }
    frame_le(&mut framed, &0_u64.to_le_bytes());
    *blake3::hash(&framed).as_bytes()
}

impl Store {
    /// The record holding up archiving at the head of the unarchived range, if the head is
    /// one above the carry bound. Reads one index entry and one catalog row.
    pub fn archive_held(&self) -> Result<Option<ArchiveHold>, Error> {
        let writer = self.lock()?;
        let state = writer.archive.as_ref().ok_or_else(|| {
            Error::new("UNSUPPORTED", "archiving is not configured for this store")
        })?;
        let through = state.borrow().through.clone();
        let Some(entry) = writer
            .eviction_entries(through.as_deref().map(str::as_bytes))
            .find(|entry| {
                entry.as_ref().map_or(true, |(key, _)| {
                    let position = key
                        .strip_prefix(super::recordless::PREFIX)
                        .unwrap_or(&key[b"time:".len()..]);
                    through
                        .as_deref()
                        .is_none_or(|through| position > through.as_bytes())
                })
            })
        else {
            return Ok(None);
        };
        let (key, _) = entry?;
        let Some(position) = key.strip_prefix(b"time:") else {
            return Ok(None);
        };
        let position =
            std::str::from_utf8(position).map_err(|_| corrupt("position is not UTF-8"))?;
        let Some(id) = position.get(21..) else {
            return Err(corrupt("position is malformed"));
        };
        let record: Record = writer.get(id)?;
        let bytes = encodable_length(&record);
        Ok(
            hold_reason(&writer, &record, bytes)?.map(|reason| ArchiveHold {
                record_id: record.id,
                bytes,
                position: position.to_owned(),
                reason,
            }),
        )
    }

    /// Select and encode the next due segment, or `None` when nothing is due.
    ///
    /// An entry is due once `now` passes `start_fraction` of its retention window, or all
    /// entries are due at the size trigger. The segment starts after the archive watermark
    /// and closes at the segment byte or entry limit, the UTC day of its first entry, or the
    /// horizon `now - max(10 minutes, buffered flush interval)`. The intent is recorded here;
    /// calling again while one is in flight rebuilds that same segment. A record above the
    /// carry bound is held, named in a warning, and stops selection before it.
    pub fn archive_prepare(&self, now: u64) -> Result<Option<PreparedSegment>, Error> {
        let store_id = self.store_id()?;
        let mut cap = None;
        let mut shortened: Option<(u64, &'static str)> = None;
        loop {
            // The guard ends with this block: `select` takes the writer itself, one batch at
            // a time, and the mutex is not re-entrant.
            let inflight = self
                .lock()?
                .archive
                .as_ref()
                .ok_or_else(|| {
                    Error::new("UNSUPPORTED", "archiving is not configured for this store")
                })?
                .borrow()
                .intent
                .clone();
            let selection = select(self, now, inflight.as_ref(), cap)?;
            let Some(selection) = selection else {
                return Ok(None);
            };
            let entries = selection.records + selection.artifacts;
            let attempt = std::time::Instant::now();
            match self.encode_selection(&store_id, selection) {
                Ok(mut prepared) => {
                    prepared.shortened = shortened.map(|(selected, code)| Shortened {
                        selected,
                        kept: prepared.records + prepared.artifacts,
                        code,
                    });
                    return Ok(Some(prepared));
                }
                // A segment that cannot be encoded is halved until the failing entry stands
                // alone, so the error names it instead of repeating on the whole range.
                Err((error, _))
                    if entries > 1 && inflight.is_none() && error.code != "UNSUPPORTED" =>
                {
                    let halved = entries / 2;
                    self.note_archive_halving();
                    tracing::warn!(
                        code = error.code,
                        message = %error.message,
                        entries,
                        cap = halved,
                        elapsed_ms = u64::try_from(attempt.elapsed().as_millis()).unwrap_or(u64::MAX),
                        "archive segment encoding failed; the range is halved and retried"
                    );
                    shortened.get_or_insert((entries, error.code));
                    cap = Some(halved);
                }
                Err((error, first)) => {
                    tracing::warn!(
                        entry = first,
                        code = error.code,
                        "archive held: entry cannot be encoded; it is not archived and not deleted"
                    );
                    return Err(Error::new(
                        error.code,
                        format!("archive cannot encode {first}: {}", error.message),
                    ));
                }
            }
        }
    }

    fn note_archive_halving(&self) {
        self.archive_halvings
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// How many times `archive_prepare` halved its range after an encode error since this
    /// store was opened. Not stored: it restarts at zero with the process.
    #[must_use]
    pub fn archive_halvings(&self) -> u64 {
        self.archive_halvings
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn encode_selection(
        &self,
        store_id: &str,
        selection: Selection,
    ) -> Result<PreparedSegment, (Error, String)> {
        let first = selection.first.clone();
        self.build_segment(store_id, selection)
            .map_err(|error| (error, first))
    }

    fn build_segment(
        &self,
        store_id: &str,
        selection: Selection,
    ) -> Result<PreparedSegment, Error> {
        let mut contents = Vec::with_capacity(selection.encoded.len());
        for record in &selection.encoded {
            let length = record.byte_end.saturating_sub(record.byte_start);
            let length = usize::try_from(length).map_err(|_| corrupt("record length overflows"))?;
            // A record with no bytes still gets its (empty) blob and link, so a reader finds
            // every covered record the same way.
            let bytes = if length == 0 {
                Vec::new()
            } else {
                let writer = self.lock_page()?;
                record_bytes(&writer, record.chunks.clone(), 0, length)?
            };
            contents.push((record.id.clone(), bytes));
        }
        let ids: Vec<String> = selection
            .encoded
            .iter()
            .map(|record| record.id.clone())
            .collect();
        let artifact_ids: Vec<String> = selection.artifact_ids.iter().cloned().collect();
        let (file, absent) = archive_encode::encode(self, &ids, &artifact_ids, contents)?;
        let files = vec![(format!("{}-{}.gts", selection.first, selection.last), file)];
        let id = segment_id(store_id, &selection.first, &selection.last, &files);
        self.archive_intend(&ArchiveIntent {
            segment_id: id,
            first: selection.first.clone(),
            last: selection.last.clone(),
            records: selection.records,
            artifacts: selection.artifacts,
        })?;
        Ok(PreparedSegment {
            files,
            from: selection.first,
            through: selection.last,
            segment_id: id,
            records: selection.records,
            artifacts: selection.artifacts,
            retired_records: selection.retired,
            retired_artifacts: absent,
            shortened: None,
        })
    }
}
