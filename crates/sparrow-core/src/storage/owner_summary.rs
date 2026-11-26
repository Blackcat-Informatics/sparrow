// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The per-owner payload summary: for each artifact and record, the owner's retention
//! scalars and the remaining multiplicity of each distinct chunk digest it owns, so the
//! retention metrics read one row per owner instead of one reference per chunk occurrence.
//!
//! One row per owner entry in the default family, `owner-summary:<owner id>`, where the
//! owner id is the `artifact:` or `record:` key it summarises. The value is
//!
//! ```text
//! version (1)  evicted (0|1)  received_at (u64)  expires_at (u64)
//! parent length (u16)  parent  distinct digests (u32)
//! (digest (32 bytes)  occurrences (u64))...        ascending by digest, occurrences > 0
//! ```
//!
//! with every integer big-endian. `parent` is empty for an artifact and the parent artifact
//! identity for a record. A row follows the owner's effective state: an artifact's chunks
//! after its manifest retirement position, and its evicted flag from its entry or its
//! retirement state; a record's own chunks and evicted flag. Whether an owner is live at an
//! instant is not stored: it depends on the instant and the pin set, so readers derive it
//! from `expires_at`, `evicted`, the owner identity and the parent.
//!
//! [`apply`] is the one owner: [`super::Writer::apply`] runs it on every batch, so every
//! change to an `artifact:`, `record:` or `artifact-retirement:` key, whichever writer
//! stages it, changes the row in the same atomic batch. An entry the batch writes gets its
//! row computed in full from the written entry; an entry the batch deletes loses its row; a
//! retirement state that advances without rewriting the entry subtracts the chunks it
//! passes from the stored row. A subtraction that would go below 0 is a `STORAGE_FAILURE`;
//! nothing is clamped.
//!
//! A store written before the rows existed, or last written by a binary that does not know
//! the family, is fenced at open: every stored row and the checkpoint are discarded and a
//! checkpointed online migration (`owner-summary`) rebuilds the rows from the authoritative
//! entries in bounded pages. Readers trust the rows only once the checkpoint reads complete
//! ([`complete`]); until then they read the entries.

use super::manifest_retire;
use super::store_counters::Reader;
use super::{Catalog, Error, Record, durability, parse, source_summary::failure};
use purrdf_hash::hex;
use rocksdb::{DB, WriteBatch};
use std::collections::BTreeMap;

/// The key family.
pub(super) const PREFIX: &[u8] = b"owner-summary:";
/// End of the key family range.
const PREFIX_END: &[u8] = b"owner-summary;";
/// Migration checkpoint: `[0]` complete; `[1]` then the last summarised artifact id in the
/// artifact phase; `[2]` then the last summarised record id in the record phase.
pub(super) const CHECKPOINT: &str = "owner-summary-upgrade";
const ARTIFACT: &[u8] = b"artifact:";
const RECORD: &[u8] = b"record:";
const VERSION: u8 = 1;
/// Owner entries one migration page summarises.
const PAGE_OWNERS: usize = 256;
/// Entry bytes one migration page reads; a page ends after the entry that reaches it.
const PAGE_BYTES: usize = 32 * 1024 * 1024;
/// Longest owner identity.
const MAX_ID: usize = 128;

/// The row key of an owner.
pub(super) fn summary_key(owner: &str) -> String {
    format!("owner-summary:{owner}")
}

/// The key prefix that holds the summaries of artifacts.
pub(super) const ARTIFACT_PREFIX: &[u8] = b"owner-summary:artifact:";
/// The key prefix that holds the summaries of records.
pub(super) const RECORD_PREFIX: &[u8] = b"owner-summary:record:";

fn retirement_key(id: &str) -> String {
    format!("artifact-retirement:{id}")
}

/// One owner's summary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Summary {
    pub(super) received_at: u64,
    pub(super) expires_at: u64,
    pub(super) evicted: bool,
    /// The parent artifact of a record; empty for an artifact.
    pub(super) parent: String,
    /// Remaining occurrences of each distinct chunk digest.
    pub(super) chunks: BTreeMap<[u8; 32], u64>,
}

impl Summary {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(28 + self.parent.len() + self.chunks.len() * 40);
        bytes.push(VERSION);
        bytes.push(u8::from(self.evicted));
        bytes.extend_from_slice(&self.received_at.to_be_bytes());
        bytes.extend_from_slice(&self.expires_at.to_be_bytes());
        // INVARIANT: a parent is at most MAX_ID bytes, checked before a summary is built.
        bytes.extend_from_slice(&(self.parent.len() as u16).to_be_bytes());
        bytes.extend_from_slice(self.parent.as_bytes());
        // INVARIANT: a summary holds at most the digests of one 16 MiB catalog row.
        bytes.extend_from_slice(&(self.chunks.len() as u32).to_be_bytes());
        for (digest, count) in &self.chunks {
            bytes.extend_from_slice(digest);
            bytes.extend_from_slice(&count.to_be_bytes());
        }
        bytes
    }

    /// Decode the row stored under `key`, checking its shape against the owner the key names.
    pub(super) fn decode(key: &[u8], bytes: &[u8]) -> Result<Self, Error> {
        let invalid = || failure("invalid owner summary row");
        let owner = key.strip_prefix(PREFIX).ok_or_else(invalid)?;
        let record = owner.starts_with(RECORD);
        if !(record || owner.starts_with(ARTIFACT)) || owner.len() > MAX_ID {
            return Err(invalid());
        }
        let mut input = bytes;
        let mut take = |length: usize| -> Result<&[u8], Error> {
            let (head, tail) = input.split_at_checked(length).ok_or_else(invalid)?;
            input = tail;
            Ok(head)
        };
        let word = |slice: &[u8]| -> Result<u64, Error> {
            Ok(u64::from_be_bytes(slice.try_into().map_err(|_| invalid())?))
        };
        if take(1)? != [VERSION] {
            return Err(invalid());
        }
        let evicted = match take(1)? {
            [0] => false,
            [1] => true,
            _ => return Err(invalid()),
        };
        let received_at = word(take(8)?)?;
        let expires_at = word(take(8)?)?;
        let parent_length = usize::from(u16::from_be_bytes(
            take(2)?.try_into().map_err(|_| invalid())?,
        ));
        let parent = std::str::from_utf8(take(parent_length)?)
            .map_err(|_| invalid())?
            .to_owned();
        if record != (parent.starts_with("artifact:") && parent.len() <= MAX_ID)
            || (!record && !parent.is_empty())
        {
            return Err(invalid());
        }
        let count = u32::from_be_bytes(take(4)?.try_into().map_err(|_| invalid())?);
        let mut chunks = BTreeMap::new();
        let mut last: Option<[u8; 32]> = None;
        for _ in 0..count {
            let digest: [u8; 32] = take(32)?.try_into().map_err(|_| invalid())?;
            let occurrences = word(take(8)?)?;
            if occurrences == 0 || last.is_some_and(|last| last >= digest) {
                return Err(invalid());
            }
            last = Some(digest);
            chunks.insert(digest, occurrences);
        }
        if !input.is_empty() {
            return Err(invalid());
        }
        Ok(Self {
            received_at,
            expires_at,
            evicted,
            parent,
            chunks,
        })
    }

    fn remove(&mut self, digest: [u8; 32], owner: &str) -> Result<(), Error> {
        match self.chunks.get_mut(&digest) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                self.chunks.remove(&digest);
            }
            None => {
                return Err(failure(format!(
                    "owner summary of {owner} would go below 0 for a retired chunk; the summary is inconsistent with the catalog"
                )));
            }
        }
        Ok(())
    }
}

/// The occurrences of each distinct digest among `chunks`.
fn occurrences(chunks: &[String]) -> Result<BTreeMap<[u8; 32], u64>, Error> {
    let mut counted = BTreeMap::new();
    for chunk in chunks {
        let digest = hex::decode_32_canonical(chunk)
            .ok_or_else(|| failure("retention ownership has a noncanonical payload identity"))?;
        *counted.entry(digest).or_insert(0_u64) += 1;
    }
    Ok(counted)
}

/// An artifact's summary from its entry `row` and its manifest retirement state: its chunks
/// after the retirement position, and its evicted flag from the entry or the state.
fn artifact_summary(id: &str, row: &[u8], retirement: Option<&[u8]>) -> Result<Summary, Error> {
    let state = retirement.map(manifest_retire::position).transpose()?;
    let mut remaining = usize::MAX;
    let header = manifest_retire::header(
        row,
        &mut remaining,
        state.map_or(0, |(_, retired, _)| retired),
        usize::MAX,
    )?;
    if header.artifact.id != id {
        return Err(failure(format!(
            "artifact catalog entry {id} names another identity"
        )));
    }
    if state.is_some_and(|(total, ..)| total != header.chunk_count()) {
        return Err(failure(
            "manifest retirement count disagrees with stored artifact",
        ));
    }
    let (received_at, expires_at) = (header.artifact.received_at, header.artifact.expires_at);
    let evicted = header.artifact.evicted || state.is_some_and(|(.., evicted)| evicted);
    Ok(Summary {
        received_at,
        expires_at,
        evicted,
        parent: String::new(),
        chunks: occurrences(&header.into_chunks())?,
    })
}

/// A record's summary from its entry `row`.
fn record_summary(id: &str, row: &[u8]) -> Result<Summary, Error> {
    let record = Record::decode(&parse(row)?)?;
    if record.id != id {
        return Err(failure(format!(
            "record catalog entry {id} names another identity"
        )));
    }
    if !record.artifact_id.starts_with("artifact:") || record.artifact_id.len() > MAX_ID {
        return Err(failure("record parent identity is not a bounded artifact"));
    }
    Ok(Summary {
        received_at: record.received_at,
        expires_at: record.expires_at,
        evicted: record.evicted,
        parent: record.artifact_id,
        chunks: occurrences(&record.chunks)?,
    })
}

/// Committed migration position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Progress {
    Complete,
    /// Artifacts at or before the id are summarised; `None` is the start.
    Artifacts(Option<String>),
    /// Every artifact and the records at or before the id are summarised.
    Records(Option<String>),
}

impl Progress {
    fn encode(&self) -> Vec<u8> {
        match self {
            Self::Complete => vec![0],
            Self::Artifacts(after) => {
                [&[1][..], after.as_deref().unwrap_or("").as_bytes()].concat()
            }
            Self::Records(after) => [&[2][..], after.as_deref().unwrap_or("").as_bytes()].concat(),
        }
    }

    /// Opaque status watermark; absent once complete.
    pub(super) fn watermark(&self) -> Option<String> {
        match self {
            Self::Complete => None,
            Self::Artifacts(None) => Some("start".to_owned()),
            Self::Artifacts(Some(id)) | Self::Records(Some(id)) => Some(id.clone()),
            Self::Records(None) => Some("record: start".to_owned()),
        }
    }
}

/// Read the committed migration position; a fenced store without one refuses.
pub(super) fn progress(
    read: impl FnOnce(&str) -> Result<Option<Vec<u8>>, Error>,
) -> Result<Progress, Error> {
    let bytes = read(CHECKPOINT)?.ok_or_else(|| {
        failure("required owner summary migration state missing; restore the original catalog")
    })?;
    let invalid = || failure("invalid owner summary migration checkpoint");
    let after = |rest: &[u8], prefix: &str| -> Result<Option<String>, Error> {
        if rest.is_empty() {
            return Ok(None);
        }
        std::str::from_utf8(rest)
            .ok()
            .filter(|id| id.starts_with(prefix) && id.len() <= MAX_ID)
            .map(|id| Some(id.to_owned()))
            .ok_or_else(invalid)
    };
    match bytes.split_first() {
        Some((0, [])) => Ok(Progress::Complete),
        Some((1, rest)) => Ok(Progress::Artifacts(after(rest, "artifact:")?)),
        Some((2, rest)) => Ok(Progress::Records(after(rest, "record:")?)),
        _ => Err(invalid()),
    }
}

/// Whether every owner entry has its summary row, so a reader may trust the rows.
pub(super) fn complete(
    read: impl FnOnce(&str) -> Result<Option<Vec<u8>>, Error>,
) -> Result<bool, Error> {
    Ok(progress(read)? == Progress::Complete)
}

/// Fence older writers into `batch` and seed the migration: the writer family lifts, every
/// row and checkpoint an older identity left is discarded (a binary that does not know the
/// family maintains none of them, so they cannot be trusted), the `done` counter starts at
/// 0 and the checkpoint starts at the first artifact, or complete when the store holds no
/// owner. Returns the fenced format.
pub(super) fn fence(
    db: &DB,
    format: &[u8],
    batch: &mut WriteBatch,
) -> Result<&'static [u8], Error> {
    batch.delete_range(PREFIX, PREFIX_END);
    batch.delete(CHECKPOINT);
    let owners = super::any_key(db, ARTIFACT)? || super::any_key(db, RECORD)?;
    batch.put(
        CHECKPOINT,
        if owners {
            Progress::Artifacts(None)
        } else {
            Progress::Complete
        }
        .encode(),
    );
    super::store_counters::seed_owner_summary(batch);
    let format = super::writer_format::enable_owner_summary(format)?;
    batch.put("store_format", format);
    Ok(format)
}

/// Final value of each observed owner entry and retirement state in one batch.
#[derive(Default)]
struct Changes {
    owners: BTreeMap<String, Option<Vec<u8>>>,
    retirements: BTreeMap<String, Option<Vec<u8>>>,
    derived: bool,
}

impl durability::BatchObserver for Changes {
    fn observe(&mut self, name: &[u8], value: Option<&[u8]>) -> Result<(), Error> {
        let text = |bytes: &[u8]| {
            std::str::from_utf8(bytes)
                .map(str::to_owned)
                .map_err(|_| failure("invalid catalog key"))
        };
        let value = value.map(<[u8]>::to_vec);
        if name.starts_with(ARTIFACT) || name.starts_with(RECORD) {
            self.owners.insert(text(name)?, value);
        } else if let Some(id) = name.strip_prefix(manifest_retire::PREFIX) {
            self.retirements.insert(text(id)?, value);
        } else if name.starts_with(PREFIX) {
            self.derived = true;
        }
        Ok(())
    }

    fn refuse_merge(&self, name: &[u8]) -> Option<Error> {
        (name.starts_with(ARTIFACT)
            || name.starts_with(RECORD)
            || name.starts_with(manifest_retire::PREFIX)
            || name.starts_with(PREFIX))
        .then(|| failure("the owner summary requires exact catalog writes, not merges"))
    }
}

/// Maintain the rows for every change `batch` makes to an artifact or record entry or to a
/// manifest retirement state, appending the row puts and deletes to the same batch. Runs on
/// every write through [`super::Writer::apply`].
pub(super) fn apply(db: &DB, batch: &mut WriteBatch) -> Result<(), Error> {
    let mut changes = Changes::default();
    durability::observe_batch(batch, &mut changes)?;
    if changes.owners.is_empty() && changes.retirements.is_empty() {
        // A migration page, or a batch with no owner: nothing follows an owner.
        return Ok(());
    }
    if changes.derived {
        return Err(failure(
            "owner summary rows are written only with the owner change they follow",
        ));
    }
    for (id, value) in &changes.owners {
        let key = summary_key(id);
        let Some(bytes) = value else {
            batch.delete(key);
            continue;
        };
        let summary = if id.starts_with("artifact:") {
            let retirement = match changes.retirements.get(id) {
                Some(next) => next.clone(),
                None => db.get(retirement_key(id))?,
            };
            artifact_summary(id, bytes, retirement.as_deref())?
        } else {
            record_summary(id, bytes)?
        };
        batch.put(key, summary.encode());
    }
    for (id, next) in &changes.retirements {
        if !changes.owners.contains_key(id) {
            advance(db, batch, id, next.as_deref())?;
        }
    }
    Ok(())
}

/// Move the stored row of an artifact whose manifest retirement state changes without its
/// entry: subtract the chunks the state passes. An artifact with no stored row is left to
/// the migration, which reads its final state when it reaches it.
fn advance(db: &DB, batch: &mut WriteBatch, id: &str, next: Option<&[u8]>) -> Result<(), Error> {
    let key = summary_key(id);
    let Some(stored) = db.get(&key)? else {
        // Skipping is correct only for an artifact the migration has not reached: it reads
        // the final retirement state when it does. For one it has passed, the row is lost.
        let passed = match progress(|key| Ok(db.get(key)?))? {
            Progress::Complete | Progress::Records(_) => true,
            Progress::Artifacts(after) => after.is_some_and(|after| id <= after.as_str()),
        };
        if passed {
            return Err(failure(format!(
                "owner summary row of {id} is missing though the migration has passed it; restore the original catalog"
            )));
        }
        return Ok(());
    };
    let row = db.get(id)?.ok_or_else(|| {
        failure(format!(
            "manifest retirement state of {id} has no artifact catalog entry"
        ))
    })?;
    let before = db
        .get(retirement_key(id))?
        .map(|bytes| manifest_retire::position(&bytes))
        .transpose()?;
    let after = next.map(manifest_retire::position).transpose()?;
    let (from, to) = (
        before.map_or(0, |(_, retired, _)| retired),
        after.map_or(0, |(_, retired, _)| retired),
    );
    let Some(after) = after.filter(|_| to >= from) else {
        // A state that was removed or moved back: derive the row again from the entry.
        batch.put(key, artifact_summary(id, &row, next)?.encode());
        return Ok(());
    };
    let mut summary = Summary::decode(key.as_bytes(), &stored)?;
    let passed = usize::try_from(to - from)
        .map_err(|_| failure("manifest position exceeds address space"))?;
    if passed > 0 {
        let mut remaining = usize::MAX;
        let header = manifest_retire::header(&row, &mut remaining, from, passed)?;
        if header.chunk_count() != after.0 {
            return Err(failure(
                "manifest retirement count disagrees with stored artifact",
            ));
        }
        let chunks = header.into_chunks();
        if chunks.len() != passed {
            return Err(failure("manifest retirement passes chunks the entry lacks"));
        }
        for chunk in &chunks {
            let digest = hex::decode_32_canonical(chunk).ok_or_else(|| {
                failure("retention ownership has a noncanonical payload identity")
            })?;
            summary.remove(digest, id)?;
        }
    }
    summary.evicted |= after.2;
    batch.put(key, summary.encode());
    Ok(())
}

/// Stage one migration page: the rows of the owner entries it reaches and the advanced
/// checkpoint in one batch, and whether the migration is complete after it. `None` when it
/// was already complete.
pub(super) fn page(db: &DB) -> Result<Option<(WriteBatch, bool)>, Error> {
    page_of(db, PAGE_OWNERS)
}

/// [`page`] over at most `limit` owner entries.
pub(super) fn page_of(db: &DB, limit: usize) -> Result<Option<(WriteBatch, bool)>, Error> {
    let progress = progress(|key| Ok(db.get(key)?))?;
    let (artifacts, after) = match &progress {
        Progress::Complete => return Ok(None),
        Progress::Artifacts(after) => (true, after.clone()),
        Progress::Records(after) => (false, after.clone()),
    };
    let reader = Reader::db(db);
    let mut batch = WriteBatch::default();
    let (mut examined, mut bytes) = (0_usize, 0_usize);
    let mut last = None;
    let mut full = false;
    reader.scan(
        if artifacts { ARTIFACT } else { RECORD },
        after.as_deref().map(str::as_bytes),
        |key, value| {
            if examined == limit || bytes >= PAGE_BYTES {
                full = true;
                return Ok(false);
            }
            let id = std::str::from_utf8(key).map_err(|_| failure("invalid owner catalog key"))?;
            examined += 1;
            bytes = bytes.saturating_add(value.len());
            let summary = if artifacts {
                let retirement = db.get(retirement_key(id))?;
                artifact_summary(id, value, retirement.as_deref())?
            } else {
                record_summary(id, value)?
            };
            batch.put(summary_key(id), summary.encode());
            last = Some(id.to_owned());
            Ok(true)
        },
    )?;
    let next = match (full, artifacts) {
        (true, true) => Progress::Artifacts(last.or(after)),
        (true, false) => Progress::Records(last.or(after)),
        (false, true) => Progress::Records(None),
        (false, false) => Progress::Complete,
    };
    batch.put(CHECKPOINT, next.encode());
    Ok(Some((batch, next == Progress::Complete)))
}

/// Lower the writer fence to its source bytes family and leave every row and the checkpoint
/// in place, as a store an older binary has opened and written is. The next open discards
/// the rows and seeds the online `owner-summary` migration again. Offline test fixtures only.
#[cfg(feature = "test-support")]
pub fn lower_owner_summary_for_legacy_fixture(db: &DB) -> Result<(), Error> {
    let format = db
        .get("store_format")?
        .ok_or_else(|| failure("writer format missing"))?;
    db.put(
        "store_format",
        super::writer_format::lower_owner_summary(&format),
    )?;
    Ok(())
}

/// Stored and recomputed owner summary rows, by key. Acceptance tests only.
#[cfg(feature = "test-support")]
pub type OwnerSummaryAudit = (BTreeMap<String, Vec<u8>>, BTreeMap<String, Vec<u8>>);

#[cfg(feature = "test-support")]
impl super::Store {
    /// Every stored owner summary row and the row recomputed from the owner's entry and
    /// retirement state, under the writer lock. A complete summary stores exactly the
    /// recomputed rows. Acceptance tests only.
    pub fn owner_summary_audit_for_tests(&self) -> Result<OwnerSummaryAudit, Error> {
        let writer = self.lock()?;
        let reader = Reader::db(&writer.db);
        let mut stored = BTreeMap::new();
        reader.scan(PREFIX, None, |key, value| {
            stored.insert(String::from_utf8_lossy(key).into_owned(), value.to_vec());
            Ok(true)
        })?;
        let mut recomputed = BTreeMap::new();
        for prefix in [ARTIFACT, RECORD] {
            reader.scan(prefix, None, |key, value| {
                let id =
                    std::str::from_utf8(key).map_err(|_| failure("invalid owner catalog key"))?;
                let summary = if prefix == ARTIFACT {
                    let retirement = writer.db.get(retirement_key(id))?;
                    artifact_summary(id, value, retirement.as_deref())?
                } else {
                    record_summary(id, value)?
                };
                recomputed.insert(summary_key(id), summary.encode());
                Ok(true)
            })?;
        }
        Ok((stored, recomputed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use crate::storage::Store;

    /// INVARIANT: a summary row decodes to exactly what it encoded, and a row whose digests
    /// are unordered, whose count is zero or whose parent contradicts its owner kind is
    /// refused rather than read as a different owner.
    #[test]
    fn a_summary_row_round_trips_and_a_malformed_row_is_refused() {
        let artifact = Summary {
            received_at: 5,
            expires_at: 9,
            evicted: false,
            parent: String::new(),
            chunks: BTreeMap::from([([1; 32], 3), ([2; 32], 1)]),
        };
        let key = summary_key("artifact:a");
        let encoded = artifact.encode();
        assert_eq!(Summary::decode(key.as_bytes(), &encoded).unwrap(), artifact);
        // A record must name its parent artifact; an artifact must not.
        assert!(Summary::decode(summary_key("record:r").as_bytes(), &encoded).is_err());
        let record = Summary {
            parent: "artifact:a".into(),
            ..artifact.clone()
        };
        assert_eq!(
            Summary::decode(summary_key("record:r").as_bytes(), &record.encode()).unwrap(),
            record
        );
        assert!(Summary::decode(key.as_bytes(), &record.encode()).is_err());
        // Digests ascend and occurrences are positive.
        let mut swapped = encoded.clone();
        let first = swapped.len() - 80;
        let (head, tail) = swapped.split_at_mut(first + 40);
        head[first..].swap_with_slice(&mut tail[..40]);
        assert!(Summary::decode(key.as_bytes(), &swapped).is_err());
        let mut zero = encoded.clone();
        let end = zero.len();
        zero[end - 8..].copy_from_slice(&0_u64.to_be_bytes());
        assert!(Summary::decode(key.as_bytes(), &zero).is_err());
        assert!(Summary::decode(key.as_bytes(), &encoded[..encoded.len() - 1]).is_err());
    }

    /// INVARIANT: the owner puts a summary row into the very batch that carries its entry,
    /// deletes it in the batch that deletes the entry, and subtracts retired chunks in the
    /// batch that advances a manifest retirement state. So the entry and its row commit
    /// together or not at all on the real write path, and a batch that brings its own row
    /// with an entry change is refused.
    #[test]
    fn the_row_travels_in_the_batch_that_creates_deletes_or_retires_the_entry() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), sparrow_test_support::storage_config!(Config)).unwrap();
        let writer = store.lock().unwrap();
        let digest = |byte: u8| hex::encode(&[byte; 32]);
        let row = |id: &str, chunks: &[String]| {
            format!(
                r#"{{"chunks":{},"record_ids":["record:r"],"evicted":false,"payload_id":"blake3:{}","byte_length":"7","expires_at":"9","received_at":"1","format":"log","source":"s","id":"{id}"}}"#,
                json_array(chunks),
                digest(0),
            )
        };
        let chunks = vec![digest(1), digest(1), digest(2)];
        let key = summary_key("artifact:a");

        // Publication: the row follows the entry in its own batch.
        let mut batch = WriteBatch::default();
        batch.put("artifact:a", row("artifact:a", &chunks));
        apply(&writer.db, &mut batch).unwrap();
        let stored = writer_rows(&batch, &key).expect("row staged with the entry");
        let summary = Summary::decode(key.as_bytes(), &stored).unwrap();
        assert_eq!(summary.chunks, BTreeMap::from([([1; 32], 2), ([2; 32], 1)]));
        writer.db.write(batch).unwrap();

        // Partial retirement: the state passes one chunk; the row loses exactly that one.
        let mut batch = WriteBatch::default();
        let mut state = [0_u8; 17];
        state[..8].copy_from_slice(&3_u64.to_be_bytes());
        state[8..16].copy_from_slice(&1_u64.to_be_bytes());
        batch.put(retirement_key("artifact:a"), state);
        apply(&writer.db, &mut batch).unwrap();
        let after = Summary::decode(key.as_bytes(), &writer_rows(&batch, &key).unwrap()).unwrap();
        assert_eq!(after.chunks, BTreeMap::from([([1; 32], 1), ([2; 32], 1)]));
        writer.db.write(batch).unwrap();

        // Deleting the entry deletes its row in that batch.
        let mut batch = WriteBatch::default();
        batch.delete("artifact:a");
        apply(&writer.db, &mut batch).unwrap();
        assert_eq!(writer_rows(&batch, &key), None);
        assert!(batch.len() >= 2);

        // A retirement state for an artifact whose row is missing is refused once the
        // migration has passed it, and left to the migration while it has not.
        writer
            .db
            .put("artifact:b", row("artifact:b", &chunks))
            .unwrap();
        let mut batch = WriteBatch::default();
        batch.put(retirement_key("artifact:b"), state);
        assert_eq!(
            apply(&writer.db, &mut batch).unwrap_err().code,
            "STORAGE_FAILURE"
        );
        writer
            .db
            .put(CHECKPOINT, Progress::Artifacts(None).encode())
            .unwrap();
        let mut batch = WriteBatch::default();
        batch.put(retirement_key("artifact:b"), state);
        apply(&writer.db, &mut batch).unwrap();

        // A batch that brings its own row with an entry change is refused.
        let mut batch = WriteBatch::default();
        batch.put("artifact:a", row("artifact:a", &chunks));
        batch.put(&key, []);
        assert_eq!(
            apply(&writer.db, &mut batch).unwrap_err().code,
            "STORAGE_FAILURE"
        );
    }

    fn json_array(items: &[String]) -> String {
        format!(
            "[{}]",
            items
                .iter()
                .map(|item| format!("\"{item}\""))
                .collect::<Vec<_>>()
                .join(",")
        )
    }

    /// The last put of `key` in `batch`, if the batch holds no later delete of it.
    fn writer_rows(batch: &WriteBatch, key: &str) -> Option<Vec<u8>> {
        #[derive(Default)]
        struct Last(Option<Vec<u8>>, Vec<u8>);
        impl durability::BatchObserver for Last {
            fn observe(&mut self, name: &[u8], value: Option<&[u8]>) -> Result<(), Error> {
                if name == self.1.as_slice() {
                    self.0 = value.map(<[u8]>::to_vec);
                }
                Ok(())
            }
            fn refuse_merge(&self, _: &[u8]) -> Option<Error> {
                None
            }
        }
        let mut last = Last(None, key.as_bytes().to_vec());
        durability::observe_batch(batch, &mut last).unwrap();
        last.0
    }
}
