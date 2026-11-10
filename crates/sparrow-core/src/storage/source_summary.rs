// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Exact per-source counters and the per-source artifact index (#14).
//!
//! Three derived row families follow the catalog's retirement state, not the retention
//! deadline:
//!
//! - `source-summary:<source hash>`: the source label and its exact `retained_records`
//!   (record entries present and not evicted), `complete_artifacts` (artifact entries
//!   present and not evicted, by their own flag or their manifest retirement state) and
//!   `fragment_parents` (artifacts that are not complete while at least one of their
//!   records is retained). A source whose three counts are all 0 has no row.
//! - `artifact-retained:<artifact id>`: the artifact's retained record count, present
//!   exactly while the artifact is complete or has a retained record.
//! - `source-artifact:<source hash>:<received_at as 16 hex digits>:<artifact id>`: the
//!   per-source artifact index, present under the same condition.
//!
//! [`apply`] is the one owner: [`super::Writer::apply`] runs it on every batch before the
//! batch is written, so every change to a `record:`, `artifact:` or `artifact-retirement:`
//! key, whichever writer stages it, changes these rows in the same atomic batch. A count
//! that would go below 0, or a missing row for a source that has counted entries, is a
//! `STORAGE_FAILURE` naming the source; nothing is clamped.
//!
//! Stores written before the rows existed are fenced at open with the source summary writer
//! families and seeded by an online migration (`source-summary`). Its first phase walks the
//! artifact entries and counts each with its records; its second phase walks the record
//! entries and counts the retained records of parents whose entry was already deleted. A
//! write during the migration maintains the rows of every artifact the migration has
//! already counted, or that the write itself creates, and leaves the others for the
//! migration, which reads their state when it reaches them.
//!
//! Until it completes, readers add a [`Completion`] to the stored rows: the migration's own
//! counting passes run to the end over the reader's view without writing, so a listing
//! answers exactly as it will once the migration is complete.

use super::store_counters::Reader;
use super::{Error, Writer, integer, manifest_retire, parse, read_field};
use purrdf_lex::json::{self, Object};
use rocksdb::{DB, WriteBatch};
use std::collections::BTreeMap;

pub(super) const SUMMARY: &str = "source-summary:";
const RETAINED: &str = "artifact-retained:";
pub(super) const INDEX: &str = "source-artifact:";
/// Migration checkpoint: `[0]` complete; `[1]` then the last counted artifact ID in the
/// artifact phase; `[2]` then the last examined record ID in the orphan record phase.
pub(super) const CHECKPOINT: &str = "source-summary-upgrade";
const ARTIFACT_PREFIX: &[u8] = b"artifact:";
const RECORD_PREFIX: &[u8] = b"record:";
/// Artifacts one migration page counts. Each is read with its records.
const PAGE_ARTIFACTS: usize = 512;
/// Record entries one migration page reads; a page stops after the artifact or record that
/// reaches it, so one artifact is always counted whole.
const PAGE_RECORDS: usize = 16384;

/// The store's source-key hash: BLAKE3 of the exact source label, as lower-case hex.
pub(super) fn source_hash(source: &str) -> String {
    super::hex::encode(super::blake3::hash(source.as_bytes()).as_bytes())
}

pub(super) fn summary_key(source: &str) -> String {
    format!("{SUMMARY}{}", source_hash(source))
}

fn retained_key(id: &str) -> String {
    format!("{RETAINED}{id}")
}

/// The per-source artifact index prefix of one source.
pub(super) fn index_prefix(source: &str) -> String {
    format!("{INDEX}{}:", source_hash(source))
}

/// One source's index key at a receipt time.
pub(super) fn index_key(source: &str, received_at: u64, id: &str) -> String {
    format!("{}{received_at:016x}:{id}", index_prefix(source))
}

/// The receipt time and artifact identity an index key names under `prefix`.
pub(super) fn index_entry<'k>(prefix: &[u8], key: &'k [u8]) -> Result<(u64, &'k str), Error> {
    let invalid = || Error::new("STORAGE_FAILURE", "invalid per-source artifact index key");
    let rest = key.strip_prefix(prefix).ok_or_else(invalid)?;
    let (time, id) = rest.split_at_checked(16).ok_or_else(invalid)?;
    let id = id.strip_prefix(b":").ok_or_else(invalid)?;
    let time = std::str::from_utf8(time)
        .ok()
        .and_then(|time| u64::from_str_radix(time, 16).ok())
        .ok_or_else(invalid)?;
    let id = std::str::from_utf8(id)
        .ok()
        .filter(|id| id.starts_with("artifact:") && id.len() > 9 && id.len() <= 128)
        .ok_or_else(invalid)?;
    Ok((time, id))
}

/// A `STORAGE_FAILURE` for a broken derived counter row; the store counters share it.
pub(super) fn failure(message: impl Into<String>) -> Error {
    Error::new("STORAGE_FAILURE", message.into())
}

/// One source's exact counts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Counts {
    pub(super) source: String,
    pub(super) retained_records: u64,
    pub(super) complete_artifacts: u64,
    pub(super) fragment_parents: u64,
}

impl Counts {
    fn encode(&self) -> String {
        json::write_compact(
            &Object::new()
                .with("source", self.source.clone())
                .with("retained_records", self.retained_records.to_string())
                .with("complete_artifacts", self.complete_artifacts.to_string())
                .with("fragment_parents", self.fragment_parents.to_string())
                .into(),
        )
    }

    /// Decode one summary row and check that its key names its source.
    pub(super) fn decode(key: &[u8], bytes: &[u8]) -> Result<Self, Error> {
        let value = parse(bytes)?;
        let counts = Self {
            source: read_field(&value, "source")?,
            retained_records: integer(&value, "retained_records")?,
            complete_artifacts: integer(&value, "complete_artifacts")?,
            fragment_parents: integer(&value, "fragment_parents")?,
        };
        if counts.source.is_empty()
            || counts.source.len() > 512
            || summary_key(&counts.source).as_bytes() != key
        {
            return Err(failure(
                "per-source counter row disagrees with its source key",
            ));
        }
        Ok(counts)
    }

    fn is_empty(&self) -> bool {
        self.retained_records == 0 && self.complete_artifacts == 0 && self.fragment_parents == 0
    }
}

/// Signed changes to one source's counts.
#[derive(Default)]
struct Delta {
    retained_records: i64,
    complete_artifacts: i64,
    fragment_parents: i64,
    /// Whether any changed artifact of the source was counted before the batch.
    counted_before: bool,
}

fn shift(source: &str, name: &str, value: u64, change: i64) -> Result<u64, Error> {
    value.checked_add_signed(change).ok_or_else(|| {
        failure(format!(
            "{name} of source {source:?} would go below 0 (stored {value}, change {change}); the per-source counters are inconsistent with the catalog"
        ))
    })
}

/// `counts` with `delta` applied; a count below 0 refuses by naming the source.
fn shifted(mut counts: Counts, delta: &Delta) -> Result<Counts, Error> {
    let source = counts.source.clone();
    counts.retained_records = shift(
        &source,
        "retained_records",
        counts.retained_records,
        delta.retained_records,
    )?;
    counts.complete_artifacts = shift(
        &source,
        "complete_artifacts",
        counts.complete_artifacts,
        delta.complete_artifacts,
    )?;
    counts.fragment_parents = shift(
        &source,
        "fragment_parents",
        counts.fragment_parents,
        delta.fragment_parents,
    )?;
    Ok(counts)
}

/// Read one source's summary row from a database or snapshot reader.
pub(super) fn counts(
    read: impl FnOnce(&str) -> Result<Option<Vec<u8>>, Error>,
    source: &str,
) -> Result<Option<Counts>, Error> {
    let key = summary_key(source);
    read(&key)?
        .map(|bytes| Counts::decode(key.as_bytes(), &bytes))
        .transpose()
}

/// Add `deltas` to the summary rows in `batch`. A missing row for a source that has
/// counted entries, or a count below 0, refuses the batch by naming the source.
fn merge(db: &DB, batch: &mut WriteBatch, deltas: BTreeMap<String, Delta>) -> Result<(), Error> {
    for (source, delta) in deltas {
        // A batch whose changes cancel out leaves the row as it is.
        if (
            delta.retained_records,
            delta.complete_artifacts,
            delta.fragment_parents,
        ) == (0, 0, 0)
        {
            continue;
        }
        let stored = counts(|key| Ok(db.get(key)?), &source)?;
        let next = match stored {
            Some(counts) => counts,
            None => {
                if delta.counted_before || super::any_key(db, index_prefix(&source).as_bytes())? {
                    return Err(failure(format!(
                        "source {source:?} has counted catalog entries but no per-source counter row"
                    )));
                }
                Counts {
                    source: source.clone(),
                    ..Counts::default()
                }
            }
        };
        let next = shifted(next, &delta)?;
        if next.is_empty() {
            batch.delete(summary_key(&source));
        } else {
            batch.put(summary_key(&source), next.encode());
        }
    }
    Ok(())
}

/// How an artifact's retained count was established.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// Every retained record of the artifact is counted.
    Full,
    /// A parent whose entry was deleted before the migration reached it: only retained
    /// records at or before the record phase's watermark are counted.
    Partial,
}

/// Which record changes of an artifact the counters include.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Counted {
    All,
    /// Only records the orphan record phase has passed.
    Watermark,
}

/// One `artifact-retained:` row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Retained {
    count: u64,
    mode: Mode,
}

impl Retained {
    fn read(reader: &Reader<'_>, id: &str) -> Result<Option<Self>, Error> {
        let Some(bytes) = reader.get(retained_key(id).as_bytes())? else {
            return Ok(None);
        };
        let (count, mode) = bytes
            .split_first_chunk::<8>()
            .ok_or_else(|| failure(format!("invalid retained record count of {id}")))?;
        let mode = match mode {
            [0] => Mode::Full,
            [1] => Mode::Partial,
            _ => return Err(failure(format!("invalid retained record count of {id}"))),
        };
        Ok(Some(Self {
            count: u64::from_be_bytes(*count),
            mode,
        }))
    }

    fn encode(self) -> [u8; 9] {
        let mut bytes = [0; 9];
        bytes[..8].copy_from_slice(&self.count.to_be_bytes());
        bytes[8] = u8::from(self.mode == Mode::Partial);
        bytes
    }
}

/// Committed migration position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Progress {
    /// Every catalog entry is counted.
    Complete,
    /// Counting artifacts; artifacts at or before the ID are counted.
    Artifacts(Option<String>),
    /// Counting the retained records of deleted parents; records at or before the ID are.
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

    /// Whether an artifact whose entry exists has been counted.
    fn artifact_counted(&self, id: &str) -> bool {
        match self {
            Self::Complete | Self::Records(_) => true,
            Self::Artifacts(after) => after.as_deref().is_some_and(|after| id <= after),
        }
    }

    /// Whether the record phase has passed a record of a deleted parent.
    fn record_counted(&self, id: &str) -> bool {
        match self {
            Self::Complete => true,
            Self::Artifacts(_) => false,
            Self::Records(after) => after.as_deref().is_some_and(|after| id <= after),
        }
    }

    /// Opaque status watermark; absent once complete.
    pub(super) fn watermark(&self) -> Option<String> {
        match self {
            Self::Complete => None,
            Self::Artifacts(None) => Some("start".to_owned()),
            Self::Artifacts(Some(id)) | Self::Records(Some(id)) => Some(id.clone()),
            Self::Records(None) => Some("record phase start".to_owned()),
        }
    }
}

/// Read the committed migration position; a fenced store without one refuses.
pub(super) fn progress(
    read: impl FnOnce(&str) -> Result<Option<Vec<u8>>, Error>,
) -> Result<Progress, Error> {
    let bytes = read(CHECKPOINT)?.ok_or_else(|| {
        failure("required per-source counter migration state missing; restore the original catalog")
    })?;
    let invalid = || failure("invalid per-source counter migration checkpoint");
    let after = |rest: &[u8]| -> Result<Option<String>, Error> {
        if rest.is_empty() {
            return Ok(None);
        }
        std::str::from_utf8(rest)
            .map(|id| Some(id.to_owned()))
            .map_err(|_| invalid())
    };
    match bytes.split_first() {
        Some((0, [])) => Ok(Progress::Complete),
        Some((1, rest)) => Ok(Progress::Artifacts(after(rest)?)),
        Some((2, rest)) => Ok(Progress::Records(after(rest)?)),
        _ => Err(invalid()),
    }
}

/// Fence older writers into `batch` and seed the migration. With no catalog entry to count
/// it is complete as it starts. Counter rows, index rows or a checkpoint already present
/// under an older writer identity are not trusted: no binary that writes them leaves an
/// older identity in place, so they are discarded in the same batch and the migration
/// recounts every source from the catalog entries. Returns the fenced writer format.
pub(super) fn fence(
    db: &DB,
    format: &[u8],
    batch: &mut WriteBatch,
) -> Result<&'static [u8], Error> {
    for prefix in [SUMMARY, RETAINED, INDEX] {
        // Every family prefix ends in ':'; ';' is the next byte, so the range is exactly
        // the family.
        let end = format!("{};", &prefix[..prefix.len() - 1]);
        batch.delete_range(prefix.as_bytes(), end.as_bytes());
    }
    batch.delete(CHECKPOINT);
    let pending = super::any_key(db, ARTIFACT_PREFIX)? || super::any_key(db, RECORD_PREFIX)?;
    batch.put(
        CHECKPOINT,
        if pending {
            Progress::Artifacts(None)
        } else {
            Progress::Complete
        }
        .encode(),
    );
    let format = super::writer_format::enable_source_summary(format)?;
    batch.put("store_format", format);
    Ok(format)
}

/// A record entry's identity and retirement state.
struct RecordState {
    artifact: String,
    source: String,
    received_at: u64,
    retained: bool,
}

fn record_state(id: &str, bytes: &[u8]) -> Result<RecordState, Error> {
    let value = parse(bytes)?;
    if read_field::<String>(&value, "id")? != id {
        return Err(failure(format!(
            "record catalog entry {id} names another identity"
        )));
    }
    Ok(RecordState {
        artifact: read_field(&value, "artifact_id")?,
        source: read_field(&value, "source")?,
        received_at: integer(&value, "received_at")?,
        retained: !read_field::<bool>(&value, "evicted")?,
    })
}

/// An artifact entry's identity and own eviction flag.
struct EntryState {
    source: String,
    received_at: u64,
    evicted: bool,
}

fn entry_state(id: &str, bytes: &[u8]) -> Result<EntryState, Error> {
    let mut remaining = usize::MAX;
    let header = manifest_retire::header(bytes, &mut remaining, 0, 0)?;
    if header.artifact.id != id {
        return Err(failure(format!(
            "artifact catalog entry {id} names another identity"
        )));
    }
    Ok(EntryState {
        source: header.artifact.source,
        received_at: header.artifact.received_at,
        evicted: header.artifact.evicted,
    })
}

/// The identity both an artifact entry and each of its records must agree on.
fn agree(
    id: &str,
    known: &mut Option<(String, u64)>,
    source: &str,
    received_at: u64,
) -> Result<(), Error> {
    match known {
        Some((known_source, known_time)) => {
            if known_source != source || *known_time != received_at {
                return Err(failure(format!(
                    "artifact {id} and its records disagree on source or receipt (source {known_source:?} at {known_time}, then {source:?} at {received_at})"
                )));
            }
        }
        None => *known = Some((source.to_owned(), received_at)),
    }
    Ok(())
}

/// Final value of each observed key in one batch.
#[derive(Default)]
struct Changes {
    records: BTreeMap<String, Option<Vec<u8>>>,
    artifacts: BTreeMap<String, Option<Vec<u8>>>,
    retirements: BTreeMap<String, Option<Vec<u8>>>,
    derived: bool,
}

impl Changes {
    fn observed(name: &[u8]) -> bool {
        name.starts_with(RECORD_PREFIX)
            || name.starts_with(ARTIFACT_PREFIX)
            || name.starts_with(manifest_retire::PREFIX)
            || name.starts_with(SUMMARY.as_bytes())
            || name.starts_with(RETAINED.as_bytes())
            || name.starts_with(INDEX.as_bytes())
    }
}

impl super::durability::BatchObserver for Changes {
    fn observe(&mut self, name: &[u8], value: Option<&[u8]>) -> Result<(), Error> {
        let text = || {
            std::str::from_utf8(name)
                .map(str::to_owned)
                .map_err(|_| failure("invalid catalog key"))
        };
        let value = value.map(<[u8]>::to_vec);
        if name.starts_with(RECORD_PREFIX) {
            self.records.insert(text()?, value);
        } else if name.starts_with(ARTIFACT_PREFIX) {
            self.artifacts.insert(text()?, value);
        } else if let Some(id) = name.strip_prefix(manifest_retire::PREFIX) {
            let id =
                std::str::from_utf8(id).map_err(|_| failure("invalid manifest retirement key"))?;
            self.retirements.insert(id.to_owned(), value);
        } else if name.starts_with(SUMMARY.as_bytes())
            || name.starts_with(RETAINED.as_bytes())
            || name.starts_with(INDEX.as_bytes())
        {
            self.derived = true;
        }
        Ok(())
    }
    fn refuse_merge(&self, name: &[u8]) -> Option<Error> {
        Self::observed(name)
            .then(|| failure("per-source counters require exact catalog writes, not merges"))
    }
}

/// One artifact's record changes in a batch.
#[derive(Default)]
struct Touched {
    /// Signed retained-record change and record ID, per changed record.
    records: Vec<(String, i64)>,
    identity: Option<(String, u64)>,
}

/// Maintain the per-source counters, retained counts and artifact index for every change
/// `batch` makes to a record, artifact or manifest retirement entry, appending the derived
/// writes to the same batch. Runs on every write through [`Writer::apply`].
pub(super) fn apply(writer: &Writer, batch: &mut WriteBatch) -> Result<(), Error> {
    let mut changes = Changes::default();
    super::durability::observe_batch(batch, &mut changes)?;
    let catalog = !changes.records.is_empty()
        || !changes.artifacts.is_empty()
        || !changes.retirements.is_empty();
    if !catalog {
        return Ok(());
    }
    if changes.derived {
        return Err(failure(
            "per-source counter rows are written only with the catalog change they follow",
        ));
    }
    let db = &*writer.db;
    let progress = progress(|key| Ok(db.get(key)?))?;
    let mut touched = BTreeMap::<String, Touched>::new();
    for (id, next) in &changes.records {
        let before = db
            .get_pinned(id)?
            .map(|bytes| record_state(id, &bytes))
            .transpose()?;
        let after = next
            .as_deref()
            .map(|bytes| record_state(id, bytes))
            .transpose()?;
        let (Some(state), other) = (before.as_ref().or(after.as_ref()), after.as_ref()) else {
            continue;
        };
        if let Some(other) = other
            && (other.artifact != state.artifact
                || other.source != state.source
                || other.received_at != state.received_at)
        {
            return Err(failure(format!(
                "record {id} cannot change its parent, source or receipt"
            )));
        }
        let change = i64::from(after.as_ref().is_some_and(|state| state.retained))
            - i64::from(before.as_ref().is_some_and(|state| state.retained));
        let entry = touched.entry(state.artifact.clone()).or_default();
        agree(
            &state.artifact,
            &mut entry.identity,
            &state.source,
            state.received_at,
        )?;
        entry.records.push((id.clone(), change));
    }
    for id in changes.artifacts.keys().chain(changes.retirements.keys()) {
        touched.entry(id.clone()).or_default();
    }
    let mut deltas = BTreeMap::<String, Delta>::new();
    for (id, mut change) in touched {
        let before = db
            .get_pinned(&id)?
            .map(|bytes| entry_state(&id, &bytes))
            .transpose()?;
        let after = match changes.artifacts.get(&id) {
            Some(next) => next
                .as_deref()
                .map(|bytes| entry_state(&id, bytes))
                .transpose()?,
            None => before.as_ref().map(|state| EntryState {
                source: state.source.clone(),
                received_at: state.received_at,
                evicted: state.evicted,
            }),
        };
        for state in before.iter().chain(after.iter()) {
            agree(&id, &mut change.identity, &state.source, state.received_at)?;
        }
        let retired_before = db
            .get_pinned(format!("artifact-retirement:{id}"))?
            .map(|bytes| manifest_retire::evicted_flag(&bytes))
            .transpose()?
            .unwrap_or(false);
        let retired_after = match changes.retirements.get(&id) {
            Some(next) => next
                .as_deref()
                .map(manifest_retire::evicted_flag)
                .transpose()?
                .unwrap_or(false),
            None => retired_before,
        };
        let complete_before = before
            .as_ref()
            .is_some_and(|state| !state.evicted && !retired_before);
        let complete_after = after
            .as_ref()
            .is_some_and(|state| !state.evicted && !retired_after);
        let row = Retained::read(&Reader::db(db), &id)?;
        let created = before.is_none() && after.is_some();
        let Some((source, received_at)) = change.identity else {
            // Neither an entry nor a record: a retirement state of an artifact already gone.
            continue;
        };
        // Which of this batch's record changes the counters already include.
        let counted = match (row, created) {
            (Some(_), true) => {
                return Err(failure(format!(
                    "new artifact {id} of source {source:?} already has a retained record count"
                )));
            }
            (_, true)
            | (
                Some(Retained {
                    mode: Mode::Full, ..
                }),
                false,
            ) => Counted::All,
            (Some(_), false) if before.is_some() => {
                return Err(failure(format!(
                    "artifact {id} of source {source:?} has a partial retained count while its entry exists"
                )));
            }
            (Some(_), false) => Counted::Watermark,
            (None, false) if before.is_some() => {
                if !progress.artifact_counted(&id) {
                    // The migration has not reached this artifact; it reads the state then.
                    continue;
                }
                Counted::All
            }
            (None, false) => {
                if matches!(progress, Progress::Artifacts(_)) {
                    continue;
                }
                Counted::Watermark
            }
        };
        let full = created || row.is_some_and(|row| row.mode == Mode::Full) || before.is_some();
        let (complete_before, complete_after) = if full {
            (complete_before, complete_after)
        } else {
            (false, false)
        };
        let count_before = row.map_or(0, |row| row.count);
        if complete_before && row.is_none() {
            return Err(failure(format!(
                "complete artifact {id} of source {source:?} has no retained record count"
            )));
        }
        let change_records: i64 = change
            .records
            .iter()
            .filter(|(record, _)| counted == Counted::All || progress.record_counted(record))
            .map(|(_, change)| *change)
            .sum();
        let count_after = count_before.checked_add_signed(change_records).ok_or_else(|| {
            failure(format!(
                "retained records of artifact {id} of source {source:?} would go below 0 (stored {count_before}, change {change_records})"
            ))
        })?;
        let listed_before = complete_before || count_before > 0;
        let listed_after = complete_after || count_after > 0;
        let mode = row.map_or(Mode::Full, |row| row.mode);
        if listed_after {
            batch.put(
                retained_key(&id),
                Retained {
                    count: count_after,
                    mode,
                }
                .encode(),
            );
        } else if row.is_some() {
            batch.delete(retained_key(&id));
        }
        match (listed_before, listed_after) {
            (false, true) => batch.put(index_key(&source, received_at, &id), []),
            (true, false) => batch.delete(index_key(&source, received_at, &id)),
            _ => {}
        }
        let fragment = |complete: bool, count: u64| i64::from(!complete && count > 0);
        let delta = deltas.entry(source).or_default();
        delta.counted_before |= listed_before;
        delta.retained_records += change_records;
        delta.complete_artifacts += i64::from(complete_after) - i64::from(complete_before);
        delta.fragment_parents +=
            fragment(complete_after, count_after) - fragment(complete_before, count_before);
    }
    merge(db, batch, deltas)
}

/// Stage one migration page: counted rows and the advanced checkpoint in one batch, and
/// whether the migration is complete after it. `None` when it was already complete.
pub(super) fn page(db: &DB) -> Result<Option<(WriteBatch, bool)>, Error> {
    let reader = Reader::db(db);
    let mut batch = WriteBatch::default();
    let mut deltas = BTreeMap::<String, Delta>::new();
    let next = match progress(|key| reader.get(key.as_bytes()))? {
        Progress::Complete => return Ok(None),
        Progress::Artifacts(after) => artifact_page(
            &reader,
            after,
            (PAGE_ARTIFACTS, PAGE_RECORDS),
            (&mut batch, &mut deltas),
            &mut || Ok(()),
        )?,
        Progress::Records(after) => record_page(
            &reader,
            after,
            PAGE_RECORDS,
            (&mut batch, &mut deltas),
            &mut || Ok(()),
        )?,
    };
    merge(db, &mut batch, deltas)?;
    batch.put(CHECKPOINT, next.encode());
    Ok(Some((batch, next == Progress::Complete)))
}

/// Where a counting pass puts the rows it counts: a migration page's batch, or the
/// in-memory completion a reader takes while the migration is unfinished.
trait Rows {
    /// One artifact's retained record count.
    fn retained(&mut self, id: &str, row: Retained);
    /// One per-source artifact index row.
    fn index(&mut self, key: String);
}

impl Rows for WriteBatch {
    fn retained(&mut self, id: &str, row: Retained) {
        self.put(retained_key(id), row.encode());
    }
    fn index(&mut self, key: String) {
        self.put(key, []);
    }
}

/// The catalog entries a [`Completion`] over `reader` reads, in the migration's own unit
/// (artifact and record entries it has not passed), or `None` while the store counters
/// seed and no total is known. Zero once the migration is complete. A fixed number of
/// point reads.
pub(super) fn owed_entries(reader: &Reader<'_>, progress: &Progress) -> Result<Option<u64>, Error> {
    use super::store_counters::{Counter, Positions, values};
    if *progress == Progress::Complete {
        return Ok(Some(0));
    }
    let positions = Positions::read(|key| reader.get(key.as_bytes()))?;
    if !positions.seeded() {
        return Ok(None);
    }
    let rows = values(reader)?;
    let total = rows[&Counter::Artifacts]
        .checked_add(rows[&Counter::Records])
        .ok_or_else(|| failure("source-summary population overflow"))?;
    Ok(Some(
        total.saturating_sub(rows[&Counter::SourceSummaryDone]),
    ))
}

/// Which owed index rows a [`Completion`] keeps.
pub(super) type Keep = Box<dyn Fn(&str) -> bool>;

/// What the unfinished migration still owes, as one reader sees the store: the count
/// changes per source and the per-source index rows its remaining pages would write.
/// Stored rows plus this completion equal the rows the finished migration leaves for the
/// same catalog, because it runs the migration's own counting passes to the end.
pub(super) struct Completion {
    deltas: BTreeMap<String, Delta>,
    keep: Keep,
    /// The first owed index rows `keep` selected, at most `rows` of them, in key order.
    pub(super) index: std::collections::BTreeSet<String>,
    rows: usize,
    /// Whether `keep` selected more than `rows` rows, so rows after the last kept one exist.
    pub(super) truncated: bool,
}

impl Rows for Completion {
    fn retained(&mut self, _: &str, _: Retained) {}
    fn index(&mut self, key: String) {
        if (self.keep)(&key) {
            self.index.insert(key);
            if self.index.len() > self.rows {
                self.index.pop_last();
                self.truncated = true;
            }
        }
    }
}

impl Completion {
    /// Complete the migration at `progress` over `reader` without writing, keeping the first
    /// `rows` owed index rows `keep` selects; `None` once the migration is complete. The pass reads
    /// the catalog entries the migration has not reached, one entry each, as its pages
    /// would; `entry` runs before each artifact and record entry it examines, so a caller
    /// can count the pass's progress in the migration's own units and stop it.
    pub(super) fn owed(
        reader: &Reader<'_>,
        progress: &Progress,
        index: (usize, Keep),
        entry: &mut dyn FnMut() -> Result<(), Error>,
    ) -> Result<Option<Self>, Error> {
        if *progress == Progress::Complete {
            return Ok(None);
        }
        let (rows, keep) = index;
        let mut completion = Self {
            deltas: BTreeMap::new(),
            keep,
            index: std::collections::BTreeSet::new(),
            rows,
            truncated: false,
        };
        let mut deltas = BTreeMap::new();
        let mut next = progress.clone();
        loop {
            next = match next {
                Progress::Complete => break,
                Progress::Artifacts(after) => artifact_page(
                    reader,
                    after,
                    (PAGE_ARTIFACTS, PAGE_RECORDS),
                    (&mut completion, &mut deltas),
                    entry,
                )?,
                // One pass: a deleted parent's records are counted together, as the
                // pages count them across their boundaries.
                Progress::Records(after) => record_page(
                    reader,
                    after,
                    usize::MAX,
                    (&mut completion, &mut deltas),
                    entry,
                )?,
            };
        }
        completion.deltas = deltas;
        Ok(Some(completion))
    }

    /// One source's counts after the migration: `stored` (absent when the source has no
    /// row) with the owed changes applied; `None` when every count is 0.
    pub(super) fn counts(
        &self,
        source: &str,
        stored: Option<Counts>,
    ) -> Result<Option<Counts>, Error> {
        let mut next = stored.unwrap_or_else(|| Counts {
            source: source.to_owned(),
            ..Counts::default()
        });
        if let Some(delta) = self.deltas.get(source) {
            next = shifted(next, delta)?;
        }
        Ok((!next.is_empty()).then_some(next))
    }

    /// Bytes the owed count changes hold while a reader applies them.
    pub(super) fn bytes(&self) -> usize {
        self.deltas
            .keys()
            .map(|source| source.capacity() + 64 + std::mem::size_of::<Delta>())
            .sum()
    }

    /// Sources with owed changes, in order.
    pub(super) fn sources(&self) -> impl Iterator<Item = &str> {
        self.deltas.keys().map(String::as_str)
    }
}

fn artifact_page(
    reader: &Reader<'_>,
    after: Option<String>,
    limits: (usize, usize),
    (rows, deltas): (&mut impl Rows, &mut BTreeMap<String, Delta>),
    entry: &mut dyn FnMut() -> Result<(), Error>,
) -> Result<Progress, Error> {
    let (mut artifacts, mut records) = (0, 0);
    let mut last = None;
    let mut full = false;
    reader.scan(
        ARTIFACT_PREFIX,
        after.as_deref().map(str::as_bytes),
        |key, bytes| {
            if artifacts == limits.0 || records >= limits.1 {
                full = true;
                return Ok(false);
            }
            entry()?;
            let id = std::str::from_utf8(key)
                .map_err(|_| failure("invalid artifact catalog key"))?
                .to_owned();
            artifacts += 1;
            records += count_artifact(reader, &id, bytes, rows, deltas)?;
            last = Some(id);
            Ok(true)
        },
    )?;
    Ok(if full {
        Progress::Artifacts(last.or(after))
    } else {
        Progress::Records(None)
    })
}

/// Count one artifact with its records unless a write already counted it; returns the
/// record entries read.
fn count_artifact(
    reader: &Reader<'_>,
    id: &str,
    bytes: &[u8],
    rows: &mut impl Rows,
    deltas: &mut BTreeMap<String, Delta>,
) -> Result<usize, Error> {
    match Retained::read(reader, id)? {
        Some(Retained {
            mode: Mode::Full, ..
        }) => return Ok(0),
        Some(_) => {
            return Err(failure(format!(
                "artifact {id} has a partial retained count while its entry exists"
            )));
        }
        None => {}
    }
    let mut remaining = usize::MAX;
    let header = manifest_retire::header(bytes, &mut remaining, 0, 0)?;
    if header.artifact.id != id {
        return Err(failure(format!(
            "artifact catalog entry {id} names another identity"
        )));
    }
    let retired = reader
        .get(format!("artifact-retirement:{id}").as_bytes())?
        .map(|bytes| manifest_retire::evicted_flag(&bytes))
        .transpose()?
        .unwrap_or(false);
    let complete = !header.artifact.evicted && !retired;
    let source = header.artifact.source.clone();
    let received_at = header.artifact.received_at;
    let ids = header.record_ids(bytes, &mut remaining, 0, usize::MAX)?;
    let mut retained = 0_u64;
    for record in &ids {
        let Some(bytes) = reader.get(record.as_bytes())? else {
            continue;
        };
        let state = record_state(record, &bytes)?;
        if state.artifact != id {
            return Err(failure(format!(
                "record {record} is listed by artifact {id} but names parent {}",
                state.artifact
            )));
        }
        let mut known = Some((source.clone(), received_at));
        agree(id, &mut known, &state.source, state.received_at)?;
        retained += u64::from(state.retained);
    }
    if complete || retained > 0 {
        rows.retained(
            id,
            Retained {
                count: retained,
                mode: Mode::Full,
            },
        );
        rows.index(index_key(&source, received_at, id));
        let delta = deltas.entry(source).or_default();
        delta.retained_records += i64::try_from(retained)
            .map_err(|_| failure(format!("retained records of artifact {id} overflow")))?;
        delta.complete_artifacts += i64::from(complete);
        delta.fragment_parents += i64::from(!complete && retained > 0);
    }
    Ok(ids.len())
}

fn record_page(
    reader: &Reader<'_>,
    after: Option<String>,
    limit: usize,
    (rows, deltas): (&mut impl Rows, &mut BTreeMap<String, Delta>),
    entry: &mut dyn FnMut() -> Result<(), Error>,
) -> Result<Progress, Error> {
    // Deleted parents counted by this page, with their row before it.
    let mut parents = BTreeMap::<String, (Option<Retained>, u64, String, u64)>::new();
    let mut last = None;
    let mut records = 0;
    let mut exhausted = true;
    reader.scan(
        RECORD_PREFIX,
        after.as_deref().map(str::as_bytes),
        |key, bytes| {
            if records == limit {
                exhausted = false;
                return Ok(false);
            }
            entry()?;
            records += 1;
            let id = std::str::from_utf8(key)
                .map_err(|_| failure("invalid record catalog key"))?
                .to_owned();
            let state = record_state(&id, bytes)?;
            last = Some(id);
            if !state.retained || reader.get(state.artifact.as_bytes())?.is_some() {
                return Ok(true);
            }
            let parent = match parents.entry(state.artifact.clone()) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let row = Retained::read(reader, &state.artifact)?;
                    entry.insert((row, 0, state.source.clone(), state.received_at))
                }
            };
            if parent.0.is_some_and(|row| row.mode == Mode::Full) {
                return Ok(true);
            }
            let mut known = Some((parent.2.clone(), parent.3));
            agree(
                &state.artifact,
                &mut known,
                &state.source,
                state.received_at,
            )?;
            parent.1 += 1;
            Ok(true)
        },
    )?;
    for (id, (row, added, source, received_at)) in parents {
        if added == 0 {
            continue;
        }
        let before = row.map_or(0, |row| row.count);
        rows.retained(
            &id,
            Retained {
                count: before + added,
                mode: Mode::Partial,
            },
        );
        if before == 0 {
            rows.index(index_key(&source, received_at, &id));
        }
        let delta = deltas.entry(source).or_default();
        delta.counted_before |= before > 0;
        delta.retained_records += i64::try_from(added)
            .map_err(|_| failure(format!("retained records of artifact {id} overflow")))?;
        delta.fragment_parents += i64::from(before == 0);
    }
    Ok(if exhausted {
        Progress::Complete
    } else {
        Progress::Records(last.or(after))
    })
}

/// Remove every per-source counter row, retained count, index row and the migration
/// checkpoint, and lower the writer fence to its owner graph family, as a store written
/// before #14 is. Offline test fixtures only.
#[cfg(feature = "test-support")]
pub fn remove_source_summary_for_legacy_fixture(db: &DB) -> Result<(), Error> {
    let mut batch = WriteBatch::default();
    for prefix in [SUMMARY, RETAINED, INDEX] {
        for row in db.prefix_iterator(prefix.as_bytes()) {
            let (key, _) = row?;
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            batch.delete(key);
        }
    }
    batch.delete(CHECKPOINT);
    let format = db
        .get("store_format")?
        .ok_or_else(|| failure("writer format missing"))?;
    batch.put(
        "store_format",
        super::writer_format::lower_source_summary(&format),
    );
    db.write(batch)?;
    Ok(())
}

/// Stored and recounted `[retained_records, complete_artifacts, fragment_parents]` per
/// source.
#[cfg(feature = "test-support")]
pub type SourceCountAudit = (BTreeMap<String, [u64; 3]>, BTreeMap<String, [u64; 3]>);

#[cfg(feature = "test-support")]
impl super::Store {
    /// Every stored per-source counter row and a recount of the same counts from the
    /// catalog entries, read under the writer lock. Acceptance tests only.
    pub fn source_count_audit_for_tests(&self) -> Result<SourceCountAudit, Error> {
        let writer = self.lock()?;
        Ok((stored_counts(&writer.db)?, recount(&writer.db)?))
    }
}

/// A recount of every source's counts from the catalog entries.
#[cfg(feature = "test-support")]
fn recount(db: &DB) -> Result<BTreeMap<String, [u64; 3]>, Error> {
    let mut retained = BTreeMap::<String, (String, u64)>::new();
    for row in db.prefix_iterator(RECORD_PREFIX) {
        let (key, bytes) = row?;
        if !key.starts_with(RECORD_PREFIX) {
            break;
        }
        let id = std::str::from_utf8(&key).map_err(|_| failure("invalid record key"))?;
        let state = record_state(id, &bytes)?;
        let entry = retained.entry(state.artifact).or_insert((state.source, 0));
        entry.1 += u64::from(state.retained);
    }
    let mut counts = BTreeMap::<String, [u64; 3]>::new();
    for row in db.prefix_iterator(ARTIFACT_PREFIX) {
        let (key, bytes) = row?;
        if !key.starts_with(ARTIFACT_PREFIX) {
            break;
        }
        let id = std::str::from_utf8(&key).map_err(|_| failure("invalid artifact key"))?;
        let state = entry_state(id, &bytes)?;
        let retired = db
            .get_pinned(format!("artifact-retirement:{id}"))?
            .map(|bytes| manifest_retire::evicted_flag(&bytes))
            .transpose()?
            .unwrap_or(false);
        let complete = !state.evicted && !retired;
        let records = retained.remove(id).map_or(0, |(_, count)| count);
        let row = counts.entry(state.source).or_default();
        row[0] += records;
        row[1] += u64::from(complete);
        row[2] += u64::from(!complete && records > 0);
    }
    for (_, (source, records)) in retained {
        let row = counts.entry(source).or_default();
        row[0] += records;
        row[2] += u64::from(records > 0);
    }
    counts.retain(|_, row| *row != [0; 3]);
    Ok(counts)
}

/// Every stored summary row, by source.
#[cfg(feature = "test-support")]
fn stored_counts(db: &DB) -> Result<BTreeMap<String, [u64; 3]>, Error> {
    let mut rows = BTreeMap::new();
    for row in db.prefix_iterator(SUMMARY.as_bytes()) {
        let (key, bytes) = row?;
        if !key.starts_with(SUMMARY.as_bytes()) {
            break;
        }
        let counts = Counts::decode(&key, &bytes)?;
        rows.insert(
            counts.source,
            [
                counts.retained_records,
                counts.complete_artifacts,
                counts.fragment_parents,
            ],
        );
    }
    Ok(rows)
}
