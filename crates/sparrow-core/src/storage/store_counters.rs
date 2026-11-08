// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Exact store-wide counters that `status` and the migration report read without the writer
//! lock (#146).
//!
//! Each counter row `store-counter:<name>` holds a big-endian `u64`. A counter is a sum of
//! terms; a term counts the keys under one prefix, optionally only record entries that are
//! not evicted, at or before a bound:
//!
//! - population counters count every key of their prefix: `records`, `retained-records`,
//!   `artifacts`, `graph-declarations` (`rdf:g:`), `record-graphs`
//!   (`provenance-rdf:record:`), `payload-chunks` (`ref:`) and `json-projections`
//!   (`projection-ref:`);
//! - one `done:<migration>` counter per data migration counts the keys of its source
//!   prefixes at or before the position its committed checkpoint names.
//!
//! [`apply`] is the one owner: [`super::Writer::apply`] runs it on every batch, so every
//! change to a counted key and every checkpoint move changes the rows in the same atomic
//! batch. A key the batch creates, deletes or changes between retained and evicted adjusts
//! each counter whose bound covers it after the batch; a checkpoint the batch moves adds
//! the keys between its old and new position, read from the database before the batch.
//! The two together are exact whichever of them a batch contains. A count that would go
//! below 0, or a missing row, refuses the write with `STORAGE_FAILURE`; nothing is clamped.
//!
//! A store written before the counters is fenced at open with zero rows and seeded by the
//! first online migration, `store-counters`. Its pages only advance its checkpoint across
//! the keys of one prefix at a time; [`apply`] counts the keys it passes. Every term's
//! bound is also capped at that seeding position, so a row always counts exactly the keys
//! its bound covers. Until seeding completes, readers report the rows as pending: exact
//! through the seeding watermark, never a total. Nothing recounts at run time.

use super::{Error, event_fallback_rdf, rdf_index, read_field, source_summary};
use rocksdb::{DB, Direction, IteratorMode, WriteBatch};
use source_summary::failure;
use std::cmp::Ordering;
use std::collections::BTreeMap;

const ROW_PREFIX: &str = "store-counter:";
/// Seeding checkpoint: `[0]` complete; `[1, prefix index]` then the last counted key of
/// that prefix, if any.
pub(super) const CHECKPOINT: &str = "store-counters-upgrade";
/// Keys one seeding page passes. A page reads keys and, for records, their eviction flag.
const SEED_PAGE: usize = 16384;
/// Totals #142 recorded for completed migrations; the counters replace them.
const LEGACY_TOTAL_PREFIX: &str = "migration-total:";

const RECORD: &[u8] = b"record:";
const ARTIFACT: &[u8] = b"artifact:";
const GRAPH: &[u8] = b"rdf:g:";
const RECORD_GRAPH: &[u8] = b"provenance-rdf:record:";
const PAYLOAD: &[u8] = b"ref:";
const PROJECTION: &[u8] = b"projection-ref:";
/// Counted prefixes, in seeding order.
const SEEDED: [&[u8]; 6] = [RECORD, ARTIFACT, GRAPH, RECORD_GRAPH, PAYLOAD, PROJECTION];

/// Which keys of a prefix a term counts.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Filter {
    Any,
    /// Record entries whose `evicted` flag is false.
    Retained,
}

/// The position a term's bound follows, besides the seeding position.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Follows {
    /// Every key of the prefix.
    Population,
    OwnerGraphIndex,
    EventFallbackRdf,
    SourceSummaryArtifacts,
    SourceSummaryRecords,
}

struct Term {
    prefix: &'static [u8],
    filter: Filter,
    follows: Follows,
}

/// One counter row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Counter {
    Records,
    RetainedRecords,
    Artifacts,
    GraphDeclarations,
    RecordGraphs,
    PayloadChunks,
    JsonProjections,
    OwnerGraphIndexDone,
    EventFallbackRdfDone,
    SourceSummaryDone,
}

impl Counter {
    pub(super) const ALL: [Self; 10] = [
        Self::Records,
        Self::RetainedRecords,
        Self::Artifacts,
        Self::GraphDeclarations,
        Self::RecordGraphs,
        Self::PayloadChunks,
        Self::JsonProjections,
        Self::OwnerGraphIndexDone,
        Self::EventFallbackRdfDone,
        Self::SourceSummaryDone,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Records => "records",
            Self::RetainedRecords => "retained-records",
            Self::Artifacts => "artifacts",
            Self::GraphDeclarations => "graph-declarations",
            Self::RecordGraphs => "record-graphs",
            Self::PayloadChunks => "payload-chunks",
            Self::JsonProjections => "json-projections",
            Self::OwnerGraphIndexDone => "done:owner-graph-index",
            Self::EventFallbackRdfDone => "done:event-fallback-rdf",
            Self::SourceSummaryDone => "done:source-summary",
        }
    }

    fn key(self) -> String {
        format!("{ROW_PREFIX}{}", self.name())
    }

    fn terms(self) -> &'static [Term] {
        use Filter::{Any, Retained};
        use Follows::{
            EventFallbackRdf, OwnerGraphIndex, Population, SourceSummaryArtifacts,
            SourceSummaryRecords,
        };
        match self {
            Self::Records => &[Term {
                prefix: RECORD,
                filter: Any,
                follows: Population,
            }],
            Self::RetainedRecords => &[Term {
                prefix: RECORD,
                filter: Retained,
                follows: Population,
            }],
            Self::Artifacts => &[Term {
                prefix: ARTIFACT,
                filter: Any,
                follows: Population,
            }],
            Self::GraphDeclarations => &[Term {
                prefix: GRAPH,
                filter: Any,
                follows: Population,
            }],
            Self::RecordGraphs => &[Term {
                prefix: RECORD_GRAPH,
                filter: Any,
                follows: Population,
            }],
            Self::PayloadChunks => &[Term {
                prefix: PAYLOAD,
                filter: Any,
                follows: Population,
            }],
            Self::JsonProjections => &[Term {
                prefix: PROJECTION,
                filter: Any,
                follows: Population,
            }],
            Self::OwnerGraphIndexDone => &[Term {
                prefix: GRAPH,
                filter: Any,
                follows: OwnerGraphIndex,
            }],
            Self::EventFallbackRdfDone => &[Term {
                prefix: RECORD_GRAPH,
                filter: Any,
                follows: EventFallbackRdf,
            }],
            Self::SourceSummaryDone => &[
                Term {
                    prefix: ARTIFACT,
                    filter: Any,
                    follows: SourceSummaryArtifacts,
                },
                Term {
                    prefix: RECORD,
                    filter: Any,
                    follows: SourceSummaryRecords,
                },
            ],
        }
    }
}

/// An inclusive upper bound on the keys of one prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Bound {
    Nothing,
    Through(Vec<u8>),
    Everything,
}

impl Bound {
    fn covers(&self, key: &[u8]) -> bool {
        match self {
            Self::Nothing => false,
            Self::Through(last) => key <= last.as_slice(),
            Self::Everything => true,
        }
    }

    fn after(after: Option<Vec<u8>>) -> Self {
        after.map_or(Self::Nothing, Self::Through)
    }
}

impl PartialOrd for Bound {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Bound {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Through(a), Self::Through(b)) => a.cmp(b),
            _ => self.rank().cmp(&other.rank()),
        }
    }
}

impl Bound {
    fn rank(&self) -> u8 {
        match self {
            Self::Nothing => 0,
            Self::Through(_) => 1,
            Self::Everything => 2,
        }
    }
}

/// Committed seeding position.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Seed {
    Complete,
    /// Prefixes before the index are counted; that prefix is counted through the key.
    At(usize, Option<Vec<u8>>),
}

impl Seed {
    fn encode(&self) -> Vec<u8> {
        match self {
            Self::Complete => vec![0],
            Self::At(index, after) => {
                let mut bytes = vec![1, u8::try_from(*index).unwrap_or(u8::MAX)];
                bytes.extend_from_slice(after.as_deref().unwrap_or_default());
                bytes
            }
        }
    }

    fn decode(bytes: Option<&[u8]>) -> Result<Self, Error> {
        let bytes = bytes.ok_or_else(|| {
            failure("required store counter seeding state missing; restore the original catalog")
        })?;
        match bytes {
            [0] => Ok(Self::Complete),
            [1, index, after @ ..] if usize::from(*index) < SEEDED.len() => Ok(Self::At(
                usize::from(*index),
                (!after.is_empty()).then(|| after.to_vec()),
            )),
            _ => Err(failure("invalid store counter seeding checkpoint")),
        }
    }

    fn bound(&self, prefix: &[u8]) -> Bound {
        let Self::At(index, after) = self else {
            return Bound::Everything;
        };
        let position = SEEDED
            .iter()
            .position(|seeded| *seeded == prefix)
            .unwrap_or(SEEDED.len());
        match position.cmp(index) {
            Ordering::Less => Bound::Everything,
            Ordering::Equal => Bound::after(after.clone()),
            Ordering::Greater => Bound::Nothing,
        }
    }

    fn watermark(&self) -> Option<String> {
        match self {
            Self::Complete => None,
            Self::At(index, after) => {
                let prefix = String::from_utf8_lossy(SEEDED[*index]);
                Some(match after {
                    None if *index == 0 => "start".to_owned(),
                    None => format!("{prefix} start"),
                    Some(key) => hex_key(key),
                })
            }
        }
    }
}

fn hex_key(key: &[u8]) -> String {
    match std::str::from_utf8(key) {
        Ok(text) if text.chars().all(|c| !c.is_control()) => text.to_owned(),
        _ => super::hex::encode(key),
    }
}

/// Every checkpoint a term's bound follows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Positions {
    seed: Seed,
    owner: Bound,
    fallback: Bound,
    summary: (Bound, Bound),
}

/// The checkpoint keys [`Positions`] reads.
fn checkpoint_key(name: &[u8]) -> bool {
    name == CHECKPOINT.as_bytes()
        || name == rdf_index::OWNER_GRAPH_UPGRADE.as_bytes()
        || name == event_fallback_rdf::CHECKPOINT.as_bytes()
        || name == source_summary::CHECKPOINT.as_bytes()
}

impl Positions {
    pub(super) fn read(
        mut read: impl FnMut(&str) -> Result<Option<Vec<u8>>, Error>,
    ) -> Result<Self, Error> {
        let seed = Seed::decode(read(CHECKPOINT)?.as_deref())?;
        let owner = match rdf_index::owner_graph_progress(&mut read)? {
            None => Bound::Everything,
            Some(after) => {
                Bound::after(after.map(|(graph, owner)| rdf_index::graph_key(graph, owner)))
            }
        };
        let fallback = match event_fallback_rdf::decode_frontier(
            read(event_fallback_rdf::CHECKPOINT)?.as_deref(),
        )? {
            None => Bound::Everything,
            Some(after) => {
                Bound::after(after.map(|owner| format!("provenance-rdf:{owner}").into_bytes()))
            }
        };
        let summary = match source_summary::progress(&mut read)? {
            source_summary::Progress::Complete => (Bound::Everything, Bound::Everything),
            source_summary::Progress::Artifacts(after) => {
                (Bound::after(after.map(String::into_bytes)), Bound::Nothing)
            }
            source_summary::Progress::Records(after) => (
                Bound::Everything,
                Bound::after(after.map(String::into_bytes)),
            ),
        };
        Ok(Self {
            seed,
            owner,
            fallback,
            summary,
        })
    }

    /// Whether seeding is complete, so the rows count the whole store rather than the keys
    /// through the seeding watermark.
    pub(super) fn seeded(&self) -> bool {
        self.seed == Seed::Complete
    }

    /// The seeding watermark, absent once complete.
    pub(super) fn seed_watermark(&self) -> Option<String> {
        self.seed.watermark()
    }

    /// The term's bound, capped at the seeding position, as the rows are.
    fn bound(&self, term: &Term) -> Bound {
        let follows = match term.follows {
            Follows::Population => Bound::Everything,
            Follows::OwnerGraphIndex => self.owner.clone(),
            Follows::EventFallbackRdf => self.fallback.clone(),
            Follows::SourceSummaryArtifacts => self.summary.0.clone(),
            Follows::SourceSummaryRecords => self.summary.1.clone(),
        };
        follows.min(self.seed.bound(term.prefix))
    }
}

/// A database or snapshot reader that counts the keys it reads.
pub(super) struct Reader<'a> {
    source: Source<'a>,
    keys: std::cell::Cell<u64>,
}

enum Source<'a> {
    Db(&'a DB),
    Snapshot(&'a rocksdb::Snapshot<'a>),
}

impl<'a> Reader<'a> {
    pub(super) fn db(db: &'a DB) -> Self {
        Self {
            source: Source::Db(db),
            keys: std::cell::Cell::new(0),
        }
    }

    pub(super) fn snapshot(snapshot: &'a rocksdb::Snapshot<'a>) -> Self {
        Self {
            source: Source::Snapshot(snapshot),
            keys: std::cell::Cell::new(0),
        }
    }

    /// Keys read so far: one per point read, one per iterator row returned.
    pub(super) fn keys(&self) -> u64 {
        self.keys.get()
    }

    pub(super) fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.keys.set(self.keys.get() + 1);
        Ok(match self.source {
            Source::Db(db) => db.get(key)?,
            Source::Snapshot(snapshot) => snapshot.get(key)?,
        })
    }

    /// Visit the keys under `prefix` after `after` (exclusive) in order, until `visit`
    /// returns `false`.
    pub(super) fn scan(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        mut visit: impl FnMut(&[u8], &[u8]) -> Result<bool, Error>,
    ) -> Result<(), Error> {
        let start = after.unwrap_or(prefix);
        let mode = IteratorMode::From(start, Direction::Forward);
        let iterator = match self.source {
            Source::Db(db) => db.iterator(mode),
            Source::Snapshot(snapshot) => snapshot.iterator(mode),
        };
        for row in iterator {
            let (key, value) = row?;
            self.keys.set(self.keys.get() + 1);
            if !key.starts_with(prefix) {
                break;
            }
            if after.is_some_and(|after| *key == *after) {
                continue;
            }
            if !visit(&key, &value)? {
                break;
            }
        }
        Ok(())
    }
}

fn retained(key: &[u8], value: &[u8]) -> Result<bool, Error> {
    let record = super::parse(value)?;
    if read_field::<String>(&record, "id")?.as_bytes() != key {
        return Err(failure(format!(
            "record catalog entry {} names another identity",
            String::from_utf8_lossy(key)
        )));
    }
    let retired = match record.get("retired") {
        None => false,
        Some(_) => read_field::<bool>(&record, "retired")?,
    };
    Ok(!read_field::<bool>(&record, "evicted")? && !retired)
}

fn included(term: &Term, key: &[u8], value: Option<&[u8]>) -> Result<bool, Error> {
    match (value, term.filter) {
        (None, _) => Ok(false),
        (Some(_), Filter::Any) => Ok(true),
        (Some(value), Filter::Retained) => retained(key, value),
    }
}

/// Keys of the term's prefix after `low` through `high`.
fn count(reader: &Reader<'_>, term: &Term, low: &Bound, high: &Bound) -> Result<u64, Error> {
    let after = match low {
        Bound::Everything => return Ok(0),
        Bound::Nothing => None,
        Bound::Through(key) => Some(key.as_slice()),
    };
    if high <= low {
        return Ok(0);
    }
    let mut total = 0_u64;
    reader.scan(term.prefix, after, |key, value| {
        if !high.covers(key) {
            return Ok(false);
        }
        total += u64::from(included(term, key, Some(value))?);
        Ok(true)
    })?;
    Ok(total)
}

/// The value a counter row holds: its keys through each term's bound, which the seeding
/// position caps as it caps the row. Audits only; no runtime path recounts (#146).
#[cfg(feature = "test-support")]
fn recount(reader: &Reader<'_>, positions: &Positions, counter: Counter) -> Result<u64, Error> {
    counter.terms().iter().try_fold(0_u64, |total, term| {
        let value = count(reader, term, &Bound::Nothing, &positions.bound(term))?;
        total
            .checked_add(value)
            .ok_or_else(|| failure("store counter recount overflow"))
    })
}

/// Read one counter row; a missing or malformed row refuses.
pub(super) fn stored(reader: &Reader<'_>, counter: Counter) -> Result<u64, Error> {
    let bytes = reader.get(counter.key().as_bytes())?.ok_or_else(|| {
        failure(format!(
            "store counter {} missing; restore the original catalog",
            counter.name()
        ))
    })?;
    let bytes: [u8; 8] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| failure(format!("invalid store counter {}", counter.name())))?;
    Ok(u64::from_be_bytes(bytes))
}

/// Every counter row. Each is exact through its bound: once seeding is complete that is
/// the whole store, and while it seeds the row counts the keys through the seeding
/// watermark (see [`Positions::seeded`]). A fixed number of point reads.
pub(super) fn values(reader: &Reader<'_>) -> Result<BTreeMap<Counter, u64>, Error> {
    Counter::ALL
        .into_iter()
        .map(|counter| Ok((counter, stored(reader, counter)?)))
        .collect()
}

/// Fence a store that has no counters: zero rows, the seeding checkpoint, and no #142
/// totals. With no counted key at all, seeding is complete as it starts.
pub(super) fn fence(db: &DB, batch: &mut WriteBatch) -> Result<(), Error> {
    for (prefix, end) in [
        (ROW_PREFIX, "store-counter;"),
        (LEGACY_TOTAL_PREFIX, "migration-total;"),
    ] {
        batch.delete_range(prefix.as_bytes(), end.as_bytes());
    }
    for counter in Counter::ALL {
        batch.put(counter.key(), 0_u64.to_be_bytes());
    }
    let mut pending = false;
    for prefix in SEEDED {
        pending |= super::any_key(db, prefix)?;
    }
    batch.put(
        CHECKPOINT,
        if pending {
            Seed::At(0, None)
        } else {
            Seed::Complete
        }
        .encode(),
    );
    Ok(())
}

/// Stage one seeding page: the advanced checkpoint alone, and whether seeding is complete
/// after it. [`apply`] counts the keys the checkpoint passes in the same batch. `None` when
/// seeding was already complete.
pub(super) fn page(db: &DB) -> Result<Option<(WriteBatch, bool)>, Error> {
    let Seed::At(index, after) = Seed::decode(db.get(CHECKPOINT)?.as_deref())? else {
        return Ok(None);
    };
    let reader = Reader::db(db);
    let mut passed = 0;
    let mut last = None;
    reader.scan(SEEDED[index], after.as_deref(), |key, _| {
        if passed == SEED_PAGE {
            return Ok(false);
        }
        passed += 1;
        last = Some(key.to_vec());
        Ok(true)
    })?;
    let next = if passed == SEED_PAGE {
        Seed::At(index, last)
    } else if index + 1 < SEEDED.len() {
        Seed::At(index + 1, None)
    } else {
        Seed::Complete
    };
    let mut batch = WriteBatch::default();
    batch.put(CHECKPOINT, next.encode());
    Ok(Some((batch, next == Seed::Complete)))
}

/// Final value of each counted key and checkpoint in one batch.
#[derive(Default)]
struct Changes {
    keys: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    checkpoints: BTreeMap<String, Option<Vec<u8>>>,
    derived: bool,
}

impl super::durability::BatchObserver for Changes {
    fn observe(&mut self, name: &[u8], value: Option<&[u8]>) -> Result<(), Error> {
        if SEEDED.iter().any(|prefix| name.starts_with(prefix)) {
            self.keys.insert(name.to_vec(), value.map(<[u8]>::to_vec));
        } else if checkpoint_key(name) {
            let name = std::str::from_utf8(name).map_err(|_| failure("invalid checkpoint key"))?;
            self.checkpoints
                .insert(name.to_owned(), value.map(<[u8]>::to_vec));
        } else if name.starts_with(ROW_PREFIX.as_bytes()) {
            self.derived = true;
        }
        Ok(())
    }

    fn refuse_merge(&self, name: &[u8]) -> Option<Error> {
        (SEEDED.iter().any(|prefix| name.starts_with(prefix))
            || checkpoint_key(name)
            || name.starts_with(ROW_PREFIX.as_bytes()))
        .then(|| failure("store counters require exact writes, not merges"))
    }
}

/// Maintain every counter row for the counted keys and checkpoints `batch` changes,
/// appending the row writes to the same batch. Runs on every write through
/// [`super::Writer::apply`].
pub(super) fn apply(db: &DB, batch: &mut WriteBatch) -> Result<(), Error> {
    let mut changes = Changes::default();
    super::durability::observe_batch(batch, &mut changes)?;
    if changes.derived {
        return Err(failure(
            "store counter rows are written only by their owner, with the change they follow",
        ));
    }
    if changes.keys.is_empty() && changes.checkpoints.is_empty() {
        return Ok(());
    }
    let reader = Reader::db(db);
    let before = Positions::read(|key| reader.get(key.as_bytes()))?;
    let after = Positions::read(|key| match changes.checkpoints.get(key) {
        Some(value) => Ok(value.clone()),
        None => reader.get(key.as_bytes()),
    })?;
    // Each changed key's inclusion before the batch, per filter, read once.
    let mut prior = BTreeMap::<&[u8], Option<Vec<u8>>>::new();
    for key in changes.keys.keys() {
        prior.insert(key, reader.get(key)?);
    }
    for counter in Counter::ALL {
        let mut change = 0_i64;
        for term in counter.terms() {
            let (old, new) = (before.bound(term), after.bound(term));
            let moved = match old.cmp(&new) {
                Ordering::Less => i64::try_from(count(&reader, term, &old, &new)?),
                Ordering::Greater => {
                    i64::try_from(count(&reader, term, &new, &old)?).map(|value| -value)
                }
                Ordering::Equal => Ok(0),
            }
            .map_err(|_| failure("store counter change overflow"))?;
            change += moved;
            for (key, value) in changes.keys.range(term.prefix.to_vec()..) {
                if !key.starts_with(term.prefix) {
                    break;
                }
                if !new.covers(key) {
                    continue;
                }
                let was = prior.get(key.as_slice()).and_then(Option::as_deref);
                change += i64::from(included(term, key, value.as_deref())?)
                    - i64::from(included(term, key, was)?);
            }
        }
        if change == 0 {
            continue;
        }
        let value = stored(&reader, counter)?;
        let next = value.checked_add_signed(change).ok_or_else(|| {
            failure(format!(
                "store counter {} would go below 0 (stored {value}, change {change}); the counters are inconsistent with the catalog",
                counter.name()
            ))
        })?;
        batch.put(counter.key(), next.to_be_bytes());
    }
    Ok(())
}

/// Remove every counter row and the seeding checkpoint and lower the writer fence to its
/// source summary family, as a store written before #146 is. Offline test fixtures only.
#[cfg(feature = "test-support")]
pub fn remove_store_counters_for_legacy_fixture(db: &DB) -> Result<(), Error> {
    let mut batch = WriteBatch::default();
    batch.delete_range(ROW_PREFIX.as_bytes(), b"store-counter;");
    batch.delete(CHECKPOINT);
    let format = db
        .get("store_format")?
        .ok_or_else(|| failure("writer format missing"))?;
    batch.put(
        "store_format",
        super::writer_format::lower_store_counters(&format),
    );
    db.write(batch)?;
    Ok(())
}

/// Stored and recounted counter rows, by name.
#[cfg(feature = "test-support")]
pub type StoreCounterAudit = (BTreeMap<String, u64>, BTreeMap<String, u64>);

#[cfg(feature = "test-support")]
impl super::Store {
    /// Every counter row and a recount of the same counters from the keys they count, read
    /// from one snapshot. Acceptance tests only.
    pub fn store_counter_audit_for_tests(&self) -> Result<StoreCounterAudit, Error> {
        let snapshot = self.unlocked.db.snapshot();
        let reader = Reader::snapshot(&snapshot);
        let positions = Positions::read(|key| reader.get(key.as_bytes()))?;
        let mut rows = BTreeMap::new();
        let mut recounted = BTreeMap::new();
        for counter in Counter::ALL {
            rows.insert(counter.name().to_owned(), stored(&reader, counter)?);
            recounted.insert(
                counter.name().to_owned(),
                recount(&reader, &positions, counter)?,
            );
        }
        Ok((rows, recounted))
    }
}
