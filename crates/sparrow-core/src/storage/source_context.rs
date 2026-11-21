// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Generation-marked source timeline cache over the authoritative record/time catalog.

use super::store_counters::Reader;
use super::{Error, Record, Writer, catalog_upgrade, integer, parse, read_field, store_counters};
use rocksdb::{DB, WriteBatch};
use std::collections::BTreeMap;

const PREFIX_STR: &str = "source-context-v1:";
const PREFIX: &[u8] = PREFIX_STR.as_bytes();
/// The first key after the family: `;` follows the prefix's final `:`.
const FAMILY_END: &[u8] = b"source-context-v1;";
/// Cache generation marker. Bump [`GENERATION`] when the cache's meaning changes; any other
/// value, or none, resets the cache at open.
const GENERATION_KEY: &str = "source-context-v1:!generation";
const GENERATION: &[u8] = b"1";
// INVARIANT: the marker sorts before every cache entry (`!` precedes the hex source
// hashes), so a preceding binary's key-order clear deletes it in its first batch.
const _: () = assert!(GENERATION_KEY.as_bytes()[PREFIX_STR.len()] < b'0');
const FAILURE: &str = "source-context-cache-failure";
/// The rebuild migration's checkpoint.
pub(super) const CHECKPOINT: &str = JOB.checkpoint;
const SCRATCH_BYTES: usize = 32 * 1024 * 1024;
const METADATA_BYTES: usize = 32 * 1024 * 1024;
/// Distinct records one atomic write may change source context for (32,768): the bound of the
/// metadata admission. A sealed import of more records is refused at layout, before preparation.
pub(super) const MAX_CONTEXT_RECORDS: usize = METADATA_BYTES / 1024;
const JOB: catalog_upgrade::Job = catalog_upgrade::Job {
    checkpoint: "source-context-cache-upgrade",
    source: "record:",
    target: PREFIX,
    page_entries: 512,
    page_bytes: SCRATCH_BYTES,
    charge: reconstruction_charge,
    publish,
};

fn prefix(source: &str) -> String {
    hashed_prefix(&super::source_summary::source_hash(source))
}

/// The record receipt index prefix of the source whose key hash is `hash`.
pub(super) fn hashed_prefix(hash: &str) -> String {
    format!("{PREFIX_STR}{hash}:")
}

pub(super) fn key(record: &Record) -> String {
    key_parts(&record.source, record.received_at, &record.id)
}

fn key_parts(source: &str, received_at: u64, id: &str) -> String {
    format!("{}{:020}:{id}", prefix(source), received_at)
}

fn charge(bytes: &[u8]) -> Result<usize, Error> {
    let mut remaining = SCRATCH_BYTES;
    Writer::charge_catalog_shape(&mut remaining, bytes)?;
    Ok(SCRATCH_BYTES - remaining)
}

struct Header {
    id: String,
    source: String,
    received_at: u64,
}

impl Header {
    fn key(&self) -> String {
        key_parts(&self.source, self.received_at, &self.id)
    }
}

fn decode(bytes: &[u8]) -> Result<Header, Error> {
    charge(bytes)?;
    let value = parse(bytes)?;
    Ok(Header {
        id: read_field(&value, "id")?,
        source: read_field(&value, "source")?,
        received_at: integer(&value, "received_at")?,
    })
}

fn publish(
    db: &DB,
    id: &str,
    bytes: &[u8],
    _charge: usize,
    batch: &mut WriteBatch,
) -> Result<(), Error> {
    let record = decode(bytes).map_err(catalog_failure)?;
    if record.id != id || record.source.is_empty() || record.source.len() > 512 {
        return Err(Error::new(
            "CONTEXT_CATALOG_INVALID",
            "source context catalog identity or source is invalid",
        ));
    }
    let primary = format!("time:{:020}:{}", record.received_at, record.id);
    if db.get_pinned(primary)?.is_some() {
        // Preserve the selected primary read's failure boundary: constructing
        // this derivative does not certify a possibly corrupt primary value.
        // `indexed` checks that value before publishing any context response.
        batch.put(record.key(), record.id.as_bytes());
    }
    Ok(())
}

/// Fence the cache at open: keep it when its generation marker is current, it carries no
/// latched reconstruction failure and its checkpoint decodes; otherwise stage one reset into
/// `batch`.
///
/// [`update`] maintains the cache in the same atomic batch as every catalog write, so a
/// crash cannot leave it stale. Every binary that wrote this store before the marker
/// existed rebuilt the cache on each open by point-deleting its prefix in key order; its
/// first deletion batch removes the marker, which sorts first. A cache such a binary
/// touched, or left half cleared, therefore has no marker and is reset here.
///
/// The reset is one range deletion of the whole family, never a point tombstone per key,
/// and it seeds the `source-context` online migration, which the maintenance worker pages
/// through the writer. Until it completes, [`Timeline::walk`] serves every reader from the primary
/// time index.
pub(super) fn fence(db: &DB, batch: &mut WriteBatch) -> Result<(), Error> {
    let checkpoint = db.get(CHECKPOINT)?;
    let intact = db.get_pinned(GENERATION_KEY)?.as_deref() == Some(GENERATION)
        && db.get_pinned(FAILURE)?.is_none()
        && catalog_upgrade::read_progress(CHECKPOINT, |_| Ok(checkpoint)).is_ok();
    if intact {
        return Ok(());
    }
    batch.delete_range(PREFIX, FAMILY_END);
    batch.delete(FAILURE);
    batch.put(GENERATION_KEY, GENERATION);
    let progress = catalog_upgrade::Progress {
        after: None,
        complete: !super::any_key(db, JOB.source.as_bytes())?,
    };
    super::put(batch, CHECKPOINT, &progress);
    store_counters::reset(batch, store_counters::Counter::SourceContextDone);
    Ok(())
}

/// Stage one rebuild page with its checkpoint, and whether the rebuild is complete after
/// it; `None` when it was already complete. A page that finds corrupt record metadata latches
/// the failure durably instead, and readers keep reading the primary time index.
///
/// A latched rebuild re-verifies: the next page runs again from the same checkpoint. When the
/// corrupt metadata is gone (its record retired or rewritten), the page succeeds and clears
/// the latch in its own batch, and the rebuild continues. While the metadata is still corrupt
/// it commits nothing and fails, so the worker retries it on its migration retry interval
/// rather than paging back to back.
pub(super) fn page(db: &DB) -> Result<Option<(WriteBatch, bool)>, Error> {
    let latched = db.get_pinned(FAILURE)?.is_some();
    let progress = catalog_upgrade::progress(db, &JOB)?;
    if progress.complete {
        return Ok(None);
    }
    match catalog_upgrade::page(db, &JOB, progress, JOB.page_entries) {
        Err(error) if error.code == "CONTEXT_CATALOG_INVALID" && latched => Err(error),
        Err(error) if error.code == "CONTEXT_CATALOG_INVALID" => {
            let mut batch = WriteBatch::default();
            batch.put(FAILURE, error.message.as_bytes());
            Ok(Some((batch, false)))
        }
        Ok((mut batch, complete)) => {
            if latched {
                batch.delete(FAILURE);
            }
            Ok(Some((batch, complete)))
        }
        Err(error) => Err(error),
    }
}

/// Whether the rebuild still has pages to run, a latched one included: it re-verifies (see
/// [`page`]). The rebuild runs last of the migrations, so a latched one delays no other.
pub(super) fn pending(db: &DB) -> Result<bool, Error> {
    Ok(!catalog_upgrade::progress(db, &JOB)?.complete)
}

/// The rebuild's status watermark, absent once complete, from any reader.
pub(super) fn watermark(
    read: impl FnOnce(&str) -> Result<Option<Vec<u8>>, Error>,
) -> Result<Option<String>, Error> {
    let progress = catalog_upgrade::read_progress(CHECKPOINT, read)?;
    Ok((!progress.complete).then(|| progress.after.unwrap_or_else(|| "start".to_owned())))
}

/// The durably latched reconstruction failure, if any, from any reader.
pub(super) fn latched_failure(
    read: impl FnOnce(&str) -> Result<Option<Vec<u8>>, Error>,
) -> Result<Option<String>, Error> {
    Ok(read(FAILURE)?.map(|bytes| String::from_utf8_lossy(&bytes).into_owned()))
}

fn reconstruction_charge(bytes: &[u8]) -> Result<usize, Error> {
    charge(bytes).map_err(catalog_failure)
}

fn catalog_failure(error: Error) -> Error {
    if error.code == "STORAGE_FAILURE" {
        Error::new("CONTEXT_CATALOG_INVALID", error.message)
    } else {
        error
    }
}

/// Whether the cache is complete: its rebuild finished without a latched failure. Until it
/// is, [`Timeline::walk`] reads the authoritative primary time index instead, so a reader never
/// waits for the rebuild and never sees a partial timeline.
pub(super) fn cached(db: &DB) -> Result<bool, Error> {
    Ok(db.get_pinned(FAILURE)?.is_none() && catalog_upgrade::progress(db, &JOB)?.complete)
}

/// The state one source-timeline walk reads: the database under the writer or a snapshot,
/// whether its cache is complete ([`cached`] for that state), and the caller's cancellation.
pub(super) struct Timeline<'a> {
    pub reader: &'a Reader<'a>,
    pub cached: bool,
    pub cancellation: &'a crate::QueryCancellation,
}

impl Timeline<'_> {
    /// Walk the receipt timeline of the source whose key hash is `hash`, ordered by
    /// `(received_at, record ID)`, from `received_at` and `after` (exclusive; `None` starts
    /// forward at the first record accepted at `received_at`, or in reverse at the last one
    /// accepted before it) in `direction`, stopping at the first entry whose receipt time is
    /// outside `bounds`. `visit` receives each retained record's receipt time and ID and
    /// returns whether to continue.
    ///
    /// This is the one timeline every source-local reader uses. A complete cache yields the
    /// source's entries directly. Otherwise the walk reads the primary `time:` index, which
    /// holds every retained record of every source, and skips records of other sources by
    /// their catalog headers without charging the caller; it yields exactly the entries the
    /// rebuilt cache will hold, in the same order. The bounds stop both reads, so a skipped
    /// run of other sources' records never extends past the caller's receipt interval.
    /// Cancellation is checked every 4,096 entries read; a caller that may skip many
    /// entries reads from a snapshot without the writer lock (see [`super::context`]).
    pub(super) fn walk(
        &self,
        hash: &str,
        position: (u64, Option<&str>),
        bounds: impl std::ops::RangeBounds<u64>,
        direction: rocksdb::Direction,
        mut visit: impl FnMut(u64, &str) -> Result<bool, Error>,
    ) -> Result<(), Error> {
        let Timeline {
            reader,
            cached,
            cancellation,
        } = *self;
        let (received_at, after) = position;
        let family = if cached {
            hashed_prefix(hash)
        } else {
            "time:".to_owned()
        };
        let start = format!("{family}{received_at:020}:{}", after.unwrap_or(""));
        let invalid = || Error::new("STORAGE_FAILURE", "invalid source timeline entry");
        let mut read = 0_u64;
        reader.iterate(start.as_bytes(), direction, |key, value| {
            if read.is_multiple_of(4096) {
                cancellation.checkpoint()?;
            }
            read += 1;
            if !key.starts_with(family.as_bytes()) {
                return Ok(false);
            }
            if after.is_some() && key == start.as_bytes() {
                return Ok(true);
            }
            let rest = &key[family.len()..];
            let time = rest
                .get(..20)
                .and_then(|time| std::str::from_utf8(time).ok())
                .and_then(|time| time.parse::<u64>().ok())
                .ok_or_else(invalid)?;
            if !bounds.contains(&time) {
                return Ok(false);
            }
            let id = rest
                .get(21..)
                .and_then(|id| std::str::from_utf8(id).ok())
                .filter(|id| rest[20] == b':' && value == id.as_bytes())
                .ok_or_else(|| {
                    Error::new(
                        "STORAGE_FAILURE",
                        "source timeline entry value disagrees with its key",
                    )
                })?;
            let selected = cached || {
                let bytes = reader.get(id.as_bytes())?.ok_or_else(|| {
                    Error::new("STORAGE_FAILURE", "time index references a missing record")
                })?;
                let header = decode(&bytes)?;
                if header.id != id || header.received_at != time {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "time index does not match its record",
                    ));
                }
                super::source_summary::source_hash(&header.source) == hash
            };
            Ok(!selected || visit(time, id)?)
        })
    }
}

#[derive(Default)]
struct Change {
    metadata: Option<Option<String>>,
    time: Option<(String, bool)>,
}

struct Changes {
    rows: BTreeMap<String, Change>,
}

impl Changes {
    fn entry(&mut self, id: &str) -> Result<&mut Change, Error> {
        if !id.starts_with("record:") || id.len() <= 7 || id.len() > 128 {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "source context update requires a bounded record ID",
            ));
        }
        if !self.rows.contains_key(id) && self.rows.len() >= MAX_CONTEXT_RECORDS {
            return Err(Error::input_bound(format!(
                "atomic source context update exceeds metadata admission of {MAX_CONTEXT_RECORDS} distinct records; split the input into parts of at most that many records"
            )));
        }
        Ok(self.rows.entry(id.to_owned()).or_default())
    }
}

impl super::durability::BatchObserver for Changes {
    fn observe(&mut self, name: &[u8], value: Option<&[u8]>) -> Result<(), Error> {
        if name.starts_with(b"record:") {
            let id = std::str::from_utf8(name)
                .map_err(|_| Error::new("STORAGE_FAILURE", "invalid record catalog key"))?;
            let metadata = value
                .map(|bytes| {
                    let record = decode(bytes)?;
                    if record.id != id || record.source.is_empty() || record.source.len() > 512 {
                        return Err(Error::new(
                            "STORAGE_FAILURE",
                            "source context publication identity or source disagrees",
                        ));
                    }
                    Ok(record.key())
                })
                .transpose()?;
            self.entry(id)?.metadata = Some(metadata);
        } else if name.starts_with(b"time:") {
            let name = std::str::from_utf8(name)
                .map_err(|_| Error::new("STORAGE_FAILURE", "invalid source context time key"))?;
            let id = name
                .get(26..)
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "invalid time index width"))?;
            if value.is_some_and(|value| value != id.as_bytes()) {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "time index value disagrees with its identity",
                ));
            }
            self.entry(id)?.time = Some((name.to_owned(), value.is_some()));
        }
        Ok(())
    }
    fn refuse_merge(&self, name: &[u8]) -> Option<Error> {
        (name.starts_with(b"record:") || name.starts_with(b"time:") || name.starts_with(PREFIX))
            .then(|| {
                Error::new(
                    "STORAGE_FAILURE",
                    "source context requires exact catalog/time writes, not merges",
                )
            })
    }
}

/// Fold both catalog and primary-time mutations before appending atomic cache writes.
/// Record updates and primary deletions may occur in either batch order.
pub(super) fn update(writer: &Writer, batch: &mut WriteBatch) -> Result<(), Error> {
    let mut changes = Changes {
        rows: BTreeMap::new(),
    };
    super::durability::observe_batch(batch, &mut changes)?;
    for (id, change) in changes.rows {
        let old = writer
            .db
            .get_pinned(&id)?
            .map(|bytes| decode(&bytes))
            .transpose()?;
        let old_key = old.as_ref().map(Header::key);
        let next = match &change.metadata {
            Some(Some(key)) => Some(key.clone()),
            Some(None) => None,
            None => old_key.clone(),
        };
        if old_key.is_some() && next.is_some() && old_key != next {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "record source and acceptance identity cannot change",
            ));
        }
        let known = next.as_ref().or(old_key.as_ref()).ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "time update references missing record metadata",
            )
        })?;
        let suffix = known
            .strip_prefix(PREFIX_STR)
            .and_then(|key| key.get(65..))
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "invalid source context key"))?;
        let primary = format!("time:{suffix}");
        let visible = if let Some((name, present)) = change.time {
            if name != primary {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "source context time does not match its record",
                ));
            }
            present
        } else {
            writer.db.get_pinned(&primary)?.as_deref() == Some(id.as_bytes())
        };
        if next.is_none() && visible {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "record deletion must remove its primary time index",
            ));
        }
        if let Some(old_key) = old_key {
            batch.delete(old_key);
        }
        if visible && let Some(next) = next {
            batch.put(next, id.as_bytes());
        }
    }
    Ok(())
}

/// Read one record [`Timeline::walk`] yielded from `reader`, charged to `remaining`, after checking
/// its primary time index entry and that it belongs to `source`.
pub(super) fn record(
    reader: &Reader<'_>,
    source: &str,
    received_at: u64,
    id: &str,
    remaining: &mut usize,
) -> Result<Record, Error> {
    let primary = format!("time:{received_at:020}:{id}");
    if reader.get(primary.as_bytes())?.as_deref() != Some(id.as_bytes()) {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "source context index disagrees with primary time index",
        ));
    }
    let record = super::time_record(reader, primary.as_bytes(), id.as_bytes(), remaining)?;
    if record.source != source {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "source context index disagrees with record source",
        ));
    }
    Ok(record)
}
