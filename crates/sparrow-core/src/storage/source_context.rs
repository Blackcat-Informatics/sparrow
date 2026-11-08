// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Session-rebuilt source timeline cache over the authoritative record/time catalog.

use super::{Error, Record, Writer, catalog_upgrade, integer, parse, read_field};
use rocksdb::{DB, WriteBatch};
use std::collections::BTreeMap;

const PREFIX_STR: &str = "source-context-v1:";
const PREFIX: &[u8] = PREFIX_STR.as_bytes();
const FAILURE: &str = "source-context-cache-failure";
const SCRATCH_BYTES: usize = 32 * 1024 * 1024;
const METADATA_BYTES: usize = 32 * 1024 * 1024;
const JOB: catalog_upgrade::Job = catalog_upgrade::Job {
    checkpoint: "source-context-cache-upgrade",
    source: "record:",
    target: PREFIX,
    page_entries: 512,
    page_bytes: SCRATCH_BYTES,
    charge: reconstruction_charge,
    publish,
};

pub(super) fn prefix(source: &str) -> String {
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

/// This cache is not a catalog format or a writer fence. Rebuilding every open
/// permits preceding binaries to write authoritative rows without stale answers.
/// Cold reconstruction uses the shared finite catalog pager, never a resident corpus.
pub(super) fn rebuild(db: &DB) -> Result<(), Error> {
    loop {
        let mut batch = WriteBatch::default();
        let mut count = 0;
        for entry in db.prefix_iterator(PREFIX).take(512) {
            let (key, _) = entry?;
            if !key.starts_with(PREFIX) {
                break;
            }
            batch.delete(&key);
            count += 1;
        }
        if count == 0 {
            break;
        }
        catalog_upgrade::commit(db, batch)?;
    }
    let mut batch = WriteBatch::default();
    batch.delete(JOB.checkpoint);
    batch.delete(FAILURE);
    catalog_upgrade::commit(db, batch)?;
    let mut batch = WriteBatch::default();
    catalog_upgrade::seed(db, &JOB, &mut batch)?;
    catalog_upgrade::commit(db, batch)?;
    match catalog_upgrade::run(db, &JOB, None) {
        Err(error) if error.code == "CONTEXT_CATALOG_INVALID" => {
            let mut batch = WriteBatch::default();
            batch.put(FAILURE, error.message.as_bytes());
            catalog_upgrade::commit(db, batch)
        }
        result => result,
    }
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

pub(super) fn ready(writer: &Writer) -> Result<(), Error> {
    if writer.db.get_pinned(FAILURE)?.is_some() {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "source context reconstruction found corrupt record metadata; repair the authoritative catalog and reopen before requesting context",
        ));
    }
    Ok(())
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
        if !self.rows.contains_key(id) && self.rows.len() >= METADATA_BYTES / 1024 {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "atomic source context update exceeds metadata admission",
            ));
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

pub(super) fn indexed(
    writer: &Writer,
    source: &str,
    name: &[u8],
    id: &[u8],
    remaining: &mut usize,
) -> Result<Record, Error> {
    let prefix = prefix(source);
    let suffix = name
        .strip_prefix(prefix.as_bytes())
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "context index source prefix disagrees"))?;
    let mut primary = b"time:".to_vec();
    primary.extend_from_slice(suffix);
    if writer.db.get_pinned(&primary)?.as_deref() != Some(id) {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "source context index disagrees with primary time index",
        ));
    }
    let record = writer.time_record(&primary, id, remaining)?;
    if record.source != source {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "source context index disagrees with record source",
        ));
    }
    Ok(record)
}
