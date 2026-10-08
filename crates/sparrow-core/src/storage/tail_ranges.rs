// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Ordered membership of retained original bytes in a file generation.

use super::{Catalog, Error, Record, catalog_upgrade, parse};
use rocksdb::{DB, WriteBatch};

const PREFIX: &[u8] = b"tail-range:";
const JOB: catalog_upgrade::Job = catalog_upgrade::Job {
    checkpoint: "tail-range-index-upgrade",
    source: "record:",
    target: PREFIX,
    page_bytes: 32 * 1024 * 1024,
    charge: upgrade_charge,
    publish: upgrade_publish,
};

fn key(record: &Record) -> Option<String> {
    record.tail.as_ref().map(|position| {
        format!(
            "tail-range:{}:{:020}:{}",
            position.generation_id, position.source_start, record.id
        )
    })
}

pub(super) fn publish(batch: &mut WriteBatch, record: &Record) {
    if !record.evicted
        && !record.chunks.is_empty()
        && let Some(key) = key(record)
    {
        batch.put(key, record.id.as_bytes());
    }
}

pub(super) fn retire(batch: &mut WriteBatch, record: &Record) {
    if let Some(key) = key(record) {
        batch.delete(key);
    }
}

pub(super) fn seed(db: &DB, batch: &mut WriteBatch) -> Result<(), Error> {
    catalog_upgrade::seed(db, &JOB, batch)
}

pub(super) fn upgrade(db: &DB) -> Result<(), Error> {
    catalog_upgrade::run(db, &JOB)
}

fn upgrade_charge(bytes: &[u8]) -> Result<usize, Error> {
    // Bound catalog decoding plus the range key/value and write-batch allocation.
    Ok(bytes.len().saturating_mul(65).saturating_add(512))
}

fn upgrade_publish(
    identity: &str,
    bytes: &[u8],
    _charge: usize,
    batch: &mut WriteBatch,
) -> Result<(), Error> {
    let record = Record::decode(&parse(bytes)?)?;
    if record.id != identity {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "tail range upgrade record identity mismatch",
        ));
    }
    publish(batch, &record);
    Ok(())
}
