// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Ordered membership of retained original bytes in a file generation.

use super::{Catalog, Error, Record, Writer, catalog_upgrade, parse, tail};
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

/// Validate the complete logical cover while assembling only the requested bounded bytes.
/// The writer guard serializes this read against retirement and pin changes.
pub(super) fn bytes(
    writer: &Writer,
    id: &str,
    offset: u64,
    length: usize,
    now: u64,
) -> Result<Vec<u8>, Error> {
    const VISITS: usize = 100_000;
    const ROW_BYTES: usize = 64 * 1024;
    const CATALOG_BYTES: usize = 64 * 1024 * 1024;
    let generation = tail::registry(writer)?
        .into_iter()
        .find(|row| row.id == id)
        .ok_or_else(|| Error::new("NOT_FOUND", "file generation absent"))?;
    if !generation.complete || generation.gap.is_some() {
        return Err(Error::new(
            "EVIDENCE_UNAVAILABLE",
            "file generation has no complete terminal checkpoint",
        ));
    }
    if generation.records == 0
        && generation
            .expires_at
            .is_some_and(|deadline| now >= deadline)
    {
        return Err(Error::new(
            "EVIDENCE_UNAVAILABLE",
            "empty completed file metadata expired",
        ));
    }
    let end = offset
        .checked_add(length as u64)
        .filter(|end| *end <= generation.offset)
        .ok_or_else(|| Error::new("INVALID_ARGUMENT", "byte range is outside completed file"))?;
    let mut visits = VISITS;
    let mut pin_bytes = CATALOG_BYTES;
    let protected = writer.protected_matching(
        now,
        |_| true,
        || {
            visits = visits.checked_sub(1).ok_or_else(|| {
                Error::new(
                    "RESOURCE_LIMIT",
                    "file byte cover exceeds catalog visit admission",
                )
            })?;
            Ok(())
        },
        Some(1024 * 1024),
        Some(&mut pin_bytes),
    )?;
    let prefix = format!("tail-range:{id}:");
    let mut remaining = CATALOG_BYTES;
    let mut position = 0;
    let mut records = 0_u64;
    let mut result = Vec::with_capacity(length);
    for entry in writer.db.prefix_iterator(prefix.as_bytes()) {
        let (index, identity) = entry?;
        if !index.starts_with(prefix.as_bytes()) {
            break;
        }
        visits = visits.checked_sub(1).ok_or_else(|| {
            Error::new(
                "RESOURCE_LIMIT",
                "file byte cover exceeds catalog visit admission",
            )
        })?;
        if index.len() > 512 || identity.len() > 128 {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "tail range index exceeds identity bounds",
            ));
        }
        let identity = std::str::from_utf8(&identity).map_err(|_| {
            Error::new(
                "STORAGE_FAILURE",
                "tail range index has invalid record identity",
            )
        })?;
        if !identity.starts_with("record:") || identity.len() <= "record:".len() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "tail range index requires a concrete record identity",
            ));
        }
        let record: Record = writer
            .get_admitted(identity, |bytes| {
                if bytes.len() > ROW_BYTES {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "tail range record exceeds streaming catalog row admission",
                    ));
                }
                remaining = remaining.checked_sub(bytes.len()).ok_or_else(|| {
                    Error::new(
                        "RESOURCE_LIMIT",
                        "file byte cover exceeds streaming catalog byte admission",
                    )
                })?;
                Ok(())
            })
            .map_err(|error| {
                if error.code == "NOT_FOUND" {
                    Error::new(
                        "STORAGE_FAILURE",
                        "tail range index references a missing record",
                    )
                } else {
                    error
                }
            })?;
        let source = record.tail.as_ref().ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "tail range index references unrelated record",
            )
        })?;
        if record.id != identity
            || key(&record).as_deref().map(str::as_bytes) != Some(index.as_ref())
            || source.declaration != generation.declaration
            || source.source_end > generation.offset
            || source.source_start < position
            || record.evicted
            || record.chunks.is_empty()
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "tail range index contradicts original record or generation",
            ));
        }
        if source.source_start != position || !writer.live_record(&record, now, &protected) {
            return Err(Error::new(
                "EVIDENCE_UNAVAILABLE",
                "complete file byte cover is no longer retained; read surviving records",
            ));
        }
        if length != 0 && source.source_start < end && source.source_end > offset {
            let start = offset.max(source.source_start);
            let count = (end.min(source.source_end) - start) as usize;
            result.extend_from_slice(&super::read_chunk_range(
                record
                    .chunks
                    .iter()
                    .map(|digest| writer.payload_chunk(digest)),
                start - source.source_start,
                count,
            )?);
        }
        position = source.source_end;
        records += 1;
    }
    if position != generation.offset {
        return Err(Error::new(
            "EVIDENCE_UNAVAILABLE",
            "complete file byte cover is no longer retained; read surviving records",
        ));
    }
    if records != generation.records || result.len() != length {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "complete file coverage contradicts its terminal checkpoint",
        ));
    }
    Ok(result)
}
