// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Immutable pages from the single record matcher and native ranker.

use super::{Error, Store, Value, catalog_lease, json, parse, random_id};
use std::time::Instant;

const ROW_BYTES: usize = 32 * 1024;
const METADATA_BYTES: usize = 32 * 1024;

/// Public query surface bound into every continuation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RecordOperation {
    /// Combined record qualification, including fields and source time.
    #[default]
    Records,
    /// Standalone native text search, sharing record capture and ranking.
    Text,
}

/// A complete bounded capture or a continuation with identical query semantics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordRead {
    /// Prevent replay through a different operation even with equivalent predicates.
    pub operation: RecordOperation,
    /// Membership, filters and acceptance interval; limit is the page item ceiling.
    pub scope: crate::QueryScope,
    /// Same-record predicates, source time and optional native text ranking.
    pub query: crate::RecordQuery,
    /// Opaque immutable continuation, or a fresh capture.
    pub cursor: Option<String>,
}

impl catalog_lease::Selector for RecordRead {
    fn capacity(&self) -> usize {
        self.scope.capacity()
            + self.query.capacity()
            + self.cursor.as_ref().map_or(0, String::capacity)
    }
}

/// Encoded native result metadata and bounded row bodies, without retained storage versions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordPageBody {
    metadata: Vec<u8>,
    rows: Vec<Vec<u8>>,
}

impl RecordPageBody {
    fn capture(mut result: Value) -> Result<Self, Error> {
        let object = result
            .as_object_mut()
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "record capture is not an object"))?;
        let mut values = object
            .remove("rows")
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "record capture rows are missing"))?;
        let values = values
            .as_array_mut()
            .map(std::mem::take)
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "record capture rows are not an array"))?;
        let metadata = json::write_compact(&result).into_bytes();
        if metadata.len() > METADATA_BYTES || values.len() > catalog_lease::MAX_ITEMS {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "record capture metadata/items exceed admission; narrow query",
            ));
        }
        let mut bytes = metadata.len();
        let mut rows = Vec::with_capacity(values.len());
        for value in values {
            let row = json::write_compact(&value).into_bytes();
            bytes = bytes.saturating_add(row.len() + std::mem::size_of::<Vec<u8>>());
            if row.len() > ROW_BYTES || bytes > catalog_lease::CAPTURE_BYTES {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "record capture exceeds row/byte admission; narrow query; no page published",
                ));
            }
            rows.push(row);
        }
        Ok(Self { metadata, rows })
    }

    /// Reconstruct only this admitted page through PurRDF's native JSON codec.
    pub fn result(&self) -> Result<Value, Error> {
        let mut result = parse(&self.metadata)?;
        let rows = self
            .rows
            .iter()
            .map(|bytes| parse(bytes))
            .collect::<Result<Vec<_>, _>>()?;
        result
            .as_object_mut()
            .ok_or_else(|| {
                Error::new(
                    "STORAGE_FAILURE",
                    "captured record metadata is not an object",
                )
            })?
            .insert("rows", Value::array(rows));
        Ok(result)
    }
}

impl catalog_lease::PageBody for RecordPageBody {
    const PAGE_ITEMS: usize = 1000;
    fn len(&self) -> usize {
        self.rows.len()
    }
    fn capacity(&self) -> usize {
        self.metadata.capacity()
            + self.rows.capacity() * std::mem::size_of::<Vec<u8>>()
            + self.rows.iter().map(Vec::capacity).sum::<usize>()
    }
    fn page(&self, start: usize, end: usize) -> Self {
        Self {
            metadata: self.metadata.clone(),
            rows: self.rows[start..end].to_vec(),
        }
    }
    fn page_end(&self, start: usize, page_size: usize) -> usize {
        // Typed replies render record rows and ranked evidence separately.
        let mut bytes = self.metadata.len() * 2 + 4096;
        let mut end = start;
        for row in self
            .rows
            .iter()
            .skip(start)
            .take(page_size.min(Self::PAGE_ITEMS))
        {
            let charge = row.len() * 2 + 1024;
            if bytes + charge > catalog_lease::PAGE_BYTES {
                break;
            }
            bytes += charge;
            end += 1;
        }
        end
    }
    fn is_boundary(&self, start: usize, page_size: usize) -> bool {
        let mut position = 0;
        while position < start {
            let next = self.page_end(position, page_size);
            if next <= position {
                return false;
            }
            position = next;
        }
        position == start
    }
}

/// Captured query receipt and one page. Reading does not renew retention or the lease.
pub type RecordReadPage = catalog_lease::CatalogReadPage<RecordPageBody>;
pub(super) type Reads = catalog_lease::Reads<RecordRead, RecordPageBody>;

impl Store {
    /// Capture all admitted record matches once, then replay immutable bounded pages.
    pub fn records_read(&self, query: &RecordRead, now: u64) -> Result<RecordReadPage, Error> {
        query.scope.validate()?;
        query.query.validate()?;
        if query.operation == RecordOperation::Text
            && (query.query.text.is_none()
                || !query.query.fields.is_empty()
                || query.query.event_interval.is_some())
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "standalone text capture requires text and admits no record fields or event interval",
            ));
        }
        if query
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.len() > 160)
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "record cursor exceeds 160 bytes",
            ));
        }
        let mut selector = query.clone();
        selector.cursor = None;
        let mut reads = self
            .record_reads
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "record read registry poisoned"))?;
        reads.purge(now);
        if let Some(cursor) = &query.cursor {
            return reads.resume(
                &selector,
                cursor,
                query.scope.limit,
                query.scope.scan_budget,
                now,
            );
        }
        let started = Instant::now();
        let expires_at = now
            .checked_add(catalog_lease::LEASE_MS)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "record read deadline overflow"))?;
        crate::format_instant(now)?;
        crate::format_instant(expires_at)?;
        let value = self.records_query_capture(&query.scope, &query.query, now, true)?;
        let store_id = super::read_field::<String>(&value, "store_id")?;
        let scanned = usize::try_from(super::integer(&value["coverage"], "visited")?)
            .map_err(|_| Error::new("STORAGE_FAILURE", "record capture scan count overflow"))?;
        let body = RecordPageBody::capture(value)?;
        reads.publish(
            catalog_lease::Snapshot {
                bytes: 0,
                id: random_id("record-read")?,
                store_id,
                selector,
                as_of: now,
                expires_at,
                started,
                page_size: query.scope.limit,
                scan_budget: query.scope.scan_budget,
                scanned,
                body,
            },
            now,
            catalog_lease::CAPTURE_BYTES,
            catalog_lease::HANDLE_BYTES,
            catalog_lease::MAX_HANDLES,
        )
    }

    pub(super) fn record_read_stats(&self, now: u64) -> Result<(usize, usize), Error> {
        let mut reads = self
            .record_reads
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "record read registry poisoned"))?;
        reads.purge(now);
        Ok((reads.snapshots.len(), reads.capacity()))
    }
}
