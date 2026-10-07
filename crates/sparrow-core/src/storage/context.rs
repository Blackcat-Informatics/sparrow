// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Bounded source-local neighbors over one serialized membership capture.

use super::{Error, Record, Store, Writer};
use crate::QueryScope;
use rocksdb::{Direction, IteratorMode};
use std::collections::BTreeSet;

/// Why a directional context scan stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContextFrontier {
    /// The requested number of retained neighbors was found.
    RequestedCount,
    /// No further catalog entries remain in the requested interval.
    ScopeBoundary,
    /// The catalog visit ceiling was reached; nearer neighbors may be missing.
    ScanBudget,
}

/// Directional context evidence. Counts cover visited catalog entries only.
#[derive(Debug)]
pub struct ContextSide {
    /// Chronologically sorted retained neighbors, excluding the anchor.
    pub records: Vec<Record>,
    /// Catalog entries examined, including other sources and retired entries.
    pub visited: u64,
    /// Observed expired/evicted entries from the anchor's source.
    pub retired: u64,
    /// Explicit reason that this scan ended.
    pub frontier: ContextFrontier,
}

/// Context ordered by `(received_at, record_id)`, independent of producer order.
#[derive(Debug)]
pub struct Context {
    /// Store identity of all returned references.
    pub store_id: String,
    /// One captured eligibility instant for the complete operation.
    pub membership_at: u64,
    /// The retained requested record.
    pub anchor: Record,
    /// Earlier retained records from the same source in the selected interval.
    pub before: ContextSide,
    /// Later retained records from the same source in the selected interval.
    pub after: ContextSide,
}

impl Store {
    /// Capture bounded neighbors of a retained record, with visible scan/retention gaps.
    /// `scope.limit` bounds total output including the anchor. `scan_budget` applies
    /// independently to each direction. The writer lock keeps membership consistent;
    /// the operation creates no pin and does not promise complete producer history.
    pub fn records_context(
        &self,
        id: &str,
        scope: &QueryScope,
        before: usize,
        after: usize,
        now: u64,
    ) -> Result<Context, Error> {
        scope.validate()?;
        if !id.starts_with("record:") || before.saturating_add(after) >= scope.limit {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "context requires a record ID and before + after + 1 <= limit",
            ));
        }
        let writer = self.lock()?;
        let protected = writer.protected(now)?;
        let anchor: Record = writer.get(id)?;
        if !writer.live_record(&anchor, now, &protected) {
            return Err(Error::new(
                "EVIDENCE_UNAVAILABLE",
                "context anchor expired or was evicted",
            ));
        }
        if anchor.received_at < scope.received_from
            || anchor.received_at >= scope.received_before
            || scope.source.as_ref().is_some_and(|s| *s != anchor.source)
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "context anchor is outside the requested scope",
            ));
        }
        let anchor_key = format!("time:{:020}:{}", anchor.received_at, anchor.id);
        if writer.db.get(&anchor_key)?.as_deref() != Some(anchor.id.as_bytes()) {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "context anchor time index is missing or inconsistent",
            ));
        }
        let earlier = scan_context_neighbors(
            &writer,
            &anchor,
            scope,
            before,
            Direction::Reverse,
            now,
            &protected,
        )?;
        let later = scan_context_neighbors(
            &writer,
            &anchor,
            scope,
            after,
            Direction::Forward,
            now,
            &protected,
        )?;
        Ok(Context {
            store_id: writer.id.clone(),
            membership_at: now,
            anchor,
            before: earlier,
            after: later,
        })
    }
}

fn scan_context_neighbors(
    writer: &Writer,
    anchor: &Record,
    scope: &QueryScope,
    count: usize,
    direction: Direction,
    now: u64,
    protected: &BTreeSet<String>,
) -> Result<ContextSide, Error> {
    let mut result = ContextSide {
        records: Vec::new(),
        visited: 0,
        retired: 0,
        frontier: ContextFrontier::ScopeBoundary,
    };
    if count == 0 {
        result.frontier = ContextFrontier::RequestedCount;
        return Ok(result);
    }
    let start = format!("time:{:020}:{}", anchor.received_at, anchor.id);
    for entry in writer
        .db
        .iterator(IteratorMode::From(start.as_bytes(), direction))
    {
        let (key, value) = entry?;
        if !key.starts_with(b"time:") {
            break;
        }
        if key.as_ref() == start.as_bytes() {
            continue;
        }
        if result.visited == scope.scan_budget as u64 {
            result.frontier = ContextFrontier::ScanBudget;
            break;
        }
        let id = std::str::from_utf8(&value)
            .map_err(|e| Error::new("STORAGE_FAILURE", format!("invalid time index: {e}")))?;
        let record: Record = writer.get(id).map_err(|error| {
            if error.code == "NOT_FOUND" {
                Error::new(
                    "STORAGE_FAILURE",
                    "context time index references a missing record",
                )
            } else {
                error
            }
        })?;
        if key.as_ref() != format!("time:{:020}:{}", record.received_at, record.id).as_bytes() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "context time index does not match its record",
            ));
        }
        if record.received_at < scope.received_from || record.received_at >= scope.received_before {
            break;
        }
        result.visited += 1;
        if record.source != anchor.source {
            continue;
        }
        if !writer.live_record(&record, now, protected) {
            result.retired += 1;
            continue;
        }
        result.records.push(record);
        if result.records.len() == count {
            result.frontier = ContextFrontier::RequestedCount;
            break;
        }
    }
    if matches!(direction, Direction::Reverse) {
        result.records.reverse();
    }
    Ok(result)
}
