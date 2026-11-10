// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Bounded source-local neighbors over one serialized membership capture.

use super::source_context::Timeline;
use super::store_counters::Reader;
use super::{Catalog, Error, Record, Store, catalog_lease, json, random_id};
use crate::QueryScope;
use rocksdb::Direction;
use std::collections::{BTreeMap, BTreeSet};

/// Host-owned context admission; record interpretation profiles remain in each row.
pub const CONTEXT_CAPTURE_PROFILE: &str = "sparrow-context-capture/3";

/// Effective ceilings bound into one context capture.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextLimits {
    /// Independent chronological visits for each direction.
    pub direction_visits: u64,
    /// Aggregate conservative catalog decoding allowance, including the anchor.
    pub catalog_bytes: u64,
    /// Aggregate conservative typed/agent reply allowance.
    pub response_bytes: u64,
    /// Requested total output ceiling, including the anchor.
    pub output_records: u32,
    /// Requested earlier neighbors, excluding the anchor.
    pub requested_before: u32,
    /// Requested later neighbors, excluding the anchor.
    pub requested_after: u32,
    /// Independent membership member traversal ceiling.
    pub membership_member_visits: u64,
    /// Independent membership record traversal ceiling.
    pub membership_record_visits: u64,
    /// Conservative workspace reserved for the frozen scope receipt and its hashing.
    pub scope_receipt_bytes: u64,
}

struct Admission {
    catalog: usize,
    response: usize,
}
impl Admission {
    fn output(&mut self, record: &Record) -> Result<(), Error> {
        let charge = catalog_lease::response_row_bytes(json::write_compact(&record.encode()).len());
        self.response = self.response.checked_sub(charge).ok_or_else(|| {
            Error::new("RESOURCE_LIMIT", "context reply exceeds aggregate byte admission; request fewer neighbors; no result published")
        })?;
        Ok(())
    }
}

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
    /// Source timeline entries examined, including retired entries.
    pub visited: u64,
    /// Observed expired/evicted entries from the anchor's source.
    pub retired: u64,
    /// Diagnostic membership evaluated in this direction.
    pub diagnostics: super::diagnostic::DiagnosticCoverage,
    /// Explicit reason that this scan ended.
    pub frontier: ContextFrontier,
}

/// Context ordered by `(received_at, record_id)`, independent of producer order.
#[derive(Debug)]
pub struct Context {
    /// Opaque identity of this single, non-paginated capture.
    pub snapshot_id: String,
    /// Frozen logical scope, resolved at the same boundary as anchor membership.
    pub scope: super::Value,
    /// Actual host admission ceilings used for this capture.
    pub limits: ContextLimits,
    /// Store identity of all returned references.
    pub store_id: String,
    /// One captured eligibility instant for the complete operation.
    pub membership_at: u64,
    /// The retained requested record.
    pub anchor: Record,
    /// Diagnostic membership of the anchor and both disjoint directions.
    pub diagnostics: super::diagnostic::DiagnosticCoverage,
    /// Earlier retained records from the same source in the selected interval.
    pub before: ContextSide,
    /// Later retained records from the same source in the selected interval.
    pub after: ContextSide,
}

impl Store {
    /// Capture bounded neighbors of a retained record, with visible scan/retention gaps.
    /// `scope.limit` bounds total output including the anchor. `scan_budget` applies
    /// independently to each direction. The operation creates no pin and does not promise
    /// complete producer history.
    pub fn records_context(
        &self,
        id: &str,
        scope: &QueryScope,
        before: usize,
        after: usize,
        now: u64,
    ) -> Result<Context, Error> {
        self.records_context_cancellable(
            id,
            scope,
            (before, after),
            now,
            &crate::QueryCancellation::default(),
        )
    }

    /// [`Store::records_context`] with cooperative caller cancellation. The writer lock is
    /// held only to capture the protected set, the scope's membership and one snapshot at
    /// the same instant; the anchor and both neighbor walks then read that snapshot without
    /// the lock, so a walk that skips other sources' records while the source timeline
    /// cache rebuilds never holds back a write (#158).
    pub fn records_context_cancellable(
        &self,
        id: &str,
        scope: &QueryScope,
        neighbors: (usize, usize),
        now: u64,
        cancellation: &crate::QueryCancellation,
    ) -> Result<Context, Error> {
        let (before, after) = neighbors;
        scope.validate()?;
        if !id.starts_with("record:") || before.saturating_add(after) >= scope.limit {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "context requires a record ID and before + after + 1 <= limit",
            ));
        }
        crate::format_instant(now)?;
        let writer = self.lock()?;
        cancellation.checkpoint()?;
        let mut admission = Admission {
            catalog: catalog_lease::SCAN_BYTES - super::QUERY_SCOPE_WORKSPACE_BYTES,
            response: catalog_lease::PAGE_BYTES - 4096,
        };
        let protected = writer.protected(now)?;
        let selection =
            writer.query_selection_membership(&scope.selection, now, &protected, None)?;
        let scope_receipt = super::scope_receipt::capture(&writer.id, scope, selection.as_ref())?;
        // Every write takes the writer lock, so the cache state read here is the state of
        // the snapshot taken with it.
        let cached = super::source_context::cached(&writer.db)?;
        let store_id = writer.id.clone();
        let db = std::sync::Arc::clone(&writer.db);
        let snapshot = db.snapshot();
        drop(writer);
        let reader = Reader::snapshot(&snapshot);
        admission.response = admission
            .response
            .checked_sub(catalog_lease::response_row_bytes(
                json::write_compact(&scope_receipt).len(),
            ))
            .ok_or_else(|| {
                Error::new(
                    "RESOURCE_LIMIT",
                    "context scope receipt exceeds reply admission",
                )
            })?;
        let membership = selection.map(|selection| selection.records);
        let anchor = super::record_entry(&reader, id, &mut admission.catalog)?;
        super::source_time::validate_event_record(&reader, &anchor)?;
        if !super::live_record(&anchor, now, &protected) {
            return Err(Error::new(
                "EVIDENCE_UNAVAILABLE",
                "context anchor expired or was evicted",
            ));
        }
        let mut diagnostics = super::diagnostic::DiagnosticCoverage::default();
        if membership
            .as_ref()
            .is_some_and(|members| !members.contains_key(id))
            || anchor.received_at < scope.received_from
            || anchor.received_at >= scope.received_before
            || !scope.matches_source(&anchor.source)
            || !super::diagnostic::matches_diagnostic_scope(
                &reader,
                scope,
                &anchor,
                &mut diagnostics,
                true,
            )?
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "context anchor is outside the requested scope",
            ));
        }
        let anchor_key = format!("time:{:020}:{}", anchor.received_at, anchor.id);
        if reader.get(anchor_key.as_bytes())?.as_deref() != Some(anchor.id.as_bytes()) {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "context anchor time index is missing or inconsistent",
            ));
        }
        if cached
            && reader
                .get(super::source_context::key(&anchor).as_bytes())?
                .as_deref()
                != Some(anchor.id.as_bytes())
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "context anchor source index is missing or inconsistent",
            ));
        }
        admission.output(&anchor)?;
        let timeline = Timeline {
            reader: &reader,
            cached,
            cancellation,
        };
        let earlier = scan_context_neighbors(
            &timeline,
            &anchor,
            scope,
            (before, Direction::Reverse),
            now,
            &protected,
            (membership.as_ref(), &mut admission),
        )?;
        let later = scan_context_neighbors(
            &timeline,
            &anchor,
            scope,
            (after, Direction::Forward),
            now,
            &protected,
            (membership.as_ref(), &mut admission),
        )?;
        diagnostics.merge(earlier.diagnostics);
        diagnostics.merge(later.diagnostics);
        Ok(Context {
            scope: scope_receipt,
            snapshot_id: random_id("context-read")?,
            limits: ContextLimits {
                scope_receipt_bytes: super::QUERY_SCOPE_WORKSPACE_BYTES as u64,
                direction_visits: scope.scan_budget as u64,
                catalog_bytes: catalog_lease::SCAN_BYTES as u64,
                response_bytes: catalog_lease::PAGE_BYTES as u64,
                output_records: scope.limit as u32,
                requested_before: before as u32,
                requested_after: after as u32,
                membership_member_visits: super::query_membership::MAX_SELECTION_VISITS as u64,
                membership_record_visits: super::query_membership::MAX_SELECTION_VISITS as u64,
            },
            store_id,
            membership_at: now,
            anchor,
            diagnostics,
            before: earlier,
            after: later,
        })
    }
}

fn scan_context_neighbors(
    timeline: &Timeline<'_>,
    anchor: &Record,
    scope: &QueryScope,
    neighbors: (usize, Direction),
    now: u64,
    protected: &BTreeSet<String>,
    capture: (Option<&BTreeMap<String, u64>>, &mut Admission),
) -> Result<ContextSide, Error> {
    let (membership, admission) = capture;
    let (count, direction) = neighbors;
    let mut result = ContextSide {
        records: Vec::new(),
        visited: 0,
        retired: 0,
        diagnostics: super::diagnostic::DiagnosticCoverage::default(),
        frontier: ContextFrontier::ScopeBoundary,
    };
    if count == 0 {
        result.frontier = ContextFrontier::RequestedCount;
        return Ok(result);
    }
    let hash = super::source_summary::source_hash(&anchor.source);
    let reader = timeline.reader;
    timeline.walk(
        &hash,
        (anchor.received_at, Some(&anchor.id)),
        // Receipt bounds stop the walk before the visit budget: once the timeline leaves
        // the interval, no nearer neighbor remains.
        scope.received_from..scope.received_before,
        direction,
        |received_at, id| {
            if result.visited == scope.scan_budget as u64 {
                result.frontier = ContextFrontier::ScanBudget;
                return Ok(false);
            }
            let record = super::source_context::record(
                reader,
                &anchor.source,
                received_at,
                id,
                &mut admission.catalog,
            )?;
            result.visited += 1;
            if membership.is_some_and(|members| !members.contains_key(&record.id)) {
                return Ok(true);
            }
            super::source_time::validate_event_record(reader, &record)?;
            if !super::live_record(&record, now, protected) {
                result.retired += 1;
                return Ok(true);
            }
            if !super::diagnostic::matches_diagnostic_scope(
                reader,
                scope,
                &record,
                &mut result.diagnostics,
                scope.require_complete_index,
            )? {
                return Ok(true);
            }
            admission.output(&record)?;
            result.records.push(record);
            if result.records.len() == count {
                result.frontier = ContextFrontier::RequestedCount;
                return Ok(false);
            }
            Ok(true)
        },
    )?;
    if matches!(direction, Direction::Reverse) {
        result.records.reverse();
    }
    Ok(result)
}
