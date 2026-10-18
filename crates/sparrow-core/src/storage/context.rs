// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Bounded source-local neighbors over one serialized membership capture.

use super::{Catalog, Error, Record, Store, Writer, catalog_lease, json, random_id};
use crate::QueryScope;
use rocksdb::{Direction, IteratorMode};
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
        crate::format_instant(now)?;
        let writer = self.lock()?;
        super::source_context::ready(&writer)?;
        let mut admission = Admission {
            catalog: catalog_lease::SCAN_BYTES - super::QUERY_SCOPE_WORKSPACE_BYTES,
            response: catalog_lease::PAGE_BYTES - 4096,
        };
        let protected = writer.protected(now)?;
        let selection =
            writer.query_selection_membership(&scope.selection, now, &protected, None)?;
        let scope_receipt = super::scope_receipt::capture(&writer.id, scope, selection.as_ref())?;
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
        let anchor: Record = writer.get_limited(id, &mut admission.catalog)?;
        writer.validate_event_record(&anchor)?;
        if !writer.live_record(&anchor, now, &protected) {
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
            || !writer.matches_diagnostic_scope(scope, &anchor, &mut diagnostics, true)?
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
        if writer
            .db
            .get(super::source_context::key(&anchor))?
            .as_deref()
            != Some(anchor.id.as_bytes())
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "context anchor source index is missing or inconsistent",
            ));
        }
        admission.output(&anchor)?;
        let earlier = scan_context_neighbors(
            &writer,
            &anchor,
            scope,
            (before, Direction::Reverse),
            now,
            &protected,
            (membership.as_ref(), &mut admission),
        )?;
        let later = scan_context_neighbors(
            &writer,
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
            store_id: writer.id.clone(),
            membership_at: now,
            anchor,
            diagnostics,
            before: earlier,
            after: later,
        })
    }
}

fn scan_context_neighbors(
    writer: &Writer,
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
    let prefix = super::source_context::prefix(&anchor.source);
    let start = super::source_context::key(anchor);
    for entry in writer
        .db
        .iterator(IteratorMode::From(start.as_bytes(), direction))
    {
        let (key, value) = entry?;
        if !key.starts_with(prefix.as_bytes()) {
            break;
        }
        if key.as_ref() == start.as_bytes() {
            continue;
        }
        if result.visited == scope.scan_budget as u64 {
            result.frontier = ContextFrontier::ScanBudget;
            break;
        }
        let record = super::source_context::indexed(
            writer,
            &anchor.source,
            &key,
            &value,
            &mut admission.catalog,
        )?;
        if record.received_at < scope.received_from || record.received_at >= scope.received_before {
            break;
        }
        result.visited += 1;
        if record.source != anchor.source
            || membership.is_some_and(|members| !members.contains_key(&record.id))
        {
            continue;
        }
        writer.validate_event_record(&record)?;
        if !writer.live_record(&record, now, protected) {
            result.retired += 1;
            continue;
        }
        if !writer.matches_diagnostic_scope(
            scope,
            &record,
            &mut result.diagnostics,
            scope.require_complete_index,
        )? {
            continue;
        }
        admission.output(&record)?;
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
