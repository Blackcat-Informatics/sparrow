// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Finite catalog expiry, sharing all dependency retirement with eviction.

use super::eviction::EvictionLimits;
use super::manifest_retire::{self, Envelope};
use super::{DAY, Error, RETENTION_MS, Store, Writer, put, recordless, retire};
use rocksdb::{Direction, IteratorMode};
use std::collections::{BTreeMap, BTreeSet};

/// Longest the maintenance worker waits between expiry passes: protects against a wall
/// clock that stepped forward, and paces the request-row scan.
pub const EXPIRY_CEILING_MS: u64 = 60 * 60 * 1000;

/// A seek position in the receipt order, `{received_at:020}:{id}`; `true` includes it.
type Position = (Vec<u8>, bool);

/// The later of two positions; an exclusive position beats an inclusive one at the same key,
/// because it records that the key was processed. `None` is the start of the index.
fn later(a: Option<Position>, b: Option<Position>) -> Option<Position> {
    match (a, b) {
        (Some(a), Some(b)) => Some(match a.0.cmp(&b.0) {
            std::cmp::Ordering::Less => b,
            std::cmp::Ordering::Greater => a,
            std::cmp::Ordering::Equal => (a.0, a.1 && b.1),
        }),
        (a, b) => a.or(b),
    }
}

/// Move `position` back to `key`, inclusive, when it lies at or after it.
fn lower(position: &mut Option<Position>, key: &[u8]) {
    if position
        .as_ref()
        .is_some_and(|(at, _)| at.as_slice() >= key)
    {
        *position = Some((key.to_vec(), true));
    }
}

/// Where the hourly request-row scan stands.
#[derive(Clone, PartialEq, Eq)]
enum RequestScan {
    /// Finished at this clock reading, or never run.
    Waiting(Option<u64>),
    /// In progress; resume after this key.
    Walking(Option<Vec<u8>>),
}

/// Ephemeral native seek positions; durable evidence never depends on them. A restart
/// re-walks at most about a day of retired rows, because rows are deleted one day after
/// they retire.
#[derive(Clone)]
pub(super) struct Cursor {
    /// Next key the retirement walk examines.
    retire: Option<Position>,
    /// Next key the deletion walk examines.
    delete: Option<Position>,
    /// Smallest key a walk skipped because a pin holds it, since the last rewind.
    held: Option<Vec<u8>>,
    /// Earliest own deadline of a deferred entry: when it arrives, the walks rewind to it.
    deferred: Option<u64>,
    /// Protection the last pass saw; a member that disappears releases held rows.
    protected: BTreeSet<String>,
    requests: RequestScan,
    /// Rows expiry has read, counted where each row is read; read by acceptance tests.
    reads: u64,
}

impl Default for Cursor {
    fn default() -> Self {
        Self {
            retire: None,
            delete: None,
            held: None,
            deferred: None,
            protected: BTreeSet::new(),
            requests: RequestScan::Waiting(None),
            reads: 0,
        }
    }
}

impl Cursor {
    /// A publication behind a cursor moves it back, so the new key is still reached.
    pub(super) fn published(&mut self, key: &[u8]) {
        lower(&mut self.retire, key);
        lower(&mut self.delete, key);
    }

    /// Protection ended for some member: the rows held on it may now retire.
    fn rewind(&mut self) {
        if let Some(held) = self.held.take() {
            lower(&mut self.retire, &held);
            lower(&mut self.delete, &held);
        }
    }
}

/// Pin rows an expiry pass collects while it captures protection.
#[derive(Default)]
pub(super) struct PinSweep {
    /// Keys of expired pin rows, for deletion.
    pub(super) expired: Vec<Box<[u8]>>,
    /// Earliest deadline of a live pin: protection ends there without an external event.
    next_expiry: Option<u64>,
}

impl PinSweep {
    pub(super) fn live(&mut self, expires_at: u64) {
        self.next_expiry = Some(self.next_expiry.map_or(expires_at, |at| at.min(expires_at)));
    }
}

/// Why a finite expiry pass stopped.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExpiryFrontier {
    /// Everything due was handled; the next pass starts where this one stopped.
    #[default]
    Exhausted,
    /// Maximum catalog entries inspected.
    ScanBudget,
    /// Maximum record/artifact dependency retirement units.
    UnitLimit,
    /// The next complete record cannot fit remaining payload ownership admission.
    PayloadBudget,
    /// Catalog capture stopped after a complete safe prefix.
    CatalogBudget,
    /// The next due entry is not yet covered by a committed archive segment. Archiving
    /// is a prefix, so nothing after it is due either; an archive commit wakes expiry.
    ArchiveLag,
}

/// Durable logical expiry observations, independent of native physical reclamation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExpiryReport {
    /// Catalog entries inspected, separately from bounded pin protection capture.
    pub visited: usize,
    /// Records whose dependency closure was retired.
    pub records: u64,
    /// Artifacts whose complete manifest ownership was partially or fully retired.
    pub artifacts: u64,
    /// Payload ownership references retired through the shared owner.
    pub payload_refs: usize,
    /// Expired catalog and retry rows removed.
    pub deleted: usize,
    /// Due entries, at most one per pass, that stopped it at the archive watermark.
    pub archive_waiting: u64,
    /// Entries seen whose own `expires_at` differs from receipt plus retention. They are
    /// honoured at their own deadline, never fatal; a nonzero count needs investigation.
    /// A per-pass observation: one entry can be counted again by later passes.
    pub anomalies: u64,
    /// Explicit bounded frontier.
    pub frontier: ExpiryFrontier,
    /// Due work remains; run another pass without waiting.
    pub continuation: bool,
    /// Earliest clock reading at which a pass can find work with no external event: the
    /// next retirement or deletion deadline and the earliest live pin deadline. `None`
    /// when nothing is scheduled. Pin release, pin commit and archive commit are
    /// external events that wake the worker.
    pub next_due: Option<u64>,
}

impl Store {
    /// Perform one finite expiry pass; never collect the resident corpus.
    /// `max_records` bounds total record/artifact retirement units in this operation.
    /// Current protection is captured under the writer lock on every pass.
    /// Hard failures commit nothing and do not advance any cursor.
    ///
    /// The pass reads the receipt-ordered time index from its cursors and stops at the
    /// first key whose deadline has not arrived, so its cost follows the due work, never
    /// the size of the store. Request rows have no time index; they are scanned once
    /// per [`EXPIRY_CEILING_MS`].
    pub fn expire_bounded(&self, now: u64, limits: EvictionLimits) -> Result<ExpiryReport, Error> {
        limits.validate()?;
        self.import_reclaim(now)?;
        self.lock()?.expire_bounded(now, limits)
    }

    /// One finite expiry pass that runs while free space is below the floor (#267).
    ///
    /// Its commits are reclamation: they run under the maintenance reserve instead of the
    /// floor, like the pressure pass's retirement batch, and are the same class and size
    /// (`limits` bound them; pass the pressure pass's). A free-space reading below the reserve
    /// refuses before anything is read, with the reserve's `STORAGE_PRESSURE` error. Import
    /// reclamation is not run: it is the other duty skipped below the floor.
    ///
    /// Whatever happens, the floor episode is told ageing out had its turn, so early
    /// retirement of archived evidence may follow. Retiring any unit opens an in-memory
    /// pressure wait (no durable marker, no format change) so the pass measures the freed
    /// bytes and forces a compaction when they do not come.
    pub fn expire_below_floor(
        &self,
        now: u64,
        limits: EvictionLimits,
    ) -> Result<ExpiryReport, Error> {
        limits.validate()?;
        let writer = self.lock()?;
        let outcome: Result<(u64, ExpiryReport), Error> = (|| {
            if let Some(shortfall) = writer
                .space
                .shortfall_below(writer.config.maintenance_reserve)?
            {
                return Err(Error::new("STORAGE_PRESSURE", shortfall));
            }
            let linked = writer.linked_physical_bytes()?;
            let report = writer.reclaiming(|| writer.expire_bounded(now, limits))?;
            Ok((linked, report))
        })();
        let retired_from = match &outcome {
            Ok((linked, report))
                if report.records != 0 || report.artifacts != 0 || report.payload_refs != 0 =>
            {
                Some(*linked)
            }
            _ => None,
        };
        let told = writer.expiry_ran_below_floor(now, retired_from);
        let (_, report) = outcome?;
        told?;
        Ok(report)
    }

    /// Default bounded expiry pass; returns dependency retirement units, not freed bytes.
    /// Callers requiring complete traversal inspect `expire_bounded` continuation.
    pub fn expire(&self, now: u64) -> Result<u64, Error> {
        let report = self.expire_bounded(now, EvictionLimits::default())?;
        Ok(report.records + report.artifacts)
    }

    /// Rows expiry has read since the store opened, counted where expiry reads each row.
    /// Acceptance tests only.
    #[cfg(feature = "test-support")]
    pub fn expiry_reads_for_tests(&self) -> Result<u64, Error> {
        Ok(self.lock()?.expiry.borrow().reads)
    }
}

/// What a walk does after one entry.
enum Step {
    Next,
    /// A pin holds the entry: skipped, not retired.
    Held,
    Stop(ExpiryFrontier),
}

struct Walked {
    position: Option<Position>,
    frontier: Option<ExpiryFrontier>,
    /// Deadline of the first entry that was not yet due.
    due: Option<u64>,
}

/// One bounded pass: the shared budgets, batch and per-pass parent state of both walks.
struct Pass<'a> {
    writer: &'a Writer,
    now: u64,
    limits: EvictionLimits,
    protected: &'a BTreeSet<String>,
    catalog: usize,
    report: ExpiryReport,
    retirement: Option<retire::Retirement<'a>>,
    parents: BTreeMap<String, Envelope>,
    /// Parents whose graph and manifest are fully retired in this pass.
    done: BTreeSet<String>,
    deleted_parents: BTreeSet<String>,
    held: Option<Vec<u8>>,
    /// Earliest own deadline of an entry deferred past its index position.
    deferred: Option<u64>,
    reads: u64,
}

fn staged<'b, 'a>(
    slot: &'b mut Option<retire::Retirement<'a>>,
    writer: &'a Writer,
) -> Result<&'b mut retire::Retirement<'a>, Error> {
    let current = match slot.take() {
        Some(current) => current,
        None => retire::Retirement::new(writer)?,
    };
    Ok(slot.insert(current))
}

fn received_at(position: &[u8]) -> Result<u64, Error> {
    let digits = position
        .get(..20)
        .filter(|_| position.get(20) == Some(&b':'));
    digits
        .and_then(|digits| std::str::from_utf8(digits).ok())
        .and_then(|digits| digits.parse().ok())
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "expiry time index key lacks a receipt instant",
            )
        })
}

impl Pass<'_> {
    fn units(&self) -> u64 {
        self.report.records + self.report.artifacts
    }

    fn hold(&mut self, position: &[u8]) {
        if self.held.as_deref().is_none_or(|held| position < held) {
            self.held = Some(position.to_vec());
        }
    }

    /// Walk the merged receipt order from `start` while entries are due. The retirement
    /// walk (`delete` false) retires at the deadline; the deletion walk deletes one day
    /// later, retiring first whatever the retirement walk has not reached.
    fn walk(&mut self, start: Option<&Position>, delete: bool) -> Result<Walked, Error> {
        let writer = self.writer;
        let mut walked = Walked {
            position: start.cloned(),
            frontier: None,
            due: None,
        };
        for entry in writer.eviction_entries(start.map(|(key, _)| key.as_slice())) {
            let (key, value) = entry?;
            self.reads += 1;
            let recordless = key.starts_with(recordless::PREFIX);
            let at = &key[if recordless {
                recordless::PREFIX.len()
            } else {
                b"time:".len()
            }..];
            if start.is_some_and(|(previous, inclusive)| !inclusive && at == previous.as_slice()) {
                continue;
            }
            let deadline = received_at(at)?.checked_add(RETENTION_MS).ok_or_else(|| {
                Error::new("STORAGE_FAILURE", "expiry time index deadline overflows")
            })?;
            let due = if delete {
                deadline.saturating_add(DAY)
            } else {
                deadline
            };
            if due > self.now {
                walked.due = Some(due);
                break;
            }
            if self.report.visited == self.limits.scan_budget {
                walked.frontier = Some(ExpiryFrontier::ScanBudget);
                walked.position = Some((at.to_vec(), true));
                break;
            }
            let step = if recordless {
                self.recordless(&key, &value, deadline, delete)?
            } else {
                self.record(&key, &value, deadline, delete)?
            };
            match step {
                Step::Next => walked.position = Some((at.to_vec(), false)),
                Step::Held => {
                    self.hold(at);
                    walked.position = Some((at.to_vec(), false));
                }
                Step::Stop(frontier) => {
                    walked.frontier = Some(frontier);
                    walked.position = Some((at.to_vec(), true));
                    break;
                }
            }
        }
        Ok(walked)
    }

    /// A catalog admission refusal stops the pass after a complete safe prefix; it is a hard
    /// failure only when not one entry fit.
    fn refuse(&self, error: Error) -> Result<ExpiryFrontier, Error> {
        if error.code == "RESOURCE_LIMIT" && self.report.visited != 0 {
            Ok(ExpiryFrontier::CatalogBudget)
        } else {
            Err(error)
        }
    }

    fn recordless(
        &mut self,
        key: &[u8],
        value: &[u8],
        deadline: u64,
        delete: bool,
    ) -> Result<Step, Error> {
        self.reads += 1;
        let artifact = match self
            .writer
            .indexed_recordless(key, value, &mut self.catalog)
        {
            Ok(artifact) => artifact,
            Err(error) => return self.refuse(error).map(Step::Stop),
        };
        self.report.visited += 1;
        let Some((delete, deferred)) = self.own_deadline(artifact.expires_at, deadline, delete)
        else {
            return Ok(Step::Held);
        };
        if self.protected.contains(&artifact.id) {
            return Ok(Step::Held);
        }
        if !self.writer.archived(artifact.received_at, &artifact.id) {
            self.report.archive_waiting += 1;
            return Ok(Step::Stop(ExpiryFrontier::ArchiveLag));
        }
        if self.units() == self.limits.max_records as u64 {
            return Ok(Step::Stop(ExpiryFrontier::UnitLimit));
        }
        let retirement = staged(&mut self.retirement, self.writer)?;
        if retirement.artifact_graph(&artifact.id, artifact.received_at)? {
            self.report.artifacts += 1;
        }
        if delete {
            retirement.batch.delete(&artifact.id);
            retirement.batch.delete(key);
            manifest_retire::remove(&mut retirement.batch, &artifact.id);
            self.report.deleted += 1;
        }
        Ok(if deferred { Step::Held } else { Step::Next })
    }

    fn record(
        &mut self,
        key: &[u8],
        value: &[u8],
        deadline: u64,
        delete: bool,
    ) -> Result<Step, Error> {
        let writer = self.writer;
        self.reads += 1;
        let mut record = match writer.time_record(key, value, &mut self.catalog) {
            Ok(record) => record,
            Err(error) => return self.refuse(error).map(Step::Stop),
        };
        self.report.visited += 1;
        let Some((delete, deferred)) = self.own_deadline(record.expires_at, deadline, delete)
        else {
            return Ok(Step::Held);
        };
        // A pin on the artifact holds all its records; a pin on the record alone holds
        // only the record, and its parent artifact retires and is deleted regardless.
        let held = writer.live_record(&record, self.now, self.protected);
        if held && self.protected.contains(&record.artifact_id) {
            return Ok(Step::Held);
        }
        // Archiving is a prefix of the receipt order, and a record's parent artifact
        // sorts before it, so this one test also covers the parent.
        if !writer.archived(record.received_at, &record.id) {
            self.report.archive_waiting += 1;
            return Ok(Step::Stop(ExpiryFrontier::ArchiveLag));
        }
        let parent = record.artifact_id.clone();
        if !self.parents.contains_key(&parent) {
            self.reads += 1;
            let take = self.limits.max_payload_refs - self.report.payload_refs;
            let loaded = writer.read_catalog(&parent, |bytes| {
                Envelope::read(writer, &parent, bytes, take, &mut self.catalog)
            });
            match loaded {
                Ok(envelope) => {
                    self.parents.insert(parent.clone(), envelope);
                }
                // The deletion walk removes the parent with the first due sibling, whose
                // position deadline a day on is when this one may find it gone. Any other
                // absence is an anomaly: counted, and the record proceeds without a parent.
                Err(error) if error.code == "NOT_FOUND" => {
                    if deadline.saturating_add(DAY) > self.now {
                        self.report.anomalies += 1;
                    }
                }
                Err(error) => {
                    // This record is not yet processed: it must not count as progress, so a
                    // parent that cannot fit a fresh catalog budget fails loudly.
                    self.report.visited -= 1;
                    let frontier = self.refuse(error);
                    self.report.visited += 1;
                    return frontier.map(Step::Stop);
                }
            }
        }
        if !held && !record.evicted && !record.retired {
            if self.units() == self.limits.max_records as u64 {
                return Ok(Step::Stop(ExpiryFrontier::UnitLimit));
            }
            if record.chunks.len() > self.limits.max_payload_refs {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "complete expired record exceeds payload ownership admission; increase the pass limit",
                ));
            }
            if record.chunks.len() > self.limits.max_payload_refs - self.report.payload_refs {
                return Ok(Step::Stop(ExpiryFrontier::PayloadBudget));
            }
            self.report.payload_refs += record.chunks.len();
            let retirement = staged(&mut self.retirement, writer)?;
            if retirement.record(&mut record, 1)? {
                self.report.records += 1;
            }
            // The marker drops the record from status `live_records` in this batch, a day
            // before the entry itself is deleted (#146).
            record.retired = true;
            put(&mut retirement.batch, &record.id, &record);
        }
        // A parent whose own deadline is later than its records' is not touched early.
        let blocked = self.parent_blocked(&parent, deadline, delete);
        if !blocked.0
            && let Some(stop) = self.parent(&parent)?
        {
            return Ok(Step::Stop(stop));
        }
        let blocked = blocked.0 || blocked.1;
        if delete {
            let retirement = staged(&mut self.retirement, writer)?;
            if !held && !blocked {
                retirement.batch.delete(&record.id);
                retirement
                    .batch
                    .delete(format!("time:{:020}:{}", record.received_at, record.id));
                self.report.deleted += 1;
            }
            if let Some(envelope) = self.parents.get(&parent)
                && !blocked
                && self.deleted_parents.insert(parent.clone())
            {
                retirement.batch.delete(&envelope.id);
                manifest_retire::remove(&mut retirement.batch, &envelope.id);
                self.report.deleted += 1;
            }
        }
        Ok(if held || deferred || blocked {
            Step::Held
        } else {
            Step::Next
        })
    }

    /// Honour an entry's own deadline over the one its index position implies. Every
    /// writer sets `expires_at = received_at + RETENTION_MS`, but a row that differs (an
    /// older store, an edit) must never stop the pass: an earlier own deadline retires
    /// when the walk reaches it, and a later one defers the entry, counted in
    /// `ExpiryReport.anomalies` and scheduled in `next_due`, until its own deadline.
    /// Returns `None` when the entry is not yet due by its own deadline; else whether its
    /// deletion is due now, and whether it was deferred to a later pass for deletion.
    /// A parent artifact follows the same rule through `parent_blocked`. The only
    /// data-dependent hard failure left is a dangling or mismatched time-index key, a
    /// broken index and not one odd row.
    fn own_deadline(
        &mut self,
        expires_at: u64,
        position_deadline: u64,
        delete: bool,
    ) -> Option<(bool, bool)> {
        if expires_at != position_deadline {
            self.report.anomalies += 1;
        }
        if expires_at > self.now {
            self.defer(expires_at);
            return None;
        }
        let deletable = delete && expires_at.saturating_add(DAY) <= self.now;
        let deferred = delete && !deletable;
        if deferred {
            self.defer(expires_at.saturating_add(DAY));
        }
        Some((deletable, deferred))
    }

    fn defer(&mut self, until: u64) {
        self.deferred = Some(self.deferred.map_or(until, |at| at.min(until)));
    }

    /// Whether the parent's own deadline keeps it, and with it its records, from retiring
    /// (first) or from deletion (second) at this record's position. Defers to that deadline.
    fn parent_blocked(&mut self, id: &str, position_deadline: u64, delete: bool) -> (bool, bool) {
        let Some(expires_at) = self.parents.get(id).map(|envelope| envelope.expires_at) else {
            return (false, false);
        };
        if expires_at != position_deadline {
            self.report.anomalies += 1;
        }
        if expires_at > self.now {
            self.defer(expires_at);
            return (true, false);
        }
        let later = expires_at.saturating_add(DAY);
        if delete && later > self.now {
            self.defer(later);
            return (false, true);
        }
        (false, false)
    }

    /// Retire one parent artifact's graph and manifest slices, once per pass.
    fn parent(&mut self, id: &str) -> Result<Option<ExpiryFrontier>, Error> {
        if self.done.contains(id) {
            return Ok(None);
        }
        let Some(envelope) = self.parents.get_mut(id) else {
            return Ok(None);
        };
        if self.report.records + self.report.artifacts == self.limits.max_records as u64 {
            return Ok(Some(ExpiryFrontier::UnitLimit));
        }
        let retirement = staged(&mut self.retirement, self.writer)?;
        let graph = retirement.artifact_graph(&envelope.id, envelope.received_at)?;
        if graph {
            self.report.artifacts += 1;
        }
        if envelope.pending() != 0 {
            let remaining = self.limits.max_payload_refs - self.report.payload_refs;
            if remaining == 0 {
                return Ok(Some(ExpiryFrontier::PayloadBudget));
            }
            self.report.payload_refs += retirement.artifact(envelope, remaining)?;
            if !graph {
                self.report.artifacts += 1;
            }
            if envelope.pending() != 0 {
                return Ok(Some(ExpiryFrontier::PayloadBudget));
            }
        }
        self.done.insert(id.to_owned());
        Ok(None)
    }

    /// Delete retry rows whose window has passed; they have no time index. Returns the scan
    /// state and the budget frontier that interrupted it, if any.
    fn requests(
        &mut self,
        after: Option<Vec<u8>>,
    ) -> Result<(RequestScan, Option<ExpiryFrontier>), Error> {
        let writer = self.writer;
        let prefix = b"request:".as_slice();
        let mut last = after.clone();
        let from = after.as_deref().unwrap_or(prefix);
        for entry in writer
            .db
            .iterator(IteratorMode::From(from, Direction::Forward))
        {
            let (key, bytes) = entry?;
            if !key.starts_with(prefix) {
                break;
            }
            if after.as_deref() == Some(&*key) {
                continue;
            }
            if self.report.visited == self.limits.scan_budget {
                return Ok((RequestScan::Walking(last), Some(ExpiryFrontier::ScanBudget)));
            }
            if key.len() > 1024 {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "expiry catalog key exceeds its bounded identity contract",
                ));
            }
            self.reads += 1;
            let (expires_at, managed) =
                match manifest_retire::request_expiry(writer, &key, &bytes, &mut self.catalog) {
                    Ok(value) => value,
                    Err(error) => {
                        let frontier = self.refuse(error)?;
                        return Ok((RequestScan::Walking(last), Some(frontier)));
                    }
                };
            self.report.visited += 1;
            if !managed && expires_at <= self.now {
                staged(&mut self.retirement, writer)?.batch.delete(&key);
                self.report.deleted += 1;
            }
            last = Some(key.to_vec());
        }
        Ok((RequestScan::Waiting(Some(self.now)), None))
    }
}

impl Writer {
    fn expire_bounded(&self, now: u64, limits: EvictionLimits) -> Result<ExpiryReport, Error> {
        let mut pin_catalog = limits.max_pin_catalog_bytes;
        let mut pin_visits = 0;
        let mut sweep = PinSweep::default();
        // Pin capture has its own budget, so pins never starve the evidence walks, and
        // expired pin rows are deleted here, in their own batch, before any walk can
        // stop the pass.
        let protected = self.capture_pins(
            now,
            |_| true,
            || {
                pin_visits += 1;
                if pin_visits > limits.scan_budget {
                    Err(Error::new(
                        "RESOURCE_LIMIT",
                        "pin capture exceeds expiry scan budget",
                    ))
                } else {
                    Ok(())
                }
            },
            None,
            Some(&mut pin_catalog),
            Some(&mut sweep),
        )?;
        let mut deleted_pins = 0;
        if !sweep.expired.is_empty() {
            let mut batch = rocksdb::WriteBatch::default();
            for key in &sweep.expired {
                batch.delete(key);
            }
            self.commit(batch)?;
            deleted_pins = sweep.expired.len();
        }
        let mut saved = self.expiry.borrow_mut();
        let mut local = saved.clone();
        if local.deferred.is_some_and(|at| at <= now) {
            local.deferred = None;
            local.rewind();
        }
        if local
            .protected
            .iter()
            .any(|member| !protected.contains(member))
        {
            local.rewind();
        }
        let mut pass = Pass {
            writer: self,
            now,
            limits,
            protected: &protected,
            catalog: limits.max_catalog_bytes,
            report: ExpiryReport {
                deleted: deleted_pins,
                ..ExpiryReport::default()
            },
            retirement: None,
            parents: BTreeMap::new(),
            done: BTreeSet::new(),
            deleted_parents: BTreeSet::new(),
            held: local.held.take(),
            deferred: local.deferred.take(),
            reads: pin_visits as u64,
        };
        let deleted = pass.walk(local.delete.as_ref(), true)?;
        local.delete = deleted.position;
        let mut frontier = deleted.frontier;
        let mut due = deleted.due;
        if frontier.is_none() {
            // The deletion walk retired whatever it passed.
            let start = later(local.retire.take(), local.delete.clone());
            let retired = pass.walk(start.as_ref(), false)?;
            local.retire = retired.position;
            frontier = retired.frontier;
            due = [due, retired.due].into_iter().flatten().min();
        } else {
            local.retire = later(local.retire.take(), local.delete.clone());
        }
        if matches!(frontier, None | Some(ExpiryFrontier::ArchiveLag)) {
            let scan = match &local.requests {
                RequestScan::Waiting(Some(at)) if now.abs_diff(*at) < EXPIRY_CEILING_MS => None,
                RequestScan::Waiting(_) => Some(None),
                RequestScan::Walking(after) => Some(after.clone()),
            };
            if let Some(after) = scan {
                let (state, stopped) = pass.requests(after)?;
                local.requests = state;
                frontier = stopped.or(frontier);
            }
        }
        let Pass {
            retirement,
            mut report,
            held,
            deferred,
            reads,
            ..
        } = pass;
        let batch = match retirement {
            Some(retirement) => retirement.finish()?,
            None => rocksdb::WriteBatch::default(),
        };
        // The shared writer appends the bounded format and feature fence.
        if batch.size_in_bytes().saturating_add(256) > limits.max_batch_bytes {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "expiry batch exceeds native write admission",
            ));
        }
        if !batch.is_empty() {
            self.commit(batch)?;
        }
        report.frontier = frontier.unwrap_or_default();
        report.continuation = frontier.is_some_and(|stopped| stopped != ExpiryFrontier::ArchiveLag);
        report.next_due = if report.continuation {
            None
        } else {
            [due, sweep.next_expiry, deferred]
                .into_iter()
                .flatten()
                .min()
        };
        local.held = held;
        local.deferred = deferred;
        local.protected = protected;
        local.reads += reads;
        *saved = local;
        Ok(report)
    }
}
