// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Durable completed-input ownership, separate from ordinary retry receipts.

use super::{
    Artifact, Catalog, Error, RETRY_WINDOW, Store, Writer, blake3, hex, integer, json, parse, put,
    read_field, validate_ingest_labels,
};
use purrdf_lex::json::{Object, Value};
use rocksdb::WriteBatch;
use std::ops::RangeInclusive;

/// Maximum simultaneously active source-ownership journals in one store.
pub const MAX_CONSUMPTION_JOURNALS: usize = 16;
const JOURNAL_BYTES: usize = 8192;
pub(super) const FORMAT: &[u8] = b"sparrow.store/34";
pub(super) const TAIL_FORMAT: &[u8] = b"sparrow.store/37";

/// Directory identities anchoring the source namespace across recovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumptionDirectories {
    /// Watch-root filesystem device.
    pub root_device: u64,
    /// Watch-root inode.
    pub root_inode: u64,
    /// Original source-parent filesystem device.
    pub parent_device: u64,
    /// Original source-parent inode.
    pub parent_inode: u64,
}

impl Catalog for ConsumptionDirectories {
    fn encode(&self) -> Value {
        Object::new()
            .with("root_device", self.root_device.to_string())
            .with("root_inode", self.root_inode.to_string())
            .with("parent_device", self.parent_device.to_string())
            .with("parent_inode", self.parent_inode.to_string())
            .into()
    }
    fn decode(value: &Value) -> Result<Self, Error> {
        Ok(Self {
            root_device: integer(value, "root_device")?,
            root_inode: integer(value, "root_inode")?,
            parent_device: integer(value, "parent_device")?,
            parent_inode: integer(value, "parent_inode")?,
        })
    }
}

/// One observed completed-file generation. The filesystem coordinator supplies
/// these observations from its already opened regular-file descriptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumptionGeneration {
    /// Filesystem device identity.
    pub device: u64,
    /// Opened inode identity.
    pub inode: u64,
    /// Complete source length.
    pub byte_length: u64,
    /// Modification time seconds and nanoseconds.
    pub modified: (i64, u32),
    /// Inode-change time seconds and nanoseconds.
    pub changed: (i64, u32),
}

impl Catalog for ConsumptionGeneration {
    fn encode(&self) -> Value {
        Object::new()
            .with("device", self.device.to_string())
            .with("inode", self.inode.to_string())
            .with("byte_length", self.byte_length.to_string())
            .with("modified_seconds", self.modified.0.to_string())
            .with("modified_nanos", u64::from(self.modified.1))
            .with("changed_seconds", self.changed.0.to_string())
            .with("changed_nanos", u64::from(self.changed.1))
            .into()
    }

    fn decode(value: &Value) -> Result<Self, Error> {
        let generation = Self {
            device: integer(value, "device")?,
            inode: integer(value, "inode")?,
            byte_length: integer(value, "byte_length")?,
            modified: (
                read_field::<String>(value, "modified_seconds")?
                    .parse()
                    .map_err(|_| Error::new("STORAGE_FAILURE", "invalid modification seconds"))?,
                read_field(value, "modified_nanos")?,
            ),
            changed: (
                read_field::<String>(value, "changed_seconds")?
                    .parse()
                    .map_err(|_| Error::new("STORAGE_FAILURE", "invalid change seconds"))?,
                read_field(value, "changed_nanos")?,
            ),
        };
        generation
            .validate()
            .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
        Ok(generation)
    }
}

impl ConsumptionGeneration {
    fn validate(&self) -> Result<(), Error> {
        if self.modified.1 >= 1_000_000_000 || self.changed.1 >= 1_000_000_000 {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "file clocks require nanoseconds below one billion",
            ));
        }
        Ok(())
    }
}

/// Recovery journal for one source generation. Presence does not pin evidence,
/// prove filesystem readiness, or authorize a path unlink.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumptionJournal {
    /// The reserved ordinary import identity.
    pub import_id: String,
    /// Stable request identity in the reserved consumption namespace.
    pub request_id: String,
    /// Configured watch identity.
    pub watch: String,
    /// Original relative Unix path bytes; no UTF-8 filename restriction.
    pub relative_path: Vec<u8>,
    /// Logical intake source.
    pub source: String,
    /// Explicit source interpretation.
    pub format: String,
    /// Observation before atomic filesystem claim.
    pub observed: ConsumptionGeneration,
    /// Namespace identities captured before claiming.
    pub directories: ConsumptionDirectories,
    /// Observation after claim and directory synchronization.
    pub claimed: Option<ConsumptionGeneration>,
    /// Original atomically published artifact, independent of retry expiry.
    pub published_artifact: Option<String>,
    /// Original terminal checkpoint for source retirement without reimporting bytes.
    pub tail: Option<super::tail::TailGeneration>,
    /// Durable acknowledgement of the original checkpoint under this claim.
    pub tail_published: bool,
    /// Explicit directory-synced removal confirmation clock.
    pub removed_at: Option<u64>,
    /// Publication attempts and the last reason the claim stayed pending. Not part of the
    /// claim's identity.
    pub history: ClaimHistory,
}

/// Room each new journal keeps for its history to grow: two clocks and a pending reason.
const HISTORY_ROOM: usize = 448;
/// Most bytes a recorded pending reason occupies once JSON-encoded.
const REASON_BYTES: usize = 320;
/// Room a journal keeps for the removal clock and the completed-receipt wrapper.
const COMPLETION_ROOM: usize = 128;
/// Room an unclaimed journal keeps for its claimed observation.
const CLAIMED_ROOM: usize = 384;

/// Durable publication history of one claim, kept in its journal through completion or
/// retirement. A journal reserved before schema 71 has no first-seen time and counts only
/// attempts made since.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClaimHistory {
    /// Unix milliseconds of the reservation.
    pub first_seen: Option<u64>,
    /// Publication attempts: each one that left the claim pending, and the one completing it.
    pub attempts: u32,
    /// Unix milliseconds of the latest attempt.
    pub last_attempt_at: Option<u64>,
    /// Unix milliseconds of the latest attempt that left the claim pending.
    pub last_pending_at: Option<u64>,
    /// Why that attempt left the claim pending.
    pub last_pending_reason: Option<String>,
}

fn optional_clock(value: &Value, name: &'static str) -> Result<Option<u64>, Error> {
    if value.get(name).is_none() {
        return Ok(None);
    }
    read_field::<String>(value, name)?
        .parse()
        .map(Some)
        .map_err(|_| Error::new("STORAGE_FAILURE", format!("invalid claim {name}")))
}

impl Catalog for ClaimHistory {
    fn encode(&self) -> Value {
        let mut out = Object::new().with("attempts", u64::from(self.attempts));
        for (name, clock) in [
            ("first_seen", self.first_seen),
            ("last_attempt_at", self.last_attempt_at),
            ("last_pending_at", self.last_pending_at),
        ] {
            if let Some(clock) = clock {
                out = out.with(name, clock.to_string());
            }
        }
        if let Some(reason) = &self.last_pending_reason {
            out = out.with("last_pending_reason", reason.clone());
        }
        out.into()
    }

    fn decode(value: &Value) -> Result<Self, Error> {
        Ok(Self {
            first_seen: optional_clock(value, "first_seen")?,
            attempts: read_field(value, "attempts")?,
            last_attempt_at: optional_clock(value, "last_attempt_at")?,
            last_pending_at: optional_clock(value, "last_pending_at")?,
            last_pending_reason: match value.get("last_pending_reason") {
                None => None,
                Some(_) => Some(read_field(value, "last_pending_reason")?),
            },
        })
    }
}

/// The longest prefix of `reason` whose JSON encoding fits in `budget` bytes.
fn fit_reason(reason: &str, budget: usize) -> String {
    let mut used = 2;
    let mut fitted = String::new();
    for c in reason.chars() {
        let mut one = [0; 4];
        let bytes = json::write_compact(&Value::from(&*c.encode_utf8(&mut one))).len() - 2;
        if used + bytes > budget {
            break;
        }
        used += bytes;
        fitted.push(c);
    }
    fitted
}

impl ConsumptionJournal {
    /// Record one attempt that left this active claim pending, at `now`, because of `reason`.
    /// The reason is shortened to the journal's remaining room; a journal with no room left
    /// even for the clocks refuses, so the record never grows past what readers accept.
    fn record_pending(&mut self, now: u64, reason: &str) -> Result<(), Error> {
        let history = &mut self.history;
        history.attempts = history.attempts.saturating_add(1);
        history.last_attempt_at = Some(now);
        history.last_pending_at = Some(now);
        history.last_pending_reason = Some(String::new());
        let room = COMPLETION_ROOM
            + if self.claimed.is_none() {
                CLAIMED_ROOM
            } else {
                0
            };
        let used = json::write_compact(&self.encode()).len() + room;
        let budget = JOURNAL_BYTES
            .checked_sub(used)
            .ok_or_else(|| {
                Error::new(
                    "RESOURCE_LIMIT",
                    "the claim journal has no room left to record a pending attempt",
                )
            })?
            .min(REASON_BYTES - 2)
            + 2;
        self.history.last_pending_reason = Some(fit_reason(reason, budget));
        Ok(())
    }
}

fn key(id: &str) -> String {
    format!("consumption:{id}")
}

/// Most claims a store keeps as retired unrecoverable. Each is an operator's audit record and
/// is never removed, so the bound is a hard refusal rather than an eviction.
pub const MAX_RETIRED_CONSUMPTION_CLAIMS: usize = 64;
const RETIRED_PREFIX: &str = "consumption-retired:";
const RETIREMENT_TEXT: usize = 512;

fn retired_key(id: &str) -> String {
    format!("{RETIRED_PREFIX}{id}")
}

/// Whether claim `id` is retired unrecoverable; its consumption import is then reclaimable.
pub(super) fn retired(writer: &Writer, id: &str) -> Result<bool, Error> {
    Ok(writer.db.get(retired_key(id))?.is_some())
}

/// An operator's durable retirement of a claim that can never complete: claimed, its private
/// source missing, and never published. The journal leaves the active recovery set, so it no
/// longer counts as a pending claim, and is kept with this record as retired unrecoverable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumptionRetirement {
    /// Unix milliseconds of the retirement.
    pub retired_at: u64,
    /// The authenticated administrator, as the daemon identified the connection.
    pub caller: String,
    /// The operator's reason.
    pub reason: String,
}

/// A retired unrecoverable claim: its original journal and the retirement record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetiredConsumption {
    /// The journal as it stood when retired: claimed, unpublished, never removed.
    pub journal: ConsumptionJournal,
    /// Who retired it, when and why.
    pub retirement: ConsumptionRetirement,
}

impl Catalog for RetiredConsumption {
    fn encode(&self) -> Value {
        Object::new()
            .with("journal", self.journal.encode())
            .with("retired_at", self.retirement.retired_at.to_string())
            .with("caller", self.retirement.caller.clone())
            .with("reason", self.retirement.reason.clone())
            .into()
    }

    fn decode(value: &Value) -> Result<Self, Error> {
        let retired = Self {
            journal: ConsumptionJournal::decode(super::field(value, "journal")?)?,
            retirement: ConsumptionRetirement {
                retired_at: integer(value, "retired_at")?,
                caller: read_field(value, "caller")?,
                reason: read_field(value, "reason")?,
            },
        };
        if !retired.unrecoverable() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "retired consumption claim is not an unpublished claim",
            ));
        }
        Ok(retired)
    }
}

impl RetiredConsumption {
    fn unrecoverable(&self) -> bool {
        self.journal.claimed.is_some()
            && self.journal.published_id().is_none()
            && self.journal.removed_at.is_none()
    }
}

fn completed_key(id: &str) -> String {
    format!("request:consumption-complete:{id}")
}

impl Catalog for ConsumptionJournal {
    fn encode(&self) -> Value {
        Object::new()
            .with("import_id", self.import_id.clone())
            .with("request_id", self.request_id.clone())
            .with("watch", self.watch.clone())
            .with("relative_path", hex::encode(&self.relative_path))
            .with("source", self.source.clone())
            .with("format", self.format.clone())
            .with("observed", self.observed.encode())
            .with("directories", self.directories.encode())
            .with(
                "claimed",
                self.claimed
                    .as_ref()
                    .map(Catalog::encode)
                    .unwrap_or(Value::Null),
            )
            .with("published_artifact", self.published_artifact.clone())
            .with("tail", self.tail.as_ref().map(Catalog::encode))
            .with("tail_published", self.tail_published)
            .with("removed_at", self.removed_at.map(|v| v.to_string()))
            .with("history", self.history.encode())
            .into()
    }

    fn decode(value: &Value) -> Result<Self, Error> {
        let journal = Self {
            import_id: read_field(value, "import_id")?,
            request_id: read_field(value, "request_id")?,
            watch: read_field(value, "watch")?,
            relative_path: hex::decode_canonical(&read_field::<String>(value, "relative_path")?)
                .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?,
            source: read_field(value, "source")?,
            format: read_field(value, "format")?,
            observed: ConsumptionGeneration::decode(super::field(value, "observed")?)?,
            directories: ConsumptionDirectories::decode(super::field(value, "directories")?)?,
            claimed: match super::field(value, "claimed")? {
                Value::Null => None,
                value => Some(ConsumptionGeneration::decode(value)?),
            },
            published_artifact: read_field(value, "published_artifact")?,
            tail: value
                .get("tail")
                .filter(|value| !value.is_null())
                .map(super::tail::TailGeneration::decode)
                .transpose()?,
            tail_published: match value.get("tail_published") {
                Some(_) => read_field(value, "tail_published")?,
                None if value.get("tail").is_none_or(Value::is_null) => false,
                None => {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "tail claim publication state missing",
                    ));
                }
            },
            removed_at: read_field::<Option<String>>(value, "removed_at")?
                .map(|v| {
                    v.parse()
                        .map_err(|_| Error::new("STORAGE_FAILURE", "invalid removal clock"))
                })
                .transpose()?,
            // A journal written before schema 71 has no history.
            history: match value.get("history") {
                None => ClaimHistory::default(),
                Some(history) => ClaimHistory::decode(history)?,
            },
        };
        journal
            .validate()
            .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
        if journal.request_id != journal.reserved_request()
            || journal.import_id != super::import::import_id(&journal.request_id)
            || (journal.published_id().is_some() && journal.claimed.is_none())
            || (journal.removed_at.is_some() && journal.published_id().is_none())
            || journal
                .published_artifact
                .as_ref()
                .is_some_and(|id| !id.starts_with("artifact:") || id.len() > 128)
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "inconsistent consumption journal",
            ));
        }
        Ok(journal)
    }
}

impl ConsumptionJournal {
    pub(super) fn reservation(
        watch: &str,
        relative_path: &[u8],
        source: &str,
        format: &str,
        observed: ConsumptionGeneration,
        directories: ConsumptionDirectories,
        now: u64,
    ) -> Result<Self, Error> {
        let mut journal = Self {
            import_id: String::new(),
            request_id: "reservation".into(),
            watch: watch.into(),
            relative_path: relative_path.into(),
            source: source.into(),
            format: format.into(),
            observed,
            directories,
            claimed: None,
            published_artifact: None,
            tail: None,
            tail_published: false,
            removed_at: None,
            history: ClaimHistory {
                first_seen: Some(now),
                ..ClaimHistory::default()
            },
        };
        journal.validate()?;
        journal.request_id = journal.reserved_request();
        journal.import_id = super::import::import_id(&journal.request_id);
        Ok(journal)
    }

    fn validate(&self) -> Result<(), Error> {
        validate_source(&self.watch, &self.relative_path, &self.observed)?;
        if self.directories.root_device != self.directories.parent_device
            || self.directories.root_device != self.observed.device
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "consumption requires source and anchored directories on the same filesystem",
            ));
        }
        validate_ingest_labels(&self.request_id, &self.source, &self.format)?;
        if self.tail_published && self.tail.is_none() {
            return Err(Error::new(
                "CONFLICT",
                "tail publication requires its original checkpoint",
            ));
        }
        if let Some(tail) = &self.tail {
            let declaration = &tail.declaration;
            if !tail.complete
                || tail.gap.is_some()
                || tail.finish.as_ref().is_none_or(|finish| {
                    finish.finished_at.is_none() || finish.intent.observed != self.observed
                })
                || declaration.watch != self.watch
                || declaration.source != self.source
                || declaration.format != self.format
                || declaration.directories.root_device != self.directories.root_device
                || declaration.directories.root_inode != self.directories.root_inode
                || self.published_artifact.is_some()
            {
                return Err(Error::new(
                    "CONFLICT",
                    "tail claim requires the original complete gap-free producer checkpoint and rooted source observation",
                ));
            }
        }
        if let Some(claimed) = &self.claimed {
            claimed.validate()?;
            if claimed.device != self.observed.device
                || claimed.inode != self.observed.inode
                || claimed.byte_length != self.observed.byte_length
                || claimed.modified != self.observed.modified
            {
                return Err(Error::new(
                    "CONFLICT",
                    "claim observation changed the source generation or content clocks",
                ));
            }
        }
        Ok(())
    }

    fn reserved_request(&self) -> String {
        let mut parts = vec![
            self.watch.clone().into(),
            hex::encode(&self.relative_path).into(),
            self.observed.encode(),
            self.directories.encode(),
        ];
        if let Some(tail) = &self.tail {
            parts.push(tail.encode());
        }
        let identity = json::write_compact(&Value::array(parts));
        format!(
            "consume:{}",
            hex::encode(blake3::hash(identity.as_bytes()).as_bytes())
        )
    }

    /// Original durable publication identity; tail retirement does not mint an artifact.
    pub fn published_id(&self) -> Option<&str> {
        self.published_artifact.as_deref().or_else(|| {
            self.tail
                .as_ref()
                .filter(|_| self.tail_published)
                .map(|tail| tail.id.as_str())
        })
    }

    pub(super) fn bind_tail(&mut self, tail: super::tail::TailGeneration) -> Result<(), Error> {
        self.tail = Some(tail);
        self.validate()?;
        self.request_id = self.reserved_request();
        self.import_id = super::import::import_id(&self.request_id);
        Ok(())
    }
}

pub(super) fn validate_source(
    watch: &str,
    path: &[u8],
    observed: &ConsumptionGeneration,
) -> Result<(), Error> {
    observed.validate()?;
    validate_consumption_path(watch, path)
}

/// Validate a bounded registered watch and raw relative source path before probing it.
pub fn validate_consumption_path(watch: &str, path: &[u8]) -> Result<(), Error> {
    if watch.is_empty()
        || watch.len() > 128
        || path.len() > 2048
        || path.split(|b| *b == b'/').any(|part| {
            part.is_empty() || part == b"." || part == b".." || part == b".sparrow-inflight"
        })
        || path.contains(&0)
    {
        return Err(Error::new(
            "INVALID_ARGUMENT",
            "require a bounded watch and relative source path without traversal or claim-directory components",
        ));
    }
    Ok(())
}

impl Writer {
    pub(super) fn consumption(&self, id: &str) -> Result<Option<ConsumptionJournal>, Error> {
        self.consumption_admitted(id, None)
    }

    pub(super) fn consumption_admitted(
        &self,
        id: &str,
        remaining: Option<&mut usize>,
    ) -> Result<Option<ConsumptionJournal>, Error> {
        if id
            .strip_prefix("import:")
            .and_then(hex::decode_32_canonical)
            .is_none()
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "consumption recovery requires a canonical import identity",
            ));
        }
        let Some(bytes) = self.db.get(key(id))?.or(self.db.get(completed_key(id))?) else {
            return Ok(None);
        };
        if bytes.len() > JOURNAL_BYTES {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "consumption journal exceeds 8 KiB",
            ));
        }
        if let Some(remaining) = remaining {
            Self::charge_catalog_shape(remaining, &bytes)?;
        }
        let value = parse(&bytes)?;
        let journal = ConsumptionJournal::decode(value.get("journal").unwrap_or(&value))?;
        if journal.tail.is_some()
            && !matches!(
                super::writer_format::read(&self.db)?.as_deref(),
                Some(
                    TAIL_FORMAT
                        | super::pressure::WATCH_FORMAT
                        | super::receiver_stream::WATCH_FORMAT
                        | super::manifest_retire::FORMAT
                        | super::manifest_retire::WATCH_FORMAT
                )
            )
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "tail source ownership is missing its writer fence",
            ));
        }
        if journal.import_id != id {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "consumption journal key mismatch",
            ));
        }
        Ok(Some(journal))
    }

    /// The retired unrecoverable claim `id`, if any, charged against an admitted read.
    pub(super) fn consumption_retired_admitted(
        &self,
        id: &str,
        remaining: &mut usize,
    ) -> Result<Option<RetiredConsumption>, Error> {
        let Some(bytes) = self.db.get(retired_key(id))? else {
            return Ok(None);
        };
        Self::charge_catalog_shape(remaining, &bytes)?;
        let claim = RetiredConsumption::decode(&parse(&bytes)?)?;
        if claim.journal.import_id != id {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "retired consumption claim key mismatch",
            ));
        }
        Ok(Some(claim))
    }

    pub(super) fn require_consumption(
        &self,
        request: &str,
        source: &str,
        format: &str,
        length: u64,
    ) -> Result<bool, Error> {
        self.require_consumption_admitted(request, source, format, length, None)
    }

    pub(super) fn require_consumption_admitted(
        &self,
        request: &str,
        source: &str,
        format: &str,
        length: u64,
        remaining: Option<&mut usize>,
    ) -> Result<bool, Error> {
        if !request.starts_with("consume:") {
            return Ok(false);
        }
        let journal = self.consumption_admitted(&super::import::import_id(request), remaining)?.ok_or_else(|| Error::new("CONFLICT", "consumption request has no recovery journal; reserve the source generation first"))?;
        if journal.tail.is_some() {
            return Err(Error::new(
                "CONFLICT",
                "completed tail claims acknowledge original checkpoints; importing their bytes would duplicate records",
            ));
        }
        if journal.request_id != request
            || journal.source != source
            || journal.format != format
            || journal.observed.byte_length != length
        {
            return Err(Error::new(
                "CONFLICT",
                "consumption input contradicts the reserved source generation",
            ));
        }
        if journal.claimed.is_none() {
            return Err(Error::new(
                "CONFLICT",
                "synchronize and record the filesystem claim before importing",
            ));
        }
        Ok(true)
    }

    pub(super) fn publish_consumption(
        &self,
        batch: &mut WriteBatch,
        manifest: &super::import::ImportManifest,
        artifact: &Artifact,
    ) -> Result<(), Error> {
        if !manifest.consumption {
            return Ok(());
        }
        self.require_consumption(
            &manifest.request_id,
            &manifest.source,
            &manifest.format,
            manifest.byte_length,
        )?;
        let mut journal = self.consumption(&manifest.id)?.ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "managed import lost its consumption journal",
            )
        })?;
        if journal.published_artifact.is_some() || journal.removed_at.is_some() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "consumption publication already recorded",
            ));
        }
        journal.published_artifact = Some(artifact.id.clone());
        put(batch, &key(&manifest.id), &journal);
        Ok(())
    }
}

impl Store {
    /// Reserve one source generation before any rename. Repeated reservation
    /// returns its original journal; changed interpretation refuses atomically.
    /// `now` is the claim's first-seen time.
    #[expect(
        clippy::too_many_arguments,
        reason = "one reservation names its watch, path, interpretation, generation, anchors and time"
    )]
    pub fn consumption_reserve(
        &self,
        watch: &str,
        relative_path: &[u8],
        source: &str,
        format: &str,
        observed: ConsumptionGeneration,
        directories: ConsumptionDirectories,
        now: u64,
    ) -> Result<ConsumptionJournal, Error> {
        crate::format_instant(now)?;
        let journal = ConsumptionJournal::reservation(
            watch,
            relative_path,
            source,
            format,
            observed,
            directories,
            now,
        )?;
        self.reserve_consumption(journal, None)
    }

    pub(super) fn reserve_consumption(
        &self,
        journal: ConsumptionJournal,
        handoff: Option<(&super::handoff::ConsumptionHandoff, u64)>,
    ) -> Result<ConsumptionJournal, Error> {
        let writer = self.lock()?;
        if let Some((intent, now)) = handoff
            && let Some(previous) = writer.handoff(intent, now)?
        {
            return Ok(previous);
        }
        if journal.tail.is_none()
            && journal.observed.byte_length
                > writer.config.disk_budget - writer.config.maintenance_reserve
        {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "source generation exceeds the available disk budget",
            ));
        }
        let previous = writer.consumption(&journal.import_id)?;
        if let Some(previous) = &previous {
            if previous.source != journal.source || previous.format != journal.format {
                return Err(Error::new(
                    "CONFLICT",
                    "source generation already reserved with different interpretation",
                ));
            }
            if handoff.is_none() {
                return Ok(previous.clone());
            }
        }
        if previous.is_none()
            && writer.count_prefix("consumption:")? >= MAX_CONSUMPTION_JOURNALS as u64
        {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "16 consumption journals occupied; resolve claims before reserving more",
            ));
        }
        let mut batch = WriteBatch::default();
        let is_new = previous.is_none();
        let journal = previous.unwrap_or(journal);
        // Leave room for the claimed observation, completed receipt wrapper, removal clock
        // and history without exceeding the reader's 8 KiB ceiling.
        if json::write_compact(&journal.encode()).len()
            > JOURNAL_BYTES - COMPLETION_ROOM - CLAIMED_ROOM - HISTORY_ROOM
        {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "source ownership proof exceeds the 8 KiB journal budget",
            ));
        }
        if is_new {
            put(&mut batch, &key(&journal.import_id), &journal);
        }
        // Older writers must refuse before they can reclaim source-owned staging.
        if super::writer_format::read(&writer.db)?.as_deref() == Some(super::FORMAT) {
            super::writer_format::stage(&writer.db, &mut batch, FORMAT)?;
        }
        if let Some((intent, now)) = handoff {
            writer.bind_handoff(&mut batch, intent, &journal, now)?;
        }
        if journal.tail.is_some() {
            super::writer_format::stage(&writer.db, &mut batch, TAIL_FORMAT)?;
        }
        writer.admit_import_batch(&batch)?;
        writer.commit(batch)?;
        Ok(journal)
    }

    /// Recover original ownership and publication state without a retry lease.
    pub fn consumption_get(&self, id: &str) -> Result<ConsumptionJournal, Error> {
        self.lock()?
            .consumption(id)?
            .ok_or_else(|| Error::new("NOT_FOUND", "unknown consumption journal"))
    }

    /// Acknowledge a claimed terminal checkpoint without creating records,
    /// payloads, pins, or new acceptance/retention clocks. The sealed journal
    /// remains authoritative after bounded generation metadata expires.
    pub fn consumption_publish_tail(
        &self,
        id: &str,
        claimed: &ConsumptionGeneration,
    ) -> Result<ConsumptionJournal, Error> {
        let writer = self.lock()?;
        let mut journal = writer
            .consumption(id)?
            .ok_or_else(|| Error::new("NOT_FOUND", "unknown consumption journal"))?;
        if journal.tail.is_none() || journal.claimed.as_ref() != Some(claimed) {
            return Err(Error::new(
                "CONFLICT",
                "tail publication requires the original claimed terminal checkpoint",
            ));
        }
        if journal.tail_published {
            return Ok(journal);
        }
        journal.tail_published = true;
        journal.validate()?;
        let mut batch = WriteBatch::default();
        put(&mut batch, &key(id), &journal);
        writer.admit_import_batch(&batch)?;
        writer.commit(batch)?;
        Ok(journal)
    }

    /// Enumerate the bounded active recovery set, including unpublished claims
    /// and published sources whose removal has not been confirmed.
    pub fn consumption_pending(&self) -> Result<Vec<ConsumptionJournal>, Error> {
        let writer = self.lock()?;
        let mut journals = Vec::new();
        for entry in writer.db.prefix_iterator("consumption:") {
            let (name, _) = entry?;
            let Some(id) = name.strip_prefix(b"consumption:") else {
                break;
            };
            if journals.len() >= MAX_CONSUMPTION_JOURNALS {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "active consumption registry exceeds its admitted bound",
                ));
            }
            let id = std::str::from_utf8(id)
                .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
            let journal = writer
                .consumption(id)?
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "active consumption entry missing"))?;
            if journal.removed_at.is_some() {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "removed consumption journal remains active",
                ));
            }
            journals.push(journal);
        }
        Ok(journals)
    }

    /// Retire a claim that can never complete, as an operator's explicit decision. The
    /// caller has verified that the claimed private source is missing; this refuses unless the
    /// journal is active, belongs to `watch`, holds a claimed generation and has no recorded
    /// publication, so a claim whose publication is recorded is never retired. One batch
    /// moves the journal out of the active recovery set into the retired record and ends the
    /// staging lease of the claim's import if one began, so the next [`Store::import_reclaim`]
    /// frees its staged bytes and journal slot. A claim already retired is refused, so each
    /// retirement is recorded once.
    pub fn consumption_retire(
        &self,
        id: &str,
        watch: &str,
        caller: &str,
        reason: &str,
        now: u64,
    ) -> Result<RetiredConsumption, Error> {
        crate::format_instant(now)?;
        for (name, text) in [("caller", caller), ("reason", reason)] {
            if text.trim().is_empty() || text.len() > RETIREMENT_TEXT || text.contains('\n') {
                return Err(Error::new(
                    "INVALID_ARGUMENT",
                    format!(
                        "claim retirement {name} requires 1..{RETIREMENT_TEXT} bytes on one line"
                    ),
                ));
            }
        }
        let writer = self.lock()?;
        let journal = writer.consumption(id)?;
        if writer.db.get(retired_key(id))?.is_some() {
            return Err(Error::new(
                "CONFLICT",
                "the claim is already retired unrecoverable",
            ));
        }
        let journal = match journal {
            Some(journal) if writer.db.get(key(id))?.is_some() => journal,
            Some(_) => {
                return Err(Error::new(
                    "CONFLICT",
                    "the claim's source removal is already confirmed; nothing is pending",
                ));
            }
            None => return Err(Error::new("NOT_FOUND", "unknown consumption journal")),
        };
        if journal.watch != watch {
            return Err(Error::new("CONFLICT", "the claim belongs to another watch"));
        }
        if journal.published_id().is_some() {
            return Err(Error::new(
                "CONFLICT",
                "the claim's publication is recorded; recovery removes its source, never retire it",
            ));
        }
        if journal.claimed.is_none() {
            return Err(Error::new(
                "CONFLICT",
                "the claim never moved its source; recovery still reads it from the original path",
            ));
        }
        if writer.count_prefix(RETIRED_PREFIX)? >= MAX_RETIRED_CONSUMPTION_CLAIMS as u64 {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "64 retired claims are recorded; no further claim can be retired",
            ));
        }
        let retired = RetiredConsumption {
            journal,
            retirement: ConsumptionRetirement {
                retired_at: now,
                caller: caller.into(),
                reason: reason.into(),
            },
        };
        let mut batch = WriteBatch::default();
        put(&mut batch, &retired_key(id), &retired);
        batch.delete(key(id));
        // A begun import of this claim can never publish now; release it to reclamation.
        writer.end_retired_consumption_import(&mut batch, id, now)?;
        writer.commit(batch)?;
        Ok(retired)
    }

    /// Every retired unrecoverable claim, read by one iterator (its own implicit snapshot)
    /// without the writer lock, so status and watch listings never wait for a writer.
    pub fn consumption_retired(&self) -> Result<Vec<RetiredConsumption>, Error> {
        self.refuse_poisoned()?;
        let mut retired = Vec::new();
        for entry in self.unlocked.db.prefix_iterator(RETIRED_PREFIX) {
            let (name, bytes) = entry?;
            if !name.starts_with(RETIRED_PREFIX.as_bytes()) {
                break;
            }
            if retired.len() >= MAX_RETIRED_CONSUMPTION_CLAIMS {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "retired consumption claims exceed their admitted bound",
                ));
            }
            let claim = RetiredConsumption::decode(&parse(&bytes)?)?;
            if name[RETIRED_PREFIX.len()..] != *claim.journal.import_id.as_bytes() {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "retired consumption claim key mismatch",
                ));
            }
            retired.push(claim);
        }
        Ok(retired)
    }

    /// Record the stable descriptor observation only after the same-filesystem
    /// atomic claim and affected directory synchronization have succeeded.
    pub fn consumption_claimed(
        &self,
        id: &str,
        claimed: ConsumptionGeneration,
    ) -> Result<ConsumptionJournal, Error> {
        let writer = self.lock()?;
        let mut journal = writer
            .consumption(id)?
            .ok_or_else(|| Error::new("NOT_FOUND", "unknown consumption journal"))?;
        if let Some(previous) = &journal.claimed {
            if previous != &claimed {
                return Err(Error::new(
                    "CONFLICT",
                    "claim retry changed its descriptor observation",
                ));
            }
            return Ok(journal);
        }
        journal.claimed = Some(claimed);
        journal.validate()?;
        let mut batch = WriteBatch::default();
        put(&mut batch, &key(id), &journal);
        writer.admit_import_batch(&batch)?;
        writer.commit(batch)?;
        Ok(journal)
    }

    /// Confirm a coordinator's successful claim unlink and directory sync.
    /// This method never removes a path and refuses unpublished/mismatched input.
    pub fn consumption_removed(
        &self,
        id: &str,
        publication_id: &str,
        claimed: &ConsumptionGeneration,
        now: u64,
    ) -> Result<ConsumptionJournal, Error> {
        crate::format_instant(now)?;
        let writer = self.lock()?;
        let mut journal = writer
            .consumption(id)?
            .ok_or_else(|| Error::new("NOT_FOUND", "unknown consumption journal"))?;
        if journal.published_id() != Some(publication_id)
            || journal.claimed.as_ref() != Some(claimed)
        {
            return Err(Error::new(
                "CONFLICT",
                "removal confirmation requires the original published claim and evidence identity",
            ));
        }
        if journal.removed_at.is_some() {
            return Ok(journal);
        }
        now.checked_add(RETRY_WINDOW)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "removal retry deadline overflow"))?;
        journal.removed_at = Some(now);
        journal.history.attempts = journal.history.attempts.saturating_add(1);
        journal.history.last_attempt_at = Some(now);
        let mut batch = WriteBatch::default();
        let saved: Value = Object::new()
            .with("journal", journal.encode())
            .with("expires_at", (now + RETRY_WINDOW).to_string())
            .into();
        batch.put(completed_key(id), json::write_compact(&saved));
        batch.delete(key(id));
        writer.finish_handoffs(&mut batch, id, now)?;
        writer.commit(batch)?;
        Ok(journal)
    }

    /// Record that one publication attempt of active claim `id` left it pending at `now`,
    /// and why. The claim stays in the active set; only its history changes.
    pub fn consumption_attempt_pending(
        &self,
        id: &str,
        reason: &str,
        now: u64,
    ) -> Result<ConsumptionJournal, Error> {
        crate::format_instant(now)?;
        let writer = self.lock()?;
        if writer.db.get(key(id))?.is_none() {
            return Err(Error::new("NOT_FOUND", "no active consumption journal"));
        }
        let mut journal = writer
            .consumption(id)?
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "active consumption entry missing"))?;
        journal.record_pending(now, reason)?;
        let mut batch = WriteBatch::default();
        put(&mut batch, &key(id), &journal);
        writer.commit(batch)?;
        Ok(journal)
    }

    /// Every claim that is pending now, and every completed or abandoned claim whose pending
    /// span overlaps `since..=until` Unix milliseconds (an abandoned claim also when it was
    /// retired inside the window), read from one snapshot without the writer lock. The span
    /// runs from the claim's reservation to its latest pending attempt, because the store
    /// keeps no earlier attempt times; a claim with an attempt in the window always
    /// overlaps it, and one that pended only before and after the window may too. Completed claims are
    /// kept for the two-day retry window. More than [`MAX_LISTED_CLAIMS`] matches refuse,
    /// so the caller narrows the window rather than receiving a partial list.
    pub fn consumption_claims(&self, since: u64, until: u64) -> Result<Vec<ClaimListing>, Error> {
        if since > until {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "a claim window must not end before it starts",
            ));
        }
        self.refuse_poisoned()?;
        let window = since..=until;
        let snapshot = self.unlocked.db.snapshot();
        let mut claims = Vec::new();
        for (prefix, state) in [
            ("consumption:", ClaimState::Pending),
            ("request:consumption-complete:", ClaimState::Complete),
            (RETIRED_PREFIX, ClaimState::Abandoned),
        ] {
            let rows = snapshot.iterator(rocksdb::IteratorMode::From(
                prefix.as_bytes(),
                rocksdb::Direction::Forward,
            ));
            for row in rows {
                let (name, bytes) = row?;
                let Some(id) = name.strip_prefix(prefix.as_bytes()) else {
                    break;
                };
                if bytes.len() > JOURNAL_BYTES * 2 {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "claim record exceeds its admitted size",
                    ));
                }
                let value = parse(&bytes)?;
                let (journal, finished_at) = match state {
                    ClaimState::Pending => (ConsumptionJournal::decode(&value)?, None),
                    ClaimState::Complete => {
                        let journal = ConsumptionJournal::decode(super::field(&value, "journal")?)?;
                        let removed = journal.removed_at;
                        (journal, removed)
                    }
                    ClaimState::Abandoned => {
                        let retired = RetiredConsumption::decode(&value)?;
                        (retired.journal, Some(retired.retirement.retired_at))
                    }
                };
                if journal.import_id.as_bytes() != id {
                    return Err(Error::new("STORAGE_FAILURE", "claim record key mismatch"));
                }
                if !listed_in(state, &journal.history, finished_at, &window) {
                    continue;
                }
                if claims.len() >= MAX_LISTED_CLAIMS {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        format!(
                            "more than {MAX_LISTED_CLAIMS} claims match; narrow the time window"
                        ),
                    ));
                }
                claims.push(ClaimListing {
                    journal,
                    state,
                    finished_at,
                });
            }
        }
        claims.sort_by(|a, b| {
            (a.journal.history.first_seen, &a.journal.import_id)
                .cmp(&(b.journal.history.first_seen, &b.journal.import_id))
        });
        Ok(claims)
    }
}

/// Whether one claim belongs in a listing window. Pending claims always do. A complete or
/// abandoned claim does when its pending span overlaps the window: the span runs from
/// `first_seen` (the reservation) to `last_pending_at`, the only attempt times the journal
/// keeps, so a claim with any pending attempt in the window overlaps it. A claim that never
/// pended has no span. A journal reserved before schema 71 has no `first_seen`, so its span
/// is its latest pending attempt alone. An abandoned claim is also listed when it was
/// retired in the window. A completed journal with no history at all (reserved before
/// schema 71, so whether it went pending is unknown) is listed when it finished in the
/// window, so a window from before the upgrade can still show its claims complete.
fn listed_in(
    state: ClaimState,
    history: &ClaimHistory,
    finished_at: Option<u64>,
    window: &RangeInclusive<u64>,
) -> bool {
    let unrecorded = history.first_seen.is_none() && history.last_pending_at.is_none();
    let finished = finished_at.is_some_and(|at| window.contains(&at));
    let spans = history.last_pending_at.is_some_and(|last| {
        let first = history.first_seen.map_or(last, |seen| seen.min(last));
        first <= *window.end() && last >= *window.start()
    });
    match state {
        ClaimState::Pending => true,
        ClaimState::Complete => spans || (unrecorded && finished),
        ClaimState::Abandoned => spans || finished,
    }
}

/// Most claims one [`Store::consumption_claims`] listing returns.
pub const MAX_LISTED_CLAIMS: usize = 2048;

/// Where one claim stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimState {
    /// In the active recovery set: not yet published and removed.
    Pending,
    /// Published and its source removal confirmed.
    Complete,
    /// Retired unrecoverable by an administrator; it will never publish.
    Abandoned,
}

/// One claim in a [`Store::consumption_claims`] listing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimListing {
    /// The claim's journal, including its history.
    pub journal: ConsumptionJournal,
    /// Where the claim stands.
    pub state: ClaimState,
    /// Unix milliseconds of its completion or retirement; none while pending.
    pub finished_at: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One reserved journal at time 1000.
    fn fixture() -> ConsumptionJournal {
        ConsumptionJournal::reservation(
            "watch",
            b"input.log",
            "source",
            "log",
            ConsumptionGeneration {
                device: 1,
                inode: 2,
                byte_length: 4,
                modified: (10, 20),
                changed: (10, 30),
            },
            ConsumptionDirectories {
                root_device: 1,
                root_inode: 100,
                parent_device: 1,
                parent_inode: 101,
            },
            1_000,
        )
        .unwrap()
    }

    /// INVARIANT (issue 160): a journal written before schema 71 has no history, and every
    /// active and completed claim of a live store is such a journal at the switch. It must
    /// decode with an empty history rather than refuse, or recovery and listing would fail.
    #[test]
    fn a_journal_without_history_decodes_with_an_empty_history() {
        let journal = fixture();
        let text = json::write_compact(&journal.encode());
        let legacy = text.replace(r#","history":{"attempts":0,"first_seen":"1000"}"#, "");
        assert_ne!(legacy, text, "the fixture removes the history field");
        let decoded = ConsumptionJournal::decode(&parse(legacy.as_bytes()).unwrap()).unwrap();
        assert_eq!(decoded.history, ClaimHistory::default());
        assert_eq!(decoded.import_id, journal.import_id);
    }

    /// INVARIANT (issue 160): a pending reason never grows the journal past what its reader
    /// accepts, however much JSON escaping expands it, and is cut rather than refused.
    #[test]
    fn a_pending_reason_is_cut_to_its_encoded_budget() {
        let mut journal = fixture();
        journal
            .record_pending(2_000, &"\"\u{1}".repeat(4096))
            .unwrap();
        let reason = journal.history.last_pending_reason.clone().unwrap();
        assert!(json::write_compact(&Value::from(reason.as_str())).len() <= REASON_BYTES);
        assert!(!reason.is_empty());
        assert!(
            json::write_compact(&journal.encode()).len() + COMPLETION_ROOM + CLAIMED_ROOM
                <= JOURNAL_BYTES
        );
    }

    /// INVARIANT (issue 160): the zero-loss check lists, for a window, every claim whose
    /// pending span overlaps it. A completed journal from before schema 71 has no pending time, so it is
    /// listed by its completion time; otherwise claims pending in the incident windows before
    /// the upgrade could never be shown complete. A recorded claim that completed without
    /// going pending stays out.
    #[test]
    fn a_completed_claim_without_history_is_listed_by_its_completion_time() {
        let within = 2_000..=3_000;
        let unrecorded = ClaimHistory::default();
        assert!(listed_in(
            ClaimState::Complete,
            &unrecorded,
            Some(2_500),
            &within
        ));
        assert!(!listed_in(
            ClaimState::Complete,
            &unrecorded,
            Some(3_500),
            &within
        ));
        let at_once = ClaimHistory {
            first_seen: Some(1_000),
            ..ClaimHistory::default()
        };
        assert!(!listed_in(
            ClaimState::Complete,
            &at_once,
            Some(2_500),
            &within
        ));
        assert!(listed_in(ClaimState::Pending, &unrecorded, None, &within));
    }
}
