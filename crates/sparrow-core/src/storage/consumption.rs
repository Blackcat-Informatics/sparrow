// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Durable completed-input ownership, separate from ordinary retry receipts.

use super::{
    Artifact, Catalog, Error, RETRY_WINDOW, Store, Writer, blake3, hex, integer, json, parse, put,
    read_field, validate_ingest_labels,
};
use purrdf_lex::json::{Object, Value};
use rocksdb::WriteBatch;

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
}

fn key(id: &str) -> String {
    format!("consumption:{id}")
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
                self.db.get("store_format")?.as_deref(),
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
    pub fn consumption_reserve(
        &self,
        watch: &str,
        relative_path: &[u8],
        source: &str,
        format: &str,
        observed: ConsumptionGeneration,
        directories: ConsumptionDirectories,
    ) -> Result<ConsumptionJournal, Error> {
        let journal = ConsumptionJournal::reservation(
            watch,
            relative_path,
            source,
            format,
            observed,
            directories,
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
        // Leave room for the claimed observation, completed receipt wrapper,
        // and removal clock without exceeding the reader's 8 KiB ceiling.
        if json::write_compact(&journal.encode()).len() > JOURNAL_BYTES - 512 {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "source ownership proof exceeds the 8 KiB journal budget",
            ));
        }
        if is_new {
            put(&mut batch, &key(&journal.import_id), &journal);
        }
        // Older writers must refuse before they can reclaim source-owned staging.
        if writer.db.get("store_format")?.as_deref() == Some(super::FORMAT) {
            batch.put("store_format", FORMAT);
        }
        if let Some((intent, now)) = handoff {
            writer.bind_handoff(&mut batch, intent, &journal, now)?;
        }
        if journal.tail.is_some() {
            batch.put("store_format", TAIL_FORMAT);
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
}
