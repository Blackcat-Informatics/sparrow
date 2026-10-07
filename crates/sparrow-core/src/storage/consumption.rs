// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Durable completed-input ownership, separate from ordinary retry receipts.

use super::{
    Artifact, Catalog, Error, RETRY_WINDOW, Store, Writer, blake3, hex, integer, json, parse, put,
    read_field, validate_ingest_labels,
};
use purrdf_lex::json::{Object, Value};
use rocksdb::WriteBatch;

const MAX_JOURNALS: usize = 16;
const JOURNAL_BYTES: usize = 8192;
pub(super) const FORMAT: &[u8] = b"sparrow.store/6";

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
    /// Observation after claim and directory synchronization.
    pub claimed: Option<ConsumptionGeneration>,
    /// Original atomically published artifact, independent of retry expiry.
    pub published_artifact: Option<String>,
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
            .with(
                "claimed",
                self.claimed
                    .as_ref()
                    .map(Catalog::encode)
                    .unwrap_or(Value::Null),
            )
            .with("published_artifact", self.published_artifact.clone())
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
            claimed: match super::field(value, "claimed")? {
                Value::Null => None,
                value => Some(ConsumptionGeneration::decode(value)?),
            },
            published_artifact: read_field(value, "published_artifact")?,
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
            || (journal.published_artifact.is_some() && journal.claimed.is_none())
            || (journal.removed_at.is_some() && journal.published_artifact.is_none())
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
    fn validate(&self) -> Result<(), Error> {
        self.observed.validate()?;
        validate_ingest_labels(&self.request_id, &self.source, &self.format)?;
        if self.watch.is_empty()
            || self.watch.len() > 128
            || self.relative_path.len() > 2048
            || self.relative_path.split(|b| *b == b'/').any(|part| {
                part.is_empty() || part == b"." || part == b".." || part == b".sparrow-inflight"
            })
            || self.relative_path.contains(&0)
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "require a bounded watch and relative source path without traversal or claim-directory components",
            ));
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
        let identity = json::write_compact(&Value::array(vec![
            self.watch.clone().into(),
            hex::encode(&self.relative_path).into(),
            self.observed.encode(),
        ]));
        format!(
            "consume:{}",
            hex::encode(blake3::hash(identity.as_bytes()).as_bytes())
        )
    }
}

impl Writer {
    pub(super) fn consumption(&self, id: &str) -> Result<Option<ConsumptionJournal>, Error> {
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
        let value = parse(&bytes)?;
        let journal = ConsumptionJournal::decode(value.get("journal").unwrap_or(&value))?;
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
        if !request.starts_with("consume:") {
            return Ok(false);
        }
        let journal = self.consumption(&super::import::import_id(request))?.ok_or_else(|| Error::new("CONFLICT", "consumption request has no recovery journal; reserve the source generation first"))?;
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
    ) -> Result<ConsumptionJournal, Error> {
        let mut journal = ConsumptionJournal {
            import_id: String::new(),
            request_id: "reservation".into(),
            watch: watch.into(),
            relative_path: relative_path.into(),
            source: source.into(),
            format: format.into(),
            observed,
            claimed: None,
            published_artifact: None,
            removed_at: None,
        };
        journal.validate()?;
        journal.request_id = journal.reserved_request();
        journal.import_id = super::import::import_id(&journal.request_id);
        let writer = self.lock()?;
        if journal.observed.byte_length
            > writer.config.disk_budget - writer.config.maintenance_reserve
        {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "source generation exceeds the available disk budget",
            ));
        }
        if let Some(previous) = writer.consumption(&journal.import_id)? {
            if previous.source != source || previous.format != format {
                return Err(Error::new(
                    "CONFLICT",
                    "source generation already reserved with different interpretation",
                ));
            }
            return Ok(previous);
        }
        if writer.count_prefix("consumption:")? >= MAX_JOURNALS as u64 {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "16 consumption journals occupied; resolve claims before reserving more",
            ));
        }
        let mut batch = WriteBatch::default();
        put(&mut batch, &key(&journal.import_id), &journal);
        // Older writers must refuse before they can reclaim source-owned staging.
        batch.put("store_format", FORMAT);
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
            if journals.len() >= MAX_JOURNALS {
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
        artifact_id: &str,
        claimed: &ConsumptionGeneration,
        now: u64,
    ) -> Result<ConsumptionJournal, Error> {
        crate::format_instant(now)?;
        let writer = self.lock()?;
        let mut journal = writer
            .consumption(id)?
            .ok_or_else(|| Error::new("NOT_FOUND", "unknown consumption journal"))?;
        if journal.published_artifact.as_deref() != Some(artifact_id)
            || journal.claimed.as_ref() != Some(claimed)
        {
            return Err(Error::new(
                "CONFLICT",
                "removal confirmation requires the original published claim and artifact",
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
        writer.commit(batch)?;
        Ok(journal)
    }
}
