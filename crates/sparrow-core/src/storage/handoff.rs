// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Stable explicit handoff identities bound atomically to consumption ownership.

use super::consumption::{ConsumptionDirectories, ConsumptionGeneration, ConsumptionJournal};
use super::{Catalog, Error, RETRY_WINDOW, Store, Writer, integer, put, read_field};
use purrdf_lex::json::{Object, Value};
use rocksdb::WriteBatch;

pub(super) const FORMAT: &[u8] = b"sparrow.store/36";
const PREFIX: &str = "request:consumption-handoff:";
const MAX_HANDOFFS: u64 = 128;

/// Caller-owned retry identity and exact completed generation. The server must
/// validate the registered watch revision and producer authority before admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumptionHandoff {
    /// Reuse unchanged on retry, including after the original path disappears.
    pub request_id: String,
    /// Registered watch identity.
    pub watch: String,
    /// Exact administrative revision authorizing first admission.
    pub watch_revision: u64,
    /// Relative Unix path bytes, preserved without a UTF-8 restriction.
    pub relative_path: Vec<u8>,
    /// Exact generation the producer has finished writing.
    pub observed: ConsumptionGeneration,
    /// Original completed tail checkpoint, when retiring already accepted records.
    pub tail_generation_id: Option<String>,
}

impl ConsumptionHandoff {
    fn validate(&self) -> Result<(), Error> {
        if self.request_id.is_empty()
            || self.request_id.len() > 128
            || self.watch_revision == 0
            || self
                .tail_generation_id
                .as_ref()
                .is_some_and(|id| !id.starts_with("file-generation:") || id.len() > 128)
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "handoff requires a request ID of 1..128 bytes and a nonzero watch revision",
            ));
        }
        super::consumption::validate_source(&self.watch, &self.relative_path, &self.observed)
    }

    fn key(&self) -> String {
        format!("{PREFIX}{}", self.request_id)
    }
}

struct Binding {
    intent: ConsumptionHandoff,
    import_id: String,
    expires_at: u64,
    completed: bool,
}

impl Binding {
    fn validate_journal(&self, journal: &ConsumptionJournal) -> Result<(), Error> {
        let intent = &self.intent;
        if journal.import_id != self.import_id
            || journal.watch != intent.watch
            || journal.relative_path != intent.relative_path
            || journal.observed != intent.observed
            || journal.tail.as_ref().map(|tail| &tail.id) != intent.tail_generation_id.as_ref()
            || journal
                .tail
                .as_ref()
                .is_some_and(|tail| tail.declaration.watch_revision != intent.watch_revision)
            || self.completed != journal.removed_at.is_some()
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "handoff binding contradicts its consumption journal",
            ));
        }
        Ok(())
    }
    fn read(key: &[u8], bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > 8192 {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "handoff binding exceeds 8 KiB",
            ));
        }
        let binding = Self::decode(&super::parse(bytes)?)?;
        if key != binding.intent.key().as_bytes() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "handoff request key mismatch",
            ));
        }
        Ok(binding)
    }
}

impl Catalog for Binding {
    fn encode(&self) -> Value {
        Object::new()
            .with("request_id", self.intent.request_id.clone())
            .with("watch", self.intent.watch.clone())
            .with("watch_revision", self.intent.watch_revision.to_string())
            .with(
                "relative_path",
                super::hex::encode(&self.intent.relative_path),
            )
            .with("observed", self.intent.observed.encode())
            .with("tail_generation_id", self.intent.tail_generation_id.clone())
            .with("import_id", self.import_id.clone())
            .with("expires_at", self.expires_at.to_string())
            .with("completed", self.completed)
            .into()
    }

    fn decode(value: &Value) -> Result<Self, Error> {
        let binding = Self {
            intent: ConsumptionHandoff {
                request_id: read_field(value, "request_id")?,
                watch: read_field(value, "watch")?,
                watch_revision: integer(value, "watch_revision")?,
                relative_path: super::hex::decode_canonical(&read_field::<String>(
                    value,
                    "relative_path",
                )?)
                .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?,
                observed: ConsumptionGeneration::decode(super::field(value, "observed")?)?,
                tail_generation_id: value
                    .get("tail_generation_id")
                    .map(|_| read_field(value, "tail_generation_id"))
                    .transpose()?
                    .flatten(),
            },
            import_id: read_field(value, "import_id")?,
            expires_at: integer(value, "expires_at")?,
            completed: read_field(value, "completed")?,
        };
        binding
            .intent
            .validate()
            .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
        Ok(binding)
    }
}

impl Writer {
    pub(super) fn handoff_status_import(
        &self,
        request: &str,
        now: u64,
        remaining: &mut usize,
    ) -> Result<Option<String>, Error> {
        let key = format!("{PREFIX}{request}");
        let Some(bytes) = self.db.get_pinned(&key)? else {
            return Ok(None);
        };
        Self::charge_catalog_shape(remaining, &bytes)?;
        let binding = Binding::read(key.as_bytes(), &bytes)?;
        if binding.completed && binding.expires_at <= now {
            return Ok(None);
        }
        let journal = self
            .consumption_admitted(&binding.import_id, Some(remaining))?
            .ok_or_else(|| {
                Error::new(
                    "STORAGE_FAILURE",
                    "handoff status lost its ownership journal",
                )
            })?;
        binding.validate_journal(&journal)?;
        Ok(Some(binding.import_id))
    }

    pub(super) fn handoff(
        &self,
        intent: &ConsumptionHandoff,
        now: u64,
    ) -> Result<Option<ConsumptionJournal>, Error> {
        let Some(bytes) = self.db.get(intent.key())? else {
            return Ok(None);
        };
        let binding = Binding::read(intent.key().as_bytes(), &bytes)?;
        if binding.intent != *intent {
            return Err(Error::new(
                "CONFLICT",
                "handoff request ID already binds different arguments",
            ));
        }
        let journal = self.consumption(&binding.import_id)?;
        if binding.completed && binding.expires_at <= now {
            return Err(Error::new(
                "NOT_FOUND",
                "completed handoff retry window expired",
            ));
        }
        let journal = journal.ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "handoff lost its consumption ownership journal",
            )
        })?;
        binding.validate_journal(&journal)?;
        Ok(Some(journal))
    }

    pub(super) fn bind_handoff(
        &self,
        batch: &mut WriteBatch,
        intent: &ConsumptionHandoff,
        journal: &ConsumptionJournal,
        now: u64,
    ) -> Result<(), Error> {
        if self.db.get("watch_registry")?.is_none() {
            return Err(Error::new(
                "CONFLICT",
                "explicit handoff requires a durable registered watch catalog",
            ));
        }
        if self.count_prefix(PREFIX)? >= MAX_HANDOFFS {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "128 handoff retry bindings occupied; wait for completed receipt expiry",
            ));
        }
        let expires_at = journal
            .removed_at
            .unwrap_or(now)
            .checked_add(RETRY_WINDOW)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "handoff retry deadline overflow"))?;
        if journal.removed_at.is_some() && expires_at <= now {
            return Err(Error::new(
                "NOT_FOUND",
                "completed source ownership retry window expired",
            ));
        }
        put(
            batch,
            &intent.key(),
            &Binding {
                intent: intent.clone(),
                import_id: journal.import_id.clone(),
                expires_at,
                completed: journal.removed_at.is_some(),
            },
        );
        if super::writer_format::read(&self.db)?.as_deref() != Some(super::consumption::TAIL_FORMAT)
        {
            super::writer_format::stage(&self.db, batch, FORMAT)?;
        }
        Ok(())
    }

    pub(super) fn finish_handoffs(
        &self,
        batch: &mut WriteBatch,
        id: &str,
        now: u64,
    ) -> Result<(), Error> {
        let expires_at = now
            .checked_add(RETRY_WINDOW)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "handoff retry deadline overflow"))?;
        for (index, entry) in self.db.prefix_iterator(PREFIX).enumerate() {
            let (key, bytes) = entry?;
            if !key.starts_with(PREFIX.as_bytes()) {
                break;
            }
            if index as u64 >= MAX_HANDOFFS {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "handoff registry exceeds its admitted bound",
                ));
            }
            let mut binding = Binding::read(&key, &bytes)?;
            if binding.import_id == id {
                binding.expires_at = expires_at;
                binding.completed = true;
                put(batch, &binding.intent.key(), &binding);
            }
        }
        Ok(())
    }

    pub(super) fn managed_consumption_request(
        &self,
        key: &[u8],
        value: &Value,
    ) -> Result<bool, Error> {
        if let Some(id) = key.strip_prefix(b"request:import-publication:") {
            let id = std::str::from_utf8(id)
                .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
            return Ok(self.consumption(id)?.is_some());
        }
        if key.starts_with(PREFIX.as_bytes()) {
            let binding = Binding::decode(value)?;
            if key != binding.intent.key().as_bytes() {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "handoff request key mismatch",
                ));
            }
            if !binding.completed {
                let journal = self.consumption(&binding.import_id)?.ok_or_else(|| {
                    Error::new(
                        "STORAGE_FAILURE",
                        "active handoff lost its consumption ownership journal",
                    )
                })?;
                if journal.removed_at.is_some() {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "active handoff contradicts a completed consumption journal",
                    ));
                }
            }
            return Ok(!binding.completed);
        }
        Ok(false)
    }
}

impl Store {
    /// Recover the original handoff before inspecting today's watch or path.
    /// Active source ownership outlives the ordinary 48-hour retry lease.
    pub fn consumption_handoff_get(
        &self,
        intent: &ConsumptionHandoff,
        now: u64,
    ) -> Result<Option<ConsumptionJournal>, Error> {
        intent.validate()?;
        self.lock()?.handoff(intent, now)
    }

    /// Atomically reserve ownership and bind the producer's stable retry ID.
    /// This receipt proves admission, not publication or source deletion.
    pub fn consumption_handoff_reserve(
        &self,
        intent: &ConsumptionHandoff,
        source: &str,
        format: &str,
        directories: ConsumptionDirectories,
        now: u64,
    ) -> Result<ConsumptionJournal, Error> {
        intent.validate()?;
        if let Some(previous) = self.consumption_handoff_get(intent, now)? {
            return Ok(previous);
        }
        let mut journal = ConsumptionJournal::reservation(
            &intent.watch,
            &intent.relative_path,
            source,
            format,
            intent.observed.clone(),
            directories,
        )?;
        if let Some(id) = &intent.tail_generation_id {
            let tail = self.tail_generation(id)?;
            if tail.declaration.watch_revision != intent.watch_revision {
                return Err(Error::new(
                    "CONFLICT",
                    "tail handoff changed the original watch revision",
                ));
            }
            journal.bind_tail(tail)?;
        }
        self.reserve_consumption(journal, Some((intent, now)))
    }
}
