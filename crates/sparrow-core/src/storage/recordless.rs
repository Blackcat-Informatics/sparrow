// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Chronological recordless-artifact catalog and resumable legacy writer upgrade.

use super::{Artifact, Catalog, Error, Writer, json, parse, read_field};
use rocksdb::{DB, Direction, IteratorMode, WriteBatch, WriteOptions};

pub(super) const PREFIX: &[u8] = b"empty-time:";
const UPGRADE: &str = "recordless-index-upgrade";
const PAGE_BYTES: usize = 32 * 1024 * 1024;
const PAGE_ENTRIES: usize = 512;

pub(super) fn key(artifact: &Artifact) -> String {
    format!("empty-time:{:020}:{}", artifact.received_at, artifact.id)
}

pub(super) fn publish(batch: &mut WriteBatch, artifact: &Artifact) -> Result<(), Error> {
    if artifact.record_ids.is_empty() {
        if artifact.byte_length != 0 || !artifact.chunks.is_empty() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "recordless artifact has unaccounted original bytes",
            ));
        }
        if !artifact.evicted {
            batch.put(key(artifact), artifact.id.as_bytes());
        }
    }
    Ok(())
}

/// Preserve the pre-existing consumption/watch/handoff writer fence on upgrade.
pub(super) fn legacy_target(format: &[u8]) -> Option<&'static [u8]> {
    match format {
        b"sparrow.store/5" => Some(super::FORMAT),
        b"sparrow.store/6" => Some(super::consumption::FORMAT),
        b"sparrow.store/7" => Some(super::watch_registry::FORMAT),
        b"sparrow.store/8" => Some(super::handoff::FORMAT),
        _ => None,
    }
}

pub(super) fn requires_watch_registry(format: &[u8]) -> bool {
    matches!(format, b"sparrow.store/7" | b"sparrow.store/8")
        || format == super::watch_registry::FORMAT
        || format == super::handoff::FORMAT
}

#[derive(Default)]
struct Progress {
    after: Option<String>,
    complete: bool,
}

impl Catalog for Progress {
    fn encode(&self) -> json::Value {
        json::Object::new()
            .with("after", self.after.clone())
            .with("complete", self.complete)
            .into()
    }

    fn decode(value: &json::Value) -> Result<Self, Error> {
        let progress = Self {
            after: read_field(value, "after")?,
            complete: read_field(value, "complete")?,
        };
        if progress
            .after
            .as_ref()
            .is_some_and(|key| !key.starts_with("artifact:") || key.len() > 128)
            || (progress.complete && progress.after.is_some())
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "invalid recordless index upgrade checkpoint",
            ));
        }
        Ok(progress)
    }
}

/// Startup is unavailable until this persistent index is complete. Each page is durable.
pub(super) fn upgrade(db: &DB) -> Result<(), Error> {
    fence(db)?;
    loop {
        let bytes = db.get(UPGRADE)?.ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "required recordless index upgrade state missing; restore the original catalog",
            )
        })?;
        if bytes.len() > 1024 {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "recordless upgrade checkpoint exceeds its bound",
            ));
        }
        let progress = Progress::decode(&parse(&bytes)?)?;
        if progress.complete {
            return Ok(());
        }
        step(db, progress, PAGE_ENTRIES)?;
    }
}

fn fence(db: &DB) -> Result<(), Error> {
    let format = db.get("store_format")?.ok_or_else(|| {
        Error::new(
            "STORAGE_FAILURE",
            "store format missing during recordless index upgrade",
        )
    })?;
    if let Some(target) = legacy_target(&format) {
        let present = match db.prefix_iterator(PREFIX).next() {
            Some(row) => row?.0.starts_with(PREFIX),
            None => false,
        };
        if db.get(UPGRADE)?.is_some() || present {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "legacy store contains unfenced recordless index state",
            ));
        }
        let mut batch = WriteBatch::default();
        batch.put("store_format", target);
        super::put(&mut batch, UPGRADE, &Progress::default());
        commit(db, batch)?;
    }
    Ok(())
}

fn commit(db: &DB, batch: WriteBatch) -> Result<(), Error> {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options.disable_wal(false);
    db.write_opt(batch, &options)?;
    Ok(())
}

fn step(db: &DB, mut progress: Progress, maximum: usize) -> Result<(), Error> {
    let start = progress.after.clone();
    let key = start.as_deref().unwrap_or("artifact:");
    let mut batch = WriteBatch::default();
    let mut remaining = PAGE_BYTES;
    let mut visited = 0;
    progress.complete = true;
    for entry in db.iterator(IteratorMode::From(key.as_bytes(), Direction::Forward)) {
        let (key, bytes) = entry?;
        if !key.starts_with(b"artifact:") {
            break;
        }
        if start
            .as_ref()
            .is_some_and(|after| key.as_ref() == after.as_bytes())
        {
            continue;
        }
        if visited == maximum {
            progress.complete = false;
            break;
        }
        let identity = std::str::from_utf8(&key).map_err(|_| {
            Error::new(
                "STORAGE_FAILURE",
                "invalid artifact catalog key during recordless upgrade",
            )
        })?;
        if identity.len() > 128 {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "artifact key exceeds recordless upgrade bound",
            ));
        }
        let populated = has_records(&bytes)?;
        let charge = if populated {
            bytes.len()
        } else {
            bytes.len().saturating_mul(65)
        };
        if charge > remaining {
            if visited == 0 {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "one artifact exceeds the recordless upgrade page admission",
                ));
            }
            progress.complete = false;
            break;
        }
        remaining -= charge;
        // Large populated manifests are validated by the native streaming reader, not materialized.
        if !populated {
            let artifact = Artifact::decode(&parse(&bytes)?)?;
            if artifact.id != identity {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "recordless upgrade artifact identity mismatch",
                ));
            }
            publish(&mut batch, &artifact)?;
        }
        progress.after = Some(identity.to_owned());
        visited += 1;
    }
    if progress.complete {
        progress.after = None;
    }
    super::put(&mut batch, UPGRADE, &progress);
    commit(db, batch)
}

/// Native JSON event traversal inspects only the storage manifest's record membership.
fn has_records(bytes: &[u8]) -> Result<bool, Error> {
    if bytes.len() > PAGE_BYTES {
        return Err(Error::new(
            "RESOURCE_LIMIT",
            "artifact catalog exceeds recordless upgrade input admission",
        ));
    }
    let limits = json::Limits {
        max_depth: 128,
        max_values: 1_000_000,
        max_string_bytes: super::CATALOG_STRING_BYTES,
        unique_members: true,
    };
    // Whole-value validation owns duplicate-member admission; raw event iteration does not.
    let mut validation = json::Reader::from_slice(bytes, limits)?;
    validation.check_value()?;
    validation.finish()?;
    let mut reader = json::Reader::from_slice(bytes, limits)?;
    reader.begin_object()?;
    let mut populated = None;
    while let Some(name) = reader.next_key()? {
        if name.decode()?.as_ref() == "record_ids" {
            reader.begin_array()?;
            let mut any = false;
            while reader.next_item()? {
                if reader.peek_kind() != Some(json::Kind::String) {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "artifact record membership requires strings",
                    ));
                }
                reader.check_value()?;
                any = true;
            }
            populated = Some(any);
        } else {
            reader.check_value()?;
        }
    }
    reader.finish()?;
    populated.ok_or_else(|| {
        Error::new(
            "STORAGE_FAILURE",
            "artifact record membership missing during recordless upgrade",
        )
    })
}

impl Writer {
    pub(super) fn indexed_recordless(
        &self,
        key: &[u8],
        value: &[u8],
        remaining: &mut usize,
    ) -> Result<Artifact, Error> {
        let id = std::str::from_utf8(value)
            .map_err(|_| Error::new("STORAGE_FAILURE", "invalid recordless time index identity"))?;
        if !id.starts_with("artifact:") || id.len() <= "artifact:".len() || id.len() > 128 {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "recordless time index requires a bounded artifact identity",
            ));
        }
        let artifact: Artifact = self.get_limited(id, remaining).map_err(|error| {
            if error.code == "NOT_FOUND" {
                Error::new(
                    "STORAGE_FAILURE",
                    "recordless time index references missing artifact",
                )
            } else {
                error
            }
        })?;
        if artifact.id != id
            || artifact.evicted
            || !artifact.record_ids.is_empty()
            || artifact.byte_length != 0
            || !artifact.chunks.is_empty()
            || key != self::key(&artifact).as_bytes()
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "recordless time index disagrees with its artifact",
            ));
        }
        Ok(artifact)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact(id: &str) -> Artifact {
        Artifact {
            buffered: None,
            id: id.into(),
            source: "upgrade-test".into(),
            format: "log".into(),
            received_at: 1,
            expires_at: 30 * super::super::DAY,
            byte_length: 0,
            payload_id: format!(
                "blake3:{}",
                super::super::hex::encode(super::super::blake3::hash(b"").as_bytes())
            ),
            record_ids: Vec::new(),
            chunks: Vec::new(),
            evicted: false,
        }
    }

    #[test]
    fn upgrade_resumes_durable_native_pages_after_reopen_and_rejects_missing_state() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        db.put("store_format", b"sparrow.store/5").unwrap();
        for id in ["artifact:a", "artifact:b", "artifact:c"] {
            let mut batch = WriteBatch::default();
            super::super::put(&mut batch, id, &artifact(id));
            commit(&db, batch).unwrap();
        }
        fence(&db).unwrap();
        assert_eq!(
            db.get("store_format").unwrap().as_deref(),
            Some(super::super::FORMAT)
        );
        step(&db, Progress::default(), 1).unwrap();
        let progress =
            Progress::decode(&parse(&db.get(UPGRADE).unwrap().unwrap()).unwrap()).unwrap();
        assert!(!progress.complete);
        assert_eq!(progress.after.as_deref(), Some("artifact:a"));
        assert!(db.get(key(&artifact("artifact:a"))).unwrap().is_some());
        assert!(db.get(key(&artifact("artifact:b"))).unwrap().is_none());
        drop(db);
        let db = DB::open_default(dir.path()).unwrap();
        upgrade(&db).unwrap();
        for id in ["artifact:a", "artifact:b", "artifact:c"] {
            assert!(db.get(key(&artifact(id))).unwrap().is_some());
        }
        assert!(
            Progress::decode(&parse(&db.get(UPGRADE).unwrap().unwrap()).unwrap())
                .unwrap()
                .complete
        );
        db.delete(UPGRADE).unwrap();
        assert_eq!(upgrade(&db).unwrap_err().code, "STORAGE_FAILURE");
    }

    #[test]
    fn upgrade_error_commits_no_index_prefix_and_native_reader_skips_large_membership() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        db.put("store_format", b"sparrow.store/5").unwrap();
        let mut batch = WriteBatch::default();
        super::super::put(&mut batch, "artifact:a", &artifact("artifact:a"));
        batch.put("artifact:b", br#"{"record_ids":false}"#);
        commit(&db, batch).unwrap();
        fence(&db).unwrap();
        assert_eq!(
            step(&db, Progress::default(), PAGE_ENTRIES)
                .unwrap_err()
                .code,
            "STORAGE_FAILURE"
        );
        assert!(db.get(key(&artifact("artifact:a"))).unwrap().is_none());
        assert!(
            Progress::decode(&parse(&db.get(UPGRADE).unwrap().unwrap()).unwrap())
                .unwrap()
                .after
                .is_none()
        );
        let value: json::Value = json::Object::new()
            .with("record_ids", vec!["record:member"; 100_000])
            .into();
        let bytes = json::write_compact(&value);
        assert!(has_records(bytes.as_bytes()).unwrap());
        assert_eq!(
            has_records(br#"{"record_ids":[],"record_ids":[]}"#)
                .unwrap_err()
                .code,
            "STORAGE_FAILURE"
        );
    }
}
