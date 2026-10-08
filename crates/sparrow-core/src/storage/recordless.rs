// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Chronological recordless-artifact catalog and resumable legacy writer upgrade.

#[cfg(test)]
use super::catalog_upgrade::Progress;
use super::catalog_upgrade::commit;
use super::{Artifact, Catalog, Error, Writer, json, parse};
use rocksdb::{DB, WriteBatch};

pub(super) const PREFIX: &[u8] = b"empty-time:";
const UPGRADE: &str = "recordless-index-upgrade";
const PAGE_BYTES: usize = 32 * 1024 * 1024;
#[cfg(test)]
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

/// Shared legacy format identity and required index backfill.
pub(super) struct LegacyFormat {
    pub target: &'static [u8],
    pub needs_recordless_index: bool,
    pub diagnostic_mappings: bool,
    pub producer_registry: bool,
    pub tail_registry: bool,
    pub finish_authority: bool,
}

/// Preserve consumption/watch/handoff authority through every admitted upgrade.
pub(super) fn legacy_format(format: &[u8]) -> Option<LegacyFormat> {
    let target = match format {
        b"sparrow.store/5" | b"sparrow.store/9" | b"sparrow.store/13" | b"sparrow.store/17"
        | b"sparrow.store/21" | b"sparrow.store/25" | b"sparrow.store/29" => super::FORMAT,
        b"sparrow.store/6" | b"sparrow.store/10" | b"sparrow.store/14" | b"sparrow.store/18"
        | b"sparrow.store/22" | b"sparrow.store/26" | b"sparrow.store/30" => {
            super::consumption::FORMAT
        }
        b"sparrow.store/7" | b"sparrow.store/11" | b"sparrow.store/15" | b"sparrow.store/19"
        | b"sparrow.store/23" | b"sparrow.store/27" | b"sparrow.store/31" => {
            super::watch_registry::FORMAT
        }
        b"sparrow.store/8" | b"sparrow.store/12" | b"sparrow.store/16" | b"sparrow.store/20"
        | b"sparrow.store/24" | b"sparrow.store/28" | b"sparrow.store/32" => super::handoff::FORMAT,
        _ => return None,
    };
    Some(LegacyFormat {
        target,
        finish_authority: matches!(
            format,
            b"sparrow.store/29" | b"sparrow.store/30" | b"sparrow.store/31" | b"sparrow.store/32"
        ),
        diagnostic_mappings: matches!(
            format,
            b"sparrow.store/13"
                | b"sparrow.store/14"
                | b"sparrow.store/15"
                | b"sparrow.store/16"
                | b"sparrow.store/17"
                | b"sparrow.store/18"
                | b"sparrow.store/19"
                | b"sparrow.store/20"
                | b"sparrow.store/21"
                | b"sparrow.store/22"
                | b"sparrow.store/23"
                | b"sparrow.store/24"
                | b"sparrow.store/25"
                | b"sparrow.store/26"
                | b"sparrow.store/27"
                | b"sparrow.store/28"
                | b"sparrow.store/29"
                | b"sparrow.store/30"
                | b"sparrow.store/31"
                | b"sparrow.store/32"
        ),
        producer_registry: matches!(
            format,
            b"sparrow.store/17"
                | b"sparrow.store/18"
                | b"sparrow.store/19"
                | b"sparrow.store/20"
                | b"sparrow.store/21"
                | b"sparrow.store/22"
                | b"sparrow.store/23"
                | b"sparrow.store/24"
                | b"sparrow.store/25"
                | b"sparrow.store/26"
                | b"sparrow.store/27"
                | b"sparrow.store/28"
                | b"sparrow.store/29"
                | b"sparrow.store/30"
                | b"sparrow.store/31"
                | b"sparrow.store/32"
        ),
        tail_registry: matches!(
            format,
            b"sparrow.store/25"
                | b"sparrow.store/26"
                | b"sparrow.store/27"
                | b"sparrow.store/28"
                | b"sparrow.store/29"
                | b"sparrow.store/30"
                | b"sparrow.store/31"
                | b"sparrow.store/32"
        ),
        needs_recordless_index: matches!(
            format,
            b"sparrow.store/5" | b"sparrow.store/6" | b"sparrow.store/7" | b"sparrow.store/8"
        ),
    })
}

pub(super) fn requires_watch_registry(format: &[u8]) -> bool {
    matches!(
        format,
        b"sparrow.store/7"
            | b"sparrow.store/8"
            | b"sparrow.store/11"
            | b"sparrow.store/12"
            | b"sparrow.store/15"
            | b"sparrow.store/16"
            | b"sparrow.store/19"
            | b"sparrow.store/20"
            | b"sparrow.store/23"
            | b"sparrow.store/24"
            | b"sparrow.store/27"
            | b"sparrow.store/28"
            | b"sparrow.store/31"
            | b"sparrow.store/32"
    ) || format == super::watch_registry::FORMAT
        || format == super::handoff::FORMAT
}

/// Startup is unavailable until this persistent index is complete. Each page is durable.
pub(super) fn upgrade(db: &DB) -> Result<(), Error> {
    fence(db)?;
    super::catalog_upgrade::run(db, &JOB)
}

fn fence(db: &DB) -> Result<(), Error> {
    let format = db.get("store_format")?.ok_or_else(|| {
        Error::new(
            "STORAGE_FAILURE",
            "store format missing during recordless index upgrade",
        )
    })?;
    if let Some(legacy) = legacy_format(&format).filter(|legacy| legacy.needs_recordless_index) {
        let mut batch = WriteBatch::default();
        batch.put("store_format", legacy.target);
        if db.get(super::producer_stream::KEY)?.is_some() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "legacy store has unfenced producer stream registry",
            ));
        }
        batch.put(super::producer_stream::KEY, b"[]");
        if db.get(super::tail::KEY)?.is_some() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "legacy store has unfenced file-generation registry",
            ));
        }
        batch.put(super::tail::KEY, b"[]");
        super::catalog_upgrade::seed(db, &JOB, &mut batch)?;
        super::tail_ranges::seed(db, &mut batch)?;
        commit(db, batch)?;
    }
    Ok(())
}

const JOB: super::catalog_upgrade::Job = super::catalog_upgrade::Job {
    checkpoint: UPGRADE,
    source: "artifact:",
    target: PREFIX,
    page_bytes: PAGE_BYTES,
    charge: upgrade_charge,
    publish: upgrade_publish,
};

fn upgrade_charge(bytes: &[u8]) -> Result<usize, Error> {
    Ok(if has_records(bytes)? {
        bytes.len()
    } else {
        bytes.len().saturating_mul(65)
    })
}

fn upgrade_publish(
    identity: &str,
    bytes: &[u8],
    charge: usize,
    batch: &mut WriteBatch,
) -> Result<(), Error> {
    // Populated manifests were validated by the native streaming reader without materialization.
    if charge != bytes.len() {
        let artifact = Artifact::decode(&parse(bytes)?)?;
        if artifact.id != identity {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "recordless upgrade artifact identity mismatch",
            ));
        }
        publish(batch, &artifact)?;
    }
    Ok(())
}

#[cfg(test)]
fn step(db: &DB, progress: Progress, maximum: usize) -> Result<(), Error> {
    super::catalog_upgrade::step(db, &JOB, progress, maximum)
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
