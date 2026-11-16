// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The payload id index (#323): from the BLAKE3 digest of an artifact's bytes to the
//! artifacts that hold it, so "does the store already hold this content" is a point read.
//!
//! One key per held artifact in the default family, `payload-id:<64 hex digest>:<artifact
//! id>`, with an empty value. The artifact id is part of the key because the same bytes
//! ingested under two requests make two artifacts; a prefix scan of one digest returns
//! them all. An artifact is indexed exactly while it is complete: its entry exists and
//! neither the entry nor its manifest retirement state marks it evicted. This is the same
//! completeness the per-source counters use, so an artifact that pressure eviction retired
//! leaves the index in the batch that retires it, and an artifact whose entry is deleted
//! (expiry, recordless eviction) leaves it in the batch that deletes the entry. An
//! archived artifact stays indexed until then: the index describes what the store holds,
//! not what the archive holds.
//!
//! Every ingest path stores the digest of all of an artifact's own bytes as its
//! `payload_id`: inline ingestion (including a tail checkpoint, whose artifact is exactly
//! its checkpointed byte range, and a zero-byte recordless artifact, whose id is the digest
//! of no bytes) hashes the input, and a published import verifies the hash of the whole
//! staged artifact against the manifest before it publishes. The id of one part of a set is
//! the hash of that part's artifact, so a part is found by its own digest and never by the
//! digest of the file it was cut from. Nothing is left unindexed for want of a digest.
//!
//! [`apply`] is the one owner: [`super::Writer::apply`] runs it on every batch, so every
//! change to an `artifact:` or `artifact-retirement:` key, whichever writer stages it,
//! changes the index in the same atomic batch, and an ingest cannot commit an entry
//! without its key. [`super::store_counters`] counts the keys as a store counter.
//!
//! A store written before the index is fenced at open and seeded by an online migration
//! (`payload-index`): bounded pages walk the artifact entries in order, put the key of
//! each complete one and advance the checkpoint in one batch. A write during the migration
//! keeps every key it creates or retires exact whatever the checkpoint says; the migration
//! writes the key of an entry it reaches again as a no-op. Until the migration completes a
//! lookup says it is pending: what it returns is held, and what it does not return is
//! unknown.

use super::manifest_retire::{self, header};
use super::store_counters::Reader;
use super::{Error, Store, durability, source_summary::failure};
use rocksdb::{DB, WriteBatch};
use std::collections::BTreeMap;

/// The key family.
pub(super) const PREFIX: &[u8] = b"payload-id:";
/// Migration checkpoint: `[0]` complete; `[1]` then the last examined artifact id.
pub(super) const CHECKPOINT: &str = "payload-index-upgrade";
const ARTIFACT: &[u8] = b"artifact:";
/// Artifact entries one migration page examines.
const PAGE_ARTIFACTS: usize = 1024;
/// Entry bytes one migration page reads; a page ends after the entry that reaches it.
const PAGE_BYTES: usize = 64 * 1024 * 1024;
/// Most artifact ids one lookup page returns.
pub const MAX_LOOKUP_PAGE: usize = 1000;

/// The 64-hex digest of a `blake3:`-prefixed payload id.
fn digest(payload_id: &str) -> Result<&str, Error> {
    payload_id
        .strip_prefix("blake3:")
        .filter(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .ok_or_else(|| {
            Error::new(
                "INVALID_ARGUMENT",
                "a payload id is blake3: and 64 lower-case hex digits",
            )
        })
}

fn digest_prefix(digest: &str) -> String {
    format!("payload-id:{digest}:")
}

fn entry_key(payload_id: &str, artifact_id: &str) -> Result<String, Error> {
    Ok(format!(
        "{}{artifact_id}",
        digest_prefix(digest(payload_id)?)
    ))
}

/// Committed migration position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Progress {
    Complete,
    /// Artifacts at or before the id are examined; `None` is the start.
    After(Option<String>),
}

impl Progress {
    fn encode(&self) -> Vec<u8> {
        match self {
            Self::Complete => vec![0],
            Self::After(after) => [&[1][..], after.as_deref().unwrap_or("").as_bytes()].concat(),
        }
    }

    /// Opaque status watermark; absent once complete.
    pub(super) fn watermark(&self) -> Option<String> {
        match self {
            Self::Complete => None,
            Self::After(None) => Some("start".to_owned()),
            Self::After(Some(id)) => Some(id.clone()),
        }
    }

    /// The last examined artifact id, as the counters' bound.
    pub(super) fn after(&self) -> Option<Option<&str>> {
        match self {
            Self::Complete => None,
            Self::After(after) => Some(after.as_deref()),
        }
    }
}

/// Read the committed migration position; a fenced store without one refuses.
pub(super) fn progress(
    read: impl FnOnce(&str) -> Result<Option<Vec<u8>>, Error>,
) -> Result<Progress, Error> {
    let bytes = read(CHECKPOINT)?.ok_or_else(|| {
        failure("required payload index migration state missing; restore the original catalog")
    })?;
    let invalid = || failure("invalid payload index migration checkpoint");
    match bytes.split_first() {
        Some((0, [])) => Ok(Progress::Complete),
        Some((1, [])) => Ok(Progress::After(None)),
        Some((1, rest)) => std::str::from_utf8(rest)
            .ok()
            .filter(|id| id.starts_with("artifact:"))
            .map(|id| Progress::After(Some(id.to_owned())))
            .ok_or_else(invalid),
        _ => Err(invalid()),
    }
}

/// Fence older writers into `batch` and seed the migration: the writer family lifts, any
/// key or checkpoint an older identity left is discarded (no binary that writes them leaves
/// an older identity in place), the two counter rows start at 0 and the checkpoint starts
/// at the first artifact, or complete when there is none. Returns the fenced format.
pub(super) fn fence(
    db: &DB,
    format: &[u8],
    batch: &mut WriteBatch,
) -> Result<&'static [u8], Error> {
    batch.delete_range(PREFIX, b"payload-id;");
    batch.delete(CHECKPOINT);
    batch.put(
        CHECKPOINT,
        if super::any_key(db, ARTIFACT)? {
            Progress::After(None)
        } else {
            Progress::Complete
        }
        .encode(),
    );
    super::store_counters::seed_payload_index(batch);
    let format = super::writer_format::enable_payload_index(format)?;
    batch.put("store_format", format);
    Ok(format)
}

/// An artifact entry's payload id and whether it is complete.
struct State {
    payload_id: String,
    evicted: bool,
}

fn state(id: &str, bytes: &[u8]) -> Result<State, Error> {
    let mut remaining = usize::MAX;
    let header = header(bytes, &mut remaining, 0, 0)?;
    if header.artifact.id != id {
        return Err(failure(format!(
            "artifact catalog entry {id} names another identity"
        )));
    }
    Ok(State {
        payload_id: header.artifact.payload_id,
        evicted: header.artifact.evicted,
    })
}

/// The index key an artifact owns, when it is complete: it has an entry that is not evicted
/// and no evicting retirement state.
fn owned_key(id: &str, state: Option<&State>, retired: bool) -> Result<Option<String>, Error> {
    state
        .filter(|state| !state.evicted && !retired)
        .map(|state| entry_key(&state.payload_id, id))
        .transpose()
}

/// Final value of each observed key in one batch.
#[derive(Default)]
struct Changes {
    artifacts: BTreeMap<String, Option<Vec<u8>>>,
    retirements: BTreeMap<String, Option<Vec<u8>>>,
    derived: bool,
}

impl durability::BatchObserver for Changes {
    fn observe(&mut self, name: &[u8], value: Option<&[u8]>) -> Result<(), Error> {
        let text = |bytes: &[u8]| {
            std::str::from_utf8(bytes)
                .map(str::to_owned)
                .map_err(|_| failure("invalid catalog key"))
        };
        let value = value.map(<[u8]>::to_vec);
        if name.starts_with(ARTIFACT) {
            self.artifacts.insert(text(name)?, value);
        } else if let Some(id) = name.strip_prefix(manifest_retire::PREFIX) {
            self.retirements.insert(text(id)?, value);
        } else if name.starts_with(PREFIX) {
            self.derived = true;
        }
        Ok(())
    }

    fn refuse_merge(&self, name: &[u8]) -> Option<Error> {
        (name.starts_with(ARTIFACT)
            || name.starts_with(manifest_retire::PREFIX)
            || name.starts_with(PREFIX))
        .then(|| failure("the payload index requires exact catalog writes, not merges"))
    }
}

/// Maintain the index for every change `batch` makes to an artifact entry or its manifest
/// retirement state, appending the key puts and deletes to the same batch. Runs on every
/// write through [`super::Writer::apply`], before the store counters observe the batch.
pub(super) fn apply(db: &DB, batch: &mut WriteBatch) -> Result<(), Error> {
    let mut changes = Changes::default();
    durability::observe_batch(batch, &mut changes)?;
    if changes.artifacts.is_empty() && changes.retirements.is_empty() {
        // A migration page, or a batch with no artifact: nothing follows an artifact.
        return Ok(());
    }
    if changes.derived {
        return Err(failure(
            "payload index keys are written only with the artifact change they follow",
        ));
    }
    let retired = |bytes: Option<Vec<u8>>| -> Result<bool, Error> {
        bytes
            .map(|bytes| manifest_retire::evicted_flag(&bytes))
            .transpose()
            .map(|flag| flag.unwrap_or(false))
    };
    let touched: std::collections::BTreeSet<&String> = changes
        .artifacts
        .keys()
        .chain(changes.retirements.keys())
        .collect();
    for id in touched {
        let before = db
            .get_pinned(id)?
            .map(|bytes| state(id, &bytes))
            .transpose()?;
        let stored_retirement = retired(db.get(format!("artifact-retirement:{id}"))?)?;
        let retired_after = match changes.retirements.get(id) {
            Some(next) => retired(next.clone())?,
            None => stored_retirement,
        };
        let key_before = owned_key(id, before.as_ref(), stored_retirement)?;
        // An artifact the batch does not rewrite keeps its stored entry.
        let key_after = match changes.artifacts.get(id) {
            Some(next) => {
                let after = next.as_deref().map(|bytes| state(id, bytes)).transpose()?;
                owned_key(id, after.as_ref(), retired_after)?
            }
            None => owned_key(id, before.as_ref(), retired_after)?,
        };
        if key_before == key_after {
            continue;
        }
        if let Some(key) = key_before {
            batch.delete(key);
        }
        if let Some(key) = key_after {
            batch.put(key, []);
        }
    }
    Ok(())
}

/// Stage one migration page: the keys of the complete artifacts it examines and the
/// advanced checkpoint in one batch, and whether the migration is complete after it.
/// `None` when it was already complete.
pub(super) fn page(db: &DB) -> Result<Option<(WriteBatch, bool)>, Error> {
    page_of(db, PAGE_ARTIFACTS)
}

/// [`page`] over at most `limit` artifact entries.
fn page_of(db: &DB, limit: usize) -> Result<Option<(WriteBatch, bool)>, Error> {
    let Progress::After(after) = progress(|key| Ok(db.get(key)?))? else {
        return Ok(None);
    };
    let reader = Reader::db(db);
    let mut batch = WriteBatch::default();
    let (mut examined, mut bytes) = (0_usize, 0_usize);
    let mut last = None;
    let mut full = false;
    reader.scan(
        ARTIFACT,
        after.as_deref().map(str::as_bytes),
        |key, value| {
            if examined == limit || bytes >= PAGE_BYTES {
                full = true;
                return Ok(false);
            }
            let id =
                std::str::from_utf8(key).map_err(|_| failure("invalid artifact catalog key"))?;
            examined += 1;
            bytes = bytes.saturating_add(value.len());
            let entry = state(id, value)?;
            let retired = db
                .get(format!("artifact-retirement:{id}"))?
                .map(|bytes| manifest_retire::evicted_flag(&bytes))
                .transpose()?
                .unwrap_or(false);
            if let Some(key) = owned_key(id, Some(&entry), retired)? {
                batch.put(key, []);
            }
            last = Some(id.to_owned());
            Ok(true)
        },
    )?;
    let next = if full {
        Progress::After(last.or(after))
    } else {
        Progress::Complete
    };
    batch.put(CHECKPOINT, next.encode());
    Ok(Some((batch, next == Progress::Complete)))
}

/// One page of a payload lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadLookup {
    /// Held artifacts of the digest after the cursor, in artifact id order, at most one page.
    pub artifacts: Vec<String>,
    /// The cursor of the next page; absent on the last page.
    pub next: Option<String>,
    /// Whether the index is complete. While it is not, every id returned is held and an
    /// absent one is unknown, never absent.
    pub complete: bool,
}

impl Store {
    /// The artifacts that hold the content with `payload_id` (`blake3:` and 64 lower-case
    /// hex digits): one page after `after` (an artifact id), at most `page_size` of them
    /// (1 to [`MAX_LOOKUP_PAGE`]). Read from one snapshot, without the writer lock. Exact
    /// when [`PayloadLookup::complete`]; pending while the `payload-index` migration runs.
    pub fn payload_lookup(
        &self,
        payload_id: &str,
        after: Option<&str>,
        page_size: usize,
    ) -> Result<PayloadLookup, Error> {
        let digest = digest(payload_id)?;
        if page_size == 0 || page_size > MAX_LOOKUP_PAGE {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                format!("a payload lookup page holds 1 to {MAX_LOOKUP_PAGE} artifacts"),
            ));
        }
        let prefix = digest_prefix(digest);
        let cursor = after
            .map(|id| {
                id.starts_with("artifact:")
                    .then(|| format!("{prefix}{id}"))
                    .ok_or_else(|| Error::new("INVALID_ARGUMENT", "invalid payload lookup cursor"))
            })
            .transpose()?;
        let snapshot = self.unlocked.db.snapshot();
        let reader = Reader::snapshot(&snapshot);
        let complete = progress(|key| reader.get(key.as_bytes()))? == Progress::Complete;
        let mut artifacts = Vec::new();
        let mut more = false;
        reader.scan(
            prefix.as_bytes(),
            cursor.as_deref().map(str::as_bytes),
            |key, _| {
                if artifacts.len() == page_size {
                    more = true;
                    return Ok(false);
                }
                let id = key
                    .strip_prefix(prefix.as_bytes())
                    .and_then(|id| std::str::from_utf8(id).ok())
                    .ok_or_else(|| failure("invalid payload index key"))?;
                artifacts.push(id.to_owned());
                Ok(true)
            },
        )?;
        let next = more.then(|| artifacts.last().cloned()).flatten();
        Ok(PayloadLookup {
            artifacts,
            next,
            complete,
        })
    }
}

/// The counters' bound of the migration's checkpoint: `None` once complete, else the last
/// examined artifact id, if any.
pub(super) fn bound_progress(
    read: impl FnOnce(&str) -> Result<Option<Vec<u8>>, Error>,
) -> Result<Option<Option<Vec<u8>>>, Error> {
    Ok(progress(read)?
        .after()
        .map(|after| after.map(|id| id.as_bytes().to_vec())))
}

/// Remove every index key and the migration checkpoint and lower the writer fence to its
/// store counter family, as a store written before #323 is. The next open fences it again
/// and seeds the online `payload-index` migration. Offline test fixtures only.
#[cfg(feature = "test-support")]
pub fn remove_payload_index_for_legacy_fixture(db: &DB) -> Result<(), Error> {
    let mut batch = WriteBatch::default();
    for row in db.prefix_iterator(PREFIX) {
        let (key, _) = row?;
        if !key.starts_with(PREFIX) {
            break;
        }
        batch.delete(key);
    }
    batch.delete(CHECKPOINT);
    let format = db
        .get("store_format")?
        .ok_or_else(|| failure("writer format missing"))?;
    batch.put(
        "store_format",
        super::writer_format::lower_payload_index(&format),
    );
    db.write(batch)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use crate::storage::{Artifact, blake3, hex, put};

    fn artifact(id: &str, bytes: &[u8]) -> Artifact {
        Artifact {
            buffered: None,
            id: id.into(),
            source: "source".into(),
            format: "log".into(),
            received_at: 1,
            expires_at: 2,
            byte_length: bytes.len() as u64,
            payload_id: format!("blake3:{}", hex::encode(blake3::hash(bytes).as_bytes())),
            record_ids: Vec::new(),
            intake: None,
            chunks: Vec::new(),
            evicted: false,
        }
    }

    /// Every put and delete one batch holds, in order.
    #[derive(Default)]
    struct Seen(Vec<(String, bool)>);

    impl durability::BatchObserver for Seen {
        fn observe(&mut self, name: &[u8], value: Option<&[u8]>) -> Result<(), Error> {
            self.0
                .push((String::from_utf8_lossy(name).into_owned(), value.is_some()));
            Ok(())
        }
        fn refuse_merge(&self, _: &[u8]) -> Option<Error> {
            None
        }
    }

    fn seen(batch: &WriteBatch) -> Vec<(String, bool)> {
        let mut seen = Seen::default();
        durability::observe_batch(batch, &mut seen).unwrap();
        seen.0
    }

    /// INVARIANT (#323): the owner puts an artifact's key into the very batch that carries its
    /// entry, deletes it in the batch that deletes the entry, and deletes it in the batch that
    /// retires the artifact through its manifest retirement state. So the entry and the key
    /// commit together or not at all, on the real write path, and a crash cannot leave one.
    #[test]
    fn the_key_travels_in_the_batch_that_creates_retires_or_deletes_the_entry() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), sparrow_test_support::storage_config!(Config)).unwrap();
        let writer = store.lock().unwrap();
        let held = artifact("artifact:held", b"");
        let key = entry_key(&held.payload_id, &held.id).unwrap();

        let mut batch = WriteBatch::default();
        put(&mut batch, &held.id, &held);
        apply(&writer.db, &mut batch).unwrap();
        assert!(seen(&batch).contains(&(key.clone(), true)));

        // The entry and key are stored; deleting the entry deletes the key in that batch.
        let mut stored = WriteBatch::default();
        put(&mut stored, &held.id, &held);
        stored.put(&key, []);
        writer.db.write(stored).unwrap();
        let mut batch = WriteBatch::default();
        batch.delete(&held.id);
        apply(&writer.db, &mut batch).unwrap();
        assert!(seen(&batch).contains(&(key.clone(), false)));

        // Retiring through the manifest state keeps the entry and drops the key.
        let mut batch = WriteBatch::default();
        let mut state = [0_u8; 17];
        state[16] = 1;
        batch.put(format!("artifact-retirement:{}", held.id), state);
        apply(&writer.db, &mut batch).unwrap();
        assert!(seen(&batch).contains(&(key.clone(), false)));

        // A batch that brings its own key with an entry change is refused.
        let mut batch = WriteBatch::default();
        put(&mut batch, &held.id, &held);
        batch.put(&key, []);
        assert_eq!(
            apply(&writer.db, &mut batch).unwrap_err().code,
            "STORAGE_FAILURE"
        );
    }

    /// INVARIANT (#323): the migration resumes after the last committed page. A store killed
    /// between pages (dropped, then reopened on the same directory) answers pending with the
    /// keys committed so far, finishes from its checkpoint without repeating or skipping an
    /// artifact, and ends with a count row equal to its complete artifacts.
    #[test]
    fn the_migration_resumes_after_a_restart_between_pages() {
        let dir = tempfile::tempdir().unwrap();
        let config = sparrow_test_support::storage_config!(Config);
        let store = Store::open(dir.path(), config.clone()).unwrap();
        for index in 0..5 {
            store
                .ingest(
                    &format!("request-{index}"),
                    "source",
                    "log",
                    format!("line {index}\n").as_bytes(),
                    10,
                )
                .unwrap();
        }
        drop(store);
        let db = sparrow_test_support::catalog_db(dir.path()).unwrap();
        remove_payload_index_for_legacy_fixture(&db).unwrap();
        drop(db);
        let store = Store::open(dir.path(), config.clone()).unwrap();
        while store.migrations().unwrap()[super::super::migration::Migration::StoreCounters.index()]
            .watermark
            .is_some()
        {
            store.migrate_step().unwrap().unwrap();
        }
        let indexed = |store: &Store| -> usize {
            (0..5)
                .map(|index| {
                    let id = format!(
                        "blake3:{}",
                        hex::encode(blake3::hash(format!("line {index}\n").as_bytes()).as_bytes())
                    );
                    store.payload_lookup(&id, None, 10).unwrap().artifacts.len()
                })
                .sum()
        };
        // Two pages of two artifacts each, committed through the ordinary write path.
        for _ in 0..2 {
            let writer = store.lock().unwrap();
            let (batch, complete) = page_of(&writer.db, 2).unwrap().unwrap();
            assert!(!complete);
            writer.commit(batch).unwrap();
        }
        assert_eq!(indexed(&store), 4);
        drop(store);
        let store = Store::open(dir.path(), config).unwrap();
        assert_eq!(indexed(&store), 4);
        while store.migrate_step().unwrap().is_some() {}
        assert_eq!(indexed(&store), 5);
        let (rows, recounted) = store.store_counter_audit_for_tests().unwrap();
        assert_eq!(rows["payload-index"], 5);
        assert_eq!(rows, recounted);
    }
}
