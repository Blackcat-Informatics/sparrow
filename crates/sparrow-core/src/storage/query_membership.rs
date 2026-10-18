// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Store-owned query membership, shared by field, text and context reads.

use super::{Artifact, Catalog, Error, Pin, Record, Writer, catalog_lease::CaptureAdmission};
use crate::{QueryScope, QuerySelection};
use rocksdb::{IteratorMode, ReadOptions};
use std::collections::{BTreeMap, BTreeSet};

pub(super) const MAX_SELECTION_VISITS: usize = 100_000;

/// Resolved record and whole-artifact occurrence membership, including recordless artifacts.
#[derive(Default)]
pub(super) struct SelectionMembers {
    pub records: BTreeMap<String, u64>,
    pub artifacts: BTreeSet<String>,
    pub parents: BTreeSet<String>,
    pub recordless: BTreeMap<String, u64>,
    visits: usize,
}

impl SelectionMembers {
    /// Admit a retained record and its parent through the shared reference law.
    fn admit_record(
        &mut self,
        writer: &Writer,
        id: &str,
        artifact: Option<&str>,
        now: u64,
        protected: &BTreeSet<String>,
        admission: &mut Option<&mut CaptureAdmission<'_>>,
    ) -> Result<(), Error> {
        if self.visits == MAX_SELECTION_VISITS {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "selection expansion exceeds 100000 record visits",
            ));
        }
        self.visits += 1;
        if id.len() > 128 || !id.starts_with("record:") {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "invalid selected record identity",
            ));
        }
        let record: Record = writer.selection_catalog(id, admission).map_err(|error| {
            if artifact.is_some() && error.code == "NOT_FOUND" {
                Error::new(
                    "STORAGE_FAILURE",
                    "selected complete artifact references a missing record",
                )
            } else {
                error
            }
        })?;
        if record.id != id || artifact.is_some_and(|parent| record.artifact_id != parent) {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "selected record identity or artifact ownership disagrees",
            ));
        }
        if !writer.live_record(&record, now, protected) {
            return Err(Error::new(
                "EVIDENCE_UNAVAILABLE",
                format!("selected record expired or evicted: {id}"),
            ));
        }
        let key = format!("time:{:020}:{id}", record.received_at);
        if writer.db.get_pinned(key)?.as_deref() != Some(id.as_bytes()) {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "selected record temporal index is missing or inconsistent",
            ));
        }
        self.parents.insert(record.artifact_id.clone());
        self.records.insert(id.to_owned(), record.received_at);
        Ok(())
    }
}

impl Writer {
    fn selection_catalog<T: Catalog>(
        &self,
        key: &str,
        admission: &mut Option<&mut CaptureAdmission<'_>>,
    ) -> Result<T, Error> {
        let Some(admission) = admission else {
            return self.get(key);
        };
        self.get_admitted(key, |bytes| admission.entry(bytes.len()))
    }

    /// The record-query facade uses the same reference owner as artifact discovery.
    pub(super) fn query_membership(
        &self,
        scope: &QueryScope,
        now: u64,
        protected: &BTreeSet<String>,
    ) -> Result<Option<BTreeMap<String, u64>>, Error> {
        self.query_selection_membership(&scope.selection, now, protected, None)
            .map(|members| members.map(|members| members.records))
    }

    /// Validate every selected reference before filters; preserve complete artifact identity.
    pub(super) fn query_selection_membership(
        &self,
        selection: &QuerySelection,
        now: u64,
        protected: &BTreeSet<String>,
        mut admission: Option<&mut CaptureAdmission<'_>>,
    ) -> Result<Option<SelectionMembers>, Error> {
        let members = match selection {
            QuerySelection::Live => return Ok(None),
            QuerySelection::Evidence(ids) => ids.iter().cloned().collect::<BTreeSet<_>>(),
            QuerySelection::Pin {
                id,
                expected_revision,
            } => {
                let pin: Pin = self.selection_catalog(id, &mut admission)?;
                if pin.id != *id {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "pin identity disagrees with catalog key",
                    ));
                }
                if pin.expires_at <= now {
                    return Err(Error::new("PIN_NOT_ACTIVE", "selected pin has expired"));
                }
                if expected_revision.is_some_and(|revision| revision != pin.revision) {
                    return Err(Error::new("CONFLICT", "selected pin revision changed"));
                }
                pin.members
            }
        };
        if members.len() > MAX_SELECTION_VISITS {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "selection exceeds 100000 direct members",
            ));
        }
        let mut resolved = SelectionMembers::default();
        for member in members {
            if member.len() > 128 {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "selected member identity exceeds catalog bound",
                ));
            }
            if member.starts_with("record:") {
                resolved.admit_record(self, &member, None, now, protected, &mut admission)?;
            } else if member.starts_with("artifact:") {
                let artifact: Artifact = self.selection_catalog(&member, &mut admission)?;
                if artifact.id != member {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "selected artifact identity disagrees with catalog key",
                    ));
                }
                if !self.live_artifact(&artifact, now, protected) {
                    return Err(Error::new(
                        "EVIDENCE_UNAVAILABLE",
                        format!("complete selected artifact expired or evicted: {member}"),
                    ));
                }
                if artifact.record_ids.len() > MAX_SELECTION_VISITS {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "selected artifact exceeds 100000 record references",
                    ));
                }
                if artifact.record_ids.is_empty() {
                    resolved
                        .recordless
                        .insert(member.clone(), artifact.received_at);
                }
                resolved.artifacts.insert(member.clone());
                resolved.parents.insert(member.clone());
                for id in &artifact.record_ids {
                    resolved.admit_record(
                        self,
                        id,
                        Some(&member),
                        now,
                        protected,
                        &mut admission,
                    )?;
                }
            } else {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "pin contains an invalid evidence identity",
                ));
            }
        }
        Ok(Some(resolved))
    }

    /// Live reads stream the time catalog; explicit reads seek only resolved records.
    pub(super) fn query_candidates<'a>(
        &'a self,
        scope: &QueryScope,
        now: u64,
        protected: &BTreeSet<String>,
    ) -> Result<Box<dyn Iterator<Item = Result<Record, Error>> + 'a>, Error> {
        self.query_candidates_from_members(
            scope,
            self.query_membership(scope, now, protected)?,
            |id| self.get(id),
        )
    }

    /// Reuse candidate ordering with a caller-admitted catalog lookup. Membership
    /// is resolved once under the same writer boundary before this iterator starts.
    pub(super) fn query_candidates_from_members<'a>(
        &'a self,
        scope: &QueryScope,
        members: Option<BTreeMap<String, u64>>,
        mut lookup: impl FnMut(&str) -> Result<Record, Error> + 'a,
    ) -> Result<Box<dyn Iterator<Item = Result<Record, Error>> + 'a>, Error> {
        if let Some(members) = members {
            let mut timeline = members
                .into_iter()
                .filter(|(_, time)| *time >= scope.received_from && *time < scope.received_before)
                .map(|(id, time)| (time, id))
                .collect::<Vec<_>>();
            timeline.sort_unstable();
            return Ok(Box::new(
                timeline.into_iter().map(move |(_, id)| lookup(&id)),
            ));
        }
        Ok(Box::new(temporal_candidates(
            scope.received_from,
            scope.received_before,
            |mode, options| self.db.iterator_opt(mode, options),
            lookup,
        )))
    }
}

/// One temporal iterator for current and native-snapshot reads. Storage lookup
/// remains with its backend; interval and index integrity do not diverge.
pub(super) fn temporal_candidates<'a, I>(
    from: u64,
    before: u64,
    scan: impl FnOnce(IteratorMode<'_>, ReadOptions) -> I,
    mut lookup: impl FnMut(&str) -> Result<Record, Error> + 'a,
) -> impl Iterator<Item = Result<Record, Error>> + 'a
where
    I: Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>> + 'a,
{
    let start = format!("time:{from:020}:");
    let mut options = ReadOptions::default();
    // The native upper bound stops before decoding a later record, even when
    // its index entry or catalog body is corrupt.
    options.set_iterate_upper_bound(format!("time:{before:020}:").into_bytes());
    let mut iterator = scan(
        IteratorMode::From(start.as_bytes(), rocksdb::Direction::Forward),
        options,
    );
    std::iter::from_fn(move || {
        let (key, id) = match iterator.next()? {
            Ok(entry) => entry,
            Err(error) => return Some(Err(error.into())),
        };
        if !key.starts_with(b"time:") {
            return None;
        }
        Some((|| {
            let id = std::str::from_utf8(&id)
                .map_err(|_| Error::new("STORAGE_FAILURE", "invalid temporal index identity"))?;
            let record: Record = lookup(id).map_err(|error| {
                if error.code == "NOT_FOUND" {
                    Error::new(
                        "STORAGE_FAILURE",
                        "temporal index references a missing record",
                    )
                } else {
                    error
                }
            })?;
            if record.id != id
                || key.as_ref() != format!("time:{:020}:{id}", record.received_at).as_bytes()
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "temporal index disagrees with record identity/time",
                ));
            }
            Ok(record)
        })())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{DAY, json, parse, source_time};

    #[test]
    fn snapshot_worker_preserves_temporal_versions_and_shared_integrity_checks() {
        let dir = tempfile::tempdir().unwrap();
        let db = std::sync::Arc::new(rocksdb::DB::open_default(dir.path()).unwrap());
        let raw = br#"{"job":"original"}"#;
        let (facts, graph) = crate::projection::json_facts(
            Some(raw),
            raw.len() as u64,
            &format!(
                "blake3:{}",
                purrdf_hash::hex::encode(purrdf_hash::blake3::hash(raw).as_bytes())
            ),
            &crate::projection::json_profile().unwrap(),
        )
        .unwrap();
        graph.unwrap();
        db.put("json:snapshot-fixture", json::write_compact(&facts))
            .unwrap();
        let template = Record {
            id: "record:inside".into(),
            artifact_id: "artifact:parent".into(),
            source: "snapshot-fixture".into(),
            received_at: 20,
            expires_at: 30 * DAY + 20,
            byte_start: 0,
            byte_end: raw.len() as u64,
            projection_id: "json:snapshot-fixture".into(),
            rdf_projection_id: String::new(),
            event_time: source_time::EventTime::unknown("no_profile", None),
            diagnostics: Default::default(),
            producer: None,
            receiver: None,
            syslog: None,
            tail: None,
            chunks: Vec::new(),
            evicted: false,
        };
        for (id, time) in [
            ("record:past", 10),
            ("record:inside", 20),
            ("record:future", 30),
        ] {
            let mut record = template.clone();
            record.id = id.into();
            record.received_at = time;
            db.put(id, json::write_compact(&record.encode())).unwrap();
            db.put(format!("time:{time:020}:{id}"), id).unwrap();
        }
        // Corrupt bodies outside [20,30) are never loaded by either backend.
        db.put("record:past", b"bad past catalog").unwrap();
        db.put("record:future", b"bad future catalog").unwrap();
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (read_tx, read_rx) = std::sync::mpsc::sync_channel(1);
        let decode = |bytes: Option<Vec<u8>>| {
            Record::decode(&parse(
                &bytes.ok_or_else(|| Error::new("NOT_FOUND", "catalog absent"))?,
            )?)
        };
        let reader_db = db.clone();
        let expected = template.encode();
        let worker = std::thread::spawn(move || {
            // Own the Arc and borrow the native snapshot locally: no unchecked
            // lifetime extension or self-referential registry is required.
            let snapshot = reader_db.snapshot();
            ready_tx.send(snapshot.sequence_number()).unwrap();
            read_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            let records = temporal_candidates(
                20,
                30,
                |mode, options| snapshot.iterator_opt(mode, options),
                |id| decode(snapshot.get(id)?),
            )
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].encode(), expected);
            let mut coverage = crate::storage::record_fields::FieldCoverage::default();
            let fields = crate::storage::record_fields::qualify_from_projection(
                &records[0],
                &[crate::FieldConstraint {
                    predicate: crate::JsonPredicate {
                        path: Some("/job".into()),
                        kind: None,
                        lexical: None,
                        decoded: Some("original".into()),
                    },
                    parent_binding: None,
                    parent_path: None,
                }],
                &mut coverage,
                100,
                true,
                |id| snapshot.get_pinned(id).map_err(Error::from),
            )
            .unwrap()
            .unwrap();
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0]["path"].as_str(), Some("/job"));
            assert_eq!(fields[0]["source_start"].as_str(), Some("8"));
            assert_eq!(fields[0]["source_end"].as_str(), Some("16"));
            assert_eq!(&raw[8..16], b"original");
            assert_eq!(coverage.failed, 0);
        });
        let sequence = ready_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete("record:inside");
        batch.delete("json:snapshot-fixture");
        batch.delete("time:00000000000000000020:record:inside");
        let mut replacement = template.clone();
        replacement.id = "record:new".into();
        batch.put(&replacement.id, json::write_compact(&replacement.encode()));
        batch.put("time:00000000000000000020:record:new", &replacement.id);
        db.write(batch).unwrap();
        assert!(db.latest_sequence_number() > sequence);
        read_tx.send(()).unwrap();
        worker.join().unwrap();
        let current = temporal_candidates(
            20,
            30,
            |mode, options| db.iterator_opt(mode, options),
            |id| decode(db.get(id)?),
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].id, replacement.id);
        db.delete(&replacement.id).unwrap();
        let missing = temporal_candidates(
            20,
            30,
            |mode, options| db.iterator_opt(mode, options),
            |_| Err(Error::new("NOT_FOUND", "missing")),
        )
        .next()
        .unwrap()
        .unwrap_err();
        assert_eq!(missing.code, "STORAGE_FAILURE");
        let injected = temporal_candidates(
            20,
            30,
            |mode, options| db.iterator_opt(mode, options),
            |_| Err(Error::new("INJECTED_IO", "failed")),
        )
        .next()
        .unwrap()
        .unwrap_err();
        assert_eq!(injected.code, "INJECTED_IO");
        let mismatch = temporal_candidates(
            20,
            30,
            |mode, options| db.iterator_opt(mode, options),
            |_| Ok(template.clone()),
        )
        .next()
        .unwrap()
        .unwrap_err();
        assert_eq!(mismatch.code, "STORAGE_FAILURE");
    }
}
