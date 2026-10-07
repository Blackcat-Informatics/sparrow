// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Store-owned query membership, shared by field, text and context reads.

use super::{Artifact, Error, Pin, Record, Writer};
use crate::{QueryScope, QuerySelection};
use rocksdb::IteratorMode;
use std::collections::{BTreeMap, BTreeSet};

const MAX_SELECTION_VISITS: usize = 100_000;

impl Writer {
    /// Resolve every reference before filtering. Retain IDs/timestamps, not record bodies.
    pub(super) fn query_membership(
        &self,
        scope: &QueryScope,
        now: u64,
        protected: &BTreeSet<String>,
    ) -> Result<Option<BTreeMap<String, u64>>, Error> {
        let members = match &scope.selection {
            QuerySelection::Live => return Ok(None),
            QuerySelection::Evidence(ids) => ids.iter().cloned().collect::<BTreeSet<_>>(),
            QuerySelection::Pin {
                id,
                expected_revision,
            } => {
                let pin: Pin = self.get(id)?;
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
        let mut records = BTreeMap::new();
        let mut visits = 0;
        let mut admit = |id: &str, artifact: Option<&str>| -> Result<(), Error> {
            if visits == MAX_SELECTION_VISITS {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "selection expansion exceeds 100000 record visits",
                ));
            }
            visits += 1;
            if id.len() > 128 || !id.starts_with("record:") {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "invalid selected record identity",
                ));
            }
            let record: Record = self.get(id).map_err(|error| {
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
            if !self.live_record(&record, now, protected) {
                return Err(Error::new(
                    "EVIDENCE_UNAVAILABLE",
                    format!("selected record expired or evicted: {id}"),
                ));
            }
            let key = format!("time:{:020}:{id}", record.received_at);
            if self.db.get(key)?.as_deref() != Some(id.as_bytes()) {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "selected record temporal index is missing or inconsistent",
                ));
            }
            records.insert(id.to_owned(), record.received_at);
            Ok(())
        };
        for member in members {
            if member.len() > 128 {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "selected member identity exceeds catalog bound",
                ));
            }
            if member.starts_with("record:") {
                admit(&member, None)?;
            } else if member.starts_with("artifact:") {
                let artifact: Artifact = self.get(&member)?;
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
                for id in &artifact.record_ids {
                    admit(id, Some(&member))?;
                }
            } else {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "pin contains an invalid evidence identity",
                ));
            }
        }
        Ok(Some(records))
    }

    /// Live reads stream the time catalog; explicit reads seek only resolved records.
    pub(super) fn query_candidates<'a>(
        &'a self,
        scope: &QueryScope,
        now: u64,
        protected: &BTreeSet<String>,
    ) -> Result<Box<dyn Iterator<Item = Result<Record, Error>> + 'a>, Error> {
        if let Some(members) = self.query_membership(scope, now, protected)? {
            let mut timeline = members
                .into_iter()
                .filter(|(_, time)| *time >= scope.received_from && *time < scope.received_before)
                .map(|(id, time)| (time, id))
                .collect::<Vec<_>>();
            timeline.sort_unstable();
            return Ok(Box::new(
                timeline.into_iter().map(move |(_, id)| self.get(&id)),
            ));
        }
        let start = format!("time:{:020}:", scope.received_from);
        let mut iterator = self.db.iterator(IteratorMode::From(
            start.as_bytes(),
            rocksdb::Direction::Forward,
        ));
        Ok(Box::new(std::iter::from_fn(move || {
            let (key, id) = match iterator.next()? {
                Ok(entry) => entry,
                Err(error) => return Some(Err(error.into())),
            };
            if !key.starts_with(b"time:") {
                return None;
            }
            Some((|| {
                let id = std::str::from_utf8(&id).map_err(|_| {
                    Error::new("STORAGE_FAILURE", "invalid temporal index identity")
                })?;
                let record: Record = self.get(id).map_err(|error| {
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
        })))
    }
}
