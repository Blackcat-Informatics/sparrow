// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Store-owned query membership, shared by field, text and context reads.

use super::{Artifact, Catalog, Error, Pin, Record, Writer, catalog_lease::CaptureAdmission};
use crate::{QueryScope, QuerySelection};
use rocksdb::IteratorMode;
use std::collections::{BTreeMap, BTreeSet};

pub(super) const MAX_SELECTION_VISITS: usize = 100_000;

/// Resolved record and whole-artifact occurrence membership, including recordless artifacts.
#[derive(Default)]
pub(super) struct SelectionMembers {
    pub records: BTreeMap<String, u64>,
    pub artifacts: BTreeSet<String>,
    pub parents: BTreeSet<String>,
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
        self.get_admitted(key, |bytes| admission.entry(bytes))
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
