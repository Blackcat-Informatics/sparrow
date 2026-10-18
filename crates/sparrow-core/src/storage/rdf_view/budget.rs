// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Sparrow-owned admission for a RocksDB RDF read and its native operator scratch.

use super::{Error, RdfReadEvidence, RdfReadLimits, ReadError};
use purrdf::dataset_view::{OwnedWorkspaceReservation, WorkspaceReservation};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

pub(super) struct Budget {
    state: Mutex<State>,
    limits: RdfReadLimits,
    deadline: Instant,
    cancellation: crate::QueryCancellation,
}

struct State {
    error: Option<ReadError>,
    evidence: RdfReadEvidence,
}

pub(super) struct Reservation {
    budget: Arc<Budget>,
    bytes: u64,
    control_bytes: u64,
}

struct RetainedReservation {
    reservation: Reservation,
    _scope: Arc<Reservation>,
}

impl Budget {
    #[cfg(test)]
    pub(super) fn new(limits: RdfReadLimits) -> Result<Arc<Self>, ReadError> {
        Self::new_cancellable(limits, crate::QueryCancellation::default())
    }

    pub(super) fn new_cancellable(
        limits: RdfReadLimits,
        cancellation: crate::QueryCancellation,
    ) -> Result<Arc<Self>, ReadError> {
        if limits.live_bytes == 0 || limits.lease_ms == 0 || limits.lease_ms > 60000 {
            return Err(Arc::new(Error::new(
                "INVALID_ARGUMENT",
                "RDF read requires live_bytes > 0 and lease_ms 1..60000",
            )));
        }
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                error: None,
                evidence: RdfReadEvidence::default(),
            }),
            limits,
            deadline: Instant::now() + Duration::from_millis(limits.lease_ms),
            cancellation,
        }))
    }

    fn state(&self) -> MutexGuard<'_, State> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.error.get_or_insert_with(|| {
                    Arc::new(Error::new(
                        "STORAGE_FAILURE",
                        "RDF read admission state poisoned",
                    ))
                });
                state
            }
        }
    }

    fn ready(&self, state: &mut State) -> Result<(), ReadError> {
        if let Err(error) = self.cancellation.checkpoint() {
            state.error.get_or_insert_with(|| Arc::new(error));
        }
        if Instant::now() >= self.deadline {
            state.error.get_or_insert_with(|| {
                Arc::new(Error::new("RESULT_EXPIRED", "RDF read lease ended"))
            });
        }
        match &state.error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    pub(super) fn checkpoint(&self) -> (Option<ReadError>, RdfReadEvidence) {
        let mut state = self.state();
        let _ = self.ready(&mut state);
        (state.error.clone(), state.evidence.clone())
    }

    pub(super) fn fail(&self, error: Error) -> ReadError {
        let mut state = self.state();
        state.error.get_or_insert_with(|| Arc::new(error)).clone()
    }

    pub(super) fn request(&self, bytes: u64, row: bool) -> Result<(), ReadError> {
        self.admit(bytes, row, 1)
    }

    /// Admit the pinned catalog body before decoding, without inventing another read.
    pub(super) fn admit_bytes(&self, bytes: u64) -> Result<(), ReadError> {
        self.admit(bytes, false, 0)
    }

    fn admit(&self, bytes: u64, row: bool, request_count: u64) -> Result<(), ReadError> {
        let mut state = self.state();
        self.ready(&mut state)?;
        let requested = state
            .evidence
            .requests
            .checked_add(request_count)
            .zip(state.evidence.admitted_bytes.checked_add(bytes))
            .zip(state.evidence.row_visits.checked_add(u64::from(row)));
        let Some(((requests, admitted_bytes), row_visits)) = requested else {
            let error = Arc::new(Error::new(
                "RESOURCE_LIMIT",
                "RDF cumulative read admission overflow",
            ));
            state.error = Some(error.clone());
            return Err(error);
        };
        if requests > self.limits.requests
            || admitted_bytes > self.limits.io_bytes
            || row_visits > self.limits.row_visits
        {
            let error = Arc::new(Error::new(
                "RESOURCE_LIMIT",
                format!(
                    "RDF read request, byte or row budget exhausted: attempted requests={requests}/{}, io_bytes={admitted_bytes}/{}, row_visits={row_visits}/{}",
                    self.limits.requests, self.limits.io_bytes, self.limits.row_visits,
                ),
            ));
            state.error = Some(error.clone());
            return Err(error);
        }
        state.evidence.requests = requests;
        state.evidence.admitted_bytes = admitted_bytes;
        state.evidence.row_visits = row_visits;
        Ok(())
    }

    pub(super) fn reserve(self: &Arc<Self>, bytes: u64) -> Result<Reservation, ReadError> {
        let mut reservation = Reservation {
            budget: self.clone(),
            bytes: 0,
            control_bytes: 0,
        };
        reservation.resize(bytes)?;
        Ok(reservation)
    }

    pub(super) fn reserve_owned(
        self: &Arc<Self>,
        bytes: u64,
        scope: &Arc<Reservation>,
    ) -> Result<OwnedWorkspaceReservation<ReadError>, ReadError> {
        let control_bytes = size_of::<RetainedReservation>() as u64;
        let mut reservation = self.reserve(control_bytes)?;
        reservation.control_bytes = control_bytes;
        reservation.resize(bytes)?;
        purrdf_lex::allocation::try_boxed(RetainedReservation {
            reservation,
            _scope: Arc::clone(scope),
        })
        .map(|reservation| reservation as OwnedWorkspaceReservation<ReadError>)
        .map_err(|error| {
            self.fail(Error::new(
                "RESOURCE_LIMIT",
                format!("RDF owned workspace allocation failed: {error}"),
            ))
        })
    }

    pub(super) fn ceiling(&self) -> u64 {
        self.limits.live_bytes
    }
}

impl Reservation {
    pub(super) fn grow(&mut self, bytes: u64) -> Result<(), ReadError> {
        let requested = self.bytes.checked_add(bytes).ok_or_else(|| {
            self.budget
                .fail(Error::new("RESOURCE_LIMIT", "RDF scope capacity overflow"))
        })?;
        self.resize(requested)
    }
}

impl WorkspaceReservation for RetainedReservation {
    type Error = ReadError;

    fn resize(&mut self, bytes: u64) -> Result<(), ReadError> {
        self.reservation.resize(bytes)
    }
}

impl WorkspaceReservation for Reservation {
    type Error = ReadError;

    fn resize(&mut self, bytes: u64) -> Result<(), ReadError> {
        let mut state = self.budget.state();
        self.budget.ready(&mut state)?;
        let requested = state
            .evidence
            .live_bytes
            .checked_sub(self.bytes)
            .and_then(|live| live.checked_add(bytes))
            .and_then(|live| live.checked_add(self.control_bytes));
        let Some(requested) = requested.filter(|live| *live <= self.budget.limits.live_bytes)
        else {
            let error = Arc::new(Error::new(
                "RESOURCE_LIMIT",
                "RDF live workspace/guard budget exhausted",
            ));
            state.error = Some(error.clone());
            return Err(error);
        };
        state.evidence.live_bytes = requested;
        state.evidence.peak_live_bytes = state.evidence.peak_live_bytes.max(requested);
        self.bytes = bytes + self.control_bytes;
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut state = self.budget.state();
        match state.evidence.live_bytes.checked_sub(self.bytes) {
            Some(live) => state.evidence.live_bytes = live,
            None => {
                state.error.get_or_insert_with(|| {
                    Arc::new(Error::new(
                        "STORAGE_FAILURE",
                        "RDF reservation accounting underflow",
                    ))
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservations_release_capacity_but_never_clear_a_refusal() {
        let limits = RdfReadLimits {
            live_bytes: 128,
            ..Default::default()
        };
        let budget = Budget::new(limits).unwrap();
        let first = budget.reserve(64).unwrap();
        let second = budget.reserve(64).unwrap();
        assert_eq!(budget.checkpoint().1.live_bytes, 128);
        let error = budget.reserve(1).err().unwrap();
        assert_eq!(error.code, "RESOURCE_LIMIT");
        drop(first);
        drop(second);
        assert_eq!(budget.checkpoint().1.live_bytes, 0);
        assert_eq!(budget.checkpoint().1.peak_live_bytes, 128);
        assert!(Arc::ptr_eq(&error, &budget.reserve(0).err().unwrap()));
    }

    #[test]
    fn catalog_body_admission_shares_the_original_counters_and_sticky_refusal() {
        let budget = Budget::new(RdfReadLimits {
            requests: 1,
            row_visits: 1,
            io_bytes: 10,
            ..Default::default()
        })
        .unwrap();
        budget.request(2, true).unwrap();
        budget.admit_bytes(8).unwrap();
        let evidence = budget.checkpoint().1;
        assert_eq!(
            (
                evidence.requests,
                evidence.row_visits,
                evidence.admitted_bytes
            ),
            (1, 1, 10)
        );
        let error = budget.admit_bytes(1).unwrap_err();
        assert_eq!(error.code, "RESOURCE_LIMIT");
        assert_eq!(budget.checkpoint().1, evidence);
        assert!(Arc::ptr_eq(&error, &budget.request(0, false).unwrap_err()));
    }

    #[test]
    fn expired_lease_refuses_without_reading_or_waiting() {
        let mut budget = Budget::new(RdfReadLimits::default()).unwrap();
        Arc::get_mut(&mut budget).unwrap().deadline = Instant::now();
        assert_eq!(budget.request(1, false).unwrap_err().code, "RESULT_EXPIRED");
        assert_eq!(budget.checkpoint().1.requests, 0);
        assert_eq!(budget.checkpoint().1.admitted_bytes, 0);
    }

    #[test]
    fn cumulative_read_overflow_refuses_without_saturating_the_receipt() {
        let budget = Budget::new(RdfReadLimits {
            io_bytes: u64::MAX,
            ..Default::default()
        })
        .unwrap();
        budget.request(u64::MAX, true).unwrap();
        let before = budget.checkpoint().1;
        let error = budget.request(1, true).unwrap_err();
        assert_eq!(error.code, "RESOURCE_LIMIT");
        assert_eq!(budget.checkpoint().1, before);
        assert!(Arc::ptr_eq(&error, &budget.request(0, false).unwrap_err()));
    }

    #[test]
    fn source_refusal_identifies_attempted_limits_without_changing_consumed_evidence() {
        let budget = Budget::new(RdfReadLimits {
            io_bytes: 1,
            ..Default::default()
        })
        .unwrap();
        budget.request(1, true).unwrap();
        let before = budget.checkpoint().1;
        let error = budget.request(1, true).unwrap_err();
        assert_eq!(error.code, "RESOURCE_LIMIT");
        assert!(error.message.contains("requests=2/100000"));
        assert!(error.message.contains("io_bytes=2/1"));
        assert!(error.message.contains("row_visits=2/100000"));
        assert_eq!(budget.checkpoint().1, before);
        assert!(Arc::ptr_eq(&error, &budget.request(0, false).unwrap_err()));
    }

    #[test]
    fn owned_workspace_retains_its_control_charge_and_shared_budget() {
        let budget = Budget::new(RdfReadLimits {
            live_bytes: 256,
            ..Default::default()
        })
        .unwrap();
        let weak = Arc::downgrade(&budget);
        let scope = Arc::new(budget.reserve(16).unwrap());
        let mut retained = budget.reserve_owned(64, &scope).unwrap();
        let control = 16 + size_of::<RetainedReservation>() as u64;
        assert_eq!(budget.checkpoint().1.live_bytes, 64 + control);
        drop(scope);
        drop(budget);
        let observer = weak.upgrade().expect("retained output owns its account");
        retained.resize(0).unwrap();
        assert_eq!(observer.checkpoint().1.live_bytes, control);
        let competing = observer.reserve(256 - control).unwrap();
        let error = retained.resize(1).unwrap_err();
        assert_eq!(error.code, "RESOURCE_LIMIT");
        drop(competing);
        assert_eq!(observer.checkpoint().1.live_bytes, control);
        drop(retained);
        assert_eq!(observer.checkpoint().1.live_bytes, 0);
        assert!(Arc::ptr_eq(&error, &observer.reserve(0).err().unwrap()));
        drop(observer);
        assert!(weak.upgrade().is_none());
    }
}
