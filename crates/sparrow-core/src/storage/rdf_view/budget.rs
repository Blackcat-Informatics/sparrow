// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Sparrow-owned admission for a RocksDB RDF read and its native operator scratch.

use super::{Error, RdfReadEvidence, RdfReadLimits, ReadError};
use purrdf::dataset_view::WorkspaceReservation;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

pub(super) struct Budget {
    state: Mutex<State>,
    limits: RdfReadLimits,
    deadline: Instant,
}

struct State {
    error: Option<ReadError>,
    evidence: RdfReadEvidence,
}

pub(super) struct Reservation {
    budget: Arc<Budget>,
    bytes: u64,
}

impl Budget {
    pub(super) fn new(limits: RdfReadLimits) -> Result<Arc<Self>, ReadError> {
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
        let mut state = self.state();
        self.ready(&mut state)?;
        let requests = state.evidence.requests.saturating_add(1);
        let admitted_bytes = state.evidence.admitted_bytes.saturating_add(bytes);
        let row_visits = state.evidence.row_visits.saturating_add(u64::from(row));
        if requests > self.limits.requests
            || admitted_bytes > self.limits.io_bytes
            || row_visits > self.limits.row_visits
        {
            let error = Arc::new(Error::new(
                "RESOURCE_LIMIT",
                "RDF read request, byte or row budget exhausted",
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
        };
        reservation.resize(bytes)?;
        Ok(reservation)
    }

    pub(super) fn ceiling(&self) -> u64 {
        self.limits.live_bytes
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
            .and_then(|live| live.checked_add(bytes));
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
        self.bytes = bytes;
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
    fn expired_lease_refuses_without_reading_or_waiting() {
        let mut budget = Budget::new(RdfReadLimits::default()).unwrap();
        Arc::get_mut(&mut budget).unwrap().deadline = Instant::now();
        assert_eq!(budget.request(1, false).unwrap_err().code, "RESULT_EXPIRED");
        assert_eq!(budget.checkpoint().1.requests, 0);
        assert_eq!(budget.checkpoint().1.admitted_bytes, 0);
    }
}
