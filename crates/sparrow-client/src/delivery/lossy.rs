// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Disk-free loss accounting. Wire sequence and FIFO admission order can differ.

use super::*;

#[derive(Default)]
pub(super) struct Progress {
    pub(super) mismatch: Option<String>,
    pub(super) refusal: Option<Arc<Error>>,
    pub(super) sent: u64,
    pub(super) worker_losses: u64,
    pub(super) reported_losses: u64,
    cut: Option<Cut>,
}

struct Cut {
    admitted: u64,
    losses: Option<u64>,
}

impl State {
    fn settled(&self) -> u64 {
        self.admitted - self.count as u64
    }

    pub(super) fn capture_cut(&mut self) {
        self.lossy.cut = Some(Cut {
            admitted: self.admitted,
            losses: (self.count == 0).then_some(self.lossy.worker_losses),
        });
    }
    pub(super) fn cut_losses(&self) -> Option<u64> {
        self.lossy.cut.as_ref().and_then(|cut| cut.losses)
    }

    pub(super) fn cut_room(&self) -> usize {
        self.lossy
            .cut
            .as_ref()
            .filter(|cut| cut.losses.is_none())
            .map_or(usize::MAX, |cut| {
                usize::try_from(cut.admitted - self.settled()).unwrap_or(usize::MAX)
            })
    }

    /// Release a contiguous FIFO prefix with one outcome. A racing fence keeps the
    /// exact loss total at its prefix even if the observer is scheduled much later.
    pub(super) fn release_credit(
        &mut self,
        bytes: usize,
        count: usize,
        lost: bool,
    ) -> Result<(), Error> {
        let before = self.settled();
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or(Error::Protocol("producer byte ownership underflow"))?;
        self.count = self
            .count
            .checked_sub(count)
            .ok_or(Error::Protocol("producer event ownership underflow"))?;
        let after = self.settled();
        if let Some(cut) = &mut self.lossy.cut
            && cut.losses.is_none()
            && (before..=after).contains(&cut.admitted)
        {
            cut.losses =
                Some(self.lossy.worker_losses + if lost { cut.admitted - before } else { 0 });
        }
        if lost {
            self.lossy.worker_losses += count as u64;
        }
        Ok(())
    }
}
impl Shared {
    pub(super) fn discard_batch(
        &self,
        batch: &Arc<EventBatch>,
        error: Error,
        replacement: Option<String>,
        sent: bool,
    ) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Protocol("producer queue state poisoned"))?;
        let pending = state
            .pending
            .take()
            .ok_or(Error::Protocol("producer refusal has no pending batch"))?;
        if !Arc::ptr_eq(batch, &pending.batch) {
            return Err(Error::Protocol("producer refusal changed pending identity"));
        }
        let count = batch.events.len();
        if sent {
            state.lossy.sent = state.lossy.sent.max(
                batch
                    .events
                    .last()
                    .ok_or(Error::Protocol("refused empty batch"))?
                    .sequence,
            );
        }
        state.release_credit(pending.bytes, count, true)?;
        let mut discarded = count;
        let mut queue = VecDeque::new();
        if let Some(observed) = replacement {
            state.lossy.mismatch = Some(observed);
            queue = std::mem::take(&mut state.queue);
            let bytes = state.bytes;
            state.release_credit(bytes, queue.len(), true)?;
            discarded += queue.len();
        }
        self.drops[DropReason::Refused as usize].fetch_add(discarded as u64, Ordering::Relaxed);
        state.lossy.refusal = Some(Arc::new(error));
        state.transient = None;
        drop(state);
        drop(queue);
        self.notify_barrier();
        Ok(())
    }

    pub(super) fn rebind_memory(&self, store: &str) -> Result<(), Error> {
        if !crate::receipt::valid_store_id(store) {
            return Err(Error::InvalidArgument("invalid persistent store id"));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Protocol("producer queue state poisoned"))?;
        if state.lossy.mismatch.as_deref() != Some(store) {
            return Err(Error::InvalidArgument(
                "rebind requires the observed replacement store id",
            ));
        }
        state.store = state.lossy.mismatch.take();
        Ok(())
    }
}
