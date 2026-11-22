// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Independent disk intake and network replay. The admission mutex never owns file I/O.

use super::*;
use crate::{DeliveryMode, Durability, spool::Log};
use tokio::sync::oneshot;

/// The two workers signal through the fence's predicate and wait channels only
/// after releasing their reference to the journal. These fields own no disk handles.
pub(super) struct Completion {
    state: Arc<Mutex<State>>,
    changed: Arc<Condvar>,
    async_changed: Arc<tokio::sync::Notify>,
    room: Arc<tokio::sync::Notify>,
}

impl Completion {
    pub(super) fn new(shared: &Shared) -> Option<Self> {
        shared.spool.as_ref().map(|_| Self {
            state: shared.state.clone(),
            changed: shared.changed.clone(),
            async_changed: shared.async_changed.clone(),
            room: shared.intake_room.clone(),
        })
    }
    pub(super) fn release(self) {
        if let Ok(mut state) = self.state.lock() {
            state.released_workers += 1;
        }
        self.changed.notify_all();
        self.async_changed.notify_one();
        self.room.notify_waiters();
    }
    pub(super) fn both_released(state: &State) -> bool {
        state.released_workers == 2
    }
}

struct Offer {
    disk: u64,
    event: ProducerEvent,
    bytes: usize,
    token: Vec<u8>,
    ack: Option<oneshot::Sender<()>>,
}

fn reserve(bytes: usize, token: usize) -> u64 {
    (bytes + token + 128) as u64
}

impl Delivery {
    pub(super) fn refuse(&self, reason: Refusal) -> Admission {
        let shared = &self.shared;
        shared.bump(&shared.refusals[reason as usize]);
        if matches!(shared.config.mode, DeliveryMode::LosslessConfirmed(_)) {
            Admission::Refused { reason }
        } else {
            let drop = if reason == Refusal::SpoolFull {
                DropReason::SpoolFull
            } else {
                reason.drop_reason()
            };
            shared.reject(drop, None)
        }
    }

    fn permit(
        &self,
        offer: &Offer,
        exact: bool,
    ) -> Result<(std::sync::MutexGuard<'_, State>, u64), Refusal> {
        let shared = &self.shared;
        let config = shared.config.mode.spool().ok_or(Refusal::Invalid)?;
        let state = shared.state.lock().map_err(|_| Refusal::SpoolFailed)?;
        if state.failure.is_some() || shared.exhausted.load(Ordering::Acquire) {
            return Err(Refusal::SpoolFailed);
        }
        if state.closing || shared.aborted.load(Ordering::Acquire) {
            return Err(Refusal::Closed);
        }
        let used = state.spool.as_ref().ok_or(Refusal::SpoolFailed)?.bytes;
        let free = config.max_bytes.saturating_sub(used);
        if offer.disk > free {
            return Err(Refusal::SpoolFull);
        }
        if offer.disk > free.saturating_sub(state.reserved_disk) {
            return Err(if exact {
                Refusal::Busy
            } else {
                Refusal::SpoolFull
            });
        }
        if state.count >= shared.config.max_queued_events
            || offer.bytes + offer.token.len()
                > shared.config.max_queued_bytes.saturating_sub(state.bytes)
        {
            return Err(Refusal::Busy);
        }
        let sequence = state.sequence.checked_add(1).ok_or(Refusal::SpoolFailed)?;
        Ok((state, sequence))
    }

    fn enqueue(
        &self,
        mut state: std::sync::MutexGuard<'_, State>,
        sequence: u64,
        mut input: Offer,
    ) {
        input.event.sequence = sequence;
        state.sequence = sequence;
        state.admitted_sequence = sequence;
        state.admitted += 1;
        state.bytes += input.bytes + input.token.len();
        state.count += 1;
        state.reserved_disk += input.disk;
        state.queue.push_back(Queued {
            disk: input.disk,
            event: input.event,
            bytes: input.bytes,
            at: Instant::now(),
            token: input.token,
            ack: input.ack,
        });
        drop(state);
        self.shared.intake_work.notify_one();
    }

    pub(super) fn spool_offer(&self, event: ProducerEvent) -> Admission {
        self.shared.bump(&self.shared.offered);
        let Ok(bytes) = event.admission_bytes() else {
            return self.refuse(Refusal::Invalid);
        };
        let mut offer = Offer {
            disk: reserve(bytes, 0),
            event,
            bytes,
            token: Vec::new(),
            ack: None,
        };
        let mut admission = self.permit(&offer, false);
        if matches!(admission, Err(Refusal::SpoolFull))
            && matches!(self.shared.config.mode, DeliveryMode::LossySpooled(_))
        {
            offer.disk = Log::frame_bytes(&offer.event, &[]);
            admission = self.permit(&offer, true);
        }
        match admission {
            Ok((state, sequence)) => {
                self.enqueue(state, sequence, offer);
                Admission::Queued { sequence }
            }
            Err(reason) => self.refuse(reason),
        }
    }

    /// Append and synchronize an event on the local spool, independently of remote delivery.
    pub async fn send(&self, event: ProducerEvent) -> Result<SpoolReceipt, SendError> {
        self.send_with_token(event, &[]).await
    }

    /// Confirm local disk ownership of the event and its opaque application cursor together.
    /// Cancellation after memory admission leaves the accepted operation running.
    pub async fn send_with_token(
        &self,
        event: ProducerEvent,
        token: &[u8],
    ) -> Result<SpoolReceipt, SendError> {
        self.confirmed_offer(event, token, false).await
    }

    /// Wait for bounded intake room in lossy spooled mode. A synchronized event returns
    /// its receipt. Only an actual full on-disk segment budget drops valid input and
    /// returns `Ok(None)`. Setup, closed admission and storage failures remain errors.
    pub async fn offer_spooled(
        &self,
        event: ProducerEvent,
    ) -> Result<Option<SpoolReceipt>, SendError> {
        match self.confirmed_offer(event, &[], true).await {
            Ok(receipt) => Ok(Some(receipt)),
            Err(SendError::Refused {
                reason: Refusal::SpoolFull,
                ..
            }) => {
                self.shared.reject(DropReason::SpoolFull, None);
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    async fn confirmed_offer(
        &self,
        event: ProducerEvent,
        token: &[u8],
        backpressure: bool,
    ) -> Result<SpoolReceipt, SendError> {
        let refused = |reason, event| {
            self.shared.bump(&self.shared.refusals[reason as usize]);
            Err(SendError::Refused {
                reason,
                event: Box::new(event),
            })
        };
        // A disk confirmation API cannot offer an event on a disk-free handle.
        if self.shared.config.disk_free() {
            return refused(Refusal::Invalid, event);
        }
        self.shared.bump(&self.shared.offered);
        let Ok(bytes) = event.admission_bytes() else {
            return refused(Refusal::Invalid, event);
        };
        let Some(config) = self.shared.config.mode.spool() else {
            return refused(Refusal::Invalid, event);
        };
        if token.len() > 256
            || backpressure && !matches!(self.shared.config.mode, DeliveryMode::LossySpooled(_))
        {
            return refused(Refusal::Invalid, event);
        }
        if bytes + token.len() > self.shared.config.max_queued_bytes {
            return refused(Refusal::Invalid, event);
        }
        let deadline = tokio::time::Instant::now() + config.max_wait;
        let (tx, rx) = oneshot::channel();
        let offer = Offer {
            disk: if backpressure {
                Log::frame_bytes(&event, token)
            } else {
                reserve(bytes, token.len())
            },
            event,
            bytes,
            token: token.to_owned(),
            ack: Some(tx),
        };
        let sequence = loop {
            let room = self.shared.intake_room.notified();
            tokio::pin!(room);
            room.as_mut().enable();
            let reason = match self.permit(&offer, backpressure) {
                Ok((state, seq)) => {
                    self.enqueue(state, seq, offer);
                    break seq;
                }
                Err(reason) => reason,
            };
            match reason {
                Refusal::Busy if backpressure => room.await,
                Refusal::Busy if tokio::time::Instant::now() < deadline => {
                    let _ = tokio::time::timeout_at(deadline, room).await;
                }
                reason => return refused(reason, offer.event),
            }
        };
        match rx.await {
            Ok(()) => Ok(SpoolReceipt {
                client_instance: self.shared.instance,
                sequence,
            }),
            Err(_) => Err(SendError::Uncertain { sequence }),
        }
    }

    /// Cursor on the latest local spool checkpoint, retained after segment cleanup.
    pub fn accepted_token(&self) -> Result<Vec<u8>, FlushError> {
        Ok(self
            .shared
            .barrier_state()?
            .spool
            .as_ref()
            .ok_or(FlushError::Unavailable("delivery has no spool"))?
            .accepted_token
            .clone())
    }

    /// Cursor on the latest certified remote receipt, retained across process restarts.
    pub fn confirmed_token(&self) -> Result<Vec<u8>, FlushError> {
        Ok(self
            .shared
            .barrier_state()?
            .spool
            .as_ref()
            .ok_or(FlushError::Unavailable("delivery has no spool"))?
            .confirmed_token
            .clone())
    }

    /// Wait for fast offers through the captured sequence to reach local disk, even offline.
    pub async fn flush_local(&self, timeout: Duration) -> Result<u64, FlushError> {
        let target = {
            let mut state = self.shared.barrier_state()?;
            let target = state.admitted_sequence;
            state.force_local = state.force_local.max(target);
            target
        };
        self.shared.intake_work.notify_one();
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or(DEADLINE_RANGE)?;
        loop {
            let ready = self.shared.intake_room.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            {
                let state = self.shared.barrier_state()?;
                if let Some(error) = &state.failure {
                    return Err(FlushError::Delivery(error.clone()));
                }
                let synced = state
                    .spool
                    .as_ref()
                    .ok_or(FlushError::Unavailable("delivery has no spool"))?
                    .synced_sequence;
                if synced >= target {
                    return Ok(synced);
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(FlushError::TimedOut(Box::new(self.shared.stats(&state))));
                }
            }
            let _ = tokio::time::timeout_at(deadline, ready).await;
        }
    }

    /// Ask the network worker to retry a held prefix immediately.
    pub fn retry_held(&self) {
        self.shared.operator_retry.notify_waiters();
        self.shared.retry_wake.notify_one();
    }

    /// Close admission, synchronize every fast offer and stop both workers without
    /// waiting for the server. Reopen the directory after releasing all other handles.
    pub async fn close_local(self, timeout: Duration) -> Result<u64, FlushError> {
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or(DEADLINE_RANGE)?;
        {
            let mut state = self.shared.barrier_state()?;
            if state.spool.is_none() {
                return Err(FlushError::Unavailable("delivery has no spool"));
            }
            state.closing = true;
        }
        self.shared.intake_room.notify_waiters();
        self.shared.intake_work.notify_all();
        let synced = self.flush_local(timeout).await?;
        self.shared.aborted.store(true, Ordering::Release);
        self.shared.abort_wake.notify_one();
        self.shared.wake.notify_one();
        loop {
            let released = self.shared.intake_room.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            if Completion::both_released(&*self.shared.barrier_state()?) {
                return Ok(synced);
            }
            if tokio::time::timeout_at(deadline, released).await.is_err() {
                return Err(FlushError::TimedOut(Box::new(self.stats()?)));
            }
        }
    }

    /// Bind an unsealed prefix to an explicitly named store. An unresolved seal stays on
    /// its original store until an exact receipt establishes its outcome.
    pub async fn rebind(&self, store: &str) -> Result<(), Error> {
        if self.shared.config.disk_free() {
            return self.shared.rebind_memory(store);
        }
        let spool = self
            .shared
            .spool
            .clone()
            .ok_or(Error::InvalidArgument("delivery has no spool"))?;
        let store = store.to_owned();
        tokio::task::spawn_blocking(move || {
            spool
                .lock()
                .map_err(|_| Error::Protocol("spool lock poisoned"))?
                .rebind(&store)
        })
        .await
        .map_err(std::io::Error::other)??;
        self.retry_held();
        Ok(())
    }
}

pub(super) fn start_intake(shared: &Arc<Shared>) -> Result<(), Error> {
    let worker = shared.clone();
    let completed =
        Completion::new(shared).ok_or(Error::Protocol("spool intake requires a disk journal"))?;
    std::thread::Builder::new()
        .name("sparrow-spool".into())
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| intake(&worker)));
            if let Some(error) = match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(_) => Some(Error::Protocol("spool intake panicked")),
            } {
                worker.fail(error);
                // Intake has stopped. Even a poisoned queue retains its ack
                // senders; release them so assigned offers learn their outcome
                // is uncertain. Leave admission closed on the poisoned state.
                let mut state = worker
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                for queued in &mut state.queue {
                    queued.ack.take();
                }
            }
            worker.intake_room.notify_waiters();
            worker.wake.notify_one();
            drop(worker);
            completed.release();
        })?;
    Ok(())
}

fn disk(shared: &Shared) -> Result<std::sync::MutexGuard<'_, Log>, Error> {
    shared
        .spool
        .as_ref()
        .ok_or(Error::Protocol("configured spool is missing"))?
        .lock()
        .map_err(|_| Error::Protocol("spool lock poisoned"))
}

fn state(shared: &Shared) -> Result<std::sync::MutexGuard<'_, State>, Error> {
    shared
        .state
        .lock()
        .map_err(|_| Error::Protocol("producer queue state poisoned"))
}

fn intake(shared: &Shared) -> Result<(), Error> {
    let interval = shared
        .config
        .mode
        .spool()
        .ok_or(Error::Protocol("disk intake requires spool limits"))?
        .sync_interval;
    let mut synced_at = Instant::now();
    loop {
        let (group, force): (Vec<_>, bool) = {
            let mut state = state(shared)?;
            loop {
                let stats = state
                    .spool
                    .as_ref()
                    .ok_or(Error::Protocol("missing spool progress"))?;
                let dirty = stats.synced_sequence < stats.spooled_sequence;
                let done = state.closing
                    || state.failure.is_some()
                    || shared.aborted.load(Ordering::Acquire);
                let force = done
                    || state.force_local > stats.synced_sequence
                    || synced_at.elapsed() >= interval;
                if !state.queue.is_empty() || dirty && force {
                    let count = state.queue.len().min(shared.config.batch_events);
                    break (state.queue.drain(..count).collect(), force);
                }
                if done {
                    return Ok(());
                }
                state = if dirty {
                    shared
                        .intake_work
                        .wait_timeout(state, interval.saturating_sub(synced_at.elapsed()))
                        .map_err(|_| Error::Protocol("spool sync wait poisoned"))?
                        .0
                } else {
                    shared
                        .intake_work
                        .wait(state)
                        .map_err(|_| Error::Protocol("spool intake wait poisoned"))?
                };
            }
        };
        let mut log = disk(shared)?;
        let at = crate::unix_millis_now()?;
        for item in &group {
            log.append(&item.event, &item.token, at)?;
        }
        if force || group.iter().any(|item| item.ack.is_some()) {
            log.sync()?;
            synced_at = Instant::now();
        }
        {
            let mut state = state(shared)?;
            state.spool = Some(log.stats.clone());
            for item in &group {
                state.count -= 1;
                state.bytes -= item.bytes + item.token.len();
                state.reserved_disk -= item.disk;
            }
        }
        drop(log);
        for item in group {
            if let Some(ack) = item.ack {
                let _ = ack.send(());
            }
        }
        shared.intake_room.notify_waiters();
        shared.wake.notify_one();
    }
}

pub(super) async fn deliver(shared: &Shared) -> Result<(), Error> {
    let mut connection: Option<Client> = None;
    let mut certified: Option<(crate::spool::Prepared, ArtifactReceipt)> = None;
    let mut discovered_at = Instant::now();
    let mut ready_since = None;
    let mut backoff = Backoff::new(&shared.config);
    loop {
        if shared.aborted.load(Ordering::Acquire) {
            return Ok(());
        }
        let (wait, delay, cleanup) = {
            let state = state(shared)?;
            let spool = state
                .spool
                .as_ref()
                .ok_or(Error::Protocol("missing spool progress"))?;
            if state.failure.is_some()
                || state.closing
                    && state.durable_sequence >= state.admitted_sequence
                    && !spool.cleanup_pending
            {
                return Ok(());
            }
            let pending = spool
                .spooled_sequence
                .saturating_sub(state.durable_sequence);
            let delay = if pending > 0
                && pending < shared.config.batch_events as u64
                && spool.bytes < shared.config.batch_bytes as u64
                && !state.closing
                && state.force_through <= state.durable_sequence
                && spool.sealed_batch.is_none()
            {
                let at = ready_since.get_or_insert_with(Instant::now);
                shared.config.flush_interval.saturating_sub(at.elapsed())
            } else {
                Duration::ZERO
            };
            (pending == 0, delay, spool.cleanup_pending)
        };
        if wait && !cleanup {
            ready_since = None;
            tokio::select! { _=shared.wake.notified()=>{}, _=shared.abort_wake.notified()=>return Ok(()) };
            continue;
        }
        if !delay.is_zero() && !cleanup {
            tokio::select! { _=shared.wake.notified()=>{}, _=tokio::time::sleep(delay)=>{}, _=shared.abort_wake.notified()=>return Ok(()) }
            continue;
        }
        let attempt = async {
            if cleanup {
                let mut log = disk(shared)?;
                log.finish_confirmation()?;
                let mut state = state(shared)?;
                state.spool = Some(log.stats.clone());
                state.transient = None;
                return Ok(());
            }
            // Once the server certified this batch, only the disk checkpoint remains.
            // Keep that evidence across ENOSPC or sync failures, including after the
            // server's retry window closes or its socket becomes unavailable.
            if let Some((prepared, receipt)) = &certified {
                let mut log = disk(shared)?;
                log.confirm(prepared, &receipt.id)?;
                let mut state = state(shared)?;
                state.spool = Some(log.stats.clone());
                state.durable_sequence = log.head.confirmed;
                state.durable = log.head.confirmed;
                state.receipt = certified.take().map(|(_, receipt)| receipt);
                state.pending = None;
                state.transient = None;
                state.held = None;
                return Ok(());
            }
            // Discovery samples the server clock. Renew a long-idle connection before
            // preparing new seals. A recovered seal always keeps its original deadline.
            if connection.is_none() || discovered_at.elapsed() >= Duration::from_secs(60) {
                drop(connection.take());
                connection = Some(
                    Client::open(
                        &shared.config.endpoint,
                        shared.config.expect.as_ref(),
                        crate::receipt::Contract::Producer,
                    )
                    .await?,
                );
                discovered_at = Instant::now();
            }
            let client = connection
                .as_ref()
                .ok_or(Error::Protocol("spool connection missing"))?;
            let info = client.discovery();
            let limits = info
                .event_limits()
                .bounded(shared.config.batch_events, shared.config.batch_bytes);
            let prepared = {
                let mut log = disk(shared)?;
                let prepared = log
                    .prepare(&info.store_id, info.sealed_event_deadline_ms, limits)?
                    .ok_or(Error::Protocol("spool pending range vanished"))?;
                let mut state = state(shared)?;
                state.spool = Some(log.stats.clone());
                state.pending = Some(Pending {
                    original: Default::default(),
                    batch: prepared.batch.clone(),
                    bytes: 0,
                    at: Instant::now(),
                });
                prepared
            };
            shared.intake_room.notify_waiters();
            let receipt = client
                .submit_events(
                    &prepared.batch,
                    Durability::Durable,
                    Some(prepared.deadline),
                )
                .await?;
            certified = Some((prepared, receipt));
            Ok::<_, Error>(())
        };
        let result = tokio::select! { _=shared.abort_wake.notified()=>return Ok(()), result=tokio::time::timeout(shared.config.rpc_timeout,attempt)=> result.unwrap_or_else(|_| Err(Error::Io(std::io::Error::new(std::io::ErrorKind::TimedOut,"sealed event RPC timed out")))) };
        match result {
            Ok(()) => {
                ready_since = None;
                backoff.reset();
                shared.notify_barrier();
                shared.intake_room.notify_waiters();
            }
            Err(error) => {
                let manual = connection.is_some()
                    && !cleanup
                    && certified.is_none()
                    && (disposition(&error) == Disposition::SetAside
                        || matches!(&error,Error::Domain{code,..} if code == STORE_MISMATCH));
                connection = None;
                if manual {
                    // Register before publishing the reason so an immediate operator reply
                    // cannot be missed. Ordinary flushes and further appends cannot resume it.
                    let retry = shared.operator_retry.notified();
                    tokio::pin!(retry);
                    retry.as_mut().enable();
                    shared.retry(error, true)?;
                    tokio::select! { _=retry=>{}, _=shared.abort_wake.notified()=>return Ok(()) }
                    continue;
                }
                let delay = if matches!(error, Error::Unsupported(crate::PRODUCER_PROFILE)) {
                    Duration::from_secs(30)
                } else {
                    backoff.take()
                };
                shared.retry(error, false)?;
                tokio::select! { _=tokio::time::sleep(delay)=>{}, _=shared.retry_wake.notified()=>{}, _=shared.abort_wake.notified()=>return Ok(()) }
            }
        }
    }
}
