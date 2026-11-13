// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The UDP side of the syslog listener: a reader that never stops reading except when its queue
//! is full, and a publisher that stores the queue in order (#255).

use super::*;

/// A UDP reader's full-queue waits that follow one another until the queue next drains are one
/// episode for the journal, with its own announcement state.
#[derive(Clone)]
pub(super) struct FullEpisode {
    pub(super) waits: u64,
    pub(super) longest: Duration,
    pub(super) announced: bool,
    /// The kernel drop count when the episode began, when it could be read.
    pub(super) drops_at_start: Option<u64>,
}

impl Metrics {
    /// The reader stops on a full queue. The kernel count is read first so the episode's
    /// start is a fresh figure.
    fn full_wait_begin(&self, held: usize) -> io::Result<()> {
        self.sample_kernel()?;
        self.full_wait(|status, now| {
            status.in_hand = Some(held);
            status.begin_full_wait(now)
        })
    }
    /// An observer tick while the reader waits: announce the episode if the interval allows.
    fn full_wait_tick(&self) -> io::Result<()> {
        self.sample_kernel()?;
        self.full_wait(|status, now| Ok(status.announce_full(now)))
    }
    /// The reader has room again.
    fn full_wait_end(&self) -> io::Result<()> {
        self.sample_kernel()?;
        self.full_wait(|status, now| Ok(status.end_full_wait(now)))
    }
    fn full_wait(
        &self,
        step: impl FnOnce(&mut Status, tokio::time::Instant) -> io::Result<Option<String>>,
    ) -> io::Result<()> {
        let mut line = None;
        self.update(|status| {
            line = step(status, tokio::time::Instant::now())?;
            Ok(())
        })?;
        if let Some(line) = line {
            journal(&line);
        }
        Ok(())
    }
    /// One datagram entered the queue at `cost` budget bytes.
    fn enqueued(&self, cost: usize) -> io::Result<()> {
        self.update(|status| {
            status.queued_datagrams += 1;
            status.queued_bytes += cost as u64;
            Ok(())
        })
    }
    /// One datagram left the queue, stored or counted. When that empties the queue after an
    /// episode, the kernel count is read once more and the full-queue episode ends.
    fn dequeued(&self, cost: usize) -> io::Result<()> {
        let mut sample = false;
        self.update(|status| {
            status.queued_datagrams = status
                .queued_datagrams
                .checked_sub(1)
                .ok_or_else(|| io::Error::other("syslog queue counter underflow"))?;
            status.queued_bytes = status
                .queued_bytes
                .checked_sub(cost as u64)
                .ok_or_else(|| io::Error::other("syslog queue byte counter underflow"))?;
            sample = status.queued_datagrams == 0 && status.sample_due;
            Ok(())
        })?;
        if sample {
            self.sample_kernel()?;
            let mut line = None;
            self.update(|status| {
                status.sample_due = false;
                line = status.settle_full();
                Ok(())
            })?;
            if let Some(line) = line {
                journal(&line);
            }
        }
        Ok(())
    }
    /// Count one datagram read and not stored.
    fn lose(&self, payload: usize) -> io::Result<()> {
        self.update(|status| {
            increment(&mut status.counts.lost_at_shutdown)?;
            status.counts.lost_at_shutdown_bytes = status
                .counts
                .lost_at_shutdown_bytes
                .saturating_add(payload as u64);
            Ok(())
        })
    }
    /// The reader failed holding a datagram it had not queued: count it lost and end its wait.
    fn lose_unqueued(&self, payload: usize, error: &io::Error) -> io::Result<()> {
        self.update(|status| {
            increment(&mut status.counts.lost_at_shutdown)?;
            status.counts.lost_at_shutdown_bytes = status
                .counts
                .lost_at_shutdown_bytes
                .saturating_add(payload as u64);
            // A line for an episode that ends here would say the queue drained.
            let _ = status.end_full_wait(tokio::time::Instant::now());
            status.failure = Some(format!(
                "UDP reader failed: {error}; the datagram just read was lost"
            ));
            Ok(())
        })
    }
    /// The publisher failed: everything still queued, and the datagram the reader holds while it
    /// waits for room, is lost. Counts it, ends the reader's wait (the reader goes with the
    /// publisher, so the wait gauge must stop growing), records the failure and returns the
    /// datagrams and bytes lost for the error text.
    fn abandon_queue(&self, error: &io::Error) -> io::Result<(usize, u64)> {
        let mut lost = (0, 0);
        self.update(|status| {
            let held = status.in_hand.take();
            lost = (
                status.queued_datagrams + usize::from(held.is_some()),
                status
                    .queued_bytes
                    .saturating_sub((status.queued_datagrams * ENTRY_OVERHEAD) as u64)
                    + held.unwrap_or(0) as u64,
            );
            // A line for an episode that ends here would say the queue drained.
            let _ = status.end_full_wait(tokio::time::Instant::now());
            status.queued_datagrams = 0;
            status.queued_bytes = 0;
            status.counts.lost_at_shutdown =
                status.counts.lost_at_shutdown.saturating_add(lost.0 as u64);
            status.counts.lost_at_shutdown_bytes =
                status.counts.lost_at_shutdown_bytes.saturating_add(lost.1);
            status.failure = Some(format!(
                "UDP publisher failed: {error}; {} queued datagram(s) lost",
                lost.0
            ));
            Ok(())
        })?;
        Ok(lost)
    }
}

impl Status {
    /// A datagram was stored just past the archive watermark; returns the journal line to print,
    /// at most one a report interval, since a clock behind the watermark affects every datagram
    /// until it recovers. The count is in status.
    pub(super) fn past_watermark_line(
        &mut self,
        read_at: u64,
        commit: u64,
        stamp: u64,
        now: tokio::time::Instant,
    ) -> Option<String> {
        if self
            .fenced_announced_at
            .is_some_and(|at| now.duration_since(at) < HOLD_REPORT_INTERVAL)
        {
            return None;
        }
        self.fenced_announced_at = Some(now);
        Some(format!(
            "sparrowd: syslog listener {}: stored a datagram just past the archive watermark because the clock is behind it ({} so far); read at {read_at} ms, clock {commit} ms, stamped {stamp} ms; its received_at is that fence-adjusted time, not the clock",
            self.definition.id, self.counts.restamped_past_watermark
        ))
    }

    /// The reader stops on a full queue at `now`; returns the journal line to print, if any.
    fn begin_full_wait(&mut self, now: tokio::time::Instant) -> io::Result<Option<String>> {
        increment(&mut self.counts.reader_full_waits)?;
        self.waiting_since = Some(now);
        self.sample_due = true;
        let drops = self.kernel_drops();
        self.full
            .get_or_insert(FullEpisode {
                waits: 0,
                longest: Duration::ZERO,
                announced: false,
                drops_at_start: drops,
            })
            .waits += 1;
        Ok(self.announce_full(now))
    }

    /// Announce a full-queue episode the report interval suppressed, once it has passed. Called
    /// at every wait's start and on every observer tick during a wait, as a pressure hold is at
    /// every offer, so a long wait is never left unannounced.
    fn announce_full(&mut self, now: tokio::time::Instant) -> Option<String> {
        let quiet = self
            .full_announced_at
            .is_none_or(|at| now.duration_since(at) >= HOLD_REPORT_INTERVAL);
        let (queued, bytes, bound) = (
            self.queued_datagrams,
            self.queued_bytes,
            self.definition.hold_bytes(),
        );
        let drops = self.kernel_drops();
        let episode = self
            .full
            .as_mut()
            .filter(|episode| !episode.announced && quiet)?;
        episode.announced = true;
        self.full_announced_at = Some(now);
        let drops = drops.map_or_else(|| "unknown".to_owned(), |drops| drops.to_string());
        Some(format!(
            "sparrowd: syslog listener {}: receive queue full ({queued} datagram(s), {bytes} of {bound} byte(s)); the reader waits for the publisher, so the kernel buffer takes arrivals and drops the newest when it fills (kernel drops so far {drops})",
            self.definition.id
        ))
    }

    /// The reader has room again at `now`.
    fn end_full_wait(&mut self, now: tokio::time::Instant) -> Option<String> {
        self.in_hand = None;
        if let Some(since) = self.waiting_since.take() {
            let waited = now.duration_since(since);
            self.counts.reader_full_longest_ms = self
                .counts
                .reader_full_longest_ms
                .max(u64::try_from(waited.as_millis()).unwrap_or(u64::MAX));
            if let Some(episode) = self.full.as_mut() {
                episode.longest = episode.longest.max(waited);
            }
        }
        self.settle_full()
    }

    /// End the episode once the reader is not waiting and the queue is empty; returns the
    /// leaving line when the entering line was printed.
    fn settle_full(&mut self) -> Option<String> {
        if self.waiting_since.is_some() || self.queued_datagrams != 0 {
            return None;
        }
        let drops = self.kernel_drops();
        let episode = self.full.take().filter(|episode| episode.announced)?;
        let dropped = match (episode.drops_at_start, drops) {
            (Some(start), Some(now)) => now.saturating_sub(start).to_string(),
            _ => "an unknown number of".to_owned(),
        };
        Some(format!(
            "sparrowd: syslog listener {}: receive queue drained; the reader waited {} time(s), longest {} ms; the kernel dropped {dropped} datagram(s) meanwhile",
            self.definition.id,
            episode.waits,
            episode.longest.as_millis()
        ))
    }
}

/// The instant a datagram is stamped with, read when the reader takes it off the socket.
pub(super) type Clock = Arc<dyn Fn() -> io::Result<u64> + Send + Sync>;

pub(super) fn wall_clock() -> Clock {
    Arc::new(|| crate::now().map_err(io::Error::other))
}

/// One datagram waiting for the publisher. Its budget returns when the entry is dropped.
struct Entry {
    bytes: Arc<[u8]>,
    read_at: u64,
    /// Budget bytes this entry holds: its length plus [`ENTRY_OVERHEAD`].
    cost: usize,
    _budget: OwnedSemaphorePermit,
}

/// Take UDP datagrams off the socket as fast as they arrive and queue them for the publisher.
///
/// The reader never calls the store and never waits for a permit. It stops reading only when the
/// queue's byte budget cannot cover the datagram in hand, and then waits for the publisher to
/// free budget; a datagram arriving meanwhile stays in the kernel buffer, which drops the newest
/// when it fills and counts it. Nothing is dropped here. On a stop the reader drains the socket,
/// up to the queue bound, and closes the queue so the publisher can finish it. The publisher
/// ending first is a failure and drops the reader with it, so nothing keeps filling a queue that
/// nobody drains.
pub(super) async fn listen_udp(
    socket: UdpSocket,
    metrics: Metrics,
    store: sparrow_core::Store,
    operations: Arc<Semaphore>,
    stop: watch::Receiver<bool>,
    wake: PressureWake,
    clock: Clock,
) -> io::Result<()> {
    let definition = metrics.snapshot()?.definition;
    let bound = definition.hold_bytes();
    if bound < sparrow_core::SYSLOG_MESSAGE_BYTES + ENTRY_OVERHEAD {
        return Err(io::Error::other(format!(
            "syslog listener {}: hold_bytes {bound} cannot hold one maximal datagram",
            definition.id
        )));
    }
    let (queue, entries) = mpsc::unbounded_channel();
    let serving = {
        let reader = Reader {
            socket: &socket,
            queue,
            budget: Arc::new(Semaphore::new(bound)),
            metrics: &metrics,
            wake: wake.subscribe(),
            clock,
            last: 0,
            buffer: vec![0; sparrow_core::SYSLOG_MESSAGE_BYTES],
        }
        .read(bound, stop.clone());
        let publisher = publish_queue(
            entries,
            &definition.source,
            &store,
            &metrics,
            &operations,
            stop,
            wake.subscribe(),
        );
        tokio::pin!(reader, publisher);
        tokio::select! {
            read = &mut reader => {
                // The queue's sender went with the reader: the publisher finishes what is queued.
                let published = (&mut publisher).await;
                published.and(read)
            }
            published = &mut publisher => published.and(Err(io::Error::other(
                "the UDP syslog publisher ended while the reader was running",
            ))),
        }
    };
    // The last kernel sample, taken while the socket is open and then frozen, whether the loop
    // ended by stop or by an error: the socket's row leaves the table when it closes, and a
    // later status request must still show this count as final.
    let finished = metrics.finish_kernel();
    serving.and(finished)
}

struct Reader<'a> {
    socket: &'a UdpSocket,
    queue: mpsc::UnboundedSender<Entry>,
    budget: Arc<Semaphore>,
    metrics: &'a Metrics,
    wake: Waiter,
    clock: Clock,
    /// The last stamp, so a wall clock stepping back stalls the stamp instead of reversing it.
    last: u64,
    /// A UDP payload never exceeds 65507 bytes, under the store's ceiling, so a whole datagram
    /// always fits and nothing is cut; `max_message_bytes` frames TCP lines only.
    buffer: Vec<u8>,
}

impl Reader<'_> {
    /// Read until a stop, then drain the socket; `bound` is the queue's byte budget.
    async fn read(mut self, bound: usize, mut stop: watch::Receiver<bool>) -> io::Result<()> {
        loop {
            if *stop.borrow() {
                break;
            }
            let size = tokio::select! {
                changed = stop.changed() => { changed.map_err(io::Error::other)?; continue; },
                received = self.socket.recv_from(&mut self.buffer) => received?.0,
            };
            self.enqueue(size).await?;
        }
        // Datagrams already in the kernel at the stop are kept, up to the queue bound: an
        // arrival rate that never lets the buffer empty cannot hold the stop. The read is a
        // plain non-blocking receive, which does not depend on the runtime having seen the
        // socket ready.
        let mut drained = 0;
        while drained < bound {
            let received = rustix::net::recvfrom(
                self.socket,
                &mut self.buffer[..],
                rustix::net::RecvFlags::DONTWAIT,
            );
            let size = match received {
                Ok((size, _, _)) => size,
                Err(rustix::io::Errno::AGAIN) => break,
                Err(error) => return Err(error.into()),
            };
            drained += size + ENTRY_OVERHEAD;
            self.enqueue(size).await?;
        }
        Ok(())
    }

    /// Stamp the datagram in the buffer, take its budget and queue it.
    async fn enqueue(&mut self, size: usize) -> io::Result<()> {
        let read_at = match (self.clock)() {
            Ok(read_at) => read_at.max(self.last),
            Err(error) => return self.lose_in_hand(size, error),
        };
        self.last = read_at;
        let cost = size + ENTRY_OVERHEAD;
        let wanted = u32::try_from(cost).map_err(io::Error::other)?;
        let budget = match self.budget.clone().try_acquire_many_owned(wanted) {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => match self.wait_for_room(wanted, size).await {
                Ok(permit) => permit,
                // The wait ends with the error: the datagram in hand is lost and counted, and
                // the wait gauge stops (a tick, the budget closing).
                Err(error) => return self.lose_in_hand(size, error),
            },
            Err(error @ TryAcquireError::Closed) => {
                return self.lose_in_hand(size, io::Error::other(error));
            }
        };
        self.metrics.enqueued(cost)?;
        self.queue
            .send(Entry {
                bytes: Arc::from(&self.buffer[..size]),
                read_at,
                cost,
                _budget: budget,
            })
            .map_err(|_| io::Error::other("the UDP syslog publisher ended"))
    }

    /// The reader fails with a datagram it has taken off the socket and not queued: count it
    /// lost, end any wait it was in so the wait gauge stops growing, and return the error naming
    /// the loss.
    fn lose_in_hand(&self, size: usize, error: io::Error) -> io::Result<()> {
        self.metrics.lose_unqueued(size, &error)?;
        Err(io::Error::new(
            error.kind(),
            format!("{error}; the datagram just read ({size} byte(s)) was counted lost"),
        ))
    }

    /// Wait for the publisher to free `wanted` budget bytes with the datagram in hand. The wait
    /// blocks on the budget and, so the journal can announce a long wait the report interval
    /// suppressed at entry, on the observer tick (counted as held so the tick sends it); it owns
    /// no timer. The one acquire future is kept across ticks to keep its place in the queue. A
    /// stop does not end it: the datagram in hand is still published.
    async fn wait_for_room(
        &mut self,
        wanted: u32,
        size: usize,
    ) -> io::Result<OwnedSemaphorePermit> {
        self.metrics.full_wait_begin(size)?;
        let _held = self.wake.hold();
        self.wake.mark_seen();
        let acquire = self.budget.clone().acquire_many_owned(wanted);
        tokio::pin!(acquire);
        let permit = loop {
            tokio::select! {
                permit = &mut acquire => break permit.map_err(io::Error::other)?,
                () = self.wake.woken() => self.metrics.full_wait_tick()?,
            }
        };
        self.metrics.full_wait_end()?;
        Ok(permit)
    }
}

/// Publish the queue in order, one datagram at a time, each through [`publish_held`].
///
/// An error from the store returns at once, which stops the reader and the daemon: the datagrams
/// still queued cannot be published, and they are counted and named in the error. On a stop under
/// pressure the first refused datagram and everything after it are counted once, as
/// `lost_at_shutdown`, not as one durable discard each.
async fn publish_queue(
    mut entries: mpsc::UnboundedReceiver<Entry>,
    source: &str,
    store: &sparrow_core::Store,
    metrics: &Metrics,
    operations: &Arc<Semaphore>,
    mut stop: watch::Receiver<bool>,
    mut wake: Waiter,
) -> io::Result<()> {
    let mut abandoned = false;
    let mut lost = (0_u64, 0_u64);
    while let Some(entry) = entries.recv().await {
        if !abandoned {
            let published = publish_held(
                store,
                source,
                Packet {
                    bytes: entry.bytes.clone(),
                    intake: Intake::Udp {
                        read_at: entry.read_at,
                    },
                },
                metrics,
                operations,
                &mut stop,
                &mut wake,
            )
            .await;
            match published {
                Ok(Published::Pressured) => abandoned = true,
                Ok(_) => {}
                Err(error) => {
                    let (datagrams, bytes) = metrics.abandon_queue(&error)?;
                    return Err(io::Error::new(
                        error.kind(),
                        format!(
                            "{error}; {datagrams} queued datagram(s), {bytes} byte(s), cannot be published; the UDP listener stops"
                        ),
                    ));
                }
            }
        }
        if abandoned {
            let payload = entry.bytes.len();
            metrics.lose(payload)?;
            lost = (lost.0 + 1, lost.1 + payload as u64);
        }
        metrics.dequeued(entry.cost)?;
    }
    if lost.0 != 0 {
        let id = metrics.snapshot()?.definition.id;
        journal(&format!(
            "sparrowd: syslog listener {id}: stopped under storage pressure; {} datagram(s), {} byte(s), were read and not stored (lost at shutdown)",
            lost.0, lost.1
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::syslog_listener::{
        printed,
        tests::{
            Listener, frame_taken, listener_with, relieve, settle, status_json, stored_lengths,
            yield_until,
        },
    };

    /// A UDP listener under test: its stamping clock is injected, so datagram `n` is read at
    /// `base + 1000 * n` (an hour before now, inside retention), whatever the commit instant is.
    struct Udp {
        fixture: Listener,
        address: SocketAddr,
        sender: UdpSocket,
        base: u64,
        stop: watch::Sender<bool>,
        task: tokio::task::JoinHandle<io::Result<()>>,
    }

    async fn udp(hold_bytes: Option<usize>) -> Udp {
        udp_with(hold_bytes, false, u64::MAX).await
    }

    /// A UDP listener whose store may archive, and whose clock fails at read number `clock_fails_at`.
    async fn udp_with(hold_bytes: Option<usize>, archiving: bool, clock_fails_at: u64) -> Udp {
        let fixture = listener_with(SyslogTransport::Udp, 1, 1000, archiving);
        fixture
            .metrics
            .update(|status| {
                status.definition.hold_bytes = hold_bytes;
                Ok(())
            })
            .unwrap();
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let base = crate::now().unwrap() - 3_600_000;
        let reads = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let clock: Clock = Arc::new(move || {
            let read = reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if read >= clock_fails_at {
                return Err(io::Error::other("the clock failed"));
            }
            Ok(base + 1000 * read)
        });
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(listen_udp(
            socket,
            fixture.metrics.clone(),
            fixture.store.clone(),
            fixture.operations.clone(),
            stopped,
            fixture.wake.clone(),
            clock,
        ));
        Udp {
            fixture,
            address,
            sender: UdpSocket::bind("127.0.0.1:0").await.unwrap(),
            base,
            stop,
            task,
        }
    }

    impl Udp {
        async fn send(&self, len: usize) {
            self.sender
                .send_to(&vec![b'x'; len], self.address)
                .await
                .unwrap();
        }
        fn status(&self) -> Status {
            self.fixture.metrics.snapshot().unwrap()
        }
        async fn until_queued(&self, queued: usize) {
            yield_until("the reader has queued the datagrams", || {
                self.status().queued_datagrams == queued
            })
            .await;
        }
        async fn until_stored(&self, accepted: u64) {
            yield_until("the datagrams are stored", || {
                self.status().counts.accepted == accepted
            })
            .await;
        }
        async fn pressure(&self) {
            self.fixture
                .store
                .space_floor()
                .refuse_writes_for_tests(true);
        }
        /// How the listener ended, once it has.
        async fn joined(&mut self) -> io::Result<()> {
            (&mut self.task).await.unwrap()
        }
        /// Stop the listener and return how it ended.
        async fn finish(self) -> io::Result<()> {
            self.stop.send_replace(true);
            self.task.await.unwrap()
        }
    }

    /// Datagrams of 59 900 bytes and up, 17 of which fit the smallest queue.
    const FULL_ENTRIES: usize = 17;
    fn big(n: usize) -> usize {
        59_900 + n
    }

    /// Under pressure, queue 17 datagrams, let the 18th be read and wait for room, then send
    /// `in_kernel` more that stay in the kernel buffer. Returns every length sent, in order.
    async fn fill_queue(udp: &Udp, in_kernel: usize) -> Vec<u64> {
        let waits = udp.status().counts.reader_full_waits + 1;
        let mut lengths = Vec::new();
        for n in 0..=FULL_ENTRIES {
            udp.send(big(n)).await;
            lengths.push(big(n) as u64);
            if n < FULL_ENTRIES {
                udp.until_queued(n + 1).await;
            }
        }
        yield_until("the reader waits for room", || {
            udp.status().counts.reader_full_waits == waits
        })
        .await;
        for n in 1..=in_kernel {
            udp.send(big(FULL_ENTRIES + n)).await;
            lengths.push(big(FULL_ENTRIES + n) as u64);
        }
        lengths
    }

    /// Value (#255): a datagram the store refuses under pressure is a log in hand. It was
    /// refused and counted before; now it is queued behind the reader and stored in send order
    /// when pressure clears, with nothing counted refused. Fails on the old loop, which counted
    /// every datagram refused while the head was held.
    #[tokio::test(start_paused = true)]
    async fn datagrams_under_pressure_are_queued_and_stored_in_send_order_when_it_clears() {
        let udp = udp(None).await;
        udp.pressure().await;
        let lengths: [u64; 5] = [101, 102, 103, 104, 105];
        for len in lengths {
            udp.send(len as usize).await;
        }
        udp.until_queued(lengths.len()).await;
        relieve(&udp.fixture);
        udp.until_stored(5).await;
        assert_eq!(stored_lengths(&udp.fixture.store), lengths);
        assert_eq!(udp.status().counts.refused, 0);
        udp.finish().await.unwrap();
    }

    /// Value (#255): a record's `received_at` is the instant the reader took the datagram off
    /// the socket, not the commit instant, so a datagram that waited in the queue still reads
    /// as received when it arrived. The injected clock is an hour behind the real one. Fails on
    /// the old code, which stamped at commit.
    #[tokio::test(start_paused = true)]
    async fn a_stored_datagram_carries_the_instant_it_was_read() {
        let udp = udp(None).await;
        for len in [101, 102, 103] {
            udp.send(len).await;
        }
        udp.until_stored(3).await;
        let mut records = udp.fixture.store.records(crate::now().unwrap()).unwrap();
        records.sort_by_key(|record| record.receiver.as_ref().map(|position| position.sequence));
        let stamps: Vec<u64> = records.iter().map(|record| record.received_at).collect();
        assert_eq!(
            stamps,
            [udp.base, udp.base + 1000, udp.base + 2000],
            "stamped at commit, not at read"
        );
        udp.finish().await.unwrap();
    }

    /// Value (#255): the reader never waits for the store. While the head datagram is held the
    /// reader goes on taking datagrams off the socket, those sent before the hold was seen and
    /// those sent long after, and it is never stopped for lack of room. Fails on the old loop,
    /// which read nothing while a datagram was in the store's hands.
    #[tokio::test(start_paused = true)]
    async fn the_reader_keeps_reading_while_the_head_datagram_is_held() {
        let udp = udp(None).await;
        udp.pressure().await;
        for len in [101, 102, 103, 104, 105] {
            udp.send(len).await;
        }
        udp.until_queued(5).await;
        yield_until("the head is held", || frame_taken(&udp.status())).await;
        tokio::time::advance(Duration::from_secs(600)).await;
        for len in [106, 107, 108] {
            udp.send(len).await;
        }
        udp.until_queued(8).await;
        let status = udp.status();
        assert_eq!(
            (status.counts.accepted, status.counts.refused),
            (0, 0),
            "nothing was stored or discarded"
        );
        assert_eq!(status.counts.reader_full_waits, 0);
        relieve(&udp.fixture);
        udp.until_stored(8).await;
        udp.finish().await.unwrap();
    }

    /// Value (#255): a full queue stops the reader and hands arrivals to the kernel buffer;
    /// nothing is dropped in userspace. With the smallest queue, 17 datagrams fit and the 18th
    /// waits in hand; two more stay in the kernel. A paused hour changes nothing (the wait has
    /// no timer). When pressure clears all 20 are stored, in send order, none refused, and the
    /// status carries the gauges and the longest wait. Fails on the old loop, which discarded
    /// the datagrams read while the first was refused.
    #[tokio::test(start_paused = true)]
    async fn a_full_queue_stops_the_reader_and_the_kernel_buffer_takes_over() {
        let udp = udp(Some(MIN_HOLD_BYTES)).await;
        udp.pressure().await;
        let lengths = fill_queue(&udp, 2).await;
        tokio::time::advance(Duration::from_secs(3600)).await;
        settle(&udp.fixture).await;
        let waiting = udp.status();
        assert_eq!(waiting.queued_datagrams, FULL_ENTRIES);
        assert_eq!(
            waiting.counts.reader_full_waits, 1,
            "the kernel's two were not read"
        );
        assert!(waiting.queued_bytes <= MIN_HOLD_BYTES as u64);
        let json = status_json(&waiting);
        assert_eq!(json["queued_datagrams"].as_u64(), Some(FULL_ENTRIES as u64));
        assert_eq!(json["hold_bytes"].as_str(), Some("1048576"));
        assert!(
            json["reader_full_longest_ms"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
                >= 3_600_000
        );
        relieve(&udp.fixture);
        udp.until_stored(lengths.len() as u64).await;
        assert_eq!(stored_lengths(&udp.fixture.store), lengths);
        let done = udp.status();
        assert_eq!(done.counts.refused, 0);
        assert!(done.counts.reader_full_longest_ms >= 3_600_000);
        udp.finish().await.unwrap();
    }

    /// Value (#255): a datagram that arrives while the queue drains joins its tail, so arrival
    /// order into the store is arrival order at the socket. Fails if a newer datagram is stored
    /// before an older queued one.
    #[tokio::test(start_paused = true)]
    async fn datagrams_arriving_during_the_drain_are_stored_after_the_older_queue() {
        let udp = udp(None).await;
        udp.pressure().await;
        for len in [101, 102, 103] {
            udp.send(len).await;
        }
        udp.until_queued(3).await;
        relieve(&udp.fixture);
        udp.until_stored(1).await;
        for len in [104, 105] {
            udp.send(len).await;
        }
        udp.until_stored(5).await;
        assert_eq!(
            stored_lengths(&udp.fixture.store),
            [101, 102, 103, 104, 105]
        );
        udp.finish().await.unwrap();
    }

    /// Value (#255, the PR 244 lesson): a full-queue episode is always announced. A second
    /// episode that starts inside the report interval prints nothing; with no timer, only the
    /// observer tick after the interval announces it; its end line carries the waits and the
    /// kernel drops during the episode. The kernel table is a file the test rewrites.
    #[tokio::test(start_paused = true)]
    async fn a_suppressed_full_queue_episode_is_announced_by_the_observer_tick_and_its_end_counts_drops()
     {
        let udp = udp(Some(MIN_HOLD_BYTES)).await;
        let dir = tempfile::tempdir().unwrap();
        let table = dir.path().join("udp");
        let write = |drops: u64| {
            std::fs::write(
                &table,
                format!(
                    "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops\n  1: 0100007F:0202 00000000:0000 07 00000000:00000000 00:00000000 00000000  1000        0 777 2 0000000000000000 {drops}\n"
                ),
            )
            .unwrap();
        };
        write(0);
        let buffer = crate::udp_kernel::Buffer {
            requested: 8 << 20,
            granted: 16 << 20,
            enlarge_error: None,
        };
        udp.fixture
            .metrics
            .update(|status| {
                status.kernel = Some(crate::udp_kernel::Kernel::at(table.clone(), 777, buffer));
                Ok(())
            })
            .unwrap();
        // The first episode is announced and, when the queue drains, its end is journaled.
        udp.pressure().await;
        fill_queue(&udp, 0).await;
        let full_lines = || {
            printed::lines()
                .into_iter()
                .filter(|line| line.contains("receive queue full"))
                .collect::<Vec<_>>()
        };
        assert_eq!(full_lines().len(), 1);
        relieve(&udp.fixture);
        udp.until_stored(18).await;
        yield_until("the first episode ends", || udp.status().full.is_none()).await;
        let drained = |lines: Vec<String>| {
            lines
                .into_iter()
                .filter(|line| line.contains("receive queue drained"))
                .collect::<Vec<_>>()
        };
        assert!(drained(printed::lines())[0].contains("the reader waited 1 time(s)"));
        // The second episode starts inside the interval: no line, and no timer announces it.
        udp.pressure().await;
        fill_queue(&udp, 0).await;
        tokio::time::advance(HOLD_REPORT_INTERVAL + Duration::from_secs(1)).await;
        settle(&udp.fixture).await;
        assert_eq!(full_lines().len(), 1, "no timer announces it");
        write(7);
        udp.fixture.wake.observe_tick();
        yield_until("the tick announces the episode", || full_lines().len() == 2).await;
        let line = full_lines()[1].clone();
        assert!(
            line.contains("receive queue full (17 datagram(s)")
                && line.contains("kernel drops so far 7"),
            "{line}"
        );
        write(9);
        relieve(&udp.fixture);
        udp.until_stored(36).await;
        yield_until("the second episode ends", || udp.status().full.is_none()).await;
        let line = drained(printed::lines())[1].clone();
        assert!(
            line.contains("waited 1 time(s)") && line.contains("dropped 9 datagram(s)"),
            "{line}"
        );
        udp.finish().await.unwrap();
    }

    /// Value (#255): a store error that is not pressure ends `listen` at once, with the queued
    /// datagrams counted and named, so the reader stops filling a queue nobody drains and the
    /// daemon's worker health gate stops loudly. No stop is sent: a listen that waited for its
    /// reader would never return.
    #[tokio::test(start_paused = true)]
    async fn a_publisher_error_ends_the_listener_and_names_the_queued_datagrams() {
        let mut udp = udp(None).await;
        udp.pressure().await;
        for len in [101, 102, 103] {
            udp.send(len).await;
        }
        udp.until_queued(3).await;
        let floor = udp.fixture.store.space_floor();
        floor.refuse_writes_for_tests(false);
        floor.refuse_next_write_for_tests("STORAGE_FAILURE");
        udp.fixture.wake.wake();
        yield_until("listen returns without a stop", || udp.task.is_finished()).await;
        let error = udp.joined().await.unwrap_err();
        assert!(
            error.to_string().contains("3 queued datagram(s)"),
            "{error}"
        );
        udp.send(104).await;
        settle(&udp.fixture).await;
        let status = udp.status();
        assert_eq!(
            status.queued_datagrams, 0,
            "the failed queue is counted lost"
        );
        assert_eq!(
            (status.counts.lost_at_shutdown, status.counts.accepted),
            (3, 0)
        );
        udp.send(105).await;
        settle(&udp.fixture).await;
        assert_eq!(
            udp.status().counts.lost_at_shutdown,
            3,
            "nothing reads after the failure"
        );
        assert!(
            status
                .failure
                .is_some_and(|failure| failure.contains("UDP publisher failed"))
        );
    }

    /// Fence the store's archive at `instant`: one record stamped then is archived, so a
    /// publication stamped at or before that instant is refused as behind the watermark.
    fn fence_at(store: &sparrow_core::Store, instant: u64) {
        let seed = store
            .ingest("fence-seed", "source", "log", b"seed\n", instant)
            .unwrap();
        let position = format!("{instant:020}:{}", seed.record_ids[0]);
        let intent = sparrow_core::ArchiveIntent {
            segment_id: [1; 32],
            first: position.clone(),
            last: position,
            records: 1,
            artifacts: 0,
        };
        store.archive_intend(&intent).unwrap();
        store.archive_commit(&intent.segment_id).unwrap();
    }

    /// The stored UDP records in store order (the fence seed has no receiver position).
    fn udp_records(store: &sparrow_core::Store) -> Vec<sparrow_core::Record> {
        let mut records = store.records(crate::now().unwrap()).unwrap();
        records.retain(|record| record.receiver.is_some());
        records.sort_by_key(|record| record.receiver.as_ref().map(|position| position.sequence));
        records
    }

    /// Value (#255): the archive fence refuses a publication behind the watermark with its own
    /// code. A datagram whose read instant is behind a real fence is offered once more with the
    /// commit instant and counted restamped, never refused for its stamp. Fails on the old
    /// code, which answered the fence with `STORAGE_FAILURE` and stopped the listener.
    #[tokio::test(start_paused = true)]
    async fn a_datagram_behind_the_archive_watermark_is_restamped_with_the_commit_instant() {
        let udp = udp_with(None, true, u64::MAX).await;
        fence_at(&udp.fixture.store, udp.base + 1_800_000);
        let before = crate::now().unwrap();
        udp.send(101).await;
        udp.send(102).await;
        udp.until_stored(2).await;
        let status = udp.status();
        assert_eq!(
            (
                status.counts.restamped,
                status.counts.restamped_past_watermark,
                status.counts.refused
            ),
            (2, 0, 0)
        );
        for record in udp_records(&udp.fixture.store) {
            assert!(
                record.received_at >= before,
                "stamped with the commit instant"
            );
        }
        udp.finish().await.unwrap();
    }

    /// Value (#255, mandate: a log is never refused for stamping): when the clock has stepped
    /// back behind the archive watermark the commit instant is refused too. The datagram is
    /// then stored just past the fence, counted `restamped_past_watermark` and journaled once
    /// (the second datagram is inside the report interval), and the listener goes on. Fails on
    /// the code that gave up after the second refusal and stopped the daemon.
    #[tokio::test(start_paused = true)]
    async fn a_datagram_refused_at_both_instants_is_stored_just_past_the_archive_watermark() {
        let udp = udp_with(None, true, u64::MAX).await;
        let fence = crate::now().unwrap() + 600_000;
        fence_at(&udp.fixture.store, fence);
        udp.send(101).await;
        udp.send(102).await;
        udp.until_stored(2).await;
        let status = udp.status();
        assert_eq!(
            (
                status.counts.restamped,
                status.counts.restamped_past_watermark,
                status.counts.refused
            ),
            (0, 2, 0)
        );
        assert!(!udp.task.is_finished(), "the listener goes on");
        for record in udp_records(&udp.fixture.store) {
            assert_eq!(record.received_at, fence + 1);
        }
        let lines: Vec<String> = printed::lines()
            .into_iter()
            .filter(|line| line.contains("just past the archive watermark"))
            .collect();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains("not the clock"), "{lines:?}");
        udp.finish().await.unwrap();
    }

    /// Value (#255): on a stop the reader drains the socket and the publisher publishes the
    /// queue. Seventeen datagrams are queued, one waits in hand and two sit in the kernel when
    /// the stop comes with the store healthy: all 20 are stored. Fails without the drain, which
    /// lost the two in the kernel.
    #[tokio::test(start_paused = true)]
    async fn a_stop_drains_the_kernel_buffer_and_publishes_the_queue() {
        let mut udp = udp(Some(MIN_HOLD_BYTES)).await;
        udp.pressure().await;
        let lengths = fill_queue(&udp, 2).await;
        udp.fixture
            .store
            .space_floor()
            .refuse_writes_for_tests(false);
        udp.stop.send_replace(true);
        udp.until_stored(lengths.len() as u64).await;
        udp.joined().await.unwrap();
        let status = udp.status();
        assert_eq!(stored_lengths(&udp.fixture.store), lengths);
        assert_eq!(
            (status.counts.lost_at_shutdown, status.counts.refused),
            (0, 0)
        );
    }

    /// Value (#255): when a stop finds the store still under pressure, what was read and not
    /// stored is counted once, as `lost_at_shutdown` datagrams and bytes with one journal line,
    /// not as one discard each. Fails on the old loop, which counted every datagram refused.
    #[tokio::test(start_paused = true)]
    async fn a_stop_under_pressure_counts_the_remainder_once() {
        let mut udp = udp(None).await;
        udp.pressure().await;
        for len in [101, 102, 103] {
            udp.send(len).await;
        }
        udp.until_queued(3).await;
        let before = printed::lines().len();
        udp.stop.send_replace(true);
        yield_until("the queue is counted", || {
            udp.status().counts.lost_at_shutdown == 3
        })
        .await;
        udp.joined().await.unwrap();
        let status = udp.status();
        assert_eq!(status.counts.lost_at_shutdown_bytes, 306);
        assert_eq!(
            (
                status.counts.refused,
                status.counts.accepted,
                status.queued_datagrams
            ),
            (0, 0, 0)
        );
        let lines: Vec<String> = printed::lines()[before..]
            .iter()
            .filter(|line| line.contains("lost at shutdown"))
            .cloned()
            .collect();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains("3 datagram(s), 306 byte(s)"), "{lines:?}");
        assert!(stored_lengths(&udp.fixture.store).is_empty());
    }

    /// Value (#255): a queued datagram costs its length plus `ENTRY_OVERHEAD`, so a flood of
    /// one-byte datagrams is bounded in count as well as bytes: the smallest queue holds
    /// exactly 1 MiB / 129 of them, and the next waits in hand. Coverage of the budget rule;
    /// the old code had no queue.
    #[tokio::test(start_paused = true)]
    async fn one_byte_datagrams_are_bounded_by_the_entry_overhead() {
        let udp = udp(Some(MIN_HOLD_BYTES)).await;
        udp.pressure().await;
        let fits = MIN_HOLD_BYTES / (1 + ENTRY_OVERHEAD);
        let mut sent = 0;
        while sent <= fits {
            let batch = 100.min(fits + 1 - sent);
            for _ in 0..batch {
                udp.send(1).await;
            }
            sent += batch;
            if sent <= fits {
                udp.until_queued(sent).await;
            }
        }
        yield_until("the reader waits for room", || {
            udp.status().counts.reader_full_waits == 1
        })
        .await;
        let status = udp.status();
        assert_eq!(status.queued_datagrams, fits);
        assert_eq!(status.queued_bytes, (fits * (1 + ENTRY_OVERHEAD)) as u64);
        udp.finish().await.unwrap();
    }

    /// Value (#255): a publisher error with a full queue loses the queue and also the datagram
    /// the reader holds while it waits for room. Both are counted and named, and the reader's
    /// wait ends with the listener, so the longest-wait gauge stops growing. Fails on the code
    /// that counted only the queue and left the wait running.
    #[tokio::test(start_paused = true)]
    async fn a_publisher_error_with_a_full_queue_counts_the_datagram_the_reader_holds() {
        let mut udp = udp(Some(MIN_HOLD_BYTES)).await;
        udp.pressure().await;
        let lengths = fill_queue(&udp, 0).await;
        let floor = udp.fixture.store.space_floor();
        floor.refuse_writes_for_tests(false);
        floor.refuse_next_write_for_tests("STORAGE_FAILURE");
        udp.fixture.wake.wake();
        yield_until("listen returns without a stop", || udp.task.is_finished()).await;
        let error = udp.joined().await.unwrap_err();
        assert!(
            error.to_string().contains("18 queued datagram(s)"),
            "{error}"
        );
        let status = udp.status();
        assert_eq!(status.counts.lost_at_shutdown, 18);
        assert_eq!(
            status.counts.lost_at_shutdown_bytes,
            lengths.iter().sum::<u64>()
        );
        assert_eq!((status.queued_datagrams, status.in_hand), (0, None));
        let longest = |status: &Status| status_json(status)["reader_full_longest_ms"].clone();
        tokio::time::advance(Duration::from_secs(3600)).await;
        assert_eq!(
            longest(&udp.status()),
            longest(&status),
            "the wait gauge stopped"
        );
    }

    /// Value (#255, the design's reader-error path): a reader error closes the queue; the
    /// publisher still stores what is queued, and only then does `listen` return the reader's
    /// error. The clock failing at the third read is the reader's error.
    #[tokio::test(start_paused = true)]
    async fn a_reader_error_lets_the_publisher_finish_the_queue_then_is_returned() {
        let mut udp = udp_with(None, false, 2).await;
        udp.pressure().await;
        udp.send(101).await;
        udp.send(102).await;
        udp.until_queued(2).await;
        udp.send(103).await;
        settle(&udp.fixture).await;
        assert!(
            !udp.task.is_finished(),
            "the queue is still to be published"
        );
        relieve(&udp.fixture);
        yield_until("listen returns", || udp.task.is_finished()).await;
        let error = udp.joined().await.unwrap_err();
        assert!(error.to_string().contains("the clock failed"), "{error}");
        let status = udp.status();
        assert_eq!(status.counts.accepted, 2);
        // The datagram whose stamp failed was taken off the socket: it is counted lost, named.
        assert_eq!(
            (
                status.counts.lost_at_shutdown,
                status.counts.lost_at_shutdown_bytes
            ),
            (1, 103)
        );
        assert!(error.to_string().contains("counted lost"), "{error}");
        assert_eq!(stored_lengths(&udp.fixture.store), [101, 102]);
    }

    /// Value (#255): a reader that fails while it waits for room with a datagram in hand counts
    /// that datagram lost and ends the wait, so the longest-wait gauge stops growing. The wait
    /// is made to fail by closing its budget, the one failure a test can reach there. Fails on
    /// the reader that returned the error and left the wait running.
    #[tokio::test(start_paused = true)]
    async fn a_reader_error_inside_the_full_wait_counts_the_datagram_and_stops_the_gauge() {
        let fixture = listener_with(SyslogTransport::Udp, 1, 1000, false);
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let (queue, _entries) = mpsc::unbounded_channel();
        let budget = Arc::new(Semaphore::new(1000));
        let mut reader = Reader {
            socket: &socket,
            queue,
            budget: budget.clone(),
            metrics: &fixture.metrics,
            wake: fixture.wake.subscribe(),
            clock: Arc::new(|| Ok(1)),
            last: 0,
            buffer: vec![b'x'; 600],
        };
        // The budget has 1000 bytes; the datagram costs 600 + 128: it fits once, not twice.
        reader.enqueue(600).await.unwrap();
        let metrics = fixture.metrics.clone();
        let (outcome, ()) = tokio::join!(reader.enqueue(600), async {
            yield_until("the reader waits", || {
                metrics.snapshot().unwrap().waiting_since.is_some()
            })
            .await;
            budget.close();
        });
        assert!(outcome.is_err());
        let status = fixture.metrics.snapshot().unwrap();
        assert_eq!(
            (
                status.counts.lost_at_shutdown,
                status.counts.lost_at_shutdown_bytes
            ),
            (1, 600)
        );
        assert!(status.waiting_since.is_none() && status.in_hand.is_none());
    }
}
