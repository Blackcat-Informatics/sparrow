// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One joined, serial maintenance owner; RPC retains its own request budget.
//! The one-time table rewrite runs beside it in its own joined lane, because a
//! native slice takes seconds and must not delay WAL synchronization.

use sparrow_core::{Error, EvictionLimits, RECOMPRESSION_SLICE_BYTES, RecompressionPhase, Store};
use std::time::Duration;
use tokio::sync::watch;

/// Pause after each complete expiry traversal. Expiry affects reads immediately,
/// so this only delays physical retirement. One traversal parses the whole
/// catalog under the writer lock; restarting it every second starved ingest.
const EXPIRY_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// Pause between table rewrite steps, and before a yielded step looks again.
/// The gap also lets queued native compactions start between slices.
const RECOMPRESSION_INTERVAL: Duration = Duration::from_secs(5);

pub(super) struct Worker {
    stop: watch::Sender<bool>,
    tasks: Vec<tokio::task::JoinHandle<Result<(), String>>>,
}

impl Worker {
    pub(super) fn start(store: Store, flush: Duration) -> Self {
        let (stop, stopped) = watch::channel(false);
        Self {
            stop,
            tasks: vec![
                tokio::spawn(run(store.clone(), stopped.clone(), flush)),
                tokio::spawn(recompress(store, stopped)),
            ],
        }
    }

    pub(super) fn is_finished(&self) -> bool {
        self.tasks.iter().any(tokio::task::JoinHandle::is_finished)
    }

    /// Both lanes stop after their current native step; a running slice completes.
    pub(super) async fn shutdown(mut self) -> Result<(), String> {
        self.stop.send_replace(true);
        let mut failures = Vec::new();
        for task in self.tasks.drain(..) {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(error),
                Err(error) => failures.push(format!("maintenance join failed: {error}")),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        // Never abort a native write; the joined loop stops after its completion.
        self.stop.send_replace(true);
    }
}

#[derive(Clone, Copy)]
enum Job {
    Flush,
    Pressure,
    Reclaim,
    Expiry,
}

impl Job {
    fn name(self) -> &'static str {
        match self {
            Self::Flush => "buffered WAL synchronization",
            Self::Pressure => "pressure maintenance",
            Self::Reclaim => "import journal reclamation",
            Self::Expiry => "retention maintenance",
        }
    }

    /// Only expiry changes its continuation. Other duties cannot lose that work.
    fn execute(self, store: &Store) -> Result<Option<bool>, Error> {
        match self {
            Self::Flush => store.flush_buffered().map(|_| None),
            Self::Pressure => store
                .pressure_maintain(sparrow_server::now()?, EvictionLimits::default())
                .map(|_| None),
            // Published journals hold one of 16 import slots until reclaimed.
            Self::Reclaim => store.import_reclaim(sparrow_server::now()?).map(|_| None),
            Self::Expiry => store
                .expire_bounded(sparrow_server::now()?, EvictionLimits::default())
                .map(|report| Some(report.continuation)),
        }
    }
}

async fn run(
    store: Store,
    mut stopped: watch::Receiver<bool>,
    flush_interval: Duration,
) -> Result<(), String> {
    let mut flush = tokio::time::interval(flush_interval);
    let mut pressure = tokio::time::interval(Duration::from_secs(1));
    let mut reclaim = tokio::time::interval(Duration::from_secs(1));
    let mut expiry = tokio::time::interval(EXPIRY_SWEEP_INTERVAL);
    for interval in [&mut flush, &mut pressure, &mut reclaim, &mut expiry] {
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    }
    // Buffered synchronization retains its configured initial delay.
    flush.tick().await;
    let mut continuation = false;
    loop {
        let job = tokio::select! {
            biased;
            _ = stopped.changed() => return Ok(()),
            job = async {
                // Keep duties fair even when a flush interval is shorter than
                // native work. Only stop admission has strict priority.
                tokio::select! {
                    _ = flush.tick() => Job::Flush,
                    _ = pressure.tick() => Job::Pressure,
                    _ = reclaim.tick() => Job::Reclaim,
                    _ = expiry.tick(), if !continuation => Job::Expiry,
                    _ = tokio::task::yield_now(), if continuation => Job::Expiry,
                }
            } => job,
        };
        // Below the free-space floor every write is refused. Synchronizing
        // accepted writes is still allowed and still needed.
        if !matches!(job, Job::Flush)
            && store
                .space_floor()
                .shortfall()
                .map_err(|error| format!("free-space check failed: {error}"))?
                .is_some()
        {
            continuation = false;
            continue;
        }
        // Await this complete bounded native job before admitting another duty.
        let result = blocking(&store, job.name(), move |store| job.execute(store)).await?;
        match result {
            Ok(Some(more)) => {
                // The pause runs from the end of a traversal, however long it took.
                if !more {
                    expiry.reset();
                }
                continuation = more;
            }
            Ok(None) => {}
            Err(error) => {
                eprintln!("sparrowd: {} failed: {error}", job.name());
                if matches!(job, Job::Expiry) {
                    // Retry a refused pass on the ordinary tick, never a busy loop.
                    continuation = false;
                }
            }
        }
        tokio::task::yield_now().await;
    }
}

/// Run one bounded native duty off the async threads.
async fn blocking<T: Send + 'static>(
    store: &Store,
    name: &'static str,
    duty: impl FnOnce(&Store) -> Result<T, Error> + Send + 'static,
) -> Result<Result<T, Error>, String> {
    let owner = store.clone();
    let (result, latched) = tokio::task::spawn_blocking(move || {
        let result = duty(&owner);
        let latched = match &result {
            Ok(_) => Ok(None),
            Err(_) => owner.durability_status().map(|status| status.failure),
        };
        (result, latched)
    })
    .await
    .map_err(|error| format!("{name} join failed: {error}"))?;
    // A failed write leaves durability uncertain until the store is reopened.
    // Ending the daemon lets its supervisor reopen it instead of refusing every write.
    if let Some(failure) = latched.map_err(|error| format!("{name} status failed: {error}"))? {
        return Err(format!(
            "store durability uncertain; exiting so it is reopened: {failure}"
        ));
    }
    Ok(result)
}

/// The low-priority rewrite lane. Each step plans and records under the writer
/// lock and compacts without it; the store yields to pressure, native work and
/// space. A completed rewrite leaves the lane idle until shutdown.
async fn recompress(store: Store, mut stopped: watch::Receiver<bool>) -> Result<(), String> {
    const NAME: &str = "table rewrite";
    let mut tick = tokio::time::interval(RECOMPRESSION_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = stopped.changed() => return Ok(()),
            _ = tick.tick() => {}
        }
        match blocking(&store, NAME, |store| {
            store.recompress_step(RECOMPRESSION_SLICE_BYTES)
        })
        .await?
        {
            Ok(status) if status.phase == RecompressionPhase::Complete => {
                // Ignore a closed channel: the owner has gone, so stop as well.
                let _ = stopped.changed().await;
                return Ok(());
            }
            Ok(_) => {}
            Err(error) => eprintln!("sparrowd: {NAME} failed: {error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn backlog_drains_between_ticks_with_pins_and_buffered_flushes() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let path =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let config = sparrow_server::ServerConfig::read(&path).unwrap();
        let store = config.open_store().unwrap();
        let now = sparrow_server::now().unwrap();
        let old = now - 31 * 86_400_000;
        let input = (0..1024)
            .map(|i| format!("expired record {i}\n"))
            .collect::<String>();
        let artifact = store
            .ingest("backlog", "source", "log", input.as_bytes(), old)
            .unwrap();
        store
            .pin("backlog-pin", "case", &artifact.record_ids[..1], 40, old)
            .unwrap();
        let fresh = store
            .ingest_with_durability(
                "buffered",
                "source",
                "opaque",
                b"buffered",
                now,
                sparrow_core::Durability::Buffered,
            )
            .unwrap();
        assert_eq!(store.durability_status().unwrap().pending_writes, 1);
        assert_eq!(
            store.status(now).unwrap()["unique_payload_chunks"].as_str(),
            Some("1025")
        );
        let worker = Worker::start(store.clone(), Duration::from_millis(1));
        // Thirty-two default passes would take over thirty seconds under the
        // former once-per-second driver. This allows ample host scheduling slack.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if store.status(sparrow_server::now().unwrap()).unwrap()["unique_payload_chunks"]
                    .as_str()
                    == Some("2")
                    && store.durability_status().unwrap().pending_writes == 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        worker.shutdown().await.unwrap();
        assert_eq!(
            store.bytes(&artifact.record_ids[0], 0, 17, now).unwrap(),
            b"expired record 0\n"
        );
        assert_eq!(store.bytes(&fresh.id, 0, 8, now).unwrap(), b"buffered");
        assert_eq!(store.artifact(&artifact.id).unwrap_err().code, "NOT_FOUND");
    }

    #[tokio::test]
    async fn import_journals_are_reclaimed_between_expiry_sweeps() {
        // INVARIANT: a finished import frees its slot within seconds even though
        // complete expiry traversals pause for a minute.
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let path =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let store = sparrow_server::ServerConfig::read(&path)
            .unwrap()
            .open_store()
            .unwrap();
        let worker = Worker::start(store.clone(), Duration::from_millis(1));
        // Let the first traversal finish so the next one is a minute away.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let input = b"cancelled import";
        let payload = format!(
            "blake3:{}",
            purrdf_hash::hex::encode(purrdf_hash::blake3::hash(input).as_bytes())
        );
        let now = sparrow_server::now().unwrap();
        let manifest = store
            .import_begin(
                "cancelled",
                "source",
                "opaque",
                input.len() as u64,
                &payload,
                now,
            )
            .unwrap();
        store.import_cancel(&manifest.id, now).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            // A canceled journal reads as IMPORT_EXPIRED until it is reclaimed.
            while store
                .import_get(&manifest.id, sparrow_server::now().unwrap())
                .err()
                .is_none_or(|error| error.code != "NOT_FOUND")
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            store
                .import_get(&manifest.id, sparrow_server::now().unwrap())
                .unwrap_err()
                .code,
            "NOT_FOUND"
        );
        worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_before_start_preserves_unflushed_work_for_final_fence() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let path =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let config = sparrow_server::ServerConfig::read(&path).unwrap();
        let store = config.open_store().unwrap();
        store
            .ingest_with_durability(
                "stopped-buffer",
                "source",
                "opaque",
                b"buffered",
                sparrow_server::now().unwrap(),
                sparrow_core::Durability::Buffered,
            )
            .unwrap();
        let worker = Worker::start(store.clone(), Duration::from_millis(1));
        tokio::time::timeout(Duration::from_secs(2), worker.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(store.durability_status().unwrap().pending_writes, 1);
        store.flush().unwrap();
        assert_eq!(store.durability_status().unwrap().pending_writes, 0);
    }
}
