// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One joined, serial maintenance owner; RPC retains its own request budget.

use sparrow_core::{Error, EvictionLimits, Store};
use std::time::Duration;
use tokio::sync::watch;

pub(super) struct Worker {
    stop: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<Result<(), String>>>,
}

impl Worker {
    pub(super) fn start(store: Store, flush: Duration) -> Self {
        let (stop, stopped) = watch::channel(false);
        Self {
            stop,
            task: Some(tokio::spawn(run(store, stopped, flush))),
        }
    }

    pub(super) fn is_finished(&self) -> bool {
        self.task
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
    }

    pub(super) async fn shutdown(mut self) -> Result<(), String> {
        self.stop.send_replace(true);
        self.task
            .take()
            .ok_or("maintenance join owner missing")?
            .await
            .map_err(|error| format!("maintenance join failed: {error}"))?
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
    Expiry,
}

impl Job {
    fn name(self) -> &'static str {
        match self {
            Self::Flush => "buffered WAL synchronization",
            Self::Pressure => "pressure maintenance",
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
    let mut expiry = tokio::time::interval(Duration::from_secs(1));
    for interval in [&mut flush, &mut pressure, &mut expiry] {
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
                    _ = expiry.tick(), if !continuation => Job::Expiry,
                    _ = tokio::task::yield_now(), if continuation => Job::Expiry,
                }
            } => job,
        };
        let owner = store.clone();
        // Await this complete bounded native job before admitting another duty.
        let result = tokio::task::spawn_blocking(move || job.execute(&owner))
            .await
            .map_err(|error| format!("{} join failed: {error}", job.name()))?;
        match result {
            Ok(Some(more)) => continuation = more,
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
