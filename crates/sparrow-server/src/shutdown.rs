// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Complete owned teardown, preserving the primary failure and every cleanup failure.

use sparrow_server::admission::Admission;
use sparrow_server::local_endpoint::LocalEndpoint;

#[derive(Default)]
pub(super) struct Workers {
    pub consumer: Option<sparrow_server::watches::ConsumeWorker>,
    pub syslog: Option<sparrow_server::syslog_listener::SyslogWorker>,
    pub maintenance: Option<super::maintenance::Worker>,
    pub archive: Option<sparrow_server::archive::Worker>,
}

pub(super) async fn finish(
    initial: Result<(), Box<dyn std::error::Error>>,
    workers: Workers,
    store: &sparrow_core::Store,
    endpoints: &[&LocalEndpoint],
    admission: &Admission,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut errors = Vec::with_capacity(6);
    // The teardown reports its errors as one text; the class of the start refusal survives it.
    let refused = initial
        .as_ref()
        .err()
        .is_some_and(|error| sparrow_server::start_wait::is_permanent(error.as_ref()));
    record_failure(&mut errors, "daemon", initial);
    if let Some(worker) = workers.archive {
        record_failure(&mut errors, "archive shutdown", worker.shutdown().await);
    }
    if let Some(worker) = workers.consumer {
        record_failure(&mut errors, "consume shutdown", worker.shutdown().await);
    }
    if let Some(worker) = workers.syslog {
        record_failure(&mut errors, "syslog shutdown", worker.shutdown().await);
    }
    if let Some(worker) = workers.maintenance {
        record_failure(&mut errors, "maintenance shutdown", worker.shutdown().await);
    }
    // Intake workers may need their owned RPC transport to finish an admitted import.
    // Once joined, drain and close both native budgets before the final WAL fence.
    let drained = admission.drain().await;
    if drained.is_ok() {
        let flushed = store.flush();
        let closed = flushed.is_ok();
        record_failure(&mut errors, "store flush", flushed);
        if closed {
            // The flush made every acknowledged write durable. Abort background compaction
            // now so the store's drop does not wait for a running job to finish its output.
            store.cancel_background_work();
            // Every worker has stopped and admission is closed, so nothing can change the count
            // again; the journal line records the count at the stop.
            // While the store counters seed the count is not a total, so the line says pending.
            match (store.store_id(), store.unique_payload_chunks()) {
                (Ok(id), Ok(Some(chunks))) => {
                    eprintln!("sparrowd: closed store={id} unique_payload_chunks={chunks}");
                }
                (Ok(id), Ok(None)) => {
                    eprintln!("sparrowd: closed store={id} unique_payload_chunks=pending");
                }
                (Err(error), _) | (_, Err(error)) => {
                    errors.push(format!("closing count: {error}"));
                }
            }
        }
    } else if let Err(error) = drained {
        errors.push(error.to_string());
        errors.push("store flush withheld because RPC admission could not be drained".into());
    }
    for endpoint in endpoints {
        record_failure(
            &mut errors,
            "local endpoint cleanup",
            endpoint.remove_name(),
        );
    }
    if errors.is_empty() {
        Ok(())
    } else {
        let error: Box<dyn std::error::Error> = errors.join("; ").into();
        Err(if refused {
            sparrow_server::start_wait::permanent(error)
        } else {
            error
        })
    }
}

fn record_failure<T, E: std::fmt::Display>(
    errors: &mut Vec<String>,
    phase: &str,
    result: Result<T, E>,
) {
    if let Err(error) = result {
        errors.push(format!("{phase}: {error}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Ordinary work remains owned until its permit drops, and a completed drain
    /// permanently closes the shared operation budget.
    #[tokio::test]
    async fn final_drain_waits_for_all_workers_and_closes_fresh_admission() {
        let operations = Arc::new(tokio::sync::Semaphore::new(2));
        let admission = Admission::new(operations.clone());
        let worker = operations.clone().acquire_owned().await.unwrap();
        let barrier = admission.drain();
        tokio::pin!(barrier);
        tokio::select! {
            biased;
            _ = &mut barrier => panic!("drain completed with an admitted worker"),
            _ = tokio::task::yield_now() => {}
        }
        assert!(operations.clone().try_acquire_owned().is_err());
        drop(worker);
        barrier.await.unwrap();
        assert!(operations.is_closed());
        assert!(matches!(
            operations.try_acquire_owned(),
            Err(tokio::sync::TryAcquireError::Closed)
        ));
    }

    /// The final fence covers a buffered writer whose caller disappeared and still waits
    /// for an admitted diagnostic after that writer releases admission.
    #[tokio::test]
    async fn final_flush_covers_buffered_worker_after_its_caller_disappears() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let config =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let config = sparrow_server::ServerConfig::read(&config).unwrap();
        let store = config.open_store().unwrap();
        let identity = store.store_id().unwrap();
        let operations = Arc::new(tokio::sync::Semaphore::new(2));
        let admission = Admission::new(operations.clone());
        let permit = admission.writer().await.unwrap();
        let diagnostic = admission.diagnostic().unwrap();
        let owner = store.clone();
        let (entered, running) = tokio::sync::oneshot::channel();
        let (release, paused) = std::sync::mpsc::channel();
        let (published, receipt) = tokio::sync::oneshot::channel();
        let (completed, abandoned) = tokio::sync::oneshot::channel();
        let caller = tokio::spawn(abandoned);
        let worker = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            entered.send(()).unwrap();
            paused.recv().unwrap();
            let result = owner.ingest_with_durability(
                "drain-buffered",
                "drain",
                "opaque",
                b"buffered evidence",
                1000,
                sparrow_core::Durability::Buffered,
            );
            published.send(result).unwrap();
            assert!(
                completed.send(()).is_err(),
                "abandoned caller retained its response"
            );
        });
        running.await.unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        {
            let teardown = finish(Ok(()), Workers::default(), &store, &[], &admission);
            tokio::pin!(teardown);
            tokio::select! {
                biased;
                _ = &mut teardown => panic!("final flush overtook an admitted worker"),
                _ = tokio::task::yield_now() => {}
            }
            assert_eq!(
                admission.writer().await.err().unwrap().code,
                "SHUTTING_DOWN"
            );
            assert_eq!(admission.reader().err().unwrap().code, "SHUTTING_DOWN");
            assert_eq!(admission.diagnostic().err().unwrap().code, "SHUTTING_DOWN");
            release.send(()).unwrap();
            worker.await.unwrap();
            let buffered = store.durability_status().unwrap();
            assert_eq!(buffered.pending_writes, 1);
            assert!(buffered.accepted_through > buffered.durable_through);
            tokio::select! {
                biased;
                _ = &mut teardown => panic!("final flush overtook an admitted diagnostic"),
                () = std::future::ready(()) => {}
            }
            assert_eq!(store.durability_status().unwrap().pending_writes, 1);
            drop(diagnostic);
            teardown.await.unwrap();
        }
        let artifact = receipt.await.unwrap().unwrap();
        let status = store.durability_status().unwrap();
        assert_eq!(status.pending_writes, 0);
        assert_eq!(status.pending_bytes, 0);
        assert_eq!(status.accepted_through, status.durable_through);
        assert!(operations.is_closed());
        drop(store);
        let reopened = config.open_store().unwrap();
        assert_eq!(reopened.store_id().unwrap(), identity);
        assert_eq!(
            reopened.bytes(&artifact.id, 0, 17, 1001).unwrap(),
            b"buffered evidence"
        );
    }
}
