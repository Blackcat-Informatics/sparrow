// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Complete owned teardown, preserving the primary failure and every cleanup failure.

use crate::local_endpoint::LocalEndpoint;

pub(super) async fn finish(
    initial: Result<(), Box<dyn std::error::Error>>,
    consumer: Option<sparrow_server::watches::ConsumeWorker>,
    syslog: Option<sparrow_server::syslog_listener::SyslogWorker>,
    store: &sparrow_core::Store,
    endpoint: Option<&LocalEndpoint>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut errors = Vec::with_capacity(5);
    record_failure(&mut errors, "daemon", initial);
    if let Some(worker) = consumer {
        record_failure(&mut errors, "consume shutdown", worker.shutdown().await);
    }
    if let Some(worker) = syslog {
        record_failure(&mut errors, "syslog shutdown", worker.shutdown().await);
    }
    record_failure(&mut errors, "store flush", store.flush());
    if let Some(endpoint) = endpoint {
        record_failure(
            &mut errors,
            "local endpoint cleanup",
            endpoint.remove_name(),
        );
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; ").into())
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
