// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Replace a drained storage session while the process keeps its listeners bound.

use sparrow_core::Store;
use sparrow_server::{ServerConfig, serving::Sockets, start_wait};
use std::{error::Error, fmt, future::Future, io, sync::Arc, time::Duration};

pub(super) fn status(phase: &str, detail: &str) {
    let line = format!("recovering store: {phase}: {detail}");
    eprintln!("sparrowd: {line}");
    start_wait::notify_status(&line);
}

enum Refusal {
    Inspect(io::Error),
    Space(String),
    Open(Box<dyn Error + Send + Sync>),
}

impl Refusal {
    fn retry(&self) -> Option<(&'static str, Duration)> {
        match self {
            Self::Space(_) => Some(("waiting for space for WAL replay", start_wait::OPEN_RETRY)),
            Self::Inspect(_) => Some(("waiting for the store filesystem", start_wait::BIND_RETRY)),
            Self::Open(error) => {
                if let Some(hold) = start_wait::open_hold(error.as_ref()) {
                    return Some((hold.key, hold.interval));
                }
                if error.is::<io::Error>() {
                    return Some(("waiting for the store filesystem", start_wait::BIND_RETRY));
                }
                let error = error.downcast_ref::<sparrow_core::Error>()?;
                let phase = match error.code {
                    "NOT_FOUND" => "waiting for the store path",
                    "STORE_MISMATCH" => "waiting for the original store",
                    "STORAGE_FAILURE" => "waiting for readable storage",
                    _ => return None,
                };
                Some((phase, start_wait::BIND_RETRY))
            }
        }
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inspect(error) => fmt::Display::fmt(error, f),
            Self::Space(shortfall) => f.write_str(shortfall),
            Self::Open(error) => fmt::Display::fmt(error, f),
        }
    }
}

fn try_open(config: &ServerConfig, id: &str) -> Result<Store, Refusal> {
    let floor = config.open_floor().map_err(Refusal::Inspect)?;
    if let Some(shortfall) = floor
        .shortfall()
        .map_err(|error| Refusal::Open(error.into()))?
    {
        return Err(Refusal::Space(shortfall));
    }
    config.reopen_store(id).map_err(Refusal::Open)
}

/// Keep accepting and disconnecting peers while closing, replaying or waiting for media.
pub(super) async fn run(
    old: Store,
    config: Arc<ServerConfig>,
    id: Arc<str>,
    sockets: &Sockets<'_>,
    stopped: impl Future<Output = Result<(), Box<dyn Error>>>,
) -> Result<Option<Store>, Box<dyn Error>> {
    sockets
        .reject_while(reopen(old, config, id, stopped, status), |line| {
            eprintln!("{line}")
        })
        .await
}

pub(super) async fn reopen(
    old: Store,
    config: Arc<ServerConfig>,
    id: Arc<str>,
    stopped: impl Future<Output = Result<(), Box<dyn Error>>>,
    mut report: impl FnMut(&str, &str),
) -> Result<Option<Store>, Box<dyn Error>> {
    tokio::pin!(stopped);
    report("closing the old RocksDB handle", &id);
    let mut close = tokio::task::spawn_blocking(move || old.close_for_reopen());
    let pending: Arc<[String]> = tokio::select! {
        biased;
        stop = &mut stopped => {
            report("stopping after native close", &id);
            unverified(&id, &close.await?);
            stop?;
            return Ok(None);
        }
        closed = &mut close => closed?.into(),
    };
    let mut backoff = start_wait::BIND_RETRY;
    let mut last_hold = None;
    loop {
        if last_hold.is_none() {
            report("opening and replaying the original store", &id);
        }
        let mut attempt = tokio::task::spawn_blocking({
            let (config, id, pending) = (config.clone(), id.clone(), pending.clone());
            move || {
                let store = try_open(&config, &id)?;
                let missing = missing_occurrences(&store, &pending)
                    .map_err(|error| Refusal::Open(error.into()))?;
                Ok::<_, Refusal>((store, missing))
            }
        });
        let (opened, stop) = tokio::select! {
            biased;
            stop = &mut stopped => {
                report("stopping after the native reopen attempt", &id);
                (attempt.await, Some(stop))
            }
            opened = &mut attempt => (opened, None),
        };
        let opened = opened.inspect_err(|_| unverified(&id, &pending))?;
        if let Some(stop) = stop {
            match opened {
                Ok((store, missing)) => {
                    reconciled(&id, &pending, &missing, &mut report);
                    tokio::task::spawn_blocking(move || drop(store)).await?;
                }
                Err(error) => {
                    report("native reopen refused during stop", &error.to_string());
                    unverified(&id, &pending);
                }
            }
            stop?;
            return Ok(None);
        }
        let error = match opened {
            Ok((store, missing)) => {
                reconciled(&id, &pending, &missing, &mut report);
                return Ok(Some(store));
            }
            Err(error) => error,
        };
        let Some((phase, base)) = error.retry() else {
            unverified(&id, &pending);
            return Err(format!("cannot reopen original store {id}: {error}").into());
        };
        if last_hold != Some(phase) {
            backoff = base;
        }
        report(
            phase,
            &format!("store={id} {error}. Reopening in {} s", backoff.as_secs()),
        );
        last_hold = Some(phase);
        tokio::select! {
            biased;
            stop = &mut stopped => {
                unverified(&id, &pending);
                stop?;
                return Ok(None);
            }
            () = tokio::time::sleep(backoff) => {}
        }
        backoff = backoff.saturating_mul(2).min(start_wait::OPEN_RETRY);
    }
}

fn reconciled(
    id: &str,
    pending: &[String],
    missing: &[String],
    report: &mut impl FnMut(&str, &str),
) {
    for occurrence in missing {
        eprintln!(
            "sparrowd: store recovery missing buffered occurrence store={id} occurrence={occurrence}"
        );
    }
    report(
        "replay complete",
        &format!(
            "store={id} pending_occurrences={} missing_occurrences={}",
            pending.len(),
            missing.len()
        ),
    );
}

pub(super) fn unverified(id: &str, pending: &[String]) {
    if !pending.is_empty() {
        eprintln!(
            "sparrowd: store stopped with unverified buffered occurrences store={id} count={} occurrences={pending:?}",
            pending.len()
        );
    }
}

fn missing_occurrences(
    store: &Store,
    pending: &[String],
) -> Result<Vec<String>, sparrow_core::Error> {
    let mut missing = Vec::new();
    for id in pending {
        match store.artifact(id) {
            Ok(_) => {}
            Err(error) if error.code == "NOT_FOUND" => missing.push(id.clone()),
            Err(error) => return Err(error),
        }
    }
    Ok(missing)
}
