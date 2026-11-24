// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The workers and RPC capabilities derived from one RocksDB open.

use super::{
    SERVING,
    maintenance::{self, End},
    shutdown::{self, Workers},
};
use sparrow_core::Store;
use sparrow_server::{
    Rpc, ServerConfig,
    admission::{Admission, Ceiling},
    archive,
    local_endpoint::LocalEndpoint,
    pressure_wake::PressureWake,
    serving::{self, Sockets},
    start_wait::{self, Wait},
    syslog_listener::{self, SyslogRegistry, SyslogWorker},
    watches::{ConsumeWorker, PrivateIntake, WatchRegistry},
};
use std::{error::Error, sync::Arc, time::Duration};
use tokio::{signal::unix::Signal, sync::Semaphore, task::LocalSet};

#[derive(Clone)]
pub(super) struct Process {
    pub config: Arc<ServerConfig>,
    pub intake: bool,
    pub intake_label: &'static str,
    pub private: Option<PrivateIntake>,
    pub archive: Option<archive::Filesystem>,
}

pub(super) struct Session {
    pub store: Store,
    operations: Arc<Semaphore>,
    admission: Arc<Admission>,
    watches: WatchRegistry,
    workers: Workers,
}

impl Session {
    pub(super) async fn prepare(store: Store, process: &Process) -> Result<Self, Box<dyn Error>> {
        let process = process.clone();
        tokio::task::spawn_blocking(move || Self::prepare_blocking(store, &process))
            .await?
            .map_err(start_wait::consume_start_failure)
    }

    fn prepare_blocking(
        store: Store,
        process: &Process,
    ) -> Result<Self, sparrow_server::consumption::Error> {
        let config = &process.config;
        sparrow_server::staging_refusal::install(&store)?;
        let operations = Arc::new(Semaphore::new(config.max_concurrent_requests));
        let admission = Admission::new(operations.clone());
        // A reclamation commit wakes begin-import callers waiting for journal capacity.
        store.set_import_slot_waker(Some(Arc::new({
            let admission = admission.clone();
            move || admission.import_slot_freed()
        })));
        let (watches, consumer) = if config.query_only || !process.intake {
            (WatchRegistry::stored(&store)?, None)
        } else {
            let watches = WatchRegistry::default();
            let consumer = ConsumeWorker::start_with(
                store.clone(),
                &config.consume,
                config.socket_path.clone(),
                watches.clone(),
                operations.clone(),
                process.private.clone(),
                config.output_paths(),
            )?;
            (watches, Some(consumer))
        };
        Ok(Self {
            store,
            operations,
            admission,
            watches,
            workers: Workers {
                consumer,
                ..Default::default()
            },
        })
    }

    pub(super) async fn prepare_replacement(
        store: Store,
        process: &Process,
        sockets: &Sockets<'_>,
        signal: &mut Signal,
    ) -> Result<Option<Self>, Box<dyn Error>> {
        sockets
            .reject_while(
                async {
                    let preparing = Self::prepare(store, process);
                    tokio::pin!(preparing);
                    tokio::select! {
                        biased;
                        result = &mut preparing => result.map(Some),
                        stop = interrupted(signal) => {
                            super::recovery::status(
                                "stopping after consumer initialization",
                                "waiting for the owned storage session to settle",
                            );
                            preparing.await?.finish(stop.map(|()| End::Stopped), &[]).await?;
                            Ok(None)
                        }
                    }
                },
                journal,
            )
            .await
    }

    pub(super) async fn finish(
        &mut self,
        result: Result<End, Box<dyn Error>>,
        endpoints: &[&LocalEndpoint],
    ) -> Result<End, Box<dyn Error>> {
        shutdown::finish(
            result,
            std::mem::take(&mut self.workers),
            &self.store,
            endpoints,
            &self.admission,
        )
        .await
    }

    async fn start_syslog(
        &self,
        config: &Arc<ServerConfig>,
        registry: &SyslogRegistry,
        wake: &PressureWake,
        signal: &mut Signal,
    ) -> Result<Wait<Option<SyslogWorker>>, Box<dyn Error>> {
        let spill = config.syslog_spill_dir();
        let mut checked = tokio::task::spawn_blocking({
            let config = config.clone();
            let spill = spill.clone();
            move || syslog_listener::check_spill_owners(&spill, &config.syslog)
        });
        // Initial startup retains a pending interrupt for the existing serving teardown.
        // A replacement already has bound listeners and can stop after this owned check.
        tokio::select! {
            result = &mut checked => result?.map_err(start_wait::bind_failure)?,
            stop = interrupted(signal), if SERVING.load(std::sync::atomic::Ordering::Relaxed) => {
                super::recovery::status("stopping after syslog spill inspection", &spill.display().to_string());
                checked.await?.map_err(start_wait::bind_failure)?;
                stop?;
                return Ok(Wait::Interrupted);
            }
        }
        if config.syslog.is_empty() {
            return Ok(Wait::Ready(None));
        }
        start_wait::until(
            signal,
            "bind the syslog listeners",
            async || {
                SyslogWorker::start(
                    self.store.clone(),
                    &config.syslog,
                    self.operations.clone(),
                    registry.clone(),
                    wake.clone(),
                    &spill,
                )
                .await
                .map(Some)
            },
            start_wait::bind_hold,
        )
        .await
        .map_err(start_wait::bind_failure)
    }

    pub(super) async fn serve(
        &mut self,
        process: &Process,
        sockets: &Sockets<'_>,
        signal: &mut Signal,
    ) -> Result<End, Box<dyn Error>> {
        let config = &process.config;
        let wake = PressureWake::default();
        let syslog = SyslogRegistry::default();
        let start = self.start_syslog(config, &syslog, &wake, signal);
        let started = if SERVING.load(std::sync::atomic::Ordering::Relaxed) {
            sockets.reject_while(start, journal).await
        } else {
            start.await
        };
        match started {
            Ok(Wait::Ready(worker)) => self.workers.syslog = worker,
            stopped => return self.finish(stopped.map(|_| End::Stopped), &[]).await,
        }
        let connections = Ceiling::new("RPC connection ceiling", config.max_rpc_connections);
        let archive_status = archive::ArchiveStatus::new();
        let mut rpc = Rpc::new(
            self.store.clone(),
            self.operations.clone(),
            config.max_pin_members,
        )
        .with_source_bindings(config.source_bindings.clone())
        .with_archive_status(archive_status.clone())
        .with_admission(self.admission.clone())
        .with_connections(connections.clone())
        .with_watches(self.watches.clone())
        .with_syslog(syslog);
        if let Some((_, socket)) = sockets.ingest {
            rpc = rpc.with_ingest_socket(socket.clone());
        }
        if config.query_only {
            rpc = rpc.query_only();
        } else if let Some(consumer) = self.workers.consumer.as_ref()
            && config.max_concurrent_requests >= 2
        {
            rpc = rpc.with_watch_controller(consumer.controller());
        }
        eprintln!(
            "sparrowd: store={} capnp={} data={} max_rpc_connections={} max_concurrent_requests={} query_only={} intake={} {}",
            self.store.store_id()?,
            config.socket_path.display(),
            config.data_dir.display(),
            config.max_rpc_connections,
            config.max_concurrent_requests,
            config.query_only,
            process.intake_label,
            self.store.open_files()
        );
        if let (Some(path), Some(gid)) = (&config.ingest_socket_path, config.ingest_socket_gid) {
            eprintln!(
                "sparrowd: ingest_socket={} gid={gid} max_ingest_connections={}",
                path.display(),
                config.max_ingest_connections
            );
        }
        if SERVING.swap(true, std::sync::atomic::Ordering::Relaxed) {
            start_wait::notify_status("serving");
        } else {
            start_wait::notify_serving();
        }
        // A query-only daemon starts neither writer maintenance nor archive.
        self.workers.maintenance = (!config.query_only).then(|| {
            maintenance::Worker::start(
                self.store.clone(),
                Duration::from_millis(u64::from(config.buffered_flush_interval_ms)),
                wake,
            )
        });
        if let Some((archive, filesystem)) = config.archive.as_ref().zip(process.archive.as_ref()) {
            let worker = archive::Worker::start(
                filesystem,
                self.store.clone(),
                archive.dir.clone(),
                Duration::from_secs(archive.check_secs),
                Duration::from_secs(archive.stall_warn_secs),
                Duration::from_secs(archive.io_timeout_secs),
                archive_status,
            );
            match worker {
                Ok(worker) => self.workers.archive = Some(worker),
                Err(error) => return self.finish(Err(error.into()), &[]).await,
            }
        }
        self.run_local(
            sockets,
            rpc,
            &connections,
            interrupted(signal),
            super::recovery::status,
        )
        .await
    }

    async fn run_local(
        &mut self,
        sockets: &Sockets<'_>,
        rpc: Rpc,
        connections: &Arc<Ceiling>,
        interrupt: impl std::future::Future<Output = Result<(), Box<dyn Error>>>,
        report: impl FnMut(&str, &str),
    ) -> Result<End, Box<dyn Error>> {
        let local = LocalSet::new();
        let outcome = local
            .run_until(serve_session(
                self,
                sockets,
                &rpc,
                connections,
                interrupt,
                report,
            ))
            .await;
        // Cancel the connection tasks here, so transport teardown releases their
        // Cap'n Proto capabilities before the store closes.
        drop(local);
        outcome
    }
}

pub(super) async fn interrupted(signal: &mut Signal) -> Result<(), Box<dyn Error>> {
    signal
        .recv()
        .await
        .map(|_| ())
        .ok_or_else(|| "daemon interrupt subscription terminated unexpectedly".into())
}

fn journal(line: &str) {
    eprintln!("{line}");
}

async fn serve_session(
    session: &mut Session,
    sockets: &Sockets<'_>,
    rpc: &Rpc,
    connections: &Arc<Ceiling>,
    interrupt: impl std::future::Future<Output = Result<(), Box<dyn Error>>>,
    mut report: impl FnMut(&str, &str),
) -> Result<End, Box<dyn Error>> {
    let stopped = serving::serve(
        sockets,
        rpc,
        connections,
        interrupt,
        worker_health(&mut session.workers),
        journal,
    )
    .await;
    sockets
        .reject_while(
            async {
                let outcome = match stopped {
                    Ok(Some(ended)) => ended.join(&mut session.workers).await,
                    Ok(None) => Ok(End::Stopped),
                    Err(error) => Err(error),
                };
                if let Ok(End::Reopen(cause)) = &outcome {
                    rpc.cancel_queries();
                    report("draining the old session", cause);
                }
                // The LocalSet still polls admitted streams while intake workers settle.
                session.finish(outcome, &[]).await
            },
            journal,
        )
        .await
}

pub(super) enum WorkerEnded {
    Consumer,
    Syslog,
    Maintenance,
}

impl WorkerEnded {
    pub(super) async fn join(self, workers: &mut Workers) -> Result<End, Box<dyn Error>> {
        match self {
            Self::Consumer => {
                if let Some(worker) = workers.consumer.take() {
                    worker.shutdown().await?;
                }
                Err("completed-consumption worker terminated unexpectedly".into())
            }
            Self::Syslog => {
                if let Some(worker) = workers.syslog.take() {
                    worker.shutdown().await?;
                }
                Err("syslog intake worker terminated unexpectedly".into())
            }
            Self::Maintenance => match workers.maintenance.take() {
                Some(worker) => match worker.shutdown().await? {
                    end @ End::Reopen(_) => Ok(end),
                    End::Stopped => Err("maintenance worker terminated unexpectedly".into()),
                },
                None => Err("maintenance join owner missing".into()),
            },
        }
    }
}

/// Observe worker completion without taking the join owner out of its slot.
pub(super) async fn worker_health(workers: &mut Workers) -> WorkerEnded {
    tokio::select! {
        () = async {
            match workers.consumer.as_mut() {
                Some(worker) => worker.ended().await,
                None => std::future::pending().await,
            }
        } => WorkerEnded::Consumer,
        () = async {
            match workers.syslog.as_mut() {
                Some(worker) => worker.ended().await,
                None => std::future::pending().await,
            }
        } => WorkerEnded::Syslog,
        () = async {
            match workers.maintenance.as_mut() {
                Some(worker) => worker.ended().await,
                None => std::future::pending().await,
            }
        } => WorkerEnded::Maintenance,
    }
}

#[cfg(test)]
mod tests;
