// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use super::*;
use crate::recovery;
use sparrow_client::{Client, DurabilityWatermark, Error as ClientError, sparrow_capnp as wire};
use std::{
    cell::Cell,
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    sync::{Mutex, mpsc},
};
use tokio::sync::oneshot;

/// A stopped store retires its live query and idle capabilities, keeps its listeners bound
/// while the original media is absent, and serves confirmed bytes through a fresh session.
#[tokio::test]
async fn recovery_replaces_the_store_session_without_rebinding_its_listeners() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("store");
    let parked = dir.path().join("unavailable");
    let socket = data.join("rpc.sock");
    let config = sparrow_test_support::write_server_config(dir.path(), &data, &socket);
    let config = Arc::new(ServerConfig::read(&config).unwrap());
    let store = config.open_store().unwrap();
    let evidence = b"confirmed\r\nexact \xff\n";
    let artifact = store
        .ingest(
            "reopen-confirmed",
            "source",
            "log",
            evidence,
            sparrow_server::now().unwrap(),
        )
        .unwrap();
    while store.migrate_step().unwrap().is_some() {}
    let id: Arc<str> = store.store_id().unwrap().into();
    let durability = store.durability_status().unwrap();
    let old_fence = DurabilityWatermark {
        store_id: id.to_string(),
        session_id: durability.session_id,
        sequence: durability.durable_through,
    };
    let (paused, reading) = oneshot::channel();
    let paused = Mutex::new(Some(paused));
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    store
        .pause_source_reads_for_tests(move || {
            if let Some(paused) = paused.lock().unwrap().take() {
                let _ = paused.send(());
                let _ = released.lock().unwrap().recv();
            }
        })
        .unwrap();
    let private = LocalEndpoint::bind(&socket, &store).await.unwrap();
    let ingest_dir = dir.path().join("ingest");
    fs::create_dir(&ingest_dir).unwrap();
    fs::set_permissions(&ingest_dir, fs::Permissions::from_mode(0o2750)).unwrap();
    let gid = fs::metadata(&ingest_dir).unwrap().gid();
    let ingest_path = ingest_dir.join("rpc.sock");
    let ingest = LocalEndpoint::bind_ingest(&ingest_path, gid).await.unwrap();
    let producer_socket =
        sparrow_server::ingest_socket::IngestSocket::new(ingest_path.clone(), gid, 8);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let secret = dir.path().join("tcp.secret");
    fs::write(&secret, [9; 32]).unwrap();
    fs::set_permissions(&secret, fs::Permissions::from_mode(0o600)).unwrap();
    let sockets = Sockets {
        private: &private,
        ingest: Some((&ingest, &producer_socket)),
        tcp: Some((&listener, sparrow_client::TcpSecret::read(&secret).unwrap())),
    };
    let names = || {
        [
            fs::metadata(&socket).unwrap().ino(),
            fs::metadata(&ingest_path).unwrap().ino(),
        ]
    };
    let bound_names = names();
    let process = Process {
        config: config.clone(),
        intake: false,
        intake_label: "false",
        private: None,
        archive: None,
    };
    let mut session = Session::prepare(store.clone(), &process).await.unwrap();
    let buffered = store
        .ingest_with_durability(
            "reopen-buffered",
            "source",
            "log",
            evidence,
            None,
            sparrow_server::now().unwrap(),
            sparrow_core::Durability::Buffered,
        )
        .unwrap();
    assert!(buffered.buffered.is_some());
    store
        .latch_durability_for_tests("injected fatal WAL failure")
        .unwrap();
    // The latch exists before maintenance starts. Keep recovery paused until the old read owns
    // a native snapshot, so a timer cannot certify the buffered write or retire the old session.
    store.space_floor().set_free_for_tests(Some(0));
    session.workers.maintenance = Some(maintenance::Worker::start_paced(
        store.clone(),
        Duration::from_millis(10),
        PressureWake::default(),
        maintenance::Pace {
            bound: Duration::ZERO,
            ..maintenance::Pace::DAEMON
        },
    ));
    let (draining, drained) = oneshot::channel();
    let (unavailable, absent) = oneshot::channel();
    let (ready, serving) = oneshot::channel();
    let (stop, stopped) = oneshot::channel::<()>();
    let mut draining = Some(draining);
    let mut unavailable = Some(unavailable);
    let waiting_for = Cell::new("the old connections");
    let recovery_phase = std::cell::RefCell::new(String::new());
    let daemon = async {
        let connections = Ceiling::new("RPC connection ceiling", 8);
        let rpc = Rpc::new(
            session.store.clone(),
            session.operations.clone(),
            config.max_pin_members,
        )
        .with_admission(session.admission.clone());
        let end = session
            .run_local(
                &sockets,
                rpc,
                &connections,
                std::future::pending(),
                |phase, _| {
                    assert_eq!(phase, "draining the old session");
                    draining.take().unwrap().send(()).unwrap();
                },
            )
            .await
            .unwrap();
        assert!(matches!(end, End::Reopen(_)));
        let replacement = sockets
            .reject_while(
                recovery::reopen(
                    session.store,
                    config.clone(),
                    id.clone(),
                    std::future::pending(),
                    |phase, detail| {
                        *recovery_phase.borrow_mut() = format!("{phase}: {detail}");
                        match phase {
                            "opening and replaying the original store" => {
                                fs::rename(&data, &parked).unwrap()
                            }
                            "waiting for the store path" => {
                                unavailable.take().unwrap().send(()).unwrap()
                            }
                            "replay complete" => {
                                assert!(
                                    detail.contains("pending_occurrences=1 missing_occurrences=0")
                                );
                            }
                            _ => {}
                        }
                    },
                ),
                journal,
            )
            .await
            .unwrap()
            .unwrap();
        let mut replacement = Session::prepare(replacement, &process).await.unwrap();
        let connections = Ceiling::new("RPC connection ceiling", 8);
        let rpc = Rpc::new(
            replacement.store.clone(),
            replacement.operations.clone(),
            config.max_pin_members,
        )
        .with_admission(replacement.admission.clone());
        ready.send(()).unwrap();
        let end = replacement
            .run_local(
                &sockets,
                rpc,
                &connections,
                async {
                    stopped.await?;
                    Ok(())
                },
                |_, _| panic!("unexpected second recovery"),
            )
            .await
            .unwrap();
        assert!(matches!(end, End::Stopped));
    };
    let client = LocalSet::new();
    let exercise = client.run_until(async {
        let idle = Client::connect(&socket).await.unwrap();
        let active = Client::connect(&socket).await.unwrap();
        let mut request = active.capability().sources_list_request();
        let mut query = request.get().init_query();
        query.set_store_id(&id);
        query.set_schema_version(sparrow_client::SCHEMA_VERSION);
        query.set_page_size(200);
        query.set_scan_budget(10_000);
        let listing = tokio::task::spawn_local(request.send().promise);
        waiting_for.set("the old query's native snapshot");
        reading.await.unwrap();
        store.space_floor().set_free_for_tests(None);
        drop(store);
        waiting_for.set("retirement to start");
        drained.await.unwrap();
        assert!(!listing.is_finished());
        waiting_for.set("a TCP disconnect during retirement");
        assert!(
            Client::connect_tcp(address)
                .await
                .err()
                .unwrap()
                .retryable()
        );
        release.send(()).unwrap();
        waiting_for.set("the original store to be reported unavailable");
        absent.await.unwrap();
        assert!(!data.exists());
        waiting_for.set("a producer disconnect while the store is unavailable");
        assert!(
            Client::connect(&ingest_path)
                .await
                .err()
                .unwrap()
                .retryable()
        );
        waiting_for.set("the idle capability to disconnect");
        assert!(
            idle.bytes(&artifact.id, 0, 1)
                .await
                .unwrap_err()
                .retryable()
        );
        waiting_for.set("the active query to finish with a retryable outcome");
        match listing.await.unwrap() {
            Ok(reply) => {
                let result = reply.get().unwrap().get_result().unwrap();
                let wire::result::Error(error) = result.which().unwrap() else {
                    panic!("a cancelled old query published a result");
                };
                let error = error.unwrap();
                assert_eq!(error.get_code().unwrap().to_str().unwrap(), "SHUTTING_DOWN");
                assert!(error.get_retryable());
            }
            Err(error) => assert_eq!(error.kind, capnp::ErrorKind::Disconnected),
        }
        fs::rename(&parked, &data).unwrap();
        waiting_for.set("the replacement session to open");
        serving.await.unwrap();
        assert_eq!(names(), bound_names);
        waiting_for.set("the replacement endpoints to serve");
        let client = Client::connect(&socket).await.unwrap();
        let discovery = client.discovery();
        assert_eq!(discovery.store_id, old_fence.store_id);
        assert_ne!(discovery.durability_session, old_fence.session_id);
        for client in [
            Client::connect_tcp(address).await.unwrap(),
            Client::connect(&ingest_path).await.unwrap(),
        ] {
            assert_eq!(client.discovery().store_id, discovery.store_id);
            assert_eq!(
                client.discovery().durability_session,
                discovery.durability_session
            );
        }
        assert_eq!(
            client
                .bytes(&artifact.id, 0, evidence.len() as u32)
                .await
                .unwrap(),
            evidence
        );
        assert_eq!(
            client
                .bytes(&buffered.id, 0, evidence.len() as u32)
                .await
                .unwrap(),
            evidence
        );
        assert!(matches!(
            client.flush(&old_fence).await,
            Err(ClientError::Domain { code, .. }) if code == "STALE_WATERMARK"
        ));
        waiting_for.set("the replacement session to stop");
        stop.send(()).unwrap();
    });
    tokio::time::timeout(Duration::from_secs(45), async {
        tokio::join!(daemon, exercise);
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "timed out waiting for {} ({})",
            waiting_for.get(),
            recovery_phase.borrow()
        )
    });
}
