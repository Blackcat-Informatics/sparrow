// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Typed wire acceptance through a real daemon and private Unix socket.

use sparrow_server::{connect, sparrow_capnp as wire};
use sparrow_test_support::write_server_config;
use std::{
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

// A child can briefly inherit CLOEXEC sockets before exec closes them.
static PROCESS_START: std::sync::Mutex<()> = std::sync::Mutex::new(());
type TcpReservation = (std::net::TcpListener, std::sync::MutexGuard<'static, ()>);
trait ProcessStart {
    type Child;
    fn native_spawn(&mut self) -> std::io::Result<Self::Child>;
    fn admitted_spawn(&mut self) -> std::io::Result<Self::Child> {
        let _guard = PROCESS_START.lock().unwrap();
        self.native_spawn()
    }
}
impl ProcessStart for Command {
    type Child = Child;
    fn native_spawn(&mut self) -> std::io::Result<Self::Child> {
        self.spawn()
    }
}
impl ProcessStart for tokio::process::Command {
    type Child = tokio::process::Child;
    fn native_spawn(&mut self) -> std::io::Result<Self::Child> {
        self.spawn()
    }
}
trait ProcessOutput {
    fn admitted_output(&mut self) -> std::io::Result<std::process::Output>;
}
impl ProcessOutput for Command {
    fn admitted_output(&mut self) -> std::io::Result<std::process::Output> {
        let _guard = PROCESS_START.lock().unwrap();
        self.output()
    }
}

struct Daemon(Child);

fn configure_tcp_listener(config: &Path) -> (std::net::SocketAddr, TcpReservation) {
    let guard = PROCESS_START.lock().unwrap();
    // Released bind(0) ports race the kernel's outbound ephemeral allocations.
    static NEXT_PORT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(10240);
    let range = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range").unwrap();
    let bounds = range
        .split_whitespace()
        .map(|value| value.parse::<u32>().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(bounds.len(), 2);
    let mut reservation = None;
    for _ in 0..128 {
        let mut port = NEXT_PORT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if port >= bounds[0] {
            port += bounds[1] - bounds[0] + 1;
        }
        assert!(
            port <= u32::from(u16::MAX),
            "no non-ephemeral unprivileged test port is available"
        );
        match std::net::TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, port as u16)) {
            Ok(listener) => {
                reservation = Some(listener);
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => continue,
            Err(error) => panic!("test TCP listener admission failed: {error}"),
        }
    }
    let reservation = reservation.expect("test TCP listener range is occupied");
    let port = reservation.local_addr().unwrap().port();
    let mut text = std::fs::read_to_string(config).unwrap();
    text.push_str(&format!("rpc_tcp_address = \"0.0.0.0:{port}\"\n"));
    std::fs::write(config, text).unwrap();
    (([127, 0, 0, 1], port).into(), (reservation, guard))
}

#[tokio::test(flavor = "current_thread")]
async fn typed_tail_handoff_retires_original_checkpoint_over_tcp_and_replays_through_cli_mcp_and_reopen()
 {
    tokio::task::LocalSet::new().run_until(async {
        use purrdf_lex::json::Object;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("active");
        std::fs::create_dir(&root).unwrap();
        let input = root.join("finished.log");
        std::fs::write(&input, b"first\nlast").unwrap();
        let data = dir.path().join("store");
        let socket = data.join("rpc.sock");
        let config = write_server_config(dir.path(), &data, &socket);
        let (address, reservation) = configure_tcp_listener(&config);
        drop(reservation);
        append_tail_config(&config, &root, "log");
        let daemon = start(&config, &socket).await;
        let client = sparrow_client::Client::connect_tcp(address).await.unwrap();
        watch_until(client.capability(), |watch| watch.get_tail_checkpoint_records() == 1).await;
        let status = cli(&socket, &["status"]);
        let generation = status["consume_watches"][0]["tail_generation_ids"][0].as_str().unwrap().to_owned();
        let intent_path = dir.path().join("handoff.capnp");
        let early = Command::new(env!("CARGO_BIN_EXE_sparrow")).arg(&socket).args([
            "watch", "handoff-prepare", "tail-retire", "completed", "1", "finished.log", intent_path.to_str().unwrap(), &generation,
        ]).admitted_output().unwrap();
        assert!(!early.status.success());
        assert!(!intent_path.exists());
        assert!(!root.join(".sparrow-inflight").exists());
        let observed = sparrow_client::CompletedFileIdentity::capture(&input.metadata().unwrap());
        let finish = sparrow_client::TailFinishIntent {
            store_id: client.discovery().store_id.clone(),
            request_id: "tail-retire-finish".into(),
            generation_id: generation.clone(),
            watch_revision: 1,
            observed,
        };
        client.tail_finish(&finish).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if client.tail_finish_get(&finish).await.unwrap().finished_at.is_some() { break; }
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let original_records = cli(&socket, &["records", "--source", "consume-acceptance"])["rows"].clone();
        let original_checkpoint = cli(&socket, &["file-generation", &finish.store_id, &generation]);
        let prepared = cli(&socket, &[
            "watch", "handoff-prepare", "tail-retire", "completed", "1", "finished.log", intent_path.to_str().unwrap(), &generation,
        ]);
        assert_eq!(prepared["phase"].as_str(), Some("prepared"));
        let saved = std::fs::read(&intent_path).unwrap();
        let intent = sparrow_client::CompletedHandoff::read(saved.as_slice()).unwrap();
        assert_eq!(intent.tail_generation_id.as_deref(), Some(generation.as_str()));
        assert_eq!(intent.generation, observed);
        let mut missing_target = intent.clone();
        missing_target.request_id = "tail-retire-no-target".into();
        missing_target.tail_generation_id = None;
        assert!(matches!(client.handoff(&missing_target).await, Err(sparrow_client::Error::Domain { code, .. }) if code == "CONFLICT"));
        assert!(input.exists());
        assert!(!root.join(".sparrow-inflight").exists());
        let admitted = client.handoff(&intent).await.unwrap();
        assert_eq!(admitted.request, intent);
        let deadline = Instant::now() + Duration::from_secs(5);
        let completed = loop {
            let receipt = client.handoff_get(&intent).await.unwrap();
            if receipt.phase == wire::HandoffPhase::Removed { break receipt; }
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert_eq!(completed.import_id, admitted.import_id);
        assert!(completed.artifact_id.is_none());
        assert_eq!(completed.tail_generation_id.as_deref(), Some(generation.as_str()));
        let ingestion = agent_ingestion_status(&socket, client.capability(), &intent.store_id, &sparrow_client::IngestSelector::Handoff(intent.request_id.clone())).await;
        assert_eq!(ingestion["deletion"].as_str(), Some("removed"));
        assert_eq!(ingestion["total_records"].as_str(), Some("0"));
        assert_eq!(ingestion["inspected_records"].as_str(), Some("0"));
        assert_eq!(ingestion["tail"]["generation_id"].as_str(), Some(generation.as_str()));
        assert!(ingestion["accepted"].is_null());
        assert!(!ingestion["removed_at"].is_null());
        assert!(!input.exists());
        assert_eq!(client.bytes(&generation, 0, 10).await.unwrap(), b"first\nlast");
        std::fs::write(&input, b"replacement stays independent").unwrap();
        let rendered = cli(&socket, &["watch", "handoff-status", intent_path.to_str().unwrap()]);
        assert_eq!(rendered["phase"].as_str(), Some("removed"));
        assert_eq!(rendered["tail_generation_id"].as_str(), Some(generation.as_str()));
        assert!(rendered["artifact_id"].is_null());
        assert_eq!(cli(&socket, &["watch", "handoff", intent_path.to_str().unwrap()]), rendered);
        let arguments = Object::new().with("store_id", intent.store_id.clone()).with("request_id", intent.request_id.clone())
            .with("watch", intent.watch.clone()).with("watch_revision", intent.watch_revision.to_string())
            .with("relative_path_hex", purrdf_hash::hex::encode(&intent.relative_path)).with("tail_generation_id", generation.clone())
            .with("observed", Object::new().with("device", observed.device.to_string()).with("inode", observed.inode.to_string())
                .with("byte_length", observed.length.to_string()).with("modified_seconds", observed.modified_seconds).with("modified_nanos", observed.modified_nanos)
                .with("changed_seconds", observed.changed_seconds).with("changed_nanos", observed.changed_nanos));
        let mut mcp = Mcp::start(&socket);
        mcp.initialize(1).await;
        for (number, operation) in [(2, "watch_handoff_get"), (3, "watch_handoff")] {
            let response = mcp.tool(number, operation, arguments.clone().into()).await;
            assert_eq!(response["isError"].as_bool(), Some(false));
            assert_eq!(response["structuredContent"], rendered);
        }
        let mut conflicting = intent.clone();
        conflicting.tail_generation_id = Some("file-generation:another".into());
        assert!(matches!(client.handoff(&conflicting).await, Err(sparrow_client::Error::Domain { code, .. }) if code == "CONFLICT"));
        assert_eq!(std::fs::read(&intent_path).unwrap(), saved);
        assert_eq!(cli(&socket, &["records", "--source", "consume-acceptance"])["rows"], original_records);
        assert_eq!(cli(&socket, &["file-generation", &intent.store_id, &generation]), original_checkpoint);
        drop(mcp); drop(client); drop(daemon);
        std::fs::remove_file(&socket).unwrap();
        let _daemon = start(&config, &socket).await;
        let client = sparrow_client::Client::connect_tcp(address).await.unwrap();
        assert_eq!(client.handoff(&intent).await.unwrap(), completed);
        assert_eq!(cli(&socket, &["watch", "handoff-status", intent_path.to_str().unwrap()]), rendered);
        assert_eq!(client.bytes(&generation, 0, 10).await.unwrap(), b"first\nlast");
        assert_eq!(std::fs::read(&input).unwrap(), b"replacement stays independent");
        let status = cli(&socket, &["status"]);
        assert_eq!(status["store_format"].as_str(), Some("sparrow.store/59"));
        assert_eq!(status["live_records"].as_str(), Some("2"));
        assert_eq!(cli(&socket, &["records", "--source", "consume-acceptance"])["rows"], original_records);
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_tail_completion_preserves_partial_bytes_retry_clocks_and_retained_sources_across_reopen()
 {
    tokio::task::LocalSet::new()
        .run_until(async {
            use purrdf_lex::json::Object;
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("active");
            std::fs::create_dir(&root).unwrap();
            let input = root.join("finished.log");
            std::fs::write(&input, b"first\r\nlast").unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let (address, reservation) = configure_tcp_listener(&config);
            drop(reservation);
            append_tail_config(&config, &root, "log");
            let daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect_tcp(address).await.unwrap();
            watch_until(client.capability(), |watch| {
                watch.get_tail_checkpoint_records() == 1
            })
            .await;
            let status = cli(&socket, &["status"]);
            let id = status["consume_watches"][0]["tail_generation_ids"][0]
                .as_str()
                .unwrap();
            let store = client.discovery().store_id.clone();
            assert!(matches!(client.bytes(id, 0, 1).await, Err(sparrow_client::Error::Domain { code, .. }) if code == "EVIDENCE_UNAVAILABLE"));
            let previous =
                cli(&socket, &["records", "--source", "consume-acceptance"])["rows"][0].clone();
            let intent_path = dir.path().join("finish.capnp");
            let prepared = cli(
                &socket,
                &[
                    "file-finish-prepare",
                    "producer-finish",
                    &store,
                    id,
                    input.to_str().unwrap(),
                    intent_path.to_str().unwrap(),
                ],
            );
            assert_eq!(prepared["phase"].as_str(), Some("prepared"));
            let saved = std::fs::read(&intent_path).unwrap();
            let intent = sparrow_client::TailFinishIntent::read(saved.as_slice()).unwrap();
            for destination in [intent_path.clone(), root.join("cannot-ingest-intent.capnp")] {
                let output = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                    .arg(&socket)
                    .args([
                        "file-finish-prepare",
                        "producer-finish",
                        &store,
                        id,
                        input.to_str().unwrap(),
                        destination.to_str().unwrap(),
                    ])
                    .admitted_output()
                    .unwrap();
                assert!(!output.status.success());
            }
            assert_eq!(std::fs::read(&intent_path).unwrap(), saved);
            assert!(!root.join("cannot-ingest-intent.capnp").exists());
            assert!(matches!(client.tail_finish_get(&intent).await.unwrap_err(),
            sparrow_client::Error::Domain { ref code, .. } if code == "NOT_FOUND"));
            let mut changed = intent.clone();
            changed.observed.length += 1;
            assert!(matches!(client.tail_finish(&changed).await.unwrap_err(),
            sparrow_client::Error::Domain { ref code, .. } if code == "CONFLICT"));
            let admitted = client.tail_finish(&intent).await.unwrap();
            let finished = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let receipt = client.tail_finish_get(&intent).await.unwrap();
                    if receipt.finished_at.is_some() {
                        break receipt;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(finished.admitted_at, admitted.admitted_at);
            assert_eq!(finished.offset, 11);
            assert_eq!(finished.records, 2);
            assert!(finished.gap.is_none());
            assert_eq!(client.tail_finish(&intent).await.unwrap(), finished);
            assert_eq!(client.bytes(id, 0, 11).await.unwrap(), b"first\r\nlast");
            assert_eq!(client.bytes(id, 5, 6).await.unwrap(), b"\r\nlast");
            let export = dir.path().join("whole-file-export.log");
            cli(&socket, &["bytes", &store, id, "0", "11", export.to_str().unwrap()]);
            assert_eq!(std::fs::read(&export).unwrap(), b"first\r\nlast");
            for (foreign, expected) in [(true, "STORE_MISMATCH"), (false, "UNSUPPORTED")] {
                let mut call = client.capability().bytes_read_request();
                let mut query = call.get().init_request().init_file_generation();
                query.set_store_id(if foreign { "store:foreign" } else { &store });
                query.set_id(id);
                query.set_schema_version(if foreign { sparrow_client::SCHEMA_VERSION } else { sparrow_client::SCHEMA_VERSION - 1 });
                let response = call.send().promise.await.unwrap();
                assert!(matches!(response.get().unwrap().get_result().unwrap().which().unwrap(), wire::result::Error(error) if error.as_ref().unwrap().get_code().unwrap().to_str().unwrap() == expected));
            }
            let original_rows =
                cli(&socket, &["records", "--source", "consume-acceptance"])["rows"].clone();
            assert_eq!(original_rows.as_array().unwrap().len(), 2);
            assert!(original_rows.as_array().unwrap().contains(&previous));
            assert_eq!(std::fs::read(&input).unwrap(), b"first\r\nlast");
            assert!(!root.join(".sparrow-inflight").exists());
            let rendered = cli(
                &socket,
                &["file-finish-status", intent_path.to_str().unwrap()],
            );
            assert_eq!(rendered["complete"].as_bool(), Some(true));
            let observed = intent.observed;
            let arguments = Object::new()
                .with("store_id", store.clone())
                .with("request_id", intent.request_id.clone())
                .with("generation_id", id)
                .with("watch_revision", intent.watch_revision.to_string())
                .with(
                    "observed",
                    Object::new()
                        .with("device", observed.device.to_string())
                        .with("inode", observed.inode.to_string())
                        .with("byte_length", observed.length.to_string())
                        .with("modified_seconds", observed.modified_seconds)
                        .with("modified_nanos", observed.modified_nanos)
                        .with("changed_seconds", observed.changed_seconds)
                        .with("changed_nanos", observed.changed_nanos),
                );
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            for (number, operation) in [(2, "file_finish_get"), (3, "file_finish")] {
                let result = mcp.tool(number, operation, arguments.clone().into()).await;
                assert_eq!(result["isError"].as_bool(), Some(false));
                assert_eq!(result["structuredContent"], rendered);
            }

            let range = mcp.tool(4, "bytes_read", Object::new().with("store_id", store.clone())
                .with("id", id).with("offset", "5").with("length", 6_u64).with("encoding", "utf8").into()).await;
            assert_eq!(range["isError"].as_bool(), Some(false));
            assert_eq!(range["structuredContent"]["data"].as_str(), Some("\r\nlast"));
            drop(mcp);
            drop(client);
            drop(daemon);
            std::fs::remove_file(&socket).unwrap();
            let daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect_tcp(address).await.unwrap();
            watch_until(client.capability(), |watch| {
                watch.get_coverage().unwrap() == wire::WatchCoverage::Covered
                    && watch.get_tail_descriptors() == 0
                    && watch.get_tail_checkpoint_records() == 2
            })
            .await;
            assert_eq!(client.tail_finish_get(&intent).await.unwrap(), finished);
            assert_eq!(
                cli(&socket, &["records", "--source", "consume-acceptance"])["rows"],
                original_rows
            );
            std::fs::write(&input, b"producer violated finality\n").unwrap();
            watch_until(client.capability(), |watch| {
                matches!(
                    watch.get_failure().unwrap().which().unwrap(),
                    wire::optional_text::Value(_)
                )
            })
            .await;
            assert_eq!(
                cli(&socket, &["records", "--source", "consume-acceptance"])["rows"],
                original_rows
            );
            assert_eq!(client.tail_finish_get(&intent).await.unwrap(), finished);
            assert_eq!(client.bytes(id, 0, 11).await.unwrap(), b"first\r\nlast");
            // Original admission remains recoverable after the producer independently removes the file.
            std::fs::remove_file(&input).unwrap();
            assert_eq!(client.bytes(id, 0, 11).await.unwrap(), b"first\r\nlast");
            assert_eq!(client.tail_finish(&intent).await.unwrap(), finished);
            assert_eq!(std::fs::read(&intent_path).unwrap(), saved);
            changed.observed.length = observed.length + 1;
            assert!(matches!(client.tail_finish(&changed).await.unwrap_err(),
            sparrow_client::Error::Domain { ref code, .. } if code == "CONFLICT"));
            drop(client);
            drop(daemon);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn tail_worker_waits_for_the_shared_operation_budget_without_advancing_or_dropping_input() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("active");
    std::fs::create_dir(&root).unwrap();
    let input = root.join("current.log");
    std::fs::write(&input, b"n\n").unwrap();
    let data = dir.path().join("store");
    let socket = data.join("rpc.sock");
    let config = write_server_config(dir.path(), &data, &socket);
    append_tail_config(&config, &root, "log");
    let config = sparrow_server::ServerConfig::read(&config).unwrap();
    let store = config.open_store().unwrap();
    let operations = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
    let mut lease = Some(operations.acquire().await.unwrap());
    let registry = sparrow_server::watches::WatchRegistry::default();
    let worker = sparrow_server::watches::ConsumeWorker::start(
        store.clone(),
        &config.consume,
        socket,
        registry.clone(),
        operations.clone(),
    )
    .unwrap();
    for waiting in [true, false] {
        let state = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let state = registry.snapshot().unwrap().pop().unwrap();
                if state.tail_admission_waiting == waiting
                    && state.tail_descriptors == 1
                    && state.tail_checkpoint_records == u64::from(!waiting)
                {
                    break state;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(state.failure.is_none());
        assert_eq!(
            store
                .tail_generation(&state.tail_generation_ids[0])
                .unwrap()
                .offset,
            if waiting { 0 } else { 2 }
        );
        assert_eq!(std::fs::read(&input).unwrap(), b"n\n");
        drop(lease.take());
    }
    worker.shutdown().await.unwrap();
    assert!(!root.join(".sparrow-inflight").exists());
}

#[tokio::test(flavor = "current_thread")]
async fn pending_tail_completion_recovers_bounded_drains_and_reports_changed_or_missing_generations()
 {
    tokio::task::LocalSet::new()
        .run_until(async {
            use std::os::unix::fs::MetadataExt;
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("active");
            std::fs::create_dir(&root).unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config_path = write_server_config(dir.path(), &data, &socket);
            append_tail_config(&config_path, &root, "log");
            let text = std::fs::read_to_string(&config_path)
                .unwrap()
                .replace("max_artifact_bytes = 8388608", "max_artifact_bytes = 1024");
            std::fs::write(&config_path, text).unwrap();
            let config = sparrow_server::ServerConfig::read(&config_path).unwrap();
            let store = config.open_store().unwrap();
            let store_id = store.store_id().unwrap();
            let directory = root.metadata().unwrap();
            let mut intents = Vec::new();
            for name in ["resume.log", "changed.log", "missing.log", "empty.log"] {
                let path = root.join(name);
                let bytes = if name == "empty.log" {
                    Vec::new()
                } else {
                    let mut bytes = b"accepted\n".repeat(300);
                    bytes.extend_from_slice(b"final-without-newline");
                    bytes
                };
                std::fs::write(&path, &bytes).unwrap();
                let observed =
                    sparrow_client::CompletedFileIdentity::capture(&path.metadata().unwrap());
                let generation = store
                    .tail_start(
                        &sparrow_core::TailStart {
                            request_id: format!("restart-start:{name}"),
                            watch: "completed".into(),
                            watch_revision: 1,
                            directories: sparrow_core::ConsumptionDirectories {
                                root_device: directory.dev(),
                                root_inode: directory.ino(),
                                parent_device: directory.dev(),
                                parent_inode: directory.ino(),
                            },
                            path: name.as_bytes().to_vec(),
                            source: "consume-acceptance".into(),
                            format: "log".into(),
                            device: observed.device,
                            inode: observed.inode,
                        },
                        sparrow_server::now().unwrap(),
                    )
                    .unwrap();
                if !bytes.is_empty() {
                    store
                        .tail_append(
                            &generation.id,
                            0,
                            &bytes[..9],
                            false,
                            sparrow_server::now().unwrap(),
                        )
                        .unwrap();
                }
                let intent = sparrow_client::TailFinishIntent {
                    store_id: store_id.clone(),
                    request_id: format!("restart-finish:{name}"),
                    generation_id: generation.id.clone(),
                    watch_revision: 1,
                    observed,
                };
                store
                    .tail_finish_start(
                        &sparrow_core::TailFinishIntent {
                            request_id: intent.request_id.clone(),
                            generation_id: generation.id,
                            watch_revision: 1,
                            observed: sparrow_core::ConsumptionGeneration {
                                device: observed.device,
                                inode: observed.inode,
                                byte_length: observed.length,
                                modified: (
                                    observed.modified_seconds,
                                    observed.modified_nanos as u32,
                                ),
                                changed: (observed.changed_seconds, observed.changed_nanos as u32),
                            },
                        },
                        sparrow_server::now().unwrap(),
                    )
                    .unwrap();
                intents.push(intent);
            }
            drop(store);
            std::fs::write(root.join("changed.log"), b"changed after admission\n").unwrap();
            std::fs::remove_file(root.join("missing.log")).unwrap();
            let daemon = start(&config_path, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let receipts = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let mut receipts = Vec::new();
                    for intent in &intents {
                        receipts.push(client.tail_finish_get(intent).await.unwrap());
                    }
                    if receipts
                        .iter()
                        .all(|receipt| receipt.finished_at.is_some() || receipt.gap.is_some())
                    {
                        break receipts;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert!(receipts[0].finished_at.is_some());
            assert_eq!(receipts[0].offset, intents[0].observed.length);
            assert_eq!(receipts[0].records, 301);
            assert_eq!(receipts[1].gap.as_deref(), Some("identity_changed"));
            assert_eq!(receipts[2].gap.as_deref(), Some("read_gap"));
            for receipt in &receipts[1..3] {
                assert_eq!(receipt.offset, 9);
                assert!(receipt.finished_at.is_none());
            }
            assert_eq!(receipts[3].offset, 0);
            assert_eq!(receipts[3].records, 0);
            assert!(receipts[3].finished_at.is_some());
            assert_eq!(
                cli(&socket, &["status"])["live_records"].as_str(),
                Some("303")
            );
            for (intent, expected) in intents.iter().zip(&receipts) {
                assert_eq!(client.tail_finish(intent).await.unwrap(), *expected);
            }
            assert!(!root.join(".sparrow-inflight").exists());
            assert_eq!(
                std::fs::read(root.join("changed.log")).unwrap(),
                b"changed after admission\n"
            );
            drop(client);
            drop(daemon);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn tail_capture_refusal_preserves_bytes_and_missing_recovery_reports_a_durable_gap() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("active");
            std::fs::create_dir(&root).unwrap();
            let outside = dir.path().join("outside.log");
            std::fs::write(&outside, b"outside\n").unwrap();
            std::os::unix::fs::symlink(&outside, root.join("alias.log")).unwrap();
            let input = root.join("current.log");
            let bytes = vec![b'x'; 1025];
            std::fs::write(&input, &bytes).unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            append_tail_config(&config, &root, "log");
            let text = std::fs::read_to_string(&config)
                .unwrap()
                .replace("max_artifact_bytes = 8388608", "max_artifact_bytes = 1024");
            std::fs::write(&config, text).unwrap();
            let daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            watch_until(client.capability(), |watch| {
                matches!(
                    watch.get_failure().unwrap().which().unwrap(),
                    wire::optional_text::Value(_)
                )
            })
            .await;
            let status = cli(&socket, &["status"]);
            let watch = &status["consume_watches"][0];
            assert_eq!(watch["tail_read_bytes"].as_str(), Some("1024"));
            assert_eq!(watch["tail_descriptors"].as_u64(), Some(1));
            assert_eq!(watch["tail_checkpoint_records"].as_str(), Some("0"));
            let first_id = watch["tail_generation_ids"][0].as_str().unwrap().to_owned();
            assert_eq!(
                cli(
                    &socket,
                    &["file-generation", &client.discovery().store_id, &first_id]
                )["offset"]
                    .as_str(),
                Some("0")
            );
            assert_eq!(std::fs::read(&input).unwrap(), bytes);
            std::fs::write(&input, b"good\n").unwrap();
            watch_until(client.capability(), |watch| {
                watch.get_tail_checkpoint_records() == 1
                    && watch.get_tail_generation_ids().unwrap().len() == 2
            })
            .await;
            let status = cli(&socket, &["status"]);
            let second_id = status["consume_watches"][0]["tail_generation_ids"]
                .as_array()
                .unwrap()
                .iter()
                .map(|id| id.as_str().unwrap())
                .find(|id| *id != first_id)
                .unwrap()
                .to_owned();
            drop(client);
            drop(daemon);
            std::fs::remove_file(&socket).unwrap();
            std::fs::remove_file(&input).unwrap();
            let daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            watch_until(client.capability(), |watch| {
                watch.get_coverage().unwrap() == wire::WatchCoverage::Covered
                    && watch.get_tail_descriptors() == 0
            })
            .await;
            let old = cli(
                &socket,
                &["file-generation", &client.discovery().store_id, &second_id],
            );
            assert_eq!(old["gap"].as_str(), Some("read_gap"));
            assert_eq!(old["offset"].as_str(), Some("5"));
            assert_eq!(std::fs::read(&outside).unwrap(), b"outside\n");
            assert!(root.join("alias.log").is_symlink());
            assert!(!root.join(".sparrow-inflight").exists());
            drop(client);
            drop(daemon);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn active_tail_watches_preserve_partial_lines_rotated_descriptors_and_reopened_checkpoints() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use std::io::Write;
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("active");
            std::fs::create_dir(&root).unwrap();
            let active = root.join("current.jsonl");
            let mut writer = std::fs::OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(&active)
                .unwrap();
            writer.write_all(b"{\"n\":1}\r\n{\"n\":").unwrap();
            writer.sync_all().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            append_tail_config(&config, &root, "jsonl");
            let daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            watch_until(client.capability(), |watch| {
                watch.get_tail_checkpoint_records() == 1 && watch.get_tail_descriptors() == 1
            })
            .await;
            let first = cli(&socket, &["status"]);
            assert_eq!(
                first["consume_watches"][0]["definition"]["mode"].as_str(),
                Some("tail")
            );
            assert_eq!(
                first["consume_watches"][0]["tail_read_bytes"].as_str(),
                Some("1048576")
            );
            let first_id = first["consume_watches"][0]["tail_generation_ids"][0]
                .as_str()
                .unwrap()
                .to_owned();
            let checkpoint = cli(
                &socket,
                &["file-generation", &client.discovery().store_id, &first_id],
            );
            assert_eq!(checkpoint["offset"].as_str(), Some("9"));
            assert_eq!(checkpoint["complete"].as_bool(), Some(false));
            assert!(!root.join(".sparrow-inflight").exists());
            writer.write_all(b"2}\n").unwrap();
            writer.sync_all().unwrap();
            watch_until(client.capability(), |watch| {
                watch.get_tail_checkpoint_records() == 2
            })
            .await;
            let rotated = root.join("rotated.jsonl");
            std::fs::rename(&active, &rotated).unwrap();
            writer.write_all(b"{\"n\":3}\n").unwrap();
            writer.sync_all().unwrap();
            std::fs::write(&active, b"{\"n\":4}\n").unwrap();
            watch_until(client.capability(), |watch| {
                watch.get_tail_checkpoint_records() == 4 && watch.get_tail_descriptors() == 2
            })
            .await;
            let rows = cli(&socket, &["records", "--source", "consume-acceptance"])["rows"].clone();
            assert_eq!(rows.as_array().unwrap().len(), 4);
            assert_eq!(
                std::fs::read(&rotated).unwrap(),
                b"{\"n\":1}\r\n{\"n\":2}\n{\"n\":3}\n"
            );
            drop(client);
            drop(daemon);
            std::fs::remove_file(&socket).unwrap();
            let daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            watch_until(client.capability(), |watch| {
                watch.get_tail_checkpoint_records() == 4 && watch.get_tail_descriptors() == 2
            })
            .await;
            assert_eq!(
                cli(&socket, &["records", "--source", "consume-acceptance"])["rows"],
                rows
            );
            let before = cli(&socket, &["status"]);
            let generation_ids = before["consume_watches"][0]["tail_generation_ids"]
                .as_array()
                .unwrap();
            let current_id = generation_ids
                .iter()
                .map(|value| value.as_str().unwrap())
                .find(|id| *id != first_id)
                .unwrap()
                .to_owned();
            std::fs::write(&active, b"0\n").unwrap();
            watch_until(client.capability(), |watch| {
                watch.get_tail_checkpoint_records() == 5
                    && watch.get_tail_generation_ids().unwrap().len() == 3
            })
            .await;
            let old = cli(
                &socket,
                &["file-generation", &client.discovery().store_id, &current_id],
            );
            assert_eq!(old["gap"].as_str(), Some("truncated"));
            assert_eq!(old["offset"].as_str(), Some("8"));
            assert_eq!(std::fs::read(&active).unwrap(), b"0\n");
            assert!(!root.join(".sparrow-inflight").exists());
            drop(client);
            drop(writer);
            drop(daemon);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn file_generation_queries_preserve_raw_provenance_and_refuse_foreign_or_stale_schemas() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let (store_id, generation_id, record_id, empty_id) = {
                let store = sparrow_server::ServerConfig::read(&config)
                    .unwrap()
                    .open_store()
                    .unwrap();
                let declaration = sparrow_core::TailStart {
                    request_id: "generation-wire".into(),
                    watch: "active-log".into(),
                    watch_revision: 7,
                    directories: sparrow_core::ConsumptionDirectories {
                        root_device: 1,
                        root_inode: 2,
                        parent_device: 3,
                        parent_inode: 4,
                    },
                    path: b"nested/active\xff.log".to_vec(),
                    source: "wire-tail".into(),
                    format: "log".into(),
                    device: 5,
                    inode: 6,
                };
                let generation = store
                    .tail_start(&declaration, sparrow_server::now().unwrap())
                    .unwrap();
                let mut empty = declaration.clone();
                empty.request_id = "generation-wire-empty".into();
                empty.source = "wire-empty".into();
                empty.inode += 1;
                empty.path = b"empty.log".to_vec();
                let empty = store
                    .tail_start(&empty, sparrow_server::now().unwrap())
                    .unwrap();
                let receipt = store
                    .tail_append(
                        &generation.id,
                        0,
                        b"event\r\nunfinished",
                        false,
                        sparrow_server::now().unwrap(),
                    )
                    .unwrap();
                let record = receipt.artifact.unwrap().record_ids[0].clone();
                store
                    .tail_discontinue(
                        &generation.id,
                        7,
                        "truncated",
                        sparrow_server::now().unwrap(),
                    )
                    .unwrap();
                (store.store_id().unwrap(), generation.id, record, empty.id)
            };
            let (address, reservation) = configure_tcp_listener(&config);
            drop(reservation);
            let daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect_tcp(address).await.unwrap();
            let result = cli(&socket, &["file-generation", &store_id, &generation_id]);
            assert_eq!(result["offset"].as_str(), Some("7"));
            assert_eq!(result["records"].as_str(), Some("1"));
            assert_eq!(result["gap"].as_str(), Some("truncated"));
            assert_eq!(result["complete"].as_bool(), Some(false));
            assert_eq!(result["continuity_unknown"].as_bool(), Some(true));
            assert_eq!(
                result["declaration"]["relative_path_hex"].as_str(),
                Some("6e65737465642f616374697665ff2e6c6f67")
            );
            assert_eq!(result["declaration"]["watch_revision"].as_str(), Some("7"));
            assert_eq!(result["declaration"]["parent_inode"].as_str(), Some("4"));
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            let args = purrdf_lex::json::Object::new()
                .with("store_id", store_id.clone())
                .with("generation_id", generation_id.clone())
                .into();
            let answer = mcp.tool(2, "file_generation_get", args).await;
            assert_eq!(answer["structuredContent"], result);
            let record = mcp
                .tool(
                    3,
                    "record_get",
                    purrdf_lex::json::Object::new()
                        .with("store_id", store_id.clone())
                        .with("id", record_id.clone())
                        .into(),
                )
                .await;
            let tail = &record["structuredContent"]["tail"];
            assert_eq!(tail["generation_id"].as_str(), Some(generation_id.as_str()));
            assert_eq!(tail["source_start"].as_str(), Some("0"));
            assert_eq!(tail["source_end"].as_str(), Some("7"));
            assert_eq!(tail["declaration"], result["declaration"]);
            let sources = cli(&socket, &["sources", &store_id, "--page-size", "1"]);
            assert_eq!(sources["total_items"].as_str(), Some("2"));
            assert_eq!(sources["rows"][0]["source_id"].as_str(), Some("wire-empty"));
            assert_eq!(sources["rows"][0]["retained_records"].as_str(), Some("0"));
            assert_eq!(
                sources["rows"][0]["tail_generation_ids"][0].as_str(),
                Some(empty_id.as_str())
            );
            let continuation = mcp
                .tool(
                    4,
                    "result_next",
                    purrdf_lex::json::Object::new()
                        .with("store_id", store_id.clone())
                        .with("cursor", sources["next_cursor"].as_str().unwrap())
                        .into(),
                )
                .await;
            assert_eq!(
                continuation["structuredContent"]["rows"][0]["tail_generation_ids"][0].as_str(),
                Some(generation_id.as_str())
            );
            let empty = mcp
                .tool(
                    5,
                    "file_generation_get",
                    purrdf_lex::json::Object::new()
                        .with("store_id", store_id.clone())
                        .with("generation_id", empty_id.clone())
                        .into(),
                )
                .await;
            assert_eq!(empty["structuredContent"]["offset"].as_str(), Some("0"));
            assert_eq!(empty["structuredContent"]["records"].as_str(), Some("0"));
            let mut request = client.capability().sources_list_request();
            let mut query = request.get().init_query();
            query.set_store_id(&store_id);
            query.set_schema_version(sparrow_client::SCHEMA_VERSION);
            query.set_page_size(200);
            query.set_scan_budget(10000);
            query.init_source().set_value("wire-tail");
            let response = request.send().promise.await.unwrap();
            let rows = rpc_ok(response.get().unwrap().get_result().unwrap())
                .get_rows()
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows.get(0)
                    .get_tail_generation_ids()
                    .unwrap()
                    .get(0)
                    .unwrap()
                    .to_str()
                    .unwrap(),
                generation_id
            );
            let context = cli(
                &socket,
                &[
                    "context", &store_id, &record_id, "--before", "0", "--after", "0",
                ],
            );
            assert_eq!(context["anchor"]["tail"], *tail);
            for (store, schema, id, expected) in [
                (
                    "store:foreign",
                    sparrow_client::SCHEMA_VERSION,
                    generation_id.as_str(),
                    "STORE_MISMATCH",
                ),
                (
                    store_id.as_str(),
                    sparrow_client::SCHEMA_VERSION - 1,
                    generation_id.as_str(),
                    "UNSUPPORTED",
                ),
                (
                    store_id.as_str(),
                    sparrow_client::SCHEMA_VERSION,
                    "file-generation:missing",
                    "NOT_FOUND",
                ),
            ] {
                let mut request = client.capability().tail_generation_get_request();
                let mut query = request.get().init_query();
                query.set_store_id(store);
                query.set_schema_version(schema);
                query.set_id(id);
                let response = request.send().promise.await.unwrap();
                match response
                    .get()
                    .unwrap()
                    .get_result()
                    .unwrap()
                    .which()
                    .unwrap()
                {
                    wire::result::Error(error) => assert_eq!(
                        error.unwrap().get_code().unwrap().to_str().unwrap(),
                        expected
                    ),
                    _ => panic!("invalid generation query returned a successful prefix"),
                }
            }
            drop(client);
            drop(mcp);
            drop(daemon);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn tcp_logging_worker_retains_pending_identity_across_outage_and_daemon_reopen() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let (address, reservation) = configure_tcp_listener(&config);
            drop(reservation);
            let mut settings = sparrow_client::DeliveryConfig::tcp(address, "tcp-logging");
            settings.batch_events = 1;
            settings.rpc_timeout = Duration::from_millis(100);
            settings.retry_interval = Duration::from_millis(20);
            let delivery = sparrow_client::Delivery::start(settings).unwrap();
            assert_eq!(
                admit(&delivery, delivery_event("queued before TCP listener")).await,
                1
            );
            assert!(matches!(
                delivery_barrier(&delivery, false, Duration::from_millis(150)).await,
                Err(sparrow_client::FlushError::TimedOut(_))
            ));
            let first_pending = delivery.pending_batch().unwrap().unwrap();
            let daemon = start(&config, &socket).await;
            let first =
                reached_delivery(delivery_barrier(&delivery, false, Duration::from_secs(5)).await);
            assert_eq!(first.durable_sequence, 1);
            assert_eq!(first.durable_events, 1);
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            assert_eq!(
                client.events(&first_pending).await.unwrap(),
                first.last_receipt.clone().unwrap()
            );
            drop(client);
            drop(daemon);
            assert_eq!(
                admit(&delivery, delivery_event("queued during TCP outage")).await,
                2
            );
            assert!(matches!(
                delivery_barrier(&delivery, false, Duration::from_millis(150)).await,
                Err(sparrow_client::FlushError::TimedOut(_))
            ));
            let second_pending = delivery.pending_batch().unwrap().unwrap();
            assert_eq!(
                first_pending.client_instance,
                second_pending.client_instance
            );
            assert_ne!(first_pending.request_id, second_pending.request_id);
            assert_eq!(second_pending.events[0].sequence, 2);
            std::fs::remove_file(&socket).unwrap();
            let daemon = start(&config, &socket).await;
            let second =
                reached_delivery(delivery_barrier(&delivery, true, Duration::from_secs(5)).await);
            assert_eq!(second.durable_sequence, 2);
            assert_eq!(second.durable_events, 2);
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            assert_eq!(
                client.events(&second_pending).await.unwrap(),
                second.last_receipt.unwrap()
            );
            assert_eq!(
                client.events(&first_pending).await.unwrap(),
                first.last_receipt.unwrap()
            );
            let response = client
                .capability()
                .status_request()
                .send()
                .promise
                .await
                .unwrap();
            assert_eq!(
                rpc_ok(response.get().unwrap().get_result().unwrap()).get_live_records(),
                2
            );
            assert_eq!(delivery.stats().unwrap().pending_events, 0);
            drop(delivery);
            drop(client);
            drop(daemon);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn remote_agent_adapters_share_all_query_and_mutation_paths_and_report_connection_failure() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use purrdf_lex::json::Object;
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let (address, reservation) = configure_tcp_listener(&config);
            drop(reservation);
            let daemon = start(&config, &socket).await;
            let endpoint = format!("tcp://{address}");
            let (store, artifact, _) = exercise(endpoint.as_str()).await;
            let tcp = cli(endpoint.as_str(), &["status"]);
            let unix = cli(&socket, &["status"]);
            assert_eq!(tcp["store_id"], unix["store_id"]);
            assert_eq!(tcp["live_records"], unix["live_records"]);
            assert_eq!(tcp["store_id"].as_str(), Some(store.as_str()));
            assert_eq!(
                cli(endpoint.as_str(), &["records", "--source", "synthetic-rpc"])["rows"],
                cli(&socket, &["records", "--source", "synthetic-rpc"])["rows"]
            );
            let native = sparrow_client::Client::connect(&socket).await.unwrap();
            assert_eq!(native.artifact(&artifact).await.unwrap().id, artifact);
            drop(native);
            for invalid in ["tcp://127.0.0.1:0", "tcp://hostname:518"] {
                let result = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                    .args([invalid, "status"])
                    .admitted_output()
                    .unwrap();
                assert!(!result.status.success());
                assert!(result.stdout.is_empty());
            }
            drop(daemon); // Reaped by the shared daemon owner before testing refusal.
            let result = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                .args([&endpoint, "status"])
                .admitted_output()
                .unwrap();
            assert!(!result.status.success());
            assert!(result.stdout.is_empty());
            let mut mcp = Mcp::start(endpoint.as_str());
            mcp.initialize(1).await;
            let result = mcp.tool(2, "status", Object::new().into()).await;
            assert_eq!(result["isError"].as_bool(), Some(true));
            assert_eq!(
                result["structuredContent"]["error"]["code"].as_str(),
                Some("RPC_FAILURE")
            );
            assert_eq!(
                result["structuredContent"]["error"]["retryable"].as_bool(),
                Some(true)
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_tcp_uses_the_same_typed_service_store_and_retry_receipts_as_unix() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let (address, reservation) = configure_tcp_listener(&config);
            drop(reservation);
            let daemon = start(&config, &socket).await;
            let tcp = sparrow_client::Client::connect_tcp(address).await.unwrap();
            let unix = sparrow_client::Client::connect(&socket).await.unwrap();
            assert_eq!(tcp.discovery().store_id, unix.discovery().store_id);
            let receipt = tcp
                .ingest(
                    "tcp-retry",
                    "public-tcp",
                    wire::Format::Jsonl,
                    b"{\"message\":\"typed public TCP\"}\n",
                )
                .await
                .unwrap();
            assert_eq!(receipt.durability, wire::Durability::Durable);
            assert_eq!(
                unix.ingest(
                    "tcp-retry",
                    "public-tcp",
                    wire::Format::Jsonl,
                    b"{\"message\":\"typed public TCP\"}\n"
                )
                .await
                .unwrap(),
                receipt
            );
            assert_eq!(unix.artifact(&receipt.id).await.unwrap(), receipt);
            assert_eq!(
                rpc_ok(
                    tcp.capability()
                        .status_request()
                        .send()
                        .promise
                        .await
                        .unwrap()
                        .get()
                        .unwrap()
                        .get_result()
                        .unwrap()
                )
                .get_live_records(),
                1
            );
            drop(tcp);
            drop(unix);
            drop(daemon);
            std::fs::remove_file(&socket).unwrap();
            let daemon = start(&config, &socket).await;
            let tcp = sparrow_client::Client::connect_tcp(address).await.unwrap();
            assert_eq!(
                tcp.ingest(
                    "tcp-retry",
                    "public-tcp",
                    wire::Format::Jsonl,
                    b"{\"message\":\"typed public TCP\"}\n"
                )
                .await
                .unwrap(),
                receipt
            );
            drop(tcp);
            drop(daemon);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn configured_syslog_listeners_publish_exact_packets_and_report_transport_failures() {
    use tokio::io::AsyncWriteExt;
    tokio::task::LocalSet::new().run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let socket = data.join("rpc.sock");
        let config = write_server_config(dir.path(), &data, &socket);
        let mut text = std::fs::read_to_string(&config).unwrap();
        // Four independent packet publishers plus the status observer need five permits.
        assert_eq!(text.matches("max_concurrent_requests = 4").count(), 1);
        text = text.replace("max_concurrent_requests = 4", "max_concurrent_requests = 5");
        std::fs::write(&config, text).unwrap();
        let (rpc_address, reservation) = configure_tcp_listener(&config);
        drop(reservation);
        for (id, transport, framing) in [("octets", "tcp", Some("octet_counting")), ("datagrams", "udp", None), ("lf", "tcp", Some("lf")), ("crlf", "tcp", Some("cr_lf"))] {
            append_syslog_config(&config, id, "127.0.0.1:0", &format!("syslog-{id}"), transport, framing);
        }
        let daemon = start(&config, &socket).await;
        let client = sparrow_client::Client::connect(&socket).await.unwrap();
        let response = client.capability().status_request().send().promise.await.unwrap();
        let status = rpc_ok(response.get().unwrap().get_result().unwrap());
        let listeners = status.get_syslog_listeners().unwrap();
        assert_eq!(listeners.len(), 4);
        let tcp = sparrow_client::Client::connect_tcp(rpc_address).await.unwrap();
        let endpoint = format!("tcp://{rpc_address}");
        let store_id = client.discovery().store_id.clone();
        let first = cli(&endpoint, &["sources", &store_id, "--page-size", "1"]);
        let mut discovered = first["rows"].as_array().unwrap().clone();
        let snapshot = first["snapshot_id"].as_str().unwrap().to_owned();
        let mut cursor = first["next_cursor"].as_str().unwrap().to_owned();
        let mut mcp = Mcp::start(&socket);
        mcp.initialize(1).await;
        loop {
            let page = mcp.tool(2, "result_next", purrdf_lex::json::Object::new().with("store_id", store_id.clone()).with("cursor", cursor.clone()).into()).await;
            assert_eq!(page["isError"].as_bool(), Some(false));
            let page = &page["structuredContent"];
            discovered.extend(page["rows"].as_array().unwrap().iter().cloned());
            if page["next_cursor"].is_null() { break; }
            cursor = page["next_cursor"].as_str().unwrap().to_owned();
        }
        assert_eq!(discovered.len(), 4);
        assert!(cli(&socket, &["result-release", &store_id, &snapshot])["released"].as_bool().unwrap());
        for row in &discovered {
            let source = row["source_id"].as_str().unwrap();
            let ids = row["syslog_listener_ids"].as_array().unwrap();
            assert_eq!(ids.len(), 1);
            let id = ids[0].as_str().unwrap();
            assert_eq!(row["retained_records"].as_str(), Some("0"));
            let cli_listener = cli(&endpoint, &["syslog-listener", &store_id, source, id]);
            let mcp_listener = mcp.tool(3, "syslog_listener_get", purrdf_lex::json::Object::new().with("store_id", store_id.clone()).with("source", source).with("listener_id", id).into()).await;
            assert_eq!(mcp_listener["isError"].as_bool(), Some(false));
            assert_eq!(cli_listener, mcp_listener["structuredContent"]);
            assert_eq!(cli_listener["listener"]["accepted_messages"].as_str(), Some("0"));
            assert_eq!(cli_listener["listener"]["continuity_unknown"].as_bool(), Some(true));
            let mut request = tcp.capability().syslog_listener_get_request();
            let mut query = request.get().init_query();
            query.set_store_id(&store_id);query.set_source(source);query.set_id(id);query.set_schema_version(sparrow_client::SCHEMA_VERSION);
            let response = request.send().promise.await.unwrap();
            let native = rpc_ok(response.get().unwrap().get_result().unwrap());
            assert_eq!(native.get_id().unwrap().to_str().unwrap(), id);
            assert_eq!(native.get_address().unwrap().to_str().unwrap(), cli_listener["listener"]["address"].as_str().unwrap());
        }
        for (store, source, id, schema, code) in [
            (store_id.as_str(), "syslog-octets", "octets", sparrow_client::SCHEMA_VERSION - 1, "UNSUPPORTED"),
            ("store:other", "syslog-octets", "octets", sparrow_client::SCHEMA_VERSION, "STORE_MISMATCH"),
            (store_id.as_str(), "different-source", "octets", sparrow_client::SCHEMA_VERSION, "NOT_FOUND"),
            (store_id.as_str(), "syslog-octets", "missing", sparrow_client::SCHEMA_VERSION, "NOT_FOUND"),
            (store_id.as_str(), "syslog-octets", "", sparrow_client::SCHEMA_VERSION, "INVALID_ARGUMENT"),
        ] {
            let mut request = tcp.capability().syslog_listener_get_request();let mut query = request.get().init_query();
            query.set_store_id(store);query.set_source(source);query.set_id(id);query.set_schema_version(schema);
            let response = request.send().promise.await.unwrap();
            let wire::result::Error(error) = response.get().unwrap().get_result().unwrap().which().unwrap() else { panic!("invalid listener read accepted") };
            assert_eq!(error.unwrap().get_code().unwrap().to_str().unwrap(), code);
        }
        mcp.close(true).await;
        let addresses: Vec<std::net::SocketAddr> = listeners.iter().map(|listener| {
            assert_eq!(listener.get_accepted_messages(), 0);
            assert!(listener.get_continuity_unknown());
            assert_eq!(listener.get_kernel_drops_unknown(), listener.get_transport().unwrap() == wire::SyslogTransport::Udp);
            listener.get_address().unwrap().to_str().unwrap().parse().unwrap()
        }).collect();
        let modern = b"<34>1 2026-10-08T06:25:00.123Z host app 123 ID [id@32473 p=\"x\\]y\" p=\"\\q\"] needle\nnext";
        let mut octets = tokio::net::TcpStream::connect(addresses[0]).await.unwrap();
        for message in [modern.as_slice(), modern.as_slice(), b"<13>Oct  8 06:25:00 host daemon[123]: legacy needle", b"<999>bad needle"] {
            octets.write_all(format!("{} ", message.len()).as_bytes()).await.unwrap();
            for part in message.chunks(3) { octets.write_all(part).await.unwrap(); }
        }
        octets.write_all(b"9 short").await.unwrap(); octets.shutdown().await.unwrap();
        let udp = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp.send_to(modern, addresses[1]).await.unwrap();
        udp.send_to(&vec![b'x';481], addresses[1]).await.unwrap();
        let mut lf = tokio::net::TcpStream::connect(addresses[2]).await.unwrap();
        lf.write_all(b"<13>1 - host app - - - lf needle\r\n").await.unwrap(); lf.shutdown().await.unwrap();
        let mut crlf = tokio::net::TcpStream::connect(addresses[3]).await.unwrap();
        crlf.write_all(b"<13>1 - host app - - - crlf\nneedle\r\n").await.unwrap(); crlf.shutdown().await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let response = client.capability().status_request().send().promise.await.unwrap();
            let status = rpc_ok(response.get().unwrap().get_result().unwrap());
            let listeners = status.get_syslog_listeners().unwrap();
            if listeners.get(0).get_accepted_messages() == 4 && listeners.get(0).get_framing_failures() == 1
                && listeners.get(1).get_accepted_messages() == 1 && listeners.get(1).get_truncated_datagrams() == 1
                && listeners.get(2).get_accepted_messages() == 1 && listeners.get(3).get_accepted_messages() == 1 {
                assert_eq!(listeners.get(0).get_interpretation_failures(), 1);
                assert_eq!(status.get_live_records(), 7);
                break;
            }
            assert!(Instant::now() < deadline, "syslog publication did not reach expected counters");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let idle = tokio::net::TcpStream::connect(addresses[0]).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let response = client.capability().status_request().send().promise.await.unwrap();
            let listener = rpc_ok(response.get().unwrap().get_result().unwrap()).get_syslog_listeners().unwrap().get(0);
            if listener.get_active_connections() == 1 { break; }
            assert!(Instant::now() < deadline); tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let excess = tokio::net::TcpStream::connect(addresses[0]).await.unwrap();
        loop {
            let response = client.capability().status_request().send().promise.await.unwrap();
            let listener = rpc_ok(response.get().unwrap().get_result().unwrap()).get_syslog_listeners().unwrap().get(0);
            if listener.get_rejected_connections() == 1 && listener.get_timed_out_connections() == 1 && listener.get_active_connections() == 0 { break; }
            assert!(Instant::now() < deadline); tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let observed = cli(&endpoint, &["syslog-listener", &store_id, "syslog-octets", "octets"]);
        assert_eq!(observed["listener"]["accepted_messages"].as_str(), Some("4"));
        assert_eq!(observed["listener"]["interpretation_failures"].as_str(), Some("1"));
        assert_eq!(observed["listener"]["framing_failures"].as_str(), Some("1"));
        let receiver_sources = cli(&endpoint, &["sources", &store_id]);
        let mut receiver_ids = std::collections::BTreeMap::new();
        let mut receiver_mcp = Mcp::start(&socket);
        receiver_mcp.initialize(1).await;
        for source in receiver_sources["rows"].as_array().unwrap() {
            let ids = source["receiver_window_ids"].as_array().unwrap();
            assert_eq!(ids.len(), 1);
            let id = ids[0].as_str().unwrap();
            let history = cli(&endpoint, &["receiver-window", &store_id, id]);
            assert_eq!(history["source"], source["source_id"]);
            assert_eq!(history["continuity_unknown"].as_bool(), Some(true));
            assert_eq!(history["pre_commit_loss_unknown"].as_bool(), Some(true));
            let result = receiver_mcp.tool(2, "receiver_window_get", purrdf_lex::json::Object::new().with("store_id", store_id.clone()).with("window_id", id).into()).await;
            assert_eq!(result["isError"].as_bool(), Some(false));
            assert_eq!(history, result["structuredContent"]);
            if source["source_id"].as_str() == Some("syslog-octets") {
                assert_eq!(history["observations"].as_str(), Some("6"));
                assert_eq!(history["accepted"].as_str(), Some("4"));
                assert_eq!(history["framing_failures"].as_str(), Some("1"));
                assert_eq!(history["timed_out"].as_str(), Some("1"));
                assert_eq!(history["gaps"][0]["first"].as_str(), Some("5"));
            }
            if source["source_id"].as_str() == Some("syslog-datagrams") {
                assert_eq!(history["observations"].as_str(), Some("2"));
                assert_eq!(history["accepted"].as_str(), Some("1"));
                assert_eq!(history["truncated"].as_str(), Some("1"));
            }
            receiver_ids.insert(source["source_id"].as_str().unwrap().to_owned(), id.to_owned());
        }
        assert!(receiver_sources["next_cursor"].is_null(), "receiver fixture must fit one source page, whose lease retires automatically");
        let id = &receiver_ids["syslog-octets"];
        for (sid, schema, window, code) in [
            (store_id.as_str(), sparrow_client::SCHEMA_VERSION-1, id.as_str(), "UNSUPPORTED"),
            ("store:other", sparrow_client::SCHEMA_VERSION, id.as_str(), "STORE_MISMATCH"),
            (store_id.as_str(), sparrow_client::SCHEMA_VERSION, "syslog-receiver:absent", "NOT_FOUND"),
            (store_id.as_str(), sparrow_client::SCHEMA_VERSION, "producer-stream:wrong", "INVALID_ARGUMENT"),
        ] {
            let mut request = tcp.capability().receiver_window_get_request();
            let mut query = request.get().init_query(); query.set_store_id(sid); query.set_schema_version(schema); query.set_id(window);
            let response = request.send().promise.await.unwrap();
            let wire::result::Error(error) = response.get().unwrap().get_result().unwrap().which().unwrap() else { panic!("invalid receiver lookup accepted") };
            assert_eq!(error.unwrap().get_code().unwrap().to_str().unwrap(), code);
        }
        receiver_mcp.close(true).await;
        drop(idle); drop(excess); drop(tcp); drop(client); drop(daemon);
        let store = sparrow_server::ServerConfig::read(&config).unwrap().open_store().unwrap();
        let now = sparrow_server::now().unwrap();
        let rows: Vec<_> = store.records(now).unwrap().into_iter().filter(|row| row.source == "syslog-octets").collect();
        assert_eq!(rows.len(), 4);
        assert_eq!(store.receiver_window(&receiver_ids["syslog-octets"], now).unwrap().observations, 6);
        for record in rows {
            let position = record.receiver.as_ref().unwrap();
            assert_eq!(position.window_id, receiver_ids["syslog-octets"]);
            assert!((1..=4).contains(&position.sequence));
            let bytes = store.bytes(&record.artifact_id, record.byte_start, (record.byte_end-record.byte_start) as usize, now).unwrap();
            let snapshot = record.syslog.unwrap();
            if bytes == modern { assert_eq!(snapshot.structured_data[0].parameters.len(), 2); }
            else if bytes.starts_with(b"<999>") { assert!(snapshot.diagnostic.is_some()); }
            else { assert_eq!(bytes, b"<13>Oct  8 06:25:00 host daemon[123]: legacy needle"); }
        }
        for (source, expected) in [("syslog-lf", b"<13>1 - host app - - - lf needle\r".as_slice()), ("syslog-crlf", b"<13>1 - host app - - - crlf\nneedle")] {
            let rows: Vec<_> = store.records(now).unwrap().into_iter().filter(|row| row.source == source).collect(); assert_eq!(rows.len(), 1);
            assert_eq!(store.bytes(&rows[0].artifact_id, 0, expected.len(), now).unwrap(), expected);
        }
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn syslog_connection_failure_stops_siblings_and_reopens_exact_durable_packet() {
    use tokio::io::AsyncWriteExt;
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            for id in ["failing", "idle"] {
                append_syslog_config(&config, id, "127.0.0.1:0", id, "tcp", Some("lf"));
            }
            let mut config = sparrow_server::ServerConfig::read(&config).unwrap();
            // Outlast the failure deadline: ordinary frame timeout cannot stop the sibling.
            for listener in &mut config.syslog {
                listener.read_timeout_ms = 60000;
            }
            let store = config.open_store().unwrap();
            let operations = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
            let registry = sparrow_server::syslog_listener::SyslogRegistry::default();
            let worker = sparrow_server::syslog_listener::SyslogWorker::start(
                store.clone(),
                &config.syslog,
                operations.clone(),
                registry.clone(),
            )
            .await
            .unwrap();
            // Keep the typed observer available while deliberately failing intake admission.
            let observer: wire::sparrow::Client = capnp_rpc::new_client(
                sparrow_server::Rpc::new(
                    store.clone(),
                    std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
                    1024 * 1024,
                    100,
                )
                .with_syslog(registry),
            );
            let response = observer.status_request().send().promise.await.unwrap();
            let status = rpc_ok(response.get().unwrap().get_result().unwrap());
            let addresses: Vec<std::net::SocketAddr> = status
                .get_syslog_listeners()
                .unwrap()
                .iter()
                .map(|listener| {
                    listener
                        .get_address()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .parse()
                        .unwrap()
                })
                .collect();
            let mut writer = tokio::net::TcpStream::connect(addresses[0]).await.unwrap();
            let packet = b"<13>1 - host app - - - durable before failure";
            writer.write_all(packet).await.unwrap();
            writer.write_all(b"\n").await.unwrap();
            let mut idle = tokio::net::TcpStream::connect(addresses[1]).await.unwrap();
            idle.write_all(b"partial frame").await.unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let response = observer.status_request().send().promise.await.unwrap();
                let listeners = rpc_ok(response.get().unwrap().get_result().unwrap())
                    .get_syslog_listeners()
                    .unwrap();
                if listeners.get(0).get_accepted_messages() == 1
                    && listeners.get(1).get_active_connections() == 1
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "syslog fixture did not admit both peers"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            operations.close();
            writer
                .write_all(b"<13>1 - host app - - - refused\n")
                .await
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            while !worker.is_finished() {
                assert!(
                    Instant::now() < deadline,
                    "failed listener group did not stop"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let error = worker.shutdown().await.unwrap_err().to_string();
            assert!(
                error.contains("semaphore closed"),
                "primary failure lost: {error}"
            );
            let response = observer.status_request().send().promise.await.unwrap();
            let status = rpc_ok(response.get().unwrap().get_result().unwrap());
            assert_eq!(status.get_live_records(), 1);
            for listener in status.get_syslog_listeners().unwrap().iter() {
                assert_eq!(
                    listener.get_active_connections(),
                    0,
                    "sibling connection was aborted instead of joined"
                );
            }
            drop(response);
            drop(observer);
            drop(writer);
            drop(idle);
            store.flush().unwrap();
            drop(store);
            let reopened = config.open_store().unwrap();
            let now = sparrow_server::now().unwrap();
            let records = reopened.records(now).unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].source, "failing");
            assert_eq!(
                reopened
                    .bytes(&records[0].artifact_id, 0, packet.len(), now)
                    .unwrap(),
                packet
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn idle_rpc_connections_share_a_finite_transport_budget_independent_of_storage_work() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let original = std::fs::read_to_string(&config).unwrap();
            assert_eq!(
                sparrow_server::ServerConfig::read(&config)
                    .unwrap()
                    .max_rpc_connections,
                16
            );
            for invalid in [0, 257] {
                std::fs::write(
                    &config,
                    format!("{original}\nmax_rpc_connections = {invalid}\n"),
                )
                .unwrap();
                assert!(
                    sparrow_server::ServerConfig::read(&config)
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("max_rpc_connections must be 1..256")
                );
            }
            std::fs::write(
                &config,
                original.replace("max_concurrent_requests = 4", "max_concurrent_requests = 1")
                    + "\nmax_rpc_connections = 6\n",
            )
            .unwrap();
            let (address, reservation) = configure_tcp_listener(&config);
            drop(reservation);
            let daemon = start(&config, &socket).await;
            let mut idle = Vec::new();
            for index in 0..5 {
                let client = if index % 2 == 0 {
                    sparrow_client::Client::connect_tcp(address).await.unwrap()
                } else {
                    sparrow_client::Client::connect(&socket).await.unwrap()
                };
                let response = client
                    .capability()
                    .status_request()
                    .send()
                    .promise
                    .await
                    .unwrap();
                rpc_ok(response.get().unwrap().get_result().unwrap());
                idle.push(client);
            }
            // Five idle capabilities leave the single storage permit available to a sixth peer.
            let sixth = sparrow_client::Client::connect_tcp(address).await.unwrap();
            let response = sixth
                .capability()
                .status_request()
                .send()
                .promise
                .await
                .unwrap();
            rpc_ok(response.get().unwrap().get_result().unwrap());
            // Unix and TCP share six transport leases; neither can over-admit a seventh peer.
            assert!(sparrow_client::Client::connect(&socket).await.is_err());
            assert!(sparrow_client::Client::connect_tcp(address).await.is_err());
            drop(sixth);
            let deadline = Instant::now() + Duration::from_secs(3);
            let recovered = loop {
                match sparrow_client::Client::connect(&socket).await {
                    Ok(client) => break client,
                    Err(error) => {
                        assert!(
                            Instant::now() < deadline,
                            "transport lease not returned: {error}"
                        );
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            };
            let receipt = recovered
                .ingest(
                    "idle-budget",
                    "idle-test",
                    wire::Format::Log,
                    b"one storage operation\n",
                )
                .await
                .unwrap();
            assert_eq!(receipt.record_ids.len(), 1);
            drop(recovered);
            drop(idle);
            drop(daemon);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn artifact_membership_scopes_share_wire_validation_and_frozen_agent_pages() {
    use purrdf_lex::json::{Object, Value};
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let store = client.discovery().store_id.clone();
            let a = client
                .ingest(
                    "artifact-scope-wire-a",
                    "a",
                    wire::Format::Jsonl,
                    b"{}\n{}\n",
                )
                .await
                .unwrap();
            let b = client
                .ingest("artifact-scope-wire-b", "b", wire::Format::Jsonl, b"{}\n")
                .await
                .unwrap();
            let empty = client
                .ingest(
                    "artifact-scope-wire-empty",
                    "empty",
                    wire::Format::Jsonl,
                    b"",
                )
                .await
                .unwrap();
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            let pin_reply = mcp
                .tool(
                    2,
                    "pin",
                    Object::new()
                        .with("request_id", "artifact-scope-wire-pin")
                        .with("name", "scope")
                        .with("days", 2_u64)
                        .with("store_id", store.clone())
                        .with(
                            "targets",
                            Value::array([
                                a.record_ids[0].as_str(),
                                b.id.as_str(),
                                empty.id.as_str(),
                            ]),
                        )
                        .into(),
                )
                .await;
            assert_eq!(pin_reply["isError"].as_bool(), Some(false));
            let pin = &pin_reply["structuredContent"];
            let scope = Value::from(
                Object::new()
                    .with("kind", "pin")
                    .with("store_id", store.clone())
                    .with("pin_id", pin["pin_id"].clone())
                    .with("expected_revision", pin["revision"].clone()),
            );
            let scope_file = dir.path().join("scope.json");
            std::fs::write(&scope_file, purrdf_lex::json::write_compact(&scope)).unwrap();
            let path = scope_file.to_str().unwrap();
            let first = cli(
                &socket,
                &[
                    "artifacts",
                    &store,
                    "--scope-file",
                    path,
                    "--page-size",
                    "1",
                ],
            );
            assert_eq!(first["total_items"].as_str(), Some("3"));
            let released = mcp
                .tool(
                    3,
                    "pin_release",
                    Object::new()
                        .with("request_id", "artifact-scope-wire-release")
                        .with("store_id", store.clone())
                        .with("pin_id", pin["pin_id"].clone())
                        .into(),
                )
                .await;
            assert_eq!(released["isError"].as_bool(), Some(false));
            let mut cursor = first["next_cursor"].as_str().unwrap().to_owned();
            let mut rows = first["rows"].as_array().unwrap().to_vec();
            for id in 4..=5 {
                let page = cli(
                    &socket,
                    &[
                        "artifacts",
                        &store,
                        "--scope-file",
                        path,
                        "--page-size",
                        "1",
                        "--cursor",
                        &cursor,
                    ],
                );
                let response = mcp
                    .tool(
                        id,
                        "artifacts_list",
                        Object::new()
                            .with("store_id", store.clone())
                            .with("scope", scope.clone())
                            .with("page_size", 1_u64)
                            .with("cursor", cursor.clone())
                            .into(),
                    )
                    .await;
                assert_eq!(response["isError"].as_bool(), Some(false));
                assert_eq!(response["structuredContent"], page);
                assert_eq!(page["snapshot_id"], first["snapshot_id"]);
                rows.extend_from_slice(page["rows"].as_array().unwrap());
                if let Some(next) = page["next_cursor"].as_str() {
                    cursor = next.to_owned();
                } else {
                    assert_eq!(id, 5);
                }
            }
            for (artifact, count) in [(&a.id, "1"), (&b.id, "1"), (&empty.id, "0")] {
                let row = rows
                    .iter()
                    .find(|row| row["artifact_id"].as_str() == Some(artifact.as_str()))
                    .unwrap();
                assert_eq!(row["retained_records"].as_str(), Some(count));
                assert_eq!(row["complete_bytes_available"].as_bool(), Some(true));
            }
            let selected = Value::from(
                Object::new()
                    .with("kind", "selection")
                    .with("store_id", store.clone())
                    .with("artifact_ids", Value::array([a.id.as_str()]))
                    .with(
                        "record_ids",
                        Value::array([a.record_ids[0].as_str(), a.record_ids[0].as_str()]),
                    ),
            );
            let response = mcp
                .tool(
                    6,
                    "artifacts_list",
                    Object::new()
                        .with("store_id", store.clone())
                        .with("scope", selected.clone())
                        .into(),
                )
                .await;
            assert_eq!(response["isError"].as_bool(), Some(false));
            assert_eq!(
                response["structuredContent"]["total_items"].as_str(),
                Some("1")
            );
            assert_eq!(
                response["structuredContent"]["rows"][0]["retained_records"].as_str(),
                Some("2")
            );
            std::fs::write(&scope_file, purrdf_lex::json::write_compact(&selected)).unwrap();
            let changed = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                .arg(&socket)
                .args([
                    "artifacts",
                    &store,
                    "--scope-file",
                    path,
                    "--page-size",
                    "1",
                    "--cursor",
                    &cursor,
                ])
                .admitted_output()
                .unwrap();
            assert!(!changed.status.success() && changed.stdout.is_empty());
            assert!(String::from_utf8_lossy(&changed.stderr).contains("CONFLICT"));
            let missing = Value::from(
                Object::new()
                    .with("kind", "selection")
                    .with("store_id", store.clone())
                    .with(
                        "record_ids",
                        Value::array([a.record_ids[0].as_str(), "record:missing"]),
                    ),
            );
            let foreign = Value::from(
                Object::new()
                    .with("kind", "selection")
                    .with("store_id", "store:foreign")
                    .with("artifact_ids", Value::array([a.id.as_str()])),
            );
            let filtered = Value::from(Object::new().with("kind", "live").with(
                "filter",
                Object::new().with("service", Value::array(Vec::<Value>::new())),
            ));
            for (input, code) in [
                (scope, "NOT_FOUND"),
                (missing, "NOT_FOUND"),
                (foreign, "STORE_MISMATCH"),
                (filtered, "INVALID_ARGUMENT"),
            ] {
                std::fs::write(&scope_file, purrdf_lex::json::write_compact(&input)).unwrap();
                let output = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                    .arg(&socket)
                    .args([
                        "artifacts",
                        &store,
                        "--scope-file",
                        path,
                        "--source",
                        "excluded",
                    ])
                    .admitted_output()
                    .unwrap();
                assert!(!output.status.success() && output.stdout.is_empty());
                assert!(
                    String::from_utf8_lossy(&output.stderr).contains(code),
                    "{output:?}"
                );
            }
            let mut request = client.capability().artifacts_list_request();
            let mut query = request.get().init_query();
            query.set_store_id(&store);
            query.set_schema_version(25);
            let response = request.send().promise.await.unwrap();
            match response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                wire::result::Error(error) => assert_eq!(
                    error.unwrap().get_code().unwrap().to_str().unwrap(),
                    "UNSUPPORTED"
                ),
                wire::result::Ok(_) => panic!("obsolete artifact membership schema admitted"),
            }
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn source_listing_exposes_declared_idle_and_occurrence_sources_through_shared_agents() {
    use purrdf_lex::json::{Object, Value};
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let root = dir.path().join("finished");
            std::fs::create_dir(&root).unwrap();
            append_consume_config(&config, &root, 30_000, 16, 16);
            let owner = sparrow_server::ServerConfig::read(&config)
                .unwrap()
                .open_store()
                .unwrap();
            let declared = owner
                .define_time_profile(
                    "z-declared",
                    "/t",
                    sparrow_core::TimeRepresentation::UnixMillis,
                )
                .unwrap();
            let diagnostic = owner
                .define_diagnostic_profile(
                    "a-source",
                    [(sparrow_core::DiagnosticDimension::Service, "/svc".into())].into(),
                )
                .unwrap();
            drop(owner);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let store = client.discovery().store_id.clone();
            watch_until(client.capability(), |watch| {
                watch.get_coverage().unwrap() == wire::WatchCoverage::Covered
            })
            .await;
            for request in ["source-wire-a", "source-wire-b"] {
                client
                    .ingest(request, "a-source", wire::Format::Jsonl, b"{}\n{}\n")
                    .await
                    .unwrap();
            }
            let first = cli(&socket, &["sources", &store, "--page-size", "1"]);
            assert_eq!(first["total_items"].as_str(), Some("3"));
            let source = &first["rows"][0];
            assert_eq!(source["source_id"].as_str(), Some("a-source"));
            assert_eq!(source["complete_artifacts"].as_str(), Some("2"));
            assert_eq!(source["fragment_parents"].as_str(), Some("0"));
            assert_eq!(source["retained_records"].as_str(), Some("4"));
            assert_eq!(
                source["diagnostic_profile_id"].as_str(),
                Some(diagnostic.id.as_str())
            );
            assert_eq!(source["time_profile_id"], Value::Null);
            assert_eq!(source["continuity_unknown"].as_bool(), Some(true));
            assert!(
                source["rdf_iri"]
                    .as_str()
                    .unwrap()
                    .starts_with("urn:sparrow:source:")
            );
            let artifacts = cli(
                &socket,
                &[
                    "artifacts",
                    &store,
                    "--source",
                    source["source_id"].as_str().unwrap(),
                ],
            );
            assert_eq!(artifacts["total_items"].as_str(), Some("2"));
            client
                .ingest("source-wire-later", "b-later", wire::Format::Jsonl, b"{}\n")
                .await
                .unwrap();
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            let mut cursor = first["next_cursor"].as_str().unwrap().to_owned();
            let mut captured = Vec::new();
            for id in 2..=3 {
                let page = cli(
                    &socket,
                    &["sources", &store, "--page-size", "1", "--cursor", &cursor],
                );
                let response = mcp
                    .tool(
                        id,
                        "sources_list",
                        Object::new()
                            .with("store_id", store.clone())
                            .with("page_size", 1_u64)
                            .with("cursor", cursor.clone())
                            .into(),
                    )
                    .await;
                assert_eq!(response["isError"].as_bool(), Some(false));
                assert_eq!(response["structuredContent"], page);
                assert_eq!(page["snapshot_id"], first["snapshot_id"]);
                assert_eq!(page["total_items"].as_str(), Some("3"));
                captured.push(page["rows"][0].clone());
                if let Some(next) = page["next_cursor"].as_str() {
                    cursor = next.to_owned();
                } else {
                    assert_eq!(id, 3);
                }
            }
            assert_eq!(
                captured[0]["source_id"].as_str(),
                Some("consume-acceptance")
            );
            assert_eq!(captured[0]["retained_records"].as_str(), Some("0"));
            assert_eq!(
                captured[0]["watches"][0]["watch_id"].as_str(),
                Some("completed")
            );
            assert_eq!(captured[0]["watches"][0]["revision"].as_str(), Some("1"));
            assert_eq!(captured[0]["watches"][0]["enabled"].as_bool(), Some(true));
            assert_eq!(captured[1]["source_id"].as_str(), Some("z-declared"));
            assert_eq!(
                captured[1]["time_profile_id"].as_str(),
                Some(declared.id.as_str())
            );
            assert_eq!(captured[1]["complete_artifacts"].as_str(), Some("0"));
            let fresh = cli(&socket, &["sources", &store]);
            assert_eq!(fresh["total_items"].as_str(), Some("4"));
            let filtered = mcp
                .tool(
                    4,
                    "sources_list",
                    Object::new()
                        .with("store_id", store.clone())
                        .with("source", "a-source")
                        .into(),
                )
                .await;
            assert_eq!(
                filtered["structuredContent"]["total_items"].as_str(),
                Some("1")
            );
            assert_eq!(filtered["structuredContent"]["rows"][0], *source);
            for (args, code) in [
                (vec!["sources", "foreign"], "STORE_MISMATCH"),
                (
                    vec!["sources", &store, "--scan-budget", "1"],
                    "RESOURCE_LIMIT",
                ),
                (
                    vec![
                        "sources",
                        &store,
                        "--source",
                        "changed",
                        "--page-size",
                        "1",
                        "--cursor",
                        &cursor,
                    ],
                    "CONFLICT",
                ),
                (
                    vec!["sources", &store, "--cursor", "bogus"],
                    "INVALID_ARGUMENT",
                ),
            ] {
                let output = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                    .arg(&socket)
                    .args(args)
                    .admitted_output()
                    .unwrap();
                assert!(!output.status.success() && output.stdout.is_empty());
                assert!(
                    String::from_utf8_lossy(&output.stderr).contains(code),
                    "{output:?}"
                );
            }
            let mut request = client.capability().sources_list_request();
            let mut query = request.get().init_query();
            query.set_store_id(&store);
            query.set_schema_version(24);
            let response = request.send().promise.await.unwrap();
            match response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                wire::result::Error(error) => assert_eq!(
                    error.unwrap().get_code().unwrap().to_str().unwrap(),
                    "UNSUPPORTED"
                ),
                wire::result::Ok(_) => panic!("obsolete source-list schema admitted"),
            }
            let status = cli(&socket, &["status"]);
            assert_eq!(status["source_read_handles"].as_str(), Some("1"));
            assert_eq!(status["max_source_read_handles"].as_str(), Some("8"));
            assert_eq!(status["max_source_read_bytes"].as_str(), Some("8388608"));
            assert_eq!(status["source_read_lease_ms"].as_str(), Some("60000"));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn artifact_listing_shares_typed_pages_unknown_origins_and_agent_refusals() {
    use purrdf_lex::json::{Object, Value};
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let configuration = sparrow_server::ServerConfig::read(&config).unwrap();
            let owner = configuration.open_store().unwrap();
            let clock = sparrow_server::now().unwrap();
            let old = clock - 31 * 86400000;
            let fragment = owner
                .ingest("wire-fragment", "wire-fragments", "jsonl", b"{}\n{}\n", old)
                .unwrap();
            owner
                .pin(
                    "wire-fragment-pin",
                    "wire-fragment",
                    &fragment.record_ids[..1],
                    40,
                    old,
                )
                .unwrap();
            owner.expire(clock).unwrap();
            drop(owner);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let store = client.discovery().store_id.clone();
            client
                .ingest(
                    "wire-list-a",
                    "wire-catalog",
                    wire::Format::Jsonl,
                    b"{}\n{}\n",
                )
                .await
                .unwrap();
            client
                .ingest(
                    "wire-list-b",
                    "wire-catalog",
                    wire::Format::Jsonl,
                    b"{}\n{}\n",
                )
                .await
                .unwrap();
            let first = cli(
                &socket,
                &[
                    "artifacts",
                    &store,
                    "--source",
                    "wire-catalog",
                    "--page-size",
                    "1",
                ],
            );
            assert_eq!(first["total_items"].as_str(), Some("2"));
            assert_eq!(first["rows"][0]["retained_records"].as_str(), Some("2"));
            assert_eq!(
                first["rows"][0]["complete_bytes_available"].as_bool(),
                Some(true)
            );
            let cursor = first["next_cursor"].as_str().unwrap();
            client
                .ingest("wire-list-c", "wire-catalog", wire::Format::Jsonl, b"{}\n")
                .await
                .unwrap();
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            let second = mcp
                .tool(
                    2,
                    "artifacts_list",
                    Object::new()
                        .with("store_id", store.clone())
                        .with("source", "wire-catalog")
                        .with("page_size", 1_u64)
                        .with("cursor", cursor)
                        .into(),
                )
                .await;
            assert!(!second["isError"].as_bool().unwrap());
            let again = cli(
                &socket,
                &[
                    "artifacts",
                    &store,
                    "--source",
                    "wire-catalog",
                    "--page-size",
                    "1",
                    "--cursor",
                    cursor,
                ],
            );
            assert_eq!(second["structuredContent"], again);
            assert_eq!(again["snapshot_id"], first["snapshot_id"]);
            assert_eq!(again["total_items"].as_str(), Some("2"));
            assert_eq!(again["has_more"].as_bool(), Some(false));
            assert_ne!(
                again["rows"][0]["artifact_id"],
                first["rows"][0]["artifact_id"]
            );
            let fragments = cli(
                &socket,
                &["artifacts", &store, "--source", "wire-fragments"],
            );
            assert_eq!(
                fragments["rows"][0]["artifact_id"].as_str(),
                Some(fragment.id.as_str())
            );
            assert_eq!(fragments["rows"][0]["origin"], Value::Null);
            assert_eq!(
                fragments["rows"][0]["complete_bytes_available"].as_bool(),
                Some(false)
            );
            assert_eq!(fragments["rows"][0]["retained_records"].as_str(), Some("1"));
            for (args, code) in [
                (
                    vec!["artifacts", &store, "--scan-budget", "1"],
                    "RESOURCE_LIMIT",
                ),
                (vec!["artifacts", "store:foreign"], "STORE_MISMATCH"),
                (
                    vec![
                        "artifacts",
                        &store,
                        "--source",
                        "changed",
                        "--page-size",
                        "1",
                        "--cursor",
                        cursor,
                    ],
                    "CONFLICT",
                ),
                (
                    vec![
                        "artifacts",
                        &store,
                        "--source",
                        "wire-fragments",
                        "--received-from",
                        "1970-01-01T00:00:00Z",
                    ],
                    "INDEX_NOT_READY",
                ),
            ] {
                let mut args = args;
                if code == "INDEX_NOT_READY" {
                    args.extend(["--received-before", "2099-01-01T00:00:00Z"]);
                }
                let output = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                    .arg(&socket)
                    .args(args)
                    .admitted_output()
                    .unwrap();
                assert!(!output.status.success() && output.stdout.is_empty());
                assert!(
                    String::from_utf8_lossy(&output.stderr).contains(code),
                    "{:?}",
                    output
                );
            }
            for version in [0, 23] {
                let mut request = client.capability().artifacts_list_request();
                let mut query = request.get().init_query();
                query.set_store_id(&store);
                query.set_schema_version(version);
                let response = request.send().promise.await.unwrap();
                match response
                    .get()
                    .unwrap()
                    .get_result()
                    .unwrap()
                    .which()
                    .unwrap()
                {
                    wire::result::Error(error) => assert_eq!(
                        error.unwrap().get_code().unwrap().to_str().unwrap(),
                        "UNSUPPORTED"
                    ),
                    wire::result::Ok(_) => panic!("obsolete artifact-list schema admitted"),
                }
            }
            let status = cli(&socket, &["status"]);
            assert_eq!(status["max_artifact_read_handles"].as_str(), Some("8"));
            assert_eq!(status["artifact_read_handles"].as_str(), Some("1"));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn bounded_discovery_does_not_read_corrupt_evidence_or_disguise_status_failure() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let configuration = sparrow_server::ServerConfig::read(&config).unwrap();
            let owner = configuration.open_store().unwrap();
            let accepted = owner
                .ingest(
                    "discovery-corruption",
                    "producer",
                    "json",
                    br#"{"message":"needle"}"#,
                    sparrow_server::now().unwrap(),
                )
                .unwrap();
            let properties = owner.properties().unwrap();
            drop(owner);
            let db = sparrow_test_support::catalog_db(&data).unwrap();
            db.put(&accepted.record_ids[0], b"{invalid").unwrap();
            drop(db);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let discovery = sparrow_client::Discovery::fetch(client.capability())
                .await
                .unwrap();
            assert_eq!(discovery, client.discovery().clone());
            assert_eq!(
                Some(discovery.store_id.as_str()),
                properties["store_id"].as_str()
            );
            let response = client
                .capability()
                .status_request()
                .send()
                .promise
                .await
                .unwrap();
            match response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                wire::result::Error(error) => assert_eq!(
                    error.unwrap().get_code().unwrap().to_str().unwrap(),
                    "STORAGE_FAILURE"
                ),
                wire::result::Ok(_) => panic!("status concealed a corrupt evidence row"),
            }
            let output = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                .arg(&socket)
                .args(["keys"])
                .admitted_output()
                .unwrap();
            assert!(!output.status.success() && output.stdout.is_empty());
            assert!(String::from_utf8_lossy(&output.stderr).contains("STORAGE_FAILURE"));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn diagnostic_mappings_and_filters_share_typed_cli_and_mcp_evidence() {
    use purrdf_lex::json::{Object, Value};
    tokio::task::LocalSet::new().run_until(async {
        let dir=tempfile::tempdir().unwrap();let data=dir.path().join("store");let socket=data.join("rpc.sock");
        let config=write_server_config(dir.path(),&data,&socket);let daemon=start(&config,&socket).await;
        let client=sparrow_client::Client::connect(&socket).await.unwrap();let store=client.discovery().store_id.clone();
        let file=dir.path().join("mappings.json");std::fs::write(&file,br#"{"service":"/svc","severity":"/level"}"#).unwrap();
        let definition=cli(&socket,&["diagnostic-define","producer",file.to_str().unwrap()]);
        let mut mcp=Mcp::start(&socket);mcp.initialize(1).await;
        let retry=mcp.tool(2,"diagnostic_define",Object::new().with("source","producer")
            .with("mappings",Object::new().with("service","/svc").with("severity","/level")).into()).await;
        assert_eq!(retry["structuredContent"],definition);
        let artifact=client.ingest("diag-wire","producer",wire::Format::Jsonl,
            b"{\"svc\":\"worker\",\"level\":\"ERROR\",\"message\":\"needle\"}\n{\"svc\":\"worker\",\"level\":\"WARN\",\"message\":\"needle\"}\n{\"svc\":\"other\",\"level\":\"ERROR\",\"message\":\"needle\"}\n").await.unwrap();
        let anchor=artifact.record_ids[..2].iter().min().unwrap();
        let pinned=mcp.tool(3,"pin",Object::new().with("store_id",store.clone()).with("request_id","diag-pin")
            .with("name","diagnostics").with("targets",vec![artifact.id.clone()]).with("days",2_u64).into()).await;
        let pin=pinned["structuredContent"]["pin_id"].as_str().unwrap();
        let filter=Object::new().with("source_ids",vec!["producer"]).with("service",vec!["worker","worker"])
            .with("severity",vec!["ERROR","WARN"]);
        let scope_file=dir.path().join("scope.json");let mut request_id=4;
        for scope in [Object::new().with("kind","live"),Object::new().with("kind","selection").with("store_id",store.clone()).with("artifact_ids",vec![artifact.id.clone()]),
            Object::new().with("kind","pin").with("store_id",store.clone()).with("pin_id",pin)] {
            let scope:Value=scope.with("filter",filter.clone()).into();std::fs::write(&scope_file,purrdf_lex::json::write_compact(&scope)).unwrap();
            for (tool,command) in [("keys_list","keys"),("values_search","values"),("text_search","text"),("records_search","records"),("records_context","context")] {
                let mut input=Object::new().with("scope",scope.clone());let mut args=vec![command];
                if tool=="text_search" {input=input.with("text","needle");args.push("needle");}
                if tool=="values_search" {input=input.with("path","/message");args.extend(["--path","/message"]);}
                if tool=="records_context" {input=input.with("store_id",store.clone()).with("id",anchor.clone());args.extend([store.as_str(),anchor.as_str()]);}
                args.extend(["--scope-file",scope_file.to_str().unwrap()]);let cli_result=cli(&socket,&args);
                let result=mcp.tool(request_id,tool,input.into()).await;request_id+=1;
                assert_eq!(result["isError"].as_bool(),Some(false),"{tool}: {result:?}");let found=&result["structuredContent"];
                if tool=="records_context" {assert_eq!(found["anchor"],cli_result["anchor"]);assert_eq!(found["after"]["rows"].as_array().unwrap().len(),1);}
                else {assert_eq!(found["rows"],cli_result["rows"]);if tool!="keys_list" {assert_eq!(found["rows"].as_array().unwrap().len(),2);}}
            }
        }
        let rdf=cli(&socket,&["rdf",&store,&artifact.record_ids[0]]);assert_eq!(rdf["diagnostic_primary_rows"].as_str(),Some("16"));
        for version in [0,21] {
            let mut rejected=client.capability().keys_list_request();
            let mut scope=rejected.get().init_scope();scope.set_schema_version(version);
            scope.init_received_before().set_unix_millis(u64::MAX);
            let reply=rejected.send().promise.await.unwrap();
            match reply.get().unwrap().get_result().unwrap().which().unwrap() {
                wire::result::Error(error)=>assert_eq!(error.unwrap().get_code().unwrap().to_str().unwrap(),"UNSUPPORTED"),
                wire::result::Ok(_)=>panic!("obsolete query schema was accepted"),
            }
        }
        let mut call=client.capability().record_get_request();let mut input=call.get().init_record();input.set_store_id(&store);input.set_id(&artifact.record_ids[0]);
        let reply=call.send().promise.await.unwrap();let evidence=rpc_ok(reply.get().unwrap().get_result().unwrap()).get_diagnostics().unwrap();
        assert_eq!(evidence.get_fields().unwrap().len(),2);
        match evidence.get_profile_id().unwrap().which().unwrap() {
            wire::optional_text::Value(value) => assert_eq!(value.unwrap().to_str().unwrap(),definition["profile_id"].as_str().unwrap()),
            wire::optional_text::Absent(()) => panic!("captured diagnostic mapping disappeared"),
        }
        let conflict=mcp.tool(request_id,"diagnostic_define",Object::new().with("source","producer").with("mappings",Object::new().with("service","/other")).into()).await;request_id+=1;
        assert_eq!(conflict["structuredContent"]["error"]["code"].as_str(),Some("CONFLICT"));
        client.ingest("diag-unprofiled","unprofiled",wire::Format::Json,br#"{"svc":"worker"}"#).await.unwrap();
        let uncertain=mcp.tool(request_id,"records_search",Object::new().with("scope",Object::new().with("kind","live")
            .with("filter",Object::new().with("service",vec!["worker"]))).into()).await;request_id+=1;
        assert_eq!(uncertain["structuredContent"]["receipt"]["coverage"]["diagnostic_scope"]["unknown"].as_str(),Some("1"));
        assert!(uncertain["structuredContent"]["receipt"]["coverage"]["eligible_records"].is_null());
        client.ingest("diag-missing","producer",wire::Format::Json,br#"{"svc":"worker","message":"needle"}"#).await.unwrap();
        let partial_scope:Value=Object::new().with("kind","live").with("filter",Object::new().with("service",vec!["worker"])
            .with("severity",vec!["ERROR","WARN"])).into();
        std::fs::write(&scope_file,purrdf_lex::json::write_compact(&partial_scope)).unwrap();
        for (tool,command) in [("keys_list","keys"),("values_search","values"),("text_search","text"),("records_search","records"),("records_context","context")] {
            let mut input=Object::new().with("scope",partial_scope.clone());let mut args=vec![command];
            if tool=="text_search" {input=input.with("text","needle");args.push("needle");}
            if tool=="values_search" {input=input.with("path","/message");args.extend(["--path","/message"]);}
            if tool=="records_context" {input=input.with("store_id",store.clone()).with("id",anchor.clone());args.extend([store.as_str(),anchor.as_str()]);}
            args.extend(["--scope-file",scope_file.to_str().unwrap()]);
            let cli_result=cli(&socket,&args);
            let partial=mcp.tool(request_id,tool,input.clone().into()).await;request_id+=1;
            assert_eq!(partial["isError"].as_bool(),Some(false));
            assert_eq!(partial["structuredContent"]["receipt"]["coverage"],cli_result["receipt"]["coverage"]);
            let coverage=&cli_result["receipt"]["coverage"];
            assert!(coverage["eligible_records"].is_null());
            assert_eq!(coverage["diagnostic_scope"]["unknown"].as_str(),Some(if tool=="records_context" {"1"} else {"2"}));
            assert_eq!(cli_result["receipt"]["certainty"].as_str(),Some("incomplete"));
            let strict=mcp.tool(request_id,tool,input.with("require_complete_index",true).into()).await;request_id+=1;
            assert_eq!(strict["structuredContent"]["error"]["code"].as_str(),Some("INDEX_NOT_READY"));
            args.extend(["--require-complete-index","true"]);
            let refused=Command::new(env!("CARGO_BIN_EXE_sparrow")).arg(&socket).args(&args).admitted_output().unwrap();
            assert!(!refused.status.success() && refused.stdout.is_empty());
        }
        for filter in [Object::new(),Object::new().with("service",Vec::<String>::new()),Object::new().with("severity",vec![""]),Object::new().with("guess_field",vec!["worker"])] {
            let invalid=mcp.tool(request_id,"keys_list",Object::new().with("scope",Object::new().with("kind","live").with("filter",filter)).into()).await;request_id+=1;
            assert_eq!(invalid["structuredContent"]["error"]["code"].as_str(),Some("INVALID_ARGUMENT"));
        }
        mcp.close(true).await;drop(client);drop(daemon);
        std::fs::remove_file(&socket).unwrap();
        let _daemon=start(&config,&socket).await;
        assert_eq!(cli(&socket,&["diagnostic-get","producer"])["profile"],definition);
        assert_eq!(cli(&socket,&["rdf",&store,&artifact.record_ids[0]])["diagnostic_primary_rows"].as_str(),Some("16"));
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn source_arrays_are_shared_by_generated_cli_and_mcp_queries() {
    use purrdf_lex::json::{Object, Value};
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let store = &client.discovery().store_id;
            let mut artifacts = Vec::new();
            for source in ["alpha", "beta", "gamma"] {
                artifacts.push(
                    client
                        .ingest(
                            source,
                            source,
                            wire::Format::Json,
                            format!(r#"{{"message":"needle","{source}":true}}"#).as_bytes(),
                        )
                        .await
                        .unwrap(),
                );
            }
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            let created = mcp
                .tool(
                    2,
                    "pin",
                    Object::new()
                        .with("store_id", store.clone())
                        .with("request_id", "source-array-pin")
                        .with("name", "case")
                        .with(
                            "targets",
                            Value::array(artifacts.iter().map(|a| a.id.clone())),
                        )
                        .with("days", 2_u64)
                        .into(),
                )
                .await;
            assert_eq!(created["isError"].as_bool(), Some(false));
            let pin = created["structuredContent"]["pin_id"].as_str().unwrap();
            let filter = Object::new().with("source_ids", vec!["alpha", "beta", "alpha"]);
            let path = dir.path().join("scope.json");
            let mut request_id = 3;
            for scope in [
                Object::new().with("kind", "live"),
                Object::new()
                    .with("kind", "selection")
                    .with("store_id", store.clone())
                    .with(
                        "artifact_ids",
                        Value::array(artifacts.iter().map(|a| a.id.clone())),
                    ),
                Object::new()
                    .with("kind", "pin")
                    .with("store_id", store.clone())
                    .with("pin_id", pin),
            ] {
                let scope: Value = scope.with("filter", filter.clone()).into();
                std::fs::write(&path, purrdf_lex::json::write_compact(&scope)).unwrap();
                for (tool, command) in [
                    ("keys_list", "keys"),
                    ("values_search", "values"),
                    ("records_search", "records"),
                    ("text_search", "text"),
                    ("records_context", "context"),
                ] {
                    let mut input = Object::new().with("scope", scope.clone());
                    let mut args = vec![command];
                    if tool == "text_search" {
                        input = input.with("text", "needle");
                        args.push("needle");
                    }
                    if tool == "values_search" {
                        input = input.with("path", "/message");
                        args.extend(["--path", "/message"]);
                    }
                    if tool == "records_context" {
                        input = input
                            .with("store_id", store.clone())
                            .with("id", artifacts[0].record_ids[0].clone());
                        args.extend([store.as_str(), artifacts[0].record_ids[0].as_str()]);
                    }
                    args.extend(["--scope-file", path.to_str().unwrap()]);
                    let cli_result = cli(&socket, &args);
                    let result = mcp.tool(request_id, tool, input.into()).await;
                    request_id += 1;
                    assert_eq!(
                        result["isError"].as_bool(),
                        Some(false),
                        "{tool}: {result:?}"
                    );
                    let found = &result["structuredContent"];
                    if tool == "records_context" {
                        assert_eq!(found["anchor"], cli_result["anchor"]);
                        assert!(found["after"]["rows"].as_array().unwrap().is_empty());
                    } else {
                        assert_eq!(found["rows"], cli_result["rows"]);
                        if tool == "keys_list" {
                            assert!(
                                found["rows"]
                                    .as_array()
                                    .unwrap()
                                    .iter()
                                    .all(|r| r["path"].as_str() != Some("/gamma"))
                            );
                        } else {
                            assert_eq!(found["rows"].as_array().unwrap().len(), 2);
                        }
                    }
                }
            }
            // Direct generated requests have the same OR semantics and typed refusal.
            for names in [vec!["alpha", "beta"], vec![], vec![""]] {
                let mut call = client.capability().keys_list_request();
                let mut scope = call.get().init_scope();
                scope.set_schema_version(sparrow_client::SCHEMA_VERSION);
                scope
                    .reborrow()
                    .init_received_before()
                    .set_unix_millis(u64::MAX);
                let mut ids = scope.init_source_ids().init_value(names.len() as u32);
                for (index, name) in names.iter().enumerate() {
                    ids.set(index as u32, name);
                }
                let response = call.send().promise.await.unwrap();
                match response
                    .get()
                    .unwrap()
                    .get_result()
                    .unwrap()
                    .which()
                    .unwrap()
                {
                    wire::result::Ok(value) => {
                        assert!(!names.is_empty());
                        assert!(matches!(
                            value
                                .unwrap()
                                .get_receipt()
                                .unwrap()
                                .get_coverage()
                                .unwrap()
                                .get_eligible_records()
                                .unwrap()
                                .which()
                                .unwrap(),
                            wire::count::Known(2)
                        ));
                    }
                    wire::result::Error(error) => {
                        assert!(names.is_empty() || names == [""]);
                        assert_eq!(
                            error.unwrap().get_code().unwrap().to_str().unwrap(),
                            "INVALID_ARGUMENT"
                        );
                    }
                }
            }
            for ids in [
                Value::array(Vec::<String>::new()),
                Value::array([""]),
                Value::array(["é".repeat(257)]),
                Value::array(vec!["alpha"; 129]),
                Value::Null,
            ] {
                let result = mcp
                    .tool(
                        request_id,
                        "records_search",
                        Object::new()
                            .with(
                                "scope",
                                Object::new()
                                    .with("kind", "live")
                                    .with("filter", Object::new().with("source_ids", ids)),
                            )
                            .into(),
                    )
                    .await;
                request_id += 1;
                assert_eq!(
                    result["structuredContent"]["error"]["code"].as_str(),
                    Some("INVALID_ARGUMENT")
                );
            }
            let excluded = mcp
                .tool(
                    request_id,
                    "records_context",
                    Object::new()
                        .with("store_id", store.clone())
                        .with("id", artifacts[2].record_ids[0].clone())
                        .with(
                            "scope",
                            Object::new().with("kind", "live").with("filter", filter),
                        )
                        .into(),
                )
                .await;
            assert_eq!(
                excluded["structuredContent"]["error"]["code"].as_str(),
                Some("INVALID_ARGUMENT")
            );
            mcp.close(true).await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_expiry_resumes_multiple_passes_and_keeps_pinned_evidence() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let settings = sparrow_server::ServerConfig::read(&config).unwrap();
            let store = settings.open_store().unwrap();
            let store_id = store.store_id().unwrap();
            let artifact = store
                .ingest(
                    "daemon-expiry-old",
                    "source",
                    "log",
                    &b"expired occurrence\n".repeat(70),
                    0,
                )
                .unwrap();
            // Protection was established while the original evidence was live.
            store
                .pin(
                    "daemon-expiry-original-pin",
                    "case",
                    &artifact.record_ids[..1],
                    30000,
                    0,
                )
                .unwrap();
            drop(store);
            let daemon = start(&config, &socket).await;
            let connection = connect(&socket).await.unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let mut retained_catalogs = 0;
                for id in &artifact.record_ids[1..] {
                    let mut request = connection.client.record_get_request();
                    let mut reference = request.get().init_record();
                    reference.set_store_id(&store_id);
                    reference.set_id(id);
                    let response = request.send().promise.await.unwrap();
                    match response
                        .get()
                        .unwrap()
                        .get_result()
                        .unwrap()
                        .which()
                        .unwrap()
                    {
                        wire::result::Ok(_) => retained_catalogs += 1,
                        wire::result::Error(error) => assert_eq!(
                            error.unwrap().get_code().unwrap().to_str().unwrap(),
                            "NOT_FOUND"
                        ),
                    }
                }
                if retained_catalogs == 0 {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "daemon did not finish bounded expiry passes"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let mut request = connection.client.bytes_read_request();
            let mut read = request.get().init_request();
            read.set_length(19);
            let mut evidence = read.init_evidence();
            evidence.set_store_id(&store_id);
            evidence.set_record(&artifact.record_ids[0]);
            let response = request.send().promise.await.unwrap();
            assert_eq!(
                rpc_ok(response.get().unwrap().get_result().unwrap())
                    .get_data()
                    .unwrap(),
                b"expired occurrence\n"
            );
            drop(connection);
            drop(daemon);
            let store = settings.open_store().unwrap();
            let now = sparrow_server::now().unwrap();
            assert_eq!(store.records(now).unwrap().len(), 1);
            assert_eq!(
                store.status(now).unwrap()["unique_payload_chunks"].as_str(),
                Some("1")
            );
            for id in &artifact.record_ids[1..] {
                assert_eq!(store.record(id).unwrap_err().code, "NOT_FOUND");
            }
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_pressure_worker_preserves_pins_and_shares_typed_cli_mcp_status() {
    tokio::task::LocalSet::new().run_until(async {
        use purrdf_lex::json::{Object, Value};
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let socket = data.join("rpc.sock");
        let config = write_server_config(dir.path(), &data, &socket);
        let text = std::fs::read_to_string(&config).unwrap()
            .replace("disk_budget = 134217728", "disk_budget = 16777216")
            .replace("maintenance_reserve = 8388608", "maintenance_reserve = 4194304");
        std::fs::write(&config, text).unwrap();
        let daemon = start(&config, &socket).await;
        let producer = sparrow_client::Client::connect(&socket).await.unwrap();
        let empty = producer.ingest("pressure-empty-old", "source", wire::Format::Log, b"").await.unwrap();
        let old = producer.ingest("pressure-old", "source", wire::Format::Log, b"old\n").await.unwrap();
        let protected = producer.ingest("pressure-pinned", "source", wire::Format::Log, b"pinned\n").await.unwrap();
        let mut mcp = Mcp::start(&socket);
        mcp.initialize(1).await;
        let pin = mcp.tool(2, "pin", Object::new().with("store_id", &protected.store_id)
            .with("request_id", "pressure-pin").with("name", "case")
            .with("targets", Value::array([protected.id.as_str()])).with("days", 40_u64).into()).await;
        assert_eq!(pin["isError"].as_bool(), Some(false));
        let pin_id = pin["structuredContent"]["pin_id"].as_str().unwrap().to_owned();
        let ballast = data.join("test-owned-pressure-ballast");
        std::fs::write(&ballast, vec![0_u8; 13 * 1024 * 1024]).unwrap();
        let error = producer.ingest("pressure-retry", "source", wire::Format::Log, b"later\n").await.unwrap_err();
        assert!(matches!(error, sparrow_client::Error::Domain { ref code, retryable: true, .. } if code == "STORAGE_PRESSURE"));
        let connection = connect(&socket).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let response = connection.client.status_request().send().promise.await.unwrap();
            let status = rpc_ok(response.get().unwrap().get_result().unwrap());
            let pressure = status.get_pressure_status().unwrap();
            assert_eq!(status.get_schema_version(), sparrow_client::SCHEMA_VERSION);
            if pressure.get_evicted_records() == 1 {
                assert_eq!(pressure.get_evicted_recordless_artifacts(), 1);
                assert!(pressure.get_requested_bytes() > 0);
                assert!(matches!(pressure.get_failure().unwrap().which().unwrap(), wire::optional_text::Absent(())));
                break;
            }
            assert!(Instant::now() < deadline, "owned daemon did not run pressure retirement");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let cli_status = cli(&socket, &["status"]);
        let mcp_status = mcp.tool(3, "status", Object::new().into()).await;
        assert_eq!(cli_status["pressure"]["evicted_records"].as_str(), Some("1"));
        assert_eq!(cli_status["pressure"]["evicted_recordless_artifacts"].as_str(), Some("1"));
        assert_eq!(mcp_status["structuredContent"]["pressure"]["evicted_recordless_artifacts"], cli_status["pressure"]["evicted_recordless_artifacts"]);
        assert_eq!(mcp_status["structuredContent"]["pressure"]["evicted_records"], cli_status["pressure"]["evicted_records"]);
        let retained = mcp.tool(4, "bytes_read", Object::new().with("store_id", &protected.store_id)
            .with("id", &protected.id).with("offset", "0").with("length", 7_u64).into()).await;
        assert_eq!(retained["isError"].as_bool(), Some(false));
        let unavailable = mcp.tool(5, "bytes_read", Object::new().with("store_id", &old.store_id)
            .with("id", &old.id).with("offset", "0").with("length", 1_u64).into()).await;
        assert_eq!(unavailable["structuredContent"]["error"]["code"].as_str(), Some("EVIDENCE_UNAVAILABLE"));
        let released = mcp.tool(6, "pin_release", Object::new().with("store_id", &protected.store_id)
            .with("request_id", "pressure-release").with("pin_id", &pin_id).into()).await;
        assert_eq!(released["isError"].as_bool(), Some(false));
        std::fs::remove_file(ballast).unwrap();
        let accepted = producer.ingest("pressure-retry", "source", wire::Format::Log, b"later\n").await.unwrap();
        assert_eq!(producer.ingest("pressure-empty-old", "source", wire::Format::Log, b"").await.unwrap(), empty);
        assert_eq!(producer.ingest("pressure-retry", "source", wire::Format::Log, b"later\n").await.unwrap(), accepted);
        mcp.close(true).await;
        drop(connection);
        drop(producer);
        drop(daemon);
    }).await;
}

fn watch_definition(
    root: &Path,
    id: &str,
    quiet_ms: u32,
    directories: usize,
    candidates: usize,
) -> sparrow_server::ConsumeWatch {
    sparrow_server::ConsumeWatch {
        id: id.into(),
        root: root.into(),
        producer_contract: sparrow_server::ProducerContract::FinishedFiles,
        source: "consume-acceptance".into(),
        format: "jsonl".into(),
        quiet_ms,
        max_directories: directories,
        max_candidates: candidates,
    }
}

fn append_consume_config(
    config: &Path,
    root: &Path,
    quiet_ms: u32,
    directories: usize,
    candidates: usize,
) {
    append_watch_config(
        config,
        &watch_definition(root, "completed", quiet_ms, directories, candidates),
    );
}

fn append_tail_config(config: &Path, root: &Path, format: &str) {
    let mut definition = watch_definition(root, "completed", 0, 16, 16);
    definition.producer_contract = sparrow_server::ProducerContract::ActiveFilesRetain;
    definition.format = format.into();
    append_watch_config(config, &definition);
}

fn append_watch_config(config: &Path, definition: &sparrow_server::ConsumeWatch) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(config)
        .unwrap();
    writeln!(
        file,
        "\n[[consume]]\n{}",
        toml::to_string(definition).unwrap()
    )
    .unwrap();
}

async fn watch_until(
    client: &wire::sparrow::Client,
    condition: impl Fn(wire::consume_watch_status::Reader<'_>) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = client.status_request().send().promise.await.unwrap();
        let status = rpc_ok(response.get().unwrap().get_result().unwrap());
        let watches = status.get_consume_watches().unwrap();
        assert_eq!(watches.len(), 1);
        if condition(watches.get(0)) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "watch status never met its acceptance condition: {:?}",
            watches.get(0)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[test]
fn consumption_configuration_requires_explicit_contract_and_disjoint_bounded_roots() {
    use sparrow_server::ServerConfig;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("completed");
    std::fs::create_dir(&root).unwrap();
    let store = dir.path().join("store");
    let config = write_server_config(dir.path(), &store, &store.join("rpc.sock"));
    append_consume_config(&config, &root, 30_000, 1024, 4096);
    let valid = std::fs::read_to_string(&config).unwrap();
    assert_eq!(
        ServerConfig::read(&config).unwrap().consume[0].quiet_ms,
        30_000
    );
    for altered in [
        valid.replace("producer_contract = \"finished_files\"\n", ""),
        valid.replace("finished_files", "arbitrary_open_files"),
        valid.replace("max_candidates = 4096", "max_candidates = 0"),
        valid.replace("quiet_ms = 30000", "quiet_ms = 600001"),
        valid.replace("max_concurrent_requests = 4", "max_concurrent_requests = 1"),
    ] {
        std::fs::write(&config, altered).unwrap();
        assert!(ServerConfig::read(&config).is_err());
    }
    let child = root.join("nested");
    std::fs::create_dir(&child).unwrap();
    std::fs::write(&config, &valid).unwrap();
    append_consume_config(&config, &child, 30_000, 1024, 4096);
    let overlap = std::fs::read_to_string(&config).unwrap().replacen(
        "id = \"completed\"",
        "id = \"other\"",
        1,
    );
    std::fs::write(&config, overlap).unwrap();
    assert!(
        ServerConfig::read(&config)
            .err()
            .unwrap()
            .to_string()
            .contains("overlap")
    );
    assert!(!root.join(".sparrow-inflight").exists());
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_handoff_consumes_exact_generations_and_replays_after_removal_and_restart() {
    tokio::task::LocalSet::new().run_until(async {
        use sparrow_client::{CompletedHandoff, CompletedFileIdentity, HandoffReceipt};
        use std::os::unix::ffi::OsStrExt;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("completed"); std::fs::create_dir(&root).unwrap();
        let data = dir.path().join("store"); let socket = data.join("rpc.sock");
        let config = write_server_config(dir.path(), &data, &socket);
        append_consume_config(&config, &root, 600_000, 16, 16);
        let daemon = start(&config, &socket).await;
        let client = sparrow_client::Client::connect(&socket).await.unwrap();
        let input = root.join("finished.jsonl"); let original = b"{\"message\":\"explicit handoff\"}\r\n";
        std::fs::write(&input, original).unwrap();
        let intent_file = dir.path().join("handoff.capnp");
        let prepared = cli(&socket, &["watch", "handoff-prepare", "explicit-1", "completed", "1", "finished.jsonl", intent_file.to_str().unwrap()]);
        assert_eq!(prepared["phase"].as_str(), Some("prepared"));
        assert!(input.exists());
        let intent = CompletedHandoff::read(std::fs::File::open(&intent_file).unwrap()).unwrap();
        let saved_bytes = std::fs::read(&intent_file).unwrap();
        let duplicate_prepare = Command::new(env!("CARGO_BIN_EXE_sparrow")).arg(&socket)
            .args(["watch", "handoff-prepare", "explicit-1", "completed", "1", "finished.jsonl", intent_file.to_str().unwrap()]).admitted_output().unwrap();
        assert!(!duplicate_prepare.status.success());
        assert_eq!(std::fs::read(&intent_file).unwrap(), saved_bytes);
        let unsafe_intent = root.join("must-not-consume.capnp");
        let unsafe_prepare = Command::new(env!("CARGO_BIN_EXE_sparrow")).arg(&socket)
            .args(["watch", "handoff-prepare", "unsafe-intent", "completed", "1", "finished.jsonl", unsafe_intent.to_str().unwrap()]).admitted_output().unwrap();
        assert!(!unsafe_prepare.status.success()); assert!(!unsafe_intent.exists());
        assert!(matches!(client.handoff_get(&intent).await, Err(sparrow_client::Error::Domain { code, .. }) if code == "NOT_FOUND"));
        for variation in 0..4 {
            let mut invalid = intent.clone(); invalid.request_id = format!("invalid-{variation}");
            match variation {
                0 => invalid.watch_revision += 1,
                1 => invalid.generation.inode += 1,
                2 => invalid.relative_path = b"../outside".to_vec(),
                _ => invalid.watch = "unregistered".into(),
            }
            assert!(matches!(client.handoff(&invalid).await, Err(sparrow_client::Error::Domain { code, .. }) if matches!(code.as_str(), "CONFLICT" | "INVALID_ARGUMENT" | "NOT_FOUND")));
            assert!(matches!(client.handoff_get(&invalid).await, Err(sparrow_client::Error::Domain { code, .. }) if matches!(code.as_str(), "INVALID_ARGUMENT" | "NOT_FOUND")));
        }
        let mut foreign = client.capability().watch_handoff_request();
        intent.encode(foreign.get().init_request()).unwrap();
        foreign.get().get_request().unwrap().set_store_id("store:foreign");
        let response = foreign.send().promise.await.unwrap();
        assert!(matches!(response.get().unwrap().get_result().unwrap().which().unwrap(), wire::result::Error(error) if error.as_ref().unwrap().get_code().unwrap().to_str().unwrap() == "STORE_MISMATCH"));
        let admitted = cli(&socket, &["watch", "handoff", intent_file.to_str().unwrap()]);
        assert_eq!(admitted["phase"].as_str(), Some("reserved"));
        assert_eq!(admitted["durability"].as_str(), Some("durable"));
        // Explicit producer handoff bypasses the configured ten-minute quiet interval.
        watch_until(client.capability(), |w| w.get_completed_claims() == 1 && w.get_pending_claims() == 0).await;
        let completed = client.handoff_get(&intent).await.unwrap();
        assert_eq!(completed.phase, wire::HandoffPhase::Removed);
        assert_eq!(completed.import_id, admitted["import_id"].as_str().unwrap());
        assert!(!input.exists());
        let artifact = completed.artifact_id.as_ref().unwrap();
        let ingestion = agent_ingestion_status(&socket, client.capability(), &intent.store_id, &sparrow_client::IngestSelector::Handoff(intent.request_id.clone())).await;
        assert_eq!(ingestion["deletion"].as_str(), Some("removed"));
        assert_eq!(ingestion["accepted"]["artifact_id"].as_str(), Some(artifact.as_str()));
        assert_eq!(ingestion["json"]["complete"].as_str(), Some("1"));
        assert_eq!(ingestion["rdf"]["complete"].as_str(), Some("1"));
        assert_eq!(ingestion["text"]["complete"].as_str(), Some("1"));
        assert!(!ingestion["removed_at"].is_null());
        let export = dir.path().join("original-export.jsonl");
        cli(&socket, &["bytes", &intent.store_id, artifact, "0", &original.len().to_string(), export.to_str().unwrap()]);
        assert_eq!(std::fs::read(export).unwrap(), original);
        std::fs::write(&input, b"replacement must remain\n").unwrap();
        assert_eq!(client.handoff(&intent).await.unwrap(), completed);
        let mut changed = intent.clone(); changed.generation = CompletedFileIdentity::capture(&input.metadata().unwrap());
        assert!(matches!(client.handoff(&changed).await, Err(sparrow_client::Error::Domain { code, .. }) if code == "CONFLICT"));
        // The SDK carries non-UTF8 filenames without a JSON envelope or lossy conversion.
        let raw_path = b"raw-\xff.jsonl";
        let raw_file = root.join(std::ffi::OsStr::from_bytes(raw_path));
        std::fs::write(&raw_file, original).unwrap();
        let raw_intent = CompletedHandoff { request_id: "explicit-raw".into(), relative_path: raw_path.to_vec(), generation: CompletedFileIdentity::capture(&raw_file.metadata().unwrap()), ..intent.clone() };
        client.handoff(&raw_intent).await.unwrap();
        watch_until(client.capability(), |w| w.get_completed_claims() == 2 && w.get_pending_claims() == 0).await;
        assert!(!raw_file.exists());
        let raw_completed: HandoffReceipt = client.handoff_get(&raw_intent).await.unwrap();
        assert_ne!(raw_completed.artifact_id, completed.artifact_id);
        cli(&socket, &["watch", "remove", "remove-after-handoff", "completed", "1"]);
        let mut after_remove = intent.clone(); after_remove.request_id = "after-removal".into();
        assert!(matches!(client.handoff(&after_remove).await, Err(sparrow_client::Error::Domain { code, .. }) if matches!(code.as_str(), "CONFLICT" | "NOT_FOUND")));
        assert_eq!(client.handoff(&intent).await.unwrap(), completed);
        assert!(cli(&socket, &["watch", "list"])["watches"].as_array().unwrap().is_empty());
        drop(client); drop(daemon); std::fs::remove_file(&socket).unwrap();
        let daemon = start(&config, &socket).await;
        let client = sparrow_client::Client::connect(&socket).await.unwrap();
        assert_eq!(client.handoff(&intent).await.unwrap(), completed);
        assert_eq!(client.handoff(&raw_intent).await.unwrap(), raw_completed);
        assert_eq!(cli(&socket, &["watch", "handoff-status", intent_file.to_str().unwrap()])["phase"].as_str(), Some("removed"));
        assert_eq!(std::fs::read(&input).unwrap(), b"replacement must remain\n");
        let response = client.capability().status_request().send().promise.await.unwrap();
        let status = rpc_ok(response.get().unwrap().get_result().unwrap());
        assert_eq!(status.get_store_format().unwrap().to_str().unwrap(), "sparrow.store/58");
        assert_eq!(status.get_live_records(), 2);
        assert!(status.get_consume_watches().unwrap().is_empty());
        drop(client); drop(daemon);
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn typed_watch_administration_is_durable_revisioned_and_does_not_resurrect_removed_authority()
{
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("completed");
            std::fs::create_dir(&root).unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let definition_file = dir.path().join("definition.toml");
            let mut definition = watch_definition(&root, "completed", 30_000, 16, 16);
            std::fs::write(&definition_file, toml::to_string(&definition).unwrap()).unwrap();
            let daemon = start(&config, &socket).await;
            assert!(
                cli(&socket, &["watch", "list"])["watches"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            let created = cli(
                &socket,
                &[
                    "watch",
                    "put",
                    "watch-create",
                    "0",
                    definition_file.to_str().unwrap(),
                ],
            );
            assert_eq!(created["durability"].as_str(), Some("durable"));
            assert_eq!(created["watch"]["revision"].as_str(), Some("1"));
            assert_eq!(created["watch"]["enabled"].as_bool(), Some(true));
            assert_eq!(
                cli(
                    &socket,
                    &[
                        "watch",
                        "put",
                        "watch-create",
                        "0",
                        definition_file.to_str().unwrap()
                    ]
                ),
                created
            );
            definition.quiet_ms = 300;
            std::fs::write(&definition_file, toml::to_string(&definition).unwrap()).unwrap();
            let updated = cli(
                &socket,
                &[
                    "watch",
                    "put",
                    "watch-update",
                    "1",
                    definition_file.to_str().unwrap(),
                ],
            );
            assert_eq!(updated["watch"]["revision"].as_str(), Some("2"));
            for (request, expected) in [("watch-create", "0"), ("stale-update", "1")] {
                let failed = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                    .arg(&socket)
                    .args([
                        "watch",
                        "put",
                        request,
                        expected,
                        definition_file.to_str().unwrap(),
                    ])
                    .admitted_output()
                    .unwrap();
                assert!(!failed.status.success());
                assert!(String::from_utf8_lossy(&failed.stderr).contains("CONFLICT"));
            }
            let producer = sparrow_client::Client::connect(&socket).await.unwrap();
            // A missing explicit source-retirement contract never authorizes a watch.
            let mut call = producer.capability().watch_put_request();
            let mut input = call.get().init_request();
            input.set_store_id(&producer.discovery().store_id);
            input.set_request_id("no-contract");
            let mut watch = input.init_definition();
            watch.set_id("unacknowledged");
            watch.set_root(root.to_str().unwrap());
            watch.set_source("source");
            let response = call.send().promise.await.unwrap();
            match response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                wire::result::Error(error) => assert_eq!(
                    error.unwrap().get_code().unwrap().to_str().unwrap(),
                    "INVALID_ARGUMENT"
                ),
                wire::result::Ok(_) => panic!("unacknowledged producer contract admitted"),
            }
            // Store scoping precedes any registry mutation or filesystem action.
            let mut call = producer.capability().watch_remove_request();
            let mut input = call.get().init_request();
            input.set_store_id("store:foreign");
            input.set_request_id("foreign-remove");
            input.set_id("completed");
            input.set_expected_revision(2);
            let response = call.send().promise.await.unwrap();
            match response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                wire::result::Error(error) => assert_eq!(
                    error.unwrap().get_code().unwrap().to_str().unwrap(),
                    "STORE_MISMATCH"
                ),
                wire::result::Ok(_) => panic!("foreign watch mutation admitted"),
            }
            // A newly configured watch reaches the actual canonical ingestion caller.
            std::fs::write(
                root.join("accepted.jsonl"),
                b"{\"message\":\"administrative intake\"}\r\n",
            )
            .unwrap();
            watch_until(producer.capability(), |watch| {
                watch.get_completed_claims() == 1
            })
            .await;
            let result = cli(
                &socket,
                &[
                    "values",
                    "--source",
                    "consume-acceptance",
                    "--path",
                    "/message",
                    "--decoded",
                    "administrative intake",
                ],
            );
            assert_eq!(result["rows"].as_array().unwrap().len(), 1);
            assert!(!root.join("accepted.jsonl").exists());
            let removed = cli(
                &socket,
                &["watch", "remove", "watch-remove", "completed", "2"],
            );
            assert_eq!(removed["watch"]["revision"].as_str(), Some("3"));
            assert_eq!(removed["watch"]["enabled"].as_bool(), Some(false));
            assert!(
                cli(&socket, &["watch", "list"])["watches"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            std::fs::write(
                root.join("preserved.jsonl"),
                b"preserved after administrative removal",
            )
            .unwrap();
            drop(producer);
            drop(daemon);
            std::fs::remove_file(&socket).unwrap();
            // Stale startup configuration is bootstrap-only once the catalog exists.
            append_consume_config(&config, &root, 0, 16, 16);
            let _daemon = start(&config, &socket).await;
            assert!(
                cli(&socket, &["watch", "list"])["watches"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                cli(
                    &socket,
                    &["watch", "remove", "watch-remove", "completed", "2"]
                ),
                removed
            );
            definition.quiet_ms = 30_000;
            std::fs::write(&definition_file, toml::to_string(&definition).unwrap()).unwrap();
            assert_eq!(
                cli(
                    &socket,
                    &[
                        "watch",
                        "put",
                        "watch-create",
                        "0",
                        definition_file.to_str().unwrap()
                    ]
                ),
                created
            );
            assert!(
                cli(&socket, &["watch", "list"])["watches"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            tokio::time::sleep(Duration::from_millis(600)).await;
            assert_eq!(
                std::fs::read(root.join("preserved.jsonl")).unwrap(),
                b"preserved after administrative removal"
            );
            assert_eq!(
                cli(&socket, &["status"])["store_format"].as_str(),
                Some("sparrow.store/57")
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn removed_watch_keeps_durable_claim_recovery_and_refuses_root_rebinding() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use sparrow_server::{ServerConfig, consumption::ClaimRoot};
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("completed");
            let other = dir.path().join("other");
            std::fs::create_dir(&root).unwrap();
            std::fs::create_dir(&other).unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            append_consume_config(&config, &root, 30_000, 16, 16);
            let id = {
                let store = ServerConfig::read(&config).unwrap().open_store().unwrap();
                let owner = ClaimRoot::open(store, "completed", &root).unwrap();
                let path = root.join("unfinished.jsonl");
                std::fs::write(&path, b"{\"message\":\"original claim\"}\n").unwrap();
                let identity =
                    sparrow_client::CompletedFileIdentity::capture(&path.metadata().unwrap());
                let journal = owner
                    .reserve(
                        Path::new("unfinished.jsonl"),
                        identity,
                        "consume-acceptance",
                        "jsonl",
                    )
                    .unwrap();
                owner
                    .recover(&journal.import_id, sparrow_server::now().unwrap())
                    .unwrap();
                journal.import_id
            };
            let private = root.join(".sparrow-inflight").join(&id);
            std::fs::write(&private, b"late writer must not be retired").unwrap();
            let daemon = start(&config, &socket).await;
            let producer = sparrow_client::Client::connect(&socket).await.unwrap();
            watch_until(producer.capability(), |watch| {
                watch.get_pending_claims() == 1
                    && matches!(
                        watch.get_failure().unwrap().which().unwrap(),
                        wire::optional_text::Value(_)
                    )
            })
            .await;
            let removed = cli(
                &socket,
                &["watch", "remove", "drain-watch", "completed", "1"],
            );
            assert_eq!(removed["watch"]["enabled"].as_bool(), Some(false));
            watch_until(producer.capability(), |watch| {
                !watch.get_enabled()
                    && watch.get_pending_claims() == 1
                    && watch.get_candidates() == 0
                    && watch.get_directories() == 0
            })
            .await;
            let definition = dir.path().join("rebind.toml");
            std::fs::write(
                &definition,
                toml::to_string(&watch_definition(&other, "completed", 0, 16, 16)).unwrap(),
            )
            .unwrap();
            let failed = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                .arg(&socket)
                .args([
                    "watch",
                    "put",
                    "rebind-pending",
                    "2",
                    definition.to_str().unwrap(),
                ])
                .admitted_output()
                .unwrap();
            assert!(!failed.status.success());
            assert!(String::from_utf8_lossy(&failed.stderr).contains("CONFLICT"));
            assert!(!other.join(".sparrow-inflight").exists());
            std::fs::write(
                root.join("new-input.jsonl"),
                b"new input remains outside the removed watch",
            )
            .unwrap();
            drop(producer);
            drop(daemon);
            std::fs::remove_file(&socket).unwrap();
            let _daemon = start(&config, &socket).await;
            let producer = sparrow_client::Client::connect(&socket).await.unwrap();
            watch_until(producer.capability(), |watch| {
                !watch.get_enabled()
                    && watch.get_pending_claims() == 1
                    && matches!(
                        watch.get_failure().unwrap().which().unwrap(),
                        wire::optional_text::Value(_)
                    )
            })
            .await;
            assert_eq!(
                cli(
                    &socket,
                    &["watch", "remove", "drain-watch", "completed", "1"]
                ),
                removed
            );
            assert_eq!(
                std::fs::read(&private).unwrap(),
                b"late writer must not be retired"
            );
            assert_eq!(
                std::fs::read(root.join("new-input.jsonl")).unwrap(),
                b"new input remains outside the removed watch"
            );
            assert_eq!(
                cli(&socket, &["status"])["live_records"].as_str(),
                Some("0")
            );
        })
        .await;
}

#[test]
fn corrupt_durable_watch_catalog_is_a_hard_startup_failure() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("store");
    let socket = data.join("rpc.sock");
    let config = write_server_config(dir.path(), &data, &socket);
    let store = sparrow_server::ServerConfig::read(&config)
        .unwrap()
        .open_store()
        .unwrap();
    store
        .watch_registry_replace(None, b"invalid durable authority")
        .unwrap();
    drop(store);
    let result = Command::new(env!("CARGO_BIN_EXE_sparrowd"))
        .arg("--config")
        .arg(&config)
        .admitted_output()
        .unwrap();
    assert!(!result.status.success());
    assert!(!socket.exists());
    assert!(String::from_utf8_lossy(&result.stderr).contains("invalid durable watch catalog"));
}

#[tokio::test(flavor = "current_thread")]
async fn watch_receipt_pressure_preserves_reserved_removal_capacity() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("completed");
            std::fs::create_dir(&root).unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            append_consume_config(&config, &root, 30_000, 16, 16);
            let _daemon = start(&config, &socket).await;
            let producer = sparrow_client::Client::connect(&socket).await.unwrap();
            let response = producer
                .capability()
                .watch_list_request()
                .send()
                .promise
                .await
                .unwrap();
            let list = rpc_ok(response.get().unwrap().get_result().unwrap());
            let definition = list.get_watches().unwrap().get(0).get_definition().unwrap();
            let mut revision = 1;
            for index in 0..120 {
                let mut call = producer.capability().watch_put_request();
                let mut input = call.get().init_request();
                input.set_store_id(&producer.discovery().store_id);
                input.set_request_id(format!("pressure-update-{index}"));
                input.set_expected_revision(revision);
                input.set_definition(definition).unwrap();
                let response = call.send().promise.await.unwrap();
                revision = rpc_ok(response.get().unwrap().get_result().unwrap())
                    .get_watch()
                    .unwrap()
                    .get_revision();
            }
            let mut call = producer.capability().watch_put_request();
            let mut input = call.get().init_request();
            input.set_store_id(&producer.discovery().store_id);
            input.set_request_id("pressure-over-limit");
            input.set_expected_revision(revision);
            input.set_definition(definition).unwrap();
            let response = call.send().promise.await.unwrap();
            match response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                wire::result::Error(error) => assert_eq!(
                    error.unwrap().get_code().unwrap().to_str().unwrap(),
                    "RESOURCE_LIMIT"
                ),
                wire::result::Ok(_) => panic!("put consumed reserved watch removal capacity"),
            }
            let mut call = producer.capability().watch_remove_request();
            let mut input = call.get().init_request();
            input.set_store_id(&producer.discovery().store_id);
            input.set_request_id("pressure-remove");
            input.set_id("completed");
            input.set_expected_revision(revision);
            let response = call.send().promise.await.unwrap();
            assert!(
                !rpc_ok(response.get().unwrap().get_result().unwrap())
                    .get_watch()
                    .unwrap()
                    .get_enabled()
            );
            assert!(
                cli(&socket, &["watch", "list"])["watches"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            std::fs::write(
                root.join("preserved.jsonl"),
                b"source after pressure removal",
            )
            .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert_eq!(
                std::fs::read(root.join("preserved.jsonl")).unwrap(),
                b"source after pressure removal"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn configured_inotify_consumption_preserves_exact_bytes_and_defers_reopened_inputs() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use std::{io::Write, os::unix::fs::symlink};
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("completed");
            std::fs::create_dir_all(root.join("nested")).unwrap();
            let store = dir.path().join("store");
            let socket = store.join("rpc.sock");
            let config = write_server_config(dir.path(), &store, &socket);
            append_consume_config(&config, &root, 600, 16, 1);
            let inputs = [
                (
                    "a.jsonl",
                    "startup-a",
                    b"{\"message\":\"startup-a\"}\r\n".as_slice(),
                ),
                (
                    "b.jsonl",
                    "startup-b",
                    b"{\"message\":\"startup-b\"}\n".as_slice(),
                ),
                (
                    "nested/c.jsonl",
                    "startup-c",
                    b"{\"message\":\"startup-c\"}".as_slice(),
                ),
            ];
            for (name, _, bytes) in inputs {
                std::fs::write(root.join(name), bytes).unwrap();
            }
            let outside = dir.path().join("outside.jsonl");
            std::fs::write(&outside, b"outside evidence").unwrap();
            symlink(&outside, root.join("symlink.jsonl")).unwrap();
            rustix::fs::mknodat(
                rustix::fs::CWD,
                root.join("fifo"),
                rustix::fs::FileType::Fifo,
                rustix::fs::Mode::from_raw_mode(0o600),
                0,
            )
            .unwrap();
            let mut daemon = start(&config, &socket).await;
            let producer = sparrow_client::Client::connect(&socket).await.unwrap();
            let client = producer.capability();
            watch_until(client, |watch| watch.get_deferred()).await;
            watch_until(client, |watch| {
                watch.get_completed_claims() == 3 && watch.get_pending_claims() == 0
            })
            .await;
            for (name, message, expected) in inputs {
                assert!(!root.join(name).exists());
                let mut request = client.values_search_request();
                let mut query = request.get().init_query();
                query.reborrow().init_path().set_value("/message");
                query.reborrow().init_decoded().set_value(message);
                let mut scope = query.init_scope();
                scope.set_schema_version(sparrow_client::SCHEMA_VERSION);
                scope
                    .reborrow()
                    .init_source()
                    .set_value("consume-acceptance");
                scope.init_received_before().set_unix_millis(u64::MAX);
                let response = request.send().promise.await.unwrap();
                let values = rpc_ok(response.get().unwrap().get_result().unwrap());
                assert_eq!(values.get_rows().unwrap().len(), 1);
                let row = values.get_rows().unwrap().get(0);
                let mut request = client.bytes_read_request();
                let mut read = request.get().init_request();
                read.set_length(expected.len() as u32);
                let mut evidence = read.init_evidence();
                evidence.set_store_id(&producer.discovery().store_id);
                evidence.set_artifact(row.get_artifact_id().unwrap());
                let response = request.send().promise.await.unwrap();
                assert_eq!(
                    rpc_ok(response.get().unwrap().get_result().unwrap())
                        .get_data()
                        .unwrap(),
                    expected
                );
            }
            assert_eq!(std::fs::read(&outside).unwrap(), b"outside evidence");
            assert!(
                root.join("symlink.jsonl")
                    .symlink_metadata()
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );
            assert!(root.join("fifo").exists());
            // A new recursive directory forces a fresh event-coverage epoch.
            std::fs::create_dir(root.join("new")).unwrap();
            let changing = root.join("new/changing.jsonl");
            std::fs::write(&changing, b"{\"message\":\"reopened\"}").unwrap();
            watch_until(client, |watch| {
                watch.get_coverage().unwrap() == wire::WatchCoverage::Covered
                    && watch.get_directories() == 3
                    && watch.get_candidates() == 1
            })
            .await;
            // Repeated observed writable reopen/append activity exceeds the quiet
            // interval overall, but each individual stable interval remains too short.
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let mut file = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&changing)
                    .unwrap();
                file.write_all(b" ").unwrap();
                drop(file);
                assert!(changing.exists());
            }
            watch_until(client, |watch| watch.get_completed_claims() == 4).await;
            assert!(!changing.exists());
            // A later generation at a consumed path is its own input occurrence.
            std::fs::write(root.join("a.jsonl"), inputs[0].2).unwrap();
            watch_until(client, |watch| watch.get_completed_claims() == 5).await;
            let status = cli(&socket, &["status"]);
            assert_eq!(
                status["consume_watches"][0]["completed_claims"].as_str(),
                Some("5")
            );
            assert_eq!(status["live_records"].as_str(), Some("5"));
            assert_eq!(status["store_format"].as_str(), Some("sparrow.store/57"));
            rustix::process::kill_process(
                rustix::process::Pid::from_raw(daemon.0.id() as i32).unwrap(),
                rustix::process::Signal::INT,
            )
            .unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            while daemon.0.try_wait().unwrap().is_none() {
                assert!(
                    Instant::now() < deadline,
                    "consumer graceful shutdown stalled"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(!socket.exists());
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn configured_consumer_recovers_confirmed_journal_and_reports_late_write_without_retirement()
{
    tokio::task::LocalSet::new()
        .run_until(async {
            use sparrow_server::{
                ServerConfig,
                consumption::{ClaimRoot, Recovery},
                watches::{ConsumeWorker, WatchRegistry},
            };
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("completed");
            std::fs::create_dir(&root).unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            append_consume_config(&config, &root, 10_000, 16, 4);
            let (confirmed, blocked) = {
                let store = ServerConfig::read(&config).unwrap().open_store().unwrap();
                let owner = ClaimRoot::open(store.clone(), "completed", &root).unwrap();
                let mut ids = Vec::new();
                for name in ["confirmed.jsonl", "blocked.jsonl"] {
                    let path = root.join(name);
                    std::fs::write(&path, format!("{{\"message\":\"{name}\"}}\n")).unwrap();
                    let identity =
                        sparrow_client::CompletedFileIdentity::capture(&path.metadata().unwrap());
                    let journal = owner
                        .reserve(Path::new(name), identity, "consume-acceptance", "jsonl")
                        .unwrap();
                    assert!(matches!(
                        owner
                            .recover(&journal.import_id, sparrow_server::now().unwrap())
                            .unwrap(),
                        Recovery::Claimed(_)
                    ));
                    ids.push(journal.import_id);
                }
                assert!(
                    ConsumeWorker::start(
                        store,
                        &[],
                        socket.clone(),
                        WatchRegistry::default(),
                        std::sync::Arc::new(tokio::sync::Semaphore::new(4))
                    )
                    .is_err()
                );
                (ids.remove(0), ids.remove(0))
            };
            std::fs::write(
                root.join("confirmed.jsonl"),
                b"replacement source remains independent",
            )
            .unwrap();
            let private = root.join(".sparrow-inflight");
            std::fs::write(private.join(&blocked), b"unexpected late writer mutation").unwrap();
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            watch_until(client.capability(), |watch| {
                watch.get_completed_claims() == 1
                    && watch.get_pending_claims() == 1
                    && matches!(
                        watch.get_failure().unwrap().which().unwrap(),
                        wire::optional_text::Value(_)
                    )
            })
            .await;
            assert!(!private.join(&confirmed).exists());
            assert_eq!(
                std::fs::read(private.join(&blocked)).unwrap(),
                b"unexpected late writer mutation"
            );
            assert_eq!(
                std::fs::read(root.join("confirmed.jsonl")).unwrap(),
                b"replacement source remains independent"
            );
            let status = cli(&socket, &["status"]);
            assert_eq!(status["live_records"].as_str(), Some("1"));
            assert!(
                status["consume_watches"][0]["failure"]
                    .as_str()
                    .unwrap()
                    .contains("changed")
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn consume_coverage_limit_and_root_replacement_fail_without_source_retirement() {
    for replace_root in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("completed");
        std::fs::create_dir_all(root.join("nested")).unwrap();
        std::fs::write(root.join("nested/evidence.jsonl"), b"preserved evidence").unwrap();
        let data = dir.path().join("store");
        let socket = data.join("rpc.sock");
        let config = write_server_config(dir.path(), &data, &socket);
        append_consume_config(
            &config,
            &root,
            10_000,
            if replace_root { 16 } else { 1 },
            16,
        );
        let mut daemon = Daemon(
            Command::new(env!("CARGO_BIN_EXE_sparrowd"))
                .arg("--config")
                .arg(&config)
                .stdout(Stdio::null())
                .admitted_spawn()
                .unwrap(),
        );
        let preserved = if replace_root {
            let deadline = Instant::now() + Duration::from_secs(30);
            while !socket.exists() {
                assert!(daemon.0.try_wait().unwrap().is_none());
                assert!(Instant::now() < deadline);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let old = dir.path().join("old-root");
            std::fs::rename(&root, &old).unwrap();
            std::fs::create_dir(&root).unwrap();
            std::fs::write(root.join("replacement.jsonl"), b"replacement evidence").unwrap();
            old
        } else {
            root.clone()
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        let status = loop {
            if let Some(status) = daemon.0.try_wait().unwrap() {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "lost watch coverage was not a terminal visible refusal"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert!(!status.success());
        assert!(
            !socket.exists(),
            "terminal daemon left its owned socket behind"
        );
        assert_eq!(
            std::fs::read(preserved.join("nested/evidence.jsonl")).unwrap(),
            b"preserved evidence"
        );
        if replace_root {
            assert_eq!(
                std::fs::read(root.join("replacement.jsonl")).unwrap(),
                b"replacement evidence"
            );
        }
    }
}

#[test]
fn anchored_claim_open_refuses_fifos_and_replaced_coordination_locks() {
    use sparrow_client::CompletedFileIdentity;
    use sparrow_server::{ServerConfig, consumption::ClaimRoot};
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("store");
    let store = ServerConfig::read(&write_server_config(
        dir.path(),
        &data,
        &data.join("rpc.sock"),
    ))
    .unwrap()
    .open_store()
    .unwrap();
    let producer = dir.path().join("producer");
    std::fs::create_dir(&producer).unwrap();
    let root = ClaimRoot::open(store, "watch", &producer).unwrap();
    let fifo = producer.join("pipe");
    rustix::fs::mknodat(
        rustix::fs::CWD,
        &fifo,
        rustix::fs::FileType::Fifo,
        rustix::fs::Mode::from_raw_mode(0o600),
        0,
    )
    .unwrap();
    let expected = CompletedFileIdentity::capture(&fifo.metadata().unwrap());
    let (sent, received) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        assert!(
            root.reserve(Path::new("pipe"), expected, "source", "log")
                .is_err()
        );
        let file = producer.join("file");
        std::fs::write(&file, b"keep source").unwrap();
        let lock = producer.join(".sparrow-inflight/.lock");
        std::fs::rename(&lock, producer.join(".sparrow-inflight/old-lock")).unwrap();
        std::fs::write(&lock, b"replacement lock").unwrap();
        assert!(
            root.reserve(
                Path::new("file"),
                CompletedFileIdentity::capture(&file.metadata().unwrap()),
                "source",
                "log"
            )
            .is_err()
        );
        assert_eq!(std::fs::read(&file).unwrap(), b"keep source");
        sent.send(()).unwrap();
    });
    received
        .recv_timeout(Duration::from_secs(2))
        .expect("a FIFO admission must not block waiting for a writer");
    worker.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn completed_tail_claim_worker_recovers_without_upload_and_preserves_replacements() {
    use sparrow_core::{
        ConsumptionDirectories, ConsumptionGeneration, ConsumptionHandoff, TailFinishIntent,
        TailStart,
    };
    use sparrow_server::{
        ServerConfig,
        consumption::{ClaimRoot, Recovery},
        watches::{ConsumeWorker, WatchRegistry},
    };
    use std::sync::Arc;
    for phase in ["reserved", "claimed", "published", "unlinked", "changed"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("active");
        std::fs::create_dir(&root).unwrap();
        let data = dir.path().join("store");
        let socket = data.join("absent-rpc.sock");
        let config_path = write_server_config(dir.path(), &data, &socket);
        append_tail_config(&config_path, &root, "log");
        let config = ServerConfig::read(&config_path).unwrap();
        let store = config.open_store().unwrap();
        let registry = WatchRegistry::default();
        let operations = Arc::new(tokio::sync::Semaphore::new(0));
        let worker = ConsumeWorker::start(
            store.clone(),
            &config.consume,
            socket.clone(),
            registry.clone(),
            operations.clone(),
        )
        .unwrap();
        assert!(!root.join(".sparrow-inflight").exists());
        worker.shutdown().await.unwrap();
        let path = root.join("finished.log");
        std::fs::write(&path, b"first\nlast").unwrap();
        let observed = sparrow_client::CompletedFileIdentity::capture(&path.metadata().unwrap());
        let directory = sparrow_client::CompletedFileIdentity::capture(&root.metadata().unwrap());
        let generation = store
            .tail_start(
                &TailStart {
                    request_id: "worker-tail-start".into(),
                    watch: "completed".into(),
                    watch_revision: 1,
                    directories: ConsumptionDirectories {
                        root_device: directory.device,
                        root_inode: directory.inode,
                        parent_device: directory.device,
                        parent_inode: directory.inode,
                    },
                    path: b"finished.log".to_vec(),
                    source: "consume-acceptance".into(),
                    format: "log".into(),
                    device: observed.device,
                    inode: observed.inode,
                },
                sparrow_server::now().unwrap(),
            )
            .unwrap();
        let first = store
            .tail_append(
                &generation.id,
                0,
                b"first\n",
                false,
                sparrow_server::now().unwrap(),
            )
            .unwrap()
            .artifact
            .unwrap();
        let record = store.record(&first.record_ids[0]).unwrap();
        let finish = TailFinishIntent {
            request_id: "worker-tail-finish".into(),
            generation_id: generation.id.clone(),
            watch_revision: 1,
            observed: ConsumptionGeneration {
                device: observed.device,
                inode: observed.inode,
                byte_length: observed.length,
                modified: (observed.modified_seconds, observed.modified_nanos as u32),
                changed: (observed.changed_seconds, observed.changed_nanos as u32),
            },
        };
        store
            .tail_finish_start(&finish, sparrow_server::now().unwrap())
            .unwrap();
        store
            .tail_append(
                &generation.id,
                6,
                b"last",
                true,
                sparrow_server::now().unwrap(),
            )
            .unwrap();
        let terminal = store.tail_generation(&generation.id).unwrap();
        let intent = ConsumptionHandoff {
            request_id: "worker-tail-handoff".into(),
            watch: "completed".into(),
            watch_revision: 1,
            relative_path: b"finished.log".to_vec(),
            observed: finish.observed,
            tail_generation_id: Some(generation.id.clone()),
        };
        let reserved = store
            .consumption_handoff_reserve(
                &intent,
                "consume-acceptance",
                "log",
                generation.declaration.directories.clone(),
                sparrow_server::now().unwrap(),
            )
            .unwrap();
        let owner = ClaimRoot::open(store.clone(), "completed", &root).unwrap();
        let private = root.join(".sparrow-inflight").join(&reserved.import_id);
        if phase != "reserved" {
            let Recovery::Claimed(mut claim) = owner
                .recover(&reserved.import_id, sparrow_server::now().unwrap())
                .unwrap()
            else {
                panic!("unpublished source must be claimed")
            };
            assert!(
                owner
                    .remove(&claim, sparrow_server::now().unwrap())
                    .is_err()
            );
            std::fs::write(&path, b"replacement stays independent").unwrap();
            if ["published", "unlinked"].contains(&phase) {
                owner.publish_tail(&mut claim).unwrap();
            }
            if phase == "unlinked" {
                std::fs::remove_file(&private).unwrap();
                std::fs::File::open(private.parent().unwrap())
                    .unwrap()
                    .sync_all()
                    .unwrap();
            }
            if phase == "changed" {
                std::fs::write(&private, b"late writer changes claimed bytes").unwrap();
            }
        }
        drop(owner);
        let pending = store.consumption_get(&reserved.import_id).unwrap();
        let worker = ConsumeWorker::start(
            store.clone(),
            &config.consume,
            socket.clone(),
            registry.clone(),
            operations.clone(),
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(store.consumption_get(&reserved.import_id).unwrap(), pending);
        assert!(
            !socket.exists(),
            "tail retirement must not need an upload server"
        );
        operations.add_permits(2);
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(!worker.is_finished(), "tail claim worker failed");
            let states = registry.snapshot().unwrap();
            if phase == "changed" {
                if states[0]
                    .failure
                    .as_ref()
                    .is_some_and(|message| message.contains("changed"))
                {
                    break;
                }
            } else if store
                .consumption_get(&reserved.import_id)
                .unwrap()
                .removed_at
                .is_some()
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "tail claim recovery stalled in {phase}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        worker.shutdown().await.unwrap();
        let completed = store.consumption_get(&reserved.import_id).unwrap();
        if phase == "changed" {
            assert!(completed.removed_at.is_none());
            assert!(!completed.tail_published);
            assert_eq!(
                std::fs::read(&private).unwrap(),
                b"late writer changes claimed bytes"
            );
        } else {
            assert!(completed.removed_at.is_some());
            assert_eq!(completed.published_id(), Some(generation.id.as_str()));
            assert!(!private.exists());
        }
        if phase != "reserved" {
            assert_eq!(
                std::fs::read(&path).unwrap(),
                b"replacement stays independent"
            );
        } else {
            assert!(!path.exists());
        }
        assert!(completed.published_artifact.is_none());
        assert_eq!(store.tail_generation(&generation.id).unwrap(), terminal);
        let unchanged = store.record(&record.id).unwrap();
        assert_eq!(unchanged.received_at, record.received_at);
        assert_eq!(unchanged.expires_at, record.expires_at);
        assert_eq!(unchanged.tail, record.tail);
        assert_eq!(
            store.records(sparrow_server::now().unwrap()).unwrap().len(),
            2
        );
        assert!(!socket.exists());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn anchored_claim_recovery_publishes_exact_generation_and_never_deletes_replacements() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use sparrow_client::CompletedFileIdentity;
            use sparrow_server::{
                Rpc, ServerConfig,
                consumption::{ClaimRoot, Recovery},
            };
            use std::{os::unix::fs::symlink, sync::Arc};
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config =
                ServerConfig::read(&write_server_config(dir.path(), &data, &socket)).unwrap();
            let store = config.open_store().unwrap();
            let producer = dir.path().join("producer");
            std::fs::create_dir_all(producer.join("nested")).unwrap();
            let listener = tokio::net::UnixListener::bind(&socket).unwrap();
            let capability: wire::sparrow::Client = capnp_rpc::new_client(Rpc::new(
                store.clone(),
                Arc::new(tokio::sync::Semaphore::new(4)),
                config.max_artifact_bytes,
                config.max_pin_members,
            ));
            let serving = tokio::task::spawn_local(async move {
                let (stream, _) = listener.accept().await.unwrap();
                sparrow_server::serve_connection(stream, capability)
                    .await
                    .unwrap();
            });
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            assert!(ClaimRoot::open(store.clone(), "watch", &data).is_err());
            assert!(ClaimRoot::open(store.clone(), "watch", dir.path()).is_err());
            let root = ClaimRoot::open(store.clone(), "watch", &producer).unwrap();
            assert!(ClaimRoot::open(store.clone(), "watch", &producer).is_err());
            let outside = dir.path().join("outside");
            std::fs::write(&outside, b"outside").unwrap();
            symlink(&outside, producer.join("link")).unwrap();
            let expected = CompletedFileIdentity::capture(&outside.metadata().unwrap());
            assert!(
                root.reserve(Path::new("link"), expected, "source", "log")
                    .is_err()
            );
            symlink(dir.path(), producer.join("escape")).unwrap();
            assert!(
                root.reserve(Path::new("escape/outside"), expected, "source", "log")
                    .is_err()
            );

            let original = producer.join("nested/file.jsonl");
            let input = b"{\"message\":\"claimed exact generation\"}\n";
            std::fs::write(&original, input).unwrap();
            let reservation = root
                .reserve(
                    Path::new("nested/file.jsonl"),
                    CompletedFileIdentity::capture(&original.metadata().unwrap()),
                    "claim-source",
                    "jsonl",
                )
                .unwrap();
            drop(root); // Interrupted after durable reservation, before rename.
            let root = ClaimRoot::open(store.clone(), "watch", &producer).unwrap();
            let mut claim = match root
                .recover(&reservation.import_id, sparrow_server::now().unwrap())
                .unwrap()
            {
                Recovery::Claimed(value) => value,
                Recovery::Removed(_) => panic!("unpublished source cannot be complete"),
            };
            let private = producer
                .join(".sparrow-inflight")
                .join(&reservation.import_id);
            assert!(!original.exists());
            assert_eq!(std::fs::read(&private).unwrap(), input);
            assert!(root.remove(&claim, sparrow_server::now().unwrap()).is_err());
            assert!(private.exists());
            std::fs::write(&original, b"replacement generation\n").unwrap();
            std::fs::rename(producer.join("nested"), producer.join("old-nested")).unwrap();
            std::fs::create_dir(producer.join("nested")).unwrap();
            std::fs::write(&original, b"replacement generation\n").unwrap();
            let receipt = root.publish(&client, &mut claim).await.unwrap();
            assert_eq!(
                store
                    .bytes(&receipt.id, 0, input.len(), sparrow_server::now().unwrap())
                    .unwrap(),
                input
            );
            drop(claim);
            drop(root); // Publication succeeded, but removal was not attempted.
            let root = ClaimRoot::open(store.clone(), "watch", &producer).unwrap();
            let mut claim = match root
                .recover(&reservation.import_id, sparrow_server::now().unwrap())
                .unwrap()
            {
                Recovery::Claimed(value) => value,
                Recovery::Removed(_) => panic!("claim still exists"),
            };
            assert_eq!(root.publish(&client, &mut claim).await.unwrap(), receipt);
            let removed = root.remove(&claim, sparrow_server::now().unwrap()).unwrap();
            assert!(removed.removed_at.is_some());
            assert!(!private.exists());
            assert_eq!(
                std::fs::read(&original).unwrap(),
                b"replacement generation\n"
            );
            assert_eq!(
                std::fs::read(producer.join("old-nested/file.jsonl")).unwrap(),
                b"replacement generation\n"
            );
            assert_eq!(
                root.remove(&claim, sparrow_server::now().unwrap()).unwrap(),
                removed
            );
            drop(claim);

            // Interrupted after rename, before the directory/claim checkpoint.
            let before = producer.join("renamed.log");
            std::fs::write(&before, b"rename checkpoint\n").unwrap();
            let renamed = root
                .reserve(
                    Path::new("renamed.log"),
                    CompletedFileIdentity::capture(&before.metadata().unwrap()),
                    "renamed-source",
                    "log",
                )
                .unwrap();
            let renamed_private = producer.join(".sparrow-inflight").join(&renamed.import_id);
            std::fs::rename(&before, &renamed_private).unwrap();
            drop(root);
            let root = ClaimRoot::open(store.clone(), "watch", &producer).unwrap();
            let mut claim = match root
                .recover(&renamed.import_id, sparrow_server::now().unwrap())
                .unwrap()
            {
                Recovery::Claimed(value) => value,
                Recovery::Removed(_) => panic!("unpublished moved source"),
            };
            assert!(claim.journal().claimed.is_some());
            let accepted = root.publish(&client, &mut claim).await.unwrap();
            std::fs::remove_file(&renamed_private).unwrap(); // Interrupted after unlink, before sync/confirmation.
            std::fs::write(&before, b"new renamed generation\n").unwrap();
            drop(claim);
            drop(root);
            let root = ClaimRoot::open(store.clone(), "watch", &producer).unwrap();
            match root
                .recover(&renamed.import_id, sparrow_server::now().unwrap())
                .unwrap()
            {
                Recovery::Removed(saved) => {
                    assert_eq!(saved.published_artifact, Some(accepted.id));
                    assert!(saved.removed_at.is_some());
                }
                Recovery::Claimed(_) => {
                    panic!("directory-synced absence must complete the original publication")
                }
            }
            assert_eq!(std::fs::read(&before).unwrap(), b"new renamed generation\n");
            assert_eq!(
                store.records(sparrow_server::now().unwrap()).unwrap().len(),
                2
            );

            // A changed private generation remains recoverable and cannot be removed.
            let changing = producer.join("changing.log");
            std::fs::write(&changing, b"initial\n").unwrap();
            let journal = root
                .reserve(
                    Path::new("changing.log"),
                    CompletedFileIdentity::capture(&changing.metadata().unwrap()),
                    "changing-source",
                    "log",
                )
                .unwrap();
            let mut claim = match root
                .recover(&journal.import_id, sparrow_server::now().unwrap())
                .unwrap()
            {
                Recovery::Claimed(value) => value,
                Recovery::Removed(_) => panic!("new claim"),
            };
            root.publish(&client, &mut claim).await.unwrap();
            let path = producer.join(".sparrow-inflight").join(&journal.import_id);
            std::fs::write(&path, b"changed\n").unwrap();
            assert!(root.remove(&claim, sparrow_server::now().unwrap()).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), b"changed\n");
            assert!(
                store
                    .consumption_get(&journal.import_id)
                    .unwrap()
                    .removed_at
                    .is_none()
            );
            drop(claim);

            // Occupied private names are never overwritten before a claim checkpoint.
            let collision = producer.join("collision.log");
            std::fs::write(&collision, b"expected collision source\n").unwrap();
            let collision_journal = root
                .reserve(
                    Path::new("collision.log"),
                    CompletedFileIdentity::capture(&collision.metadata().unwrap()),
                    "collision-source",
                    "log",
                )
                .unwrap();
            let collision_private = producer
                .join(".sparrow-inflight")
                .join(&collision_journal.import_id);
            std::fs::write(&collision_private, b"never overwrite these bytes\n").unwrap();
            assert!(
                root.recover(&collision_journal.import_id, sparrow_server::now().unwrap())
                    .is_err()
            );
            assert_eq!(
                std::fs::read(&collision).unwrap(),
                b"expected collision source\n"
            );
            assert_eq!(
                std::fs::read(&collision_private).unwrap(),
                b"never overwrite these bytes\n"
            );
            assert!(
                store
                    .consumption_get(&collision_journal.import_id)
                    .unwrap()
                    .claimed
                    .is_none()
            );

            // An unpublished missing claim never falls back to a replacement source path.
            let missing = producer.join("missing.log");
            std::fs::write(&missing, b"missing original\n").unwrap();
            let missing_journal = root
                .reserve(
                    Path::new("missing.log"),
                    CompletedFileIdentity::capture(&missing.metadata().unwrap()),
                    "missing-source",
                    "log",
                )
                .unwrap();
            let claim = match root
                .recover(&missing_journal.import_id, sparrow_server::now().unwrap())
                .unwrap()
            {
                Recovery::Claimed(value) => value,
                Recovery::Removed(_) => panic!("unpublished source"),
            };
            std::fs::remove_file(
                producer
                    .join(".sparrow-inflight")
                    .join(&missing_journal.import_id),
            )
            .unwrap();
            std::fs::write(&missing, b"unaccepted replacement\n").unwrap();
            assert!(
                root.recover(&missing_journal.import_id, sparrow_server::now().unwrap())
                    .is_err()
            );
            assert_eq!(
                std::fs::read(&missing).unwrap(),
                b"unaccepted replacement\n"
            );
            assert_eq!(
                store.records(sparrow_server::now().unwrap()).unwrap().len(),
                3
            );
            drop(claim);

            // Parent replacement refuses before moving either generation.
            std::fs::create_dir(producer.join("parent")).unwrap();
            let file = producer.join("parent/file");
            std::fs::write(&file, b"old parent\n").unwrap();
            let parent_claim = root
                .reserve(
                    Path::new("parent/file"),
                    CompletedFileIdentity::capture(&file.metadata().unwrap()),
                    "parent-source",
                    "log",
                )
                .unwrap();
            std::fs::rename(producer.join("parent"), producer.join("old-parent")).unwrap();
            std::fs::create_dir(producer.join("parent")).unwrap();
            std::fs::write(&file, b"new parent\n").unwrap();
            assert!(
                root.recover(&parent_claim.import_id, sparrow_server::now().unwrap())
                    .is_err()
            );
            assert_eq!(std::fs::read(&file).unwrap(), b"new parent\n");
            assert_eq!(
                std::fs::read(producer.join("old-parent/file")).unwrap(),
                b"old parent\n"
            );
            drop(root);

            // Unrecognized private files are preserved and prevent blind startup cleanup.
            let orphan = producer.join(".sparrow-inflight/orphan");
            std::fs::write(&orphan, b"orphan bytes").unwrap();
            assert!(ClaimRoot::open(store.clone(), "watch", &producer).is_err());
            assert_eq!(std::fs::read(&orphan).unwrap(), b"orphan bytes");
            std::fs::remove_file(&orphan).unwrap();
            std::fs::rename(&producer, dir.path().join("old-producer")).unwrap();
            std::fs::create_dir(&producer).unwrap();
            assert!(ClaimRoot::open(store.clone(), "watch", &producer).is_err());
            assert!(!producer.join(".sparrow-inflight").exists());
            drop(client);
            serving.await.unwrap();
            store.flush().unwrap();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn query_selection_is_shared_by_typed_cli_and_mcp_surfaces() {
    use purrdf_lex::json::{Object, Value};
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let store_path = dir.path().join("store");
            let socket = store_path.join("rpc.sock");
            let config = write_server_config(dir.path(), &store_path, &socket);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let store = client.discovery().store_id.clone();
            client
                .ingest(
                    "selection-noise",
                    "selection-source",
                    wire::Format::Json,
                    br#"{"message":"needle noise"}"#,
                )
                .await
                .unwrap();
            let selected = client
                .ingest(
                    "selection-target",
                    "selection-source",
                    wire::Format::Json,
                    br#"{"message":"needle target"}"#,
                )
                .await
                .unwrap();
            let selection: Value = Object::new()
                .with("kind", "selection")
                .with("store_id", store.clone())
                .with("artifact_ids", Value::array([selected.id.clone()]))
                .with("record_ids", Value::array([selected.record_ids[0].clone()]))
                .into();
            let path = dir.path().join("scope.json");
            std::fs::write(&path, purrdf_lex::json::write_compact(&selection)).unwrap();
            let path = path.to_str().unwrap();
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            for (index, (name, command)) in [
                ("keys_list", "keys"),
                ("values_search", "values"),
                ("records_search", "records"),
                ("text_search", "text"),
                ("records_context", "context"),
            ]
            .into_iter()
            .enumerate()
            {
                let mut input = Object::new().with("scope", selection.clone());
                let mut args = vec![command];
                if name == "text_search" {
                    input = input.with("text", "needle");
                    args.push("needle");
                }
                if name == "records_context" {
                    input = input
                        .with("store_id", store.clone())
                        .with("id", selected.record_ids[0].clone());
                    args.extend([store.as_str(), selected.record_ids[0].as_str()]);
                }
                args.extend(["--scope-file", path]);
                let cli_result = cli(&socket, &args);
                let result = mcp.tool(2 + index as u64, name, input.into()).await;
                assert_eq!(
                    result["isError"].as_bool(),
                    Some(false),
                    "{name}: {result:?}"
                );
                let result = &result["structuredContent"];
                if name == "records_context" {
                    assert!(result["before"]["rows"].as_array().unwrap().is_empty());
                    assert!(result["after"]["rows"].as_array().unwrap().is_empty());
                    assert_eq!(result["anchor"], cli_result["anchor"]);
                } else {
                    assert_eq!(result["rows"], cli_result["rows"]);
                    if name != "keys_list" {
                        assert!(
                            result["rows"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .all(|row| row["record_id"].as_str()
                                    == Some(selected.record_ids[0].as_str()))
                        );
                        assert!(!result["rows"].as_array().unwrap().is_empty());
                    }
                }
            }
            let created = mcp
                .tool(
                    10,
                    "pin",
                    Object::new()
                        .with("store_id", store.clone())
                        .with("request_id", "selection-pin")
                        .with("name", "case")
                        .with("targets", Value::array([selected.id.clone()]))
                        .with("days", 2_u64)
                        .into(),
                )
                .await;
            assert_eq!(created["isError"].as_bool(), Some(false));
            let id = created["structuredContent"]["pin_id"].as_str().unwrap();
            let pinned = mcp
                .tool(
                    11,
                    "records_search",
                    Object::new()
                        .with(
                            "scope",
                            Object::new()
                                .with("kind", "pin")
                                .with("store_id", store.clone())
                                .with("pin_id", id)
                                .with("expected_revision", "1"),
                        )
                        .into(),
                )
                .await;
            assert_eq!(pinned["isError"].as_bool(), Some(false));
            assert_eq!(
                pinned["structuredContent"]["rows"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
            let conflict = mcp
                .tool(
                    12,
                    "records_search",
                    Object::new()
                        .with(
                            "scope",
                            Object::new()
                                .with("kind", "pin")
                                .with("store_id", store.clone())
                                .with("pin_id", id)
                                .with("expected_revision", "2"),
                        )
                        .into(),
                )
                .await;
            assert_eq!(
                conflict["structuredContent"]["error"]["code"].as_str(),
                Some("CONFLICT")
            );
            // Direct generated requests must preserve typed domain errors, not transport exceptions.
            let mut request = client.capability().keys_list_request();
            let mut scope = request.get().init_scope();
            scope.set_schema_version(sparrow_client::SCHEMA_VERSION);
            scope
                .reborrow()
                .init_received_before()
                .set_unix_millis(u64::MAX);
            let mut target = scope.init_selection().init_evidence(1).get(0);
            target.set_store_id("store:foreign");
            target.set_artifact(&selected.id);
            let reply = request.send().promise.await.unwrap();
            match reply.get().unwrap().get_result().unwrap().which().unwrap() {
                wire::result::Error(error) => assert_eq!(
                    error.unwrap().get_code().unwrap().to_str().unwrap(),
                    "STORE_MISMATCH"
                ),
                wire::result::Ok(_) => panic!("foreign selection accepted"),
            }
            for (index, invalid) in [
                Object::new()
                    .with("kind", "selection")
                    .with("store_id", store.clone()),
                Object::new().with("kind", "live").with("pin_id", id),
                Object::new()
                    .with("kind", "selection")
                    .with("store_id", store.clone())
                    .with("record_ids", Value::array(Vec::<String>::new())),
            ]
            .into_iter()
            .enumerate()
            {
                let result = mcp
                    .tool(
                        20 + index as u64,
                        "keys_list",
                        Object::new().with("scope", invalid).into(),
                    )
                    .await;
                assert_eq!(result["isError"].as_bool(), Some(true));
                assert_eq!(
                    result["structuredContent"]["error"]["code"].as_str(),
                    Some("INVALID_ARGUMENT")
                );
            }
            mcp.close(true).await;
        })
        .await;
}
impl Drop for Daemon {
    fn drop(&mut self) {
        self.0.kill().expect("stop this test's daemon");
        self.0.wait().expect("reap this test's daemon");
    }
}

fn delivery_event(text: &str) -> sparrow_client::ProducerEvent {
    sparrow_client::ProducerEvent {
        sequence: 0,
        event_time: 1000,
        kind: wire::EventKind::Log,
        level: wire::EventLevel::Info,
        name: "delivery".into(),
        target: "application".into(),
        fields: vec![sparrow_client::EventField {
            name: "message".into(),
            value: sparrow_client::FieldValue::Text(text.into()),
        }],
        context: vec![],
        span: None,
        follows_from: None,
    }
}

async fn admit(delivery: &sparrow_client::Delivery, event: sparrow_client::ProducerEvent) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match delivery.try_send(event.clone()) {
            sparrow_client::Admission::Queued { sequence } => return sequence,
            sparrow_client::Admission::Dropped {
                reason: sparrow_client::DropReason::Contended,
                ..
            } => {
                assert!(
                    Instant::now() < deadline,
                    "producer admission remained contended"
                );
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            admission => panic!("unexpected admission: {admission:?}"),
        }
    }
}

async fn delivery_barrier(
    delivery: &sparrow_client::Delivery,
    shutdown: bool,
    timeout: Duration,
) -> Result<sparrow_client::DeliveryWatermark, sparrow_client::FlushError> {
    let handle = delivery.clone();
    tokio::task::spawn_blocking(move || {
        if shutdown {
            handle.shutdown(timeout)
        } else {
            handle.flush(timeout)
        }
    })
    .await
    .unwrap()
}

fn reached_delivery(
    result: Result<sparrow_client::DeliveryWatermark, sparrow_client::FlushError>,
) -> sparrow_client::DeliveryWatermark {
    match result {
        Ok(value) => {
            assert_eq!(value.dropped_before_fence, 0);
            value
        }
        Err(sparrow_client::FlushError::Dropped(value)) => {
            assert!(value.dropped_before_fence > 0);
            *value
        }
        Err(error) => panic!("admitted events did not reach durability: {error}"),
    }
}

struct EventRelayState {
    backend: std::cell::RefCell<wire::sparrow::Client>,
    calls: std::cell::RefCell<Vec<sparrow_client::EventBatch>>,
    accepted: std::cell::RefCell<Option<sparrow_client::ArtifactReceipt>>,
    drop_first: std::cell::Cell<bool>,
    corrupt_receipt: std::cell::Cell<bool>,
    corrupt_publication: std::cell::Cell<bool>,
    mutate_file: std::cell::RefCell<Option<std::path::PathBuf>>,
    mutate_published_file: std::cell::RefCell<Option<std::path::PathBuf>>,
    first_accepted: tokio::sync::Notify,
}
struct EventRelay(std::rc::Rc<EventRelayState>);
impl EventRelayState {
    fn corrupt(&self, mut artifact: wire::artifact::Builder<'_>, enabled: bool) {
        if enabled {
            artifact.set_payload_id(format!("blake3:{}", purrdf_hash::hex::encode(&[0; 32])));
        }
    }
}
impl wire::sparrow::Server for EventRelay {
    async fn discover(
        self: std::rc::Rc<Self>,
        _: wire::sparrow::DiscoverParams,
        mut results: wire::sparrow::DiscoverResults,
    ) -> capnp::Result<()> {
        let backend = self.0.backend.borrow().clone();
        let response = backend.discover_request().send().promise.await?;
        results.get().set_result(response.get()?.get_result()?)
    }
    async fn ingest(
        self: std::rc::Rc<Self>,
        params: wire::sparrow::IngestParams,
        mut results: wire::sparrow::IngestResults,
    ) -> capnp::Result<()> {
        let mut request = self.0.backend.borrow().ingest_request();
        request.get().set_request(params.get()?.get_request()?)?;
        let response = request.send().promise.await?;
        results.get().set_result(response.get()?.get_result()?)?;
        if let wire::result::Ok(value) = results.get().get_result()?.which()? {
            self.0.corrupt(value?, self.0.corrupt_receipt.get());
        }
        Ok(())
    }
    async fn import_begin(
        self: std::rc::Rc<Self>,
        params: wire::sparrow::ImportBeginParams,
        mut results: wire::sparrow::ImportBeginResults,
    ) -> capnp::Result<()> {
        let mut request = self.0.backend.borrow().import_begin_request();
        request.get().set_request(params.get()?.get_request()?)?;
        let response = request.send().promise.await?;
        results.get().set_result(response.get()?.get_result()?)?;
        if let Some(path) = self.0.mutate_file.borrow().as_ref() {
            std::fs::write(path, b"changed").unwrap();
        }
        if self.0.corrupt_receipt.get()
            && let wire::result::Ok(value) = results.get().get_result()?.which()?
        {
            match value?.which()? {
                wire::import_start::Accepted(value) => self.0.corrupt(value?, true),
                wire::import_start::Staging(value) => value?.set_store_id("store:foreign"),
            }
        }
        Ok(())
    }
    async fn status(
        self: std::rc::Rc<Self>,
        _: wire::sparrow::StatusParams,
        mut results: wire::sparrow::StatusResults,
    ) -> capnp::Result<()> {
        let backend = self.0.backend.borrow().clone();
        let response = backend.status_request().send().promise.await?;
        results.get().set_result(response.get()?.get_result()?)
    }
    async fn events_ingest(
        self: std::rc::Rc<Self>,
        params: wire::sparrow::EventsIngestParams,
        mut results: wire::sparrow::EventsIngestResults,
    ) -> capnp::Result<()> {
        let input = sparrow_client::EventBatch::decode(params.get()?.get_batch()?)
            .map_err(|error| capnp::Error::failed(error.to_string()))?;
        assert!(
            self.0.calls.borrow().len() < 8,
            "retry fixture exceeded its request bound"
        );
        self.0.calls.borrow_mut().push(input.clone());
        let backend = self.0.backend.borrow().clone();
        let mut request = backend.events_ingest_request();
        input
            .encode(request.get().init_batch())
            .map_err(|error| capnp::Error::failed(error.to_string()))?;
        let response = request.send().promise.await?;
        if let wire::result::Ok(value) = response.get()?.get_result()?.which()? {
            let receipt = sparrow_client::ArtifactReceipt::decode(value?)
                .map_err(|error| capnp::Error::failed(error.to_string()))?;
            if self.0.drop_first.replace(false) {
                *self.0.accepted.borrow_mut() = Some(receipt);
                self.0.first_accepted.notify_one();
                // The real daemon committed. Withhold its reply until the worker's finite deadline closes this connection.
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }
        results.get().set_result(response.get()?.get_result()?)?;
        if let wire::result::Ok(value) = results.get().get_result()?.which()? {
            self.0.corrupt(value?, self.0.corrupt_receipt.get());
        }
        Ok(())
    }

    async fn import_append(
        self: std::rc::Rc<Self>,
        params: wire::sparrow::ImportAppendParams,
        mut results: wire::sparrow::ImportAppendResults,
    ) -> capnp::Result<()> {
        let mut request = self.0.backend.borrow().import_append_request();
        request.get().set_request(params.get()?.get_request()?)?;
        let response = request.send().promise.await?;
        results.get().set_result(response.get()?.get_result()?)
    }
    async fn import_seal(
        self: std::rc::Rc<Self>,
        params: wire::sparrow::ImportSealParams,
        mut results: wire::sparrow::ImportSealResults,
    ) -> capnp::Result<()> {
        let mut request = self.0.backend.borrow().import_seal_request();
        request.get().set_import(params.get()?.get_import()?)?;
        let response = request.send().promise.await?;
        results.get().set_result(response.get()?.get_result()?)
    }
    async fn import_layout(
        self: std::rc::Rc<Self>,
        params: wire::sparrow::ImportLayoutParams,
        mut results: wire::sparrow::ImportLayoutResults,
    ) -> capnp::Result<()> {
        let mut request = self.0.backend.borrow().import_layout_request();
        request.get().set_import(params.get()?.get_import()?)?;
        let response = request.send().promise.await?;
        results.get().set_result(response.get()?.get_result()?)
    }
    async fn import_prepare(
        self: std::rc::Rc<Self>,
        params: wire::sparrow::ImportPrepareParams,
        mut results: wire::sparrow::ImportPrepareResults,
    ) -> capnp::Result<()> {
        let mut request = self.0.backend.borrow().import_prepare_request();
        request.get().set_request(params.get()?.get_request()?)?;
        let response = request.send().promise.await?;
        results.get().set_result(response.get()?.get_result()?)
    }
    async fn import_publish(
        self: std::rc::Rc<Self>,
        params: wire::sparrow::ImportPublishParams,
        mut results: wire::sparrow::ImportPublishResults,
    ) -> capnp::Result<()> {
        let mut request = self.0.backend.borrow().import_publish_request();
        request.get().set_import(params.get()?.get_import()?)?;
        let response = request.send().promise.await?;
        results.get().set_result(response.get()?.get_result()?)?;
        if let Some(path) = self.0.mutate_published_file.borrow().as_ref() {
            std::fs::write(path, b"changed").unwrap();
        }
        if let wire::result::Ok(value) = results.get().get_result()?.which()? {
            self.0.corrupt(value?, self.0.corrupt_publication.get());
        }
        Ok(())
    }
}

fn relay(
    socket: &Path,
    backend: wire::sparrow::Client,
) -> (std::rc::Rc<EventRelayState>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::UnixListener::bind(socket).unwrap();
    let state = std::rc::Rc::new(EventRelayState {
        backend: std::cell::RefCell::new(backend),
        calls: std::cell::RefCell::new(Vec::new()),
        accepted: std::cell::RefCell::new(None),
        drop_first: std::cell::Cell::new(true),
        corrupt_receipt: std::cell::Cell::new(false),
        corrupt_publication: std::cell::Cell::new(false),
        mutate_file: std::cell::RefCell::new(None),
        mutate_published_file: std::cell::RefCell::new(None),
        first_accepted: tokio::sync::Notify::new(),
    });
    let capability: wire::sparrow::Client = capnp_rpc::new_client(EventRelay(state.clone()));
    let task = tokio::task::spawn_local(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::task::spawn_local(sparrow_server::serve_connection(stream, capability.clone()));
        }
    });
    (state, task)
}

#[tokio::test(flavor = "current_thread")]
async fn opened_completed_file_upload_keeps_claimed_generation_and_replacement_path() {
    use std::io::{Read, Seek, SeekFrom};
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let store_path = dir.path().join("store");
            let socket = store_path.join("rpc.sock");
            let config = write_server_config(dir.path(), &store_path, &socket);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let original = dir.path().join("producer.jsonl");
            let claimed = dir.path().join("claimed.jsonl");
            let bytes = b"{\"message\":\"opened source generation\"}\n";
            std::fs::write(&original, bytes).unwrap();
            std::fs::rename(&original, &claimed).unwrap();
            let mut file = sparrow_client::open_completed_file(&claimed).unwrap();
            let identity =
                sparrow_client::CompletedFileIdentity::capture(&file.metadata().unwrap());
            std::fs::write(&original, b"replacement generation\n").unwrap();
            file.seek(SeekFrom::Start(7)).unwrap(); // Upload must start at byte zero.
            let receipt = client
                .import_open_file(
                    "descriptor-source",
                    "descriptor",
                    wire::Format::Jsonl,
                    &mut file,
                )
                .await
                .unwrap();
            assert_eq!(receipt.byte_length, bytes.len() as u64);
            assert_eq!(receipt.durability, wire::Durability::Durable);
            assert_eq!(
                file.stream_position().unwrap(),
                7,
                "upload preserves the caller's cursor"
            );
            assert_eq!(
                receipt.payload_id,
                format!(
                    "blake3:{}",
                    purrdf_hash::hex::encode(purrdf_hash::blake3::hash(bytes).as_bytes())
                )
            );
            identity.require(&file).unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            let mut preserved = Vec::new();
            file.read_to_end(&mut preserved).unwrap();
            assert_eq!(preserved, bytes);
            let retry = client
                .import_open_file(
                    "descriptor-source",
                    "descriptor",
                    wire::Format::Jsonl,
                    &mut file,
                )
                .await
                .unwrap();
            assert_eq!(retry, receipt);
            assert_eq!(file.stream_position().unwrap(), bytes.len() as u64);
            assert_eq!(std::fs::read(&claimed).unwrap(), bytes);
            assert_eq!(
                std::fs::read(&original).unwrap(),
                b"replacement generation\n"
            );
            let mut request = client.capability().bytes_read_request();
            let mut query = request.get().init_request();
            let mut evidence = query.reborrow().init_evidence();
            evidence.set_store_id(&receipt.store_id);
            evidence.set_artifact(&receipt.id);
            query.set_length(bytes.len() as u32);
            let response = request.send().promise.await.unwrap();
            assert_eq!(
                rpc_ok(response.get().unwrap().get_result().unwrap())
                    .get_data()
                    .unwrap(),
                bytes
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn producer_receipt_mismatch_never_certifies_or_retires_pending_input() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let store_path = dir.path().join("store");
            let socket = store_path.join("rpc.sock");
            let config = write_server_config(dir.path(), &store_path, &socket);
            let _daemon = start(&config, &socket).await;
            let genuine = sparrow_client::Client::connect(&socket).await.unwrap();
            let proxy_socket = dir.path().join("proxy.sock");
            let (state, proxy) = relay(&proxy_socket, genuine.capability().clone());
            state.drop_first.set(false);
            state.corrupt_receipt.set(true);
            let client = sparrow_client::Client::connect(&proxy_socket)
                .await
                .unwrap();
            assert!(matches!(
                client
                    .ingest("bad-direct-reply", "direct", wire::Format::Json, b"{}")
                    .await,
                Err(sparrow_client::Error::Protocol(_))
            ));
            let mut event = delivery_event("wrong receipt evidence");
            event.sequence = 1;
            let batch = sparrow_client::EventBatch {
                request_id: "bad-events-reply".into(),
                source: "events".into(),
                client_instance: [8; 16],
                events: vec![event],
            };
            assert!(matches!(
                client.events(&batch).await,
                Err(sparrow_client::Error::Protocol(_))
            ));
            let input = dir.path().join("source.json");
            std::fs::write(&input, b"{}").unwrap();
            genuine
                .ingest("bad-file-retry", "file", wire::Format::Json, b"{}")
                .await
                .unwrap();
            assert!(matches!(
                client
                    .import_file("bad-file-retry", "file", wire::Format::Json, &input)
                    .await,
                Err(sparrow_client::Error::Protocol(_))
            ));
            assert!(matches!(
                client
                    .import_file("bad-staging-reply", "file", wire::Format::Json, &input)
                    .await,
                Err(sparrow_client::Error::Protocol(_))
            ));
            assert_eq!(std::fs::read(&input).unwrap(), b"{}");
            // Mutation after the accepted-retry reply must also refuse source certification.
            std::fs::write(&input, b"initial").unwrap();
            genuine
                .ingest(
                    "late-source-change",
                    "changing-file",
                    wire::Format::Opaque,
                    b"initial",
                )
                .await
                .unwrap();
            state.corrupt_receipt.set(false);
            *state.mutate_file.borrow_mut() = Some(input.clone());
            assert!(matches!(
                client
                    .import_file(
                        "late-source-change",
                        "changing-file",
                        wire::Format::Opaque,
                        &input
                    )
                    .await,
                Err(sparrow_client::Error::InvalidArgument(_))
            ));
            assert_eq!(std::fs::read(&input).unwrap(), b"changed");
            *state.mutate_file.borrow_mut() = None;
            std::fs::write(&input, b"initial").unwrap();
            state.corrupt_publication.set(true);
            assert!(matches!(
                client
                    .import_file(
                        "bad-publication-reply",
                        "publication",
                        wire::Format::Opaque,
                        &input
                    )
                    .await,
                Err(sparrow_client::Error::Protocol(_))
            ));
            assert_eq!(std::fs::read(&input).unwrap(), b"initial");
            let committed = genuine
                .ingest(
                    "bad-publication-reply",
                    "publication",
                    wire::Format::Opaque,
                    b"initial",
                )
                .await
                .unwrap();
            assert_eq!(committed.durability, wire::Durability::Durable);
            state.corrupt_publication.set(false);
            *state.mutate_published_file.borrow_mut() = Some(input.clone());
            assert!(matches!(
                client
                    .import_file(
                        "late-publication-change",
                        "publication-change",
                        wire::Format::Opaque,
                        &input
                    )
                    .await,
                Err(sparrow_client::Error::InvalidArgument(_))
            ));
            assert_eq!(std::fs::read(&input).unwrap(), b"changed");
            let committed = genuine
                .ingest(
                    "late-publication-change",
                    "publication-change",
                    wire::Format::Opaque,
                    b"initial",
                )
                .await
                .unwrap();
            assert_eq!(committed.durability, wire::Durability::Durable);
            *state.mutate_published_file.borrow_mut() = None;
            state.corrupt_receipt.set(true);
            let delivery = sparrow_client::Delivery::start(sparrow_client::DeliveryConfig {
                flush_interval: Duration::from_millis(10),
                ..sparrow_client::DeliveryConfig::new(proxy_socket, "pending-receipt")
            })
            .unwrap();
            admit(&delivery, delivery_event("must retain unknown body")).await;
            let deadline = Instant::now() + Duration::from_secs(3);
            loop {
                if delivery.stats().unwrap().terminal_failure.is_some() {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "receipt mismatch must be a visible permanent failure"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let statistics = delivery.stats().unwrap();
            assert_eq!(statistics.durable_events, 0);
            assert_eq!(statistics.pending_events, 1);
            assert!(delivery.pending_batch().unwrap().is_some());
            assert!(matches!(
                delivery_barrier(&delivery, false, Duration::from_secs(1)).await,
                Err(sparrow_client::FlushError::Delivery(_))
            ));
            proxy.abort();
            let _ = proxy.await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn buffered_rpc_flush_sdk_cli_mcp_and_recovery_preserve_occurrence_identity() {
    tokio::task::LocalSet::new().run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let store_path = dir.path().join("store");
        let socket = store_path.join("rpc.sock");
        let config = write_server_config(dir.path(), &store_path, &socket);
        let original = std::fs::read_to_string(&config).unwrap();
        std::fs::write(&config, format!("{original}\nbuffered_flush_interval_ms = 60000\n")).unwrap();
        let daemon = start(&config, &socket).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        let client = sparrow_client::Client::connect(&socket).await.unwrap();
        assert_eq!(client.discovery().flush_interval_ms, 60000);
        let first = client.ingest_with_durability("buffered-rpc", "buffered-sdk", wire::Format::Json,
            br#"{"message":"explicit flush evidence"}"#, wire::Durability::Buffered).await.unwrap();
        assert_eq!(first.durability, wire::Durability::Buffered);
        let fence = first.buffered_through.clone().unwrap();
        assert_eq!(client.artifact(&first.id).await.unwrap(), first);
        assert_eq!(client.ingest_with_durability("buffered-rpc", "buffered-sdk", wire::Format::Json,
            br#"{"message":"explicit flush evidence"}"#, wire::Durability::Buffered).await.unwrap(), first);
        let reached = client.flush(&fence).await.unwrap();
        assert!(reached.sequence >= fence.sequence);
        let durable = client.artifact(&first.id).await.unwrap();
        assert_eq!(durable.id, first.id);
        assert_eq!(durable.record_ids, first.record_ids);
        assert_eq!(durable.durability, wire::Durability::Durable);
        assert!(durable.buffered_through.is_none());
        let mut event = delivery_event("buffered typed event");
        event.sequence = 1;
        let batch = sparrow_client::EventBatch { request_id: "buffered-event".into(),
            source: "buffered-event-sdk".into(), client_instance: [7; 16], events: vec![event] };
        let buffered_event = client.events_with_durability(&batch, wire::Durability::Buffered).await.unwrap();
        assert_eq!(buffered_event.durability, wire::Durability::Buffered);
        assert_eq!(client.events_with_durability(&batch, wire::Durability::Buffered).await.unwrap(), buffered_event);
        let durable_event = client.events(&batch).await.unwrap();
        assert_eq!(durable_event.id, buffered_event.id);
        assert_eq!(durable_event.record_ids, buffered_event.record_ids);
        assert_eq!(durable_event.durability, wire::Durability::Durable);
        let second = client.ingest_with_durability("buffered-cli", "buffered-sdk", wire::Format::Log,
            b"CLI flush evidence\n", wire::Durability::Buffered).await.unwrap();
        let token = second.buffered_through.unwrap();
        let cli = tokio::process::Command::new(env!("CARGO_BIN_EXE_sparrow"))
            .arg(&socket).args(["flush", &token.store_id, &token.session_id, &token.sequence.to_string()])
            .stdout(Stdio::piped()).stderr(Stdio::piped()).admitted_spawn().unwrap().wait_with_output().await.unwrap();
        assert!(cli.status.success(), "{}", String::from_utf8_lossy(&cli.stderr));
        assert!(String::from_utf8_lossy(&cli.stdout).contains("\"durability\":\"durable\""));
        assert_eq!(client.artifact(&second.id).await.unwrap().durability, wire::Durability::Durable);
        let third = client.ingest_with_durability("buffered-mcp", "buffered-sdk", wire::Format::Log,
            b"MCP flush evidence\n", wire::Durability::Buffered).await.unwrap();
        let token = third.buffered_through.unwrap();
        let mut mcp = Mcp::start(&socket);
        mcp.initialize(1).await;
        let operation = mcp.tool(2, "flush", purrdf_lex::json::Object::new()
            .with("store_id", token.store_id).with("session_id", token.session_id)
            .with("sequence", token.sequence.to_string()).into()).await;
        assert_eq!(operation["structuredContent"]["durability"].as_str(), Some("durable"));
        mcp.close(true).await;
        assert_eq!(client.artifact(&third.id).await.unwrap().durability, wire::Durability::Durable);
        drop(client);
        drop(daemon); // owned test process is killed/reaped, not gracefully flushed.
        std::fs::remove_file(&socket).unwrap();
        std::fs::write(&config, format!("{original}\nbuffered_flush_interval_ms = 25\n")).unwrap();
        let restarted = start(&config, &socket).await;
        let recovered = sparrow_client::Client::connect(&socket).await.unwrap();
        assert_eq!(recovered.artifact(&first.id).await.unwrap(), durable);
        assert!(matches!(recovered.flush(&fence).await,
            Err(sparrow_client::Error::Domain { code, retryable: false, .. }) if code == "STALE_WATERMARK"));
        let next = recovered.ingest_with_durability("periodic", "buffered-sdk", wire::Format::Log,
            b"automatic WAL boundary\n", wire::Durability::Buffered).await.unwrap();
        assert_eq!(next.durability, wire::Durability::Buffered);
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if recovered.artifact(&next.id).await.unwrap().durability == wire::Durability::Durable { break; }
            assert!(Instant::now() < deadline, "periodic synchronization did not cover buffered acceptance");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(recovered); drop(restarted);
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn tracing_layer_preserves_types_span_updates_and_durable_drop_accounting() {
    use sparrow_client::FieldValue;
    use tracing_subscriber::prelude::*;
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let store_path = dir.path().join("store");
            let socket = store_path.join("rpc.sock");
            let config = write_server_config(dir.path(), &store_path, &socket);
            let daemon = start(&config, &socket).await;
            let backend = connect(&socket).await.unwrap();
            let proxy_socket = dir.path().join("relay.sock");
            let (state, proxy) = relay(&proxy_socket, backend.client.clone());
            state.drop_first.set(false);
            let mut config = sparrow_client::DeliveryConfig::new(&proxy_socket, "tracing-layer");
            config.flush_interval = Duration::from_secs(60);
            let delivery = sparrow_client::Delivery::start(config).unwrap();
            let subscriber = tracing_subscriber::registry()
                .with(sparrow_client::TracingLayer::new(delivery.clone()));
            // Separate callback invocations to exercise capture rather than intentionally
            // contending the worker queue. The fence still audits every refused offer.
            let settle = || std::thread::sleep(Duration::from_millis(10));
            settle();
            tracing::subscriber::with_default(subscriber, || {
                let root = tracing::info_span!(parent: None, "root", version = 1_u64);
                settle();
                let entered = root.enter();
                settle();
                let child = tracing::info_span!("child", work = "before");
                settle();
                child.follows_from(&root);
                settle();
                let child_entered = child.enter();
                settle();
                child.record("work", "after");
                settle();
                let error = std::io::Error::other("captured error");
                let error: &(dyn std::error::Error + 'static) = &error;
                let binary: &[u8] = &[0, 255];
                for _ in 0..3 {
                    tracing::warn!(target: "typed.capture", signed = -4_i64, unsigned = u64::MAX,
                    wide_signed = i128::MIN, wide_unsigned = u128::MAX, flag = true,
                    float = f64::from_bits(0x7ff8000000000042), text = "native text",
                    binary, error, debug = ?vec![1, 2], "typed tracing observation");
                    settle();
                }
                tracing::info!(parent: None, explicit_root = true);
                settle();
                drop(child_entered);
                settle();
                drop(child);
                settle();
                drop(entered);
                settle();
                drop(root);
                settle();
            });
            let watermark =
                reached_delivery(delivery_barrier(&delivery, true, Duration::from_secs(5)).await);
            let stats = delivery.stats().unwrap();
            assert_eq!(
                stats.offered_events,
                watermark.durable_events + watermark.dropped_before_fence
            );
            {
                let calls = state.calls.borrow();
                let events: Vec<_> = calls.iter().flat_map(|batch| &batch.events).collect();
                assert_eq!(events.len() as u64, watermark.durable_events);
                let typed = events
                    .iter()
                    .find(|event| event.target == "typed.capture")
                    .expect("captured typed event");
                assert_eq!(typed.kind, wire::EventKind::Log);
                assert_eq!(typed.level, wire::EventLevel::Warn);
                assert!(typed.event_time > 0);
                let value = |name: &str| {
                    &typed
                        .fields
                        .iter()
                        .find(|field| field.name == name)
                        .unwrap()
                        .value
                };
                assert_eq!(value("signed"), &FieldValue::Signed(-4));
                assert_eq!(value("unsigned"), &FieldValue::Unsigned(u64::MAX));
                assert_eq!(value("wide_signed"), &FieldValue::Signed128(i128::MIN));
                assert_eq!(value("wide_unsigned"), &FieldValue::Unsigned128(u128::MAX));
                assert_eq!(value("flag"), &FieldValue::Boolean(true));
                assert_eq!(value("float"), &FieldValue::Float64Bits(0x7ff8000000000042));
                assert_eq!(value("text"), &FieldValue::Text("native text".into()));
                assert_eq!(value("binary"), &FieldValue::Bytes(vec![0, 255]));
                assert_eq!(value("error"), &FieldValue::Error("captured error".into()));
                assert_eq!(value("debug"), &FieldValue::Debug("[1, 2]".into()));
                assert_eq!(typed.context.len(), 2);
                assert_eq!(typed.context[0].name, "root");
                assert_eq!(typed.context[1].name, "child");
                assert_eq!(typed.context[1].parent, Some(typed.context[0].id));
                assert_eq!(typed.span, Some(typed.context[1].id));
                assert_eq!(
                    typed.context[1].fields[0].value,
                    FieldValue::Text("after".into())
                );
                let root_event = events
                    .iter()
                    .find(|event| {
                        event
                            .fields
                            .iter()
                            .any(|field| field.name == "explicit_root")
                    })
                    .unwrap();
                assert!(root_event.context.is_empty());
                assert!(root_event.span.is_none());
                for kind in [
                    wire::EventKind::SpanNew,
                    wire::EventKind::SpanRecord,
                    wire::EventKind::SpanEnter,
                    wire::EventKind::SpanExit,
                    wire::EventKind::SpanClose,
                    wire::EventKind::SpanFollowsFrom,
                ] {
                    assert!(
                        events.iter().any(|event| event.kind == kind),
                        "missing lifecycle {kind:?}"
                    );
                }
            }
            let response = backend
                .client
                .status_request()
                .send()
                .promise
                .await
                .unwrap();
            assert_eq!(
                rpc_ok(response.get().unwrap().get_result().unwrap()).get_live_records(),
                watermark.durable_events
            );
            proxy.abort();
            assert!(proxy.await.unwrap_err().is_cancelled());
            drop(backend);
            drop(daemon);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn producer_delivery_recovers_a_lost_durable_reply_with_the_same_batch() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let store_path = dir.path().join("store");
            let socket = store_path.join("rpc.sock");
            let config = write_server_config(dir.path(), &store_path, &socket);
            let daemon = start(&config, &socket).await;
            let backend = connect(&socket).await.unwrap();
            let proxy_socket = dir.path().join("relay.sock");
            let (state, proxy) = relay(&proxy_socket, backend.client.clone());
            let mut config =
                sparrow_client::DeliveryConfig::new(&proxy_socket, "delivery-lost-reply");
            config.batch_events = 1;
            config.rpc_timeout = Duration::from_millis(150);
            config.retry_interval = Duration::from_millis(20);
            let delivery = sparrow_client::Delivery::start(config).unwrap();
            let sequence = admit(&delivery, delivery_event("lost reply recovery")).await;
            let result = delivery_barrier(&delivery, true, Duration::from_secs(5)).await;
            let watermark = reached_delivery(result);
            assert_eq!(watermark.required_sequence, sequence);
            assert_eq!(watermark.durable_sequence, sequence);
            assert_eq!(watermark.durable_events, 1);
            assert_eq!(
                watermark.last_receipt.as_ref(),
                state.accepted.borrow().as_ref()
            );
            let stats = delivery.stats().unwrap();
            assert_eq!(stats.pending_events, 0);
            assert_eq!(stats.pending_bytes, 0);
            assert!(!stats.running);
            assert!(stats.retries >= 1);
            assert!(stats.last_transient_failure.is_none());
            {
                let calls = state.calls.borrow();
                assert_eq!(calls.len(), 2);
                assert_eq!(calls[0], calls[1]);
                assert_eq!(calls[0].client_instance, watermark.client_instance);
            }
            let response = backend
                .client
                .status_request()
                .send()
                .promise
                .await
                .unwrap();
            assert_eq!(
                rpc_ok(response.get().unwrap().get_result().unwrap()).get_live_records(),
                1
            );
            let mut timed_config =
                sparrow_client::DeliveryConfig::new(&socket, "delivery-lost-reply");
            assert_eq!(timed_config.flush_interval, Duration::from_secs(2));
            timed_config.flush_interval = Duration::from_millis(50);
            let timed = sparrow_client::Delivery::start(timed_config).unwrap();
            admit(&timed, delivery_event("lost reply recovery")).await;
            let deadline = Instant::now() + Duration::from_secs(3);
            while timed.stats().unwrap().durable_events != 1 {
                assert!(
                    Instant::now() < deadline,
                    "automatic batching did not deliver without a flush"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let new_process =
                reached_delivery(delivery_barrier(&timed, true, Duration::from_secs(2)).await);
            assert_ne!(new_process.client_instance, watermark.client_instance);
            assert_ne!(
                new_process.last_receipt.unwrap().id,
                watermark.last_receipt.unwrap().id
            );
            proxy.abort();
            assert!(proxy.await.unwrap_err().is_cancelled());
            drop(timed);
            drop(delivery);
            drop(backend);
            drop(daemon);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn producer_delivery_bounds_outage_backlog_and_reports_drops_after_recovery() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let store_path = dir.path().join("store");
            let socket = store_path.join("rpc.sock");
            let config = write_server_config(dir.path(), &store_path, &socket);
            let mut delivery_config =
                sparrow_client::DeliveryConfig::new(&socket, "delivery-outage");
            delivery_config.max_queued_events = 2;
            delivery_config.batch_events = 1;
            delivery_config.retry_interval = Duration::from_millis(25);
            let delivery = sparrow_client::Delivery::start(delivery_config).unwrap();
            admit(&delivery, delivery_event("before service start one")).await;
            admit(&delivery, delivery_event("before service start two")).await;
            assert!(matches!(
                delivery.try_send(delivery_event("overflow")),
                sparrow_client::Admission::Dropped { .. }
            ));
            let deadline = Instant::now() + Duration::from_secs(2);
            while delivery.pending_batch().unwrap().is_none() {
                assert!(Instant::now() < deadline);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let pending = delivery.pending_batch().unwrap().unwrap();
            let stats = delivery.stats().unwrap();
            assert_eq!(stats.pending_events, 2);
            assert_eq!(stats.in_flight_events, 1);
            assert!(stats.pending_bytes <= 4 * 1024 * 1024);
            assert!(matches!(
                delivery_barrier(&delivery, false, Duration::from_millis(40)).await,
                Err(sparrow_client::FlushError::TimedOut(_))
            ));
            assert_eq!(delivery.pending_batch().unwrap().unwrap(), pending);
            let daemon = start(&config, &socket).await;
            let result = delivery_barrier(&delivery, true, Duration::from_secs(5)).await;
            let watermark = match result {
                Err(sparrow_client::FlushError::Dropped(value)) => *value,
                result => panic!("outage drops must remain explicit: {result:?}"),
            };
            assert_eq!(watermark.durable_events, 2);
            assert!(watermark.dropped_before_fence >= 1);
            assert!(!delivery.stats().unwrap().running);
            let backend = sparrow_client::Client::connect(&socket).await.unwrap();
            let recovered = backend.events(&pending).await.unwrap();
            assert_eq!(recovered.record_ids.len(), 1);
            let response = backend
                .capability()
                .status_request()
                .send()
                .promise
                .await
                .unwrap();
            assert_eq!(
                rpc_ok(response.get().unwrap().get_result().unwrap()).get_live_records(),
                2
            );
            assert!(matches!(
                delivery.try_send(delivery_event("after shutdown")),
                sparrow_client::Admission::Dropped {
                    reason: sparrow_client::DropReason::Closed,
                    ..
                }
            ));
            drop(backend);
            drop(delivery);
            drop(daemon);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn producer_delivery_refuses_store_replacement_and_retains_unknown_pending_input() {
    tokio::task::LocalSet::new().run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first"); let second = dir.path().join("second");
        std::fs::create_dir(&first).unwrap(); std::fs::create_dir(&second).unwrap();
        let first_store = first.join("store"); let first_socket = first_store.join("rpc.sock");
        let second_store = second.join("store"); let second_socket = second_store.join("rpc.sock");
        let first_config = write_server_config(&first, &first_store, &first_socket);
        let second_config = write_server_config(&second, &second_store, &second_socket);
        let first_daemon = start(&first_config, &first_socket).await; let second_daemon = start(&second_config, &second_socket).await;
        let first_backend = connect(&first_socket).await.unwrap(); let second_backend = connect(&second_socket).await.unwrap();
        let proxy_socket = dir.path().join("relay.sock");
        let (state, proxy) = relay(&proxy_socket, first_backend.client.clone());
        let mut config = sparrow_client::DeliveryConfig::new(&proxy_socket, "store-bound-delivery");
        config.batch_events = 1; config.rpc_timeout = Duration::from_millis(150); config.retry_interval = Duration::from_millis(100);
        let delivery = sparrow_client::Delivery::start(config).unwrap();
        admit(&delivery, delivery_event("unknown accepted delivery")).await;
        tokio::time::timeout(Duration::from_secs(3), state.first_accepted.notified()).await.unwrap();
        *state.backend.borrow_mut() = second_backend.client.clone();
        let result = delivery_barrier(&delivery, true, Duration::from_secs(5)).await;
        match result { Err(sparrow_client::FlushError::Delivery(error)) => assert!(matches!(error.as_ref(), sparrow_client::Error::Domain { code, retryable: false, .. } if code == "STORE_MISMATCH")),
            result => panic!("replacement store must refuse pending delivery: {result:?}") };
        let pending = delivery.pending_batch().unwrap().unwrap(); assert_eq!(pending.as_ref(), &state.calls.borrow()[0]);
        assert_eq!(delivery.stats().unwrap().durable_events, 0);
        let response = second_backend.client.status_request().send().promise.await.unwrap();
        assert_eq!(rpc_ok(response.get().unwrap().get_result().unwrap()).get_live_records(), 0);
        let original = state.accepted.borrow().clone().unwrap();
        let first_client = sparrow_client::Client::connect(&first_socket).await.unwrap();
        assert_eq!(first_client.events(&pending).await.unwrap(), original);
        assert!(matches!(delivery.try_send(delivery_event("after terminal failure")), sparrow_client::Admission::Dropped { reason: sparrow_client::DropReason::Failed, .. }));
        proxy.abort(); assert!(proxy.await.unwrap_err().is_cancelled());
        drop(delivery); drop(first_client); drop(first_backend); drop(second_backend); drop(first_daemon); drop(second_daemon);
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn killed_owner_restarts_without_socket_unlink_and_preserves_durable_evidence() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let (address, reservation) = configure_tcp_listener(&config);
            drop(reservation);
            let daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect_tcp(address).await.unwrap();
            let receipt = client
                .ingest(
                    "socket-restart",
                    "restart",
                    wire::Format::Jsonl,
                    b"{\"message\":\"first\"}\n{\"message\":\"second\"}\n",
                )
                .await
                .unwrap();
            let before = cli(&socket, &["records", "--source", "restart"]);
            let held = cli(
                &socket,
                &["records", "--source", "restart", "--page-size", "1"],
            );
            let cursor = held["page"]["next_cursor"].as_str().unwrap().to_owned();
            drop(client);
            drop(daemon); // SIGKILL and wait; deliberately leave the original socket name.
            assert!(socket.exists());
            let mut daemon = start(&config, &socket).await;
            let after = cli(&socket, &["status"]);
            assert_eq!(after["store_id"].as_str(), Some(receipt.store_id.as_str()));
            assert_eq!(after["live_records"].as_str(), Some("2"));
            assert_eq!(after["record_read_handles"].as_str(), Some("0"));
            assert_eq!(after["record_read_bytes"].as_str(), Some("0"));
            assert_eq!(
                cli(&socket, &["records", "--source", "restart"])["rows"],
                before["rows"]
            );
            let tcp = sparrow_client::Client::connect_tcp(address).await.unwrap();
            assert_eq!(tcp.discovery().store_id, receipt.store_id);
            let old = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                .arg(&socket)
                .args(["result-next", &receipt.store_id, &cursor])
                .admitted_output()
                .unwrap();
            assert!(!old.status.success() && old.stdout.is_empty());
            assert!(
                String::from_utf8(old.stderr)
                    .unwrap()
                    .contains("RESULT_EXPIRED")
            );
            drop(tcp);
            rustix::process::kill_process(
                rustix::process::Pid::from_raw(daemon.0.id() as i32).unwrap(),
                rustix::process::Signal::INT,
            )
            .unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                if let Some(status) = daemon.0.try_wait().unwrap() {
                    assert!(status.success());
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "recovered daemon shutdown stalled"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert!(!socket.exists());
        })
        .await;
}

#[test]
fn endpoint_startup_preserves_files_symlinks_and_foreign_live_listeners() {
    use std::os::unix::fs::MetadataExt;
    for kind in ["file", "symlink", "listener"] {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        std::fs::create_dir(&data).unwrap();
        let socket = data.join("rpc.sock");
        let config = write_server_config(dir.path(), &data, &socket);
        let target = dir.path().join("preserved");
        std::fs::write(&target, b"keep").unwrap();
        let foreign = match kind {
            "file" => {
                std::fs::write(&socket, b"keep").unwrap();
                None
            }
            "symlink" => {
                std::os::unix::fs::symlink(&target, &socket).unwrap();
                None
            }
            _ => Some(std::os::unix::net::UnixListener::bind(&socket).unwrap()),
        };
        let before = std::fs::symlink_metadata(&socket).unwrap();
        let result = Command::new(env!("CARGO_BIN_EXE_sparrowd"))
            .arg("--config")
            .arg(&config)
            .admitted_output()
            .unwrap();
        assert!(
            !result.status.success(),
            "occupied endpoint was admitted: {kind}"
        );
        let stderr = String::from_utf8(result.stderr).unwrap();
        assert!(
            stderr.contains("preserving it"),
            "unexpected refusal: {stderr}"
        );
        assert!(
            !stderr.contains("pthread lock"),
            "startup left its worker unjoined"
        );
        let after = std::fs::symlink_metadata(&socket).unwrap();
        assert_eq!((after.dev(), after.ino()), (before.dev(), before.ino()));
        assert_eq!(std::fs::read(&target).unwrap(), b"keep");
        if foreign.is_some() {
            assert!(std::os::unix::net::UnixStream::connect(&socket).is_ok());
        } else {
            assert_eq!(std::fs::read(&socket).unwrap(), b"keep");
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn interrupt_during_startup_is_retained_until_owned_teardown() {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("store");
    let socket = data.join("rpc.sock");
    let config = write_server_config(dir.path(), &data, &socket);
    let (address, (reserved, guard)) = configure_tcp_listener(&config);
    drop(reserved);
    drop(guard);
    let mut daemon = tokio::process::Command::new(env!("CARGO_BIN_EXE_sparrowd"))
        .arg("--config")
        .arg(&config)
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .admitted_spawn()
        .unwrap();
    let pid = rustix::process::Pid::from_raw(daemon.id().unwrap() as i32).unwrap();
    let mut stderr = tokio::io::BufReader::new(daemon.stderr.take().unwrap());
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(30), stderr.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    // This actual startup announcement precedes opening the native store and workers.
    assert_eq!(
        line.trim(),
        format!("sparrowd: capnp_tcp=0.0.0.0:{}", address.port())
    );
    rustix::process::kill_process(pid, rustix::process::Signal::INT).unwrap();
    let status = tokio::time::timeout(Duration::from_secs(30), daemon.wait())
        .await
        .unwrap()
        .unwrap();
    stderr.read_to_string(&mut line).await.unwrap();
    assert!(
        status.success(),
        "interrupt bypassed teardown: {status}: {line}"
    );
    assert!(
        line.contains("sparrowd: store="),
        "startup did not finish: {line}"
    );
    assert!(!socket.exists(), "owned endpoint survived interrupt");
    assert!(std::net::TcpStream::connect(address).is_err());
    // Graceful startup interruption leaves a recoverable initialized store.
    tokio::task::LocalSet::new()
        .run_until(async {
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let receipt = client
                .ingest(
                    "interrupt-reopen",
                    "source",
                    wire::Format::Log,
                    b"recovered\n",
                )
                .await
                .unwrap();
            assert_eq!(
                client.bytes(&receipt.id, 0, 10).await.unwrap(),
                b"recovered\n"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_syslog_bind_refusal_cleans_owned_endpoint_and_allows_retry() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let (address, (occupied, guard)) = configure_tcp_listener(&config);
            drop(guard);
            let text = std::fs::read_to_string(&config).unwrap();
            let text = text
                .lines()
                .filter(|line| !line.starts_with("rpc_tcp_address ="))
                .collect::<Vec<_>>()
                .join("\n");
            std::fs::write(&config, text).unwrap();
            append_syslog_config(
                &config,
                "occupied",
                &address.to_string(),
                "syslog-fixture",
                "tcp",
                Some("lf"),
            );
            let result = Command::new(env!("CARGO_BIN_EXE_sparrowd"))
                .arg("--config")
                .arg(&config)
                .admitted_output()
                .unwrap();
            assert!(!result.status.success());
            let stderr = String::from_utf8(result.stderr).unwrap();
            assert!(
                stderr.contains("Address already in use"),
                "unexpected refusal: {stderr}"
            );
            assert!(
                !stderr.contains("pthread lock"),
                "unjoined startup worker: {stderr}"
            );
            assert!(
                !socket.exists(),
                "startup refusal left its owned socket behind"
            );
            assert!(std::net::TcpStream::connect(address).is_ok());
            drop(occupied);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let receipt = client
                .ingest("startup-retry", "source", wire::Format::Log, b"recovered\n")
                .await
                .unwrap();
            assert_eq!(receipt.record_ids.len(), 1);
            assert_eq!(
                cli(&socket, &["status"])["syslog_listeners"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_terminal_worker_failure_preserves_primary_and_replacement_errors() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("input");
            std::fs::create_dir(&root).unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            append_consume_config(&config, &root, 1000, 8, 8);
            append_syslog_config(
                &config,
                "active",
                "127.0.0.1:0",
                "syslog-fixture",
                "tcp",
                Some("lf"),
            );
            let mut daemon = start_with_stderr(&config, &socket, Stdio::piped()).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let receipt = client
                .ingest(
                    "terminal-cleanup",
                    "source",
                    wire::Format::Log,
                    b"durable\n",
                )
                .await
                .unwrap();
            drop(client);
            let old_socket = data.join("original.sock");
            std::fs::rename(&socket, &old_socket).unwrap();
            std::fs::write(&socket, b"preserve replacement").unwrap();
            let old_root = dir.path().join("original-input");
            std::fs::rename(&root, &old_root).unwrap();
            std::fs::create_dir(&root).unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            let status = loop {
                if let Some(status) = daemon.0.try_wait().unwrap() {
                    break status;
                }
                assert!(
                    Instant::now() < deadline,
                    "terminal worker shutdown stalled"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            assert!(!status.success());
            let mut stderr = String::new();
            std::io::Read::read_to_string(&mut daemon.0.stderr.take().unwrap(), &mut stderr)
                .unwrap();
            assert!(
                stderr.contains("root changed"),
                "primary failure was lost: {stderr}"
            );
            assert!(
                stderr.contains("local endpoint cleanup") && stderr.contains("preserving it"),
                "cleanup failure was lost: {stderr}"
            );
            assert!(!stderr.contains("pthread lock"));
            assert_eq!(std::fs::read(&socket).unwrap(), b"preserve replacement");
            std::fs::remove_dir(&root).unwrap();
            std::fs::rename(&old_root, &root).unwrap();
            std::fs::remove_file(&socket).unwrap();
            std::fs::rename(&old_socket, &socket).unwrap();
            let _reopened = start(&config, &socket).await;
            assert_eq!(
                cli(&socket, &["status"])["live_records"].as_str(),
                Some("1")
            );
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            assert_eq!(
                client
                    .ingest(
                        "terminal-cleanup",
                        "source",
                        wire::Format::Log,
                        b"durable\n"
                    )
                    .await
                    .unwrap(),
                receipt
            );
        })
        .await;
}

fn append_syslog_config(
    config: &Path,
    id: &str,
    address: &str,
    source: &str,
    transport: &str,
    framing: Option<&str>,
) {
    let mut text = std::fs::read_to_string(config).unwrap();
    text.push_str(&format!("\n[[syslog]]\nid = \"{id}\"\naddress = \"{address}\"\nsource = \"{source}\"\ntransport = \"{transport}\"\nmax_message_bytes = 480\nmax_connections = 1\nread_timeout_ms = 1000\n"));
    if let Some(framing) = framing {
        text.push_str(&format!("framing = \"{framing}\"\n"));
    }
    std::fs::write(config, text).unwrap();
}

async fn start(config: &Path, socket: &Path) -> Daemon {
    start_with_stderr(config, socket, Stdio::inherit()).await
}

async fn start_with_stderr(config: &Path, socket: &Path, stderr: Stdio) -> Daemon {
    let mut daemon = Daemon(
        Command::new(env!("CARGO_BIN_EXE_sparrowd"))
            .arg("--config")
            .arg(config)
            .stderr(stderr)
            .stdout(Stdio::null())
            .admitted_spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while tokio::net::UnixStream::connect(socket).await.is_err() {
        assert!(
            daemon.0.try_wait().unwrap().is_none(),
            "daemon exited before readiness"
        );
        assert!(Instant::now() < deadline, "daemon startup timed out");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    daemon
}

fn rpc_ok<'a, T: capnp::traits::Owned + 'static>(
    result: wire::result::Reader<'a, T>,
) -> T::Reader<'a> {
    match result.which().unwrap() {
        wire::result::Ok(value) => value.unwrap(),
        wire::result::Error(error) => panic!("RPC failure: {:?}", error.unwrap().get_message()),
    }
}

fn upload_ref(mut out: wire::import_ref::Builder<'_>, store: &str, id: &str) {
    out.set_store_id(store);
    out.set_id(id);
}

async fn agent_import_checkpoint(
    endpoint: impl AsRef<std::ffi::OsStr>,
    client: &wire::sparrow::Client,
    store: &str,
    id: &str,
) -> sparrow_client::ImportProgress {
    use purrdf_lex::json::Object;
    let progress = sparrow_client::get_import_progress(client, store, id)
        .await
        .unwrap();
    let observed = agent_ingestion_status(
        endpoint.as_ref(),
        client,
        store,
        &sparrow_client::IngestSelector::Import(id.into()),
    )
    .await;
    let requested = agent_ingestion_status(
        endpoint.as_ref(),
        client,
        store,
        &sparrow_client::IngestSelector::Request(progress.request_id.clone()),
    )
    .await;
    assert_eq!(requested["staging"], observed["staging"]);
    assert_eq!(requested["accepted"], observed["accepted"]);
    assert_eq!(
        observed["staging"]["received_bytes"].as_str(),
        Some(progress.received_bytes.to_string().as_str())
    );
    assert_eq!(
        observed["request_id"].as_str(),
        Some(progress.request_id.as_str())
    );
    assert_eq!(
        observed["staging"]["published_artifact"].as_str(),
        progress.published_artifact.as_deref()
    );
    let mut mcp = Mcp::start(endpoint);
    mcp.initialize(1).await;
    let args = Object::new().with("store_id", store).with("import_id", id);
    let certified = sparrow_client::get_ingest_status(
        client,
        store,
        &sparrow_client::IngestSelector::Import(id.into()),
        Default::default(),
    )
    .await
    .unwrap();
    let status = certified.reader().unwrap();
    assert_eq!(
        status.get_request_id().unwrap().to_str().unwrap(),
        progress.request_id
    );
    assert_eq!(
        status.get_deletion().unwrap(),
        wire::IngestDeletion::NotManaged
    );
    assert_eq!(observed["deletion"].as_str(), Some("not_managed"));
    let invalid = mcp
        .tool(3, "ingest_get", args.with("advance", true).into())
        .await;
    assert_eq!(
        invalid["structuredContent"]["error"]["code"].as_str(),
        Some("INVALID_ARGUMENT")
    );
    let wrong = mcp
        .tool(
            4,
            "ingest_get",
            Object::new()
                .with("store_id", "store:other")
                .with("import_id", id)
                .into(),
        )
        .await;
    assert_eq!(
        wrong["structuredContent"]["error"]["code"].as_str(),
        Some("STORE_MISMATCH")
    );
    let missing = mcp
        .tool(
            5,
            "ingest_get",
            Object::new()
                .with("store_id", store)
                .with(
                    "import_id",
                    "import:0000000000000000000000000000000000000000000000000000000000000000",
                )
                .into(),
        )
        .await;
    assert_eq!(
        missing["structuredContent"]["error"]["code"].as_str(),
        Some("NOT_FOUND")
    );
    let tools = mcp.call(6, "tools/list", Object::new().into()).await;
    let declaration = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"].as_str() == Some("ingest_get"))
        .unwrap();
    assert_eq!(
        declaration["inputSchema"]["oneOf"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        declaration["inputSchema"]["required"],
        purrdf_lex::json::Value::array(["store_id"])
    );
    assert_eq!(
        declaration["annotations"]["readOnlyHint"].as_bool(),
        Some(true)
    );
    mcp.close(true).await;
    assert_eq!(
        sparrow_client::get_import_progress(client, store, id)
            .await
            .unwrap(),
        progress
    );
    progress
}

async fn agent_ingestion_status(
    endpoint: impl AsRef<std::ffi::OsStr>,
    client: &wire::sparrow::Client,
    store: &str,
    selector: &sparrow_client::IngestSelector,
) -> purrdf_lex::json::Value {
    use purrdf_lex::json::{Object, Value};
    let (flag, field, id) = match selector {
        sparrow_client::IngestSelector::Import(id) => ("--import-id", "import_id", id),
        sparrow_client::IngestSelector::Request(id) => ("--request-id", "request_id", id),
        sparrow_client::IngestSelector::Handoff(id) => {
            ("--handoff-request-id", "handoff_request_id", id)
        }
    };
    let certified = sparrow_client::get_ingest_status(client, store, selector, Default::default())
        .await
        .unwrap();
    let native = certified.reader().unwrap();
    let mut obsolete = client.ingest_status_request();
    let mut query = obsolete.get().init_query();
    query.set_store_id(store);
    query.set_schema_version(sparrow_client::SCHEMA_VERSION - 1);
    selector.encode(query.init_selector()).unwrap();
    let rejected = obsolete.send().promise.await.unwrap();
    assert!(
        matches!(rejected.get().unwrap().get_result().unwrap().which().unwrap(), wire::result::Error(error) if error.as_ref().unwrap().get_code().unwrap().to_str().unwrap() == "UNSUPPORTED")
    );
    let observed = cli(endpoint.as_ref(), &["ingest-get", store, flag, id]);
    assert_eq!(
        observed["request_id"].as_str(),
        Some(native.get_request_id().unwrap().to_str().unwrap())
    );
    assert_eq!(
        observed["import_id"].as_str(),
        match native.get_import_id().unwrap().which().unwrap() {
            wire::optional_text::Value(id) => Some(id.unwrap().to_str().unwrap()),
            wire::optional_text::Absent(()) => None,
        }
    );
    assert_eq!(
        observed["inspected_records"].as_str(),
        Some(native.get_inspected_records().to_string().as_str())
    );
    let mut mcp = Mcp::start(endpoint);
    mcp.initialize(1).await;
    let result = mcp
        .tool(
            2,
            "ingest_get",
            Object::new()
                .with("store_id", store)
                .with(field, id.clone())
                .into(),
        )
        .await;
    assert_eq!(result["isError"].as_bool(), Some(false));
    let mut actual = result["structuredContent"].clone();
    let mut expected = observed.clone();
    for value in [&mut actual, &mut expected] {
        let Value::Object(object) = value else {
            panic!("status object");
        };
        assert!(object.remove("as_of").is_some());
    }
    assert_eq!(actual, expected);
    let invalid = mcp
        .tool(
            3,
            "ingest_get",
            Object::new()
                .with("store_id", store)
                .with("request_id", "ambiguous")
                .with(
                    "import_id",
                    "import:0000000000000000000000000000000000000000000000000000000000000000",
                )
                .into(),
        )
        .await;
    assert_eq!(
        invalid["structuredContent"]["error"]["code"].as_str(),
        Some("INVALID_ARGUMENT")
    );
    mcp.close(true).await;
    observed
}

#[tokio::test(flavor = "current_thread")]
async fn typed_upload_resumes_chunks_and_native_preparation_after_daemon_crashes() {
    tokio::task::LocalSet::new().run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let store_path = dir.path().join("store");
        let socket = store_path.join("rpc.sock");
        let config = write_server_config(dir.path(), &store_path, &socket);
        let (address, reservation) = configure_tcp_listener(&config);
        let tcp = format!("tcp://{address}");
        drop(reservation);
        let daemon = start(&config, &socket).await;
        let connection = connect(&socket).await.unwrap();
        let client = &connection.client;
        let response = client.status_request().send().promise.await.unwrap();
        let status = rpc_ok(response.get().unwrap().get_result().unwrap());
        assert_eq!(status.get_schema_version(), sparrow_client::SCHEMA_VERSION);
        let store = status.get_store_id().unwrap().to_str().unwrap().to_owned();
        let mut input = vec![b'a'; sparrow_core::IMPORT_CHUNK_BYTES + 16 * 1024 * 1024 + 1];
        input[sparrow_core::IMPORT_CHUNK_BYTES - 1] = b'\n';
        *input.last_mut().unwrap() = b'\n';
        input.extend_from_slice(b"last\n");
        let payload = format!("blake3:{}", purrdf_hash::hex::encode(purrdf_hash::blake3::hash(&input).as_bytes()));
        let mut request = client.import_begin_request();
        let mut begin = request.get().init_request();
        begin.set_request_id("crash-upload"); begin.set_source("streamed"); begin.set_format(wire::Format::Log);
        begin.set_byte_length(input.len() as u64); begin.set_payload_id(&payload);
        let response = request.send().promise.await.unwrap();
        let upload = rpc_ok(response.get().unwrap().get_result().unwrap());
        let progress = match upload.which().unwrap() {
            wire::import_start::Staging(value) => value.unwrap(),
            wire::import_start::Accepted(_) => panic!("new input already accepted"),
        };
        let id = progress.get_id().unwrap().to_str().unwrap().to_owned();
        assert_eq!(progress.get_received_bytes(), 0);
        let initial = agent_import_checkpoint(&tcp, client, &store, &id).await;
        assert_eq!(initial.received_bytes, 0);
        assert!(!initial.sealed && !initial.layout_complete);
        assert!(initial.published_artifact.is_none());
        let mut request = client.import_append_request();
        let mut chunk = request.get().init_request();
        upload_ref(chunk.reborrow().init_import(), &store, &id);
        chunk.set_bytes(&input[..sparrow_core::IMPORT_CHUNK_BYTES]);
        let response = request.send().promise.await.unwrap();
        assert_eq!(rpc_ok(response.get().unwrap().get_result().unwrap()).get_received_bytes(), sparrow_core::IMPORT_CHUNK_BYTES as u64);
        let prefix = agent_import_checkpoint(&socket, client, &store, &id).await;
        assert_eq!(prefix.received_bytes, sparrow_core::IMPORT_CHUNK_BYTES as u64);
        assert_eq!(prefix.expires_at, initial.expires_at);
        drop(connection);
        drop(daemon);
        std::fs::remove_file(&socket).unwrap();
        let daemon = start(&config, &socket).await;
        let connection = connect(&socket).await.unwrap();
        let client = &connection.client;
        let mut request = client.import_get_request();
        upload_ref(request.get().init_import(), &store, &id);
        let response = request.send().promise.await.unwrap();
        assert_eq!(rpc_ok(response.get().unwrap().get_result().unwrap()).get_received_bytes(), sparrow_core::IMPORT_CHUNK_BYTES as u64);
        assert_eq!(agent_import_checkpoint(&tcp, client, &store, &id).await, prefix);
        // Wrong-store and oversized payloads fail before private mutation.
        let mut request = client.import_append_request();
        let mut chunk = request.get().init_request();
        upload_ref(chunk.reborrow().init_import(), "store:other", &id);
        chunk.set_bytes(b"wrong");
        let response = request.send().promise.await.unwrap();
        assert!(matches!(response.get().unwrap().get_result().unwrap().which().unwrap(), wire::result::Error(_)));
        let mut request = client.import_append_request();
        let mut chunk = request.get().init_request();
        upload_ref(chunk.reborrow().init_import(), &store, &id);
        chunk.set_bytes(&input[..sparrow_core::IMPORT_CHUNK_BYTES + 1]);
        let response = request.send().promise.await.unwrap();
        assert!(matches!(response.get().unwrap().get_result().unwrap().which().unwrap(), wire::result::Error(_)));
        for offset in (sparrow_core::IMPORT_CHUNK_BYTES..input.len()).step_by(sparrow_core::IMPORT_CHUNK_BYTES) {
            let mut request = client.import_append_request();
            let mut chunk = request.get().init_request();
            upload_ref(chunk.reborrow().init_import(), &store, &id);
            chunk.set_offset(offset as u64);
            chunk.set_bytes(&input[offset..(offset + sparrow_core::IMPORT_CHUNK_BYTES).min(input.len())]);
            let response = request.send().promise.await.unwrap();
            rpc_ok(response.get().unwrap().get_result().unwrap());
        }
        let mut request = client.import_seal_request();
        upload_ref(request.get().init_import(), &store, &id);
        let response = request.send().promise.await.unwrap();
        assert!(rpc_ok(response.get().unwrap().get_result().unwrap()).get_sealed());
        let mut request = client.import_layout_request();
        upload_ref(request.get().init_import(), &store, &id);
        let response = request.send().promise.await.unwrap();
        assert_eq!(rpc_ok(response.get().unwrap().get_result().unwrap()).get_layout_records(), 3);
        let laid_out = agent_import_checkpoint(&tcp, client, &store, &id).await;
        assert_eq!(laid_out.received_bytes, input.len() as u64);
        assert!(laid_out.sealed && laid_out.layout_complete);
        assert_eq!(laid_out.layout_records, 3);
        assert!(laid_out.published_artifact.is_none());
        assert_eq!(laid_out.expires_at, initial.expires_at);
        let mut request = client.import_prepare_request();
        upload_ref(request.get().init_request().init_import(), &store, &id);
        let response = request.send().promise.await.unwrap();
        let preparation = rpc_ok(response.get().unwrap().get_result().unwrap());
        assert_eq!(preparation.get_prepared_records(), 1);
        let reserved = preparation.get_reserved_artifact_id().unwrap().to_str().unwrap().to_owned();
        let mut request = client.artifact_get_request();
        let mut reference = request.get().init_artifact();
        reference.set_store_id(&store); reference.set_id(&reserved);
        let response = request.send().promise.await.unwrap();
        assert!(matches!(response.get().unwrap().get_result().unwrap().which().unwrap(), wire::result::Error(_)));
        let mut request = client.import_prepare_request();
        let mut prepare = request.get().init_request();
        upload_ref(prepare.reborrow().init_import(), &store, &id);
        prepare.set_expected_record(1);
        let response = request.send().promise.await.unwrap();
        let partial = rpc_ok(response.get().unwrap().get_result().unwrap());
        assert_eq!(partial.get_prepared_records(), 1);
        let raw = match partial.get_raw().unwrap().which().unwrap() {
            wire::optional_raw_preparation::Value(value) => value.unwrap(),
            wire::optional_raw_preparation::Absent(()) => panic!("oversized raw record was not checkpointed"),
        };
        assert_eq!(raw.get_prepared_chunks(), 128);
        assert_eq!(raw.get_total_chunks(), 257);
        let raw_record = raw.get_reserved_record_id().unwrap().to_str().unwrap().to_owned();
        drop(connection);
        drop(daemon);
        std::fs::remove_file(&socket).unwrap();
        let daemon = start(&config, &socket).await;
        let copied = dir.path().join("completed.log");
        std::fs::write(&copied, &input).unwrap();
        let producer = sparrow_client::Client::connect(&socket).await.unwrap();
        assert_eq!(producer.discovery().store_id, store);
        let accepted = producer.import_file("crash-upload", "streamed", wire::Format::Log, &copied).await.unwrap();
        let published = agent_import_checkpoint(&tcp, producer.capability(), &store, &id).await;
        assert_eq!(published.published_artifact.as_deref(), Some(accepted.id.as_str()));
        assert_eq!(published.expires_at, initial.expires_at);
        assert_eq!(accepted.id, reserved);
        assert_eq!(accepted.payload_id, payload);
        assert_eq!(accepted.byte_length, input.len() as u64);
        assert_eq!(accepted.record_ids[1], raw_record);
        assert_eq!(accepted.durability, wire::Durability::Durable);
        assert_eq!(producer.artifact(&reserved).await.unwrap(), accepted);
        assert_eq!(producer.import_file("crash-upload", "streamed", wire::Format::Log, &copied).await.unwrap(), accepted);
        let args = ["ingest", "crash-upload", "streamed", "log", copied.to_str().unwrap()];
        let receipt = cli(&socket, &args);
        assert_eq!(receipt["artifact_id"].as_str(), Some(reserved.as_str()));
        assert_eq!(receipt["payload_id"].as_str(), Some(payload.as_str()));
        assert_eq!(receipt["record_ids"].as_array().unwrap().len(), 3);
        assert_eq!(receipt["record_ids"].item(1).unwrap().as_str(), Some(raw_record.as_str()));
        assert_eq!(receipt, cli(&socket, &args));
        let connection = connect(&socket).await.unwrap();
        let client = &connection.client;
        let mut reconstructed = Vec::new();
        for offset in (0..input.len()).step_by(sparrow_core::IMPORT_CHUNK_BYTES) {
            let mut request = client.bytes_read_request();
            let mut read = request.get().init_request();
            let mut evidence = read.reborrow().init_evidence();
            evidence.set_store_id(&store); evidence.set_artifact(&reserved);
            read.set_offset(offset as u64); read.set_length((input.len() - offset).min(sparrow_core::IMPORT_CHUNK_BYTES) as u32);
            let response = request.send().promise.await.unwrap();
            reconstructed.extend_from_slice(rpc_ok(response.get().unwrap().get_result().unwrap()).get_data().unwrap());
        }
        assert_eq!(reconstructed, input);
        assert_eq!(std::fs::read(copied).unwrap(), input);
        let response = client.status_request().send().promise.await.unwrap();
        assert_eq!(rpc_ok(response.get().unwrap().get_result().unwrap()).get_live_records(), 3);
        let direct = producer.ingest("producer-direct", "sdk", wire::Format::Json, b"{\"flag\":true}").await.unwrap();
        assert_eq!(producer.ingest("producer-direct", "sdk", wire::Format::Json, b"{\"flag\":true}").await.unwrap(), direct);
        let conflict = producer.ingest("producer-direct", "sdk", wire::Format::Json, b"{\"flag\":false}").await.unwrap_err();
        assert!(matches!(conflict, sparrow_client::Error::Domain { ref code, retryable: false, .. } if code == "CONFLICT"), "{conflict}");
        assert_eq!(producer.artifact(&direct.id).await.unwrap(), direct);
        drop(producer);
        drop(connection);
        drop(daemon);
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn typed_events_keep_rust_values_context_source_time_and_retry_receipts() {
    tokio::task::LocalSet::new().run_until(async {
        use sparrow_client::{EventBatch, EventField, EventSpan, FieldValue, ProducerEvent};
        let dir = tempfile::tempdir().unwrap();
        let store_path = dir.path().join("store");
        let socket = store_path.join("rpc.sock");
        let config = write_server_config(dir.path(), &store_path, &socket);
        let daemon = start(&config, &socket).await;
        let producer = sparrow_client::Client::connect(&socket).await.unwrap();
        assert_eq!(producer.discovery().max_event_batch_events, 256);
        assert_eq!(producer.discovery().max_event_batch_bytes, 1024 * 1024);
        let fields = vec![
            ("message", FieldValue::Text("allocation failed\n\"quoted\" 😺".into())),
            ("flag", FieldValue::Boolean(true)),
            ("integer", FieldValue::Signed(i64::MIN)),
            ("integer", FieldValue::Unsigned(u64::MAX)),
            ("wide", FieldValue::Signed128(i128::MIN)),
            ("wide", FieldValue::Unsigned128(u128::MAX)),
            ("nan", FieldValue::Float64Bits(0x7ff8_0000_0000_0042)),
            ("zero", FieldValue::Float64Bits((-0.0_f64).to_bits())),
            ("fraction", FieldValue::Float64Bits(0.125_f64.to_bits())),
            ("bytes", FieldValue::Bytes(vec![0, 255, 128])),
            ("debug", FieldValue::Debug("Some(3)".into())),
            ("error", FieldValue::Error("disk failure".into())),
        ].into_iter().map(|(name, value)| EventField { name: name.into(), value }).collect::<Vec<_>>();
        let span = EventSpan { id: 42, parent: None, name: "request".into(), target: "application".into(),
            fields: vec![EventField { name: "request_number".into(), value: FieldValue::Unsigned(7) }] };
        let event = ProducerEvent { sequence: 1, event_time: 1_700_000_000_001, kind: wire::EventKind::SpanNew,
            level: wire::EventLevel::Info, name: "request".into(), target: "application".into(), fields: vec![],
            context: vec![], span: Some(42), follows_from: None };
        let mut log = event.clone();
        log.sequence = 2; log.event_time += 1; log.kind = wire::EventKind::Log;
        log.name = "allocation".into(); log.fields = fields; log.context.push(span.clone());
        let mut close = event.clone();
        close.sequence = 4; close.event_time += 2; close.kind = wire::EventKind::SpanClose; close.context.push(span);
        let batch = EventBatch { request_id: "typed-events-v1".into(), source: "structured-sdk".into(),
            client_instance: [9; 16], events: vec![event, log, close] };
        let mut message = capnp::message::Builder::new_default();
        batch.encode(message.init_root::<wire::event_batch::Builder<'_>>()).unwrap();
        assert_eq!(EventBatch::decode(message.get_root_as_reader().unwrap()).unwrap(), batch);
        let receipt = producer.events(&batch).await.unwrap();
        assert_eq!(receipt.record_ids.len(), 3);
        assert_eq!(receipt.durability, wire::Durability::Durable);
        assert_eq!(producer.events(&batch).await.unwrap(), receipt);
        let mut request = producer.capability().record_get_request();
        let mut reference = request.get().init_record(); reference.set_store_id(&receipt.store_id); reference.set_id(&receipt.record_ids[0]);
        let response = request.send().promise.await.unwrap();
        let record = rpc_ok(response.get().unwrap().get_result().unwrap());
        let position = match record.get_producer().unwrap().which().unwrap() {
            wire::optional_producer_position::Value(value) => value.unwrap(),
            _ => panic!("accepted producer record has no typed stream position"),
        };
        assert_eq!(position.get_sequence(), 1);
        let stream_id = position.get_stream_id().unwrap().to_str().unwrap().to_owned();
        let initial = cli(&socket, &["producer-stream", &receipt.store_id, &stream_id]);
        assert_eq!(initial["accepted_events"].as_str(), Some("3"));
        assert_eq!(initial["unobserved_sequences"].as_str(), Some("1"));
        assert_eq!(initial["gaps"][0]["first"].as_str(), Some("3"));
        assert_eq!(initial["continuity_unknown"].as_bool(), Some(true));
        let mut mcp = Mcp::start(&socket); mcp.initialize(1).await;
        let read = mcp.tool(2, "producer_stream_get", purrdf_lex::json::Object::new()
            .with("store_id", receipt.store_id.clone()).with("stream_id", stream_id.clone()).into()).await;
        assert_eq!(read["structuredContent"], initial);
        let sources = cli(&socket, &["sources", &receipt.store_id, "--source", "structured-sdk"]);
        assert_eq!(sources["rows"][0]["producer_stream_ids"][0].as_str(), Some(stream_id.as_str()));
        let mut next = batch.clone(); next.request_id = "typed-events-next".into();
        next.events = vec![batch.events[1].clone()]; next.events[0].sequence = 8; next.events[0].event_time += 1000;
        let next_receipt = producer.events(&next).await.unwrap();
        assert_eq!(producer.events(&batch).await.unwrap(), receipt);
        let after = cli(&socket, &["producer-stream", &receipt.store_id, &stream_id]);
        assert_eq!(after["accepted_events"].as_str(), Some("4"));
        assert_eq!(after["unobserved_sequences"].as_str(), Some("4"));
        assert_eq!(after["gaps"][1]["first"].as_str(), Some("5"));
        assert_eq!(after["gaps"][1]["last"].as_str(), Some("7"));
        let mut overlap = batch.clone(); overlap.request_id = "typed-events-overlap".into();
        assert!(matches!(producer.events(&overlap).await.unwrap_err(), sparrow_client::Error::Domain { ref code, .. } if code == "CONFLICT"));
        assert_eq!(cli(&socket, &["producer-stream", &receipt.store_id, &stream_id]), after);
        assert_ne!(next_receipt.id, receipt.id);
        let mut read = producer.capability().bytes_read_request();
        let mut input = read.get().init_request();
        input.set_length(receipt.byte_length.try_into().unwrap());
        let mut evidence = input.init_evidence();
        evidence.set_store_id(&receipt.store_id); evidence.set_artifact(&receipt.id);
        let response = read.send().promise.await.unwrap();
        let bytes = rpc_ok(response.get().unwrap().get_result().unwrap()).get_data().unwrap().to_vec();
        let records = bytes.split(|byte| *byte == b'\n').filter(|line| !line.is_empty())
            .map(|line| purrdf_lex::json::read_slice(line, purrdf_lex::json::Limits::default()).unwrap()).collect::<Vec<_>>();
        assert_eq!(records.len(), 3);
        let fields = &records[1]["fields"];
        assert_eq!(fields[0]["value"].as_str(), Some("allocation failed\n\"quoted\" 😺"));
        assert_eq!(fields[1]["value"].as_bool(), Some(true));
        assert_eq!(fields[2]["value"].as_number().unwrap().lexeme(), i64::MIN.to_string());
        assert_eq!(fields[3]["value"].as_number().unwrap().lexeme(), u64::MAX.to_string());
        assert_eq!(fields[4]["value"].as_number().unwrap().lexeme(), i128::MIN.to_string());
        assert_eq!(fields[5]["value"].as_number().unwrap().lexeme(), u128::MAX.to_string());
        assert_eq!(fields[6]["bits"].as_number().unwrap().as_u64(), Some(0x7ff8_0000_0000_0042));
        assert_eq!(fields[6]["nonfinite"].as_str(), Some("nan"));
        assert!(fields[6].get("value").is_none());
        assert_eq!(fields[7]["bits"].as_number().unwrap().as_u64(), Some((-0.0_f64).to_bits()));
        assert_eq!(fields[8]["value"].as_number().unwrap().as_f64(), 0.125);
        assert_eq!(fields[9]["value"].as_str(), Some("00ff80"));
        assert_eq!(fields[10]["kind"].as_str(), Some("debug"));
        assert_eq!(fields[11]["kind"].as_str(), Some("error"));
        assert_eq!(records[1]["context"][0]["id"].as_number().unwrap().as_u64(), Some(42));
        assert_eq!(records[1]["sequence"].as_number().unwrap().as_u64(), Some(2));
        for (index, id) in receipt.record_ids.iter().enumerate() {
            let mut request = producer.capability().record_get_request();
            let mut reference = request.get().init_record(); reference.set_store_id(&receipt.store_id); reference.set_id(id);
            let response = request.send().promise.await.unwrap();
            let record = rpc_ok(response.get().unwrap().get_result().unwrap());
            match record.get_event_time().unwrap().which().unwrap() {
                wire::event_time::Known(value) => {
                    let value = value.unwrap();
                    assert_eq!(value.get_instant().unwrap().get_unix_millis(), batch.events[index].event_time);
                    let span = &bytes[value.get_source_start() as usize..value.get_source_end() as usize];
                    assert_eq!(span, batch.events[index].event_time.to_string().as_bytes());
                },
                wire::event_time::Unknown(_) => panic!("typed producer time was not indexed"),
            }
            assert_eq!(record.get_received_at().unwrap().get_unix_millis(), receipt.received_at);
        }
        let query = cli(&socket, &["records", "--source", "structured-sdk", "--text", "allocation",
            "--event-from", "2023-11-14T22:13:20.002Z", "--event-before", "2023-11-14T22:13:20.003Z"]);
        assert_eq!(query["rows"].as_array().unwrap().len(), 1);
        assert_eq!(query["rows"][0]["record_id"].as_str(), Some(receipt.record_ids[1].as_str()));
        let mut changed = batch.clone(); changed.events[1].fields[6].value = FieldValue::Float64Bits(0x7ff8_0000_0000_0043);
        assert!(matches!(producer.events(&changed).await.unwrap_err(), sparrow_client::Error::Domain { ref code, .. } if code == "CONFLICT"));
        // Generated callers cannot bypass SDK cardinality or relationship admission.
        let mut request = producer.capability().events_ingest_request();
        let mut invalid = request.get().init_batch(); invalid.set_request_id("too-many-events"); invalid.set_source("invalid");
        invalid.set_client_instance(&[9; 16]); invalid.init_events(257);
        let response = request.send().promise.await.unwrap();
        assert!(matches!(response.get().unwrap().get_result().unwrap().which().unwrap(), wire::result::Error(_)));
        let mut request = producer.capability().events_ingest_request();
        let mut invalid = request.get().init_batch();
        invalid.set_request_id("wire-byte-quota"); invalid.set_source("refused-wire-quota"); invalid.set_client_instance(&[9; 16]);
        let mut event = invalid.init_events(1).get(0);
        event.set_sequence(1);
        event.init_fields(1).get(0).set_bytes(&vec![0; 64 * 1024]);
        let response = request.send().promise.await.unwrap();
        assert!(matches!(response.get().unwrap().get_result().unwrap().which().unwrap(), wire::result::Error(_)));
        let mut request = producer.capability().source_time_get_request();
        request.get().set_source("refused-wire-quota");
        let response = request.send().promise.await.unwrap();
        assert!(matches!(rpc_ok(response.get().unwrap().get_result().unwrap()).which().unwrap(), wire::optional_source_time_profile::Absent(())));
        let mut request = producer.capability().source_time_define_request();
        let mut definition = request.get().init_definition();
        definition.set_source("incompatible-events"); definition.set_path("/timestamp"); definition.set_representation(wire::TimeRepresentation::Rfc3339);
        let response = request.send().promise.await.unwrap();
        rpc_ok(response.get().unwrap().get_result().unwrap());
        let mut incompatible = batch.clone(); incompatible.source = "incompatible-events".into(); incompatible.request_id = "incompatible-event-source".into();
        assert!(matches!(producer.events(&incompatible).await.unwrap_err(), sparrow_client::Error::Domain { ref code, .. } if code == "CONFLICT"));
        let mut invalid = batch.clone(); invalid.request_id = "invalid-order".into(); invalid.events[1].sequence = 1;
        assert!(invalid.validate().is_err());
        invalid = batch.clone(); invalid.events[1].fields[0].value = FieldValue::Text("x".repeat(16 * 1024 + 1));
        assert!(invalid.validate().is_err());
        invalid = batch.clone(); invalid.events[1].context[0].parent = Some(42);
        assert!(invalid.validate().is_err());
        drop(producer); drop(daemon); std::fs::remove_file(&socket).unwrap();
        let daemon = start(&config, &socket).await;
        let producer = sparrow_client::Client::connect(&socket).await.unwrap();
        assert_eq!(producer.events(&batch).await.unwrap(), receipt);
        assert_eq!(producer.artifact(&receipt.id).await.unwrap(), receipt);
        let response = producer.capability().status_request().send().promise.await.unwrap();
        assert_eq!(rpc_ok(response.get().unwrap().get_result().unwrap()).get_live_records(), 4);
        drop(producer); drop(daemon);
    }).await;
}

async fn exercise(endpoint: impl AsRef<std::ffi::OsStr>) -> (String, String, String) {
    let endpoint = endpoint.as_ref();
    let target = sparrow_client::Endpoint::parse(endpoint.to_str().unwrap()).unwrap();
    let connection = sparrow_client::Client::connect_endpoint(&target)
        .await
        .unwrap();
    let client = connection.capability();
    let response = client.status_request().send().promise.await.unwrap();
    let status = match response
        .get()
        .unwrap()
        .get_result()
        .unwrap()
        .which()
        .unwrap()
    {
        wire::result::Ok(value) => value.unwrap(),
        wire::result::Error(e) => panic!("status: {:?}", e.unwrap().get_message()),
    };
    let store = status.get_store_id().unwrap().to_str().unwrap().to_owned();
    let bytes = b"{\"x\":1e0,\"x\":1.00,\"message\":\"hello\\u0020world\"}\r\n{invalid}\n";
    let mut request = client.ingest_request();
    let mut ingest = request.get().init_request();
    ingest.set_request_id("rpc-acceptance-v1");
    ingest.set_source("synthetic-rpc");
    ingest.set_format(wire::Format::Jsonl);
    ingest.set_bytes(bytes);
    let response = request.send().promise.await.unwrap();
    let artifact = match response
        .get()
        .unwrap()
        .get_result()
        .unwrap()
        .which()
        .unwrap()
    {
        wire::result::Ok(value) => value.unwrap(),
        wire::result::Error(e) => panic!("ingest: {:?}", e.unwrap().get_message()),
    };
    let id = artifact.get_id().unwrap().to_str().unwrap().to_owned();
    let progress = agent_ingestion_status(
        endpoint,
        client,
        &store,
        &sparrow_client::IngestSelector::Request("rpc-acceptance-v1".into()),
    )
    .await;
    assert!(progress["import_id"].is_null());
    assert!(progress["staging"].is_null());
    assert_eq!(
        progress["accepted"]["artifact_id"].as_str(),
        Some(id.as_str())
    );
    assert_eq!(progress["total_records"].as_str(), Some("2"));
    assert_eq!(progress["json"]["complete"].as_str(), Some("1"));
    assert_eq!(progress["json"]["failed"].as_str(), Some("1"));
    assert_eq!(progress["text"]["incomplete"].as_str(), Some("1"));
    assert_eq!(artifact.get_store_id().unwrap().to_str().unwrap(), store);
    assert_eq!(artifact.get_byte_length(), bytes.len() as u64);
    assert_eq!(
        artifact.get_durability().unwrap(),
        wire::Durability::Durable
    );
    assert_eq!(artifact.get_record_ids().unwrap().len(), 2);
    let first_record = artifact
        .get_record_ids()
        .unwrap()
        .get(0)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let received = artifact.get_received_at().unwrap().get_unix_millis();
    let copied = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(copied.path(), bytes).unwrap();
    let imported = cli(
        endpoint,
        &[
            "ingest",
            "rpc-acceptance-v1",
            "synthetic-rpc",
            "jsonl",
            copied.path().to_str().unwrap(),
        ],
    );
    assert_eq!(
        imported.get("artifact_id").unwrap().as_str(),
        Some(id.as_str())
    );
    assert_eq!(
        imported.get("durability").unwrap().as_str(),
        Some("durable")
    );
    assert_eq!(std::fs::read(copied.path()).unwrap(), bytes);
    copied.as_file().set_len(0).unwrap();
    copied.as_file().set_len(8388609).unwrap();
    let streamed = cli(
        endpoint,
        &[
            "ingest",
            "oversized-cli-v1",
            "streamed-cli",
            "jsonl",
            copied.path().to_str().unwrap(),
        ],
    );
    assert_eq!(streamed["byte_length"].as_str(), Some("8388609"));
    assert_eq!(streamed["durability"].as_str(), Some("durable"));
    let retried = cli(
        endpoint,
        &[
            "ingest",
            "oversized-cli-v1",
            "streamed-cli",
            "jsonl",
            copied.path().to_str().unwrap(),
        ],
    );
    assert_eq!(streamed["artifact_id"], retried["artifact_id"]);
    let exported_dir = tempfile::tempdir().unwrap();
    let exported = exported_dir.path().join("evidence.bin");
    let length = bytes.len().to_string();
    let saved = cli(
        endpoint,
        &[
            "bytes",
            &store,
            &id,
            "0",
            &length,
            exported.to_str().unwrap(),
        ],
    );
    assert_eq!(
        saved.get("byte_length").unwrap().as_str(),
        Some(length.as_str())
    );
    assert_eq!(std::fs::read(&exported).unwrap(), bytes);
    let overwrite = Command::new(env!("CARGO_BIN_EXE_sparrow"))
        .arg(endpoint)
        .args(["bytes", &store, &id, "0", "1", exported.to_str().unwrap()])
        .admitted_output()
        .unwrap();
    assert!(!overwrite.status.success());
    assert_eq!(std::fs::read(&exported).unwrap(), bytes);
    let mut request = client.bytes_read_request();
    let mut read = request.get().init_request();
    read.set_length(bytes.len() as u32);
    let mut evidence = read.init_evidence();
    evidence.set_store_id(&store);
    evidence.set_artifact(&id);
    let response = request.send().promise.await.unwrap();
    match response
        .get()
        .unwrap()
        .get_result()
        .unwrap()
        .which()
        .unwrap()
    {
        wire::result::Ok(value) => assert_eq!(value.unwrap().get_data().unwrap(), bytes),
        wire::result::Error(e) => panic!("bytes: {:?}", e.unwrap().get_message()),
    }
    let mut request = client.keys_list_request();
    let mut scope = request.get().init_scope();
    scope.set_schema_version(sparrow_client::SCHEMA_VERSION);
    scope.reborrow().init_source().set_value("synthetic-rpc");
    scope
        .reborrow()
        .init_received_from()
        .set_unix_millis(received);
    scope.init_received_before().set_unix_millis(received + 1);
    let response = request.send().promise.await.unwrap();
    let keys = match response
        .get()
        .unwrap()
        .get_result()
        .unwrap()
        .which()
        .unwrap()
    {
        wire::result::Ok(value) => value.unwrap(),
        wire::result::Error(e) => panic!("keys: {:?}", e.unwrap().get_message()),
    };
    assert_eq!(
        keys.get_receipt().unwrap().get_certainty().unwrap(),
        wire::Certainty::Incomplete
    );
    assert_eq!(
        keys.get_receipt()
            .unwrap()
            .get_coverage()
            .unwrap()
            .get_failed_json_records(),
        1
    );
    let x = keys
        .get_rows()
        .unwrap()
        .iter()
        .find(|row| row.get_path().unwrap().to_str().unwrap() == "/x")
        .unwrap();
    assert_eq!(x.get_occurrences(), 2);
    assert_eq!(x.get_records(), 1);
    assert_eq!(x.get_kind().unwrap(), wire::JsonKind::Number);
    let text = cli(endpoint, &["text", "hello", "--source", "synthetic-rpc"]);
    assert_eq!(text["rows"].as_array().unwrap().len(), 1);
    assert_eq!(
        text["rows"][0]["record_id"].as_str(),
        Some(first_record.as_str())
    );
    assert_eq!(
        text["text_coverage"]["failed_or_incomplete"].as_str(),
        Some("1")
    );
    assert_eq!(text["receipt"]["certainty"].as_str(), Some("incomplete"));
    assert!(
        text["rows"][0]["evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|span| span["basis"].as_str() == Some("decoded_json_scalar")
                && span["coarse"].as_bool() == Some(true))
    );
    let from = sparrow_core::format_instant(received).unwrap();
    let before = sparrow_core::format_instant(received + 1).unwrap();
    let cli_keys = cli(
        endpoint,
        &[
            "keys",
            "--source",
            "synthetic-rpc",
            "--received-from",
            &from,
            "--received-before",
            &before,
        ],
    );
    let rows = cli_keys.get("rows").unwrap().as_array().unwrap();
    let x = rows
        .iter()
        .find(|row| row.get("path").unwrap().as_str() == Some("/x"))
        .unwrap();
    assert_eq!(x.get("occurrences").unwrap().as_str(), Some("2"));
    assert_eq!(
        cli_keys
            .get("receipt")
            .unwrap()
            .get("certainty")
            .unwrap()
            .as_str(),
        Some("incomplete")
    );
    let cli_values = cli(
        endpoint,
        &[
            "values",
            "--path",
            "/message",
            "--kind",
            "string",
            "--decoded",
            "hello world",
        ],
    );
    assert_eq!(cli_values.get("rows").unwrap().as_array().unwrap().len(), 1);
    assert_eq!(
        cli_values
            .get("rows")
            .unwrap()
            .item(0)
            .unwrap()
            .get("record_id")
            .unwrap()
            .as_str(),
        Some(first_record.as_str())
    );
    let limited = cli(endpoint, &["values", "--limit", "1"]);
    assert_eq!(
        limited
            .get("receipt")
            .unwrap()
            .get("execution")
            .unwrap()
            .as_str(),
        Some("complete")
    );
    assert_eq!(limited["rows"].as_array().unwrap().len(), 1);
    assert_eq!(limited["page"]["has_more"].as_bool(), Some(true));
    assert!(limited["page"]["next_cursor"].as_str().is_some());
    let excluded = cli(endpoint, &["keys", "--source", "unrelated-source"]);
    assert!(excluded.get("rows").unwrap().as_array().unwrap().is_empty());
    let invalid = Command::new(env!("CARGO_BIN_EXE_sparrow"))
        .arg(endpoint)
        .args(["keys", "--decoded", "hello"])
        .admitted_output()
        .unwrap();
    assert!(!invalid.status.success());
    assert!(invalid.stdout.is_empty());
    let mut request = client.values_search_request();
    let mut query = request.get().init_query();
    query.reborrow().init_path().set_value("/message");
    query.reborrow().init_decoded().set_value("hello world");
    let mut scope = query.init_scope();
    scope.set_schema_version(sparrow_client::SCHEMA_VERSION);
    scope.init_received_before().set_unix_millis(u64::MAX);
    let response = request.send().promise.await.unwrap();
    let values = match response
        .get()
        .unwrap()
        .get_result()
        .unwrap()
        .which()
        .unwrap()
    {
        wire::result::Ok(value) => value.unwrap(),
        wire::result::Error(e) => panic!("values: {:?}", e.unwrap().get_message()),
    };
    assert_eq!(values.get_rows().unwrap().len(), 1);
    assert_eq!(
        values
            .get_rows()
            .unwrap()
            .get(0)
            .get_record_id()
            .unwrap()
            .to_str()
            .unwrap(),
        first_record
    );
    let mut request = client.artifact_get_request();
    let mut evidence = request.get().init_artifact();
    evidence.set_store_id("wrong-store");
    evidence.set_id(&id);
    let response = request.send().promise.await.unwrap();
    match response
        .get()
        .unwrap()
        .get_result()
        .unwrap()
        .which()
        .unwrap()
    {
        wire::result::Error(error) => assert_eq!(
            error.unwrap().get_code().unwrap().to_str().unwrap(),
            "STORE_MISMATCH"
        ),
        wire::result::Ok(_) => panic!("accepted evidence from a different store"),
    }
    let mut request = client.pin_request();
    let mut input = request.get().init_request();
    input.set_request_id("rpc-pin-v1");
    input.set_name("case");
    input.set_days(1);
    let mut target = input.init_targets(1).get(0);
    target.set_store_id(&store);
    target.set_record(&first_record);
    let response = request.send().promise.await.unwrap();
    let pin = match response
        .get()
        .unwrap()
        .get_result()
        .unwrap()
        .which()
        .unwrap()
    {
        wire::result::Ok(pin) => pin.unwrap(),
        wire::result::Error(error) => panic!("pin: {:?}", error.unwrap().get_message()),
    };
    let pin_id = pin.get_id().unwrap().to_str().unwrap().to_owned();
    assert_eq!(pin.get_members().unwrap().len(), 1);
    let mut request = client.pin_renew_request();
    let mut input = request.get().init_request();
    input.set_store_id(&store);
    input.set_request_id("rpc-renew-v1");
    input.set_pin_id(&pin_id);
    input.set_days(2);
    input.init_expected_revision().set_value(1);
    let response = request.send().promise.await.unwrap();
    let renewed = match response
        .get()
        .unwrap()
        .get_result()
        .unwrap()
        .which()
        .unwrap()
    {
        wire::result::Ok(value) => value.unwrap(),
        wire::result::Error(error) => panic!("renewal: {:?}", error.unwrap().get_message()),
    };
    assert_eq!(renewed.get_revision(), 2);
    assert_eq!(renewed.get_id().unwrap().to_str().unwrap(), pin_id);
    assert_eq!(
        renewed.get_request_id().unwrap().to_str().unwrap(),
        "rpc-renew-v1"
    );
    assert_eq!(renewed.get_durability().unwrap(), wire::Durability::Durable);
    assert!(
        renewed.get_expires_at().unwrap().get_unix_millis()
            >= pin.get_expires_at().unwrap().get_unix_millis()
    );
    assert_eq!(renewed.get_members().unwrap().len(), 1);
    let cli_receipt = cli(
        endpoint,
        &["pin-renew", "rpc-renew-v1", &store, &pin_id, "2", "1"],
    );
    assert_eq!(cli_receipt["revision"].as_str(), Some("2"));
    assert_eq!(
        cli_receipt["expires_at"].as_str(),
        Some(
            sparrow_core::format_instant(renewed.get_expires_at().unwrap().get_unix_millis())
                .unwrap()
                .as_str()
        )
    );
    let mut previous = None;
    for _ in 0..2 {
        let mut request = client.pin_release_request();
        let mut input = request.get().init_request();
        input.set_store_id(&store);
        input.set_request_id("rpc-release-v1");
        input.set_pin_id(&pin_id);
        let response = request.send().promise.await.unwrap();
        let release = match response
            .get()
            .unwrap()
            .get_result()
            .unwrap()
            .which()
            .unwrap()
        {
            wire::result::Ok(release) => release.unwrap(),
            wire::result::Error(error) => panic!("release: {:?}", error.unwrap().get_message()),
        };
        let released_at = release.get_released_at().unwrap().get_unix_millis();
        if let Some(previous) = previous {
            assert_eq!(released_at, previous);
        }
        previous = Some(released_at);
        assert_eq!(release.get_pin_id().unwrap().to_str().unwrap(), pin_id);
    }
    let mut request = client.pin_request();
    let mut input = request.get().init_request();
    input.set_request_id("rpc-rejected-v1");
    input.set_name("invalid");
    input.set_days(1);
    let mut targets = input.init_targets(2);
    let mut target = targets.reborrow().get(0);
    target.set_store_id(&store);
    target.set_record(&first_record);
    let mut target = targets.get(1);
    target.set_store_id(&store);
    target.set_record("record:unknown");
    let response = request.send().promise.await.unwrap();
    match response
        .get()
        .unwrap()
        .get_result()
        .unwrap()
        .which()
        .unwrap()
    {
        wire::result::Error(error) => assert_eq!(
            error.unwrap().get_code().unwrap().to_str().unwrap(),
            "NOT_FOUND"
        ),
        wire::result::Ok(_) => panic!("published partially valid pin"),
    }
    let response = client.status_request().send().promise.await.unwrap();
    match response
        .get()
        .unwrap()
        .get_result()
        .unwrap()
        .which()
        .unwrap()
    {
        wire::result::Ok(status) => assert_eq!(status.unwrap().get_active_pins(), 0),
        wire::result::Error(error) => panic!(
            "status after rejected pin: {:?}",
            error.unwrap().get_message()
        ),
    }
    mcp_exercise(endpoint, &store, &id, &first_record, bytes).await;
    (store, id, first_record)
}

struct Mcp {
    child: tokio::process::Child,
    input: Option<tokio::process::ChildStdin>,
    output: tokio::io::BufReader<tokio::process::ChildStdout>,
}

impl Mcp {
    fn start(socket: impl AsRef<std::ffi::OsStr>) -> Self {
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_sparrow"))
            .arg(socket)
            .args(["mcp", "--stdio"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .admitted_spawn()
            .unwrap();
        let input = child.stdin.take();
        let output = tokio::io::BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            input,
            output,
        }
    }
    async fn send(&mut self, value: purrdf_lex::json::Value) {
        use tokio::io::AsyncWriteExt;
        let message = format!("{}\n", purrdf_lex::json::write_compact(&value));
        self.input
            .as_mut()
            .unwrap()
            .write_all(message.as_bytes())
            .await
            .unwrap();
    }
    async fn receive(&mut self) -> purrdf_lex::json::Value {
        use tokio::io::AsyncBufReadExt;
        let mut bytes = Vec::new();
        let length = tokio::time::timeout(
            Duration::from_secs(10),
            self.output.read_until(b'\n', &mut bytes),
        )
        .await
        .expect("MCP response timeout")
        .unwrap();
        assert!(length > 0, "MCP closed stdout before responding");
        purrdf_lex::json::read_slice(&bytes, purrdf_lex::json::Limits::default()).unwrap()
    }
    async fn call(
        &mut self,
        id: u64,
        method: &str,
        params: purrdf_lex::json::Value,
    ) -> purrdf_lex::json::Value {
        self.send(
            purrdf_lex::json::Object::new()
                .with("jsonrpc", "2.0")
                .with("id", id)
                .with("method", method)
                .with("params", params)
                .into(),
        )
        .await;
        let response = self.receive().await;
        assert_eq!(response["id"].as_u64(), Some(id));
        response
    }
    async fn close(mut self, success: bool) {
        drop(self.input.take());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), self.child.wait())
                .await
                .unwrap()
                .unwrap()
                .success(),
            success
        );
    }
    async fn initialize(&mut self, id: u64) {
        use purrdf_lex::json::{Object, Value};
        let response = self
            .call(
                id,
                "initialize",
                Object::new()
                    .with("protocolVersion", "2025-11-25")
                    .with("capabilities", Object::new())
                    .with(
                        "clientInfo",
                        Object::new().with("name", "fixture").with("version", "1"),
                    )
                    .into(),
            )
            .await;
        assert_eq!(
            response["result"]["protocolVersion"].as_str(),
            Some("2025-11-25")
        );
        self.send(Value::from(
            Object::new()
                .with("jsonrpc", "2.0")
                .with("method", "notifications/initialized"),
        ))
        .await;
    }
    async fn tool(
        &mut self,
        id: u64,
        name: &str,
        arguments: purrdf_lex::json::Value,
    ) -> purrdf_lex::json::Value {
        use purrdf_lex::json::Object;
        self.call(
            id,
            "tools/call",
            Object::new()
                .with("name", name)
                .with("arguments", arguments)
                .into(),
        )
        .await["result"]
            .clone()
    }
}

async fn mcp_exercise(
    endpoint: impl AsRef<std::ffi::OsStr>,
    store: &str,
    artifact: &str,
    record: &str,
    bytes: &[u8],
) {
    let socket = endpoint.as_ref();
    use purrdf_lex::json::{Object, Value};
    let mut mcp = Mcp::start(socket);
    use tokio::io::AsyncWriteExt;
    mcp.input
        .as_mut()
        .unwrap()
        .write_all(b"{invalid}\n")
        .await
        .unwrap();
    assert_eq!(mcp.receive().await["error"]["code"].as_i64(), Some(-32700));
    let early = mcp.call(1, "tools/list", Object::new().into()).await;
    assert_eq!(early["error"]["code"].as_i64(), Some(-32000));
    mcp.initialize(2).await;
    let tools = mcp.call(3, "tools/list", Object::new().into()).await;
    let definitions = tools["result"]["tools"].as_array().unwrap();
    assert_eq!(definitions.len(), 36);
    for (name, read_only) in [
        ("retention_metrics", true),
        ("ingest_get", true),
        ("file_finish", false),
        ("file_finish_get", true),
        ("watch_handoff", false),
        ("watch_handoff_get", true),
        ("result_release", false),
        ("syslog_listener_get", true),
        ("receiver_window_get", true),
    ] {
        assert_eq!(
            definitions
                .iter()
                .find(|tool| tool["name"].as_str() == Some(name))
                .unwrap()["annotations"]["readOnlyHint"]
                .as_bool(),
            Some(read_only)
        );
    }
    let metrics = mcp
        .tool(
            30,
            "retention_metrics",
            Object::new().with("store_id", store).into(),
        )
        .await;
    assert_eq!(metrics["isError"].as_bool(), Some(false));
    let metrics = &metrics["structuredContent"];
    let cli_metrics = cli(socket, &["retention-metrics", store]);
    for field in [
        "store_id",
        "live_records",
        "live_artifacts",
        "pinned_records",
        "pinned_artifacts",
        "oldest_received_at",
        "owned_payload_bytes",
        "live_payload_bytes",
        "pin_held_payload_bytes",
        "retirable_payload_bytes",
        "unclassified_payload_bytes",
        "payload_chunks",
        "visits",
    ] {
        assert_eq!(metrics[field], cli_metrics[field], "{field}");
    }
    assert_eq!(metrics["retirable_payload_bytes"].as_str(), Some("0"));
    assert_eq!(metrics["unclassified_payload_bytes"].as_str(), Some("0"));
    for (index, arguments, code) in [
        (
            31,
            Object::new().with("store_id", "store:other"),
            "STORE_MISMATCH",
        ),
        (
            32,
            Object::new()
                .with("store_id", store)
                .with("max_visits", 1_u32),
            "RESOURCE_LIMIT",
        ),
        (
            33,
            Object::new()
                .with("store_id", store)
                .with("catalog_bytes", 1_u32),
            "RESOURCE_LIMIT",
        ),
        (
            34,
            Object::new()
                .with("store_id", store)
                .with("payload_bytes", 1_u32),
            "RESOURCE_LIMIT",
        ),
        (
            35,
            Object::new()
                .with("store_id", store)
                .with("max_visits", 0_u32),
            "INVALID_ARGUMENT",
        ),
        (
            36,
            Object::new()
                .with("store_id", store)
                .with("catalog_bytes", Value::Null),
            "INVALID_ARGUMENT",
        ),
        (
            37,
            Object::new().with("store_id", store).with("advance", true),
            "INVALID_ARGUMENT",
        ),
    ] {
        let refused = mcp.tool(index, "retention_metrics", arguments.into()).await;
        assert_eq!(refused["isError"].as_bool(), Some(true));
        assert_eq!(
            refused["structuredContent"]["error"]["code"].as_str(),
            Some(code)
        );
    }
    assert_eq!(
        definitions
            .iter()
            .find(|tool| tool["name"].as_str() == Some("artifacts_list"))
            .unwrap()["annotations"]["readOnlyHint"]
            .as_bool(),
        Some(true)
    );
    assert!(
        definitions
            .iter()
            .any(|tool| tool["name"].as_str() == Some("sparql_query"))
    );
    assert_eq!(
        definitions
            .iter()
            .find(|tool| tool["name"].as_str() == Some("pin"))
            .unwrap()["annotations"]["readOnlyHint"]
            .as_bool(),
        Some(false)
    );
    let query = |name: &str, arguments: Value| {
        Object::new()
            .with("name", name)
            .with("arguments", arguments)
            .into()
    };
    let rdf = mcp
        .call(
            21,
            "tools/call",
            query(
                "rdf_inspect",
                Object::new()
                    .with("store_id", store)
                    .with("id", record)
                    .into(),
            ),
        )
        .await;
    assert_eq!(rdf["result"]["isError"].as_bool(), Some(false));
    let rdf = &rdf["result"]["structuredContent"];
    let cli_rdf = cli(socket, &["rdf", store, record]);
    for field in [
        "store_id",
        "record_id",
        "state",
        "metadata_graph",
        "content_graph",
        "primary_rows",
        "reifier_rows",
        "annotation_rows",
        "metadata_primary_rows",
        "diagnostic",
    ] {
        assert_eq!(rdf[field], cli_rdf[field]);
    }
    assert_eq!(rdf["state"].as_str(), Some("complete"));
    assert!(
        rdf["primary_rows"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > 20
    );
    let context = mcp
        .call(
            17,
            "tools/call",
            query(
                "records_context",
                Object::new()
                    .with("store_id", store)
                    .with("id", record)
                    .with("before", 10_u64)
                    .with("after", 10_u64)
                    .into(),
            ),
        )
        .await;
    assert_eq!(context["result"]["isError"].as_bool(), Some(false));
    let context = &context["result"]["structuredContent"];
    let cli_context = cli(
        socket,
        &["context", store, record, "--before", "10", "--after", "10"],
    );
    for key in [
        "anchor",
        "before",
        "after",
        "ordering",
        "continuity_unknown",
    ] {
        assert_eq!(context[key], cli_context[key]);
    }
    assert_eq!(context["anchor"]["record_id"].as_str(), Some(record));
    assert_eq!(context["ordering"].as_str(), Some("received_at_record_id"));
    assert_eq!(context["continuity_unknown"].as_bool(), Some(true));
    for output in [context, &cli_context] {
        let receipt = &output["receipt"];
        assert!(
            receipt["snapshot_id"]
                .as_str()
                .unwrap()
                .starts_with("context-read:")
        );
        assert_eq!(receipt["as_of"], receipt["membership_at"]);
        assert_eq!(receipt["execution_status"].as_str(), Some("complete"));
        assert!(receipt["stop_reason"].is_null() && receipt["native_governor"].is_null());
        assert_eq!(receipt["has_more"].as_bool(), Some(false));
        assert!(receipt["next_cursor"].is_null());
        assert_eq!(
            receipt["limits"]["catalog_bytes"].as_str(),
            Some("33554432")
        );
        assert_eq!(receipt["limits"]["response_bytes"].as_str(), Some("262144"));
        assert_eq!(receipt["limits"]["requested_before"].as_u64(), Some(10));
        assert_eq!(receipt["limits"]["requested_after"].as_u64(), Some(10));
        assert_eq!(
            receipt["profile_ids"][0]["id"].as_str(),
            Some(sparrow_core::CONTEXT_CAPTURE_PROFILE)
        );
    }
    assert_ne!(
        context["receipt"]["snapshot_id"],
        cli_context["receipt"]["snapshot_id"]
    );

    let neighbors: Vec<_> = ["before", "after"]
        .into_iter()
        .flat_map(|side| context[side]["rows"].as_array().unwrap())
        .collect();
    assert_eq!(neighbors.len(), 1);
    assert!(
        neighbors
            .iter()
            .all(|r| r["artifact_id"].as_str() == Some(artifact))
    );
    let mismatch = mcp
        .call(
            18,
            "tools/call",
            query(
                "records_context",
                Object::new()
                    .with("store_id", "store:another")
                    .with("id", record)
                    .into(),
            ),
        )
        .await;
    assert_eq!(
        mismatch["result"]["structuredContent"]["error"]["code"].as_str(),
        Some("STORE_MISMATCH")
    );
    let invalid_context = mcp
        .call(
            19,
            "tools/call",
            query(
                "records_context",
                Object::new()
                    .with("store_id", store)
                    .with("id", record)
                    .with("ordering", "event_time")
                    .into(),
            ),
        )
        .await;
    assert_eq!(
        invalid_context["result"]["structuredContent"]["error"]["code"].as_str(),
        Some("INVALID_ARGUMENT")
    );
    let limited_context = mcp
        .call(
            20,
            "tools/call",
            query(
                "records_context",
                Object::new()
                    .with("store_id", store)
                    .with("id", record)
                    .with("scan_budget", 1_u64)
                    .into(),
            ),
        )
        .await;
    assert_eq!(limited_context["result"]["isError"].as_bool(), Some(false));
    let anchor_only = cli(
        socket,
        &[
            "context", store, record, "--before", "0", "--after", "0", "--limit", "1",
        ],
    );
    assert!(anchor_only["before"]["rows"].as_array().unwrap().is_empty());
    assert!(anchor_only["after"]["rows"].as_array().unwrap().is_empty());
    let keys = mcp
        .call(
            4,
            "tools/call",
            query(
                "keys_list",
                Object::new().with("source", "synthetic-rpc").into(),
            ),
        )
        .await;
    assert_eq!(keys["result"]["isError"].as_bool(), Some(false));
    assert_eq!(
        keys["result"]["structuredContent"]["rows"],
        cli(socket, &["keys", "--source", "synthetic-rpc"])["rows"]
    );
    let text = mcp
        .call(
            5,
            "tools/call",
            query(
                "text_search",
                Object::new()
                    .with("source", "synthetic-rpc")
                    .with("text", "hello")
                    .into(),
            ),
        )
        .await;
    assert_eq!(
        text["result"]["structuredContent"]["rows"],
        cli(socket, &["text", "hello", "--source", "synthetic-rpc"])["rows"]
    );
    assert_eq!(
        text["result"]["structuredContent"]["text_coverage"]["failed_or_incomplete"].as_str(),
        Some("1")
    );
    let predicates = Value::array([Object::new()
        .with("path", "/message")
        .with("decoded", "hello world")
        .with("parent_binding", "root")
        .with("parent_path", "")]);
    let predicate_file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        predicate_file.path(),
        purrdf_lex::json::write_compact(&predicates),
    )
    .unwrap();
    let combined = mcp
        .call(
            40,
            "tools/call",
            query(
                "records_search",
                Object::new()
                    .with("source", "synthetic-rpc")
                    .with("text", "hello")
                    .with("fields", predicates.clone())
                    .into(),
            ),
        )
        .await;
    assert_eq!(combined["result"]["isError"].as_bool(), Some(false));
    let through_cli = cli(
        socket,
        &[
            "records",
            "--source",
            "synthetic-rpc",
            "--text",
            "hello",
            "--where-file",
            predicate_file.path().to_str().unwrap(),
        ],
    );
    assert_eq!(
        combined["result"]["structuredContent"]["rows"],
        through_cli["rows"]
    );
    assert_eq!(
        combined["result"]["structuredContent"]["text"]["rows"],
        through_cli["text"]["rows"]
    );
    assert_eq!(
        through_cli["rows"][0]["fields"][0]["bound_parent"].as_str(),
        Some("0")
    );
    assert_eq!(
        through_cli["text"]["ranking"]["documents"].as_str(),
        Some("1")
    );
    assert_eq!(
        through_cli["receipt"]["coverage"]["failed_json_records"].as_str(),
        Some("1")
    );
    let field_only = cli(
        socket,
        &[
            "records",
            "--source",
            "synthetic-rpc",
            "--where-file",
            predicate_file.path().to_str().unwrap(),
        ],
    );
    assert!(field_only["text"].is_null());
    assert_eq!(field_only["rows"], through_cli["rows"]);
    let strict = mcp
        .call(
            41,
            "tools/call",
            query(
                "records_search",
                Object::new()
                    .with("source", "synthetic-rpc")
                    .with("fields", predicates)
                    .with("require_complete_index", true)
                    .into(),
            ),
        )
        .await;
    assert_eq!(
        strict["result"]["structuredContent"]["error"]["code"].as_str(),
        Some("INDEX_NOT_READY")
    );
    let invalid_time = mcp
        .call(
            42,
            "tools/call",
            query(
                "records_search",
                Object::new()
                    .with("event_from", "2026-10-01T00:00:00Z")
                    .into(),
            ),
        )
        .await;
    assert_eq!(
        invalid_time["result"]["structuredContent"]["error"]["code"].as_str(),
        Some("INVALID_ARGUMENT")
    );
    let defined = mcp
        .call(
            50,
            "tools/call",
            query(
                "source_time_define",
                Object::new()
                    .with("source", "rpc-events")
                    .with("path", "/t")
                    .with("representation", "rfc3339")
                    .into(),
            ),
        )
        .await;
    assert_eq!(defined["result"]["isError"].as_bool(), Some(false));
    let cli_profile = cli(socket, &["source-time", "rpc-events", "/t", "rfc3339"]);
    assert_eq!(defined["result"]["structuredContent"], cli_profile);
    assert_eq!(cli_profile["durability"].as_str(), Some("durable"));
    let gotten = mcp
        .call(
            51,
            "tools/call",
            query(
                "source_time_get",
                Object::new().with("source", "rpc-events").into(),
            ),
        )
        .await;
    assert_eq!(
        gotten["result"]["structuredContent"],
        cli(socket, &["source-time-get", "rpc-events"])
    );
    assert_eq!(
        gotten["result"]["structuredContent"]["profile"],
        cli_profile
    );
    let conflict = mcp
        .call(
            52,
            "tools/call",
            query(
                "source_time_define",
                Object::new()
                    .with("source", "rpc-events")
                    .with("path", "/t")
                    .with("representation", "unix_seconds")
                    .into(),
            ),
        )
        .await;
    assert_eq!(
        conflict["result"]["structuredContent"]["error"]["code"].as_str(),
        Some("CONFLICT")
    );
    let fixture = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(fixture.path(), b"{\"t\":\"1970-01-01T00:00:02Z\",\"message\":\"failed\"}\n{\"t\":\"1970-01-01T00:00:01Z\",\"message\":\"failed\"}\n{\"t\":\"1970-01-01T00:00:03Z\",\"t\":\"1970-01-01T00:00:03Z\"}\n{\"message\":\"unknown\"}\n").unwrap();
    cli(
        socket,
        &[
            "ingest",
            "rpc-events-input",
            "rpc-events",
            "jsonl",
            fixture.path().to_str().unwrap(),
        ],
    );
    let event_args = Object::new()
        .with("source", "rpc-events")
        .with("event_from", "1970-01-01T00:00:01Z")
        .with("event_before", "1970-01-01T00:00:02Z")
        .with("text", "failed");
    let events = mcp
        .call(53, "tools/call", query("records_search", event_args.into()))
        .await;
    assert_eq!(events["result"]["isError"].as_bool(), Some(false));
    let event_cli = cli(
        socket,
        &[
            "records",
            "--source",
            "rpc-events",
            "--text",
            "failed",
            "--event-from",
            "1970-01-01T00:00:01Z",
            "--event-before",
            "1970-01-01T00:00:02Z",
        ],
    );
    let event_mcp = &events["result"]["structuredContent"];
    assert_eq!(event_mcp["rows"], event_cli["rows"]);
    assert_eq!(event_mcp["text"]["rows"], event_cli["text"]["rows"]);
    assert_eq!(event_cli["event_coverage"]["unknown"].as_str(), Some("2"));
    assert_eq!(
        event_cli["event_coverage"]["outside_interval"].as_str(),
        Some("1")
    );
    assert_eq!(event_cli["time_basis"].as_str(), Some("event_time"));
    assert_eq!(
        event_cli["rows"][0]["event_time"]["instant"].as_str(),
        Some("1970-01-01T00:00:01Z")
    );
    assert_eq!(
        event_cli["text"]["ranking"]["documents"].as_str(),
        Some("1")
    );
    assert_eq!(
        event_cli["rows"][0]["event_time"]["profile_id"],
        cli_profile["profile_id"]
    );
    let values = mcp
        .call(
            6,
            "tools/call",
            query(
                "values_search",
                Object::new()
                    .with("path", "/message")
                    .with("decoded", "hello world")
                    .into(),
            ),
        )
        .await;
    assert_eq!(
        values["result"]["structuredContent"]["rows"][0]["record_id"].as_str(),
        Some(record)
    );
    let raw = mcp
        .call(
            7,
            "tools/call",
            query(
                "bytes_read",
                Object::new()
                    .with("store_id", store)
                    .with("id", artifact)
                    .with("offset", "0")
                    .with("length", bytes.len() as u64)
                    .into(),
            ),
        )
        .await;
    assert_eq!(
        raw["result"]["structuredContent"]["data"].as_str(),
        Some(purrdf::xsd::canonical_base64(bytes).as_str())
    );
    let invalid = mcp
        .call(
            8,
            "tools/call",
            query(
                "keys_list",
                Object::new().with("event_time", "guessed").into(),
            ),
        )
        .await;
    assert_eq!(invalid["result"]["isError"].as_bool(), Some(true));
    assert_eq!(
        invalid["result"]["structuredContent"]["error"]["code"].as_str(),
        Some("INVALID_ARGUMENT")
    );
    let mismatch = mcp
        .call(
            9,
            "tools/call",
            query(
                "record_get",
                Object::new()
                    .with("store_id", "wrong-store")
                    .with("id", record)
                    .into(),
            ),
        )
        .await;
    assert_eq!(
        mismatch["result"]["structuredContent"]["error"]["code"].as_str(),
        Some("STORE_MISMATCH")
    );
    assert_eq!(
        mismatch["result"]["structuredContent"]["error"]["retryable"].as_bool(),
        Some(false)
    );
    let pin = mcp
        .call(
            10,
            "tools/call",
            query(
                "pin",
                Object::new()
                    .with("store_id", store)
                    .with("request_id", "mcp-pin-v1")
                    .with("name", "mcp-case")
                    .with("targets", Value::array([record]))
                    .with("days", 1_u64)
                    .into(),
            ),
        )
        .await;
    assert_eq!(pin["result"]["isError"].as_bool(), Some(false));
    let pin_id = pin["result"]["structuredContent"]["pin_id"]
        .as_str()
        .unwrap();
    assert_eq!(
        cli(
            socket,
            &["pin", "mcp-pin-v1", store, "mcp-case", "1", record]
        ),
        pin["result"]["structuredContent"]
    );
    for (args, code) in [
        (
            vec![
                "pin",
                "cli-invalid-store",
                "store:foreign",
                "foreign",
                "1",
                record,
            ],
            "STORE_MISMATCH",
        ),
        (
            vec!["pin", "cli-invalid-days", store, "zero", "0", record],
            "INVALID_ARGUMENT",
        ),
        (
            vec!["pin", "mcp-pin-v1", store, "changed", "1", record],
            "CONFLICT",
        ),
        (
            vec![
                "pin",
                "cli-invalid-target",
                store,
                "target",
                "1",
                "not-evidence",
            ],
            "INVALID_ARGUMENT",
        ),
    ] {
        let rejected = Command::new(env!("CARGO_BIN_EXE_sparrow"))
            .arg(socket)
            .args(args)
            .admitted_output()
            .unwrap();
        assert!(!rejected.status.success() && rejected.stdout.is_empty());
        assert!(String::from_utf8_lossy(&rejected.stderr).contains(code));
    }
    let cli_renewal = cli(
        socket,
        &["pin-renew", "mcp-renew-v1", store, pin_id, "2", "1"],
    );
    let renewed = mcp
        .call(
            30,
            "tools/call",
            query(
                "pin_renew",
                Object::new()
                    .with("store_id", store)
                    .with("request_id", "mcp-renew-v1")
                    .with("pin_id", pin_id)
                    .with("days", 2_u64)
                    .with("expected_revision", "1")
                    .into(),
            ),
        )
        .await;
    assert_eq!(renewed["result"]["isError"].as_bool(), Some(false));
    assert_eq!(renewed["result"]["structuredContent"], cli_renewal);
    assert_eq!(cli_renewal["revision"].as_str(), Some("2"));
    assert_eq!(cli_renewal["direct_member_count"].as_str(), Some("1"));
    assert_eq!(cli_renewal["durability"].as_str(), Some("durable"));
    for (call_id, arguments, code) in [
        (
            31,
            Object::new()
                .with("store_id", store)
                .with("request_id", "mcp-renew-v1")
                .with("pin_id", pin_id)
                .with("days", 3_u64)
                .with("expected_revision", "1"),
            "CONFLICT",
        ),
        (
            32,
            Object::new()
                .with("store_id", "wrong-store")
                .with("request_id", "mcp-renew-other")
                .with("pin_id", pin_id)
                .with("days", 2_u64),
            "STORE_MISMATCH",
        ),
        (
            33,
            Object::new()
                .with("store_id", store)
                .with("request_id", "mcp-renew-other")
                .with("pin_id", pin_id)
                .with("days", 2_u64)
                .with("expected_revision", 1_u64),
            "INVALID_ARGUMENT",
        ),
    ] {
        let response = mcp
            .call(call_id, "tools/call", query("pin_renew", arguments.into()))
            .await;
        assert_eq!(response["result"]["isError"].as_bool(), Some(true));
        assert_eq!(
            response["result"]["structuredContent"]["error"]["code"].as_str(),
            Some(code)
        );
    }
    let release = mcp
        .call(
            11,
            "tools/call",
            query(
                "pin_release",
                Object::new()
                    .with("store_id", store)
                    .with("request_id", "mcp-release-v1")
                    .with("pin_id", pin_id)
                    .into(),
            ),
        )
        .await;
    assert_eq!(release["result"]["isError"].as_bool(), Some(false));
    assert_eq!(
        cli(socket, &["pin-release", "mcp-release-v1", store, pin_id]),
        release["result"]["structuredContent"]
    );
    let retry = mcp
        .call(
            12,
            "tools/call",
            query(
                "pin_release",
                Object::new()
                    .with("store_id", store)
                    .with("request_id", "mcp-release-v1")
                    .with("pin_id", pin_id)
                    .into(),
            ),
        )
        .await;
    assert_eq!(
        release["result"]["structuredContent"],
        retry["result"]["structuredContent"]
    );
    let capabilities = mcp
        .call(
            13,
            "tools/call",
            query("capabilities", Object::new().into()),
        )
        .await;
    assert_eq!(
        capabilities["result"]["structuredContent"]["status"]["store_id"].as_str(),
        Some(store)
    );
    assert_eq!(
        capabilities["result"]["structuredContent"]["status"]["schema_version"].as_u64(),
        Some(u64::from(sparrow_client::SCHEMA_VERSION))
    );
    let unknown = mcp
        .call(
            14,
            "tools/call",
            query("unsupported_query", Object::new().into()),
        )
        .await;
    assert_eq!(unknown["error"]["code"].as_i64(), Some(-32602));
    let precision = mcp
        .call(9007199254740993, "ping", Object::new().into())
        .await;
    assert_eq!(precision["id"].as_u64(), Some(9007199254740993));
    let null_limit = mcp
        .call(
            15,
            "tools/call",
            query("keys_list", Object::new().with("limit", Value::Null).into()),
        )
        .await;
    assert_eq!(null_limit["result"]["isError"].as_bool(), Some(true));
    let unknown_list_field = mcp
        .call(
            16,
            "tools/list",
            Object::new().with("unexpected", true).into(),
        )
        .await;
    assert_eq!(unknown_list_field["error"]["code"].as_i64(), Some(-32602));
    mcp.close(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn mcp_cancellation_admission_and_partial_frames_do_not_lose_messages() {
    use purrdf_lex::json::{Object, Value};
    use tokio::io::AsyncWriteExt;
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("unresponsive.sock");
    // An isolated fault endpoint, never the real daemon or a model backend.
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let mut mcp = Mcp::start(&socket);
    mcp.call(
        1,
        "initialize",
        Object::new()
            .with("protocolVersion", "2025-11-25")
            .with("capabilities", Object::new())
            .with(
                "clientInfo",
                Object::new()
                    .with("name", "fault-fixture")
                    .with("version", "1"),
            )
            .into(),
    )
    .await;
    mcp.send(
        Object::new()
            .with("jsonrpc", "2.0")
            .with("method", "notifications/initialized")
            .into(),
    )
    .await;
    for id in 11..=14_u64 {
        mcp.send(
            Object::new()
                .with("jsonrpc", "2.0")
                .with("id", id)
                .with("method", "tools/call")
                .with(
                    "params",
                    Object::new()
                        .with("name", "status")
                        .with("arguments", Object::new()),
                )
                .into(),
        )
        .await;
    }
    let excess = mcp
        .call(
            15,
            "tools/call",
            Object::new()
                .with("name", "status")
                .with("arguments", Object::new())
                .into(),
        )
        .await;
    assert_eq!(
        excess["result"]["structuredContent"]["error"]["code"].as_str(),
        Some("RESOURCE_LIMIT")
    );
    mcp.input
        .as_mut()
        .unwrap()
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":99,\"method\":\"pi")
        .await
        .unwrap();
    let (peer, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept())
        .await
        .unwrap()
        .unwrap();
    drop(peer); // Complete one pending call while its next input frame is incomplete.
    let failed = mcp.receive().await;
    assert!((11..=14).contains(&failed["id"].as_u64().unwrap()));
    assert_eq!(
        failed["result"]["structuredContent"]["error"]["code"].as_str(),
        Some("RPC_FAILURE")
    );
    mcp.input
        .as_mut()
        .unwrap()
        .write_all(b"ng\"}\n")
        .await
        .unwrap();
    assert_eq!(mcp.receive().await["id"].as_u64(), Some(99));
    for id in 11..=14_u64 {
        mcp.send(
            Object::new()
                .with("jsonrpc", "2.0")
                .with("method", "notifications/cancelled")
                .with("params", Object::new().with("requestId", id))
                .into(),
        )
        .await;
    }
    let ping = mcp.call(100, "ping", Object::new().into()).await;
    assert!(ping.get("error").is_none());
    mcp.close(true).await;
    let mut oversized = Mcp::start(&socket);
    let mut bytes = vec![b' '; 128 * 1024 + 1];
    bytes.push(b'\n');
    if let Err(error) = oversized.input.as_mut().unwrap().write_all(&bytes).await {
        assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    }
    let refused = oversized.receive().await;
    assert_eq!(refused["id"], Value::Null);
    assert_eq!(refused["error"]["code"].as_i64(), Some(-32700));
    oversized.close(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn direct_promises_and_mcp_notifications_cancel_captures_without_closing_the_session() {
    use purrdf_lex::json::{Object, Value};
    use sparrow_test_support::anonymous_query_files;
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let text = std::fs::read_to_string(&config)
                .unwrap()
                .replace("disk_budget = 134217728", "disk_budget = 1073741824");
            std::fs::write(&config, text).unwrap();
            let (address, reservation) = configure_tcp_listener(&config);
            drop(reservation);
            let daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let (line, _) = sparrow_test_support::wide_jsonl_keys(1, 1, 0);
            let path = dir.path().join("cancel.jsonl");
            std::fs::write(&path, line.repeat(8000)).unwrap();
            let receipt = client
                .import_file("mcp-cancel-corpus", "cancel", wire::Format::Jsonl, &path)
                .await
                .unwrap();
            assert_eq!(receipt.record_ids.len(), 8000);
            let tcp = format!("tcp://{address}");
            for endpoint in [socket.to_str().unwrap(), tcp.as_str()] {
                use futures::FutureExt;
                let direct = if endpoint == tcp {
                    sparrow_client::Client::connect_tcp(address).await.unwrap()
                } else {
                    sparrow_client::Client::connect(&socket).await.unwrap()
                };
                let scope = |mut scope: wire::scope::Builder<'_>| {
                    scope.set_schema_version(sparrow_client::SCHEMA_VERSION);
                    scope.reborrow().init_source().set_value("cancel");
                    scope
                        .reborrow()
                        .init_received_before()
                        .set_unix_millis(u64::MAX);
                    scope.set_limit(1);
                    scope.set_scan_budget(100000);
                };
                for name in ["records", "values", "text", "keys"] {
                    let promise = match name {
                        "records" => {
                            let mut request = direct.capability().records_search_request();
                            scope(request.get().init_query().init_scope());
                            request
                                .send()
                                .promise
                                .map(|result| result.map(|_| ()))
                                .boxed_local()
                        }
                        "values" => {
                            let mut request = direct.capability().values_search_request();
                            scope(request.get().init_query().init_scope());
                            request
                                .send()
                                .promise
                                .map(|result| result.map(|_| ()))
                                .boxed_local()
                        }
                        "text" => {
                            let mut request = direct.capability().text_search_request();
                            let mut query = request.get().init_query();
                            query.set_text("needle");
                            scope(query.init_scope());
                            request
                                .send()
                                .promise
                                .map(|result| result.map(|_| ()))
                                .boxed_local()
                        }
                        "keys" => {
                            let mut request = direct.capability().keys_list_request();
                            scope(request.get().init_scope());
                            request
                                .send()
                                .promise
                                .map(|result| result.map(|_| ()))
                                .boxed_local()
                        }
                        _ => unreachable!(),
                    };
                    let caller = tokio::task::spawn_local(promise);
                    let deadline = Instant::now() + Duration::from_secs(10);
                    loop {
                        assert!(
                            !caller.is_finished(),
                            "direct {name} finished before cancellation"
                        );
                        let files = anonymous_query_files(daemon.0.id(), &data).unwrap();
                        if files
                            .iter()
                            .any(|file| name == "keys" || name == "text" || file.len() > 0)
                        {
                            break;
                        }
                        assert!(
                            Instant::now() < deadline,
                            "direct {name} staging was not observed"
                        );
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    // Drop only this promise. Keep the SDK and its transport alive;
                    // native Cap'n Proto Finish must cancel this caller independently.
                    caller.abort();
                    assert!(caller.await.unwrap_err().is_cancelled());
                    let deadline = Instant::now() + Duration::from_secs(5);
                    while !anonymous_query_files(daemon.0.id(), &data)
                        .unwrap()
                        .is_empty()
                    {
                        assert!(
                            Instant::now() < deadline,
                            "direct {name} staging survived Finish"
                        );
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    let response = direct
                        .capability()
                        .status_request()
                        .send()
                        .promise
                        .await
                        .unwrap();
                    let status = rpc_ok(response.get().unwrap().get_result().unwrap());
                    assert_eq!(status.get_record_read_handles(), 0);
                    assert_eq!(status.get_record_read_bytes(), 0);
                    assert_eq!(status.get_live_records(), 8000);
                    assert_eq!(
                        status.get_store_id().unwrap().to_str().unwrap(),
                        receipt.store_id
                    );
                }
                let mut mcp = Mcp::start(endpoint);
                mcp.initialize(1).await;
                for (ordinal, name) in [
                    "records_search",
                    "values_search",
                    "text_search",
                    "keys_list",
                ]
                .into_iter()
                .enumerate()
                {
                    assert!(
                        anonymous_query_files(daemon.0.id(), &data)
                            .unwrap()
                            .is_empty()
                    );
                    let id = if ordinal % 2 == 0 {
                        Value::from(40_u64 + ordinal as u64)
                    } else {
                        Value::from(format!("cancel-{ordinal}"))
                    };
                    let mut arguments = Object::new()
                        .with("source", "cancel")
                        .with("scan_budget", 100000_u64)
                        .with("page_size", 1_u64);
                    if name == "text_search" {
                        arguments.insert("text", Value::from("needle"));
                    }
                    mcp.send(
                        Object::new()
                            .with("jsonrpc", "2.0")
                            .with("id", id.clone())
                            .with("method", "tools/call")
                            .with(
                                "params",
                                Object::new()
                                    .with("name", name)
                                    .with("arguments", arguments),
                            )
                            .into(),
                    )
                    .await;
                    let deadline = Instant::now() + Duration::from_secs(10);
                    loop {
                        let files = anonymous_query_files(daemon.0.id(), &data).unwrap();
                        // Keys/scoring stage only after their complete aggregation/membership
                        // scan, so an empty anonymous file is the admitted in-flight witness.
                        if files.iter().any(|file| {
                            name == "keys_list" || name == "text_search" || file.len() > 0
                        }) {
                            break;
                        }
                        assert!(
                            Instant::now() < deadline,
                            "capture was not observed for {name}"
                        );
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    mcp.send(
                        Object::new()
                            .with("jsonrpc", "2.0")
                            .with("method", "notifications/cancelled")
                            .with(
                                "params",
                                Object::new()
                                    .with("requestId", id)
                                    .with("reason", "qualification"),
                            )
                            .into(),
                    )
                    .await;
                    let deadline = Instant::now() + Duration::from_secs(5);
                    while !anonymous_query_files(daemon.0.id(), &data)
                        .unwrap()
                        .is_empty()
                    {
                        assert!(
                            Instant::now() < deadline,
                            "cancelled {name} staging survived"
                        );
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    // Only the ping responds: cancellation itself and the aborted tool
                    // must not produce an unsolicited or successful-prefix response.
                    assert!(
                        mcp.call(100 + ordinal as u64, "ping", Object::new().into())
                            .await
                            .get("error")
                            .is_none()
                    );
                    let status = mcp
                        .tool(200 + ordinal as u64, "status", Object::new().into())
                        .await;
                    assert_eq!(
                        status["structuredContent"]["record_read_handles"].as_str(),
                        Some("0")
                    );
                    assert_eq!(
                        status["structuredContent"]["record_read_bytes"].as_str(),
                        Some("0")
                    );
                    assert_eq!(
                        status["structuredContent"]["live_records"].as_str(),
                        Some("8000")
                    );
                }
                let mut page = mcp
                    .tool(
                        300,
                        "records_search",
                        Object::new()
                            .with("source", "cancel")
                            .with("scan_budget", 100000_u64)
                            .with("page_size", 1000_u64)
                            .into(),
                    )
                    .await["structuredContent"]
                    .clone();
                let snapshot = page["page"]["snapshot_id"].as_str().unwrap().to_owned();
                let mut ids = std::collections::BTreeSet::new();
                let mut request_id = 301;
                loop {
                    for row in page["rows"].as_array().unwrap() {
                        assert!(ids.insert(row["record_id"].as_str().unwrap().to_owned()));
                    }
                    let Some(cursor) = page["page"]["next_cursor"].as_str() else {
                        break;
                    };
                    page = mcp
                        .tool(
                            request_id,
                            "result_next",
                            Object::new()
                                .with("store_id", receipt.store_id.clone())
                                .with("cursor", cursor)
                                .into(),
                        )
                        .await["structuredContent"]
                        .clone();
                    request_id += 1;
                }
                assert_eq!(ids, receipt.record_ids.iter().cloned().collect());
                let released = mcp
                    .tool(
                        request_id,
                        "result_release",
                        Object::new()
                            .with("store_id", receipt.store_id.clone())
                            .with("snapshot_id", snapshot)
                            .into(),
                    )
                    .await;
                assert_eq!(
                    released["structuredContent"]["released"].as_bool(),
                    Some(true)
                );
                assert!(
                    anonymous_query_files(daemon.0.id(), &data)
                        .unwrap()
                        .is_empty()
                );
                mcp.close(true).await;
            }
            assert_eq!(cli(&socket, &["status"])["active_pins"].as_str(), Some("0"));
        })
        .await;
}

async fn mcp_pin_pages(socket: &Path, store: &str, artifact: &str, record: &str) -> String {
    use purrdf_lex::json::{Object, Value};
    let mut mcp = Mcp::start(socket);
    mcp.initialize(1).await;
    let created = mcp
        .tool(
            2,
            "pin",
            Object::new()
                .with("store_id", store)
                .with("request_id", "page-case-create")
                .with("name", "page-case")
                .with("targets", Value::array([artifact, record]))
                .with("days", 2_u64)
                .into(),
        )
        .await;
    assert_eq!(created["isError"].as_bool(), Some(false));
    let old_id = created["structuredContent"]["pin_id"].as_str().unwrap();
    let input = Object::new()
        .with("store_id", store)
        .with("name", "page-case")
        .with("page_size", 1_u64);
    let first = mcp.tool(3, "pin_get", input.clone().into()).await;
    assert_eq!(first["isError"].as_bool(), Some(false));
    let first = &first["structuredContent"];
    let cursor = first["next_cursor"].as_str().unwrap().to_owned();
    assert_eq!(first["total_items"].as_str(), Some("2"));
    assert_eq!(first["has_more"].as_bool(), Some(true));
    assert_eq!(first["members"].as_array().unwrap().len(), 1);
    assert_eq!(first["answer_certainty"].as_str(), Some("exact"));
    let release = mcp
        .tool(
            4,
            "pin_release",
            Object::new()
                .with("store_id", store)
                .with("request_id", "page-case-release")
                .with("pin_id", old_id)
                .into(),
        )
        .await;
    assert_eq!(release["isError"].as_bool(), Some(false));
    let replacement = mcp
        .tool(
            5,
            "pin",
            Object::new()
                .with("store_id", store)
                .with("request_id", "page-case-replace")
                .with("name", "page-case")
                .with("targets", Value::array([record]))
                .with("days", 2_u64)
                .into(),
        )
        .await;
    let replacement_id = replacement["structuredContent"]["pin_id"].as_str().unwrap();
    assert_ne!(replacement_id, old_id);
    let cli_page = cli(
        socket,
        &[
            "pin-get",
            store,
            "--name",
            "page-case",
            "--page-size",
            "1",
            "--cursor",
            &cursor,
        ],
    );
    let second = mcp
        .tool(
            6,
            "pin_get",
            input.clone().with("cursor", cursor.clone()).into(),
        )
        .await;
    assert_eq!(second["structuredContent"], cli_page);
    assert_eq!(cli_page["snapshot_id"], first["snapshot_id"]);
    assert_eq!(cli_page["as_of"], first["as_of"]);
    assert_eq!(cli_page["lease_expires_at"], first["lease_expires_at"]);
    assert_eq!(cli_page["pin"]["pin_id"].as_str(), Some(old_id));
    assert_eq!(cli_page["members"], Value::array([record]));
    assert_eq!(cli_page["has_more"].as_bool(), Some(false));
    assert_eq!(cli_page["execution_status"].as_str(), Some("complete"));
    let current = mcp.tool(7, "pin_get", input.clone().into()).await;
    assert_eq!(
        current["structuredContent"]["pin"]["pin_id"].as_str(),
        Some(replacement_id)
    );
    let other = mcp
        .tool(
            8,
            "pin",
            Object::new()
                .with("store_id", store)
                .with("request_id", "page-other-create")
                .with("name", "page-other")
                .with("targets", Value::array([artifact]))
                .with("days", 2_u64)
                .into(),
        )
        .await;
    let other_id = other["structuredContent"]["pin_id"].as_str().unwrap();
    let listed = mcp
        .tool(
            9,
            "pins_list",
            Object::new()
                .with("store_id", store)
                .with("page_size", 1_u64)
                .into(),
        )
        .await;
    assert_eq!(listed["isError"].as_bool(), Some(false));
    let listed = &listed["structuredContent"];
    assert_eq!(listed["total_items"].as_str(), Some("2"));
    let list_cursor = listed["next_cursor"].as_str().unwrap();
    let refused = mcp
        .tool(
            10,
            "pins_list",
            Object::new()
                .with("store_id", store)
                .with("scan_budget", 1_u64)
                .into(),
        )
        .await;
    assert_eq!(
        refused["structuredContent"]["error"]["code"].as_str(),
        Some("RESOURCE_LIMIT")
    );
    for (id, request, pin) in [
        (11, "page-replacement-release", replacement_id),
        (12, "page-other-release", other_id),
    ] {
        assert_eq!(
            mcp.tool(
                id,
                "pin_release",
                Object::new()
                    .with("store_id", store)
                    .with("request_id", request)
                    .with("pin_id", pin)
                    .into()
            )
            .await["isError"]
                .as_bool(),
            Some(false)
        );
    }
    let list_page = cli(
        socket,
        &["pins", store, "--page-size", "1", "--cursor", list_cursor],
    );
    let list_again = mcp
        .tool(
            13,
            "pins_list",
            Object::new()
                .with("store_id", store)
                .with("page_size", 1_u64)
                .with("cursor", list_cursor)
                .into(),
        )
        .await;
    assert_eq!(list_again["structuredContent"], list_page);
    let mut ids = [
        listed["rows"][0]["pin_id"].as_str().unwrap(),
        list_page["rows"][0]["pin_id"].as_str().unwrap(),
    ];
    ids.sort();
    let mut expected = [replacement_id, other_id];
    expected.sort();
    assert_eq!(ids, expected);
    for (id, name, arguments, code) in [
        (
            14,
            "pins_list",
            Object::new()
                .with("store_id", store)
                .with("page_size", 1_u64)
                .with("cursor", cursor.clone()),
            "CONFLICT",
        ),
        (
            15,
            "pin_get",
            Object::new()
                .with("store_id", store)
                .with("pin_id", old_id)
                .with("page_size", 1_u64)
                .with("cursor", cursor.clone()),
            "CONFLICT",
        ),
        (
            16,
            "pin_get",
            input.clone().with("pin_id", old_id),
            "INVALID_ARGUMENT",
        ),
        (
            17,
            "pin_get",
            input.clone().with("store_id", "wrong-store"),
            "STORE_MISMATCH",
        ),
    ] {
        let result = mcp.tool(id, name, arguments.into()).await;
        assert_eq!(result["isError"].as_bool(), Some(true));
        assert_eq!(
            result["structuredContent"]["error"]["code"].as_str(),
            Some(code)
        );
    }
    assert_eq!(
        cli(socket, &["pins", store])["total_items"].as_str(),
        Some("0")
    );
    mcp.close(true).await;
    cursor
}

fn cli(socket: impl AsRef<std::ffi::OsStr>, args: &[&str]) -> purrdf_lex::json::Value {
    let output = Command::new(env!("CARGO_BIN_EXE_sparrow"))
        .arg(socket)
        .args(args)
        .admitted_output()
        .unwrap();
    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    purrdf_lex::json::read_slice(&output.stdout, purrdf_lex::json::Limits::default()).unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn recordless_graph_metadata_and_independent_coverage_reach_cli_and_mcp() {
    use purrdf_lex::json::{self, Object};
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let store = client.discovery().store_id.clone();
            let empty = client
                .ingest("empty-graph-input", "empty-graph", wire::Format::Log, b"")
                .await
                .unwrap();
            let args = Object::new()
                .with("store_id", store.clone())
                .with(
                    "query",
                    "SELECT ?artifact WHERE { ?artifact a <urn:sparrow:vocab:Artifact> }",
                )
                .with(
                    "scope",
                    Object::new()
                        .with("kind", "selection")
                        .with("store_id", store.clone())
                        .with("artifact_ids", vec![empty.id.clone()]),
                );
            let path = dir.path().join("empty-graph.json");
            std::fs::write(&path, json::write_compact(&args.clone().into())).unwrap();
            let cli = cli(&socket, &["sparql", path.to_str().unwrap()]);
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            let response = mcp.tool(2, "sparql_query", args.into()).await;
            assert_eq!(response["isError"].as_bool(), Some(false), "{response:?}");
            let mcp = &response["structuredContent"];
            assert_eq!(cli["rows"], mcp["rows"]);
            assert_eq!(cli["coverage"], mcp["coverage"]);
            assert_eq!(
                cli["coverage"]["known_selected_records"].as_str(),
                Some("0")
            );
            assert_eq!(
                cli["coverage"]["known_selected_artifacts"].as_str(),
                Some("1")
            );
            assert_eq!(
                cli["coverage"]["artifact_diagnostic_unknown"].as_str(),
                Some("0")
            );
            assert_eq!(
                cli["rows"]["native_result"]["results"]["bindings"][0]["artifact"]["value"]
                    .as_str(),
                Some(format!("urn:sparrow:{}", empty.id).as_str())
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn graph_catalog_io_profile_preserves_three_hundred_occurrences_through_cli_and_mcp() {
    use purrdf_lex::json::{self, Object};
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let store = client.discovery().store_id.clone();
            client
                .ingest(
                    "catalog-io-300",
                    "catalog-io",
                    wire::Format::Jsonl,
                    &b"{}\n".repeat(300),
                )
                .await
                .unwrap();
            let args = Object::new()
                .with("store_id", store)
                .with(
                    "query",
                    "SELECT (COUNT(*) AS ?count) WHERE { ?entity a <urn:sparrow:vocab:Record> }",
                )
                .with(
                    "layers",
                    Object::new()
                        .with("content", false)
                        .with("diagnostic", false),
                );
            let path = dir.path().join("catalog-io.json");
            std::fs::write(&path, json::write_compact(&args.clone().into())).unwrap();
            let expected = cli(&socket, &["sparql", path.to_str().unwrap()]);
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            let response = mcp.tool(2, "sparql_query", args.clone().into()).await;
            assert_eq!(response["isError"].as_bool(), Some(false), "{response:?}");
            let result = &response["structuredContent"];
            assert_eq!(result["rows"], expected["rows"]);
            assert_eq!(
                result["rows"]["native_result"]["results"]["bindings"][0]["count"]["value"]
                    .as_str(),
                Some("300")
            );
            assert_eq!(
                result["coverage"]["known_selected_records"].as_str(),
                Some("300")
            );
            assert_eq!(
                result["coverage"]["known_selected_artifacts"].as_str(),
                Some("1")
            );
            assert_eq!(
                result["limits"]["source_profile"].as_str(),
                Some(sparrow_core::RDF_SOURCE_ADMISSION_PROFILE)
            );
            assert_eq!(result["profile_ids"], expected["profile_ids"]);
            assert!(
                result["profile_ids"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|v| v.as_str() == Some(sparrow_core::RDF_SOURCE_ADMISSION_PROFILE))
            );
            assert!(
                result["source_evidence"]["admitted_bytes"]
                    .as_str()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
                    < 32 * 1024 * 1024
            );
            let artifact_args = args.with(
                "query",
                "SELECT (COUNT(*) AS ?count) WHERE { ?entity a <urn:sparrow:vocab:Artifact> }",
            );
            std::fs::write(&path, json::write_compact(&artifact_args.clone().into())).unwrap();
            let artifact_cli = cli(&socket, &["sparql", path.to_str().unwrap()]);
            let artifact_reply = mcp.tool(3, "sparql_query", artifact_args.into()).await;
            assert_eq!(artifact_reply["isError"].as_bool(), Some(false));
            assert_eq!(
                artifact_reply["structuredContent"]["rows"],
                artifact_cli["rows"]
            );
            assert_eq!(
                artifact_cli["rows"]["native_result"]["results"]["bindings"][0]["count"]["value"]
                    .as_str(),
                Some("1")
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_graph_pages_admit_repeated_projection_names_before_agent_rendering() {
    use purrdf_lex::json::{self, Object};
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let store = client.discovery().store_id.clone();
            client
                .ingest("name-price-input", "name-price", wire::Format::Json, b"{}")
                .await
                .unwrap();
            let variable = "v".repeat(5000);
            let values = (0..100)
                .map(|n| format!("<urn:row:{n}>"))
                .collect::<Vec<_>>()
                .join(" ");
            let query = format!("SELECT ?{variable} WHERE {{ VALUES ?{variable} {{ {values} }} }}");
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            let first = mcp
                .tool(
                    2,
                    "sparql_query",
                    Object::new()
                        .with("store_id", store.clone())
                        .with("query", query)
                        .with("page_size", 1000_u32)
                        .into(),
                )
                .await;
            assert_eq!(first["isError"].as_bool(), Some(false), "{first:?}");
            let mut page = first["structuredContent"].clone();
            let snapshot = page["snapshot_id"].as_str().unwrap().to_owned();
            let mut pages = 0;
            let mut actual = std::collections::BTreeSet::new();
            let mut count = 0;
            loop {
                pages += 1;
                assert!(json::write_compact(&page).len() <= 256 * 1024);
                assert_eq!(page["answer_certainty"].as_str(), Some("exact"));
                let native = purrdf::sparql::from_json(
                    json::write_compact(&page["rows"]["native_result"]).as_bytes(),
                )
                .unwrap();
                assert_eq!(native.variables, vec![variable.clone()]);
                for row in native.rows {
                    count += 1;
                    assert!(actual.insert(row.into_iter().next().unwrap().unwrap()));
                }
                let Some(cursor) = page["next_cursor"].as_str() else {
                    break;
                };
                page = cli(&socket, ["result-next", &store, cursor].as_slice());
                assert_eq!(page["snapshot_id"].as_str(), Some(snapshot.as_str()));
                assert!(pages <= 100);
            }
            assert!(
                pages > 1,
                "long native SRJ field names must affect transport page boundaries"
            );
            assert_eq!(count, 100);
            assert_eq!(
                actual,
                (0..100)
                    .map(|n| purrdf::TermValue::iri(format!("urn:row:{n}")))
                    .collect()
            );
            assert!(
                sparrow_client::release_result(client.capability(), &store, &snapshot)
                    .await
                    .unwrap()
                    .released
            );
            let status = cli(&socket, &["status"]);
            assert_eq!(status["sparql_read_handles"].as_str(), Some("0"));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_graph_queries_preserve_forms_certificates_and_leases_over_tcp_cli_and_mcp() {
    use purrdf_lex::json::{self, Object, Value};
    tokio::task::LocalSet::new().run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store"); let socket = data.join("rpc.sock");
        let config = write_server_config(dir.path(),&data,&socket);
        let (address,reservation) = configure_tcp_listener(&config); drop(reservation);
        let _daemon = start(&config,&socket).await;
        let endpoint = format!("tcp://{address}");
        let client = sparrow_client::Client::connect_endpoint(&sparrow_client::Endpoint::parse(&endpoint).unwrap()).await.unwrap();
        let store = client.discovery().store_id.clone();
        client.ingest("graph-input","graph-source",wire::Format::Json,br#"{"message":"native graph"}"#).await.unwrap();
        let mut mcp = Mcp::start(&socket); mcp.initialize(1).await;
        let path = dir.path().join("graph.json");
        for (index,query) in [
            "SELECT ?record WHERE { ?record ?predicate ?payload }",
            "ASK { ?record <urn:sparrow:vocab:payload> ?payload }",
            "CONSTRUCT { ?record <urn:copy:payload> ?payload } WHERE { ?record <urn:sparrow:vocab:payload> ?payload }",
            "DESCRIBE ?record WHERE { ?record <urn:sparrow:vocab:payload> ?payload }",
        ].into_iter().enumerate() {
            let mut args = Object::new().with("store_id",store.clone()).with("query",query).with("page_size",1_u32);
            if index==0 {
                let limits = purrdf::projections::ProjectionLimits::new(1,262144,262144,262144,16).unwrap();
                let term = purrdf::projections::ProjectionTerm::from_term_value(&purrdf::TermValue::iri("urn:sparrow:vocab:payload"),limits).unwrap().to_canonical_json(limits).unwrap();
                let term = json::read_slice(&term,json::Limits::default()).unwrap();
                args = args.with("bindings",Value::array([Object::new().with("name","predicate").with("term",term)]));
            }
            let args: Value = args.into();
            std::fs::write(&path,json::write_compact(&args)).unwrap();
            let first = cli(&endpoint,&["sparql",path.to_str().unwrap()]);
            assert_eq!(first["execution_status"].as_str(),Some("complete"),"{first:?}");
            assert_eq!(first["answer_certainty"].as_str(),Some("exact"));
            assert_eq!(first["coverage"]["selected_records"].as_str(),Some("1"));
            assert!(first["source_evidence"]["row_visits"].as_str().unwrap().parse::<u64>().unwrap()>0);
            assert!(first["native_governor"]["resources"].as_array().unwrap().len()==8);
            if index==0 { assert_eq!(first["rows"]["native_result"]["results"]["bindings"].as_array().unwrap().len(),1); }
            if index==1 { assert_eq!(first["rows"]["native_result"]["boolean"].as_bool(),Some(true)); }
            if index>=2 { assert!(first["rows"]["native_result"]["graph"].as_str().is_some()); }
            let mut page = first.clone(); let mut pages = 1;
            while let Some(cursor) = page["next_cursor"].as_str() {
                let next = mcp.tool(10+index as u64,"result_next",Object::new().with("store_id",store.clone()).with("cursor",cursor).into()).await;
                assert_eq!(next["isError"].as_bool(),Some(false),"{next:?}");
                let replay = cli(&endpoint,&["result-next",&store,cursor]);
                page = next["structuredContent"].clone(); assert_eq!(page,replay);
                assert_eq!(page["snapshot_id"],first["snapshot_id"]); assert_eq!(page["native_governor"],first["native_governor"]); pages+=1; assert!(pages<100);
            }
            let released = sparrow_client::release_result(client.capability(),&store,first["snapshot_id"].as_str().unwrap()).await.unwrap(); assert_eq!(released.released,first["has_more"].as_bool().unwrap());
            if let Some(cursor) = first["next_cursor"].as_str() { assert!(sparrow_server::run_cli(client.capability(),&["result-next".into(),store.clone(),cursor.into()]).await.is_err()); }
        }
        for (id,query,fuel,certainty) in [(30,"SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }","0","certain"),(31,"ASK { ?s ?p ?o }","0","unknown")] {
            let response = mcp.tool(id,"sparql_query",Object::new().with("store_id",store.clone()).with("query",query).with("limits",Object::new().with("fuel",fuel)).into()).await;
            assert_eq!(response["isError"].as_bool(),Some(false),"{response:?}");
            let result = &response["structuredContent"]; assert_eq!(result["answer_certainty"].as_str(),Some(certainty)); assert_eq!(result["execution_status"].as_str(),Some("partial"));
            if certainty=="unknown" { assert!(result.get("rows").is_none()); assert!(result.get("candidates").is_none()); }
            sparrow_client::release_result(client.capability(),&store,result["snapshot_id"].as_str().unwrap()).await.unwrap();
        }
        let minus = "SELECT ?r WHERE { ?r <urn:sparrow:vocab:payload> ?p MINUS { ?r <urn:sparrow:vocab:payload> ?q } }";
        let baseline = mcp.tool(39,"sparql_query",Object::new().with("store_id",store.clone()).with("query",minus).into()).await;
        assert_eq!(baseline["isError"].as_bool(),Some(false),"{baseline:?}");
        let measured = baseline["structuredContent"]["native_governor"]["resources"][0]["consumed"].as_str().unwrap().parse::<u64>().unwrap();
        assert!(measured<=1024,"tiny MINUS fixture exceeded finite fuel-cut qualification admission");
        sparrow_client::release_result(client.capability(),&store,baseline["structuredContent"]["snapshot_id"].as_str().unwrap()).await.unwrap();
        let mut saw_upper_bound = false;
        for fuel in 0..measured {
            let reply = mcp.tool(39,"sparql_query",Object::new().with("store_id",store.clone()).with("query",minus).with("limits",Object::new().with("fuel",fuel.to_string())).into()).await;
            assert_eq!(reply["isError"].as_bool(),Some(false),"{reply:?}");let receipt=&reply["structuredContent"];
            sparrow_client::release_result(client.capability(),&store,receipt["snapshot_id"].as_str().unwrap()).await.unwrap();
            if receipt["answer_certainty"].as_str()==Some("at_most") { assert!(receipt.get("rows").is_none());assert!(receipt.get("candidates").is_some());saw_upper_bound=true;break; }
        }
        assert!(saw_upper_bound,"MINUS did not expose the native at-most candidate certificate");
        let failed = mcp.tool(40,"sparql_query",Object::new().with("store_id",store.clone()).with("query","SELECT WHERE {").into()).await;
        assert_eq!(failed["isError"].as_bool(),Some(true),"{failed:?}");
        let receipt = &failed["structuredContent"]; assert_eq!(receipt["execution_status"].as_str(),Some("failed")); assert!(receipt["diagnostic"]["code"].as_str().is_some()); assert!(receipt.get("native_governor").is_some());
        sparrow_client::release_result(client.capability(),&store,receipt["snapshot_id"].as_str().unwrap()).await.unwrap();
        std::fs::write(&path,json::write_compact(&Object::new().with("store_id",store.clone()).with("query","SELECT WHERE {").into())).unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_sparrow")).arg(&endpoint).args(["sparql",path.to_str().unwrap()]).admitted_output().unwrap();
        assert!(!output.status.success());let output=json::read_slice(&output.stdout,json::Limits::default()).unwrap();assert_eq!(output["execution_status"].as_str(),Some("failed"));
        sparrow_client::release_result(client.capability(),&store,output["snapshot_id"].as_str().unwrap()).await.unwrap();
        let mut bad = client.capability().sparql_query_request(); let mut query=bad.get().init_query(); query.set_store_id(&store);query.set_schema_version(sparrow_client::SCHEMA_VERSION);query.set_query("SELECT ?x WHERE {}");let mut scope = query.reborrow().init_scope(); scope.set_schema_version(sparrow_client::SCHEMA_VERSION); scope.init_received_before().set_unix_millis(u64::MAX);
        let mut nodes = query.reborrow().init_terms(1); let mut triple = nodes.reborrow().get(0).init_triple();triple.set_subject(0);triple.set_predicate(0);triple.set_object(0);
        let mut binding = query.init_bindings(1).get(0);binding.set_name("x");binding.set_term(0);
        let response = bad.send().promise.await.unwrap(); match response.get().unwrap().get_result().unwrap().which().unwrap() { wire::result::Error(error)=>{ let error = error.unwrap(); assert_eq!(error.get_code().unwrap().to_str().unwrap(),"INVALID_ARGUMENT"); assert!(error.get_message().unwrap().to_str().unwrap().contains("earlier nodes")); },_=>panic!("cyclic wire term was admitted") }
        let status = client.capability().status_request().send().promise.await.unwrap(); assert_eq!(rpc_ok(status.get().unwrap().get_result().unwrap()).get_sparql_read_handles(),0);
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn typed_queries_and_exact_bytes_survive_daemon_crash() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let store = dir.path().join("store");
            let socket = store.join("rpc.sock");
            let config = write_server_config(dir.path(), &store, &socket);
            let daemon = start(&config, &socket).await;
            let first = exercise(&socket).await;
            let cursor = mcp_pin_pages(&socket, &first.0, &first.1, &first.2).await;
            drop(daemon); // Only this test's child is terminated.
            std::fs::remove_file(&socket).unwrap(); // Its dead test endpoint, never a live listener.
            let daemon = start(&config, &socket).await;
            let expired = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                .arg(&socket)
                .args([
                    "pin-get",
                    &first.0,
                    "--name",
                    "page-case",
                    "--page-size",
                    "1",
                    "--cursor",
                    &cursor,
                ])
                .admitted_output()
                .unwrap();
            assert!(!expired.status.success());
            assert!(expired.stdout.is_empty());
            assert!(String::from_utf8_lossy(&expired.stderr).contains("RESULT_EXPIRED"));
            assert_eq!(exercise(&socket).await, first);
            drop(daemon);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn artifact_diagnostic_scope_reports_unknown_membership_through_cli_and_mcp() {
    use purrdf_lex::json::{Object, Value};
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let configuration = sparrow_server::ServerConfig::read(&config).unwrap();
            let owner = configuration.open_store().unwrap();
            owner
                .define_diagnostic_profile(
                    "mapped",
                    [(sparrow_core::DiagnosticDimension::Service, "/svc".into())].into(),
                )
                .unwrap();
            let clock = sparrow_server::now().unwrap();
            owner
                .ingest(
                    "wire-artifact-filter-a",
                    "mapped",
                    "jsonl",
                    b"{\"svc\":\"worker\"}\n{}\n{\"svc\":\"other\"}\n",
                    clock,
                )
                .unwrap();
            owner
                .ingest(
                    "wire-artifact-filter-unknown",
                    "mapped",
                    "jsonl",
                    b"{}\n",
                    clock,
                )
                .unwrap();
            owner
                .ingest(
                    "wire-artifact-filter-excluded",
                    "excluded",
                    "jsonl",
                    b"{}\n",
                    clock,
                )
                .unwrap();
            drop(owner);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let store = client.discovery().store_id.clone();
            let scope = Value::from(
                Object::new().with("kind", "live").with(
                    "filter",
                    Object::new()
                        .with("source_ids", Value::array(["mapped"]))
                        .with("service", Value::array(["worker"])),
                ),
            );
            let scope_file = dir.path().join("scope.json");
            std::fs::write(&scope_file, purrdf_lex::json::write_compact(&scope)).unwrap();
            let path = scope_file.to_str().unwrap();
            let result = cli(&socket, &["artifacts", &store, "--scope-file", path]);
            assert_eq!(result["answer_certainty"].as_str(), Some("incomplete"));
            assert_eq!(result["known_matching_artifacts"].as_str(), Some("1"));
            assert_eq!(result["matching_artifacts"], Value::Null);
            assert_eq!(
                result["coverage"]["diagnostic_scope"]["unknown"].as_str(),
                Some("2")
            );
            assert_eq!(result["rows"][0]["retained_records"].as_str(), Some("3"));
            assert_eq!(
                result["rows"][0]["coverage"]["known_eligible_records"].as_str(),
                Some("1")
            );
            assert_eq!(
                result["rows"][0]["coverage"]["eligible_records"],
                Value::Null
            );
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            let response = mcp
                .tool(
                    2,
                    "artifacts_list",
                    Object::new()
                        .with("store_id", store.clone())
                        .with("scope", scope.clone())
                        .into(),
                )
                .await;
            assert!(!response["isError"].as_bool().unwrap());
            let payload = &response["structuredContent"];
            assert_eq!(payload["rows"], result["rows"]);
            assert_eq!(payload["coverage"], result["coverage"]);
            let strict = mcp
                .tool(
                    3,
                    "artifacts_list",
                    Object::new()
                        .with("store_id", store.clone())
                        .with("scope", scope.clone())
                        .with("require_complete_index", true)
                        .into(),
                )
                .await;
            assert!(strict["isError"].as_bool().unwrap());
            let output = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                .arg(&socket)
                .args([
                    "artifacts",
                    &store,
                    "--scope-file",
                    path,
                    "--require-complete-index",
                ])
                .admitted_output()
                .unwrap();
            assert!(!output.status.success() && output.stdout.is_empty());
            assert!(String::from_utf8_lossy(&output.stderr).contains("INDEX_NOT_READY"));
            let excluded = cli(
                &socket,
                &[
                    "artifacts",
                    &store,
                    "--scope-file",
                    path,
                    "--source",
                    "excluded",
                    "--require-complete-index",
                ],
            );
            assert_eq!(excluded["matching_artifacts"].as_str(), Some("0"));
            assert_eq!(excluded["answer_certainty"].as_str(), Some("exact"));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn record_pages_share_frozen_ranked_receipts_after_pin_release_in_cli_and_mcp() {
    use purrdf_lex::json::{Object, Value};
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let configuration = sparrow_server::ServerConfig::read(&config).unwrap();
            let owner = configuration.open_store().unwrap();
            let clock = sparrow_server::now().unwrap();
            let old = clock - 31 * 86400000;
            let artifact = owner
                .ingest(
                    "wire-record-page-old",
                    "page-source",
                    "log",
                    b"needle a\nneedle needle b\nneedle c\nneedle needle needle d\nneedle e\n",
                    old,
                )
                .unwrap();
            let pin = owner
                .pin("wire-record-page-pin", "paged", &[artifact.id], 40, old)
                .unwrap();
            drop(owner);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let store = client.discovery().store_id.clone();
            let scope = Value::from(
                Object::new()
                    .with("kind", "pin")
                    .with("store_id", store.clone())
                    .with("pin_id", pin.id.clone())
                    .with("expected_revision", pin.revision.to_string()),
            );
            let scope_file = dir.path().join("scope.json");
            std::fs::write(&scope_file, purrdf_lex::json::write_compact(&scope)).unwrap();
            let path = scope_file.to_str().unwrap();
            let first = cli(
                &socket,
                &[
                    "records",
                    "--scope-file",
                    path,
                    "--text",
                    "needle",
                    "--page-size",
                    "2",
                ],
            );
            assert_eq!(first["receipt"]["execution"].as_str(), Some("complete"));
            assert_eq!(first["page"]["total_items"].as_str(), Some("5"));
            assert_eq!(first["qualified_records"].as_str(), Some("5"));
            assert_eq!(first["rows"].as_array().unwrap().len(), 2);
            let cursor = first["page"]["next_cursor"].as_str().unwrap();
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            let released = mcp
                .tool(
                    2,
                    "pin_release",
                    Object::new()
                        .with("request_id", "wire-record-page-release")
                        .with("store_id", store.clone())
                        .with("pin_id", pin.id)
                        .into(),
                )
                .await;
            assert!(!released["isError"].as_bool().unwrap());
            let second = cli(
                &socket,
                &[
                    "records",
                    "--scope-file",
                    path,
                    "--text",
                    "needle",
                    "--page-size",
                    "2",
                    "--cursor",
                    cursor,
                ],
            );
            let replay = mcp
                .tool(
                    3,
                    "records_search",
                    Object::new()
                        .with("scope", scope.clone())
                        .with("text", "needle")
                        .with("page_size", 2_u32)
                        .with("cursor", cursor)
                        .into(),
                )
                .await;
            assert!(!replay["isError"].as_bool().unwrap());
            assert_eq!(replay["structuredContent"], second);
            assert_eq!(first["receipt"], second["receipt"]);
            assert_eq!(first["text"]["ranking"], second["text"]["ranking"]);
            assert_eq!(first["page"]["snapshot_id"], second["page"]["snapshot_id"]);
            let third = cli(
                &socket,
                &[
                    "records",
                    "--scope-file",
                    path,
                    "--text",
                    "needle",
                    "--page-size",
                    "2",
                    "--cursor",
                    second["page"]["next_cursor"].as_str().unwrap(),
                ],
            );
            let ids = [first.clone(), second.clone(), third.clone()]
                .iter()
                .flat_map(|page| {
                    page["rows"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|row| row["record_id"].as_str().unwrap().to_owned())
                        .collect::<Vec<_>>()
                })
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(ids.len(), 5);
            assert!(!third["page"]["has_more"].as_bool().unwrap());
            for (arguments, code) in [
                (
                    vec![
                        "records",
                        "--scope-file",
                        path,
                        "--text",
                        "needle",
                        "--page-size",
                        "2",
                    ],
                    "NOT_FOUND",
                ),
                (
                    vec![
                        "records",
                        "--scope-file",
                        path,
                        "--text",
                        "other",
                        "--page-size",
                        "2",
                        "--cursor",
                        cursor,
                    ],
                    "CONFLICT",
                ),
                (
                    vec![
                        "records",
                        "--scope-file",
                        path,
                        "--text",
                        "needle",
                        "--page-size",
                        "1",
                        "--cursor",
                        cursor,
                    ],
                    "CONFLICT",
                ),
                (
                    vec![
                        "records",
                        "--scope-file",
                        path,
                        "--limit",
                        "1",
                        "--page-size",
                        "2",
                    ],
                    "INVALID_ARGUMENT",
                ),
            ] {
                let output = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                    .arg(&socket)
                    .args(arguments)
                    .admitted_output()
                    .unwrap();
                assert!(!output.status.success() && output.stdout.is_empty());
                assert!(
                    String::from_utf8_lossy(&output.stderr).contains(code),
                    "{output:?}"
                );
            }
            let status = cli(&socket, &["status"]);
            assert_eq!(status["record_read_handles"].as_str(), Some("1"));
            assert_eq!(status["max_record_read_handles"].as_str(), Some("8"));
            assert_eq!(status["record_read_lease_ms"].as_str(), Some("60000"));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn record_pages_enforce_agent_byte_ceilings_and_scan_refusal() {
    use purrdf_lex::json::{Object, Value};
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let key = "k".repeat(2048);
            let line = format!("{{\"{key}\":1}}\n");
            client
                .ingest(
                    "wire-record-page-bytes",
                    "page-source",
                    wire::Format::Jsonl,
                    line.repeat(20).as_bytes(),
                )
                .await
                .unwrap();
            let fields =
                Value::array((0..4).map(|_| Object::new().with("path", format!("/{key}"))));
            let where_file = dir.path().join("fields.json");
            std::fs::write(&where_file, purrdf_lex::json::write_compact(&fields)).unwrap();
            let path = where_file.to_str().unwrap();
            let first = cli(
                &socket,
                &[
                    "records",
                    "--source",
                    "page-source",
                    "--where-file",
                    path,
                    "--page-size",
                    "100",
                ],
            );
            assert!(
                purrdf_lex::json::write_compact(&first).len() <= sparrow_core::PIN_READ_PAGE_BYTES
            );
            assert!(
                first["rows"].as_array().unwrap().len() > 1
                    && first["rows"].as_array().unwrap().len() < 20
            );
            assert_eq!(first["page"]["total_items"].as_str(), Some("20"));
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            let cursor = first["page"]["next_cursor"].as_str().unwrap();
            let continuation = mcp
                .tool(
                    2,
                    "records_search",
                    Object::new()
                        .with("source", "page-source")
                        .with("fields", fields.clone())
                        .with("page_size", 100_u32)
                        .with("cursor", cursor)
                        .into(),
                )
                .await;
            assert!(!continuation["isError"].as_bool().unwrap());
            assert!(
                purrdf_lex::json::write_compact(&continuation).len()
                    <= sparrow_core::PIN_READ_PAGE_BYTES
            );
            let output = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                .arg(&socket)
                .args(["records", "--source", "page-source", "--scan-budget", "1"])
                .admitted_output()
                .unwrap();
            assert!(!output.status.success() && output.stdout.is_empty());
            assert!(String::from_utf8_lossy(&output.stderr).contains("RESOURCE_LIMIT"));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn standalone_text_pages_freeze_native_ranking_and_refuse_cross_operation_cursors() {
    use purrdf_lex::json::Object;
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            client
                .ingest(
                    "wire-text-page-initial",
                    "text-pages",
                    wire::Format::Log,
                    b"needle a\nneedle needle b\nneedle c\n",
                )
                .await
                .unwrap();
            let first = cli(
                &socket,
                &[
                    "text",
                    "needle",
                    "--source",
                    "text-pages",
                    "--page-size",
                    "1",
                ],
            );
            assert_eq!(first["page"]["total_items"].as_str(), Some("3"));
            assert_eq!(first["receipt"]["execution"].as_str(), Some("complete"));
            assert_eq!(first["ranking"]["documents"].as_str(), Some("3"));
            let cursor = first["page"]["next_cursor"].as_str().unwrap();
            client
                .ingest(
                    "wire-text-page-later",
                    "text-pages",
                    wire::Format::Log,
                    b"needle later\n",
                )
                .await
                .unwrap();
            let second = cli(
                &socket,
                &[
                    "text",
                    "needle",
                    "--source",
                    "text-pages",
                    "--page-size",
                    "1",
                    "--cursor",
                    cursor,
                ],
            );
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            let replay = mcp
                .tool(
                    2,
                    "text_search",
                    Object::new()
                        .with("text", "needle")
                        .with("source", "text-pages")
                        .with("page_size", 1_u32)
                        .with("cursor", cursor)
                        .into(),
                )
                .await;
            assert!(!replay["isError"].as_bool().unwrap());
            assert_eq!(replay["structuredContent"], second);
            assert_eq!(second["ranking"], first["ranking"]);
            assert_eq!(second["receipt"], first["receipt"]);
            assert_eq!(second["page"]["snapshot_id"], first["page"]["snapshot_id"]);
            let last = cli(
                &socket,
                &[
                    "text",
                    "needle",
                    "--source",
                    "text-pages",
                    "--page-size",
                    "1",
                    "--cursor",
                    second["page"]["next_cursor"].as_str().unwrap(),
                ],
            );
            let captured_ids = [&first, &second, &last]
                .iter()
                .flat_map(|page| {
                    page["rows"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|row| row["record_id"].as_str().unwrap().to_owned())
                        .collect::<Vec<_>>()
                })
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(captured_ids.len(), 3);
            assert!(!last["page"]["has_more"].as_bool().unwrap());
            let current = cli(&socket, &["text", "needle", "--source", "text-pages"]);
            assert_eq!(current["page"]["total_items"].as_str(), Some("4"));
            assert_ne!(
                current["ranking"]["corpus_id"],
                first["ranking"]["corpus_id"]
            );
            let record_page = cli(
                &socket,
                &[
                    "records",
                    "--text",
                    "needle",
                    "--source",
                    "text-pages",
                    "--page-size",
                    "1",
                ],
            );
            let record_cursor = record_page["page"]["next_cursor"].as_str().unwrap();
            for (arguments, code) in [
                (
                    vec![
                        "records",
                        "--text",
                        "needle",
                        "--source",
                        "text-pages",
                        "--page-size",
                        "1",
                        "--cursor",
                        cursor,
                    ],
                    "CONFLICT",
                ),
                (
                    vec![
                        "text",
                        "needle",
                        "--source",
                        "text-pages",
                        "--page-size",
                        "1",
                        "--cursor",
                        record_cursor,
                    ],
                    "CONFLICT",
                ),
                (
                    vec![
                        "text",
                        "other",
                        "--source",
                        "text-pages",
                        "--page-size",
                        "1",
                        "--cursor",
                        cursor,
                    ],
                    "CONFLICT",
                ),
                (
                    vec![
                        "text",
                        "needle",
                        "--source",
                        "text-pages",
                        "--scan-budget",
                        "1",
                    ],
                    "RESOURCE_LIMIT",
                ),
                (
                    vec!["text", "needle", "--limit", "1", "--page-size", "1"],
                    "INVALID_ARGUMENT",
                ),
            ] {
                let output = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                    .arg(&socket)
                    .args(arguments)
                    .admitted_output()
                    .unwrap();
                assert!(!output.status.success() && output.stdout.is_empty());
                assert!(
                    String::from_utf8_lossy(&output.stderr).contains(code),
                    "{output:?}"
                );
            }
            let mut request = client.capability().text_search_request();
            let mut query = request.get().init_query();
            query.set_text("needle");
            query.init_scope().set_schema_version(28);
            let response = request.send().promise.await.unwrap();
            match response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                wire::result::Error(error) => assert_eq!(
                    error.unwrap().get_code().unwrap().to_str().unwrap(),
                    "UNSUPPORTED"
                ),
                wire::result::Ok(_) => panic!("obsolete standalone text schema admitted"),
            }
            let status = cli(&socket, &["status"]);
            assert_eq!(status["record_read_handles"].as_str(), Some("2"));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn json_pages_replay_cli_mcp_counts_and_occurrences_and_bind_operations() {
    use purrdf_lex::json::Object;
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            client
                .ingest(
                    "wire-json-pages",
                    "json-pages",
                    wire::Format::Jsonl,
                    br#"{"x":"a","x":"b"}
{"x":"c"}
"#,
                )
                .await
                .unwrap();
            let keys = cli(
                &socket,
                &["keys", "--source", "json-pages", "--page-size", "1"],
            );
            assert_eq!(keys["page"]["total_items"].as_str(), Some("2"));
            let kc = keys["page"]["next_cursor"].as_str().unwrap();
            let values = cli(
                &socket,
                &[
                    "values",
                    "--source",
                    "json-pages",
                    "--path",
                    "/x",
                    "--page-size",
                    "1",
                ],
            );
            assert_eq!(values["page"]["total_items"].as_str(), Some("3"));
            let vc = values["page"]["next_cursor"].as_str().unwrap();
            client
                .ingest(
                    "wire-json-pages-later",
                    "json-pages",
                    wire::Format::Json,
                    br#"{"x":"d","later":true}"#,
                )
                .await
                .unwrap();
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            for (id, command, tool, first, cursor) in [
                (2, "keys", "keys_list", &keys, kc),
                (3, "values", "values_search", &values, vc),
            ] {
                let mut args = vec![
                    command,
                    "--source",
                    "json-pages",
                    "--page-size",
                    "1",
                    "--cursor",
                    cursor,
                ];
                let mut input = Object::new()
                    .with("source", "json-pages")
                    .with("page_size", 1_u32)
                    .with("cursor", cursor);
                if command == "values" {
                    args.extend(["--path", "/x"]);
                    input = input.with("path", "/x");
                }
                let page = cli(&socket, &args);
                let replay = mcp.tool(id, tool, input.into()).await;
                assert!(!replay["isError"].as_bool().unwrap());
                assert_eq!(replay["structuredContent"], page);
                assert_eq!(page["receipt"], first["receipt"]);
                assert_eq!(page["page"]["snapshot_id"], first["page"]["snapshot_id"]);
                assert_eq!(page["receipt"]["execution"].as_str(), Some("complete"));
                if command == "keys" {
                    assert_eq!(page["rows"][0]["occurrences"].as_str(), Some("3"));
                    assert_eq!(page["rows"][0]["records"].as_str(), Some("2"));
                } else {
                    let third = cli(
                        &socket,
                        &[
                            "values",
                            "--source",
                            "json-pages",
                            "--path",
                            "/x",
                            "--page-size",
                            "1",
                            "--cursor",
                            page["page"]["next_cursor"].as_str().unwrap(),
                        ],
                    );
                    let captured = [first, &page, &third]
                        .into_iter()
                        .map(|p| p["rows"][0]["decoded"].as_str().unwrap())
                        .collect::<std::collections::BTreeSet<_>>();
                    assert_eq!(captured, ["a", "b", "c"].into_iter().collect());
                    assert!(!third["page"]["has_more"].as_bool().unwrap());
                }
            }
            for (args, code) in [
                (
                    vec![
                        "keys",
                        "--source",
                        "json-pages",
                        "--page-size",
                        "1",
                        "--cursor",
                        vc,
                    ],
                    "CONFLICT",
                ),
                (
                    vec![
                        "values",
                        "--source",
                        "json-pages",
                        "--path",
                        "/x",
                        "--page-size",
                        "1",
                        "--cursor",
                        kc,
                    ],
                    "CONFLICT",
                ),
                (
                    vec![
                        "values",
                        "--source",
                        "json-pages",
                        "--path",
                        "/later",
                        "--page-size",
                        "1",
                        "--cursor",
                        vc,
                    ],
                    "CONFLICT",
                ),
                (
                    vec!["keys", "--source", "json-pages", "--scan-budget", "1"],
                    "RESOURCE_LIMIT",
                ),
                (
                    vec!["keys", "--limit", "1", "--page-size", "1"],
                    "INVALID_ARGUMENT",
                ),
                (
                    vec!["values", "--page-size", "1", "--limit", "1"],
                    "INVALID_ARGUMENT",
                ),
            ] {
                let output = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                    .arg(&socket)
                    .args(args)
                    .admitted_output()
                    .unwrap();
                assert!(!output.status.success() && output.stdout.is_empty());
                assert!(
                    String::from_utf8_lossy(&output.stderr).contains(code),
                    "{output:?}"
                );
            }
            let mut request = client.capability().keys_list_request();
            request.get().init_scope().set_schema_version(29);
            let response = request.send().promise.await.unwrap();
            match response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                wire::result::Error(error) => assert_eq!(
                    error.unwrap().get_code().unwrap().to_str().unwrap(),
                    "UNSUPPORTED"
                ),
                wire::result::Ok(_) => panic!("obsolete JSON pagination schema admitted"),
            }
            let status = cli(&socket, &["status"]);
            assert_eq!(status["record_read_handles"].as_str(), Some("2"));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn large_json_disk_pages_share_tcp_unix_cli_mcp_and_typed_limits() {
    use purrdf_lex::json::Object;
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let text = std::fs::read_to_string(&config)
                .unwrap()
                .replace("disk_budget = 134217728", "disk_budget = 1073741824");
            std::fs::write(&config, text).unwrap();
            let (address, reservation) = configure_tcp_listener(&config);
            drop(reservation);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let (input, _) = sparrow_test_support::wide_jsonl_keys(250, 50, 200);
            let path = dir.path().join("wide.jsonl");
            std::fs::write(&path, &input).unwrap();
            let receipt = client
                .import_file("wire-wide-json", "wide-json", wire::Format::Jsonl, &path)
                .await
                .unwrap();
            let tcp = format!("tcp://{address}");
            for endpoint in [socket.to_str().unwrap(), tcp.as_str()] {
                let mut mcp = Mcp::start(endpoint);
                mcp.initialize(1).await;
                let mut request_id = 2;
                for (command, total) in [("keys", 12501), ("values", 12500)] {
                    let mut args = vec![
                        command,
                        "--source",
                        "wide-json",
                        "--page-size",
                        "1000",
                        "--scan-budget",
                        "100000",
                    ];
                    if command == "values" {
                        args.extend(["--kind", "string", "--decoded", "needle"]);
                    }
                    let first = cli(endpoint, &args);
                    assert_eq!(
                        first["page"]["total_items"]
                            .as_str()
                            .unwrap()
                            .parse::<usize>()
                            .unwrap(),
                        total
                    );
                    assert_eq!(
                        first["receipt"]["limits"]["capture_bytes"].as_str(),
                        Some("67108864")
                    );
                    assert_eq!(
                        first["receipt"]["limits"]["capture_items"].as_str(),
                        Some("100000")
                    );
                    let aggregation = &first["receipt"]["limits"]["key_aggregation_bytes"];
                    if command == "keys" {
                        assert_eq!(aggregation.as_str(), Some("33554432"));
                    } else {
                        assert!(aggregation.is_null());
                    }
                    let snapshot = first["page"]["snapshot_id"].as_str().unwrap().to_owned();
                    let mut page = first;
                    let mut count = 0;
                    let mut bytes = 0;
                    loop {
                        count += page["rows"].as_array().unwrap().len();
                        for row in page["rows"].as_array().unwrap() {
                            bytes += purrdf_lex::json::write_compact(row).len();
                        }
                        let Some(cursor) = page["page"]["next_cursor"].as_str() else {
                            break;
                        };
                        let next = cli(endpoint, &["result-next", &receipt.store_id, cursor]);
                        let resumed = mcp
                            .tool(
                                request_id,
                                "result_next",
                                Object::new()
                                    .with("store_id", receipt.store_id.clone())
                                    .with("cursor", cursor)
                                    .into(),
                            )
                            .await;
                        request_id += 1;
                        assert!(!resumed["isError"].as_bool().unwrap());
                        assert!(
                            resumed["structuredContent"] == next,
                            "MCP discovery continuation differs from CLI"
                        );
                        page = next;
                    }
                    assert_eq!(count, total);
                    assert!(bytes > 2 * 1024 * 1024);
                    let released = cli(endpoint, &["result-release", &receipt.store_id, &snapshot]);
                    assert_eq!(released["released"].as_bool(), Some(true));
                }
                mcp.close(true).await;
            }
            assert_eq!(
                cli(&socket, &["status"])["record_read_handles"].as_str(),
                Some("0")
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn result_next_cli_mcp_and_typed_union_replay_every_existing_leased_page() {
    use purrdf_lex::json::{Object, Value};
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let a = client
                .ingest(
                    "next-wire-a",
                    "next-source",
                    wire::Format::Jsonl,
                    br#"{"message":"needle a"}
{"message":"needle b"}
"#,
                )
                .await
                .unwrap();
            let b = client
                .ingest(
                    "next-wire-b",
                    "other",
                    wire::Format::Json,
                    br#"{"message":"needle c"}"#,
                )
                .await
                .unwrap();
            let id = a.store_id.as_str();
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            let ordinary = cli(
                &socket,
                &["records", "--source", "next-source", "--text", "needle"],
            );
            assert!(ordinary["artifact_groups"].is_null());
            let grouped = cli(
                &socket,
                &[
                    "records",
                    "--source",
                    "next-source",
                    "--text",
                    "needle",
                    "--group-by-artifact",
                    "true",
                    "--page-size",
                    "1",
                ],
            );
            let group = &grouped["artifact_groups"][0];
            assert_eq!(group["artifact_id"].as_str(), Some(a.id.as_str()));
            assert_eq!(group["captured_matches"].as_str(), Some("2"));
            assert_eq!(group["page_indices"], Value::array([0_u32]));
            assert_eq!(grouped["rows"][0], ordinary["rows"][0]);
            let group_cursor = grouped["page"]["next_cursor"].as_str().unwrap();
            let next_group = cli(&socket, &["result-next", id, group_cursor]);
            assert_eq!(next_group["artifact_groups"], grouped["artifact_groups"]);
            assert_eq!(next_group["rows"][0], ordinary["rows"][1]);
            let replay = mcp
                .tool(
                    50,
                    "records_search",
                    Object::new()
                        .with("source", "next-source")
                        .with("text", "needle")
                        .with("group_by_artifact", true)
                        .with("page_size", 1_u32)
                        .with("cursor", group_cursor)
                        .into(),
                )
                .await;
            assert!(!replay["isError"].as_bool().unwrap());
            assert_eq!(replay["structuredContent"], next_group);
            let rejected = mcp
                .tool(
                    51,
                    "records_search",
                    Object::new()
                        .with("source", "next-source")
                        .with("text", "needle")
                        .with("page_size", 1_u32)
                        .with("cursor", group_cursor)
                        .into(),
                )
                .await;
            assert_eq!(
                rejected["structuredContent"]["error"]["code"].as_str(),
                Some("CONFLICT")
            );
            assert!(
                client
                    .release_result(grouped["page"]["snapshot_id"].as_str().unwrap())
                    .await
                    .unwrap()
                    .released
            );
            let empty = cli(
                &socket,
                &[
                    "records",
                    "--source",
                    "does-not-exist",
                    "--group-by-artifact",
                    "true",
                ],
            );
            assert_eq!(empty["artifact_groups"], Value::array(Vec::<Value>::new()));
            assert_eq!(empty["rows"], Value::array(Vec::<Value>::new()));
            for (number, invalid) in [Value::from("true"), Value::Null, Value::from(1_u32)]
                .into_iter()
                .enumerate()
            {
                let rejected = mcp
                    .tool(
                        number as u64 + 60,
                        "records_search",
                        Object::new().with("group_by_artifact", invalid).into(),
                    )
                    .await;
                assert_eq!(
                    rejected["structuredContent"]["error"]["code"].as_str(),
                    Some("INVALID_ARGUMENT")
                );
            }
            let pin = mcp
                .tool(
                    2,
                    "pin",
                    Object::new()
                        .with("request_id", "next-wire-pin-a")
                        .with("name", "next-first")
                        .with("store_id", id)
                        .with("days", 1_u32)
                        .with("targets", Value::array([a.id.as_str(), b.id.as_str()]))
                        .into(),
                )
                .await;
            let pin_id = pin["structuredContent"]["pin_id"].as_str().unwrap();
            let second_pin = mcp
                .tool(
                    3,
                    "pin",
                    Object::new()
                        .with("request_id", "next-wire-pin-b")
                        .with("name", "next-second")
                        .with("store_id", id)
                        .with("days", 1_u32)
                        .with("targets", Value::array([b.id.as_str()]))
                        .into(),
                )
                .await;
            assert!(!second_pin["isError"].as_bool().unwrap());
            let where_file = dir.path().join("where.json");
            std::fs::write(
                &where_file,
                purrdf_lex::json::write_compact(&Value::array([Value::from(
                    Object::new()
                        .with("path", "/message")
                        .with("kind", "string"),
                )])),
            )
            .unwrap();
            let commands = [
                vec![
                    "records",
                    "--source",
                    "next-source",
                    "--page-size",
                    "1",
                    "--where-file",
                    where_file.to_str().unwrap(),
                ],
                vec![
                    "text",
                    "needle",
                    "--source",
                    "next-source",
                    "--page-size",
                    "1",
                ],
                vec!["keys", "--source", "next-source", "--page-size", "1"],
                vec![
                    "values",
                    "--source",
                    "next-source",
                    "--path",
                    "/message",
                    "--page-size",
                    "1",
                ],
                vec!["artifacts", id, "--page-size", "1"],
                vec!["sources", id, "--page-size", "1"],
                vec!["pins", id, "--page-size", "1"],
                vec!["pin-get", id, "--id", pin_id, "--page-size", "1"],
            ];
            for (index, args) in commands.into_iter().enumerate() {
                let first = cli(&socket, &args);
                let cursor = if index < 4 {
                    first["page"]["next_cursor"].as_str()
                } else {
                    first["next_cursor"].as_str()
                }
                .unwrap();
                let mut repeat = args.clone();
                repeat.extend(["--cursor", cursor]);
                let expected = cli(&socket, &repeat);
                let resumed = cli(&socket, &["result-next", id, cursor]);
                assert_eq!(resumed, expected, "{}", args[0]);
                if index < 4 {
                    let receipt = &resumed["receipt"];
                    assert_eq!(receipt["snapshot_id"], resumed["page"]["snapshot_id"]);
                    assert_eq!(receipt["as_of"], resumed["page"]["as_of"]);
                    assert_eq!(receipt["execution_status"].as_str(), Some("complete"));
                    assert!(
                        receipt["stop_reason"].is_null() && receipt["native_governor"].is_null()
                    );
                    assert_eq!(receipt["limits"]["page_size"].as_u64(), Some(1));
                    assert_eq!(
                        receipt["limits"]["capture_bytes"].as_str(),
                        Some("67108864")
                    );
                    if index == 2 {
                        assert_eq!(
                            receipt["limits"]["key_aggregation_bytes"].as_str(),
                            Some("33554432")
                        );
                    } else {
                        assert!(receipt["limits"]["key_aggregation_bytes"].is_null());
                    }
                    assert_eq!(receipt["limits"]["lease_ms"].as_str(), Some("60000"));
                    assert_eq!(
                        receipt["profile_ids"].as_array().unwrap().len(),
                        if index == 1 { 5 } else { 2 }
                    );
                    if index >= 2 {
                        assert_eq!(
                            receipt["limits"]["json_value_visits"].as_str(),
                            Some("10000")
                        );
                        assert!(receipt["limits"]["field_visits"].is_null());
                    } else if index == 1 {
                        assert_eq!(receipt["limits"]["posting_visits"].as_str(), Some("10000"));
                    } else {
                        assert_eq!(receipt["limits"]["field_constraints"].as_str(), Some("32"));
                    }
                }
                assert_eq!(
                    resumed["operation"].as_str(),
                    Some(
                        [
                            "records_search",
                            "text_search",
                            "keys_list",
                            "values_search",
                            "artifacts_list",
                            "sources_list",
                            "pins_list",
                            "pin_get",
                        ][index]
                    )
                );
                if index == 0 {
                    for reply in [&first, &resumed] {
                        let fields = reply["rows"][0]["fields"].as_array().unwrap();
                        assert_eq!(fields.len(), 1);
                        assert!(fields[0].get("operation").is_none());
                    }
                }
                let replay = mcp
                    .tool(
                        index as u64 + 4,
                        "result_next",
                        Object::new()
                            .with("store_id", id)
                            .with("cursor", cursor)
                            .into(),
                    )
                    .await;
                assert!(!replay["isError"].as_bool().unwrap());
                assert_eq!(replay["structuredContent"], expected);
                let mut call = client.capability().result_next_request();
                let mut query = call.get().init_query();
                query.set_store_id(id);
                query.set_schema_version(sparrow_client::SCHEMA_VERSION);
                query.set_cursor(cursor);
                let response = call.send().promise.await.unwrap();
                let page = match response
                    .get()
                    .unwrap()
                    .get_result()
                    .unwrap()
                    .which()
                    .unwrap()
                {
                    wire::result::Ok(page) => page.unwrap(),
                    wire::result::Error(error) => panic!("{:?}", error.unwrap().get_code()),
                };
                let kind = match page.which().unwrap() {
                    wire::result_page::Records(_) => "records",
                    wire::result_page::Text(_) => "text",
                    wire::result_page::Keys(_) => "keys",
                    wire::result_page::Values(_) => "values",
                    wire::result_page::Artifacts(_) => "artifacts",
                    wire::result_page::Sources(_) => "sources",
                    wire::result_page::Pins(_) => args[0],
                    wire::result_page::Sparql(_) => "sparql",
                };
                assert_eq!(kind, args[0]);
                let snapshot = cursor.split_once('/').unwrap().0;
                let wrong_store = mcp
                    .tool(
                        index as u64 + 100,
                        "result_release",
                        Object::new()
                            .with("store_id", "store:foreign")
                            .with("snapshot_id", snapshot)
                            .into(),
                    )
                    .await;
                assert!(wrong_store["isError"].as_bool().unwrap());
                assert_eq!(cli(&socket, &["result-next", id, cursor]), expected);
                let released = match index % 3 {
                    0 => cli(&socket, &["result-release", id, snapshot]),
                    1 => {
                        let out = mcp
                            .tool(
                                index as u64 + 200,
                                "result_release",
                                Object::new()
                                    .with("store_id", id)
                                    .with("snapshot_id", snapshot)
                                    .into(),
                            )
                            .await;
                        assert!(!out["isError"].as_bool().unwrap());
                        out["structuredContent"].clone()
                    }
                    _ => {
                        let out = client.release_result(snapshot).await.unwrap();
                        Object::new()
                            .with("store_id", out.store_id)
                            .with("snapshot_id", out.snapshot_id)
                            .with("released", out.released)
                            .into()
                    }
                };
                assert_eq!(released["store_id"].as_str(), Some(id));
                assert_eq!(released["snapshot_id"].as_str(), Some(snapshot));
                assert_eq!(released["released"].as_bool(), Some(true));
                assert!(!client.release_result(snapshot).await.unwrap().released);
                let out = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                    .arg(&socket)
                    .args(["result-next", id, cursor])
                    .admitted_output()
                    .unwrap();
                assert!(!out.status.success() && out.stdout.is_empty());
                assert!(String::from_utf8_lossy(&out.stderr).contains("RESULT_EXPIRED"));
            }
            for (store, cursor, code) in [
                ("store:foreign", "record-read:unknown/1", "STORE_MISMATCH"),
                (id, "not-a-cursor", "INVALID_ARGUMENT"),
                (id, "record-read:unknown/1", "RESULT_EXPIRED"),
            ] {
                let out = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                    .arg(&socket)
                    .args(["result-next", store, cursor])
                    .admitted_output()
                    .unwrap();
                assert!(!out.status.success() && out.stdout.is_empty());
                assert!(String::from_utf8_lossy(&out.stderr).contains(code));
            }
            let rejected = mcp
                .tool(
                    30,
                    "result_next",
                    Object::new()
                        .with("store_id", id)
                        .with("cursor", "record-read:unknown/1")
                        .with("page_size", 1_u32)
                        .into(),
                )
                .await;
            assert!(rejected["isError"].as_bool().unwrap());
            let mut call = client.capability().result_next_request();
            let mut query = call.get().init_query();
            query.set_store_id(id);
            query.set_schema_version(30);
            query.set_cursor("record-read:unknown/1");
            let response = call.send().promise.await.unwrap();
            match response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                wire::result::Error(error) => assert_eq!(
                    error.unwrap().get_code().unwrap().to_str().unwrap(),
                    "UNSUPPORTED"
                ),
                wire::result::Ok(_) => panic!("obsolete continuation schema admitted"),
            }
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn context_capture_limits_and_byte_refusals_reach_typed_cli_and_mcp_callers() {
    use purrdf_lex::json::Object;
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let store = client.discovery().store_id.clone();
            let anchor = client
                .ingest(
                    "wire-context-anchor",
                    "target",
                    wire::Format::Log,
                    b"anchor\n",
                )
                .await
                .unwrap();
            client
                .ingest(
                    "wire-context-noise",
                    "target",
                    wire::Format::Log,
                    b"noise\nnoise\nnoise\n",
                )
                .await
                .unwrap();
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            let partial = mcp
                .tool(
                    2,
                    "records_context",
                    Object::new()
                        .with("store_id", store.clone())
                        .with("id", anchor.record_ids[0].clone())
                        .with(
                            "scope",
                            Object::new()
                                .with("kind", "selection")
                                .with("store_id", store.clone())
                                .with(
                                    "record_ids",
                                    purrdf_lex::json::Value::array([anchor.record_ids[0].as_str()]),
                                ),
                        )
                        .with("before", 1_u32)
                        .with("after", 1_u32)
                        .with("limit", 3_u32)
                        .with("scan_budget", 1_u32)
                        .into(),
                )
                .await;
            assert_eq!(partial["isError"].as_bool(), Some(false));
            let partial = &partial["structuredContent"];
            // Three skipped same-source members guarantee at least two on one side,
            // including when acceptance times tie and opaque IDs break the tie.
            assert!(
                ["before", "after"]
                    .into_iter()
                    .any(|side| { partial[side]["frontier"].as_str() == Some("scan_budget") })
            );
            assert_eq!(
                partial["receipt"]["execution_status"].as_str(),
                Some("partial")
            );
            assert_eq!(
                partial["receipt"]["stop_reason"].as_str(),
                Some("direction_scan_budget")
            );
            assert_eq!(partial["receipt"]["certainty"].as_str(), Some("incomplete"));
            assert!(partial["receipt"]["native_governor"].is_null());
            let source = "s".repeat(512);
            let input = client
                .ingest(
                    "wire-context-large",
                    &source,
                    wire::Format::Log,
                    &b"neighbor\n".repeat(151),
                )
                .await
                .unwrap();
            let mut ids = input.record_ids;
            ids.sort();
            let id = &ids[75];
            let mut request = client.capability().records_context_request();
            let mut query = request.get().init_query();
            let mut reference = query.reborrow().init_anchor();
            reference.set_store_id(&store);
            reference.set_id(id);
            query.set_before(40);
            query.set_after(0);
            let mut scope = query.init_scope();
            scope.set_schema_version(sparrow_client::SCHEMA_VERSION);
            scope.set_limit(1000);
            scope.set_scan_budget(1000);
            scope.init_received_before().set_unix_millis(u64::MAX);
            let response = request.send().promise.await.unwrap();
            let output = rpc_ok(response.get().unwrap().get_result().unwrap());
            let capture = output.get_capture().unwrap();
            assert!(
                capture
                    .get_snapshot_id()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with("context-read:")
            );
            assert_eq!(
                capture.get_as_of().unwrap().get_unix_millis(),
                output
                    .get_receipt()
                    .unwrap()
                    .get_membership_at()
                    .unwrap()
                    .get_unix_millis()
            );
            assert_eq!(
                capture.get_host_profile().unwrap().to_str().unwrap(),
                sparrow_core::CONTEXT_CAPTURE_PROFILE
            );
            assert!(matches!(
                capture.get_native_governor().unwrap().which().unwrap(),
                wire::optional_text::Absent(())
            ));
            assert_eq!(
                capture.get_limits().unwrap().get_catalog_bytes(),
                32 * 1024 * 1024
            );
            assert_eq!(
                capture.get_limits().unwrap().get_response_bytes(),
                256 * 1024
            );
            assert_eq!(output.get_before().unwrap().get_rows().unwrap().len(), 40);
            let args = Object::new()
                .with("store_id", store.clone())
                .with("id", id.as_str())
                .with("before", 80_u32)
                .with("after", 80_u32)
                .with("limit", 1000_u32)
                .with("scan_budget", 1000_u32);
            let refused = mcp.tool(3, "records_context", args.into()).await;
            assert_eq!(refused["isError"].as_bool(), Some(true));
            assert_eq!(
                refused["structuredContent"]["error"]["code"].as_str(),
                Some("RESOURCE_LIMIT")
            );
            assert!(refused["structuredContent"].get("anchor").is_none());
            let output = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                .arg(&socket)
                .args([
                    "context",
                    &store,
                    id,
                    "--before",
                    "80",
                    "--after",
                    "80",
                    "--limit",
                    "1000",
                    "--scan-budget",
                    "1000",
                ])
                .admitted_output()
                .unwrap();
            assert!(!output.status.success() && output.stdout.is_empty());
            assert!(String::from_utf8_lossy(&output.stderr).contains("RESOURCE_LIMIT"));
            let accepted = cli(
                &socket,
                &[
                    "context", &store, id, "--before", "40", "--after", "0", "--limit", "1000",
                ],
            );
            assert!(
                purrdf_lex::json::write_compact(&accepted).len()
                    <= sparrow_core::PIN_READ_PAGE_BYTES
            );
            assert_eq!(accepted["before"]["rows"].as_array().unwrap().len(), 40);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn key_paths_share_native_counts_cursor_binding_and_typed_cli_mcp_admission() {
    use purrdf_lex::json::Object;
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let store = client.discovery().store_id.clone();
            client
                .ingest(
                    "wire-key-paths",
                    "key-paths",
                    wire::Format::Jsonl,
                    br#"{"a/b":1,"a/b":2,"a/b":"one","m~n":true,"":null}
{"a/b":null,"a/b":"two","m~n":false}
"#,
                )
                .await
                .unwrap();
            let mut request = client.capability().keys_list_request();
            request.get().init_path().set_value("/a~1b");
            let mut scope = request.get().init_scope();
            scope.set_schema_version(sparrow_client::SCHEMA_VERSION);
            scope.set_limit(1);
            scope.set_scan_budget(1000);
            scope.init_received_before().set_unix_millis(u64::MAX);
            let response = request.send().promise.await.unwrap();
            let typed = rpc_ok(response.get().unwrap().get_result().unwrap());
            assert_eq!(typed.get_page().unwrap().get_total_items(), 3);
            assert_eq!(
                typed
                    .get_rows()
                    .unwrap()
                    .get(0)
                    .get_path()
                    .unwrap()
                    .to_str()
                    .unwrap(),
                "/a~1b"
            );
            let capture = match typed
                .get_receipt()
                .unwrap()
                .get_capture()
                .unwrap()
                .which()
                .unwrap()
            {
                wire::optional_query_capture::Value(value) => value.unwrap(),
                _ => panic!("missing key capture"),
            };
            assert!(matches!(
                capture
                    .get_limits()
                    .unwrap()
                    .get_query_path_bytes()
                    .unwrap()
                    .which()
                    .unwrap(),
                wire::optional_limit::Value(65536)
            ));
            let first = cli(
                &socket,
                &[
                    "keys",
                    "--source",
                    "key-paths",
                    "--path",
                    "/a~1b",
                    "--page-size",
                    "1",
                ],
            );
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            let same = mcp
                .tool(
                    2,
                    "keys_list",
                    Object::new()
                        .with("source", "key-paths")
                        .with("path", "/a~1b")
                        .with("page_size", 1_u32)
                        .into(),
                )
                .await;
            assert_eq!(same["isError"].as_bool(), Some(false));
            assert_eq!(same["structuredContent"]["rows"], first["rows"]);
            assert_eq!(first["page"]["total_items"].as_str(), Some("3"));
            assert_eq!(
                first["receipt"]["limits"]["query_path_bytes"].as_str(),
                Some("65536")
            );
            client
                .ingest(
                    "wire-key-paths-later",
                    "key-paths",
                    wire::Format::Jsonl,
                    b"{\"a/b\":3,\"a/b\":null}\n",
                )
                .await
                .unwrap();
            let cursor = first["page"]["next_cursor"].as_str().unwrap();
            let continued = cli(
                &socket,
                &[
                    "keys",
                    "--source",
                    "key-paths",
                    "--path",
                    "/a~1b",
                    "--page-size",
                    "1",
                    "--cursor",
                    cursor,
                ],
            );
            let generic = cli(&socket, &["result-next", &store, cursor]);
            let replay = mcp
                .tool(
                    3,
                    "result_next",
                    Object::new()
                        .with("store_id", store.clone())
                        .with("cursor", cursor)
                        .into(),
                )
                .await;
            assert_eq!(generic, continued);
            assert_eq!(replay["structuredContent"], continued);
            assert_eq!(continued["receipt"], first["receipt"]);
            assert_eq!(continued["rows"][0]["kind"].as_str(), Some("number"));
            assert_eq!(continued["rows"][0]["occurrences"].as_str(), Some("2"));
            assert_eq!(continued["rows"][0]["records"].as_str(), Some("1"));
            let changed = mcp
                .tool(
                    4,
                    "keys_list",
                    Object::new()
                        .with("source", "key-paths")
                        .with("path", "")
                        .with("page_size", 1_u32)
                        .with("cursor", cursor)
                        .into(),
                )
                .await;
            assert_eq!(changed["isError"].as_bool(), Some(true));
            assert_eq!(
                changed["structuredContent"]["error"]["code"].as_str(),
                Some("CONFLICT")
            );
            let root = cli(&socket, &["keys", "--source", "key-paths", "--path", ""]);
            assert_eq!(root["rows"].as_array().unwrap().len(), 1);
            assert_eq!(root["rows"][0]["path"].as_str(), Some(""));
            assert_eq!(root["rows"][0]["occurrences"].as_str(), Some("3"));
            let fresh = cli(
                &socket,
                &["keys", "--source", "key-paths", "--path", "/a~1b"],
            );
            assert_eq!(fresh["rows"][0]["occurrences"].as_str(), Some("2"));
            assert_eq!(fresh["rows"][1]["occurrences"].as_str(), Some("3"));
            let escaped = cli(
                &socket,
                &["keys", "--source", "key-paths", "--path", "/m~0n"],
            );
            assert_eq!(escaped["rows"].as_array().unwrap().len(), 2);
            for (index, path) in [
                "bad",
                "/a~2b",
                &format!("/{}", "x".repeat(sparrow_core::JSON_QUERY_PATH_BYTES)),
            ]
            .into_iter()
            .enumerate()
            {
                let refused = mcp
                    .tool(
                        10 + index as u64,
                        "keys_list",
                        Object::new().with("path", path).into(),
                    )
                    .await;
                assert_eq!(refused["isError"].as_bool(), Some(true));
                assert_eq!(
                    refused["structuredContent"]["error"]["code"].as_str(),
                    Some("INVALID_ARGUMENT")
                );
                assert!(refused["structuredContent"].get("rows").is_none());
                let output = Command::new(env!("CARGO_BIN_EXE_sparrow"))
                    .arg(&socket)
                    .args(["keys", "--path", path])
                    .admitted_output()
                    .unwrap();
                assert!(!output.status.success() && output.stdout.is_empty());
                assert!(String::from_utf8_lossy(&output.stderr).contains("INVALID_ARGUMENT"));
            }
        })
        .await;
}
