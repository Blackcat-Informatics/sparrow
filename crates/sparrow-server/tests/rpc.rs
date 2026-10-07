// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Typed wire acceptance through a real daemon and private Unix socket.

use sparrow_server::{connect, sparrow_capnp as wire};
use std::{
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

struct Daemon(Child);

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
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(config)
        .unwrap();
    writeln!(
        file,
        "\n[[consume]]\n{}",
        toml::to_string(&watch_definition(
            root,
            "completed",
            quiet_ms,
            directories,
            candidates
        ))
        .unwrap()
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
                    .output()
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
                Some("sparrow.store/7")
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
                .output()
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
        .output()
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
            assert_eq!(status["store_format"].as_str(), Some("sparrow.store/7"));
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
                    ConsumeWorker::start(store, &[], socket.clone(), WatchRegistry::default())
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
                .spawn()
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
            .output().await.unwrap();
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

async fn start(config: &Path, socket: &Path) -> Daemon {
    let daemon = Daemon(
        Command::new(env!("CARGO_BIN_EXE_sparrowd"))
            .arg("--config")
            .arg(config)
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while !socket.exists() {
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

#[tokio::test(flavor = "current_thread")]
async fn typed_upload_resumes_chunks_and_native_preparation_after_daemon_crashes() {
    tokio::task::LocalSet::new().run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let store_path = dir.path().join("store");
        let socket = store_path.join("rpc.sock");
        let config = write_server_config(dir.path(), &store_path, &socket);
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
        let mut request = client.import_append_request();
        let mut chunk = request.get().init_request();
        upload_ref(chunk.reborrow().init_import(), &store, &id);
        chunk.set_bytes(&input[..sparrow_core::IMPORT_CHUNK_BYTES]);
        let response = request.send().promise.await.unwrap();
        assert_eq!(rpc_ok(response.get().unwrap().get_result().unwrap()).get_received_bytes(), sparrow_core::IMPORT_CHUNK_BYTES as u64);
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

fn write_server_config(dir: &Path, store: &Path, socket: &Path) -> std::path::PathBuf {
    let config = dir.join("server.toml");
    std::fs::write(&config, format!("socket_path = {socket:?}\ndata_dir = {store:?}\ndisk_budget = 134217728\nmaintenance_reserve = 8388608\nmax_artifact_bytes = 8388608\nmax_records = 10000\nmax_pin_members = 100\nmax_concurrent_requests = 4\ntext_lexicon_dir = {:?}\n", concat!(env!("CARGO_MANIFEST_DIR"), "/../../../purrdf/crates/text/lexicons/artifacts"))).unwrap();
    config
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
        assert_eq!(rpc_ok(response.get().unwrap().get_result().unwrap()).get_live_records(), 3);
        drop(producer); drop(daemon);
    }).await;
}

async fn exercise(socket: &Path) -> (String, String, String) {
    let connection = connect(socket).await.unwrap();
    let client = &connection.client;
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
        socket,
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
        socket,
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
        socket,
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
        socket,
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
        .arg(socket)
        .args(["bytes", &store, &id, "0", "1", exported.to_str().unwrap()])
        .output()
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
    let text = cli(socket, &["text", "hello", "--source", "synthetic-rpc"]);
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
        socket,
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
        socket,
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
    let limited = cli(socket, &["values", "--limit", "1"]);
    assert_eq!(
        limited
            .get("receipt")
            .unwrap()
            .get("execution")
            .unwrap()
            .as_str(),
        Some("limited")
    );
    let excluded = cli(socket, &["keys", "--source", "unrelated-source"]);
    assert!(excluded.get("rows").unwrap().as_array().unwrap().is_empty());
    let invalid = Command::new(env!("CARGO_BIN_EXE_sparrow"))
        .arg(socket)
        .args(["keys", "--decoded", "hello"])
        .output()
        .unwrap();
    assert!(!invalid.status.success());
    assert!(invalid.stdout.is_empty());
    let mut request = client.values_search_request();
    let mut query = request.get().init_query();
    query.reborrow().init_path().set_value("/message");
    query.reborrow().init_decoded().set_value("hello world");
    query
        .init_scope()
        .init_received_before()
        .set_unix_millis(u64::MAX);
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
        socket,
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
    mcp_exercise(socket, &store, &id, &first_record, bytes).await;
    (store, id, first_record)
}

struct Mcp {
    child: tokio::process::Child,
    input: Option<tokio::process::ChildStdin>,
    output: tokio::io::BufReader<tokio::process::ChildStdout>,
}

impl Mcp {
    fn start(socket: &Path) -> Self {
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_sparrow"))
            .arg(socket)
            .args(["mcp", "--stdio"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
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

async fn mcp_exercise(socket: &Path, store: &str, artifact: &str, record: &str, bytes: &[u8]) {
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
    assert_eq!(definitions.len(), 19);
    assert!(
        !definitions
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
            query("sparql_query", Object::new().into()),
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

fn cli(socket: &Path, args: &[&str]) -> purrdf_lex::json::Value {
    let output = Command::new(env!("CARGO_BIN_EXE_sparrow"))
        .arg(socket)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    purrdf_lex::json::read_slice(&output.stdout, purrdf_lex::json::Limits::default()).unwrap()
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
                .output()
                .unwrap();
            assert!(!expired.status.success());
            assert!(expired.stdout.is_empty());
            assert!(String::from_utf8_lossy(&expired.stderr).contains("RESULT_EXPIRED"));
            assert_eq!(exercise(&socket).await, first);
            drop(daemon);
        })
        .await;
}
