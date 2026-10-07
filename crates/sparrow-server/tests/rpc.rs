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
impl Drop for Daemon {
    fn drop(&mut self) {
        self.0.kill().expect("stop this test's daemon");
        self.0.wait().expect("reap this test's daemon");
    }
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

async fn exercise(socket: &Path) -> (String, String) {
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
    copied.as_file().set_len(8388609).unwrap();
    let rejected = Command::new(env!("CARGO_BIN_EXE_sparrow"))
        .arg(socket)
        .args([
            "ingest",
            "oversized-cli-v1",
            "synthetic-rpc",
            "jsonl",
            copied.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("RESULT_TOO_LARGE"));
    assert!(rejected.stdout.is_empty());
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
    (store, id)
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
    tokio::task::LocalSet::new().run_until(async {
        let dir = tempfile::tempdir().unwrap(); let store = dir.path().join("store"); let socket = store.join("rpc.sock"); let config = dir.path().join("server.toml");
        std::fs::write(&config, format!("socket_path = {:?}\ndata_dir = {:?}\ndisk_budget = 134217728\nmaintenance_reserve = 8388608\nmax_artifact_bytes = 8388608\nmax_records = 10000\nmax_pin_members = 100\nmax_concurrent_requests = 4\ntext_lexicon_dir = {:?}\n", socket, store, concat!(env!("CARGO_MANIFEST_DIR"), "/../../../purrdf/crates/text/lexicons/artifacts"))).unwrap();
        let daemon = start(&config, &socket).await;
        let first = exercise(&socket).await;
        drop(daemon); // Only this test's child is terminated.
        std::fs::remove_file(&socket).unwrap(); // Its dead test endpoint, never a live listener.
        let daemon = start(&config, &socket).await;
        assert_eq!(exercise(&socket).await, first);
        drop(daemon);
    }).await;
}
