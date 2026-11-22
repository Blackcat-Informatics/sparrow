// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Typed wire acceptance through a real daemon and private Unix socket.

use sparrow_server::{connect, sparrow_capnp as wire};
use sparrow_test_support::write_server_config;
use std::{
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

// A child can briefly inherit CLOEXEC sockets before exec closes them.
use sparrow_test_support::crash::PROCESS_START;
type TcpReservation = (std::net::TcpListener, std::sync::MutexGuard<'static, ()>);

#[tokio::test(flavor = "current_thread")]
async fn capability_documents_share_cli_mcp_and_typed_transport_contracts() {
    use purrdf_lex::json::{Object, Value};
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let (address, reservation) = configure_tcp_listener(&config);
            drop(reservation);
            let _daemon = start(&config, &socket).await;
            // INVARIANT: the daemon's group can reach the socket and nothing else in the store.
            let mode = |path: &std::path::Path| {
                std::os::unix::fs::PermissionsExt::mode(
                    &std::fs::symlink_metadata(path).unwrap().permissions(),
                ) & 0o7777
            };
            assert_eq!(mode(&data), 0o710);
            assert_eq!(mode(&socket), 0o660);
            let baseline = cli(&socket, &["capabilities"]);
            assert_eq!(baseline["transport"].as_str(), Some("capnp"));
            assert_eq!(
                baseline["status"]["producer_profiles"],
                Value::array([sparrow_client::PRODUCER_PROFILE])
            );
            assert_eq!(
                baseline["producer_profiles"],
                baseline["status"]["producer_profiles"]
            );
            assert_eq!(baseline["transports"], Value::array(["unix", "tcp"]));
            assert_eq!(
                baseline["formats"],
                Value::array(["jsonl", "log", "json", "markdown", "opaque", "syslog"])
            );
            for format in baseline["formats"].as_array().unwrap() {
                let label = format.as_str().unwrap();
                assert_eq!(
                    sparrow_client::format_label(sparrow_client::parse_format(label).unwrap()),
                    label
                );
            }
            let namespace = baseline["vocabulary"]["occurrence_namespace"]
                .as_str()
                .unwrap();
            let payload = dir.path().join("evidence.jsonl");
            std::fs::write(&payload, b"{\"message\":\"timeout\"}\n").unwrap();
            let artifact = cli(
                &socket,
                &[
                    "ingest",
                    "capability-evidence",
                    "capability-fixture",
                    "jsonl",
                    payload.to_str().unwrap(),
                ],
            );
            let request = Object::new()
                .with("store_id", baseline["status"]["store_id"].clone())
                .with(
                    "query",
                    format!("SELECT ?record WHERE {{ ?record a <{namespace}Record> }}"),
                )
                .with(
                    "layers",
                    Object::new()
                        .with("metadata", true)
                        .with("content", false)
                        .with("diagnostic", false),
                );
            let request_file = dir.path().join("query.json");
            std::fs::write(
                &request_file,
                purrdf_lex::json::write_compact(&request.into()),
            )
            .unwrap();
            let result = cli(&socket, &["sparql", request_file.to_str().unwrap()]);
            assert_eq!(result["execution_status"].as_str(), Some("complete"));
            let rows = result["rows"]["native_result"]["results"]["bindings"]
                .as_array()
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0]["record"]["value"].as_str().unwrap(),
                format!(
                    "urn:sparrow:{}",
                    artifact["record_ids"][0].as_str().unwrap()
                )
            );
            cli(
                &socket,
                &[
                    "result-release",
                    baseline["status"]["store_id"].as_str().unwrap(),
                    result["snapshot_id"].as_str().unwrap(),
                ],
            );
            assert_eq!(baseline["retention_days"].as_u64(), Some(30));
            assert_eq!(baseline["time_basis"].as_str(), Some("received_at"));
            assert_eq!(
                baseline["records_time_bases"],
                Value::array(["received_at", "event_time"])
            );
            assert_eq!(
                baseline["status"]["schema_version"].as_u64(),
                Some(u64::from(sparrow_client::SCHEMA_VERSION))
            );
            let tools = baseline["tools"].as_array().unwrap();
            let explain = tools
                .iter()
                .find(|tool| tool["name"].as_str() == Some("sparql_explain"))
                .unwrap();
            let query = tools
                .iter()
                .find(|tool| tool["name"].as_str() == Some("sparql_query"))
                .unwrap();
            assert_eq!(explain["inputSchema"], query["inputSchema"]);
            assert_eq!(
                explain["inputSchema"]["additionalProperties"].as_bool(),
                Some(false)
            );
            assert_eq!(explain["annotations"]["readOnlyHint"].as_bool(), Some(true));
            let measured = cli(&socket, &["sparql-explain", request_file.to_str().unwrap()]);
            assert_eq!(measured["operation"].as_str(), Some("sparql_explain"));
            assert_eq!(measured["execution_status"].as_str(), Some("measured"));
            assert!(measured.get("answer_certainty").is_none());
            cli(
                &socket,
                &[
                    "result-release",
                    baseline["status"]["store_id"].as_str().unwrap(),
                    measured["snapshot_id"].as_str().unwrap(),
                ],
            );
            for endpoint in [
                socket.to_str().unwrap().to_owned(),
                format!("tcp://{address}"),
            ] {
                let command = cli(&endpoint, &["capabilities"]);
                let mut mcp = Mcp::start(&endpoint);
                mcp.initialize(1).await;
                let tool = mcp.tool(2, "capabilities", Object::new().into()).await;
                assert_eq!(tool["isError"].as_bool(), Some(false));
                let list = mcp.call(3, "tools/list", Object::new().into()).await;
                assert_eq!(tool["structuredContent"]["tools"], list["result"]["tools"]);
                for document in [&command, &tool["structuredContent"]] {
                    for key in [
                        "transport",
                        "transports",
                        "tools",
                        "formats",
                        "vocabulary",
                        "retention_days",
                        "time_basis",
                        "records_time_bases",
                        "mcp_protocol",
                    ] {
                        assert_eq!(document[key], baseline[key], "{endpoint}: {key}");
                    }
                    for key in [
                        "store_id",
                        "schema_version",
                        "text_analyzer",
                        "rdf_profile",
                        "max_event_batch_bytes",
                        "max_event_batch_events",
                    ] {
                        assert_eq!(
                            document["status"][key], baseline["status"][key],
                            "{endpoint}: {key}"
                        );
                    }
                }
                let invalid = mcp
                    .tool(
                        4,
                        "capabilities",
                        Object::new().with("unsupported", true).into(),
                    )
                    .await;
                assert_eq!(invalid["isError"].as_bool(), Some(true));
                assert_eq!(
                    invalid["structuredContent"]["error"]["code"].as_str(),
                    Some("INVALID_ARGUMENT")
                );
                mcp.close(true).await;
                let invalid = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
                    .args(endpoint_args(&endpoint))
                    .args(["capabilities", "unsupported"])
                    .admitted_output()
                    .unwrap();
                assert!(!invalid.status.success());
                assert!(invalid.stdout.is_empty());
            }
        })
        .await;
}

/// INVARIANT (#323): the same bytes ingested under two requests are both found by their
/// payload id through the CLI and the MCP tool, which share one lookup; a digest nothing holds
/// answers an exact empty page; a cursor resumes after the last artifact listed.
#[tokio::test(flavor = "current_thread")]
async fn payload_lookup_finds_every_artifact_of_a_digest_through_cli_and_mcp() {
    use purrdf_lex::json::Object;
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let _daemon = start(&config, &socket).await;
            let store = cli(&socket, &["status"])["store_id"]
                .as_str()
                .unwrap()
                .to_owned();
            let payload = dir.path().join("same.log");
            std::fs::write(&payload, b"held twice\n").unwrap();
            let mut ids = Vec::new();
            let mut payload_id = String::new();
            for request in ["lookup-one", "lookup-two"] {
                let artifact = cli(
                    &socket,
                    &[
                        "ingest",
                        request,
                        "lookup-source",
                        "log",
                        payload.to_str().unwrap(),
                    ],
                );
                ids.push(artifact["artifact_id"].as_str().unwrap().to_owned());
                payload_id = artifact["payload_id"].as_str().unwrap().to_owned();
            }
            ids.sort();
            let found = cli(&socket, &["payload", &store, &payload_id]);
            assert_eq!(found["answer_certainty"].as_str(), Some("exact"));
            let listed: Vec<&str> = found["artifact_ids"]
                .as_array()
                .unwrap()
                .iter()
                .map(|id| id.as_str().unwrap())
                .collect();
            assert_eq!(listed, ids.iter().map(String::as_str).collect::<Vec<_>>());
            assert!(found["next_cursor"].is_null());
            let first = cli(
                &socket,
                &["payload", &store, &payload_id, "--page-size", "1"],
            );
            let cursor = first["next_cursor"].as_str().unwrap();
            let rest = cli(
                &socket,
                &[
                    "payload",
                    &store,
                    &payload_id,
                    "--page-size",
                    "1",
                    "--cursor",
                    cursor,
                ],
            );
            assert_eq!(rest["artifact_ids"][0].as_str(), Some(ids[1].as_str()));
            assert!(rest["next_cursor"].is_null());
            let mut mcp = Mcp::start(socket.to_str().unwrap());
            mcp.initialize(1).await;
            let result = mcp
                .tool(
                    2,
                    "payload_lookup",
                    Object::new()
                        .with("store_id", store.clone())
                        .with("payload_id", payload_id.clone())
                        .into(),
                )
                .await;
            assert_eq!(
                result["structuredContent"]["artifact_ids"]
                    .as_array()
                    .unwrap()
                    .len(),
                2,
                "{result:?}"
            );
            let absent = format!("blake3:{}", "0".repeat(64));
            let empty = cli(&socket, &["payload", &store, &absent]);
            assert_eq!(empty["answer_certainty"].as_str(), Some("exact"));
            assert!(empty["artifact_ids"].as_array().unwrap().is_empty());
            mcp.close(true).await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn log_timestamp_profiles_share_tcp_cli_unix_mcp_and_chronological_capture() {
    use purrdf_lex::json::{self, Object};
    tokio::task::LocalSet::new()
        .run_until(async {
            let directory = tempfile::tempdir().unwrap();
            let data = directory.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(directory.path(), &data, &socket);
            let (address, reservation) = configure_tcp_listener(&config);
            drop(reservation);
            let _daemon = start(&config, &socket).await;
            let endpoint = format!("tcp://{address}");
            let status = cli(&endpoint, &["status"]);
            let store = status["store_id"].as_str().unwrap();
            let definition = Object::new()
                .with("source", "plain-clock")
                .with("representation", "rfc3339")
                .with(
                    "log_capture",
                    Object::new()
                        .with("pattern", "([^ ]+) .*")
                        .with("group", 1_u32)
                        .with("flags", "s"),
                );
            let file = directory.path().join("time-profile.json");
            std::fs::write(&file, json::write_compact(&definition.clone().into())).unwrap();
            let defined = cli(&endpoint, &["source-time-define", file.to_str().unwrap()]);
            assert_eq!(defined["extraction"]["kind"].as_str(), Some("log_capture"));
            assert_eq!(
                defined["extraction"]["match_steps"].as_str(),
                Some("2000000")
            );
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            let repeated = mcp
                .tool(2, "source_time_define", definition.clone().into())
                .await;
            assert_eq!(repeated["isError"].as_bool(), Some(false));
            assert_eq!(repeated["structuredContent"], defined);
            let lookup = mcp
                .tool(
                    3,
                    "source_time_get",
                    Object::new().with("source", "plain-clock").into(),
                )
                .await;
            assert_eq!(
                lookup["structuredContent"],
                cli(&endpoint, &["source-time-get", "plain-clock"])
            );
            let input = directory.path().join("events.log");
            let bytes =
                b"2026-10-07T03:00:02.123Z late\r\n2026-10-07T05:00:01+02:00 early\nmissing\n";
            std::fs::write(&input, bytes).unwrap();
            let artifact = cli(
                &endpoint,
                &[
                    "ingest",
                    "plain-event-input",
                    "plain-clock",
                    "log",
                    input.to_str().unwrap(),
                ],
            );
            assert_eq!(artifact["record_ids"].as_array().unwrap().len(), 3);
            assert_eq!(
                cli(&endpoint, &["status"])["store_format"].as_str(),
                Some("sparrow.store/365")
            );
            let first = cli(
                &endpoint,
                &[
                    "records",
                    "--source",
                    "plain-clock",
                    "--event-from",
                    "2026-10-07T03:00:00Z",
                    "--event-before",
                    "2026-10-07T03:00:03Z",
                    "--page-size",
                    "1",
                ],
            );
            assert_eq!(first["time_basis"].as_str(), Some("event_time"));
            assert_eq!(first["rows"][0]["record_id"], artifact["record_ids"][1]);
            assert_eq!(first["event_coverage"]["unknown"].as_str(), Some("1"));
            let mirrored = mcp
                .tool(
                    4,
                    "records_search",
                    Object::new()
                        .with("source", "plain-clock")
                        .with("event_from", "2026-10-07T03:00:00Z")
                        .with("event_before", "2026-10-07T03:00:03Z")
                        .with("page_size", 1_u32)
                        .into(),
                )
                .await;
            assert_eq!(mirrored["isError"].as_bool(), Some(false));
            assert_eq!(mirrored["structuredContent"]["rows"], first["rows"]);
            assert_eq!(
                mirrored["structuredContent"]["receipt"]["scope"],
                first["receipt"]["scope"]
            );
            let cursor = first["page"]["next_cursor"].as_str().unwrap();
            let next = cli(&endpoint, &["result-next", store, cursor]);
            let replay = mcp
                .tool(
                    5,
                    "result_next",
                    Object::new()
                        .with("store_id", store)
                        .with("cursor", cursor)
                        .into(),
                )
                .await;
            assert_eq!(replay["structuredContent"], next);
            assert_eq!(next["rows"][0]["record_id"], artifact["record_ids"][0]);
            for id in [
                first["page"]["snapshot_id"].as_str().unwrap(),
                mirrored["structuredContent"]["page"]["snapshot_id"]
                    .as_str()
                    .unwrap(),
            ] {
                assert_eq!(
                    cli(&endpoint, &["result-release", store, id])["released"].as_bool(),
                    Some(true)
                );
            }
            let incompatible = mcp
                .tool(
                    6,
                    "source_time_define",
                    Object::new()
                        .with("source", "plain-clock")
                        .with("path", "/t")
                        .with("representation", "unix_millis")
                        .into(),
                )
                .await;
            assert_eq!(
                incompatible["structuredContent"]["error"]["code"].as_str(),
                Some("CONFLICT")
            );
            let ambiguous = mcp
                .tool(
                    7,
                    "source_time_define",
                    Object::new()
                        .with("source", "invalid-source")
                        .with("path", "/t")
                        .with(
                            "log_capture",
                            Object::new().with("pattern", "(x)").with("group", 1_u32),
                        )
                        .with("representation", "rfc3339")
                        .into(),
                )
                .await;
            assert_eq!(
                ambiguous["structuredContent"]["error"]["code"].as_str(),
                Some("INVALID_ARGUMENT")
            );
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let mut old = client.capability().source_time_get_request();
            old.get().set_source("plain-clock");
            let reply = old.send().promise.await.unwrap();
            let wire::result::Error(error) =
                reply.get().unwrap().get_result().unwrap().which().unwrap()
            else {
                panic!("unversioned profile lookup succeeded")
            };
            assert_eq!(
                error.unwrap().get_code().unwrap().to_str().unwrap(),
                "UNSUPPORTED"
            );
            let final_status = cli(&endpoint, &["status"]);
            assert_eq!(final_status["record_read_handles"].as_str(), Some("0"));
            assert_eq!(final_status["sparql_read_handles"].as_str(), Some("0"));
            mcp.close(true).await;
        })
        .await;
}

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
/// The longest a test waits for a child it runs to completion. A child that does not end (a
/// daemon that waits on a start condition nothing clears) fails its test here, with the child
/// killed, instead of holding the process-start lock and every other test behind it.
const CHILD_DEADLINE: Duration = Duration::from_secs(180);

impl ProcessOutput for Command {
    fn admitted_output(&mut self) -> std::io::Result<std::process::Output> {
        let child = {
            let _guard = PROCESS_START.lock().unwrap();
            self.stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .stdin(Stdio::null())
                .spawn()?
        };
        let pid = rustix::process::Pid::from_raw(child.id() as i32).unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = sender.send(child.wait_with_output());
        });
        match receiver.recv_timeout(CHILD_DEADLINE) {
            Ok(output) => output,
            Err(_) => {
                let _ = rustix::process::kill_process(pid, rustix::process::Signal::KILL);
                panic!(
                    "a child process did not end within {} s and was killed",
                    CHILD_DEADLINE.as_secs()
                );
            }
        }
    }
}

/// A daemon started with its journal lines piped, for tests of a start that waits: the test
/// reads what it waits for, then clears the condition or interrupts it. Every read is bounded.
struct Watched {
    child: tokio::process::Child,
    stderr: tokio::io::BufReader<tokio::process::ChildStderr>,
}

impl Watched {
    fn spawn(config: &Path, notify: Option<&Path>) -> Self {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_sparrowd-rpc-fixture"));
        command
            .arg("--config")
            .arg(config)
            .kill_on_drop(true)
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if let Some(path) = notify {
            command.env("NOTIFY_SOCKET", path);
        }
        let mut child = command.admitted_spawn().unwrap();
        let stderr = tokio::io::BufReader::new(child.stderr.take().unwrap());
        Self { child, stderr }
    }

    /// Read journal lines until one contains `needle`; return the lines read. Fails the test if
    /// the daemon ends first or none arrives within a minute.
    async fn until(&mut self, needle: &str) -> String {
        use tokio::io::AsyncBufReadExt;
        let mut seen = String::new();
        loop {
            let mut line = String::new();
            let length =
                tokio::time::timeout(Duration::from_secs(60), self.stderr.read_line(&mut line))
                    .await
                    .unwrap_or_else(|_| {
                        panic!("no journal line with {needle:?} within 60 s: {seen}")
                    })
                    .unwrap();
            assert!(length > 0, "the daemon ended without {needle:?}: {seen}");
            seen.push_str(&line);
            if line.contains(needle) {
                return seen;
            }
        }
    }

    fn interrupt(&self) {
        rustix::process::kill_process(
            rustix::process::Pid::from_raw(self.child.id().unwrap() as i32).unwrap(),
            rustix::process::Signal::INT,
        )
        .unwrap();
    }

    /// The exit status and the journal lines that remain.
    async fn exit(&mut self) -> (std::process::ExitStatus, String) {
        use tokio::io::AsyncReadExt;
        let status = tokio::time::timeout(Duration::from_secs(30), self.child.wait())
            .await
            .expect("the daemon did not end within 30 s")
            .unwrap();
        let mut rest = String::new();
        self.stderr.read_to_string(&mut rest).await.unwrap();
        (status, rest)
    }
}

/// The exit status of a refused start: no wait clears it and the unit does not restart it.
const REFUSED: i32 = sparrow_server::start_wait::REFUSED_EXIT_STATUS as i32;

struct Daemon(Child);

/// Secret files written by `configure_tcp_listener`, keyed by `tcp://` endpoint.
static TCP_SECRETS: std::sync::Mutex<std::collections::BTreeMap<String, std::path::PathBuf>> =
    std::sync::Mutex::new(std::collections::BTreeMap::new());

/// CLI arguments for an endpoint; a configured test TCP endpoint authenticates by CHAP.
fn endpoint_args(endpoint: impl AsRef<std::ffi::OsStr>) -> Vec<std::ffi::OsString> {
    let endpoint = endpoint.as_ref().to_owned();
    match endpoint
        .to_str()
        .and_then(|value| TCP_SECRETS.lock().unwrap().get(value).cloned())
    {
        Some(secret) => vec!["--secret-file".into(), secret.into(), endpoint],
        None => vec![endpoint],
    }
}

/// The typed endpoint for a test destination, authenticated like `endpoint_args`.
fn test_endpoint(endpoint: impl AsRef<std::ffi::OsStr>) -> sparrow_client::Endpoint {
    let args = endpoint_args(endpoint)
        .into_iter()
        .map(|arg| arg.into_string().unwrap())
        .collect::<Vec<_>>();
    sparrow_client::Endpoint::from_args(&args).unwrap().0
}

/// A CHAP-authenticated typed client for a test TCP listener.
async fn tcp_client(
    address: std::net::SocketAddr,
) -> Result<sparrow_client::Client, sparrow_client::Error> {
    let secret = TCP_SECRETS.lock().unwrap()[&format!("tcp://{address}")].clone();
    sparrow_client::Client::connect_endpoint(&sparrow_client::Endpoint::AuthenticatedTcp(
        address,
        sparrow_client::TcpSecret::read(&secret)?,
    ))
    .await
}

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
    let secret = config.with_file_name("tcp.secret");
    std::fs::write(&secret, b"sparrow test CHAP secret of 32+ bytes").unwrap();
    std::fs::set_permissions(
        &secret,
        <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o600),
    )
    .unwrap();
    let address: std::net::SocketAddr = ([127, 0, 0, 1], port).into();
    TCP_SECRETS
        .lock()
        .unwrap()
        .insert(format!("tcp://{address}"), secret.clone());
    let mut text = std::fs::read_to_string(config).unwrap();
    text.push_str(&format!(
        "rpc_tcp_address = \"0.0.0.0:{port}\"\nrpc_tcp_secret_file = \"{}\"\n",
        secret.display()
    ));
    std::fs::write(config, text).unwrap();
    (address, (reservation, guard))
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
        let client = tcp_client(address).await.unwrap();
        watch_until(client.capability(), |watch| watch.get_tail_checkpoint_records() == 1).await;
        let status = cli(&socket, &["status"]);
        let generation = status["consume_watches"][0]["tail_generation_ids"][0].as_str().unwrap().to_owned();
        let intent_path = dir.path().join("handoff.capnp");
        let early = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture")).arg(&socket).args([
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
        let client = tcp_client(address).await.unwrap();
        assert_eq!(client.handoff(&intent).await.unwrap(), completed);
        assert_eq!(cli(&socket, &["watch", "handoff-status", intent_path.to_str().unwrap()]), rendered);
        assert_eq!(client.bytes(&generation, 0, 10).await.unwrap(), b"first\nlast");
        assert_eq!(std::fs::read(&input).unwrap(), b"replacement stays independent");
        let status = cli(&socket, &["status"]);
        assert_eq!(status["store_format"].as_str(), Some("sparrow.store/356"));
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
            let client = tcp_client(address).await.unwrap();
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
                let output = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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
            let client = tcp_client(address).await.unwrap();
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
            let config = sparrow_server::ServerConfig::read(&config_path).unwrap();
            let store = config.open_store().unwrap();
            let store_id = store.store_id().unwrap();
            let directory = root.metadata().unwrap();
            let mut intents = Vec::new();
            for name in ["resume.log", "changed.log", "missing.log", "empty.log"] {
                let path = root.join(name);
                // 300 lines of 4 KiB need two 1 MiB tail captures to drain.
                let line = [b"accepted".as_slice(), &[b'a'; 4087], b"\n"].concat();
                let bytes = if name == "empty.log" {
                    Vec::new()
                } else {
                    let mut bytes = line.repeat(300);
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
                            &bytes[..line.len()],
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
                assert_eq!(receipt.offset, 4096);
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
            let bytes = vec![b'x'; 1024 * 1024 + 1];
            std::fs::write(&input, &bytes).unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            append_tail_config(&config, &root, "log");
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
            assert_eq!(watch["tail_read_bytes"].as_str(), Some("1048576"));
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

/// INVARIANT (#224): a daemon whose stored generation registry is full still tails a new file,
/// and status names that file with its counts, over the wire and rendered. Before #224 the
/// declaration was refused and the file's lines were never taken.
#[tokio::test(flavor = "current_thread")]
async fn a_full_generation_registry_still_tails_a_file_and_status_names_it() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("active");
            std::fs::create_dir(&root).unwrap();
            std::fs::write(root.join("current.jsonl"), b"{\"n\":1}\n").unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            append_tail_config(&config, &root, "jsonl");
            {
                let store = sparrow_server::ServerConfig::read(&config)
                    .unwrap()
                    .open_store()
                    .unwrap();
                for index in 0..sparrow_core::MAX_TAIL_GENERATIONS as u64 {
                    store
                        .tail_start(
                            &sparrow_core::TailStart {
                                request_id: format!("filler-{index}"),
                                watch: "filler".into(),
                                watch_revision: 1,
                                directories: sparrow_core::ConsumptionDirectories {
                                    root_device: 1,
                                    root_inode: 2,
                                    parent_device: 3,
                                    parent_inode: 4,
                                },
                                path: format!("filler{index}.log").into_bytes(),
                                source: "filler".into(),
                                format: "log".into(),
                                device: 99,
                                inode: index + 1,
                            },
                            1,
                        )
                        .unwrap();
                }
            }
            let daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            watch_until(client.capability(), |watch| {
                watch.get_tail_checkpoint_records() == 1 && watch.get_tail_descriptors() == 1
            })
            .await;
            let response = client
                .capability()
                .status_request()
                .send()
                .promise
                .await
                .unwrap();
            let status = rpc_ok(response.get().unwrap().get_result().unwrap());
            let evidence = status.get_tail_registry_evidence().unwrap();
            let named = evidence.get_named().unwrap();
            assert_eq!(named.len(), 1);
            let file = named.get(0);
            assert_eq!(file.get_path().unwrap().to_str().unwrap(), "current.jsonl");
            assert_eq!(
                (file.get_generations(), file.get_records(), file.get_bytes()),
                (1, 1, 8)
            );
            assert_eq!(
                (
                    evidence.get_total_generations(),
                    evidence.get_total_records(),
                    evidence.get_total_bytes()
                ),
                (1, 1, 8)
            );
            let rendered = cli(&socket, &["status"]);
            let report = &rendered["tail_registry_evidence"];
            assert_eq!(report["named"][0]["path"].as_str(), Some("current.jsonl"));
            assert_eq!(report["named"][0]["records"].as_str(), Some("1"));
            assert_eq!(report["named"][0]["bytes"].as_str(), Some("8"));
            assert_eq!(report["total_generations"].as_str(), Some("1"));
            assert_eq!(report["other_files_exact"].as_bool(), Some(true));
            drop(client);
            drop(daemon);
        })
        .await;
}

/// INVARIANT (#227, #238): a daemon whose source mapping registry is full still stores an event
/// batch from a new source, and status names that source with its counts, over the wire
/// (`sourceTimeLoss`, kind `source_time`) and rendered. The earlier test only saw the field
/// empty, so an enum or render mapping error would have passed.
#[tokio::test(flavor = "current_thread")]
async fn a_full_source_mapping_registry_still_takes_a_batch_and_status_names_the_source() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            {
                let store = sparrow_server::ServerConfig::read(&config)
                    .unwrap()
                    .open_store()
                    .unwrap();
                for index in 0..1024 {
                    store
                        .define_time_profile(
                            &format!("mapped-{index}"),
                            "/event_time",
                            sparrow_core::TimeRepresentation::UnixMillis,
                        )
                        .unwrap();
                }
            }
            let daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let mut event = delivery_event("past the ceiling");
            event.sequence = 1;
            let batch = sparrow_client::EventBatch {
                request_id: "past-ceiling".into(),
                source: "unmapped-source".into(),
                client_instance: [7; 16],
                events: vec![event],
            };
            let stored = client.events(&batch).await.unwrap();
            assert_eq!(stored.record_ids.len(), 1);
            let response = client
                .capability()
                .status_request()
                .send()
                .promise
                .await
                .unwrap();
            let status = rpc_ok(response.get().unwrap().get_result().unwrap());
            let loss = status.get_source_time_loss().unwrap();
            let named = loss.get_named().unwrap();
            assert_eq!(named.len(), 1);
            let row = named.get(0);
            assert_eq!(row.get_kind().unwrap(), wire::LostStreamKind::SourceTime);
            assert_eq!(
                row.get_source().unwrap().to_str().unwrap(),
                "unmapped-source"
            );
            assert_eq!(row.get_instance().unwrap().to_str().unwrap(), "");
            assert_eq!((row.get_batches(), row.get_events()), (1, 1));
            assert_eq!((loss.get_total_batches(), loss.get_total_events()), (1, 1));
            let rendered = cli(&socket, &["status"]);
            let report = &rendered["source_time_loss"];
            assert_eq!(report["named"][0]["kind"].as_str(), Some("source_time"));
            assert_eq!(
                report["named"][0]["source"].as_str(),
                Some("unmapped-source")
            );
            assert_eq!(report["named"][0]["events"].as_str(), Some("1"));
            assert_eq!(report["total_events"].as_str(), Some("1"));
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
            let client = tcp_client(address).await.unwrap();
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
            let mut settings = sparrow_client::DeliveryConfig::tcp(address, "tcp-logging")
                .with_quarantine_dir(dir.path().join("quarantine"));
            settings.batch_events = 1;
            settings.rpc_timeout = Duration::from_millis(100);
            // Production refuses sub-second intervals; this outage test needs fast retries.
            let settings = settings.with_test_timing(
                Duration::from_secs(2),
                Duration::from_millis(20),
                Duration::from_millis(20),
            );
            let delivery = sparrow_client::Delivery::start(settings).unwrap();
            assert_eq!(
                admit(&delivery, delivery_event("queued before TCP listener")),
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
                admit(&delivery, delivery_event("queued during TCP outage")),
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
                let result = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
                    .args([invalid, "status"])
                    .admitted_output()
                    .unwrap();
                assert!(!result.status.success());
                assert!(result.stdout.is_empty());
            }
            drop(daemon); // Reaped by the shared daemon owner before testing refusal.
            let result = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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

/// INVARIANT: an anonymous public TCP peer can discover and ingest but cannot read,
/// report status or administer watches; one wrong CHAP response ends authentication on
/// that connection; the right response grants the complete service; Unix runs no CHAP.
#[tokio::test(flavor = "current_thread")]
async fn public_tcp_requires_chap_for_reads_and_administration_but_not_ingestion() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("victim");
            std::fs::create_dir(&root).unwrap();
            std::fs::write(root.join("private.log"), b"must stay\n").unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let (address, reservation) = configure_tcp_listener(&config);
            drop(reservation);
            let definition_file = dir.path().join("definition.toml");
            let definition = watch_definition(&root, "victim", 300, 16, 16);
            std::fs::write(&definition_file, toml::to_string(&definition).unwrap()).unwrap();
            let daemon = start(&config, &socket).await;
            let unauthenticated = |result: Result<_, sparrow_access::AgentError>| {
                assert_eq!(result.unwrap_err().code, "UNAUTHENTICATED");
            };

            let anonymous =
                sparrow_client::Client::connect_producer(&sparrow_client::Endpoint::Tcp(address))
                    .await
                    .unwrap();
            assert_eq!(
                anonymous.discovery().producer_profiles,
                [sparrow_client::PRODUCER_PROFILE]
            );
            let receipt = anonymous
                .ingest(
                    "anonymous-ingest",
                    "anonymous",
                    wire::Format::Log,
                    b"accepted\n",
                )
                .await
                .unwrap();
            assert!(matches!(
                anonymous.bytes(&receipt.id, 0, 9).await,
                Err(sparrow_client::Error::Domain { code, .. }) if code == "UNAUTHENTICATED"
            ));
            unauthenticated(sparrow_access::run_cli(&anonymous, &["status".into()]).await);
            let put = [
                "watch",
                "put",
                "anonymous-watch",
                "0",
                definition_file.to_str().unwrap(),
            ]
            .map(String::from);
            unauthenticated(sparrow_access::run_cli(&anonymous, &put).await);
            // Claim retirement is administration: refused before CHAP (D118).
            let retire = [
                "watch",
                "retire-claim",
                "completed",
                "import:0000000000000000000000000000000000000000000000000000000000000000",
                "anonymous",
            ]
            .map(String::from);
            unauthenticated(sparrow_access::run_cli(&anonymous, &retire).await);
            assert!(
                cli(&socket, &["watch", "list"])["watches"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            assert!(root.join("private.log").exists());

            let challenge = anonymous
                .capability()
                .auth_challenge_request()
                .send()
                .promise
                .await
                .unwrap();
            assert!(
                rpc_ok(challenge.get().unwrap().get_result().unwrap())
                    .get_nonce()
                    .unwrap()
                    .len()
                    == 32
            );
            let mut wrong = anonymous.capability().authenticate_request();
            wrong.get().set_response(&[0; 32]);
            let wrong = wrong.send().promise.await.unwrap();
            assert_eq!(
                rpc_error_code(wrong.get().unwrap().get_result().unwrap()),
                "UNAUTHENTICATED"
            );
            let retry = anonymous
                .capability()
                .auth_challenge_request()
                .send()
                .promise
                .await
                .unwrap();
            assert_eq!(
                rpc_error_code(retry.get().unwrap().get_result().unwrap()),
                "UNAUTHENTICATED"
            );

            let session = tcp_client(address).await.unwrap();
            assert_eq!(
                session.bytes(&receipt.id, 0, 9).await.unwrap(),
                b"accepted\n"
            );
            let status = sparrow_access::run_cli(&session, &["status".into()])
                .await
                .unwrap();
            assert_eq!(status["store_id"].as_str(), Some(receipt.store_id.as_str()));
            assert_eq!(
                status["producer_profiles"],
                purrdf_lex::json::Value::array([sparrow_client::PRODUCER_PROFILE])
            );
            let created = sparrow_access::run_cli(&session, &put).await.unwrap();
            assert_eq!(created["watch"]["revision"].as_str(), Some("1"));
            assert_eq!(
                cli(&socket, &["watch", "list"])["watches"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );

            let unix = sparrow_client::Client::connect(&socket).await.unwrap();
            let local = unix
                .capability()
                .auth_challenge_request()
                .send()
                .promise
                .await
                .unwrap();
            assert_eq!(
                rpc_error_code(local.get().unwrap().get_result().unwrap()),
                "UNSUPPORTED"
            );
            drop(daemon);
        })
        .await;
}

/// Create a setgid 2750 ingest directory with the test user's own group and add the
/// ingest socket keys to `config`. Returns the socket path and its group.
fn configure_ingest_socket(
    root: &Path,
    config: &Path,
    max_connections: usize,
) -> (std::path::PathBuf, u32) {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let directory = root.join("ingest");
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o2750)).unwrap();
    let gid = std::fs::metadata(&directory).unwrap().gid();
    let socket = directory.join("ingest.sock");
    let base = std::fs::read_to_string(config).unwrap();
    std::fs::write(
        config,
        format!(
            "ingest_socket_path = \"{}\"\ningest_socket_gid = {gid}\nmax_ingest_connections = {max_connections}\n{base}",
            socket.display()
        ),
    )
    .unwrap();
    (socket, gid)
}

/// Start the daemon and return once its ingest socket is bound. `start` returns when the private
/// RPC socket accepts, and `sparrowd` binds the ingest socket after that (see `main.rs`), so a
/// connect straight after `start` can find no ingest socket (#347). Status is served only after
/// both sockets are bound and reports the ingest socket, so one status round trip is the
/// daemon's own readiness signal: no clock, and no probe connection to spend an ingest slot.
async fn start_with_ingest(config: &Path, socket: &Path, ingest: &Path) -> Daemon {
    let daemon = start(config, socket).await;
    let status = cli(socket, &["status"]);
    assert_eq!(
        status["ingest_socket"]["path"].as_str(),
        ingest.to_str(),
        "status does not report the ingest socket"
    );
    daemon
}

/// INVARIANT: evidence accepted on the ingest socket carries the caller's kernel-reported
/// uid, gid and pid in its typed intake, its CLI rendering and its metadata graphs, and
/// the socket inherits the configured group with mode 0660.
#[tokio::test(flavor = "current_thread")]
async fn ingest_socket_records_peer_credentials_in_provenance() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let (ingest, gid) = configure_ingest_socket(dir.path(), &config, 4);
            let daemon = start_with_ingest(&config, &socket, &ingest).await;
            let metadata = std::fs::metadata(&ingest).unwrap();
            assert_eq!((metadata.gid(), metadata.permissions().mode() & 0o7777), (gid, 0o660));

            let producer = sparrow_client::Client::connect(&ingest).await.unwrap();
            let artifact = producer
                .ingest("peer-ingest", "peer.ingest", wire::Format::Log, b"from a peer\n")
                .await
                .unwrap();
            let mut event = delivery_event("from a peer");
            event.sequence = 1;
            let events = producer
                .events(&sparrow_client::EventBatch {
                    request_id: "peer-events".into(),
                    source: "peer.events".into(),
                    client_instance: [5; 16],
                    events: vec![event],
                })
                .await
                .unwrap();
            let unidentified = sparrow_client::Client::connect(&socket)
                .await
                .unwrap()
                .ingest("private-ingest", "private", wire::Format::Log, b"owner\n")
                .await
                .unwrap();
            let expected = (
                rustix::process::geteuid().as_raw(),
                rustix::process::getegid().as_raw(),
                std::process::id(),
            );
            let store = artifact.store_id.clone();
            let private = connect(&socket).await.unwrap();
            let typed = |id: String| {
                let mut request = private.client.record_get_request();
                let mut reference = request.get().init_record();
                reference.set_store_id(&store);
                reference.set_id(&id);
                async move {
                    let response = request.send().promise.await.unwrap();
                    let record = rpc_ok(response.get().unwrap().get_result().unwrap());
                    match record.get_intake().unwrap().which().unwrap() {
                        wire::optional_intake::Absent(()) => None,
                        wire::optional_intake::Value(intake) => {
                            let intake = intake.unwrap();
                            assert_eq!(
                                intake.get_endpoint().unwrap(),
                                wire::IntakeEndpoint::IngestSocket
                            );
                            Some((intake.get_uid(), intake.get_gid(), intake.get_pid()))
                        }
                    }
                }
            };
            assert_eq!(typed(unidentified.record_ids[0].clone()).await, None);
            let (uid, gid, pid) = (
                expected.0.to_string(),
                expected.1.to_string(),
                expected.2.to_string(),
            );
            for (index, receipt) in [&artifact, &events].into_iter().enumerate() {
                let record = receipt.record_ids[0].clone();
                assert_eq!(typed(record.clone()).await, Some(expected));
                let context = cli(
                    &socket,
                    &["context", &store, &record, "--before", "0", "--after", "0"],
                );
                let intake = &context["anchor"]["intake"];
                assert_eq!(intake["endpoint"].as_str(), Some("ingest-socket"));
                assert_eq!(
                    [&intake["uid"], &intake["gid"], &intake["pid"]].map(|value| value.as_str()),
                    [Some(uid.as_str()), Some(gid.as_str()), Some(pid.as_str())]
                );
                // Selecting the artifact expands to its record: the artifact graph and the
                // record's metadata graph each carry the caller, once.
                let request = purrdf_lex::json::Object::new()
                    .with("store_id", store.clone())
                    .with(
                        "scope",
                        purrdf_lex::json::Object::new()
                            .with("kind", "selection")
                            .with("store_id", store.clone())
                            .with("artifact_ids", vec![receipt.id.clone()]),
                    )
                    .with(
                        "layers",
                        purrdf_lex::json::Object::new()
                            .with("metadata", true)
                            .with("diagnostic", false)
                            .with("content", false),
                    )
                    .with(
                        "query",
                        "PREFIX sp: <urn:sparrow:vocab:> SELECT ?e ?endpoint ?uid ?gid ?pid WHERE { ?e sp:intakeEndpoint ?endpoint ; sp:peerUid ?uid ; sp:peerGid ?gid ; sp:peerPid ?pid }",
                    );
                let path = dir.path().join(format!("peer-{index}.json"));
                std::fs::write(&path, purrdf_lex::json::write_compact(&request.into())).unwrap();
                let result = cli(&socket, &["sparql", path.to_str().unwrap()]);
                let rows = result["rows"]["native_result"]["results"]["bindings"]
                    .as_array()
                    .unwrap();
                let mut subjects: Vec<_> = rows
                    .iter()
                    .map(|row| row["e"]["value"].as_str().unwrap().to_owned())
                    .collect();
                subjects.sort();
                let mut expected_subjects =
                    vec![format!("urn:sparrow:{}", receipt.id), format!("urn:sparrow:{record}")];
                expected_subjects.sort();
                assert_eq!(subjects, expected_subjects, "{result:?}");
                for row in rows {
                    assert_eq!(row["endpoint"]["value"].as_str(), Some("ingest-socket"));
                    for (name, value) in [("uid", &uid), ("gid", &gid), ("pid", &pid)] {
                        assert_eq!(row[name]["value"].as_str(), Some(value.as_str()));
                        assert_eq!(
                            row[name]["datatype"].as_str(),
                            Some("http://www.w3.org/2001/XMLSchema#nonNegativeInteger")
                        );
                    }
                }
            }
            drop(daemon);
        })
        .await;
}

/// INVARIANT: the ingest socket admits producer operations only. Reads and administration,
/// including claim retirement, return PERMISSION_DENIED, and it offers no CHAP session (UNSUPPORTED).
#[tokio::test(flavor = "current_thread")]
async fn ingest_socket_refuses_reads_and_administration() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let (ingest, _) = configure_ingest_socket(dir.path(), &config, 4);
            let daemon = start_with_ingest(&config, &socket, &ingest).await;
            let producer = sparrow_client::Client::connect(&ingest).await.unwrap();
            let gate = producer.capability();
            let status = gate.status_request().send().promise.await.unwrap();
            let search = gate.records_search_request().send().promise.await.unwrap();
            let pin = gate.pin_request().send().promise.await.unwrap();
            let put = gate.watch_put_request().send().promise.await.unwrap();
            let read = gate.bytes_read_request().send().promise.await.unwrap();
            let retire = gate
                .watch_claim_retire_request()
                .send()
                .promise
                .await
                .unwrap();
            for code in [
                rpc_error_code(status.get().unwrap().get_result().unwrap()),
                rpc_error_code(search.get().unwrap().get_result().unwrap()),
                rpc_error_code(pin.get().unwrap().get_result().unwrap()),
                rpc_error_code(put.get().unwrap().get_result().unwrap()),
                rpc_error_code(read.get().unwrap().get_result().unwrap()),
                rpc_error_code(retire.get().unwrap().get_result().unwrap()),
            ] {
                assert_eq!(code, "PERMISSION_DENIED");
            }
            let mut authenticate = gate.authenticate_request();
            authenticate.get().set_response(&[0; 32]);
            let authenticate = authenticate.send().promise.await.unwrap();
            assert_eq!(
                rpc_error_code(authenticate.get().unwrap().get_result().unwrap()),
                "UNSUPPORTED"
            );
            assert_eq!(
                sparrow_access::run_cli(&producer, &["status".into()])
                    .await
                    .unwrap_err()
                    .code,
                "PERMISSION_DENIED"
            );
            drop(daemon);
        })
        .await;
}

/// INVARIANT (#246): ingest connections have their own ceiling, and a producer over it is not
/// reset: it connects, waits in the kernel backlog and is served when a slot frees, while a
/// private-socket CLI connection is served at once. The wait is counted in status.
#[tokio::test(flavor = "current_thread")]
async fn an_ingest_peer_over_the_ceiling_waits_and_is_served_when_a_slot_frees() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let (ingest, gid) = configure_ingest_socket(dir.path(), &config, 1);
            let daemon = start_with_ingest(&config, &socket, &ingest).await;
            let held = sparrow_client::Client::connect(&ingest).await.unwrap();
            let over = tokio::task::spawn_local({
                let ingest = ingest.clone();
                async move { sparrow_client::Client::connect(&ingest).await }
            });
            tokio::task::yield_now().await;
            let status = cli(&socket, &["status"]);
            let reported = &status["ingest_socket"];
            assert_eq!(reported["path"].as_str(), ingest.to_str());
            assert_eq!(reported["gid"].as_u64(), Some(u64::from(gid)));
            assert_eq!(reported["max_connections"].as_u64(), Some(1));
            assert_eq!(reported["active_connections"].as_u64(), Some(1));
            assert_eq!(reported["refused_connections"].as_str(), Some("0"));
            assert_eq!(reported["ceiling_wait"]["waits"].as_str(), Some("1"));
            assert_eq!(reported["ceiling_wait"]["waiting"].as_u64(), Some(1));
            assert!(!over.is_finished(), "an over-ceiling peer was not held");
            drop(held);
            let served = over.await.unwrap().expect("the waiting peer was reset");
            assert_eq!(
                served.discovery().store_id,
                status["store_id"].as_str().unwrap()
            );
            drop(served);
            drop(daemon);
        })
        .await;
}

/// Read `status` round trips on `client` until `wanted` holds of its admission section. Each turn
/// is a real status request, so this waits on the daemon's own state, not on a clock; it fails
/// after a bounded number of turns.
async fn status_until(
    client: &wire::sparrow::Client,
    wanted: impl Fn(wire::admission_status::Reader<'_>) -> bool,
) {
    for _ in 0..2000 {
        let response = client.status_request().send().promise.await.unwrap();
        let status = rpc_ok(response.get().unwrap().get_result().unwrap());
        if wanted(status.get_admission().unwrap()) {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("status never reached the expected admission state");
}

/// Begin one staged import of `name`'s bytes; its import id, or the error code.
async fn begin_numbered_import(
    client: &wire::sparrow::Client,
    name: &str,
) -> Result<String, String> {
    let input = name.as_bytes();
    let payload = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(input).as_bytes())
    );
    let mut request = client.import_begin_request();
    let mut begin = request.get().init_request();
    begin.set_request_id(name);
    begin.set_source("numbered");
    begin.set_format(wire::Format::Log);
    begin.set_byte_length(input.len() as u64);
    begin.set_payload_id(&payload);
    let response = request.send().promise.await.unwrap();
    match response
        .get()
        .unwrap()
        .get_result()
        .unwrap()
        .which()
        .unwrap()
    {
        wire::result::Ok(started) => match started.unwrap().which().unwrap() {
            wire::import_start::Staging(progress) => Ok(progress
                .unwrap()
                .get_id()
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned()),
            wire::import_start::Accepted(_) => panic!("new input already accepted"),
        },
        wire::result::Error(error) => Err(error
            .unwrap()
            .get_code()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned()),
    }
}

/// INVARIANT (#246): a seventeenth concurrent import waits for a journal slot instead of being
/// refused at once, and is served when a reclamation pass frees one (a cancelled import frees
/// its slot only when reclaimed); the wait is counted in status.
#[tokio::test(flavor = "current_thread")]
async fn a_seventeenth_import_waits_for_a_slot_and_begins_when_one_is_reclaimed() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let daemon = start(&config, &socket).await;
            let connection = connect(&socket).await.unwrap();
            let client = connection.client.clone();
            let mut ids = Vec::new();
            for index in 0..16 {
                ids.push(
                    begin_numbered_import(&client, &format!("slot-{index}"))
                        .await
                        .unwrap(),
                );
            }
            let seventeenth = tokio::task::spawn_local({
                let client = client.clone();
                async move { begin_numbered_import(&client, "slot-16").await }
            });
            status_until(&client, |admission| {
                let waiting = admission.get_import_slot_wait().unwrap();
                (waiting.get_waits(), waiting.get_waiting()) == (1, 1)
            })
            .await;
            assert!(
                !seventeenth.is_finished(),
                "the seventeenth import was refused instead of waiting"
            );
            let response = client.status_request().send().promise.await.unwrap();
            let status = rpc_ok(response.get().unwrap().get_result().unwrap());
            let store = status.get_store_id().unwrap().to_str().unwrap().to_owned();
            let mut request = client.import_cancel_request();
            upload_ref(request.get().init_import(), &store, &ids[0]);
            request.send().promise.await.unwrap();
            assert!(seventeenth.await.unwrap().is_ok());
            drop(connection);
            drop(daemon);
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
            let tcp = tcp_client(address).await.unwrap();
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
            let tcp = tcp_client(address).await.unwrap();
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
        // #215: status carries the lost-stream-evidence report and its naming bound.
        let loss = status.get_stream_evidence_loss().unwrap();
        assert_eq!(loss.get_max_named(), 16);
        assert_eq!(loss.get_total_events(), 0);
        assert!(loss.get_named().unwrap().is_empty());
        let tcp = tcp_client(rpc_address).await.unwrap();
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
            // UDP carries the kernel receive buffer and a drop counter that is either read or stated
            // unavailable (never a guessed zero); TCP has neither.
            if listener.get_transport().unwrap() == wire::SyslogTransport::Udp {
                assert_eq!(listener.get_rcvbuf_requested_bytes(), 8 * 1024 * 1024);
                assert!(listener.get_rcvbuf_granted_bytes() > 0);
                let state = listener.get_kernel_counter_state().unwrap();
                assert_ne!(state, wire::KernelCounterState::NotApplicable);
                assert_eq!(listener.get_kernel_drops_unknown(), state == wire::KernelCounterState::Unavailable);
            } else {
                assert!(!listener.get_kernel_drops_unknown());
                assert_eq!(listener.get_kernel_counter_state().unwrap(), wire::KernelCounterState::NotApplicable);
                assert_eq!(listener.get_rcvbuf_granted_bytes(), 0);
            }
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
            if listeners.get(0).get_accepted_messages() == 5 && listeners.get(0).get_framing_failures() == 1
                && listeners.get(1).get_accepted_messages() == 2 && listeners.get(1).get_truncated_datagrams() == 0
                && listeners.get(2).get_accepted_messages() == 1 && listeners.get(3).get_accepted_messages() == 1 {
                assert_eq!(listeners.get(0).get_interpretation_failures(), 2);
                assert_eq!(status.get_live_records(), 9);
                break;
            }
            assert!(Instant::now() < deadline, "syslog publication did not reach expected counters");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut idle = tokio::net::TcpStream::connect(addresses[0]).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let response = client.capability().status_request().send().promise.await.unwrap();
            let listener = rpc_ok(response.get().unwrap().get_result().unwrap()).get_syslog_listeners().unwrap().get(0);
            if listener.get_active_connections() == 1 { break; }
            assert!(Instant::now() < deadline); tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // At the ceiling the arrival closes the longest-idle connection and is served, not refused;
        // idling between frames is no timeout.
        let excess = tokio::net::TcpStream::connect(addresses[0]).await.unwrap();
        let mut byte = [0_u8];
        assert_eq!(tokio::time::timeout(Duration::from_secs(5), tokio::io::AsyncReadExt::read(&mut idle, &mut byte)).await.unwrap().unwrap(), 0);
        loop {
            let response = client.capability().status_request().send().promise.await.unwrap();
            let listener = rpc_ok(response.get().unwrap().get_result().unwrap()).get_syslog_listeners().unwrap().get(0);
            if listener.get_active_connections() == 1 { assert_eq!((listener.get_rejected_connections(), listener.get_timed_out_connections()), (0, 0)); break; }
            assert!(Instant::now() < deadline); tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let observed = cli(&endpoint, &["syslog-listener", &store_id, "syslog-octets", "octets"]);
        assert_eq!(observed["listener"]["accepted_messages"].as_str(), Some("5"));
        assert_eq!(observed["listener"]["interpretation_failures"].as_str(), Some("2"));
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
                assert_eq!(history["accepted"].as_str(), Some("5"));
                assert_eq!(history["framing_failures"].as_str(), Some("1"));
                assert_eq!(history["timed_out"].as_str(), Some("0"));
                assert_eq!(history["gaps"][0]["first"].as_str(), Some("6"));
            }
            if source["source_id"].as_str() == Some("syslog-datagrams") {
                assert_eq!(history["observations"].as_str(), Some("2"));
                assert_eq!(history["accepted"].as_str(), Some("2"));
                assert_eq!(history["truncated"].as_str(), Some("0"));
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
        assert_eq!(rows.len(), 5);
        assert_eq!(store.receiver_window(&receiver_ids["syslog-octets"], now).unwrap().observations, 6);
        for record in rows {
            let position = record.receiver.as_ref().unwrap();
            assert_eq!(position.window_id, receiver_ids["syslog-octets"]);
            assert!((1..=5).contains(&position.sequence));
            let bytes = store.bytes(&record.artifact_id, record.byte_start, (record.byte_end-record.byte_start) as usize, now).unwrap();
            let snapshot = record.syslog.unwrap();
            if bytes == modern { assert_eq!(snapshot.structured_data[0].parameters.len(), 2); }
            else if bytes.starts_with(b"<999>") || bytes == b"9 short" { assert!(snapshot.diagnostic.is_some()); }
            else { assert_eq!(bytes, b"<13>Oct  8 06:25:00 host daemon[123]: legacy needle"); }
        }
        for (source, expected) in [("syslog-lf", b"<13>1 - host app - - - lf needle\r".as_slice()), ("syslog-crlf", b"<13>1 - host app - - - crlf\nneedle")] {
            let rows: Vec<_> = store.records(now).unwrap().into_iter().filter(|row| row.source == source).collect(); assert_eq!(rows.len(), 1);
            assert_eq!(store.bytes(&rows[0].artifact_id, 0, expected.len(), now).unwrap(), expected);
        }
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn syslog_connection_failure_is_counted_keeps_siblings_and_reopens_exact_durable_packet() {
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
                Default::default(),
                &config.syslog_spill_dir(),
            )
            .await
            .unwrap();
            // Keep the typed observer available while deliberately failing intake admission.
            let observer: wire::sparrow::Client = capnp_rpc::new_client(
                sparrow_server::Rpc::new(
                    store.clone(),
                    std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
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
            // The failed connection is counted against its listener (#445); the listener
            // group keeps running.
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let response = observer.status_request().send().promise.await.unwrap();
                let listeners = rpc_ok(response.get().unwrap().get_result().unwrap())
                    .get_syslog_listeners()
                    .unwrap();
                if listeners.get(0).get_listener_errors() >= 1 {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "failed connection was not counted"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert!(
                !worker.is_finished(),
                "one connection's failure ended the group"
            );
            // The closed budget still fails the shutdown's own publications, and says so.
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
                    tcp_client(address).await.unwrap()
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
            let sixth = tcp_client(address).await.unwrap();
            let response = sixth
                .capability()
                .status_request()
                .send()
                .promise
                .await
                .unwrap();
            rpc_ok(response.get().unwrap().get_result().unwrap());
            // Unix and TCP share six transport leases. The sixth peer took the last slot: status
            // shows six active connections and the accept loop held at the ceiling, and a TCP
            // seventh peer waits in the backlog (#246) instead of being admitted or reset.
            let status_client = idle[0].capability().clone();
            let tcp_seventh = tokio::task::spawn_local(tcp_client(address));
            status_until(&status_client, |admission| {
                let ceiling = match admission.get_rpc_connections().unwrap().which().unwrap() {
                    wire::optional_connection_ceiling_status::Value(ceiling) => ceiling.unwrap(),
                    wire::optional_connection_ceiling_status::Absent(()) => {
                        panic!("no ceiling reported")
                    }
                };
                assert!(
                    ceiling.get_active_connections() <= 6,
                    "the ceiling was exceeded"
                );
                ceiling.get_max_connections() == 6
                    && ceiling.get_active_connections() == 6
                    && ceiling.get_ceiling_wait().unwrap().get_waiting() == 1
            })
            .await;
            assert!(
                !tcp_seventh.is_finished(),
                "a seventh peer was over-admitted or reset"
            );
            drop(sixth);
            let tcp_seventh = tcp_seventh
                .await
                .unwrap()
                .expect("the waiting TCP peer was reset");
            // The slot went to the TCP peer; a Unix peer now waits for the shared ceiling too.
            // Witness that it reached the daemon's listen queue: a raw connect completes only
            // once the kernel has queued the peer, with no accept, so it is deterministic. Status
            // cannot witness it: every admission counter belongs to the accept loop, which waits
            // for a slot whether or not a Unix peer exists. Then a typed client queues behind it.
            let queued = tokio::net::UnixStream::connect(&socket)
                .await
                .expect("the Unix peer did not reach the listen queue");
            let unix_seventh = tokio::task::spawn_local({
                let socket = socket.clone();
                async move { sparrow_client::Client::connect(&socket).await }
            });
            // The TCP peer took the freed slot; the Unix peers are held behind it, not admitted.
            status_until(&status_client, |admission| {
                let wire::optional_connection_ceiling_status::Value(ceiling) =
                    admission.get_rpc_connections().unwrap().which().unwrap()
                else {
                    panic!("no ceiling reported");
                };
                let ceiling = ceiling.unwrap();
                assert!(
                    ceiling.get_active_connections() <= 6,
                    "the ceiling was exceeded"
                );
                ceiling.get_active_connections() == 6
                    && ceiling.get_ceiling_wait().unwrap().get_waiting() == 1
            })
            .await;
            assert!(
                !unix_seventh.is_finished(),
                "a Unix peer was over-admitted or reset"
            );
            // The queued raw peer ends; the daemon serves or drops it and the slot passes on.
            drop(queued);
            drop(tcp_seventh);
            let recovered = unix_seventh
                .await
                .unwrap()
                .expect("the waiting Unix peer was reset");
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
            let changed = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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
                let output = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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
                let output = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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

/// Behaviour protected: artifact listing pages are typed and shared by the CLI and MCP, and an
/// artifact that is due for expiry but unarchived is held, not reclaimed. With no archive
/// policy the expiry pass retires nothing, so the listing still reports the artifact's origin,
/// its complete bytes and both retained records, and a receipt window finds it. Agent
/// refusals keep their typed codes.
#[tokio::test(flavor = "current_thread")]
async fn artifact_listing_shares_typed_pages_held_origins_and_agent_refusals() {
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
            // The fragment is 31 days old and due, and nothing is archived: expiry holds.
            assert_eq!(owner.expire(clock).unwrap(), 0);
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
            assert_eq!(first["total_items"].as_str(), Some("1"));
            // Until the walk's final page the count covers delivered rows only.
            assert_eq!(first["has_more"].as_bool(), Some(true));
            assert_eq!(first["matching_artifacts"], Value::Null);
            assert_eq!(first["known_matching_artifacts"].as_str(), Some("1"));
            assert_eq!(first["answer_certainty"].as_str(), Some("incomplete"));
            assert_eq!(first["coverage"]["catalog_complete"].as_bool(), Some(false));
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
            // A live continuation reads the next per-source index page under a new
            // handle, and the later artifact still lies ahead of it.
            let second = &second["structuredContent"];
            assert_ne!(second["snapshot_id"], first["snapshot_id"]);
            assert_eq!(second["total_items"].as_str(), Some("2"));
            assert_eq!(second["has_more"].as_bool(), Some(true));
            assert_eq!(second["matching_artifacts"], Value::Null);
            assert_eq!(second["answer_certainty"].as_str(), Some("incomplete"));
            assert_ne!(
                second["rows"][0]["artifact_id"],
                first["rows"][0]["artifact_id"]
            );
            let spent = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
                .arg(&socket)
                .args([
                    "artifacts",
                    &store,
                    "--source",
                    "wire-catalog",
                    "--page-size",
                    "1",
                    "--cursor",
                    cursor,
                ])
                .admitted_output()
                .unwrap();
            assert!(String::from_utf8_lossy(&spent.stderr).contains("RESULT_EXPIRED"));
            let cursor = second["next_cursor"].as_str().unwrap();
            let fragments = cli(
                &socket,
                &["artifacts", &store, "--source", "wire-fragments"],
            );
            assert_eq!(
                fragments["rows"][0]["artifact_id"].as_str(),
                Some(fragment.id.as_str())
            );
            assert_ne!(fragments["rows"][0]["origin"], Value::Null);
            assert_eq!(
                fragments["rows"][0]["complete_bytes_available"].as_bool(),
                Some(false)
            );
            // Counts live records at `now`: the pinned one. The unpinned record is expired but kept.
            assert_eq!(fragments["rows"][0]["retained_records"].as_str(), Some("1"));
            // The held artifact keeps its receipt, so a receipt window finds it with its origin.
            let windowed = cli(
                &socket,
                &[
                    "artifacts",
                    &store,
                    "--source",
                    "wire-fragments",
                    "--received-from",
                    "1970-01-01T00:00:00Z",
                    "--received-before",
                    "2099-01-01T00:00:00Z",
                ],
            );
            assert_eq!(windowed["rows"].as_array().unwrap().len(), 1);
            assert_eq!(
                windowed["rows"][0]["artifact_id"],
                fragments["rows"][0]["artifact_id"]
            );
            assert_ne!(windowed["rows"][0]["origin"], Value::Null);
            assert_eq!(windowed["rows"][0]["retained_records"].as_str(), Some("1"));
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
            ] {
                let output = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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
            // The final page of one uninterrupted walk states the exact count.
            let last = cli(
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
            assert_eq!(last["has_more"].as_bool(), Some(false));
            assert_eq!(last["matching_artifacts"].as_str(), Some("3"));
            assert_eq!(last["answer_certainty"].as_str(), Some("exact"));
            assert_eq!(last["coverage"]["catalog_complete"].as_bool(), Some(true));
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
            // Delivering the final page retired the last continued handle.
            assert_eq!(status["artifact_read_handles"].as_str(), Some("0"));
        })
        .await;
}

/// Discovery and status read no record row (#146: status reads fixed counter keys), so a
/// corrupt record leaves both answering with the counted totals; a read of the record
/// itself still refuses it with `STORAGE_FAILURE` rather than concealing it.
#[tokio::test(flavor = "current_thread")]
async fn bounded_discovery_and_status_do_not_read_corrupt_evidence_and_keys_refuses_it() {
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
            // Each discovery samples its own server-clock deadline. The negotiated
            // capabilities and identities are stable across both reads.
            let cached = client.discovery();
            assert!(cached.sealed_event_deadline_ms > 0 && discovery.sealed_event_deadline_ms > 0);
            assert_eq!(
                discovery,
                sparrow_client::Discovery {
                    sealed_event_deadline_ms: discovery.sealed_event_deadline_ms,
                    ..cached.clone()
                }
            );
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
            assert_eq!(
                rpc_ok(response.get().unwrap().get_result().unwrap()).get_live_records(),
                1
            );
            let output = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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
            assert_eq!(strict["structuredContent"]["error"]["code"].as_str(),Some("EVIDENCE_INCOMPLETE"));
            args.extend(["--require-complete-index","true"]);
            let refused=Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture")).arg(&socket).args(&args).admitted_output().unwrap();
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

/// Behaviour protected: with no archive policy the daemon's expiry holds at the archive gate.
/// An artifact whose records are all due is never retired: the daemon journals the held pass
/// (`archive_waiting=1`), every record stays readable by id and by bytes, and a reopened store
/// still holds every record and the pin that protected the first.
#[tokio::test(flavor = "current_thread")]
async fn daemon_expiry_holds_unarchived_evidence_at_the_archive_gate() {
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
            let mut daemon = start_with_stderr(&config, &socket, Stdio::piped()).await;
            // The daemon's first expiry pass finds the due artifact and reports it held.
            let journal = journal_lines(&mut daemon);
            await_journal(&journal, &["expiry pass", "archive_waiting=1"]);
            let connection = connect(&socket).await.unwrap();
            for id in &artifact.record_ids {
                let mut request = connection.client.record_get_request();
                let mut reference = request.get().init_record();
                reference.set_store_id(&store_id);
                reference.set_id(id);
                let response = request.send().promise.await.unwrap();
                rpc_ok(response.get().unwrap().get_result().unwrap());
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
            // Only the pinned record is live at `now`; the expired unpinned records are hidden
            // from the listing but still retained, and each is read by id below.
            assert_eq!(store.records(now).unwrap().len(), 1);
            for id in &artifact.record_ids {
                store.record(id).unwrap();
            }
        })
        .await;
}

/// Forward the daemon's stderr lines to a channel, so a test waits for a journal event under a
/// deadline instead of reading the pipe blocking.
fn journal_lines(daemon: &mut Daemon) -> std::sync::mpsc::Receiver<String> {
    use std::io::BufRead;
    let (sender, receiver) = std::sync::mpsc::channel();
    let stderr = daemon.0.stderr.take().unwrap();
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(stderr).lines() {
            let Ok(line) = line else { return };
            if sender.send(line).is_err() {
                return;
            }
        }
    });
    receiver
}

/// Wait up to 10 seconds for a journal line containing every needle.
fn await_journal(lines: &std::sync::mpsc::Receiver<String>, needles: &[&str]) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut seen = Vec::new();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let line = lines.recv_timeout(left).unwrap_or_else(|_| {
            panic!("daemon never journaled a line with {needles:?}; it journaled {seen:#?}")
        });
        if needles.iter().all(|needle| line.contains(needle)) {
            return;
        }
        seen.push(line);
    }
}

/// Behaviour protected: with no archive policy, pressure never retires evidence. The worker
/// waits on the archive (`awaiting_archive`, frontier `archive_lag`, no evicted records), a
/// write over the reserve is refused with a retryable `STORAGE_PRESSURE` and refused again
/// while the ballast stays, every record stays readable, and CLI and MCP share the typed
/// status. Once space returns the same request is accepted.
#[tokio::test(flavor = "current_thread")]
async fn daemon_pressure_worker_holds_unarchived_evidence_and_shares_typed_cli_mcp_status() {
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
            if pressure.get_phase().unwrap() == wire::PressurePhase::AwaitingArchive {
                assert_eq!(pressure.get_evicted_records(), 0);
                assert_eq!(pressure.get_evicted_recordless_artifacts(), 0);
                assert!(pressure.get_requested_bytes() > 0);
                assert!(pressure.get_passes() >= 1);
                let wire::pressure_pass::Present(pass) = pressure.get_last_pass().unwrap().which().unwrap() else { panic!("a held pass is reported") };
                assert!(pass.unwrap().get_frontier().unwrap() == wire::EvictionFrontier::ArchiveLag);
                assert!(matches!(pressure.get_failure().unwrap().which().unwrap(), wire::optional_text::Absent(())));
                break;
            }
            assert!(Instant::now() < deadline, "owned daemon never held pressure retirement at the archive gate");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let cli_status = cli(&socket, &["status"]);
        let mcp_status = mcp.tool(3, "status", Object::new().into()).await;
        assert_eq!(cli_status["pressure"]["phase"].as_str(), Some("awaiting_archive"));
        assert_eq!(cli_status["pressure"]["evicted_records"].as_str(), Some("0"));
        assert_eq!(cli_status["pressure"]["evicted_recordless_artifacts"].as_str(), Some("0"));
        // The status says retirement is held for want of an archive, and where the reserve line is.
        assert_eq!(cli_status["pressure"]["archive_configured"].as_bool(), Some(false));
        assert_eq!(cli_status["pressure"]["archive_held"].as_bool(), Some(true));
        assert_eq!(cli_status["pressure"]["disk_budget"].as_str(), Some("16777216"));
        assert_eq!(cli_status["pressure"]["maintenance_reserve"].as_str(), Some("4194304"));
        assert!(cli_status["pressure"]["reserve_headroom"].as_str().unwrap().starts_with('-'), "13 MiB of ballast puts the store over a 12 MiB line");
        assert_eq!(mcp_status["structuredContent"]["pressure"]["archive_held"], cli_status["pressure"]["archive_held"]);
        assert_eq!(mcp_status["structuredContent"]["pressure"]["reserve_headroom"], cli_status["pressure"]["reserve_headroom"]);
        assert_eq!(mcp_status["structuredContent"]["pressure"]["phase"], cli_status["pressure"]["phase"]);
        assert_eq!(mcp_status["structuredContent"]["pressure"]["evicted_recordless_artifacts"], cli_status["pressure"]["evicted_recordless_artifacts"]);
        assert_eq!(mcp_status["structuredContent"]["pressure"]["evicted_records"], cli_status["pressure"]["evicted_records"]);
        // A new store owes no table rewrite, and both adapters render the same status.
        assert_eq!(cli_status["recompression"]["phase"].as_str(), Some("complete"));
        assert_eq!(mcp_status["structuredContent"]["recompression"], cli_status["recompression"]);
        let retained = mcp.tool(4, "bytes_read", Object::new().with("store_id", &protected.store_id)
            .with("id", &protected.id).with("offset", "0").with("length", 7_u64).into()).await;
        assert_eq!(retained["isError"].as_bool(), Some(false));
        // The oldest evidence is held, not retired: it stays readable under the refusal.
        let held = mcp.tool(5, "bytes_read", Object::new().with("store_id", &old.store_id)
            .with("id", &old.id).with("offset", "0").with("length", 1_u64).into()).await;
        assert_eq!(held["isError"].as_bool(), Some(false));
        let refused = producer.ingest("pressure-retry", "source", wire::Format::Log, b"later\n").await.unwrap_err();
        assert!(matches!(refused, sparrow_client::Error::Domain { ref code, retryable: true, .. } if code == "STORAGE_PRESSURE"));
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

/// The one `.gts` segment files the archive worker has written for a store under `nas`.
fn archived_segments(nas: &Path, store_id: &str) -> Vec<std::path::PathBuf> {
    let Ok(entries) = std::fs::read_dir(nas.join(store_id)) else {
        return Vec::new();
    };
    entries
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "gts"))
        .collect()
}

/// Behaviour protected: with an archive configured, the daemon's expiry retires due evidence
/// only after the archive worker has written its segment. The record that the pin protects is
/// archived with the rest and stays; when expiry reports it retired the unpinned records, the
/// segment is already on the archive root, and the archive's watermark covers them.
#[tokio::test(flavor = "current_thread")]
async fn daemon_expiry_retires_evidence_only_after_its_segment_is_written() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            // The mount guard needs the archive directory on a device other than `/`'s and on a
            // network type or one unlike `/`'s; /dev/shm (tmpfs) is both.
            let shm = tempfile::tempdir_in("/dev/shm").unwrap();
            let nas = shm.path().join("nas");
            std::fs::create_dir(&nas).unwrap();
            let config = write_server_config(dir.path(), &data, &socket);
            let mut text = std::fs::read_to_string(&config).unwrap();
            text.push_str(&format!("\n[archive]\ndir = {nas:?}\ncheck_secs = 1\n"));
            std::fs::write(&config, text).unwrap();
            let settings = sparrow_server::ServerConfig::read(&config).unwrap();
            let store = settings.open_store().unwrap();
            let store_id = store.store_id().unwrap();
            std::fs::write(nas.join("STORE_ID"), format!("{store_id}\n")).unwrap();
            let artifact = store
                .ingest(
                    "daemon-expiry-archived",
                    "source",
                    "log",
                    &b"expired occurrence\n".repeat(5),
                    0,
                )
                .unwrap();
            store
                .pin(
                    "daemon-expiry-archived-pin",
                    "case",
                    &artifact.record_ids[..1],
                    30000,
                    0,
                )
                .unwrap();
            drop(store);
            assert!(archived_segments(&nas, &store_id).is_empty());
            let mut daemon = start_with_stderr(&config, &socket, Stdio::piped()).await;
            let journal = journal_lines(&mut daemon);
            // Expiry retires the unpinned records: no pass held, and the segment is there.
            await_journal(&journal, &["expiry pass", "archive_waiting=0", "deleted="]);
            let segments = archived_segments(&nas, &store_id);
            assert_eq!(segments.len(), 1, "one segment holds the archived evidence");
            let bytes = std::fs::read(&segments[0]).unwrap();
            assert!(
                purrdf::gts::reader::read(&bytes, false, None)
                    .diagnostics
                    .is_empty()
            );
            assert!(
                !std::fs::read_dir(nas.join(&store_id))
                    .unwrap()
                    .any(|entry| entry
                        .unwrap()
                        .path()
                        .to_string_lossy()
                        .ends_with(".partial")),
                "no partial file is left behind"
            );
            drop(daemon);
            let store = settings.open_store().unwrap();
            let state = store.archive_state().unwrap().unwrap();
            assert!(
                state.through.is_some(),
                "the watermark covers what was retired"
            );
            let now = sparrow_server::now().unwrap();
            assert_eq!(
                store.records(now).unwrap().len(),
                1,
                "only the pinned record is live"
            );
        })
        .await;
}

/// Behaviour protected: with an archive configured, the pressure worker retires evidence only
/// once its segment is on the archive root. The store that no-archive pressure holds at
/// `awaiting_archive` evicts after the archive worker commits, and by the time the status shows
/// an evicted record the segment file exists and the status no longer reports a hold.
#[tokio::test(flavor = "current_thread")]
async fn daemon_pressure_worker_evicts_only_after_its_segment_is_written() {
    tokio::task::LocalSet::new().run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let socket = data.join("rpc.sock");
        // The mount guard needs the archive directory on a device other than `/`'s and on a
        // network type or one unlike `/`'s; /dev/shm (tmpfs) is both.
        let shm = tempfile::tempdir_in("/dev/shm").unwrap();
        let nas = shm.path().join("nas");
        std::fs::create_dir(&nas).unwrap();
        let config = write_server_config(dir.path(), &data, &socket);
        let text = std::fs::read_to_string(&config).unwrap()
            .replace("disk_budget = 134217728", "disk_budget = 16777216")
            .replace("maintenance_reserve = 8388608", "maintenance_reserve = 4194304");
        std::fs::write(&config, format!("{text}\n[archive]\ndir = {nas:?}\ncheck_secs = 1\n")).unwrap();
        // Evidence 25 days old: past the archive's 75 per cent start, not yet expired.
        let settings = sparrow_server::ServerConfig::read(&config).unwrap();
        let owner = settings.open_store().unwrap();
        let store_id = owner.store_id().unwrap();
        std::fs::write(nas.join("STORE_ID"), format!("{store_id}\n")).unwrap();
        let received = sparrow_server::now().unwrap() - 25 * 86_400_000;
        owner.ingest("pressure-archived-old", "source", "log", b"old\n", received).unwrap();
        drop(owner);
        let daemon = start(&config, &socket).await;
        let producer = sparrow_client::Client::connect(&socket).await.unwrap();
        let ballast = data.join("test-owned-pressure-ballast");
        std::fs::write(&ballast, vec![0_u8; 13 * 1024 * 1024]).unwrap();
        let error = producer.ingest("pressure-archived-retry", "source", wire::Format::Log, b"later\n").await.unwrap_err();
        assert!(matches!(error, sparrow_client::Error::Domain { ref code, retryable: true, .. } if code == "STORAGE_PRESSURE"));
        let connection = connect(&socket).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let response = connection.client.status_request().send().promise.await.unwrap();
            let status = rpc_ok(response.get().unwrap().get_result().unwrap());
            let pressure = status.get_pressure_status().unwrap();
            if pressure.get_evicted_records() > 0 {
                assert_eq!(pressure.get_disk_budget(), 16_777_216);
                assert!(pressure.get_archive_configured());
                assert!(!pressure.get_archive_held(), "the archive no longer holds the evidence");
                assert!(
                    !archived_segments(&nas, &store_id).is_empty(),
                    "evidence was evicted before its segment was written"
                );
                break;
            }
            assert!(Instant::now() < deadline, "the pressure worker never evicted archived evidence");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
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
        publish_hold: false,
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
        let duplicate_prepare = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture")).arg(&socket)
            .args(["watch", "handoff-prepare", "explicit-1", "completed", "1", "finished.jsonl", intent_file.to_str().unwrap()]).admitted_output().unwrap();
        assert!(!duplicate_prepare.status.success());
        assert_eq!(std::fs::read(&intent_file).unwrap(), saved_bytes);
        let unsafe_intent = root.join("must-not-consume.capnp");
        let unsafe_prepare = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture")).arg(&socket)
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
        assert_eq!(status.get_store_format().unwrap().to_str().unwrap(), "sparrow.store/355");
        assert_eq!(status.get_live_records(), 2);
        assert!(status.get_consume_watches().unwrap().is_empty());
        drop(client); drop(daemon);
    }).await;
}

/// INVARIANT: a `watch put` whose root lies inside a path the daemon writes (here the
/// archive directory) is refused naming both paths, because live watches come from the stored
/// catalog and `server.toml` only seeds it; a root beside those paths is still accepted.
#[tokio::test(flavor = "current_thread")]
async fn watch_put_refuses_a_root_inside_a_daemon_output_path() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let archive = dir.path().join("archive");
            let inside = archive.join("inside");
            let beside = dir.path().join("beside");
            std::fs::create_dir_all(&inside).unwrap();
            std::fs::create_dir(&beside).unwrap();
            let mut text = std::fs::read_to_string(&config).unwrap();
            text.push_str(&format!("[archive]\ndir = {archive:?}\n"));
            std::fs::write(&config, text).unwrap();
            let _daemon = start(&config, &socket).await;
            let put = |root: &Path, request: &str| {
                let file = dir.path().join(format!("{request}.toml"));
                let definition = watch_definition(root, request, 30_000, 16, 16);
                std::fs::write(&file, toml::to_string(&definition).unwrap()).unwrap();
                Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
                    .args(endpoint_args(&socket))
                    .args(["watch", "put", request, "0", file.to_str().unwrap()])
                    .admitted_output()
                    .unwrap()
            };
            let refused = put(&inside, "inside");
            let errors = String::from_utf8_lossy(&refused.stderr);
            assert!(!refused.status.success(), "{errors}");
            assert!(errors.contains("overlaps archive.dir"), "{errors}");
            assert!(errors.contains(&inside.display().to_string()), "{errors}");
            let accepted = put(&beside, "beside");
            assert!(
                accepted.status.success(),
                "{}",
                String::from_utf8_lossy(&accepted.stderr)
            );
        })
        .await;
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
                let failed = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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
                Some("sparrow.store/354")
            );
        })
        .await;
}

/// INVARIANT (#222, #214): the running watch loop keeps a claim's failure when a different claim
/// succeeds. This drives the real success path (the loop's call to `record_claim_success` after
/// a publication through the daemon's ingest socket), which the worker's unit test of the method
/// cannot pin: a loop that cleared the failure on any success would pass that test and fail
/// here. A claim whose private source is gone stays pending and fails; a file that arrives
/// after its first failed attempt is consumed; at the status that first reports that file's
/// completion, the stuck claim's failure must still be there. The stuck claim's retry is 5
/// seconds after its failure and the file completes after its quiet time, so the success is
/// observed before the retry can set the failure again.
#[tokio::test(flavor = "current_thread")]
async fn another_claims_publication_leaves_a_stuck_claims_failure_in_status() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use sparrow_server::{ServerConfig, consumption::ClaimRoot};
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("completed");
            std::fs::create_dir(&root).unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            append_consume_config(&config, &root, 100, 16, 16);
            let lost = {
                let store = ServerConfig::read(&config).unwrap().open_store().unwrap();
                let owner = ClaimRoot::open(store, "completed", &root).unwrap();
                let path = root.join("lost.jsonl");
                std::fs::write(&path, b"{\"name\":\"lost\"}\n").unwrap();
                let identity =
                    sparrow_client::CompletedFileIdentity::capture(&path.metadata().unwrap());
                let journal = owner
                    .reserve(
                        Path::new("lost.jsonl"),
                        identity,
                        "consume-acceptance",
                        "jsonl",
                    )
                    .unwrap();
                owner
                    .recover(&journal.import_id, sparrow_server::now().unwrap())
                    .unwrap();
                std::fs::remove_file(root.join(".sparrow-inflight").join(&journal.import_id))
                    .unwrap();
                journal.import_id
            };
            let _daemon = start(&config, &socket).await;
            let producer = sparrow_client::Client::connect(&socket).await.unwrap();
            watch_until(producer.capability(), |watch| {
                watch.get_pending_claims() == 1
                    && matches!(
                        watch.get_failure().unwrap().which().unwrap(),
                        wire::optional_text::Value(_)
                    )
            })
            .await;
            std::fs::write(root.join("fresh.jsonl"), b"{\"name\":\"fresh\"}\n").unwrap();
            let at_completion = std::cell::RefCell::new(None);
            watch_until(producer.capability(), |watch| {
                if watch.get_completed_claims() == 0 {
                    return false;
                }
                let failure = match watch.get_failure().unwrap().which().unwrap() {
                    wire::optional_text::Value(text) => {
                        Some(text.unwrap().to_str().unwrap().to_owned())
                    }
                    wire::optional_text::Absent(()) => None,
                };
                *at_completion.borrow_mut() = Some((watch.get_pending_claims(), failure));
                true
            })
            .await;
            let (pending, failure) = at_completion.into_inner().unwrap();
            assert_eq!(pending, 1, "the stuck claim is still pending");
            let failure =
                failure.expect("the stuck claim's failure vanished with another claim's success");
            assert!(failure.contains(&lost), "{failure}");
        })
        .await;
}

/// INVARIANT (D118): `watch retire-claim` retires only a claim that can never complete. A
/// claim whose private source still exists is refused; one whose private source is missing
/// leaves pending_claims, is listed as retired-unrecoverable in status and the watch listing
/// with its caller and reason, and writes one journal line. A file that arrives while a claim
/// is stuck keeps the drain a store stop waits for (waiting files plus pending claims) above
/// zero; once every claim is retired and the file is consumed it reaches zero.
#[tokio::test(flavor = "current_thread")]
async fn retire_claim_clears_only_an_unrecoverable_claim_and_lets_the_drain_reach_zero() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use sparrow_server::{ServerConfig, consumption::ClaimRoot};
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("completed");
            std::fs::create_dir(&root).unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            append_consume_config(&config, &root, 1_000, 16, 16);
            // Two claims whose sources moved into the private claim directory.
            let [lost, replaced] = {
                let store = ServerConfig::read(&config).unwrap().open_store().unwrap();
                let owner = ClaimRoot::open(store, "completed", &root).unwrap();
                ["lost.jsonl", "replaced.jsonl"].map(|name| {
                    let path = root.join(name);
                    std::fs::write(&path, format!("{{\"name\":\"{name}\"}}\n")).unwrap();
                    let identity =
                        sparrow_client::CompletedFileIdentity::capture(&path.metadata().unwrap());
                    let journal = owner
                        .reserve(Path::new(name), identity, "consume-acceptance", "jsonl")
                        .unwrap();
                    owner
                        .recover(&journal.import_id, sparrow_server::now().unwrap())
                        .unwrap();
                    journal.import_id
                })
            };
            let inflight = root.join(".sparrow-inflight");
            // One private source is gone; the other is a different file, so neither recovers.
            std::fs::remove_file(inflight.join(&lost)).unwrap();
            std::fs::remove_file(inflight.join(&replaced)).unwrap();
            std::fs::write(inflight.join(&replaced), b"not the claimed generation\n").unwrap();
            let log = dir.path().join("daemon.log");
            let daemon = start_with_stderr(
                &config,
                &socket,
                Stdio::from(std::fs::File::create(&log).unwrap()),
            )
            .await;
            let producer = sparrow_client::Client::connect(&socket).await.unwrap();
            watch_until(producer.capability(), |watch| {
                watch.get_pending_claims() == 2
                    && matches!(
                        watch.get_failure().unwrap().which().unwrap(),
                        wire::optional_text::Value(_)
                    )
            })
            .await;
            // `watch claims` names each stuck claim as pending, with the reason its latest
            // attempt stayed pending and how long since it was first seen (issue 160).
            let mut claims = cli(&socket, &["watch", "claims"]);
            for _ in 0..100 {
                let rows = claims["claims"].as_array().unwrap();
                if rows.len() == 2 && rows.iter().all(|row| row["attempts"].as_u64() >= Some(1)) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                claims = cli(&socket, &["watch", "claims"]);
            }
            assert_eq!(claims["pending"].as_u64(), Some(2), "{claims:?}");
            for row in claims["claims"].as_array().unwrap() {
                assert_eq!(row["state"].as_str(), Some("pending"), "{row:?}");
                assert!(row["attempts"].as_u64() >= Some(1), "{row:?}");
                assert!(row["last_pending_reason"].as_str().is_some(), "{row:?}");
                assert!(
                    row["first_seen"].as_str().is_some() && row["age_ms"].as_str().is_some(),
                    "{row:?}"
                );
            }
            let retire = |claim: &str| {
                Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
                    .arg(&socket)
                    .args([
                        "watch",
                        "retire-claim",
                        "completed",
                        claim,
                        "source deleted",
                    ])
                    .admitted_output()
                    .unwrap()
            };

            // A private source that exists is recoverable or needs reconciliation, never retiring.
            let refused = retire(&replaced);
            assert!(!refused.status.success());
            let stderr = String::from_utf8_lossy(&refused.stderr);
            assert!(
                stderr.contains("CONFLICT") && stderr.contains("still exists"),
                "{stderr}"
            );
            // MCP reaches the same operation and refusal.
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            let store_id = cli(&socket, &["status"])["store_id"]
                .as_str()
                .unwrap()
                .to_owned();
            let refused = mcp
                .tool(
                    2,
                    "watch_claim_retire",
                    purrdf_lex::json::Object::new()
                        .with("store_id", store_id.as_str())
                        .with("watch", "completed")
                        .with("claim_id", replaced.as_str())
                        .with("reason", "source deleted")
                        .into(),
                )
                .await;
            assert_eq!(refused["isError"].as_bool(), Some(true), "{refused:?}");
            assert_eq!(
                refused["structuredContent"]["error"]["code"].as_str(),
                Some("CONFLICT"),
                "{refused:?}"
            );
            drop(mcp);

            // Reconcile the replaced private entry (set its bytes aside): both claims are now
            // unrecoverable. Which claim's failure the watch shows depends on retry timing, so
            // the keep-another-claim's-failure rule is tested in the watch worker's unit tests.
            std::fs::rename(inflight.join(&replaced), dir.path().join("set-aside")).unwrap();
            let retired = cli(
                &socket,
                &[
                    "watch",
                    "retire-claim",
                    "completed",
                    &lost,
                    "source deleted",
                ],
            );
            let claim = &retired["retired_claim"];
            assert_eq!(claim["claim_id"].as_str(), Some(lost.as_str()));
            assert_eq!(claim["state"].as_str(), Some("retired-unrecoverable"));
            assert_eq!(claim["reason"].as_str(), Some("source deleted"));
            assert!(
                claim["caller"].as_str().unwrap().starts_with("unix uid="),
                "{claim:?}"
            );
            let status = cli(&socket, &["status"]);
            let watch = &status["consume_watches"][0];
            assert_eq!(watch["pending_claims"].as_u64(), Some(1), "{watch:?}");
            assert_eq!(
                watch["retired_claims"][0]["claim_id"].as_str(),
                Some(lost.as_str())
            );
            let listed = cli(&socket, &["watch", "list"]);
            assert_eq!(
                listed["watches"][0]["retired_claims"][0]["claim_id"].as_str(),
                Some(lost.as_str())
            );
            // A second retirement of the same claim is refused, not recorded again.
            assert!(!retire(&lost).status.success());

            // A file arrives while a claim is stuck: the drain a store stop waits for
            // (waiting files plus pending claims) is not zero, because the unrecoverable
            // claim stays pending however the new file fares.
            std::fs::write(root.join("waiting.jsonl"), b"{\"waiting\":1}\n").unwrap();
            assert!(root.join("waiting.jsonl").exists());
            assert!(
                cli(&socket, &["status"])["consume_watches"][0]["pending_claims"]
                    .as_u64()
                    .unwrap()
                    >= 1
            );

            // Retiring the last stuck claim lets the waiting file drain and the drain reach
            // zero. Only retired claims ever failed, so no failure remains.
            cli(
                &socket,
                &[
                    "watch",
                    "retire-claim",
                    "completed",
                    &replaced,
                    "source deleted",
                ],
            );
            watch_until(producer.capability(), |watch| {
                watch.get_pending_claims() == 0 && watch.get_completed_claims() >= 1
            })
            .await;
            let status = cli(&socket, &["status"]);
            let watch = &status["consume_watches"][0];
            assert_eq!(watch["pending_claims"].as_u64(), Some(0), "{watch:?}");
            assert_eq!(watch["retired_claims"].as_array().unwrap().len(), 2);
            assert!(watch["failure"].is_null(), "{watch:?}");
            // Both stuck claims went pending in the window and ended abandoned; none pends.
            let claims = cli(
                &socket,
                &["watch", "claims", "--since", "2000-01-01T00:00:00Z"],
            );
            assert_eq!(claims["abandoned"].as_u64(), Some(2), "{claims:?}");
            assert_eq!(claims["pending"].as_u64(), Some(0), "{claims:?}");
            let waiting = std::fs::read_dir(&root)
                .unwrap()
                .filter(|entry| entry.as_ref().unwrap().file_name() != ".sparrow-inflight")
                .count();
            assert_eq!(waiting, 0, "no file waits in the root");
            drop(producer);
            drop(daemon);
            let journal = std::fs::read_to_string(&log).unwrap();
            assert_eq!(
                journal
                    .lines()
                    .filter(|line| line.contains("retired-unrecoverable"))
                    .count(),
                2,
                "one journal line per retirement: {journal}"
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
            let failed = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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

/// The typed failure of one refused write: its code and message.
fn rpc_failure<T: capnp::traits::Owned + 'static>(
    result: wire::result::Reader<'_, T>,
) -> (String, String) {
    match result.which().unwrap() {
        wire::result::Ok(_) => panic!("expected a typed RPC failure"),
        wire::result::Error(error) => {
            let error = error.unwrap();
            (
                error.get_code().unwrap().to_str().unwrap().to_owned(),
                error.get_message().unwrap().to_str().unwrap().to_owned(),
            )
        }
    }
}

/// INVARIANT: a query-only daemon on a copy of a store with a registered watch answers reads
/// as a normal daemon does, opens and claims nothing in the stored watch root, and refuses
/// writes with `READ_ONLY` before acting on them. A `--no-intake` daemon on the same copy
/// also opens and claims nothing there, yet its maintenance worker finishes the migrations.
#[tokio::test(flavor = "current_thread")]
async fn copy_daemons_claim_nothing_query_only_refuses_writes_and_no_intake_migrates() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("completed");
            std::fs::create_dir(&root).unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            append_consume_config(&config, &root, 0, 16, 16);
            let daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let consumed = root.join("consumed.jsonl");
            std::fs::write(&consumed, b"{\"copy\":1}\n").unwrap();
            watch_until(client.capability(), |watch| {
                watch.get_completed_claims() == 1 && watch.get_pending_claims() == 0
            })
            .await;
            assert!(!consumed.exists(), "the normal daemon owns the root");
            drop(client);
            drop(daemon);

            let copy = dir.path().join("copy");
            std::fs::create_dir(&copy).unwrap();
            let copied = copy.join("store");
            let status = Command::new("cp")
                .args(["-a", "--reflink=auto", "--"])
                .arg(&data)
                .arg(&copied)
                .status()
                .unwrap();
            assert!(status.success());
            // The copy owes the schema 69 owner index migration, the source summary migration and a
            // source timeline rebuild, which only a maintenance worker runs.
            let db = sparrow_test_support::catalog_db(&copied).unwrap();
            sparrow_test_support::pending_schema_69_migrations(&db).unwrap();
            sparrow_test_support::pending_source_summary_migration(&db).unwrap();
            sparrow_test_support::stale_source_context_cache(&db).unwrap();
            drop(db);
            let copy_socket = copied.join("rpc.sock");
            let copy_config = write_server_config(&copy, &copied, &copy_socket);
            let query = start_with(
                &copy_config,
                &copy_socket,
                Stdio::inherit(),
                &["--query-only"],
            )
            .await;
            let waiting = root.join("waiting.jsonl");
            std::fs::write(&waiting, b"{\"copy\":2}\n").unwrap();
            let client = sparrow_client::Client::connect(&copy_socket).await.unwrap();
            let store = client.discovery().store_id.clone();

            let response = client
                .capability()
                .status_request()
                .send()
                .promise
                .await
                .unwrap();
            let status = rpc_ok(response.get().unwrap().get_result().unwrap());
            assert!(status.get_query_only());
            let watches = status.get_consume_watches().unwrap();
            assert_eq!(watches.len(), 1, "the stored watch is reported");
            assert_eq!(
                watches.get(0).get_id().unwrap().to_str().unwrap(),
                "completed"
            );
            assert_eq!(
                watches.get(0).get_directories(),
                0,
                "its root is never opened"
            );
            assert_eq!(
                cli(&copy_socket, &["status"])["query_only"].as_bool(),
                Some(true)
            );
            let rows =
                cli(&copy_socket, &["records", "--source", "consume-acceptance"])["rows"].clone();
            assert_eq!(
                rows.as_array().unwrap().len(),
                1,
                "reads answer from the copy"
            );
            let record = rows[0]["record_id"].as_str().unwrap().to_owned();

            let mut ingest = client.capability().ingest_request();
            let mut input = ingest.get().init_request();
            input.set_request_id("query-only-ingest");
            input.set_source("query-only");
            input.set_format(wire::Format::Jsonl);
            input.set_bytes(b"{\"refused\":true}\n");
            let response = ingest.send().promise.await.unwrap();
            let (code, message) = rpc_failure(response.get().unwrap().get_result().unwrap());
            assert_eq!(code, "READ_ONLY");
            assert!(message.contains("query-only mode"), "{message}");

            let response = client
                .capability()
                .watch_list_request()
                .send()
                .promise
                .await
                .unwrap();
            let list = rpc_ok(response.get().unwrap().get_result().unwrap());
            let listed = list.get_watches().unwrap().get(0);
            let mut put = client.capability().watch_put_request();
            let mut input = put.get().init_request();
            input.set_store_id(&store);
            input.set_request_id("query-only-watch");
            input.set_expected_revision(listed.get_revision());
            input
                .set_definition(listed.get_definition().unwrap())
                .unwrap();
            let response = put.send().promise.await.unwrap();
            assert_eq!(
                rpc_failure(response.get().unwrap().get_result().unwrap()).0,
                "READ_ONLY"
            );

            let mut pin = client.capability().pin_request();
            let mut input = pin.get().init_request();
            input.set_request_id("query-only-pin");
            input.set_name("refused");
            input.set_days(1);
            let mut target = input.init_targets(1).get(0);
            target.set_store_id(&store);
            target.set_record(&record);
            let response = pin.send().promise.await.unwrap();
            assert_eq!(
                rpc_failure(response.get().unwrap().get_result().unwrap()).0,
                "READ_ONLY"
            );

            // The normal daemon consumed a quiet_ms = 0 file within one poll; give the copy longer.
            tokio::time::sleep(Duration::from_secs(2)).await;
            assert_eq!(
                std::fs::read(&waiting).unwrap(),
                b"{\"copy\":2}\n",
                "the copy claimed nothing"
            );
            let status = cli(&copy_socket, &["status"]);
            // A query-only open fences and serves, but never runs a migration page, so the
            // store counters stay pending: counted through the seeding start, not totals.
            // Status answers from the seeding checkpoint, in constant time (#158).
            assert_eq!(status["counts"].as_str(), Some("pending"));
            assert_eq!(status["live_records"].as_str(), Some("0"));
            let migrations = status["migrations"].as_array().unwrap();
            assert_eq!(migrations.len(), 5, "{migrations:?}");
            for migration in migrations {
                assert_eq!(migration["done"].as_str(), Some("0"), "{migration:?}");
                assert_eq!(
                    migration["total"].as_str(),
                    Some("counting"),
                    "{migration:?}"
                );
                assert_eq!(migration["watermark"].as_str(), Some("start"));
                assert!(migration["failure"].is_null());
            }
            // A SPARQL answer over the unmigrated record graph counts the graph served the
            // facts the backfill still owes.
            let request = purrdf_lex::json::Object::new()
                .with("store_id", store.as_str())
                .with("query", "SELECT ?s WHERE { ?s ?p ?o } LIMIT 1")
                .with(
                    "layers",
                    purrdf_lex::json::Object::new()
                        .with("metadata", true)
                        .with("content", false)
                        .with("diagnostic", false),
                );
            let request_file = copy.join("pending.json");
            std::fs::write(
                &request_file,
                purrdf_lex::json::write_compact(&request.into()),
            )
            .unwrap();
            let answer = cli(&copy_socket, &["sparql", request_file.to_str().unwrap()]);
            assert_eq!(answer["execution_status"].as_str(), Some("complete"));
            let coverage = &answer["coverage"];
            assert_eq!(
                coverage["event_fallback_pending"].as_str(),
                Some("1"),
                "{coverage:?}"
            );
            // The view derives the fallback facts, so the answer is complete (#158).
            assert_eq!(coverage["complete"].as_bool(), Some(true), "{coverage:?}");
            drop(client);
            drop(query);

            // A no-intake daemon writes and runs maintenance, but opens no stored watch root.
            let _migrating = start_with(
                &copy_config,
                &copy_socket,
                Stdio::inherit(),
                &["--no-intake"],
            )
            .await;
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            let status = loop {
                let status = cli(&copy_socket, &["status"]);
                let migrations = status["migrations"].as_array().unwrap();
                assert!(
                    migrations.iter().all(|m| m["failure"].is_null()),
                    "{migrations:?}"
                );
                if migrations.iter().all(|m| m["watermark"].is_null()) {
                    break status;
                }
                assert!(std::time::Instant::now() < deadline, "{migrations:?}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            };
            assert_eq!(status["query_only"].as_bool(), Some(false));
            assert_eq!(status["counts"].as_str(), Some("exact"));
            assert_eq!(status["live_records"].as_str(), Some("1"));
            for migration in status["migrations"].as_array().unwrap() {
                assert_eq!(migration["done"], migration["total"], "{migration:?}");
            }
            let watches = status["consume_watches"].as_array().unwrap();
            assert_eq!(watches.len(), 1, "the stored watch is reported");
            assert_eq!(watches[0]["directories"].as_u64(), Some(0), "{watches:?}");
            let answer = cli(&copy_socket, &["sparql", request_file.to_str().unwrap()]);
            // The derivation is permanent (#193): the count does not fall once migrations end.
            assert_eq!(
                answer["coverage"]["event_fallback_pending"].as_str(),
                Some("1")
            );
            assert_eq!(
                std::fs::read(&waiting).unwrap(),
                b"{\"copy\":2}\n",
                "the no-intake copy claimed nothing"
            );
        })
        .await;
}

/// INVARIANT: query-only, no-intake and private-intake modes start no configured consume root,
/// public listener or archive (the first two no ingest socket either), so a configuration that
/// names one refuses startup with the conflicting key, whether a flag or the `query_only` key
/// selects the mode, before any store or socket is created.
#[test]
fn query_only_and_no_intake_refuse_conflicting_configuration() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("store");
    let socket = data.join("rpc.sock");
    let config = write_server_config(dir.path(), &data, &socket);
    let base = std::fs::read_to_string(&config).unwrap();
    // `flag` selects the mode on the command line; without one the `query_only` key does.
    let refused = |extra: &str, flags: &[&str], key: &str| {
        std::fs::write(&config, format!("{base}{extra}")).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_sparrowd-rpc-fixture"));
        command.arg("--config").arg(&config);
        command.args(flags);
        let output = command.admitted_output().unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(REFUSED), "{extra}: {stderr}");
        let mode = match flags.first() {
            Some(&"--no-intake") => "--no-intake",
            Some(&"--private-intake") => "--private-intake",
            _ => "query_only",
        };
        assert!(
            stderr.contains(&format!("{mode} refuses the configured `{key}`")),
            "{stderr}"
        );
        assert!(!data.exists(), "refused before opening the store");
    };
    let root = dir.path().join("completed");
    std::fs::create_dir(&root).unwrap();
    let consume = toml::to_string(&watch_definition(&root, "completed", 0, 16, 16)).unwrap();
    refused(
        &format!("\n[[consume]]\n{consume}"),
        &["--query-only"],
        "consume",
    );
    refused(
        &format!("query_only = true\n\n[[consume]]\n{consume}"),
        &[],
        "consume",
    );
    refused(
        &format!("\n[[consume]]\n{consume}"),
        &["--no-intake"],
        "consume",
    );
    let secret = dir.path().join("tcp.secret");
    std::fs::write(&secret, [7; 32]).unwrap();
    let tcp = format!(
        "rpc_tcp_address = \"127.0.0.1:9\"\nrpc_tcp_secret_file = \"{}\"\n",
        secret.display()
    );
    refused(&tcp, &["--query-only"], "rpc_tcp_address");
    refused(&tcp, &["--no-intake"], "rpc_tcp_address");
    let private = root.to_str().unwrap();
    refused(
        &format!("\n[[consume]]\n{consume}"),
        &["--private-intake", private],
        "consume",
    );
    refused(&tcp, &["--private-intake", private], "rpc_tcp_address");
}

/// INVARIANT: deploy/user-unit-upgrade.sh refuses a trailing --to without a value, a daemon
/// link that does not name --from, a drop-in for the unit other than the repository's
/// limits.conf (issue #148: the window installs only that file), and a --to daemon without
/// cap_net_bind_service before it changes anything: the links and the
/// drop-in directory are untouched, and an identical limits.conf passes. Every path is a scratch
/// override, and each refusal precedes the script's first systemctl call, so no unit is ever
/// addressed.
#[test]
fn user_unit_upgrade_refuses_before_changing_anything() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let (from, to) = ("sparrowd-schema68-aaaaaaa", "sparrowd-schema69-bbbbbbb");
    let prefix = dir.path().join("prefix");
    let libexec = prefix.join("libexec").join("sparrow");
    let bin = dir.path().join("bin");
    for path in [&libexec, &bin, &prefix.join("bin")] {
        std::fs::create_dir_all(path).unwrap();
    }
    // Never executed: the preconditions only test that the images exist and match ./bin.
    let image = b"#!/bin/sh\nexit 1\n";
    for name in [
        from,
        to,
        "sparrow-schema68-aaaaaaa",
        "sparrow-schema69-bbbbbbb",
    ] {
        for parent in [&libexec, &bin] {
            let path = parent.join(name);
            std::fs::write(&path, image).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }
    let link = libexec.join("sparrowd");
    let run = |args: &[&str]| {
        let output = Command::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../deploy/user-unit-upgrade.sh"
        ))
        .args(args)
        .env("HOME", dir.path())
        .env_remove("XDG_CONFIG_HOME")
        .env("SPARROW_SCOPE", "system")
        .env_remove("SPARROW_UNIT")
        .env("SPARROW_PREFIX", &prefix)
        .env(
            "SPARROW_CONFIG",
            dir.path().join("config").join("server.toml"),
        )
        .env("SPARROW_BIN", &bin)
        .admitted_output()
        .unwrap();
        (
            output.status.code(),
            String::from_utf8_lossy(&output.stdout).into_owned(),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        )
    };
    let unchanged = |expected: &str| {
        assert_eq!(std::fs::read_link(&link).unwrap(), Path::new(expected));
    };
    let switch = ["--from", from, "--to", to];

    symlink(from, &link).unwrap();
    let (code, stdout, stderr) = run(&["--from", from, "--to"]);
    assert_eq!(code, Some(2), "{stdout}{stderr}");
    assert!(
        stderr.contains("--to needs a daemon image name"),
        "{stderr}"
    );
    unchanged(from);
    std::fs::remove_file(&link).unwrap();
    symlink("sparrowd-schema67-ccccccc", &link).unwrap();
    let (code, stdout, stderr) = run(&switch);
    assert_eq!(code, Some(1), "{stdout}{stderr}");
    assert!(
        stderr.contains("unexpected daemon link")
            && stderr.contains("not sparrowd-schema68-aaaaaaa"),
        "{stderr}"
    );
    unchanged("sparrowd-schema67-ccccccc");

    std::fs::remove_file(&link).unwrap();
    symlink(from, &link).unwrap();
    let dropins = dir.path().join(".config/systemd/user/sparrow.service.d");
    std::fs::create_dir_all(&dropins).unwrap();
    let foreign = b"[Service]\nLimitNOFILE=4096\n";
    std::fs::write(dropins.join("limits.conf"), foreign).unwrap();
    let (code, stdout, stderr) = run(&switch);
    assert_eq!(code, Some(1), "{stdout}{stderr}");
    assert!(stderr.contains("foreign drop-in"), "{stderr}");
    assert_eq!(std::fs::read(dropins.join("limits.conf")).unwrap(), foreign);
    unchanged(from);

    std::fs::copy(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../deploy/systemd/user/sparrow.service.d/limits.conf"
        ),
        dropins.join("limits.conf"),
    )
    .unwrap();
    let (code, stdout, stderr) = run(&switch);
    assert_eq!(code, Some(1), "{stdout}{stderr}");
    assert!(stderr.contains("lacks cap_net_bind_service"), "{stderr}");
    unchanged(from);
}

/// INVARIANT (issue #148): a daemon whose soft open-file limit leaves no room for the table
/// cache beside the descriptor reserve refuses to open the store and names the unit setting,
/// instead of serving until a table read fails with EMFILE. 1024 is the user manager's default,
/// which the live user unit inherited before it crashed twice.
#[test]
fn daemon_refuses_a_soft_open_file_limit_without_room_for_the_table_cache() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("store");
    let socket = data.join("rpc.sock");
    let config = write_server_config(dir.path(), &data, &socket);
    let result = Command::new("prlimit")
        .arg("--nofile=1024:1024")
        .arg(env!("CARGO_BIN_EXE_sparrowd-rpc-fixture"))
        .arg("--config")
        .arg(&config)
        .admitted_output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&result.stderr);
    // Not named as deterministic, so the ordinary failure status: the unit restarts it.
    assert_eq!(result.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains("not classified as permanent"), "{stderr}");
    assert!(!socket.exists());
    assert!(
        stderr.contains("soft open-file limit is 1024") && stderr.contains("LimitNOFILE=65536"),
        "{stderr}"
    );
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
    let result = Command::new(env!("CARGO_BIN_EXE_sparrowd-rpc-fixture"))
        .arg("--config")
        .arg(&config)
        .admitted_output()
        .unwrap();
    assert_eq!(result.status.code(), Some(1));
    assert!(!socket.exists());
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(stderr.contains("invalid durable watch catalog"));
    assert!(stderr.contains("not classified as permanent"), "{stderr}");
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
            assert_eq!(status["store_format"].as_str(), Some("sparrow.store/354"));
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

/// INVARIANT (#438): a daemon started on a store that is already over its maintenance reserve
/// comes up instead of exiting. It answers status in a pressure phase, serves reads, refuses a
/// write with the retryable `STORAGE_PRESSURE`, and accepts writes again once space returns,
/// without a restart. Both starts that reach a startup write are covered: a first start that
/// seeds a consume watch into the store's catalog, and a restart with a stored catalog.
#[tokio::test(flavor = "current_thread")]
async fn daemon_starts_on_a_store_over_its_reserve_and_recovers_without_a_restart() {
    tokio::task::LocalSet::new()
        .run_until(async {
            for restart in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let root = dir.path().join("completed");
                std::fs::create_dir(&root).unwrap();
                let data = dir.path().join("store");
                let socket = data.join("rpc.sock");
                let config = write_server_config(dir.path(), &data, &socket);
                let text = std::fs::read_to_string(&config)
                    .unwrap()
                    .replace("disk_budget = 134217728", "disk_budget = 16777216")
                    .replace("maintenance_reserve = 8388608", "maintenance_reserve = 4194304");
                std::fs::write(&config, text).unwrap();
                append_consume_config(&config, &root, 30_000, 16, 16);
                let ballast = data.join("test-owned-pressure-ballast");
                let kept = if restart {
                    // A first start without pressure stores the catalog and one record.
                    let mut daemon = start(&config, &socket).await;
                    let producer = sparrow_client::Client::connect(&socket).await.unwrap();
                    let kept = producer
                        .ingest("kept", "source", wire::Format::Log, b"kept\n")
                        .await
                        .unwrap();
                    rustix::process::kill_process(
                        rustix::process::Pid::from_raw(daemon.0.id() as i32).unwrap(),
                        rustix::process::Signal::INT,
                    )
                    .unwrap();
                    let deadline = Instant::now() + Duration::from_secs(10);
                    while daemon.0.try_wait().unwrap().is_none() {
                        assert!(Instant::now() < deadline, "graceful stop stalled");
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    Some(kept)
                } else {
                    std::fs::create_dir_all(&data).unwrap();
                    None
                };
                std::fs::write(&ballast, vec![0_u8; 13 * 1024 * 1024]).unwrap();
                let _daemon = start(&config, &socket).await;
                let producer = sparrow_client::Client::connect(&socket).await.unwrap();
                // The daemon answers on its socket: status is served under pressure.
                assert!(cli(&socket, &["status"])["store_id"].as_str().is_some());
                if let Some(kept) = &kept {
                    assert_eq!(
                        producer.bytes(&kept.id, 0, 5).await.unwrap(),
                        b"kept\n",
                        "a read is served under pressure"
                    );
                }
                let refused = producer
                    .ingest("over", "source", wire::Format::Log, b"over\n")
                    .await
                    .unwrap_err();
                assert!(
                    matches!(refused, sparrow_client::Error::Domain { ref code, retryable: true, .. } if code == "STORAGE_PRESSURE"),
                    "{refused:?}"
                );
                // The refused write moves the pressure phase off `ready`.
                let deadline = Instant::now() + Duration::from_secs(10);
                while cli(&socket, &["status"])["pressure"]["phase"].as_str() == Some("ready") {
                    assert!(Instant::now() < deadline, "pressure phase stayed ready");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                std::fs::remove_file(&ballast).unwrap();
                let deadline = Instant::now() + Duration::from_secs(30);
                loop {
                    match producer
                        .ingest("over", "source", wire::Format::Log, b"over\n")
                        .await
                    {
                        Ok(_) => break,
                        Err(error) => assert!(
                            Instant::now() < deadline,
                            "writes never resumed after space returned: {error:?}"
                        ),
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        })
        .await;
}

/// INVARIANT (#438): a daemon started with less free space than `min_free_bytes` comes up
/// instead of exiting or hanging: status answers, a stored record reads back, and a write is
/// refused with the retryable `STORAGE_PRESSURE`.
#[tokio::test(flavor = "current_thread")]
async fn daemon_starts_below_the_free_space_floor_and_serves_reads() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let mut daemon = start(&config, &socket).await;
            let producer = sparrow_client::Client::connect(&socket).await.unwrap();
            let kept = producer
                .ingest("kept", "source", wire::Format::Log, b"kept\n")
                .await
                .unwrap();
            rustix::process::kill_process(
                rustix::process::Pid::from_raw(daemon.0.id() as i32).unwrap(),
                rustix::process::Signal::INT,
            )
            .unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            while daemon.0.try_wait().unwrap().is_none() {
                assert!(Instant::now() < deadline, "graceful stop stalled");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            // A floor no host can meet: every start below it is a start under pressure.
            let text = std::fs::read_to_string(&config)
                .unwrap()
                .replace("min_free_bytes = 1048576", "min_free_bytes = 1000000000000000");
            std::fs::write(&config, text).unwrap();
            let _daemon = start(&config, &socket).await;
            let producer = sparrow_client::Client::connect(&socket).await.unwrap();
            assert!(cli(&socket, &["status"])["store_id"].as_str().is_some());
            assert_eq!(producer.bytes(&kept.id, 0, 5).await.unwrap(), b"kept\n");
            let refused = producer
                .ingest("over", "source", wire::Format::Log, b"over\n")
                .await
                .unwrap_err();
            assert!(
                matches!(refused, sparrow_client::Error::Domain { ref code, retryable: true, .. } if code == "STORAGE_PRESSURE"),
                "{refused:?}"
            );
        })
        .await;
}

/// INVARIANT (#440): the wait to open covers the replay of write-ahead log an unclean stop
/// left, not only the maintenance reserve: with a log in `data_dir` larger than the free
/// space, the daemon waits without opening the store, and exits cleanly on an interrupt.
#[tokio::test(flavor = "current_thread")]
async fn startup_waits_for_the_space_to_replay_a_left_write_ahead_log() {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("store");
    let socket = data.join("rpc.sock");
    let config = write_server_config(dir.path(), &data, &socket);
    std::fs::create_dir_all(&data).unwrap();
    // A sparse log of more bytes than the filesystem has free: it costs no blocks, and the
    // reserve alone (8 MiB) is far below the free space, so only the replay can hold the open.
    let stat = rustix::fs::statvfs(&data).unwrap();
    let free = stat.f_bavail.saturating_mul(stat.f_frsize);
    let log = data.join("000001.log");
    std::fs::File::create(&log)
        .unwrap()
        .set_len(free + (1 << 30))
        .unwrap();
    let base = std::fs::read_to_string(&config).unwrap();
    std::fs::write(
        &config,
        base.replace(
            "min_free_bytes = 1048576",
            "min_free_bytes = 9223372036854775807",
        ),
    )
    .unwrap();
    let mut daemon = tokio::process::Command::new(env!("CARGO_BIN_EXE_sparrowd-rpc-fixture"))
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
    assert!(
        line.starts_with("sparrowd: waiting to open the store: only "),
        "{line}"
    );
    assert!(
        !socket.exists(),
        "the store opened over a log it could not replay"
    );
    rustix::process::kill_process(pid, rustix::process::Signal::INT).unwrap();
    let status = tokio::time::timeout(Duration::from_secs(30), daemon.wait())
        .await
        .unwrap()
        .unwrap();
    stderr.read_to_string(&mut line).await.unwrap();
    assert!(status.success(), "{status}: {line}");
    assert!(!line.contains("sparrowd: store="), "{line}");
}

/// Run the packaged shipper once over `config`; it must only read the producer files.
fn ship(config: &Path) -> std::process::Output {
    Command::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../deploy/libexec/sparrow-ship"
    ))
    .arg(config)
    .admitted_output()
    .unwrap()
}

/// INVARIANT: the shipper never queues a whole file the store would refuse before
/// upload, and its literal bound is the store's `MAX_IMPORT_BYTES`.
#[test]
fn shipper_reports_a_whole_file_above_the_import_bound_and_mirrors_it() {
    let script = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../deploy/libexec/sparrow-ship"
    ))
    .unwrap();
    assert!(
        script.contains(&format!(
            "\nMAX_IMPORT_BYTES = {}\n",
            sparrow_core::MAX_IMPORT_BYTES
        )),
        "the shipper's MAX_IMPORT_BYTES must equal sparrow_core::MAX_IMPORT_BYTES"
    );
    let dir = tempfile::tempdir().unwrap();
    let reports = dir.path().join("reports");
    std::fs::create_dir_all(&reports).unwrap();
    std::fs::create_dir_all(dir.path().join("intake/reports")).unwrap();
    // A sparse file costs no blocks, so the bound is cheap to cross.
    let original = reports.join("huge.json");
    std::fs::File::create(&original)
        .unwrap()
        .set_len(sparrow_core::MAX_IMPORT_BYTES + 1)
        .unwrap();
    let config = dir.path().join("ship.toml");
    std::fs::write(
        &config,
        format!(
            "intake_root = {:?}\nstate_dir = {:?}\nmax_chunk_bytes = 1048576\n\
             max_chunk_lines = 50000\nmax_files_per_run = 64\n\
             max_pending_bytes = 16777216\nmax_queue_bytes = 1073741824\n\n\
             [[source]]\nname = \"reports\"\ndir = {:?}\npattern = \"*.json\"\n\
             rule = \"settled\"\nformat = \"json\"\nsettle_seconds = 0\n",
            dir.path().join("intake"),
            dir.path().join("ship"),
            reports,
        ),
    )
    .unwrap();
    let refused = ship(&config);
    assert!(!refused.status.success());
    let errors = String::from_utf8_lossy(&refused.stderr);
    assert!(errors.contains("import bound"), "{errors}");
    let report = String::from_utf8_lossy(&refused.stdout);
    assert!(report.contains("reports: queued 0"), "{report}");
    assert_eq!(
        std::fs::read_dir(dir.path().join("ship/queue/reports"))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(
        std::fs::metadata(&original).unwrap().len(),
        sparrow_core::MAX_IMPORT_BYTES + 1
    );
}

/// INVARIANT: a changed device number alone never ships a file again. The ledger key is
/// inode, size and mtime; legacy four-field lines (a device number first, renumbered at
/// every btrfs boot) are read as the same entry, mixed with new ones, and a changed size
/// still ships the file again.
#[test]
fn shipper_never_reships_a_file_whose_device_number_alone_changed() {
    let dir = tempfile::tempdir().unwrap();
    let reports = dir.path().join("reports");
    std::fs::create_dir_all(&reports).unwrap();
    std::fs::create_dir_all(dir.path().join("intake/reports")).unwrap();
    let kept = reports.join("kept.json");
    let other = reports.join("other.json");
    std::fs::write(&kept, b"{\"a\":1}").unwrap();
    std::fs::write(&other, b"{\"b\":2}").unwrap();
    // A name with a carriage return must not split its ledger line.
    std::fs::write(reports.join("odd\rname.json"), b"{\"c\":3}").unwrap();
    let config = dir.path().join("ship.toml");
    std::fs::write(
        &config,
        format!(
            "intake_root = {:?}\nstate_dir = {:?}\nmax_chunk_bytes = 1048576\n\
             max_chunk_lines = 50000\nmax_files_per_run = 64\n\
             max_pending_bytes = 16777216\nmax_queue_bytes = 1073741824\n\n\
             [[source]]\nname = \"reports\"\ndir = {:?}\npattern = \"*.json\"\n\
             rule = \"settled\"\nformat = \"json\"\nsettle_seconds = 0\n",
            dir.path().join("intake"),
            dir.path().join("ship"),
            reports,
        ),
    )
    .unwrap();
    let report = |output: &std::process::Output| {
        assert!(output.status.success(), "{output:?}");
        String::from_utf8_lossy(&output.stdout).into_owned()
    };
    assert!(report(&ship(&config)).contains("reports: queued 3"));
    // Simulate a reboot's renumbering: every line gains a different leading device number,
    // one file's line stays in the new form, so the ledger mixes both.
    let ledger = dir.path().join("ship/reports.ledger");
    let lines = std::fs::read_to_string(&ledger).unwrap();
    let mixed: String = lines
        .lines()
        .map(|line| {
            if line.ends_with("kept.json") {
                format!("4242:{line}\n")
            } else {
                format!("{line}\n")
            }
        })
        .collect();
    assert!(mixed.contains("\n4242:") || mixed.starts_with("4242:"));
    std::fs::write(&ledger, mixed).unwrap();
    assert!(report(&ship(&config)).contains("reports: queued 0"));
    // A file that really changed is shipped again.
    std::fs::write(&kept, b"{\"a\":1,\"c\":3}").unwrap();
    assert!(report(&ship(&config)).contains("reports: queued 1"));
}

/// INVARIANT: the shipper refuses at configuration load, naming both paths, a source
/// directory that equals, contains or lies inside `intake_root` or `state_dir`, including
/// through a symlink, because it would otherwise ship its own queue and ledgers forever.
#[test]
fn shipper_refuses_a_source_overlapping_its_intake_or_state_tree() {
    let dir = tempfile::tempdir().unwrap();
    let (intake, state) = (dir.path().join("intake"), dir.path().join("ship"));
    for tree in [&intake, &state] {
        std::fs::create_dir_all(tree.join("inner")).unwrap();
    }
    std::os::unix::fs::symlink(&state, dir.path().join("alias")).unwrap();
    let config = dir.path().join("ship.toml");
    let refuses = |source: &Path, key: &str| {
        std::fs::write(
            &config,
            format!(
                "intake_root = {intake:?}\nstate_dir = {state:?}\nmax_chunk_bytes = 1048576\n\
                 max_chunk_lines = 50000\nmax_files_per_run = 64\n\
                 max_pending_bytes = 16777216\nmax_queue_bytes = 1073741824\n\n\
                 [[source]]\nname = \"s\"\ndir = {source:?}\npattern = \"*.json\"\n\
                 rule = \"settled\"\nformat = \"json\"\nsettle_seconds = 0\n",
            ),
        )
        .unwrap();
        let output = ship(&config);
        let errors = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "{source:?}");
        assert!(errors.contains(&format!("overlaps {key}")), "{errors}");
        assert!(errors.contains(&source.display().to_string()), "{errors}");
    };
    refuses(&intake, "intake_root");
    refuses(&state.join("inner"), "state_dir");
    refuses(dir.path(), "intake_root");
    refuses(&dir.path().join("alias"), "state_dir");
}

/// INVARIANT (#437): a spool file leaves the spool only once the store holds every part of
/// its present content. With the store away longer than any age rule (files six days old)
/// repeated shipper runs delete nothing and keep every ledger entry; neither spool unit runs
/// anything at start besides the spool itself, so a unit restart deletes nothing; a file whose
/// publication storage pressure refuses stays; an acknowledged file is set aside for a run
/// and deleted only if it still holds nothing the store lacks, so a line a spool script
/// appends after the acknowledgement is shipped, under a new name when the old one is in use.
#[tokio::test(flavor = "current_thread")]
async fn shipper_deletes_a_spool_file_only_once_the_store_holds_every_part() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let spool = dir.path().join("spool");
            let intake = dir.path().join("intake");
            std::fs::create_dir_all(&spool).unwrap();
            std::fs::create_dir_all(intake.join("spool")).unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            // One file ships whole; the other is cut into line-bounded parts.
            let whole = spool.join("katamari-stored-a.jsonl");
            let split = spool.join("katamari-stored-b.jsonl");
            let whole_bytes = b"{\"a\":1}\n".to_vec();
            let split_bytes: Vec<u8> = (0..6)
                .flat_map(|n| format!("{{\"n\":{n},\"pad\":\"xxxxxxxxxxxxxxxx\"}}\n").into_bytes())
                .collect();
            let six_days_ago = std::time::SystemTime::now() - Duration::from_secs(6 * 86_400);
            for (path, bytes) in [(&whole, &whole_bytes), (&split, &split_bytes)] {
                std::fs::write(path, bytes).unwrap();
                std::fs::File::options()
                    .write(true)
                    .open(path)
                    .unwrap()
                    .set_modified(six_days_ago)
                    .unwrap();
            }
            // An interrupted run left a file set aside whose lines the store never held.
            let leftover = spool.join(".reaping-katamari-stored-c.jsonl");
            let leftover_bytes = b"{\"c\":3}\n".to_vec();
            std::fs::write(&leftover, &leftover_bytes).unwrap();
            let ship_config = dir.path().join("ship.toml");
            std::fs::write(
                &ship_config,
                format!(
                    "intake_root = {:?}\nstate_dir = {:?}\nmax_chunk_bytes = 64\n\
                     max_chunk_lines = 50000\nmax_files_per_run = 64\n\
                     max_pending_bytes = 16777216\nmax_queue_bytes = 1073741824\n\
                     sparrow = {:?}\nsocket = {:?}\n\n\
                     [[source]]\nname = \"spool\"\ndir = {:?}\npattern = \"katamari-*.jsonl\"\n\
                     rule = \"settled\"\nformat = \"jsonl\"\nsettle_seconds = 0\n\
                     delete_acknowledged = true\n",
                    intake,
                    dir.path().join("ship"),
                    env!("CARGO_BIN_EXE_sparrow-rpc-fixture"),
                    socket,
                    spool,
                ),
            )
            .unwrap();
            let report = |output: std::process::Output| {
                assert!(output.status.success(), "{output:?}");
                String::from_utf8_lossy(&output.stdout).into_owned()
            };
            let ledger = dir.path().join("ship/spool.ledger");
            let ledger_lines = || std::fs::read_to_string(&ledger).unwrap().lines().count();
            // The store is away: every run reports it and deletes or moves nothing.
            for _ in 0..2 {
                let shipped = report(ship(&ship_config));
                assert!(shipped.contains("store unreachable"), "{shipped}");
                assert_eq!(std::fs::read(&whole).unwrap(), whole_bytes);
                assert_eq!(std::fs::read(&split).unwrap(), split_bytes);
                assert_eq!(std::fs::read(&leftover).unwrap(), leftover_bytes);
                assert_eq!(ledger_lines(), 2);
            }
            let fed = std::fs::read_dir(intake.join("spool")).unwrap().count() as u64;
            assert!(fed > 2, "the split file ships as several parts: {fed}");
            // A unit restart runs only the spool: no start-time command can delete a file.
            for unit in ["katamari-journal-spool", "katamari-manager-spool"] {
                let text = std::fs::read_to_string(format!(
                    "{}/../../../deploy/systemd/user/{unit}.service",
                    env!("CARGO_MANIFEST_DIR")
                ))
                .unwrap();
                let commands: Vec<&str> =
                    text.lines().filter(|line| line.starts_with("Exec")).collect();
                assert_eq!(commands.len(), 1, "{unit}: {commands:?}");
                assert!(
                    commands[0]
                        .starts_with(&format!("ExecStart=%h/.local/libexec/sparrow/{unit} ")),
                    "{unit}: {commands:?}"
                );
            }
            // The store comes back and the watch publishes every waiting part.
            let config = write_server_config(dir.path(), &data, &socket);
            let text = std::fs::read_to_string(&config)
                .unwrap()
                .replace("disk_budget = 134217728", "disk_budget = 16777216")
                .replace("maintenance_reserve = 8388608", "maintenance_reserve = 4194304");
            std::fs::write(&config, text).unwrap();
            append_consume_config(&config, &intake.join("spool"), 0, 16, 16);
            let _daemon = start(&config, &socket).await;
            let producer = sparrow_client::Client::connect(&socket).await.unwrap();
            watch_until(producer.capability(), |watch| {
                watch.get_completed_claims() == fed && watch.get_pending_claims() == 0
            })
            .await;
            // Run 1: the leftover holds lines the store lacks and returns under its name; the
            // two acknowledged files are set aside, not deleted.
            let shipped = report(ship(&ship_config));
            assert!(
                shipped.contains("deleted 0 acknowledged, set aside 2, not yet acknowledged 1"),
                "{shipped}"
            );
            let restored = spool.join("katamari-stored-c.jsonl");
            assert_eq!(std::fs::read(&restored).unwrap(), leftover_bytes);
            assert!(!leftover.exists() && !whole.exists() && !split.exists());
            let whole_aside = spool.join(".reaping-katamari-stored-a.jsonl");
            let split_aside = spool.join(".reaping-katamari-stored-b.jsonl");
            assert_eq!(std::fs::read(&split_aside).unwrap(), split_bytes);
            // A spool script's late write: one line lands in the set-aside file, and the
            // script reopens the old name for the next one.
            {
                use std::io::Write;
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(&whole_aside)
                    .unwrap()
                    .write_all(b"{\"late\":1}\n")
                    .unwrap();
            }
            std::fs::write(&whole, b"{\"again\":2}\n").unwrap();
            // Run 2: the untouched set-aside file is deleted; the one with a late line returns
            // under a new name because its own name is in use again, and ships.
            let shipped = report(ship(&ship_config));
            assert!(shipped.contains("deleted 1 acknowledged"), "{shipped}");
            assert!(!split_aside.exists(), "{shipped}");
            // The name `.reaping-...-a` may be taken again by the new file, set aside once the
            // store holds it; the old file with its late line is gone from it.
            assert!(
                !whole_aside.exists() || std::fs::read(&whole_aside).unwrap() == b"{\"again\":2}\n"
            );
            let late: Vec<_> = std::fs::read_dir(&spool)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                .filter(|name| name.starts_with("katamari-stored-a-late-"))
                .collect();
            assert_eq!(late.len(), 1, "{late:?}");
            assert_eq!(
                std::fs::read(spool.join(&late[0])).unwrap(),
                b"{\"a\":1}\n{\"late\":1}\n"
            );
            // Storage pressure refuses publication: the store holds no part of the new file,
            // so it is neither deleted nor set aside while pressure lasts.
            let ballast = data.join("test-owned-pressure-ballast");
            std::fs::write(&ballast, vec![0_u8; 13 * 1024 * 1024]).unwrap();
            let refused = producer
                .ingest("pressure-probe", "probe", wire::Format::Log, b"probe\n")
                .await
                .unwrap_err();
            assert!(
                matches!(refused, sparrow_client::Error::Domain { ref code, .. } if code == "STORAGE_PRESSURE"),
                "{refused:?}"
            );
            let pressed = spool.join("katamari-stored-d.jsonl");
            std::fs::write(&pressed, b"{\"d\":4}\n").unwrap();
            let shipped = report(ship(&ship_config));
            assert!(pressed.exists(), "{shipped}");
            let shipped = report(ship(&ship_config));
            assert!(pressed.exists(), "{shipped}");
        })
        .await;
}

/// INVARIANT: an artifact above the removed 16 MiB per-artifact limit ingests whole and
/// reads back byte-exact. The shipper ships one single-object JSON file of 20 MiB whole,
/// the consume watch publishes it, and the SDK stages an in-memory copy in bounded
/// appends while one inline message stays refused at the store's batch bound.
#[tokio::test(flavor = "current_thread")]
async fn oversized_single_json_ships_whole_ingests_and_reads_back_exactly() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let reports = dir.path().join("reports");
            let intake = dir.path().join("intake");
            std::fs::create_dir_all(&reports).unwrap();
            std::fs::create_dir_all(intake.join("reports")).unwrap();
            let mut json = br#"{"refusal":""#.to_vec();
            json.resize(20 * 1024 * 1024, b'r');
            json.extend_from_slice(br#""}"#);
            let original = reports.join("refusal-evidence.json");
            std::fs::write(&original, &json).unwrap();
            let ship_config = dir.path().join("ship.toml");
            let settings = format!(
                "intake_root = {:?}\nstate_dir = {:?}\nmax_chunk_bytes = 1048576\n\
                 max_chunk_lines = 50000\nmax_files_per_run = 64\n\
                 max_pending_bytes = 16777216\nmax_queue_bytes = 1073741824\n\n\
                 [[source]]\nname = \"reports\"\ndir = {:?}\npattern = \"*.json\"\n\
                 rule = \"settled\"\nformat = \"json\"\nsettle_seconds = 0\n",
                intake,
                dir.path().join("ship"),
                reports,
            );
            // The removed knob refuses with its removal rather than shipping anything.
            std::fs::write(
                &ship_config,
                format!("max_whole_bytes = 16777216\n{settings}"),
            )
            .unwrap();
            let refused = ship(&ship_config);
            assert!(!refused.status.success());
            assert!(
                String::from_utf8_lossy(&refused.stderr).contains("`max_whole_bytes` was removed")
            );
            std::fs::write(&ship_config, &settings).unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let mut watch = watch_definition(&intake.join("reports"), "completed", 0, 16, 16);
            watch.format = "json".into();
            append_watch_config(&config, &watch);
            let _daemon = start(&config, &socket).await;
            let shipped = ship(&ship_config);
            assert!(
                shipped.status.success(),
                "{}",
                String::from_utf8_lossy(&shipped.stderr)
            );
            let report = String::from_utf8_lossy(&shipped.stdout);
            assert!(report.contains("reports: queued 1"), "{report}");
            assert!(report.contains("reports: fed 1"), "{report}");
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            watch_until(client.capability(), |watch| {
                watch.get_completed_claims() == 1 && watch.get_pending_claims() == 0
            })
            .await;
            assert_eq!(std::fs::read(&original).unwrap(), json);
            let store = client.discovery().store_id.clone();
            let listed = cli(
                &socket,
                &["artifacts", &store, "--source", "consume-acceptance"],
            );
            let row = &listed["rows"][0];
            assert_eq!(listed["rows"].as_array().unwrap().len(), 1);
            assert_eq!(row["complete_bytes_available"].as_bool(), Some(true));
            assert_eq!(
                row["origin"]["byte_length"].as_str(),
                Some(json.len().to_string().as_str())
            );
            let consumed = row["artifact_id"].as_str().unwrap().to_owned();
            let window = 1024 * 1024;
            let mut read = Vec::with_capacity(json.len());
            while read.len() < json.len() {
                let length = window.min(json.len() - read.len());
                read.extend(
                    client
                        .bytes(&consumed, read.len() as u64, length as u32)
                        .await
                        .unwrap(),
                );
            }
            assert!(read == json, "consumed artifact bytes differ");
            // One inline message stays bounded; the SDK stages the same input instead.
            let mut request = client.capability().ingest_request();
            let mut input = request.get().init_request();
            input.set_request_id("inline-oversized");
            input.set_source("sdk-oversized");
            input.set_format(wire::Format::Json);
            input.set_bytes(&json);
            let response = request.send().promise.await.unwrap();
            match response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            {
                wire::result::Error(error) => {
                    // #227: no retry shortens the request, so it is not retryable.
                    let error = error.unwrap();
                    assert_eq!(
                        error.get_code().unwrap().to_str().unwrap(),
                        "INVALID_ARGUMENT"
                    );
                    assert!(!error.get_retryable());
                }
                wire::result::Ok(_) => panic!("an inline message above 16 MiB was accepted"),
            }
            let staged = client
                .ingest("sdk-oversized", "sdk-oversized", wire::Format::Json, &json)
                .await
                .unwrap();
            assert_eq!(staged.byte_length, json.len() as u64);
            assert_eq!(staged.durability, wire::Durability::Durable);
            let tail = json.len() - window;
            assert_eq!(
                client
                    .bytes(&staged.id, tail as u64, window as u32)
                    .await
                    .unwrap(),
                json[tail..]
            );
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
            // The replacement written below at the confirmed claim's name is a new candidate.
            // At the 600-second maximum quiet interval it cannot become claimable within this
            // test's 30-second waits, so the assertions see it unclaimed however slowly the
            // daemon starts, without depending on the order of startup and a short interval.
            append_consume_config(&config, &root, 600_000, 16, 4);
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
                    && watch.get_candidates() == 1
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

/// Lost watch coverage (the directory limit, a replaced root) parks that watch with a visible
/// failure and never retires its source evidence, and the daemon stays up and answers
/// (#445: it used to exit, and only a restart cleared it).
#[tokio::test(flavor = "current_thread")]
async fn consume_coverage_limit_and_root_replacement_park_the_watch_without_source_retirement() {
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
            Command::new(env!("CARGO_BIN_EXE_sparrowd-rpc-fixture"))
                .arg("--config")
                .arg(&config)
                .stdout(Stdio::null())
                .admitted_spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        while !socket.exists() {
            assert!(daemon.0.try_wait().unwrap().is_none());
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let preserved = if replace_root {
            let old = dir.path().join("old-root");
            std::fs::rename(&root, &old).unwrap();
            std::fs::create_dir(&root).unwrap();
            std::fs::write(root.join("replacement.jsonl"), b"replacement evidence").unwrap();
            old
        } else {
            root.clone()
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            assert!(
                daemon.0.try_wait().unwrap().is_none(),
                "lost watch coverage ended the daemon"
            );
            let status = cli(&socket, &["status"]);
            if status["consume_watches"][0]["failure"].as_str().is_some() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "lost watch coverage was not a visible failure"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(daemon.0.try_wait().unwrap().is_none());
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
            let receipt = root
                .publish(&client, &mut claim, &Default::default())
                .await
                .unwrap();
            assert_eq!(
                store
                    .bytes(&receipt, 0, input.len(), sparrow_server::now().unwrap())
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
            assert_eq!(
                root.publish(&client, &mut claim, &Default::default())
                    .await
                    .unwrap(),
                receipt
            );
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
            let accepted = root
                .publish(&client, &mut claim, &Default::default())
                .await
                .unwrap();
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
                    assert_eq!(saved.published_artifact, Some(accepted));
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
            root.publish(&client, &mut claim, &Default::default())
                .await
                .unwrap();
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
                    assert_eq!(result["receipt"]["scope"], cli_result["receipt"]["scope"]);
                    assert_eq!(
                        result["receipt"]["scope"]["selection"].as_str(),
                        Some("evidence")
                    );
                    assert_eq!(
                        result["receipt"]["scope"]["direct_members"].as_str(),
                        Some("2")
                    );
                    assert_eq!(
                        result["receipt"]["scope"]["expanded_records"].as_str(),
                        Some("1")
                    );
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
        sparrow_test_support::crash::kill_and_reap(&mut self.0)
            .expect("stop and reap this test's daemon");
    }
}

/// Both spool modes preserve acknowledged offline input, cursor and span identity across
/// SIGKILL. A new producer drains the exact prefix to a real daemon and reclaims its disk.
#[tokio::test(flavor = "current_thread")]
async fn configured_spools_recover_after_producer_sigkill_and_drain_once() {
    use sparrow_client::{
        Admission, Delivery, DeliveryConfig, DeliveryMode, Endpoint, SpoolConfig,
    };
    const CHILD: &str = "SPARROW_TEST_OFFLINE_SPOOL";
    if let Some(path) = std::env::var_os(CHILD) {
        let path = std::path::PathBuf::from(path);
        let name = std::env::var("SPARROW_TEST_SPOOL_MODE").unwrap();
        let spool = SpoolConfig::new(path.join(&name));
        let mut config = DeliveryConfig::lossless(
            Endpoint::Unix(path.join("unavailable.sock")),
            &name,
            spool.clone(),
        );
        if name == "lossy" {
            config.mode = DeliveryMode::LossySpooled(spool);
        }
        let delivery = Delivery::start_async(config).await.unwrap();
        assert_eq!(delivery.next_span_id().unwrap(), 1);
        for index in 1..=12u64 {
            let event = delivery_event("crash recovery");
            if index % 2 == 0 {
                assert_eq!(
                    delivery
                        .send_with_token(event, &index.to_le_bytes())
                        .await
                        .unwrap()
                        .sequence,
                    index
                );
            } else if name == "lossy" && index == 1 {
                assert_eq!(
                    delivery
                        .offer_spooled(event)
                        .await
                        .unwrap()
                        .unwrap()
                        .sequence,
                    index
                );
            } else {
                assert_eq!(
                    delivery.try_send(event),
                    Admission::Queued { sequence: index }
                );
            }
        }
        delivery.flush_local(Duration::from_secs(5)).await.unwrap();
        sparrow_test_support::crash::checkpoint("spooled");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("store");
    let socket = store.join("rpc.sock");
    let cfg = write_server_config(dir.path(), &store, &socket);
    let _daemon = start(&cfg, &socket).await;
    for mode in ["lossless", "lossy"] {
        let marker = dir.path().join(format!("{mode}-ready"));
        let mut child = Command::new(std::env::current_exe().unwrap());
        child
            .args([
                "--exact",
                "configured_spools_recover_after_producer_sigkill_and_drain_once",
                "--nocapture",
            ])
            .env(CHILD, dir.path())
            .env("SPARROW_TEST_SPOOL_MODE", mode)
            .env("SPARROW_TEST_CRASH_POINT", "spooled")
            .env("SPARROW_TEST_CRASH_READY", &marker);
        sparrow_test_support::crash::kill_at_checkpoint(&mut child, &marker, "spooled").unwrap();
        let settings = SpoolConfig::new(dir.path().join(mode));
        let mut config =
            DeliveryConfig::lossless(Endpoint::Unix(socket.clone()), mode, settings.clone());
        if mode == "lossy" {
            config.mode = DeliveryMode::LossySpooled(settings);
        }
        let delivery = Delivery::start_async(config).await.unwrap();
        assert!(delivery.next_span_id().unwrap() > u64::from(u32::MAX));
        assert_eq!(delivery.accepted_token().unwrap(), 12u64.to_le_bytes());
        let fence = delivery
            .shutdown_async(Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(fence.durable_events, 12);
        assert_eq!(delivery.confirmed_token().unwrap(), 12u64.to_le_bytes());
        let disk = delivery.stats().unwrap().spool.unwrap();
        assert_eq!((disk.bytes, disk.segments), (0, 0));
    }
    assert_eq!(
        cli(&socket, &["status"])["live_records"].as_str(),
        Some("24")
    );
}

/// A full offline journal drops only frames exceeding its remaining physical capacity.
/// Reopening drains that prefix to the daemon and reclaimed space admits the next sequence.
#[tokio::test(flavor = "current_thread")]
async fn full_lossy_spool_recovers_drains_and_accepts_after_cleanup() {
    use sparrow_client::{Delivery, DeliveryConfig, DropReason, Endpoint, SpoolConfig};
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("store");
    let socket = store.join("rpc.sock");
    let daemon_config = write_server_config(dir.path(), &store, &socket);
    let mut spool = SpoolConfig::new(dir.path().join("spool"));
    spool.max_bytes = 1 << 20;
    spool.segment_bytes = 64 << 10;
    let config = DeliveryConfig::lossy_spooled(
        Endpoint::Unix(socket.clone()),
        "full-offline",
        spool.clone(),
    );
    let producer = Delivery::start_async(config.clone()).await.unwrap();
    let large = delivery_event(&"x".repeat(7900));
    let mut accepted = 0;
    while let Some(receipt) = producer.offer_spooled(large.clone()).await.unwrap() {
        accepted += 1;
        assert_eq!(receipt.sequence, accepted);
        assert!(accepted < 200);
    }
    let stats = producer.stats().unwrap();
    let used = stats.spool.unwrap().bytes;
    assert!(accepted > 100);
    assert!(used <= spool.max_bytes && spool.max_bytes - used < used / accepted);
    assert!(producer.offer_spooled(large).await.unwrap().is_none());
    assert_eq!(
        producer.stats().unwrap().dropped_by_reason[DropReason::SpoolFull as usize],
        2
    );
    assert_eq!(
        producer.close_local(Duration::from_secs(5)).await.unwrap(),
        accepted
    );
    let _daemon = start(&daemon_config, &socket).await;
    let producer = Delivery::start_async(config).await.unwrap();
    let timeout = Duration::from_secs(20);
    let fence = producer.flush_async(timeout).await.unwrap();
    assert_eq!(fence.durable_sequence, accepted);
    assert_eq!(producer.stats().unwrap().spool.unwrap().bytes, 0);
    let next = producer
        .offer_spooled(delivery_event("after recovery"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.sequence, accepted + 1);
    assert_eq!(
        producer
            .shutdown_async(timeout)
            .await
            .unwrap()
            .durable_sequence,
        next.sequence
    );
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

fn admit(delivery: &sparrow_client::Delivery, event: sparrow_client::ProducerEvent) -> u64 {
    match delivery.try_send(event) {
        sparrow_client::Admission::Queued { sequence } => sequence,
        admission => panic!("unexpected admission: {admission:?}"),
    }
}

async fn delivery_barrier(
    delivery: &sparrow_client::Delivery,
    shutdown: bool,
    timeout: Duration,
) -> Result<sparrow_client::DeliveryWatermark, sparrow_client::FlushError> {
    if shutdown {
        delivery.shutdown_async(timeout).await
    } else {
        delivery.flush_async(timeout).await
    }
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
    /// A simulated newer daemon, advertising this producer profile and event-count ceiling.
    profile: std::cell::Cell<Option<(bool, u32)>>,
    /// Artifact record and inline-byte ceilings independent of the dedicated event limits.
    artifact_limits: std::cell::Cell<Option<(u64, u64)>>,
    refuse_discovery: std::cell::Cell<bool>,
    calls: std::cell::RefCell<Vec<sparrow_client::EventBatch>>,
    accepted: std::cell::RefCell<Option<sparrow_client::ArtifactReceipt>>,
    drop_first: std::cell::Cell<bool>,
    corrupt_receipt: std::cell::Cell<bool>,
    corrupt_publication: std::cell::Cell<bool>,
    withhold_publication: std::cell::Cell<bool>,
    /// Answer `importPublish` with this daemon-style refusal `(code, retryable)` instead of forwarding it.
    refuse_publish: std::cell::Cell<Option<(&'static str, bool)>>,
    /// Answer `importCancel` with this daemon refusal `(code, retryable)` instead of forwarding it.
    refuse_cancel: std::cell::Cell<Option<(&'static str, bool)>>,
    /// Daemon-style refusals `(code, retryable)` answered, in order, to the next event batches
    /// instead of forwarding them.
    refusals: std::cell::RefCell<std::collections::VecDeque<(&'static str, bool)>>,
    refusal_after_accept: std::cell::Cell<Option<(&'static str, bool)>>,
    /// Replace the backend with this daemon when `importPublish` is next refused.
    swap_backend_after_refused_publish: std::cell::RefCell<Option<wire::sparrow::Client>>,
    /// Connections accepted by the relay.
    connections: std::cell::Cell<u32>,
    mutate_file: std::cell::RefCell<Option<std::path::PathBuf>>,
    mutate_published_file: std::cell::RefCell<Option<std::path::PathBuf>>,
    first_accepted: tokio::sync::Notify,
    obstruct_checkpoint: std::cell::RefCell<Option<std::path::PathBuf>>,
    discovered: tokio::sync::Notify,
}
struct EventRelay(std::rc::Rc<EventRelayState>);
impl EventRelayState {
    fn refuse_event(
        &self,
        batch: wire::event_batch::Reader<'_>,
        out: wire::result::Builder<'_, wire::artifact::Owned>,
    ) -> capnp::Result<bool> {
        let input = sparrow_client::EventBatch::decode(batch)
            .map_err(|error| capnp::Error::failed(error.to_string()))?;
        assert!(
            self.calls.borrow().len() < 8,
            "retry fixture exceeded its request bound"
        );
        self.calls.borrow_mut().push(input);
        if let Some((code, retryable)) = self.refusals.borrow_mut().pop_front() {
            let mut failure = out.init_error();
            failure.set_code(code);
            failure.set_message("injected refusal");
            failure.set_retryable(retryable);
            return Ok(true);
        }
        Ok(false)
    }

    async fn accepted_event(
        &self,
        result: wire::result::Reader<'_, wire::artifact::Owned>,
    ) -> capnp::Result<()> {
        if let wire::result::Ok(value) = result.which()? {
            let receipt = sparrow_client::ArtifactReceipt::decode(value?)
                .map_err(|error| capnp::Error::failed(error.to_string()))?;
            if let Some(refusal) = self.refusal_after_accept.take() {
                self.refusals.borrow_mut().push_back(refusal);
                *self.accepted.borrow_mut() = Some(receipt.clone());
            }
            if self.drop_first.replace(false) {
                *self.accepted.borrow_mut() = Some(receipt);
                self.first_accepted.notify_one();
                // The real daemon committed. Only the reply is lost.
                std::future::pending::<()>().await;
            }
            if let Some(path) = self.obstruct_checkpoint.borrow_mut().take() {
                std::fs::create_dir(path)?;
                self.first_accepted.notify_one();
            }
        }
        Ok(())
    }

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
        if self.0.refuse_discovery.get() {
            let mut refusal = results.get().init_result().init_error();
            refusal.set_code("INVALID_ARGUMENT");
            refusal.set_message("discovery refused before submission");
            self.0.discovered.notify_one();
            return Ok(());
        }
        let backend = self.0.backend.borrow().clone();
        let response = backend.discover_request().send().promise.await?;
        results.get().set_result(response.get()?.get_result()?)?;
        if let Some((supports, ceiling)) = self.0.profile.get()
            && let wire::result::Ok(value) = results.get().get_result()?.which()?
        {
            let mut discovery = value?;
            discovery.set_schema_version(sparrow_client::SCHEMA_VERSION + 1);
            discovery.set_max_event_batch_events(ceiling);
            if let Some((records, bytes)) = self.0.artifact_limits.get() {
                discovery.set_max_records_per_artifact(records);
                discovery.set_max_inline_artifact_bytes(bytes);
            }
            if !supports {
                discovery.init_producer_profiles(0);
            }
        }
        self.0.discovered.notify_one();
        Ok(())
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
        let input = params.get()?.get_batch()?;
        if self.0.refuse_event(input, results.get().init_result())? {
            return Ok(());
        }
        let backend = self.0.backend.borrow().clone();
        let mut request = backend.events_ingest_request();
        request.get().set_batch(input)?;
        let response = request.send().promise.await?;
        self.0.accepted_event(response.get()?.get_result()?).await?;
        results.get().set_result(response.get()?.get_result()?)?;
        if let wire::result::Ok(value) = results.get().get_result()?.which()? {
            self.0.corrupt(value?, self.0.corrupt_receipt.get());
        }
        Ok(())
    }

    async fn sealed_events_ingest(
        self: std::rc::Rc<Self>,
        params: wire::sparrow::SealedEventsIngestParams,
        mut results: wire::sparrow::SealedEventsIngestResults,
    ) -> capnp::Result<()> {
        let input = params.get()?;
        if self
            .0
            .refuse_event(input.get_batch()?, results.get().init_result())?
        {
            return Ok(());
        }
        let mut request = self.0.backend.borrow().sealed_events_ingest_request();
        request.get().set_batch(input.get_batch()?)?;
        request.get().set_store_id(input.get_store_id()?);
        request.get().set_deadline_ms(input.get_deadline_ms());
        let response = request.send().promise.await?;
        self.0.accepted_event(response.get()?.get_result()?).await?;
        results.get().set_result(response.get()?.get_result()?)?;
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
    async fn import_cancel(
        self: std::rc::Rc<Self>,
        params: wire::sparrow::ImportCancelParams,
        mut results: wire::sparrow::ImportCancelResults,
    ) -> capnp::Result<()> {
        if let Some((code, retryable)) = self.0.refuse_cancel.get() {
            let mut failure = results.get().init_result().init_error();
            failure.set_code(code);
            failure.set_message("injected refusal");
            failure.set_retryable(retryable);
            if code == "STORAGE_PRESSURE" {
                failure.set_class(wire::FailureClass::FreeSpaceFloor);
            }
            return Ok(());
        }
        let mut request = self.0.backend.borrow().import_cancel_request();
        request.get().set_import(params.get()?.get_import()?)?;
        let response = request.send().promise.await?;
        results.get().set_result(response.get()?.get_result()?)
    }
    async fn import_publish(
        self: std::rc::Rc<Self>,
        params: wire::sparrow::ImportPublishParams,
        mut results: wire::sparrow::ImportPublishResults,
    ) -> capnp::Result<()> {
        if let Some((code, retryable)) = self.0.refuse_publish.get() {
            let mut failure = results.get().init_result().init_error();
            failure.set_code(code);
            failure.set_message("injected refusal");
            failure.set_retryable(retryable);
            if let Some(next) = self.0.swap_backend_after_refused_publish.take() {
                *self.0.backend.borrow_mut() = next;
            }
            return Ok(());
        }
        let mut request = self.0.backend.borrow().import_publish_request();
        request.get().set_import(params.get()?.get_import()?)?;
        let response = request.send().promise.await?;
        if self.0.withhold_publication.replace(false)
            && let wire::result::Ok(value) = response.get()?.get_result()?.which()?
        {
            *self.0.accepted.borrow_mut() = Some(
                sparrow_client::ArtifactReceipt::decode(value?)
                    .map_err(|e| capnp::Error::failed(e.to_string()))?,
            );
            self.0.first_accepted.notify_one();
            return std::future::pending().await;
        }
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

/// Forward each connection to the daemon and cut it after `limit` request
/// bytes, as the daemon's connection lease cuts a long upload.
fn lease_cutting_relay(
    socket: &Path,
    daemon: std::path::PathBuf,
    limit: u64,
    connections: std::rc::Rc<std::cell::Cell<u32>>,
) -> tokio::task::JoinHandle<()> {
    let listener = tokio::net::UnixListener::bind(socket).unwrap();
    tokio::task::spawn_local(async move {
        loop {
            let (client, _) = listener.accept().await.unwrap();
            connections.set(connections.get() + 1);
            let daemon = tokio::net::UnixStream::connect(&daemon).await.unwrap();
            tokio::task::spawn_local(async move {
                let (client_read, mut client_write) = client.into_split();
                let (mut daemon_read, mut daemon_write) = daemon.into_split();
                let mut requests = tokio::io::AsyncReadExt::take(client_read, limit);
                tokio::select! {
                    _ = tokio::io::copy(&mut requests, &mut daemon_write) => {}
                    _ = tokio::io::copy(&mut daemon_read, &mut client_write) => {}
                }
            });
        }
    })
}

#[tokio::test(flavor = "current_thread")]
async fn cli_ingest_resumes_across_lost_connections_and_cancels_a_stalled_import() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let store_path = dir.path().join("store");
            let socket = store_path.join("rpc.sock");
            let config = write_server_config(dir.path(), &store_path, &socket);
            let _daemon = start(&config, &socket).await;
            let input = dir.path().join("lease.log");
            let mut line = vec![b'x'; 1023];
            line.push(b'\n');
            std::fs::write(
                &input,
                line.repeat(8 * sparrow_core::IMPORT_CHUNK_BYTES / line.len()),
            )
            .unwrap();
            let path = input.to_str().unwrap().to_owned();
            // About two append chunks fit in each connection before it is cut.
            let resumed = dir.path().join("resumed.sock");
            let connections = std::rc::Rc::new(std::cell::Cell::new(0));
            let relay = lease_cutting_relay(
                &resumed,
                socket.clone(),
                3 * sparrow_core::IMPORT_CHUNK_BYTES as u64,
                connections.clone(),
            );
            let args = [
                "ingest".into(),
                "lease-cut".into(),
                "lease".into(),
                "log".into(),
                path,
            ];
            let receipt = tokio::task::spawn_blocking(move || {
                cli(
                    &resumed,
                    &args.iter().map(String::as_str).collect::<Vec<_>>(),
                )
            })
            .await
            .unwrap();
            assert!(connections.get() > 3, "{} connections", connections.get());
            let genuine = sparrow_client::Client::connect(&socket).await.unwrap();
            let accepted = genuine
                .import_file("lease-cut", "lease", wire::Format::Log, &input)
                .await
                .unwrap();
            assert_eq!(receipt["artifact_id"].as_str(), Some(accepted.id.as_str()));
            relay.abort();
            // No request fits beside the first chunk: nothing advances, so the
            // client stops resuming and cancels the journal it began.
            let stalled = dir.path().join("stalled.sock");
            let relay = lease_cutting_relay(&stalled, socket.clone(), 8 * 1024, connections);
            let error = sparrow_client::import_file(
                &sparrow_client::Endpoint::Unix(stalled),
                "lease-stall",
                "lease",
                wire::Format::Log,
                &input,
            )
            .await
            .unwrap_err();
            assert!(error.retryable(), "{error}");
            // A live journal would report its staging; a canceled one has ended
            // its lease, and once reclaimed it is gone.
            let status = sparrow_client::get_ingest_status(
                genuine.capability(),
                &genuine.discovery().store_id,
                &sparrow_client::IngestSelector::Request("lease-stall".into()),
                Default::default(),
            )
            .await;
            assert!(
                matches!(&status, Err(sparrow_client::Error::Domain { code, .. })
                    if code == "IMPORT_EXPIRED" || code == "NOT_FOUND"),
                "canceled staging still holds its import slot: {:?}",
                status.err()
            );
            relay.abort();
        })
        .await;
}

/// INVARIANT (#397): a bound on the input itself reaches the SDK as a non-retryable refusal that
/// names the bound. The free `import_file` does not resume or retry it: it returns the daemon's
/// own error, not `IMPORT_RESUMABLE`, and cancels its journal once. The refusal arrives at
/// layout, before any record is prepared.
#[tokio::test(flavor = "current_thread")]
async fn an_input_over_the_publication_record_bound_is_refused_non_retryable_and_cancelled() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let store_path = dir.path().join("store");
            let socket = store_path.join("rpc.sock");
            let config = write_server_config(dir.path(), &store_path, &socket);
            let text = std::fs::read_to_string(&config).unwrap();
            std::fs::write(&config, text.replace("max_records = 10000", "max_records = 100000"))
                .unwrap();
            let _daemon = start(&config, &socket).await;
            let genuine = sparrow_client::Client::connect(&socket).await.unwrap();
            let input = dir.path().join("bound.log");
            std::fs::write(&input, b"a\n".repeat(32_769)).unwrap();
            let error = sparrow_client::import_file(
                &sparrow_client::Endpoint::Unix(socket.clone()),
                "bound-input",
                "bound",
                wire::Format::Log,
                &input,
            )
            .await
            .unwrap_err();
            assert!(
                matches!(&error, sparrow_client::Error::Domain { code, message, retryable: false, .. }
                    if code == "RESOURCE_LIMIT" && message.contains("32768")),
                "{error}"
            );
            assert!(!error.retryable(), "{error}");
            let status = sparrow_client::get_ingest_status(
                genuine.capability(),
                &genuine.discovery().store_id,
                &sparrow_client::IngestSelector::Request("bound-input".into()),
                Default::default(),
            )
            .await;
            assert!(
                matches!(&status, Err(sparrow_client::Error::Domain { code, .. })
                    if code == "IMPORT_EXPIRED" || code == "NOT_FOUND"),
                "the refused import still holds its journal: {:?}",
                status.err()
            );
        })
        .await;
}

/// Stage an unpublishable import above the inline bound through `client` and return the
/// error it ends with. The relay refuses `importPublish`, as the daemon refuses a prepared import.
async fn refused_large_ingest(
    client: &sparrow_client::Client,
    request_id: &str,
) -> sparrow_client::Error {
    let bytes = vec![b'x'; sparrow_core::INLINE_ARTIFACT_BYTES + 1];
    client
        .ingest(request_id, "large", wire::Format::Opaque, &bytes)
        .await
        .unwrap_err()
}

/// A refused import through `Client::ingest` above the inline bound cancels its staging
/// journal, so the refusal does not hold an import slot until the retry window ends (#392).
#[tokio::test(flavor = "current_thread")]
async fn refused_large_ingest_over_a_connection_cancels_its_staging_journal() {
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
            state.refuse_publish.set(Some(("RESOURCE_LIMIT", true)));
            let client = sparrow_client::Client::connect(&proxy_socket)
                .await
                .unwrap();
            let error = refused_large_ingest(&client, "refused-large").await;
            assert!(
                matches!(&error, sparrow_client::Error::Domain { code, .. } if code == "RESOURCE_LIMIT"),
                "{error}"
            );
            // A live journal would report its staging; a canceled one has ended its lease.
            let status = sparrow_client::get_ingest_status(
                genuine.capability(),
                &genuine.discovery().store_id,
                &sparrow_client::IngestSelector::Request("refused-large".into()),
                Default::default(),
            )
            .await;
            assert!(
                matches!(&status, Err(sparrow_client::Error::Domain { code, .. })
                    if code == "IMPORT_EXPIRED" || code == "NOT_FOUND"),
                "refused import still holds its import slot: {:?}",
                status.err()
            );
            proxy.abort();
        })
        .await;
}

/// A refused import whose cancel the daemon answers with an error returns `IMPORT_ABANDONED`
/// naming the import, and leaves its journal staged for the retry window (#392).
#[tokio::test(flavor = "current_thread")]
async fn refused_large_ingest_whose_cancel_is_refused_reports_import_abandoned() {
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
            state.refuse_publish.set(Some(("RESOURCE_LIMIT", true)));
            state.refuse_cancel.set(Some(("PERMISSION_DENIED", false)));
            let client = sparrow_client::Client::connect(&proxy_socket)
                .await
                .unwrap();
            let error = refused_large_ingest(&client, "abandoned-large").await;
            assert!(
                matches!(&error, sparrow_client::Error::Domain { code, message, retryable: false, .. }
                    if code == "IMPORT_ABANDONED" && message.contains("RESOURCE_LIMIT")),
                "{error}"
            );
            let status = sparrow_client::get_ingest_status(
                genuine.capability(),
                &genuine.discovery().store_id,
                &sparrow_client::IngestSelector::Request("abandoned-large".into()),
                Default::default(),
            )
            .await;
            assert!(status.is_ok(), "uncancelled journal must stay staged: {:?}", status.err());
            proxy.abort();
        })
        .await;
}

/// A large ingest over a held connection that loses the connection, as the daemon's
/// connection lease does, returns the retryable transport error without a cancel: the journal
/// is intact and the same request ID completes on a new connection (#392).
#[tokio::test(flavor = "current_thread")]
async fn large_ingest_over_a_lost_connection_is_retryable_and_resumes_under_its_request_id() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let store_path = dir.path().join("store");
            let socket = store_path.join("rpc.sock");
            let config = write_server_config(dir.path(), &store_path, &socket);
            let _daemon = start(&config, &socket).await;
            let cutting = dir.path().join("cutting.sock");
            let relay = lease_cutting_relay(
                &cutting,
                socket.clone(),
                4 * 1024 * 1024,
                std::rc::Rc::new(std::cell::Cell::new(0)),
            );
            let lossy = sparrow_client::Client::connect(&cutting).await.unwrap();
            let bytes = vec![b'x'; sparrow_core::INLINE_ARTIFACT_BYTES + 1];
            let error = lossy
                .ingest("cut-large", "large", wire::Format::Opaque, &bytes)
                .await
                .unwrap_err();
            assert!(error.retryable(), "{error}");
            assert!(
                matches!(&error, sparrow_client::Error::Transport(_)),
                "a lost connection is returned unchanged: {error}"
            );
            let fresh = sparrow_client::Client::connect(&socket).await.unwrap();
            let receipt = fresh
                .ingest("cut-large", "large", wire::Format::Opaque, &bytes)
                .await
                .unwrap();
            assert_eq!(receipt.byte_length, bytes.len() as u64);
            relay.abort();
        })
        .await;
}

/// A cancel the daemon refuses with a retryable hold (the free-space floor) leaves the journal
/// intact: the error stays retryable, names the import and keeps its class, and the same
/// request ID then completes on a fresh connection (#392).
#[tokio::test(flavor = "current_thread")]
async fn large_ingest_whose_cancel_is_held_by_the_floor_is_retryable_and_resumes() {
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
            state.refuse_publish.set(Some(("RESOURCE_LIMIT", true)));
            state.refuse_cancel.set(Some(("STORAGE_PRESSURE", true)));
            let client = sparrow_client::Client::connect(&proxy_socket)
                .await
                .unwrap();
            let error = refused_large_ingest(&client, "held-large").await;
            assert!(error.retryable(), "{error}");
            assert!(error.is_free_space_floor(), "{error}");
            assert!(
                matches!(&error, sparrow_client::Error::Domain { code, message, .. }
                    if code == "IMPORT_RESUMABLE" && message.contains("import:")),
                "{error}"
            );
            let bytes = vec![b'x'; sparrow_core::INLINE_ARTIFACT_BYTES + 1];
            let receipt = genuine
                .ingest("held-large", "large", wire::Format::Opaque, &bytes)
                .await
                .unwrap();
            assert_eq!(receipt.byte_length, bytes.len() as u64);
            proxy.abort();
        })
        .await;
}

/// A hold refused mid-upload over a held connection (`STORAGE_PRESSURE`) is returned unchanged
/// and cancels nothing: the journal stays staged and the same request ID then completes (#392).
#[tokio::test(flavor = "current_thread")]
async fn large_ingest_held_by_the_floor_is_returned_unchanged_and_resumes() {
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
            state.refuse_publish.set(Some(("STORAGE_PRESSURE", true)));
            // A cancel would answer with a different code, so reaching the daemon shows.
            state.refuse_cancel.set(Some(("PERMISSION_DENIED", false)));
            let client = sparrow_client::Client::connect(&proxy_socket)
                .await
                .unwrap();
            let error = refused_large_ingest(&client, "hold-large").await;
            assert!(
                matches!(&error, sparrow_client::Error::Domain { code, retryable: true, .. }
                    if code == "STORAGE_PRESSURE"),
                "{error}"
            );
            let bytes = vec![b'x'; sparrow_core::INLINE_ARTIFACT_BYTES + 1];
            let receipt = genuine
                .ingest("hold-large", "large", wire::Format::Opaque, &bytes)
                .await
                .unwrap();
            assert_eq!(receipt.byte_length, bytes.len() as u64);
            proxy.abort();
        })
        .await;
}

/// Live input publishes before EOF, recovers the same occurrence after losing its
/// publication reply, and leaves ordinary logs their own admission and worker.
#[tokio::test(flavor = "current_thread")]
async fn bulk_reopens_an_unacknowledged_range_and_publishes_while_input_is_open() {
    use sparrow_client::{
        Endpoint, Format,
        bulk::{BulkConfig, BulkFile, BulkImporter},
    };
    use tokio::io::AsyncWriteExt;
    let test = async {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        let socket = store.join("rpc.sock");
        let config = write_server_config(dir.path(), &store, &socket);
        let _daemon = start(&config, &socket).await;
        let backend = connect(&socket).await.unwrap();
        let relay_path = dir.path().join("bulk.sock");
        let (state, proxy) = relay(&relay_path, backend.client.clone());
        state.withhold_publication.set(true);
        let mut cfg = BulkConfig::new(Endpoint::Unix(relay_path), dir.path().join("bulk"));
        cfg.staged_bytes = 32;
        cfg.range_bytes = 16;
        let bulk = BulkImporter::new(cfg).unwrap();
        let mut job = bulk
            .open("input-17", "bulk-stream", Format::Log)
            .await
            .unwrap();
        let (mut write, mut read) = tokio::io::duplex(32);
        write.write_all(b"head\n\xffwait").await.unwrap();
        {
            let staging = job.stage_stream(0, &mut read);
            tokio::pin!(staging);
            tokio::select! {r=&mut staging=>panic!("input closed before publication: {r:?}"),
            ()=state.first_accepted.notified()=>{}}
        }
        let held = state.accepted.borrow().clone().unwrap();
        let delivery = sparrow_client::Delivery::start_async(
            sparrow_client::DeliveryConfig::new(&socket, "bulk-operational")
                .with_quarantine_dir(dir.path().join("quarantine")),
        )
        .await
        .unwrap();
        admit(
            &delivery,
            delivery_event("progress during bulk publication"),
        );
        assert_eq!(
            delivery
                .shutdown_async(Duration::from_secs(5))
                .await
                .unwrap()
                .durable_events,
            1
        );
        job.close().await.unwrap();
        let mut job = bulk
            .open("input-17", "bulk-stream", Format::Log)
            .await
            .unwrap();
        assert_eq!(job.progress().borrow().staged, 10);
        write.write_all(b"!").await.unwrap();
        drop(write);
        job.stage_stream(10, &mut read).await.unwrap();
        let summary = job.finish().await.unwrap();
        assert_eq!((summary.bytes, summary.ranges), (11, 2));
        let job = bulk
            .open("input-17", "bulk-stream", Format::Log)
            .await
            .unwrap();
        let first = job.receipt(0).await.unwrap();
        let second = job.receipt(1).await.unwrap();
        assert_eq!(first.artifact_id, held.id);
        assert_eq!(
            (first.start, first.end, second.start, second.end),
            (0, 5, 5, 11)
        );
        let client = sparrow_client::Client::connect(&socket).await.unwrap();
        assert_eq!(
            client.bytes(&first.artifact_id, 0, 5).await.unwrap(),
            b"head\n"
        );
        assert_eq!(
            client.bytes(&second.artifact_id, 0, 6).await.unwrap(),
            b"\xffwait!"
        );
        let r = backend
            .client
            .status_request()
            .send()
            .promise
            .await
            .unwrap();
        assert_eq!(
            rpc_ok(r.get().unwrap().get_result().unwrap()).get_live_records(),
            4
        );
        job.close().await.unwrap();
        // An unchanged completed file can be larger than the available disk credit.
        let path = dir.path().join("complete.bin");
        let data: Vec<u8> = (0..145).collect();
        std::fs::write(&path, &data).unwrap();
        let file = BulkFile {
            id: "large-input".into(),
            source: "bulk-file".into(),
            format: Format::Opaque,
            path: path.clone(),
        };
        let collection = bulk.import_files([file.clone()]);
        futures::pin_mut!(collection);
        let (_, result) = futures::StreamExt::next(&mut collection).await.unwrap();
        let result = result.unwrap();
        assert!(result.ranges > 1);
        let job = bulk
            .open(&file.id, &file.source, file.format)
            .await
            .unwrap();
        let progress = job.progress().borrow().clone();
        assert!(progress.first_local_range > 0 && progress.last_window.is_some());
        assert!(
            job.receipt(0).await.is_err(),
            "published windows release their local receipt rows"
        );
        let mut page = Some(result.artifact_id.clone());
        let mut groups = Vec::new();
        while let Some(id) = page {
            let artifact = client.artifact(&id).await.unwrap();
            let bytes = client
                .bytes(&id, 0, artifact.byte_length as u32)
                .await
                .unwrap();
            let value =
                purrdf_lex::json::read_slice(&bytes, purrdf_lex::json::Limits::default()).unwrap();
            assert_eq!(value["schema"].as_str(), Some("sparrow.bulk-window/1"));
            assert_eq!(value["complete"].as_bool(), Some(groups.is_empty()));
            let mut group = Vec::new();
            for part in value["parts"].as_array().unwrap() {
                let bytes = client
                    .bytes(
                        part["artifact"].as_str().unwrap(),
                        0,
                        part["length"].as_u64().unwrap() as u32,
                    )
                    .await
                    .unwrap();
                group.extend(bytes);
            }
            groups.push(group);
            page = value["previous"].as_str().map(str::to_owned);
        }
        let recovered: Vec<u8> = groups.into_iter().rev().flatten().collect();
        assert_eq!(recovered, data);
        assert_eq!(std::fs::read(path).unwrap(), data);
        job.close().await.unwrap();
        let offline_socket = dir.path().join("late.sock");
        let offline = BulkImporter::new(BulkConfig::new(
            Endpoint::Unix(offline_socket.clone()),
            dir.path().join("disconnected"),
        ))
        .unwrap();
        let snapshot = dir.path().join("snapshot.log");
        std::fs::write(&snapshot, b"offline input").unwrap();
        let mut job = offline
            .open("snapshot", "offline-file", Format::Log)
            .await
            .unwrap();
        job.stage_file(&snapshot).await.unwrap();
        let p = job.progress().borrow().clone();
        assert_eq!((p.eof, p.staged, p.confirmed), (true, 13, 0));
        job.close().await.unwrap();
        std::fs::write(&snapshot, b"caller changed the original").unwrap();
        let (_, late) = relay(&offline_socket, backend.client.clone());
        let proof = offline
            .import_file(&BulkFile {
                id: "snapshot".into(),
                source: "offline-file".into(),
                format: Format::Log,
                path: snapshot,
            })
            .await
            .unwrap();
        assert_eq!(
            client.bytes(&proof.artifact_id, 0, 13).await.unwrap(),
            b"offline input"
        );
        // A certified single range also finishes if EOF arrives after restart, offline.
        let mut job = offline.open("sealed", "pipe", Format::Log).await.unwrap();
        let mut progress = job.progress();
        let (mut write, mut read) = tokio::io::duplex(16);
        write.write_all(b"sole\n").await.unwrap();
        {
            let staging = job.stage_stream(0, &mut read);
            tokio::pin!(staging);
            tokio::select! {
                result = &mut staging => panic!("input ended before durable receipt: {result:?}"),
                confirmed = progress.wait_for(|p| p.confirmed == 5) => drop(confirmed.unwrap()),
            }
        }
        let receipt = job.receipt(0).await.unwrap();
        job.close().await.unwrap();
        let unavailable = BulkImporter::new(BulkConfig::new(
            Endpoint::Unix(dir.path().join("absent.sock")),
            dir.path().join("disconnected"),
        ))
        .unwrap();
        let mut job = unavailable
            .open("sealed", "pipe", Format::Log)
            .await
            .unwrap();
        job.stage_stream(5, tokio::io::empty()).await.unwrap();
        assert_eq!(job.finish().await.unwrap().artifact_id, receipt.artifact_id);
        late.abort();
        proxy.abort();
    };
    tokio::task::LocalSet::new()
        .run_until(tokio::time::timeout(Duration::from_secs(30), test))
        .await
        .unwrap();
}

fn relay(
    socket: &Path,
    backend: wire::sparrow::Client,
) -> (std::rc::Rc<EventRelayState>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::UnixListener::bind(socket).unwrap();
    let state = std::rc::Rc::new(EventRelayState {
        backend: std::cell::RefCell::new(backend),
        profile: std::cell::Cell::new(None),
        artifact_limits: std::cell::Cell::new(None),
        refuse_discovery: std::cell::Cell::new(false),
        calls: std::cell::RefCell::new(Vec::new()),
        accepted: std::cell::RefCell::new(None),
        drop_first: std::cell::Cell::new(true),
        corrupt_receipt: std::cell::Cell::new(false),
        corrupt_publication: std::cell::Cell::new(false),
        withhold_publication: std::cell::Cell::new(false),
        refuse_publish: std::cell::Cell::new(None),
        refuse_cancel: std::cell::Cell::new(None),
        refusals: std::cell::RefCell::new(std::collections::VecDeque::new()),
        refusal_after_accept: std::cell::Cell::new(None),
        swap_backend_after_refused_publish: std::cell::RefCell::new(None),
        connections: std::cell::Cell::new(0),
        mutate_file: std::cell::RefCell::new(None),
        mutate_published_file: std::cell::RefCell::new(None),
        first_accepted: tokio::sync::Notify::new(),
        obstruct_checkpoint: std::cell::RefCell::new(None),
        discovered: tokio::sync::Notify::new(),
    });
    let capability: wire::sparrow::Client = capnp_rpc::new_client(EventRelay(state.clone()));
    let state_for_task = state.clone();
    let task = tokio::task::spawn_local(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            state_for_task
                .connections
                .set(state_for_task.connections.get() + 1);
            tokio::task::spawn_local(sparrow_server::serve_connection(stream, capability.clone()));
        }
    });
    (state, task)
}

/// The CLI immediately drains a closed producer with its persistent identity through
/// producer-profile discovery. An active journal and a misspelled directory fail closed.
#[tokio::test(flavor = "current_thread")]
async fn cli_drains_only_an_existing_inactive_producer_spool() {
    use sparrow_client::{Delivery, DeliveryConfig, Endpoint, SpoolConfig};
    let test = async {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        let socket = store.join("rpc.sock");
        let config = write_server_config(dir.path(), &store, &socket);
        let _daemon = start(&config, &socket).await;
        let backend = connect(&socket).await.unwrap();
        let proxy_socket = dir.path().join("profile.sock");
        let (state, proxy) = relay(&proxy_socket, backend.client.clone());
        state.profile.set(Some((true, 256)));
        state.drop_first.set(false);
        let drain = |path: &Path| {
            tokio::process::Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
                .arg(&proxy_socket)
                .arg("spool-drain")
                .arg(path)
                .args(["--timeout-secs", "10"])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .admitted_spawn()
                .unwrap()
        };
        let path = dir.path().join("private");
        let missing = drain(&path).wait_with_output().await.unwrap();
        assert!(!missing.status.success() && !path.exists());
        assert!(String::from_utf8_lossy(&missing.stderr).contains("cannot drain spool"));
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
        std::fs::File::create(path.join("LOCK")).unwrap();
        let no_header = drain(&path).wait_with_output().await.unwrap();
        assert!(!no_header.status.success() && !path.join("HEADER").exists());
        let offline = DeliveryConfig::lossless(
            Endpoint::Unix(dir.path().join("absent.sock")),
            "cli-source",
            SpoolConfig::new(&path),
        );
        let producer = Delivery::start_async(offline).await.unwrap();
        let accepted = producer
            .send_with_token(delivery_event("kept"), b"cursor")
            .await
            .unwrap();
        let locked = drain(&path).wait_with_output().await.unwrap();
        assert!(!locked.status.success());
        assert!(String::from_utf8_lossy(&locked.stderr).contains("already open"));
        producer.close_local(Duration::from_secs(10)).await.unwrap();
        for _ in 0..2 {
            let drained = drain(&path).wait_with_output().await.unwrap();
            assert!(drained.status.success(), "{drained:?}");
            let proof = purrdf_lex::json::read_slice(&drained.stdout, Default::default()).unwrap();
            assert_eq!(proof["durable_sequence"].as_str(), Some("1"));
            assert_eq!(
                proof["client_instance"].as_str(),
                Some(purrdf_hash::hex::encode(&accepted.client_instance).as_str())
            );
        }
        let calls = state.calls.borrow();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].source, "cli-source");
        assert_eq!(calls[0].client_instance, accepted.client_instance);
        proxy.abort();
    };
    tokio::task::LocalSet::new().run_until(test).await;
}

/// A durable reply lost before a daemon restart keeps its sealed identity. A permanent
/// refusal then suspends retries through appends and flushes until explicit operator retry.
#[tokio::test(flavor = "current_thread")]
async fn sealed_spool_holds_across_fences_and_retries_the_original_receipt_after_restart() {
    use sparrow_client::{Delivery, DeliveryConfig, Endpoint, Error, FlushError, SpoolConfig};
    let test = async {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        let socket = store.join("rpc.sock");
        let server = write_server_config(dir.path(), &store, &socket);
        let daemon = start(&server, &socket).await;
        let proxy_socket = dir.path().join("relay.sock");
        let backend = connect(&socket).await.unwrap();
        let (state, proxy) = relay(&proxy_socket, backend.client.clone());
        let config = DeliveryConfig::lossless(
            Endpoint::Unix(proxy_socket),
            "sealed-restart",
            SpoolConfig::new(dir.path().join("private")),
        )
        .with_test_timing(
            Duration::from_millis(20),
            Duration::from_millis(30),
            Duration::from_millis(30),
        );
        let deadline = Duration::from_secs(10);
        let delivery = Delivery::start_async(config.clone()).await.unwrap();
        delivery
            .send_with_token(delivery_event("lost reply"), b"one")
            .await
            .unwrap();
        tokio::time::timeout(deadline, state.first_accepted.notified())
            .await
            .unwrap();
        delivery.close_local(deadline).await.unwrap();
        drop(backend);
        drop(daemon);
        let _daemon = start(&server, &socket).await;
        let restarted = connect(&socket).await.unwrap();
        *state.backend.borrow_mut() = restarted.client.clone();
        state.refusals.borrow_mut().push_back(("CONFLICT", false));
        state.profile.set(Some((true, 256)));
        state.artifact_limits.set(Some((256, 1)));
        let delivery = Delivery::start_async(config.clone()).await.unwrap();
        let short = Duration::from_millis(350);
        assert!(matches!(
            delivery.flush_async(short).await,
            Err(FlushError::TimedOut(_))
        ));
        let stats = delivery.stats().unwrap();
        assert!(stats.held.is_none());
        assert!(
            matches!(stats.last_transient_failure.as_deref(), Some(Error::Unsupported(message)) if message.contains("immutable batch"))
        );
        assert_eq!(state.calls.borrow().len(), 1);
        state.artifact_limits.set(None);
        assert!(matches!(
            delivery.flush_async(short).await,
            Err(FlushError::TimedOut(_))
        ));
        assert!(
            matches!(delivery.stats().unwrap().held.as_deref(),Some(Error::Domain{code,..}) if code=="CONFLICT")
        );
        delivery
            .send_with_token(delivery_event("held intake"), b"two")
            .await
            .unwrap();
        assert!(matches!(
            delivery.flush_async(short).await,
            Err(FlushError::TimedOut(_))
        ));
        assert_eq!(state.calls.borrow().len(), 2);
        let checkpoint = dir.path().join("private/HEADER.new");
        *state.obstruct_checkpoint.borrow_mut() = Some(checkpoint.clone());
        delivery.retry_held();
        tokio::time::timeout(deadline, state.first_accepted.notified())
            .await
            .unwrap();
        state.refuse_discovery.set(true);
        assert!(matches!(
            delivery.flush_async(short).await,
            Err(FlushError::TimedOut(_))
        ));
        assert!(matches!(
            delivery.stats().unwrap().last_transient_failure.as_deref(),
            Some(Error::Io(_))
        ));
        std::fs::remove_dir(checkpoint).unwrap();
        // Only the second batch still needs the daemon. The first checkpoints offline
        // using its already certified receipt, even when a new discovery is refused.
        assert!(matches!(
            delivery.flush_async(short).await,
            Err(FlushError::TimedOut(_))
        ));
        assert_eq!(delivery.confirmed_token().unwrap(), b"one");
        assert_eq!(state.calls.borrow().len(), 3);
        state.refuse_discovery.set(false);
        assert_eq!(
            delivery
                .shutdown_async(deadline)
                .await
                .unwrap()
                .durable_events,
            2
        );
        assert_eq!(delivery.confirmed_token().unwrap(), b"two");
        assert_eq!(delivery.stats().unwrap().spool.unwrap().bytes, 0);
        drop(delivery);
        let empty = Delivery::start(config.clone()).unwrap();
        assert_eq!(empty.shutdown(deadline).unwrap().durable_events, 2);
        drop(empty);
        Delivery::start(config)
            .unwrap()
            .close_local(deadline)
            .await
            .unwrap();
        let calls = state.calls.borrow();
        assert_eq!(calls.len(), 4);
        assert_eq!(calls[0], calls[1]);
        assert_eq!(calls[0], calls[2]);
        assert_eq!(
            cli(&socket, &["status"])["live_records"].as_str(),
            Some("2")
        );
        proxy.abort();
    };
    tokio::task::LocalSet::new().run_until(test).await;
}

/// A newer daemon can acquire the producer profile while a worker is running. Limits apply
/// before first dispatch and a lost durable reply keeps its exact request across lower limits.
#[tokio::test(flavor = "current_thread")]
async fn missing_producer_profile_resumes_with_negotiated_limits_and_immutable_retries() {
    use sparrow_client::{
        Client, Delivery, DeliveryConfig, Endpoint, Error, FlushError, PRODUCER_PROFILE,
    };
    let test = async {
        let dir = tempfile::tempdir().unwrap();
        let store_path = dir.path().join("store");
        let socket = store_path.join("rpc.sock");
        let config = write_server_config(dir.path(), &store_path, &socket);
        let _daemon = start(&config, &socket).await;
        let backend = connect(&socket).await.unwrap();
        let proxy_socket = dir.path().join("relay.sock");
        let endpoint = Endpoint::Unix(proxy_socket.clone());
        let (state, proxy) = relay(&proxy_socket, backend.client.clone());
        state.profile.set(Some((false, 2)));
        state.refuse_discovery.set(true);
        state.drop_first.set(false);
        let mut config = DeliveryConfig::new(&proxy_socket, "profile-upgrade")
            .with_quarantine_dir(dir.path().join("quarantine"));
        config.batch_events = 4;
        config.rpc_timeout = Duration::from_millis(250);
        let config = config.with_test_timing(
            Duration::from_millis(20),
            Duration::from_millis(50),
            Duration::from_millis(50),
        );
        let delivery = Delivery::start(config.clone()).unwrap();
        for _ in 0..5 {
            admit(&delivery, delivery_event("queued across upgrade"));
        }
        let deadline = Duration::from_secs(5);
        tokio::time::timeout(deadline, state.discovered.notified())
            .await
            .unwrap();
        let held = delivery.flush_async(Duration::from_millis(250)).await;
        assert!(matches!(held, Err(FlushError::TimedOut(_))));
        let stats = delivery.stats().unwrap();
        assert!(matches!(stats.last_transient_failure.as_deref(),
            Some(Error::Domain { code, .. }) if code == "INVALID_ARGUMENT"));
        assert_eq!(stats.quarantined_events, 0);
        assert!(state.calls.borrow().is_empty());
        state.refuse_discovery.set(false);
        let timeout = delivery.flush_async(Duration::from_millis(250)).await;
        assert!(matches!(timeout, Err(FlushError::TimedOut(_))));
        let stats = delivery.stats().unwrap();
        assert!(matches!(
            stats.last_transient_failure.as_deref(),
            Some(Error::Unsupported(PRODUCER_PROFILE))
        ));
        assert!(stats.terminal_failure.is_none());
        assert!(state.calls.borrow().is_empty());

        state.profile.set(Some((true, 2)));
        let strict = Client::connect_endpoint(&endpoint).await;
        assert!(matches!(strict, Err(Error::Unsupported(_))));
        let watermark = delivery.flush_async(deadline).await.unwrap();
        assert_eq!(watermark.durable_events, 5);
        let sizes: Vec<_> = state
            .calls
            .borrow()
            .iter()
            .map(|call| call.events.len())
            .collect();
        assert_eq!(sizes, [1, 2, 2]);
        let direct = Client::connect_producer(&endpoint).await.unwrap();
        let mut oversized = state.calls.borrow()[1].clone();
        let mut extra = oversized.events[1].clone();
        extra.sequence += 1;
        oversized.events.push(extra);
        assert!(matches!(
            direct.events(&oversized).await,
            Err(Error::InvalidArgument(_))
        ));
        let read = direct.bytes("artifact:example", 0, 1).await;
        assert!(matches!(read, Err(Error::Unsupported(_))));
        assert_eq!(state.calls.borrow().len(), 3);

        state.drop_first.set(true);
        for _ in 0..2 {
            admit(&delivery, delivery_event("unknown durable reply"));
        }
        tokio::time::timeout(deadline, state.first_accepted.notified())
            .await
            .unwrap();
        // The record cap lowers the effective count while the event cap remains two.
        state.artifact_limits.set(Some((1, u64::MAX)));
        let held = delivery.flush_async(Duration::from_secs(1)).await;
        assert!(matches!(held, Err(FlushError::TimedOut(_))));
        let stats = delivery.stats().unwrap();
        assert!(matches!(
            stats.last_transient_failure.as_deref(),
            Some(Error::Unsupported(message)) if message.contains("immutable batch")
        ));
        assert_eq!(stats.quarantined_events, 0);
        assert_eq!(state.calls.borrow().len(), 4);
        state.artifact_limits.set(None);
        let watermark = delivery.shutdown_async(deadline).await.unwrap();
        assert_eq!(watermark.durable_events, 7);
        assert_eq!(state.calls.borrow().len(), 5);
        assert_eq!(state.calls.borrow()[3], state.calls.borrow()[4]);
        // A pristine event whose actual JSONL exceeds the independently lowered inline
        // byte ceiling is rejected locally and saved, without an RPC.
        state.artifact_limits.set(Some((256, 1)));
        let delivery = Delivery::start(config).unwrap();
        admit(
            &delivery,
            delivery_event("larger than the artifact ceiling"),
        );
        let settled = delivery.shutdown_async(deadline).await;
        assert!(
            matches!(settled, Err(FlushError::Dropped(value)) if value.quarantined_before_fence == 1)
        );
        assert_eq!(state.calls.borrow().len(), 5);
        proxy.abort();
    };
    tokio::task::LocalSet::new().run_until(test).await;
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
            // The publication committed but its reply is unusable: the cancel is answered
            // CONFLICT, so the genuine receipt is retrieved instead of an error.
            let retrieved = client
                .import_file(
                    "bad-publication-reply",
                    "publication",
                    wire::Format::Opaque,
                    &input,
                )
                .await
                .unwrap();
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
            assert_eq!(committed.id, retrieved.id);
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
            // Production refuses a sub-second flush interval; this test needs a prompt batch.
            let fast = sparrow_client::DeliveryConfig::new(proxy_socket, "pending-receipt")
                .with_quarantine_dir(dir.path().join("quarantine"));
            let (retry, retry_max) = (fast.retry_interval, fast.retry_max_interval);
            let delivery = sparrow_client::Delivery::start(fast.with_test_timing(
                Duration::from_millis(10),
                retry,
                retry_max,
            ))
            .unwrap();
            admit(&delivery, delivery_event("must retain unknown body"));
            // INVARIANT (#448): a receipt that contradicts the request is a hold, not the end of
            // delivery: the batch stays pending and is retried under its identity, the failure
            // shows as the latest transient failure, and delivery completes when the daemon
            // answers truthfully again.
            let deadline = Instant::now() + Duration::from_secs(3);
            loop {
                let stats = delivery.stats().unwrap();
                if stats.retries >= 2 && stats.last_transient_failure.is_some() {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "receipt mismatch must be a visible hold"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let statistics = delivery.stats().unwrap();
            assert!(statistics.terminal_failure.is_none() && statistics.running);
            assert_eq!(statistics.durable_events, 0);
            assert_eq!(statistics.pending_events, 1);
            assert!(delivery.pending_batch().unwrap().is_some());
            state.corrupt_receipt.set(false);
            let watermark = delivery_barrier(&delivery, false, Duration::from_secs(3))
                .await
                .expect("delivery completes once the receipt is truthful");
            assert_eq!(watermark.durable_events, 1);
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
        let cli = tokio::process::Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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
        std::fs::write(&config, format!("{original}\nbuffered_flush_interval_ms = 1000\n")).unwrap();
        let restarted = start(&config, &socket).await;
        let recovered = sparrow_client::Client::connect(&socket).await.unwrap();
        assert_eq!(recovered.artifact(&first.id).await.unwrap(), durable);
        assert!(matches!(recovered.flush(&fence).await,
            Err(sparrow_client::Error::Domain { code, retryable: false, .. }) if code == "STALE_WATERMARK"));
        let next = recovered.ingest_with_durability("periodic", "buffered-sdk", wire::Format::Log,
            b"automatic WAL boundary\n", wire::Durability::Buffered).await.unwrap();
        assert_eq!(next.durability, wire::Durability::Buffered);
        let deadline = Instant::now() + Duration::from_secs(8);
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
            let mut config = sparrow_client::DeliveryConfig::new(&proxy_socket, "tracing-layer")
                .with_quarantine_dir(dir.path().join("quarantine"));
            config.flush_interval = Duration::from_secs(60);
            let delivery = sparrow_client::Delivery::start(config).unwrap();
            let output_path = dir.path().join("console.log");
            let console = sparrow_client::Console::with_writers(
                Some(delivery.clone()),
                std::io::sink(),
                std::fs::File::create(&output_path).unwrap(),
                false,
            )
            .unwrap();
            let subscriber = tracing_subscriber::registry().with(console.layer());
            let mut stdout_writer = console.writer(sparrow_client::ConsoleStream::Stdout);
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
                let captured = 37;
                sparrow_client::sparrow_eprintln!("console capture {captured}");
                log::Log::log(
                    &console,
                    &log::Record::builder()
                        .target("worker.facade")
                        .level(log::Level::Info)
                        .args(format_args!("Uploaded item"))
                        .key_values(&[("items", u64::MAX)])
                        .build(),
                );
                std::io::Write::write_all(&mut stdout_writer, &[0xff]).unwrap();
                drop(child_entered);
                settle();
                drop(child);
                settle();
                drop(entered);
                settle();
                drop(root);
                settle();
            });
            // Finish the same fragment after the producing span and subscriber have exited.
            std::io::Write::write_all(&mut stdout_writer, b"\n").unwrap();
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
                assert_eq!(
                    events
                        .iter()
                        .filter(
                            |event| event.fields.iter().any(|field| field.name == "message"
                                && field.value == FieldValue::Debug("console capture 37".into()))
                        )
                        .count(),
                    1
                );
                let facade = events
                    .iter()
                    .find(|event| event.target == "worker.facade")
                    .unwrap();
                assert_eq!(facade.context.last().unwrap().name, "child");
                assert!(
                    facade.fields.iter().any(|field| field.name == "items"
                        && field.value == FieldValue::Unsigned(u64::MAX))
                );
                let stdout = events.iter().find(|event| event.name == "stdio").unwrap();
                assert_eq!(stdout.span, facade.span);
                assert!(stdout.fields.iter().any(|field| field.name == "stream"
                    && field.value == FieldValue::Text("stdout".into())));
                assert!(stdout.fields.iter().any(|field| field.name == "message"
                    && field.value == FieldValue::Bytes(vec![0xff, b'\n'])));
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
            assert_eq!(console.flush().await.unwrap().refused, 0);
            let output = std::fs::read_to_string(output_path).unwrap();
            assert_eq!(output.matches("console capture 37").count(), 1);
            assert_eq!(output.matches("typed tracing observation").count(), 3);
            assert_eq!(output.matches("Uploaded item").count(), 1);
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
                sparrow_client::DeliveryConfig::new(&proxy_socket, "delivery-lost-reply")
                    .with_quarantine_dir(dir.path().join("quarantine"));
            config.batch_events = 1;
            config.rpc_timeout = Duration::from_millis(150);
            // Production refuses sub-second intervals; this recovery test needs fast retries.
            let config = config.with_test_timing(
                Duration::from_secs(2),
                Duration::from_millis(20),
                Duration::from_millis(20),
            );
            let delivery = sparrow_client::Delivery::start(config).unwrap();
            let sequence = admit(&delivery, delivery_event("lost reply recovery"));
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
            let timed_config = sparrow_client::DeliveryConfig::new(&socket, "delivery-lost-reply")
                .with_quarantine_dir(dir.path().join("quarantine"));
            assert_eq!(timed_config.flush_interval, Duration::from_secs(2));
            // Production refuses a sub-second flush interval; this test needs prompt batching.
            let timed_config = timed_config.with_test_timing(
                Duration::from_millis(50),
                Duration::from_secs(1),
                Duration::from_secs(30),
            );
            let timed = sparrow_client::Delivery::start(timed_config).unwrap();
            admit(&timed, delivery_event("lost reply recovery"));
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

/// INVARIANT: a daemon answer is not a broken connection. A busy refusal (retryable
/// `RESOURCE_LIMIT`) keeps retrying the same pending batch, and every retry reuses the one
/// healthy connection: before, each retry reconnected and so spent a slot of the daemon's
/// connection ceiling. The batch is delivered once the refusals stop and nothing is quarantined.
#[tokio::test(flavor = "current_thread")]
async fn producer_delivery_retries_a_busy_refusal_on_the_same_connection() {
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
            state
                .refusals
                .borrow_mut()
                .extend([("RESOURCE_LIMIT", true); 3]);
            let mut config = sparrow_client::DeliveryConfig::new(&proxy_socket, "busy-retry")
                .with_quarantine_dir(dir.path().join("quarantine"));
            config.batch_events = 1;
            let config = config.with_test_timing(
                Duration::from_secs(2),
                Duration::from_millis(20),
                Duration::from_millis(20),
            );
            let delivery = sparrow_client::Delivery::start(config).unwrap();
            admit(&delivery, delivery_event("busy then accepted"));
            let watermark =
                reached_delivery(delivery_barrier(&delivery, true, Duration::from_secs(5)).await);
            assert_eq!(watermark.durable_events, 1);
            let stats = delivery.stats().unwrap();
            assert_eq!(stats.retries, 3);
            assert_eq!(stats.quarantined_events, 0);
            assert_eq!(state.calls.borrow().len(), 4);
            assert_eq!(
                state.connections.get(),
                1,
                "a daemon answer must not force a reconnect"
            );
            proxy.abort();
            assert!(proxy.await.unwrap_err().is_cancelled());
            drop(delivery);
            drop(backend);
            drop(daemon);
        })
        .await;
}

/// The disk-free sender exports an interior sequence gap through the real server,
/// continues after a permanent refusal, and requires explicit consent to a new store.
#[tokio::test(flavor = "current_thread")]
async fn disk_free_delivery_reports_gaps_and_rebinds_only_to_the_observed_store() {
    use sparrow_client::{Admission, Delivery, DeliveryConfig, DropReason, Endpoint, FlushError};
    async fn queued(handle: &Delivery, text: &str) -> u64 {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match handle.try_send(delivery_event(text)) {
                    Admission::Queued { sequence } => return sequence,
                    Admission::Dropped {
                        reason: DropReason::Contended,
                        ..
                    } => tokio::task::yield_now().await,
                    other => panic!("unexpected loss: {other:?}"),
                }
            }
        })
        .await
        .unwrap()
    }
    tokio::task::LocalSet::new().run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let mut servers = Vec::new();
        for location in [dir.path(), other.path()] {
            let store = location.join("store");
            let socket = store.join("rpc.sock");
            let config = write_server_config(location, &store, &socket);
            let daemon = start(&config, &socket).await;
            let connection = connect(&socket).await.unwrap();
            servers.push((daemon, connection, socket));
        }
        let proxy_socket = dir.path().join("relay.sock");
        let (state, proxy) = relay(&proxy_socket, servers[0].1.client.clone());
        state.drop_first.set(false);
        let quarantine = dir.path().join("quarantine");
        let source = "disk-free-gaps";
        let mut config = DeliveryConfig::lossy(Endpoint::Unix(proxy_socket), source)
            .with_quarantine_dir(&quarantine)
            .with_test_timing(Duration::from_secs(2), Duration::from_millis(10), Duration::from_millis(30));
        config.batch_events = 1;
        config.max_queued_events = 1;
        config.rpc_timeout = Duration::from_millis(500);
        let handle = Delivery::start_async(config).await.unwrap();
        let timeout = Duration::from_secs(5);
        let first = queued(&handle, "first observed sequence").await;
        let initial = reached_delivery(handle.flush_async(timeout).await);
        let store = initial.last_receipt.unwrap().store_id;
        state.refusals.borrow_mut().push_back(("INVALID_ARGUMENT", false));
        queued(&handle, "permanently refused").await;
        // The relay and daemon run on this LocalSet. Until we yield, the in-flight
        // singleton stays charged. Lock contention can race a worker state transition.
        loop {
            match handle.try_send(delivery_event("quota")) {
                Admission::Dropped { reason: DropReason::Full, .. } => break,
                Admission::Dropped { reason: DropReason::Contended, .. } => continue,
                other => panic!("queue must be occupied: {other:?}"),
            }
        }
        let refused = reached_delivery(handle.flush_async(timeout).await);
        assert!(refused.dropped_before_fence >= 2);
        let stats = handle.stats().unwrap();
        assert_eq!(stats.dropped_by_reason[DropReason::Refused as usize], 1);
        assert!(matches!(stats.last_refusal.as_deref(), Some(sparrow_client::Error::Domain { code, message, .. }) if code == "INVALID_ARGUMENT" && message.contains("injected refusal")));
        let last = queued(&handle, "later valid observation").await;
        let fence = reached_delivery(handle.flush_async(timeout).await);
        assert_eq!(fence.durable_events, 2);
        let rows = cli(&servers[0].2, &["sources", &store, "--source", source]);
        let stream = rows["rows"][0]["producer_stream_ids"][0].as_str().unwrap();
        let window = cli(&servers[0].2, &["producer-stream", &store, stream]);
        let number = |value: &purrdf_lex::json::Value| value.as_str().unwrap().parse::<u64>().unwrap();
        assert_eq!(number(&window["unobserved_sequences"]), last-first-1);
        assert_eq!(number(&window["gaps"][0]["first"]), first+1);
        assert_eq!(number(&window["gaps"][0]["last"]), last-1);
        assert_eq!(handle.stats().unwrap().dropped_sequences, last-2);
        state.drop_first.set(true);
        queued(&handle, "unknown reply on the original store").await;
        tokio::time::timeout(timeout, state.first_accepted.notified()).await.unwrap();
        *state.backend.borrow_mut() = servers[1].1.client.clone();
        assert!(matches!(handle.flush_async(timeout).await, Err(FlushError::Dropped(_))));
        let new_store = handle.stats().unwrap().store_mismatch.unwrap();
        assert_ne!(new_store, store);
        assert!(handle.rebind(&store).await.is_err());
        assert!(matches!(handle.try_send(delivery_event("held")), Admission::Dropped { reason: DropReason::Refused, .. }));
        handle.rebind(&new_store).await.unwrap();
        queued(&handle, "after rebind").await;
        let fence = reached_delivery(handle.shutdown_async(timeout).await);
        assert_eq!(fence.last_receipt.unwrap().store_id, new_store);
        assert_eq!(fence.durable_events, 3);
        assert!(!quarantine.exists());
        proxy.abort();
    }).await;
}

/// INVARIANT (#216): a permanent refusal (a non-retryable `INVALID_ARGUMENT`: a batch that can
/// never fit) quarantines that batch into a file, with the reason, the count and every event
/// line, while a busy or capacity refusal is never quarantined. The next event is delivered
/// instead of queueing forever behind it. Before, the batch was given up as a counted drop and
/// its bytes were gone; this test fails on that behaviour because no quarantine file exists.
#[tokio::test(flavor = "current_thread")]
async fn producer_delivery_quarantines_a_permanent_refusal_without_blocking_later_events() {
    // CONFLICT (overlapping producer sequences, a differing source time mapping) used to end
    // delivery as a terminal failure and drop every later event; it is quarantined as well.
    quarantine_permanent_refusal("INVALID_ARGUMENT").await;
    quarantine_permanent_refusal("CONFLICT").await;
}

async fn quarantine_permanent_refusal(code: &'static str) {
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
            state.refusals.borrow_mut().push_back((code, false));
            let mut config = sparrow_client::DeliveryConfig::new(&proxy_socket, "quarantine").with_quarantine_dir(dir.path().join("quarantine"));
            config.batch_events = 1;
            let config = config.with_test_timing(
                Duration::from_secs(2),
                Duration::from_millis(20),
                Duration::from_millis(20),
            );
            let delivery = sparrow_client::Delivery::start(config).unwrap();
            let refused = admit(&delivery, delivery_event("permanently refused"));
            let kept = admit(&delivery, delivery_event("delivered after the refusal"));
            let watermark = match delivery_barrier(&delivery, true, Duration::from_secs(5)).await {
                Err(sparrow_client::FlushError::Dropped(value)) => *value,
                result => panic!("a quarantined event must be reported at the fence: {result:?}"),
            };
            assert_eq!((refused, kept), (1, 2));
            assert_eq!(watermark.dropped_before_fence, 1);
            assert_eq!(watermark.durable_events, 1);
            assert_eq!(watermark.durable_sequence, kept);
            let stats = delivery.stats().unwrap();
            assert_eq!(stats.quarantined_events, 1);
            assert_eq!(stats.retries, 0, "a permanent refusal is not retried");
            assert!(stats.terminal_failure.is_none());
            assert_eq!((stats.pending_events, stats.pending_bytes), (0, 0));
            assert!(matches!(
                stats.last_quarantine.as_deref(),
                Some(sparrow_client::Error::Domain { code: refused, retryable: false, .. }) if refused == code
            ));
            assert_eq!(state.calls.borrow().len(), 2);
            assert_eq!(stats.quarantine_files, 1);
            assert_eq!(stats.quarantine_write_failures, 0);
            assert_eq!(watermark.quarantined_before_fence, 1);
            let file = stats.last_quarantine_file.clone().unwrap();
            assert!(file.starts_with(dir.path().join("quarantine")));
            let kept_text = std::fs::read_to_string(&file).unwrap();
            let header = kept_text.lines().next().unwrap();
            assert!(header.contains(&format!("\"reason_code\":\"{code}\"")), "{header}");
            assert!(header.contains("\"events\":1"), "{header}");
            assert!(kept_text.contains("permanently refused"), "{kept_text}");
            let response = backend.client.status_request().send().promise.await.unwrap();
            assert_eq!(rpc_ok(response.get().unwrap().get_result().unwrap()).get_live_records(), 1);
            proxy.abort();
            assert!(proxy.await.unwrap_err().is_cancelled());
            drop(delivery);
            drop(backend);
            drop(daemon);
        })
        .await;
}

/// INVARIANT (#216): a permanently refused batch whose quarantine file cannot be written is
/// never dropped: the write failure is counted, the batch stays the pending batch and is sent
/// again, and here the daemon then takes it. Before, the batch was given up as a counted drop
/// the moment it was refused, so the fence would report a drop and nothing would be stored.
#[tokio::test(flavor = "current_thread")]
async fn producer_delivery_keeps_a_refused_batch_when_the_quarantine_cannot_be_written() {
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
            state
                .refusals
                .borrow_mut()
                .push_back(("INVALID_ARGUMENT", false));
            // A regular file where the directory should be makes every quarantine write fail.
            let blocked = dir.path().join("blocked");
            std::fs::write(&blocked, b"not a directory").unwrap();
            let mut config = sparrow_client::DeliveryConfig::new(&proxy_socket, "quarantine-held")
                .with_quarantine_dir(blocked.join("quarantine"));
            config.batch_events = 1;
            let config = config.with_test_timing(
                Duration::from_secs(2),
                Duration::from_millis(20),
                Duration::from_millis(20),
            );
            let delivery = sparrow_client::Delivery::start(config).unwrap();
            let sequence = admit(&delivery, delivery_event("refused but never dropped"));
            let watermark = delivery_barrier(&delivery, true, Duration::from_secs(5))
                .await
                .expect("the held batch is delivered once the daemon takes it");
            assert_eq!(watermark.durable_sequence, sequence);
            let stats = delivery.stats().unwrap();
            assert_eq!(stats.quarantine_write_failures, 1);
            assert_eq!((stats.quarantined_events, stats.quarantine_files), (0, 0));
            assert_eq!(stats.durable_events, 1);
            assert_eq!(state.calls.borrow().len(), 2);
            proxy.abort();
            assert!(proxy.await.unwrap_err().is_cancelled());
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
                sparrow_client::DeliveryConfig::new(&socket, "delivery-outage")
                    .with_quarantine_dir(dir.path().join("quarantine"));
            delivery_config.max_queued_events = 2;
            delivery_config.batch_events = 1;
            // Production refuses sub-second intervals; this outage test needs fast retries.
            let delivery_config = delivery_config.with_test_timing(
                Duration::from_secs(2),
                Duration::from_millis(25),
                Duration::from_millis(25),
            );
            let delivery = sparrow_client::Delivery::start(delivery_config).unwrap();
            admit(&delivery, delivery_event("before service start one"));
            admit(&delivery, delivery_event("before service start two"));
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

/// Replacing a discovered store preserves the unknown batch in quarantine. With an explicit
/// store pin, the producer holds it and a cancelled async fence leaves the next fence's
/// drop report intact while the configured store is restored.
#[tokio::test(flavor = "current_thread")]
async fn producer_delivery_refuses_store_replacement_and_retains_unknown_pending_input() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let first = dir.path().join("first");
            let second = dir.path().join("second");
            std::fs::create_dir(&first).unwrap();
            std::fs::create_dir(&second).unwrap();
            let first_store = first.join("store");
            let first_socket = first_store.join("rpc.sock");
            let second_store = second.join("store");
            let second_socket = second_store.join("rpc.sock");
            let first_config = write_server_config(&first, &first_store, &first_socket);
            let second_config = write_server_config(&second, &second_store, &second_socket);
            let first_daemon = start(&first_config, &first_socket).await;
            let second_daemon = start(&second_config, &second_socket).await;
            let first_backend = connect(&first_socket).await.unwrap();
            let second_backend = connect(&second_socket).await.unwrap();
            let proxy_socket = dir.path().join("relay.sock");
            let (state, proxy) = relay(&proxy_socket, first_backend.client.clone());
            let mut config =
                sparrow_client::DeliveryConfig::new(&proxy_socket, "store-bound-delivery")
                    .with_quarantine_dir(dir.path().join("quarantine"));
            config.batch_events = 1;
            config.rpc_timeout = Duration::from_millis(150);
            // Production refuses sub-second intervals; this test needs fast retries.
            let config = config.with_test_timing(
                Duration::from_secs(2),
                Duration::from_millis(100),
                Duration::from_millis(100),
            );
            let delivery = sparrow_client::Delivery::start(config.clone()).unwrap();
            admit(&delivery, delivery_event("unknown accepted delivery"));
            admit(&delivery, delivery_event("queued behind the pending batch"));
            tokio::time::timeout(Duration::from_secs(3), state.first_accepted.notified())
                .await
                .unwrap();
            *state.backend.borrow_mut() = second_backend.client.clone();
            // INVARIANT (#448): a store replaced behind the socket never ends delivery. The batch
            // that met it, whose acceptance by the old store is unknown, is set aside in a
            // quarantine file under STORE_MISMATCH and the worker adopts the store now behind the
            // socket, so later events are delivered there. Before, the worker stopped for good and
            // every later event was dropped as Failed.
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let stats = delivery.stats().unwrap();
                if stats.quarantined_events == 1 {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "store replacement must set the pending batch aside"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let watermark = match delivery_barrier(&delivery, true, Duration::from_secs(5)).await {
                Err(sparrow_client::FlushError::Dropped(value)) => *value,
                result => panic!("the set-aside batch is reported once: {result:?}"),
            };
            let stats = delivery.stats().unwrap();
            assert!(stats.terminal_failure.is_none());
            assert_eq!((stats.quarantined_events, watermark.durable_events), (1, 1));
            let file = stats.last_quarantine_file.clone().unwrap();
            let saved = std::fs::read_to_string(&file).unwrap();
            assert!(
                saved.contains("STORE_MISMATCH") && saved.contains("unknown accepted delivery"),
                "{saved}"
            );
            let response = second_backend
                .client
                .status_request()
                .send()
                .promise
                .await
                .unwrap();
            assert_eq!(
                rpc_ok(response.get().unwrap().get_result().unwrap()).get_live_records(),
                1,
                "the queued event reaches the store now behind the socket"
            );
            // The original store alone can certify this lost reply. A historic v1
            // file lacks a destination, so a miss there must be a read-only probe.
            let first_client = sparrow_client::Client::connect(&first_socket)
                .await
                .unwrap();
            let legacy = dir.path().join("legacy.jsonl");
            let (header,payload) = saved.split_once('\n').unwrap();
            let mut header = purrdf_lex::json::read_slice(header.as_bytes(), purrdf_lex::json::Limits::default()).unwrap().as_object().unwrap().clone();
            header.insert("profile", "sparrow.client.quarantine/1");
            header.remove("original"); header.remove("store");
            let legacy_data = format!("{}\n{payload}", purrdf_lex::json::write_compact(&header.into()));
            std::fs::write(&legacy, &legacy_data).unwrap();
            let second_client = sparrow_client::Client::connect(&second_socket).await.unwrap();
            let uncertain = sparrow_client::replay_quarantine(&second_client, &legacy).await.unwrap_err();
            assert!(matches!(uncertain, sparrow_client::Error::Domain {code, ..} if code == "UNCERTAIN_DELIVERY"));
            assert_eq!(std::fs::read_to_string(&legacy).unwrap(), legacy_data);
            assert!(sparrow_client::replay_quarantine(&second_client, &file).await.is_err());
            assert_eq!(std::fs::read_to_string(&file).unwrap(), saved);
            let historical = sparrow_client::replay_quarantine(&first_client, &legacy).await.unwrap();
            let replayed = sparrow_client::replay_quarantine(&first_client, &file)
                .await
                .unwrap();
            assert_eq!(replayed.events, 1);
            assert!(!file.exists());
            assert_eq!(
                state.accepted.borrow().clone().unwrap(), replayed.receipt
            );
            assert_eq!(historical.receipt, replayed.receipt);
            assert_eq!(cli(&first_socket, &["status"])["live_records"].as_str(), Some("1"));
            let mut pinned_config = config;
            pinned_config.expect = Some(sparrow_client::Expect::store(first_client.discovery().store_id.clone()));
            pinned_config.max_queued_events = 1;
            let pinned = sparrow_client::Delivery::start_async(pinned_config).await.unwrap();
            let before = state.calls.borrow().len();
            admit(&pinned, delivery_event("only to the first store"));
            assert!(matches!(pinned.try_send(delivery_event("full")), sparrow_client::Admission::Dropped {
                reason: sparrow_client::DropReason::Full, ..
            }));
            let mut flushing = Box::pin(pinned.flush_async(Duration::from_secs(5)));
            assert!(futures::poll!(&mut flushing).is_pending());
            assert!(matches!(pinned.flush(Duration::ZERO), Err(sparrow_client::FlushError::Busy)));
            drop(flushing);
            tokio::time::timeout(Duration::from_secs(5), async {
                while pinned.stats().unwrap().retries == 0 {
                    state.discovered.notified().await;
                }
            }).await.unwrap();
            let held = pinned.stats().unwrap();
            assert!(matches!(held.last_transient_failure.as_deref(), Some(sparrow_client::Error::Domain {code, ..}) if code == "INSTANCE_MISMATCH"));
            assert_eq!(state.calls.borrow().len(), before, "a mismatched daemon received the batch");
            *state.backend.borrow_mut() = first_backend.client.clone();
            let Err(sparrow_client::FlushError::Dropped(report)) = pinned.shutdown_async(Duration::from_secs(5)).await else {
                panic!("the restored store must receive the held batch and report the earlier drop");
            };
            assert_eq!((report.durable_events, report.dropped_before_fence, report.quarantined_before_fence), (1, 1, 0));
            pinned.flush(Duration::ZERO).unwrap();
            proxy.abort();
            assert!(proxy.await.unwrap_err().is_cancelled());
            drop(delivery);
            drop(first_client);
            drop(first_backend);
            drop(second_backend);
            drop(first_daemon);
            drop(second_daemon);
        })
        .await;
}

/// INVARIANT (#448): a refusal with a code the client does not know, marked non-retryable, is
/// set aside durably and delivery carries on; it never ends the worker. The next barrier after
/// the one that reported the quarantine is clean, with the totals kept in the statistics.
/// Before, the worker stopped for good on this code, every later event was dropped as Failed
/// and every barrier failed.
#[tokio::test(flavor = "current_thread")]
async fn producer_delivery_sets_aside_an_unknown_refusal_and_carries_on() {
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
            state
                .refusals
                .borrow_mut()
                .push_back(("A_CODE_FROM_THE_FUTURE", false));
            let mut config = sparrow_client::DeliveryConfig::new(&proxy_socket, "unknown-refusal")
                .with_quarantine_dir(dir.path().join("quarantine"));
            config.batch_events = 1;
            let config = config.with_test_timing(
                Duration::from_secs(2),
                Duration::from_millis(20),
                Duration::from_millis(20),
            );
            let delivery = sparrow_client::Delivery::start(config).unwrap();
            admit(&delivery, delivery_event("refused under a new code"));
            let kept = admit(&delivery, delivery_event("delivered after the refusal"));
            let first = match delivery_barrier(&delivery, false, Duration::from_secs(5)).await {
                Err(sparrow_client::FlushError::Dropped(value)) => *value,
                result => panic!("the set-aside event must be reported once: {result:?}"),
            };
            assert_eq!(
                (first.dropped_before_fence, first.durable_sequence),
                (1, kept)
            );
            let stats = delivery.stats().unwrap();
            assert!(stats.terminal_failure.is_none() && stats.running);
            assert_eq!(
                (
                    stats.quarantined_events,
                    stats.quarantine_files,
                    stats.retries
                ),
                (1, 1, 0)
            );
            let later = admit(&delivery, delivery_event("sent after the fence"));
            let second = delivery_barrier(&delivery, false, Duration::from_secs(5))
                .await
                .expect("a barrier that reported the drop is clean again");
            assert_eq!(
                (second.dropped_before_fence, second.durable_sequence),
                (0, later)
            );
            assert_eq!(
                delivery.stats().unwrap().quarantined_events,
                1,
                "totals stay in the statistics"
            );
            proxy.abort();
            assert!(proxy.await.unwrap_err().is_cancelled());
            drop((delivery, backend, daemon));
        })
        .await;
}

/// The operator command checkpoints its accepted prefix across a refusal, then
/// sends the outstanding part once and removes the file after both receipts.
#[tokio::test(flavor = "current_thread")]
async fn quarantine_replay_removes_the_file_only_on_acknowledgment() {
    async fn command(endpoint: &Path, file: &Path) -> std::process::Output {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"));
        cmd.current_dir(file.parent().unwrap())
            .arg(endpoint)
            .arg("quarantine-replay")
            .arg(file.file_name().unwrap());
        tokio::task::spawn_blocking(move || cmd.admitted_output().unwrap())
            .await
            .unwrap()
    }
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
            let mut config = sparrow_client::DeliveryConfig::new(&proxy_socket, "replay").with_quarantine_dir(dir.path().join("quarantine"));
            config.batch_events = 2;
            let config = config.with_test_timing(Duration::from_secs(2), Duration::from_millis(20), Duration::from_millis(20));
            let delivery = sparrow_client::Delivery::start(config).unwrap();
            admit(&delivery, delivery_event("prime discovery"));
            delivery.flush_async(Duration::from_secs(5)).await.unwrap();
            state.refusals.borrow_mut().push_back(("INVALID_ARGUMENT", false));
            admit(&delivery, delivery_event("first quarantined observation"));
            admit(&delivery, delivery_event("second quarantined observation"));
            assert!(matches!(delivery_barrier(&delivery, false, Duration::from_secs(5)).await, Err(sparrow_client::FlushError::Dropped(_))));
            let file = delivery.stats().unwrap().last_quarantine_file.clone().unwrap();
            let stem = file.file_name().unwrap().to_str().unwrap().strip_suffix(".quarantine.jsonl").unwrap();
            let scratch = file.with_file_name(format!(".{stem}.{}.tmp", std::process::id()));
            // A producer can die after linking the published name but before unlinking its scratch.
            std::fs::hard_link(&file, &scratch).unwrap();
            state.profile.set(Some((true, 1)));
            state.refusal_after_accept.set(Some(("CONFLICT", false)));
            let refused = command(&proxy_socket, &file).await;
            let stderr = String::from_utf8(refused.stderr).unwrap();
            assert!(!refused.status.success() && stderr.contains("CONFLICT"), "{stderr}");
            let kept_text = std::fs::read_to_string(&file).unwrap();
            let header = kept_text.lines().next().unwrap();
            assert!(header.contains("\"last_replay_code\":\"CONFLICT\"") && header.contains("\"replay_attempts\":1"), "{header}");
            let json = purrdf_lex::json::read_slice(header.as_bytes(), purrdf_lex::json::Limits::default()).unwrap();
            assert_eq!(json["certificates"].as_array().unwrap().len(), 1);
            assert!(json.get("replay_attempt").is_none(), "a definitive first refusal permits a fresh deadline");
            assert!(!scratch.exists(), "recover the writer's second link");
            // Simulate process loss while appending the next attempt. No RPC was authorized.
            use std::io::Write;
            std::fs::OpenOptions::new().append(true).open(&file).unwrap().write_all(br#"{"part":1,"attempt":"#).unwrap();
            let held = state.accepted.borrow().clone().unwrap();
            let direct = sparrow_client::Client::connect(&socket).await.unwrap();
            assert_eq!(cli(&socket, &["status"])["live_records"].as_str(), Some("2"));
            let calls = state.calls.borrow().len();
            let replayed = command(&proxy_socket, &file).await;
            let stdout = String::from_utf8(replayed.stdout).unwrap();
            assert!(replayed.status.success() && !file.exists(), "{}", String::from_utf8_lossy(&replayed.stderr));
            assert_eq!(state.calls.borrow().len(), calls+1, "only the second part was submitted on recovery");
            assert_eq!(direct.artifact(&held.id).await.unwrap(), held);
            assert_eq!(cli(&socket, &["status"])["live_records"].as_str(), Some("3"));
            eprintln!("quarantine-replay first exit={} stderr={} retained=true certified_parts=1 store_records=2", refused.status, stderr.trim());
            eprintln!("quarantine-replay second exit={} stdout={} first_artifact={} store_records=3", replayed.status, stdout.trim(), held.id);
            proxy.abort();
            assert!(proxy.await.unwrap_err().is_cancelled());
            drop((delivery, direct, backend, daemon));
        })
        .await;
}

/// A file-backed producer reauthenticates after secret rotation and certifies its
/// held sequence. An explicitly captured credential remains a static snapshot.
#[tokio::test(flavor = "current_thread")]
async fn rotating_a_tcp_credential_recovers_the_same_pending_delivery() {
    use sparrow_client::{Delivery, DeliveryConfig, Endpoint, TcpSecret};
    tokio::task::LocalSet::new().run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        let socket = store.join("rpc.sock");
        let config = write_server_config(dir.path(), &store, &socket);
        let (address, reservation) = configure_tcp_listener(&config);
        drop(reservation);
        let _daemon = start(&config, &socket).await;
        let path = dir.path().join("producer.secret");
        std::fs::copy(config.with_file_name("tcp.secret"), &path).unwrap();
        std::fs::write(&path, [17u8; 32]).unwrap();
        let frozen = Endpoint::AuthenticatedTcp(address, TcpSecret::read(&path).unwrap());
        let endpoint = Endpoint::Tcp(address).with_secret_file(&path).unwrap();
        let mut config = DeliveryConfig::for_endpoint(endpoint, "rotated-producer").with_test_timing(Duration::from_secs(1), Duration::from_millis(20), Duration::from_millis(50));
        config.batch_events = 1;
        let handle = Delivery::start_async(config).await.unwrap();
        let sequence = admit(&handle, delivery_event("held through authentication"));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !matches!(handle.stats().unwrap().last_transient_failure.as_deref(), Some(sparrow_client::Error::Domain {code, ..}) if code == "UNAUTHENTICATED") { tokio::task::yield_now().await; }
        }).await.unwrap();
        let replacement = dir.path().join("current.secret");
        std::fs::copy(dir.path().join("tcp.secret"), &replacement).unwrap();
        std::fs::rename(replacement, &path).unwrap();
        let result = handle.shutdown_async(Duration::from_secs(5)).await.unwrap();
        assert_eq!((result.durable_sequence, result.durable_events), (sequence, 1));
        assert!(matches!(sparrow_client::Client::connect_producer(&frozen).await, Err(sparrow_client::Error::Domain {code, ..}) if code == "UNAUTHENTICATED"));
        assert_eq!(cli(&socket, &["status"])["live_records"].as_str(), Some("1"));
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
            let client = tcp_client(address).await.unwrap();
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
            let tcp = tcp_client(address).await.unwrap();
            assert_eq!(tcp.discovery().store_id, receipt.store_id);
            let old = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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

/// A name that is not a socket, or a symlink, is a deterministic refusal: the start ends with the
/// status the unit does not restart and the name is preserved. A live foreign listener is an owner
/// that may leave, so the start waits for it, says so, preserves it, and ends cleanly on SIGINT.
#[tokio::test(flavor = "current_thread")]
async fn endpoint_startup_preserves_files_symlinks_and_foreign_live_listeners() {
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
        let stderr = if foreign.is_some() {
            let mut daemon = Watched::spawn(&config, None);
            let waiting = daemon.until("waiting to bind the local endpoint").await;
            assert!(waiting.contains("preserving it"), "{waiting}");
            assert!(daemon.child.try_wait().unwrap().is_none());
            daemon.interrupt();
            let (status, rest) = daemon.exit().await;
            assert!(
                status.success(),
                "a waiting start refused SIGINT: {status}: {rest}"
            );
            format!("{waiting}{rest}")
        } else {
            let result = Command::new(env!("CARGO_BIN_EXE_sparrowd-rpc-fixture"))
                .arg("--config")
                .arg(&config)
                .admitted_output()
                .unwrap();
            assert_eq!(
                result.status.code(),
                Some(REFUSED),
                "occupied endpoint was admitted: {kind}"
            );
            String::from_utf8(result.stderr).unwrap()
        };
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

/// INVARIANT: below the maintenance reserve (the space an open writes into) the daemon
/// neither opens its store nor accepts connections, and an interrupt while it waits for space exits cleanly.
#[tokio::test(flavor = "current_thread")]
async fn startup_below_the_maintenance_reserve_waits_without_opening_the_store() {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("store");
    let socket = data.join("rpc.sock");
    let config = write_server_config(dir.path(), &data, &socket);
    let base = std::fs::read_to_string(&config).unwrap();
    let floor = "min_free_bytes = 1048576";
    assert!(base.contains(floor));
    // No filesystem holds the reserve the open needs, so the daemon waits. (Below the floor
    // alone it opens and serves reads: `daemon_starts_below_the_free_space_floor_and_serves_reads`.)
    std::fs::write(
        &config,
        base.replace(floor, "min_free_bytes = 9223372036854775807")
            .replace(
                "disk_budget = 134217728",
                "disk_budget = 9223372036854775807",
            )
            .replace(
                "maintenance_reserve = 8388608",
                "maintenance_reserve = 9223372036854775000",
            ),
    )
    .unwrap();
    // The service manager's notification socket: the wait is also its StatusText.
    let notify = dir.path().join("notify");
    let receiver = std::os::unix::net::UnixDatagram::bind(&notify).unwrap();
    receiver
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let mut daemon = tokio::process::Command::new(env!("CARGO_BIN_EXE_sparrowd-rpc-fixture"))
        .arg("--config")
        .arg(&config)
        .env("NOTIFY_SOCKET", &notify)
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
    assert!(
        line.starts_with("sparrowd: waiting to open the store: only "),
        "{line}"
    );
    let mut status = [0_u8; 512];
    let length = receiver.recv(&mut status).unwrap();
    assert!(
        String::from_utf8_lossy(&status[..length])
            .starts_with("STATUS=waiting to open the store: only "),
        "the wait was not reported as the unit's status"
    );
    assert!(!data.exists(), "store opened below the maintenance reserve");
    rustix::process::kill_process(pid, rustix::process::Signal::INT).unwrap();
    let status = tokio::time::timeout(Duration::from_secs(30), daemon.wait())
        .await
        .unwrap()
        .unwrap();
    stderr.read_to_string(&mut line).await.unwrap();
    assert!(
        status.success(),
        "waiting startup refused interrupt: {status}: {line}"
    );
    assert!(!line.contains("sparrowd: store="), "{line}");
    assert!(!data.exists(), "interrupted wait opened the store");
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
    let mut daemon = tokio::process::Command::new(env!("CARGO_BIN_EXE_sparrowd-rpc-fixture"))
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
                .filter(|line| !line.starts_with("rpc_tcp_"))
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
            // The held port is a departing owner: the start waits for it, says so, and
            // proceeds when the holder leaves.
            let mut daemon = Watched::spawn(&config, None);
            let waiting = daemon.until("waiting to bind the syslog listeners").await;
            assert!(
                waiting.contains("Address already in use"),
                "unexpected wait: {waiting}"
            );
            assert!(
                !waiting.contains("pthread lock"),
                "unjoined startup worker: {waiting}"
            );
            assert!(daemon.child.try_wait().unwrap().is_none());
            assert!(std::net::TcpStream::connect(address).is_ok());
            drop(occupied);
            daemon.until("the condition cleared").await;
            daemon.until("sparrowd: store=").await;
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
    start_with(config, socket, stderr, &[]).await
}

async fn start_with(config: &Path, socket: &Path, stderr: Stdio, args: &[&str]) -> Daemon {
    let mut daemon = Daemon(
        Command::new(env!("CARGO_BIN_EXE_sparrowd-rpc-fixture"))
            .arg("--config")
            .arg(config)
            .args(args)
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

fn rpc_error_code<T: capnp::traits::Owned + 'static>(
    result: wire::result::Reader<'_, T>,
) -> String {
    match result.which().unwrap() {
        wire::result::Ok(_) => panic!("expected a typed RPC failure"),
        wire::result::Error(error) => error
            .unwrap()
            .get_code()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned(),
    }
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
        let (address, reservation) = configure_tcp_listener(&config);
        drop(reservation);
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
        let tracing_query = purrdf_lex::json::Object::new()
            .with("store_id", receipt.store_id.clone())
            .with("source", "structured-sdk")
            .with("layers", purrdf_lex::json::Object::new().with("metadata", true).with("diagnostic", false).with("content", false))
            .with("query", "PREFIX s: <urn:sparrow:vocab:> SELECT ?kind ?span WHERE { ?record s:tracingState \"complete\" ; s:eventKind ?kind ; s:observedSpan ?span } ORDER BY ?kind");
        let tracing_path = dir.path().join("tracing-query.json");
        std::fs::write(&tracing_path, purrdf_lex::json::write_compact(&tracing_query.clone().into())).unwrap();
        let tracing_cli = cli(format!("tcp://{address}"), &["sparql", tracing_path.to_str().unwrap()]);
        let tracing_mcp = mcp.tool(91, "sparql_query", tracing_query.into()).await;
        assert_eq!(tracing_mcp["isError"].as_bool(), Some(false));
        assert_eq!(tracing_mcp["structuredContent"]["rows"], tracing_cli["rows"]);
        assert_eq!(tracing_cli["answer_certainty"].as_str(), Some("exact"));
        let tracing_rows = tracing_cli["rows"]["native_result"]["results"]["bindings"].as_array().unwrap();
        assert_eq!(tracing_rows.len(), 3);
        for (row, kind) in tracing_rows.iter().zip(["log", "span_close", "span_new"]) {
            assert_eq!(row["kind"]["value"].as_str(), Some(kind));
            assert_eq!(row["span"]["value"].as_str(), Some("urn:sparrow:span:09090909090909090909090909090909:42"));
        }
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
        request.get().set_schema_version(sparrow_client::SCHEMA_VERSION);
        request.get().set_source("refused-wire-quota");
        let response = request.send().promise.await.unwrap();
        assert!(matches!(rpc_ok(response.get().unwrap().get_result().unwrap()).which().unwrap(), wire::optional_source_time_profile::Absent(())));
        let mut request = producer.capability().source_time_define_request();
        let mut definition = request.get().init_definition();
        definition.set_schema_version(sparrow_client::SCHEMA_VERSION);
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
        // #215: with every producer window taken, a batch from a new instance is still stored and
        // status names the stream whose gap evidence was lost. The old daemon refused it with
        // RESOURCE_LIMIT, so this fails there at the overflow send and at the status fields.
        for index in 0..63_u8 {
            let mut fill = batch.clone();
            fill.request_id = format!("fill-{index}"); fill.source = "fill".into();
            fill.client_instance = [100 + index; 16]; fill.events = vec![batch.events[0].clone()];
            producer.events(&fill).await.unwrap();
        }
        let mut late = batch.clone();
        late.request_id = "overflow".into(); late.source = "overflow-source".into();
        late.client_instance = [200; 16];
        late.events = vec![batch.events[0].clone(), batch.events[2].clone()];
        let stored = producer.events(&late).await.unwrap();
        assert_eq!(stored.record_ids.len(), 2);
        let response = producer.capability().status_request().send().promise.await.unwrap();
        let status = rpc_ok(response.get().unwrap().get_result().unwrap());
        let loss = status.get_stream_evidence_loss().unwrap();
        let named = loss.get_named().unwrap();
        assert_eq!(named.len(), 1);
        let row = named.get(0);
        assert_eq!(row.get_kind().unwrap(), wire::LostStreamKind::Producer);
        assert_eq!(row.get_source().unwrap().to_str().unwrap(), "overflow-source");
        assert_eq!(row.get_instance().unwrap().to_str().unwrap(), "c8".repeat(16));
        assert_eq!((row.get_batches(), row.get_events()), (1, 2));
        assert_eq!((loss.get_total_batches(), loss.get_total_events()), (1, 2));
        assert_eq!((loss.get_other_streams(), loss.get_other_events()), (0, 0));
        let rendered = cli(&socket, &["status"]);
        let report = &rendered["stream_evidence_loss"];
        assert_eq!(report["named"][0]["kind"].as_str(), Some("producer"));
        assert_eq!(report["named"][0]["source"].as_str(), Some("overflow-source"));
        assert_eq!(report["named"][0]["instance"].as_str(), Some("c8".repeat(16).as_str()));
        assert_eq!(report["named"][0]["batches"].as_str(), Some("1"));
        assert_eq!(report["named"][0]["events"].as_str(), Some("2"));
        assert_eq!(report["total_events"].as_str(), Some("2"));
        // #224: the tail registry evidence is on the wire and in the rendered status, empty
        // while no file was tailed without a stored checkpoint.
        let tail = status.get_tail_registry_evidence().unwrap();
        assert_eq!(tail.get_max_named(), sparrow_core::MAX_NAMED_UNRECORDED_TAILS as u32);
        assert_eq!((tail.get_total_records(), tail.get_retired_terminal()), (0, 0));
        let tail = &rendered["tail_registry_evidence"];
        assert_eq!(tail["named"].as_array().map(Vec::len), Some(0));
        assert_eq!(tail["other_files_exact"].as_bool(), Some(true));
        assert_eq!(tail["total_bytes"].as_str(), Some("0"));
        // #227: the source-time loss field is on the wire and in the rendered status, empty
        // while every source got a mapping (the 1024-source ceiling is tested in sparrow-core).
        let source_time = status.get_source_time_loss().unwrap();
        assert_eq!((source_time.get_total_batches(), source_time.get_total_events()), (0, 0));
        assert!(source_time.get_other_streams_exact());
        assert_eq!(rendered["source_time_loss"]["named"].as_array().map(Vec::len), Some(0));
        assert_eq!(rendered["source_time_loss"]["total_events"].as_str(), Some("0"));
        drop(producer); drop(daemon);
    }).await;
}

async fn exercise(endpoint: impl AsRef<std::ffi::OsStr>) -> (String, String, String) {
    let endpoint = endpoint.as_ref();
    let target = test_endpoint(endpoint);
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
    let overwrite = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
        .args(endpoint_args(endpoint))
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
    let invalid = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
        .args(endpoint_args(endpoint))
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
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
            .args(endpoint_args(socket))
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
    assert_eq!(definitions.len(), 40);
    for (name, read_only) in [
        ("sparql_explain", true),
        ("correlate", true),
        ("retention_metrics", true),
        ("ingest_get", true),
        ("file_finish", false),
        ("file_finish_get", true),
        ("watch_handoff", false),
        ("watch_handoff_get", true),
        ("watch_claim_retire", false),
        ("result_release", false),
        ("syslog_listener_get", true),
        ("receiver_window_get", true),
        ("payload_lookup", true),
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
        Some("EVIDENCE_INCOMPLETE")
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
    // The later source time and both receipt-time fallbacks (received now) lie outside.
    assert_eq!(
        event_cli["event_coverage"]["outside_interval"].as_str(),
        Some("3")
    );
    assert_eq!(event_cli["time_basis"].as_str(), Some("event_time"));
    assert_eq!(event_cli["event_time_fallback"].as_str(), Some("0"));
    assert_eq!(
        event_cli["rows"][0]["event_time"]["instant"].as_str(),
        Some("1970-01-01T00:00:01Z")
    );
    assert_eq!(
        event_cli["rows"][0]["event_time"]["basis"].as_str(),
        Some("source")
    );
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let fallback_cli = cli(
        socket,
        &[
            "records",
            "--source",
            "rpc-events",
            "--event-from",
            &sparrow_core::format_instant(now - 3_600_000).unwrap(),
            "--event-before",
            &sparrow_core::format_instant(now + 3_600_000).unwrap(),
        ],
    );
    assert_eq!(fallback_cli["event_time_fallback"].as_str(), Some("2"));
    for row in fallback_cli["rows"].as_array().unwrap() {
        let event = &row["event_time"];
        assert_eq!(event["basis"].as_str(), Some("receipt"));
        assert_eq!(event["state"].as_str(), Some("unknown"));
        assert_eq!(event["fallback_instant"], row["received_at"]);
        assert!(event["instant"].is_null());
        assert!(event["reason"].as_str().is_some());
    }
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
        let rejected = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
            .args(endpoint_args(socket))
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
                    tcp_client(address).await.unwrap()
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
    let output = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
        .args(endpoint_args(socket))
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
async fn correlation_cli_mcp_and_typed_pages_retain_native_certainty_and_pin_scopes() {
    use purrdf_lex::json::{self, Object};
    use sparrow_core::DiagnosticDimension;
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let owner = sparrow_server::ServerConfig::read(&config)
                .unwrap()
                .open_store()
                .unwrap();
            let old = sparrow_server::now().unwrap() - 31 * 86400000;
            let mut targets = Vec::new();
            for source in ["correlate-a", "correlate-b"] {
                owner
                    .define_diagnostic_profile(
                        source,
                        [(DiagnosticDimension::TraceId, "/trace".into())]
                            .into_iter()
                            .collect(),
                    )
                    .unwrap();
                targets.push(
                    owner
                        .ingest(source, source, "jsonl", b"{\"trace\":\"shared\"}\n", old)
                        .unwrap()
                        .id,
                );
            }
            targets.push(
                owner
                    .ingest(
                        "correlate-unprofiled",
                        "correlate-unprofiled",
                        "log",
                        b"unknown trace\n",
                        old,
                    )
                    .unwrap()
                    .id,
            );
            let pin = owner
                .pin("correlate-pin", "correlation", &targets, 40, old)
                .unwrap();
            drop(owner);
            let (address, reservation) = configure_tcp_listener(&config);
            drop(reservation);
            let _daemon = start(&config, &socket).await;
            let endpoint = format!("tcp://{address}");
            let client = sparrow_client::Client::connect_endpoint(
                &test_endpoint(&endpoint),
            )
            .await
            .unwrap();
            let store = client.discovery().store_id.clone();
            let args = Object::new()
                .with("store_id", store.clone())
                .with(
                    "scope",
                    Object::new()
                        .with("kind", "pin")
                        .with("store_id", store.clone())
                        .with("pin_id", pin.id.clone()),
                )
                .with(
                    "selector",
                    Object::new()
                        .with("kind", "diagnostic")
                        .with("dimension", "trace_id")
                        .with("value", "shared"),
                )
                .with("relation", "shared_trace_id")
                .with("page_size", 1_u32);
            let path = dir.path().join("correlate.json");
            std::fs::write(&path, json::write_compact(&args.clone().into())).unwrap();
            let first = cli(&endpoint, &["correlate", path.to_str().unwrap()]);
            assert_eq!(first["operation"].as_str(), Some("correlate"));
            assert_eq!(first["execution_status"].as_str(), Some("complete"));
            assert_eq!(first["answer_certainty"].as_str(), Some("exact"));
            assert_eq!(first["coverage"]["complete"].as_bool(), Some(false));
            assert_eq!(
                first["interpretations"]["diagnostics"]["trace_id"]["unknown"].as_str(),
                Some("1")
            );
            assert_eq!(
                first["correlation"]["profile"].as_str(),
                Some(sparrow_core::CORRELATION_PROFILE)
            );
            assert_eq!(first["scope"]["pin"]["revision"].as_str(), Some("1"));
            assert!(first["has_more"].as_bool().unwrap());
            let mut mcp = Mcp::start(&socket);
            mcp.initialize(1).await;
            let captured = mcp.tool(2, "correlate", args.clone().into()).await;
            assert_eq!(captured["isError"].as_bool(), Some(false), "{captured:?}");
            let captured = &captured["structuredContent"];
            assert_eq!(captured["scope"], first["scope"]);
            assert_eq!(captured["correlation"], first["correlation"]);
            assert_eq!(captured["rows"], first["rows"]);
            let strict = mcp
                .tool(
                    3,
                    "correlate",
                    args.clone().with("require_complete_index", true).into(),
                )
                .await;
            assert_eq!(strict["isError"].as_bool(), Some(true));
            assert_eq!(
                strict["structuredContent"]["error"]["code"].as_str(),
                Some("EVIDENCE_INCOMPLETE")
            );
            let mut partials = 0;
            for (index, fuel) in [0_u64, 1, 3, 10, 25].into_iter().enumerate() {
                let value = mcp
                    .tool(
                        10 + index as u64,
                        "correlate",
                        args.clone()
                            .with("limits", Object::new().with("fuel", fuel.to_string()))
                            .into(),
                    )
                    .await;
                assert_eq!(value["isError"].as_bool(), Some(false), "{value:?}");
                let value = &value["structuredContent"];
                assert_eq!(value["operation"].as_str(), Some("correlate"));
                assert_eq!(value["correlation"], first["correlation"]);
                if value["execution_status"].as_str() == Some("partial") {
                    partials += 1;
                    match value["answer_certainty"].as_str().unwrap() {
                        "at_most" => {
                            assert!(value.get("rows").is_none());
                            assert!(value.get("candidates").is_some());
                        }
                        "unknown" => {
                            assert!(value.get("rows").is_none());
                            assert!(value.get("candidates").is_none());
                        }
                        "certain" => assert!(value.get("rows").is_some()),
                        _ => panic!("native partial certificate required"),
                    }
                }
                if value["has_more"].as_bool() == Some(true) {
                    client
                        .release_result(value["snapshot_id"].as_str().unwrap())
                        .await
                        .unwrap();
                }
            }
            assert!(partials > 0);
            cli(
                &endpoint,
                &["pin-release", "correlate-release", &store, &pin.id],
            );
            let cursor = first["next_cursor"].as_str().unwrap();
            let second = cli(&endpoint, &["result-next", &store, cursor]);
            let replay = mcp
                .tool(
                    30,
                    "result_next",
                    Object::new()
                        .with("store_id", store.clone())
                        .with("cursor", cursor)
                        .into(),
                )
                .await;
            assert_eq!(replay["structuredContent"], second);
            assert_eq!(second["scope"], first["scope"]);
            assert_eq!(second["correlation"], first["correlation"]);
            assert_eq!(second["operation"].as_str(), Some("correlate"));
            client
                .release_result(first["snapshot_id"].as_str().unwrap())
                .await
                .unwrap();
            client
                .release_result(captured["snapshot_id"].as_str().unwrap())
                .await
                .unwrap();
            // Direct typed callers use the same validated store and finite governors.
            let mut call = client.capability().correlate_request();
            let mut query = call.get().init_query();
            query.set_schema_version(sparrow_client::SCHEMA_VERSION);
            query.set_store_id(&store);
            {
                let mut scope = query.reborrow().init_scope();
                scope.set_schema_version(sparrow_client::SCHEMA_VERSION);
                scope.init_received_before().set_unix_millis(u64::MAX);
            }
            let mut ids = query.reborrow().init_selector().init_evidence(1);
            let mut id = ids.reborrow().get(0);
            id.set_store_id("store:foreign");
            id.set_record("record:unknown");
            query.init_relation().set_source(());
            let response = call.send().promise.await.unwrap();
            assert!(matches!(response.get().unwrap().get_result().unwrap().which().unwrap(), wire::result::Error(error) if error.as_ref().unwrap().get_code().unwrap().to_str().unwrap() == "STORE_MISMATCH"));
            assert_eq!(
                cli(&endpoint, &["status"])["sparql_read_handles"].as_str(),
                Some("0")
            );
        })
        .await;
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
            assert_eq!(cli["scope"], mcp["scope"]);
            assert_eq!(cli["scope"]["selection"].as_str(), Some("evidence"));
            assert_eq!(cli["scope"]["direct_members"].as_str(), Some("1"));
            assert_eq!(cli["scope"]["expanded_records"].as_str(), Some("0"));
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
        let client = sparrow_client::Client::connect_endpoint(&test_endpoint(&endpoint)).await.unwrap();
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
            assert_eq!(first["scope"]["selection"].as_str(),Some("live"));
            assert!(first["scope"]["direct_members"].is_null());
            assert!(first["scope"]["expanded_records"].is_null());
            assert_eq!(first["limits"]["scope_receipt_bytes"].as_str(),Some(sparrow_core::QUERY_SCOPE_WORKSPACE_BYTES.to_string().as_str()));
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
                assert_eq!(page["scope"],first["scope"]);
                assert_eq!(page["snapshot_id"],first["snapshot_id"]); assert_eq!(page["native_governor"],first["native_governor"]); pages+=1; assert!(pages<100);
            }
            let released = sparrow_client::release_result(client.capability(),&store,first["snapshot_id"].as_str().unwrap()).await.unwrap(); assert_eq!(released.released,first["has_more"].as_bool().unwrap());
            if let Some(cursor) = first["next_cursor"].as_str() { assert!(sparrow_access::run_cli(&client,&["result-next".into(),store.clone(),cursor.into()]).await.is_err()); }
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
        let output = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture")).args(endpoint_args(&endpoint)).args(["sparql",path.to_str().unwrap()]).admitted_output().unwrap();
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
async fn stored_text_graph_relations_preserve_corpus_spans_and_explanations_over_transports() {
    use purrdf_lex::json::{self, Object, Value};
    tokio::task::LocalSet::new().run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let socket = data.join("rpc.sock");
        let config = write_server_config(dir.path(), &data, &socket);
        let (address, reservation) = configure_tcp_listener(&config);
        drop(reservation);
        let _daemon = start(&config, &socket).await;
        let endpoint = format!("tcp://{address}");
        let client = sparrow_client::Client::connect_endpoint(
            &test_endpoint(&endpoint)
        ).await.unwrap();
        let store = client.discovery().store_id.clone();
        let input = br#"{"message":"nee\u0064le needle"}
{"message":"needle more words"}
"#;
        let artifact = client.ingest("text-graph", "text-graph-source", wire::Format::Jsonl, input).await.unwrap();
        client.ingest("text-outside", "outside", wire::Format::Log, b"needle needle needle\n").await.unwrap();
        let mut mcp = Mcp::start(&socket);
        mcp.initialize(1).await;
        let rank = "SELECT ?doc ?id ?score ?rank ?matched WHERE { ?doc <urn:test:rank> (\"needle\" ?id ?score ?rank ?matched); a <urn:sparrow:vocab:Record> } ORDER BY ?rank";
        let request = Object::new().with("store_id", store.clone())
            .with("source", "text-graph-source").with("query", rank)
            .with("layers", Object::new().with("metadata", true).with("diagnostic", false).with("content", false))
            .with("text_relations", Object::new().with("ranked_iri", "urn:test:rank").with("spans_iri", "urn:test:spans"))
            .with("page_size", 1_u32);
        let path = dir.path().join("text-graph.json");
        std::fs::write(&path, json::write_compact(&request.clone().into())).unwrap();
        let first = cli(&endpoint, &["sparql", path.to_str().unwrap()]);
        let response = mcp.tool(2, "sparql_query", request.clone().into()).await;
        assert_eq!(response["isError"].as_bool(), Some(false), "{response:?}");
        let other = &response["structuredContent"];
        for key in ["rows", "text_corpus", "scope", "coverage", "native_governor", "execution_status", "answer_certainty"] {
            assert_eq!(first[key], other[key], "CLI/MCP {key}");
        }
        assert_eq!(first["execution_status"].as_str(), Some("complete"), "{first:?}");
        assert_eq!(first["answer_certainty"].as_str(), Some("exact"));
        assert_eq!(first["text_corpus"]["documents"].as_str(), Some("2"));
        assert_eq!(first["text_corpus"]["coverage"]["ready"].as_str(), Some("2"));
        assert_eq!(first["text_corpus"]["membership_complete"].as_bool(), Some(true));
        assert_eq!(first["total_items"].as_str(), Some("2"));
        client.ingest("text-later", "text-graph-source", wire::Format::Log, b"needle\n").await.unwrap();
        let cursor = first["next_cursor"].as_str().unwrap();
        let next = cli(&endpoint, &["result-next", &store, cursor]);
        let replay = mcp.tool(3, "result_next", Object::new().with("store_id", store.clone()).with("cursor", cursor).into()).await;
        assert_eq!(replay["isError"].as_bool(), Some(false), "{replay:?}");
        assert_eq!(next, replay["structuredContent"]);
        for key in ["text_corpus", "scope", "coverage", "native_governor", "sequence", "lease_expires_at"] {
            assert_eq!(next[key], first[key], "frozen {key}");
        }
        assert_eq!(next["rows"]["native_result"]["results"]["bindings"][0]["rank"]["value"].as_str(), Some("2"));
        for page in [&first, other] {
            assert!(sparrow_client::release_result(client.capability(), &store, page["snapshot_id"].as_str().unwrap()).await.unwrap().released);
        }

        let spans = "SELECT ?id ?term ?start ?end ?basis ?path ?coarse WHERE { ?doc <urn:test:spans> (\"needle\" ?id ?term ?start ?end ?basis ?path ?coarse) }";
        let span_request = request.clone().with("query", spans).with("page_size", 100_u32);
        let response = mcp.tool(4, "sparql_query", span_request.into()).await;
        assert_eq!(response["isError"].as_bool(), Some(false), "{response:?}");
        let result = &response["structuredContent"];
        assert_eq!(result["text_corpus"]["documents"].as_str(), Some("3"));
        assert_ne!(result["text_corpus"]["corpus_id"], first["text_corpus"]["corpus_id"]);
        let rows = result["rows"]["native_result"]["results"]["bindings"].as_array().unwrap();
        let mut decoded = 0;
        for row in rows {
            let start = row["start"]["value"].as_str().unwrap().parse::<u64>().unwrap();
            let end = row["end"]["value"].as_str().unwrap().parse::<u64>().unwrap();
            let bytes = mcp.tool(5, "bytes_read", Object::new().with("store_id", store.clone())
                .with("id", row["id"]["value"].as_str().unwrap()).with("offset", start.to_string()).with("length", end-start).into()).await;
            assert_eq!(bytes["isError"].as_bool(), Some(false), "{bytes:?}");
            if row["basis"]["value"].as_str() == Some("decoded_json_scalar") {
                decoded += 1;
                assert_eq!(row["path"]["value"].as_str(), Some("/message"));
                if row["coarse"]["value"].as_str() == Some("true") {
                    let expected: &[u8] = if row["id"]["value"].as_str() == Some(artifact.record_ids[0].as_str()) {
                        br#"nee\u0064le needle"#
                    } else {
                        assert_eq!(row["id"]["value"].as_str(), Some(artifact.record_ids[1].as_str()));
                        b"needle more words"
                    };
                    assert_eq!(bytes["structuredContent"]["data"].as_str(), Some(purrdf::xsd::canonical_base64(expected).as_str()));
                } else {
                    assert_eq!(bytes["structuredContent"]["data"].as_str(), Some(purrdf::xsd::canonical_base64(b"needle").as_str()));
                }
            } else {
                assert_eq!(row["basis"]["value"].as_str(), Some("raw_record"));
                assert_eq!(bytes["structuredContent"]["data"].as_str(), Some(purrdf::xsd::canonical_base64(b"needle").as_str()));
            }
        }
        assert_eq!(decoded, 3);
        sparrow_client::release_result(client.capability(), &store, result["snapshot_id"].as_str().unwrap()).await.unwrap();

        std::fs::write(&path, json::write_compact(&request.clone().into())).unwrap();
        let first = cli(&endpoint, &["sparql-explain", path.to_str().unwrap()]);
        assert_eq!(first["execution_status"].as_str(), Some("measured"));
        let mut page = first.clone();
        let mut descriptors = Vec::new();
        let mut pages = 0;
        loop {
            pages += 1;
            assert!(pages < 100);
            assert_eq!(page["text_corpus"], first["text_corpus"]);
            assert_eq!(page["explanation"]["relations"].as_array().unwrap().len(), 2);
            for item in page["explanation"]["items"].as_array().unwrap() {
                if item["kind"].as_str() == Some("relation") { descriptors.push(item.clone()); }
            }
            let Some(cursor) = page["next_cursor"].as_str() else { break; };
            let next = mcp.tool(6, "result_next", Object::new().with("store_id", store.clone()).with("cursor", cursor).into()).await;
            assert_eq!(next["isError"].as_bool(), Some(false), "{next:?}");
            assert_eq!(next["structuredContent"], cli(&endpoint, &["result-next", &store, cursor]));
            page = next["structuredContent"].clone();
        }
        assert_eq!(descriptors.len(), 2);
        for (descriptor, iri, object_arity) in [(&descriptors[0], "urn:test:rank", 5_u32), (&descriptors[1], "urn:test:spans", 8_u32)] {
            assert_eq!(descriptor["iri"].as_str(), Some(iri));
            assert_eq!(descriptor["subject_arity"], Value::from(1_u32));
            assert_eq!(descriptor["object_arity"], Value::from(object_arity));
            assert!(!descriptor["modes"].as_array().unwrap().is_empty());
        }
        assert!(sparrow_client::release_result(client.capability(), &store, first["snapshot_id"].as_str().unwrap()).await.unwrap().released);
        client.ingest("text-incomplete", "text-incomplete-source", wire::Format::Json, br#"{"message":"needle""#).await.unwrap();
        let incomplete = request.clone().with("source", "text-incomplete-source");
        let response = mcp.tool(7, "sparql_query", incomplete.clone().into()).await;
        assert_eq!(response["isError"].as_bool(), Some(false), "{response:?}");
        let result = &response["structuredContent"];
        assert_eq!(result["answer_certainty"].as_str(), Some("exact"));
        assert_eq!(result["coverage"]["complete"].as_bool(), Some(false));
        assert_eq!(result["text_corpus"]["coverage"]["failed_or_incomplete"].as_str(), Some("1"));
        assert_eq!(result["text_corpus"]["membership_complete"].as_bool(), Some(false));
        sparrow_client::release_result(client.capability(), &store, result["snapshot_id"].as_str().unwrap()).await.unwrap();
        let response = mcp.tool(8, "sparql_query", incomplete.with("require_complete_index", true).into()).await;
        assert_eq!(response["isError"].as_bool(), Some(true), "{response:?}");
        assert_eq!(response["structuredContent"]["error"]["code"].as_str(), Some("EVIDENCE_INCOMPLETE"));
        for invalid in [
            Object::new().with("ranked_iri", "urn:same").with("spans_iri", "urn:same"),
            Object::new().with("ranked_iri", "relative").with("spans_iri", "urn:spans"),
            Object::new().with("ranked_iri", "urn:rank").with("spans_iri", "urn:spans").with("invented", true),
        ] {
            let response = mcp.tool(7, "sparql_query", request.clone().with("text_relations", invalid).into()).await;
            assert_eq!(response["isError"].as_bool(), Some(true), "{response:?}");
        }
        let status = cli(&socket, &["status"]);
        assert_eq!(status["sparql_read_handles"].as_str(), Some("0"));
        assert_eq!(status["sparql_read_bytes"].as_str(), Some("0"));
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_explanation_preserves_measured_work_over_tcp_cli_mcp_and_continuation() {
    use purrdf_lex::json::{self, Object, Value};
    tokio::task::LocalSet::new().run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let socket = data.join("rpc.sock");
        let config = write_server_config(dir.path(), &data, &socket);
        let (address, reservation) = configure_tcp_listener(&config);
        drop(reservation);
        let _daemon = start(&config, &socket).await;
        let endpoint = format!("tcp://{address}");
        let client = sparrow_client::Client::connect(&socket).await.unwrap();
        let store = client.discovery().store_id.clone();
        client.ingest("explain-input", "explain-source", wire::Format::Jsonl, b"{}\n{}\n").await.unwrap();
        let mut mcp = Mcp::start(&socket);
        mcp.initialize(1).await;
        let request = Object::new().with("store_id", store.clone()).with("source", "explain-source")
            .with("query", "SELECT ?record ?payload WHERE { ?record a <urn:sparrow:vocab:Record>; ?predicate ?payload FILTER(?record != <urn:absent>) }")
            .with("bindings", Value::array([Object::new().with("name", "predicate")
                .with("term", Object::new().with("kind", "iri").with("value", "urn:sparrow:vocab:payload"))]))
            .with("layers", Object::new().with("metadata", true).with("diagnostic", false).with("content", false))
            .with("page_size", 1_u32);
        let path = dir.path().join("explain.json");
        std::fs::write(&path, json::write_compact(&request.clone().into())).unwrap();
        let tcp = cli(&endpoint, &["sparql-explain", path.to_str().unwrap()]);
        let response = mcp.tool(2, "sparql_explain", request.clone().into()).await;
        assert_eq!(response["isError"].as_bool(), Some(false), "{response:?}");
        let first = &response["structuredContent"];
        for key in ["explanation", "native_governor", "coverage", "scope", "layers", "limits", "execution_status"] {
            assert_eq!(tcp[key], first[key], "CLI/MCP explanation field {key}");
        }
        assert_eq!(first["operation"].as_str(), Some("sparql_explain"));
        assert_eq!(first["execution_status"].as_str(), Some("measured"));
        assert!(first.get("answer_certainty").is_none());
        assert_eq!(first["explanation"]["items"][0]["rows"].as_str(), Some("2"), "typed predicate substitution must restrict the measured relation");
        let snapshot = first["snapshot_id"].as_str().unwrap().to_owned();
        let mut page = first.clone();
        let mut count = 0_u64;
        let mut fuel = 0_u64;
        let mut pages = 0;
        loop {
            pages += 1;
            assert!(json::write_compact(&page).len() <= 256 * 1024);
            assert_eq!(page["scope"], first["scope"]);
            assert_eq!(page["lease_expires_at"], first["lease_expires_at"]);
            assert_eq!(page["snapshot_id"].as_str(), Some(snapshot.as_str()));
            let items = page["explanation"]["items"].as_array().unwrap();
            count += items.len() as u64;
            for item in items {
                if item["kind"].as_str() == Some("node") {
                    fuel += item["fuel_total"].as_str().unwrap().parse::<u64>().unwrap();
                }
            }
            let Some(cursor) = page["next_cursor"].as_str() else { break; };
            let next = cli(&socket, &["result-next", &store, cursor]);
            let replay = cli(&socket, &["result-next", &store, cursor]);
            assert_eq!(next, replay);
            page = next;
            assert!(pages < 100);
        }
        assert!(pages > 1);
        assert_eq!(count.to_string(), first["total_items"].as_str().unwrap());
        let resources = first["native_governor"]["resources"].as_array().unwrap();
        let native_fuel = resources.iter().find(|row| row["dimension"].as_str() == Some("fuel")).unwrap();
        assert_eq!(fuel.to_string(), native_fuel["consumed"].as_str().unwrap());
        assert!(sparrow_client::release_result(client.capability(), &store, &snapshot).await.unwrap().released);
        assert!(sparrow_client::release_result(client.capability(), &store, tcp["snapshot_id"].as_str().unwrap()).await.unwrap().released);
        let stopped = request.clone().with("limits", Object::new().with("fuel", "0"));
        let response = mcp.tool(3, "sparql_explain", stopped.into()).await;
        let stopped = &response["structuredContent"];
        assert_eq!(stopped["execution_status"].as_str(), Some("stopped"));
        assert!(stopped.get("answer_certainty").is_none());
        assert_ne!(stopped["stop_reason"], Value::Null);
        let stopped_id = stopped["snapshot_id"].as_str().unwrap();
        // An empty/single-page report need not retain a continuation handle.
        sparrow_client::release_result(client.capability(), &store, stopped_id).await.unwrap();
        let invalid = request.with("query", "INSERT DATA { <urn:s> <urn:p> <urn:o> }");
        let response = mcp.tool(4, "sparql_explain", invalid.into()).await;
        let invalid = &response["structuredContent"];
        assert_eq!(invalid["operation"].as_str(), Some("sparql_explain"));
        assert_eq!(invalid["execution_status"].as_str(), Some("failed"));
        assert!(invalid.get("answer_certainty").is_none());
        assert!(invalid.get("diagnostic").is_some());
        assert_eq!(cli(&socket, &["status"])["sparql_read_handles"].as_str(), Some("0"));
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
            let expired = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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
            let output = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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
            assert!(String::from_utf8_lossy(&output.stderr).contains("EVIDENCE_INCOMPLETE"));
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
            let frozen = &first["receipt"]["scope"];
            assert_eq!(
                frozen["profile"].as_str(),
                Some(sparrow_core::QUERY_SCOPE_PROFILE)
            );
            assert_eq!(frozen["selection"].as_str(), Some("pin"));
            assert_eq!(frozen["pin"]["id"].as_str(), Some(pin.id.as_str()));
            assert_eq!(
                frozen["pin"]["revision"].as_str(),
                Some(pin.revision.to_string().as_str())
            );
            assert_eq!(
                frozen["pin"]["expires_at_ms"].as_str(),
                Some(pin.expires_at.to_string().as_str())
            );
            assert_eq!(frozen["direct_members"].as_str(), Some("1"));
            assert_eq!(frozen["expanded_records"].as_str(), Some("5"));
            assert_eq!(
                frozen["received_before_ms"].as_str(),
                Some(u64::MAX.to_string().as_str())
            );
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
                let output = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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
            let output = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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
                let output = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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
                let output = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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

/// A delivered live artifact continuation retires the handle it continued, so
/// each surface continues its own first page and the spent cursor then expires.
async fn live_artifact_continuations(
    socket: &std::path::Path,
    client: &sparrow_client::Client,
    mcp: &mut Mcp,
    args: &[&str],
) {
    use purrdf_lex::json::Object;
    let id = args[1];
    let mut continued = Vec::new();
    for surface in 0_u64..4 {
        let first = cli(socket, args);
        let snapshot = first["snapshot_id"].as_str().unwrap().to_owned();
        let cursor = first["next_cursor"].as_str().unwrap().to_owned();
        let (rows, next_snapshot) = match surface {
            0 | 1 => {
                let next = if surface == 0 {
                    let mut repeat = args.to_vec();
                    repeat.extend(["--cursor", cursor.as_str()]);
                    cli(socket, &repeat)
                } else {
                    let next = cli(socket, &["result-next", id, &cursor]);
                    assert_eq!(next["operation"].as_str(), Some("artifacts_list"));
                    next
                };
                (next["rows"].clone(), next["snapshot_id"].clone())
            }
            2 => {
                let reply = mcp
                    .tool(
                        300 + surface,
                        "result_next",
                        Object::new()
                            .with("store_id", id)
                            .with("cursor", cursor.as_str())
                            .into(),
                    )
                    .await;
                assert!(!reply["isError"].as_bool().unwrap());
                let next = &reply["structuredContent"];
                (next["rows"].clone(), next["snapshot_id"].clone())
            }
            _ => {
                let mut call = client.capability().result_next_request();
                let mut query = call.get().init_query();
                query.set_store_id(id);
                query.set_schema_version(sparrow_client::SCHEMA_VERSION);
                query.set_cursor(&cursor);
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
                let wire::result_page::Artifacts(page) = page.which().unwrap() else {
                    panic!("artifact continuation");
                };
                let page = page.unwrap();
                let rows = page.get_rows().unwrap();
                assert_eq!(rows.len(), 1);
                let artifact = rows.get(0).get_id().unwrap().to_str().unwrap();
                let snapshot = page
                    .get_receipt()
                    .unwrap()
                    .get_snapshot_id()
                    .unwrap()
                    .to_str()
                    .unwrap();
                (
                    purrdf_lex::json::Value::from(artifact),
                    purrdf_lex::json::Value::from(snapshot),
                )
            }
        };
        assert_ne!(next_snapshot.as_str(), Some(snapshot.as_str()));
        continued.push(rows);
        let out = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
            .arg(socket)
            .args(["result-next", id, &cursor])
            .admitted_output()
            .unwrap();
        assert!(!out.status.success() && out.stdout.is_empty());
        assert!(String::from_utf8_lossy(&out.stderr).contains("RESULT_EXPIRED"));
        // The final continuation holds no handle, and delivery retired the first.
        for snapshot in [snapshot.as_str(), next_snapshot.as_str().unwrap()] {
            assert!(!client.release_result(snapshot).await.unwrap().released);
        }
    }
    for rows in &continued[1..3] {
        assert_eq!(rows, &continued[0]);
    }
    assert_eq!(
        continued[3].as_str(),
        continued[0][0]["artifact_id"].as_str()
    );
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
                if args[0] == "artifacts" {
                    live_artifact_continuations(&socket, &client, &mut mcp, &args).await;
                    continue;
                }
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
                let out = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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
                let out = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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
            assert_eq!(
                partial["receipt"]["scope"]["selection"].as_str(),
                Some("evidence")
            );
            assert_eq!(
                partial["receipt"]["scope"]["direct_members"].as_str(),
                Some("1")
            );
            assert_eq!(
                partial["receipt"]["scope"]["expanded_records"].as_str(),
                Some("1")
            );
            assert_eq!(
                partial["receipt"]["limits"]["scope_receipt_bytes"].as_str(),
                Some(
                    sparrow_core::QUERY_SCOPE_WORKSPACE_BYTES
                        .to_string()
                        .as_str()
                )
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
            let output = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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
            assert_eq!(
                same["structuredContent"]["receipt"]["scope"],
                first["receipt"]["scope"]
            );
            assert_eq!(
                first["receipt"]["scope"]["selection"].as_str(),
                Some("live")
            );
            assert!(first["receipt"]["scope"]["expanded_records"].is_null());
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
                let output = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
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

/// INVARIANT: a source listing reads a snapshot taken under the writer lock and releases the
/// lock before it reads any row, so an ingest issued while the listing is still reading
/// completes before the listing finishes, and the listing answers from its snapshot.
#[tokio::test(flavor = "current_thread")]
async fn sources_listing_does_not_hold_the_writer_lock() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use sparrow_server::{Rpc, ServerConfig};
            use std::sync::{Arc, Mutex, mpsc};
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config =
                ServerConfig::read(&write_server_config(dir.path(), &data, &socket)).unwrap();
            let store = config.open_store().unwrap();
            store
                .ingest(
                    "before-listing",
                    "listed",
                    "jsonl",
                    b"{}\n",
                    sparrow_server::now().unwrap(),
                )
                .unwrap();
            let (paused, reached) = mpsc::channel();
            let (release, released) = mpsc::channel::<()>();
            let released = Mutex::new(released);
            store
                .pause_source_reads_for_tests(move || {
                    // Later listings find the channels closed and do not wait.
                    if paused.send(()).is_ok() {
                        let _ = released.lock().unwrap().recv();
                    }
                })
                .unwrap();
            let listener = tokio::net::UnixListener::bind(&socket).unwrap();
            let capability: wire::sparrow::Client = capnp_rpc::new_client(Rpc::new(
                store.clone(),
                Arc::new(tokio::sync::Semaphore::new(4)),
                config.max_pin_members,
            ));
            let _serving = tokio::task::spawn_local(async move {
                loop {
                    let (stream, _) = listener.accept().await.unwrap();
                    tokio::task::spawn_local(sparrow_server::serve_connection(
                        stream,
                        capability.clone(),
                    ));
                }
            });
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let mut request = client.capability().sources_list_request();
            let mut query = request.get().init_query();
            query.set_store_id(&client.discovery().store_id);
            query.set_schema_version(sparrow_client::SCHEMA_VERSION);
            query.set_page_size(200);
            query.set_scan_budget(10000);
            let listing = tokio::task::spawn_local(request.send().promise);
            tokio::task::spawn_blocking(move || reached.recv())
                .await
                .unwrap()
                .unwrap();
            tokio::time::timeout(
                Duration::from_secs(30),
                client.ingest(
                    "during-listing",
                    "ingested-meanwhile",
                    wire::Format::Jsonl,
                    b"{}\n",
                ),
            )
            .await
            .expect("an ingest completes while a listing is reading")
            .unwrap();
            assert!(!listing.is_finished(), "the listing is still reading");
            release.send(()).unwrap();
            let response = listing.await.unwrap().unwrap();
            let rows = rpc_ok(response.get().unwrap().get_result().unwrap())
                .get_rows()
                .unwrap();
            let sources = rows
                .iter()
                .map(|row| row.get_source_id().unwrap().to_str().unwrap().to_owned())
                .collect::<Vec<_>>();
            assert_eq!(sources, ["listed"], "the listing answers from its snapshot");
        })
        .await;
}

/// Value (#371): a daemon configured with `instance_role = "scratch"` reports `scratch` in both
/// `discover` and `status`, which is what a sender that must refuse the live store reads.
#[tokio::test(flavor = "current_thread")]
async fn discover_and_status_report_the_configured_instance_role() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let mut text = std::fs::read_to_string(&config).unwrap();
            text.push_str("instance_role = \"scratch\"\n");
            std::fs::write(&config, text).unwrap();
            let _daemon = start(&config, &socket).await;
            let connection = connect(&socket).await.unwrap();
            let client = connection.client.clone();
            let discovered = client.discover_request().send().promise.await.unwrap();
            let discovery = rpc_ok(discovered.get().unwrap().get_result().unwrap());
            assert_eq!(
                discovery.get_instance_role().unwrap(),
                wire::InstanceRole::Scratch
            );
            let status = client.status_request().send().promise.await.unwrap();
            let status = rpc_ok(status.get().unwrap().get_result().unwrap());
            assert_eq!(
                status.get_instance_role().unwrap(),
                wire::InstanceRole::Scratch
            );
        })
        .await;
}

/// Value (#377): `discover` answers on the ingest socket with the store id and the instance role,
/// the one guard a sender has against sending test cruft to the live store. A store created with
/// `instance_role = "scratch"` reads `scratch` there; a store with no role reads `unspecified`,
/// which a sender refuses like `production`.
#[tokio::test(flavor = "current_thread")]
async fn the_ingest_socket_discover_reports_the_instance_role_and_store() {
    tokio::task::LocalSet::new()
        .run_until(async {
            for (configured, expected) in [(Some("scratch"), "scratch"), (None, "unspecified")] {
                let dir = tempfile::tempdir().unwrap();
                let data = dir.path().join("store");
                let socket = data.join("rpc.sock");
                let config = write_server_config(dir.path(), &data, &socket);
                let (ingest, _) = configure_ingest_socket(dir.path(), &config, 4);
                if let Some(role) = configured {
                    let base = std::fs::read_to_string(&config).unwrap();
                    std::fs::write(&config, format!("instance_role = \"{role}\"\n{base}")).unwrap();
                }
                let _daemon = start_with_ingest(&config, &socket, &ingest).await;
                let store = cli(&socket, &["status"])["store_id"].clone();
                let discovered = cli(&ingest, &["discover"]);
                assert_eq!(discovered["instance_role"].as_str(), Some(expected));
                assert_eq!(discovered["store_id"], store);
                assert_eq!(
                    discovered["producer_profiles"],
                    purrdf_lex::json::Value::array([sparrow_client::PRODUCER_PROFILE])
                );
            }
        })
        .await;
}

/// Start a daemon with the given `instance_role` (none for a store without a role) and return
/// it with its socket; the temporary directory lives as long as the returned guard.
async fn daemon_with_role(role: Option<&str>) -> (tempfile::TempDir, std::path::PathBuf, Daemon) {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("store");
    let socket = data.join("rpc.sock");
    let config = write_server_config(dir.path(), &data, &socket);
    if let Some(role) = role {
        let base = std::fs::read_to_string(&config).unwrap();
        std::fs::write(&config, format!("instance_role = \"{role}\"\n{base}")).unwrap();
    }
    let daemon = start(&config, &socket).await;
    (dir, socket, daemon)
}

/// Whether the daemon holds any record of the import `request_id`: none means no import call
/// ever reached it.
async fn import_reached(client: &sparrow_client::Client, request_id: &str) -> bool {
    let status = sparrow_client::get_ingest_status(
        client.capability(),
        &client.discovery().store_id,
        &sparrow_client::IngestSelector::Request(request_id.into()),
        Default::default(),
    )
    .await;
    !matches!(&status, Err(sparrow_client::Error::Domain { code, .. }) if code == "NOT_FOUND")
}

/// INVARIANT (#399): a caller that requires `scratch` (and a store id) is refused by a daemon
/// that reports `unspecified`, `production` or another store, before any import call reaches
/// it, and a matching scratch store passes. The check is the non-retryable `INSTANCE_MISMATCH`
/// naming what was required and what was reported, on both the free import and the client.
#[tokio::test(flavor = "current_thread")]
async fn a_required_instance_role_and_store_refuse_a_mismatched_daemon_before_any_import_call() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let scratch = sparrow_client::Expect::new(sparrow_client::RequiredRole::Scratch);
            let dir = tempfile::tempdir().unwrap();
            let input = dir.path().join("input.log");
            std::fs::write(&input, b"one line\n").unwrap();
            for (role, wrong_store) in [
                (None, false),
                (Some("production"), false),
                (Some("scratch"), true),
            ] {
                let (_keep, socket, _daemon) = daemon_with_role(role).await;
                let endpoint = sparrow_client::Endpoint::Unix(socket.clone());
                let genuine = sparrow_client::Client::connect(&socket).await.unwrap();
                let expect = if wrong_store {
                    scratch.clone().with_store_id("store:another")
                } else {
                    scratch.clone()
                };
                let error = sparrow_client::import_file_expecting(
                    &endpoint, &expect, "refused", "src", wire::Format::Log, &input,
                )
                .await
                .unwrap_err();
                let reported = role.unwrap_or("unspecified");
                assert!(
                    matches!(&error, sparrow_client::Error::Domain { code, message, retryable: false, .. }
                        if code == "INSTANCE_MISMATCH"
                            && message.contains("required instance_role scratch")
                            && message.contains(&format!("reports instance_role {reported}"))
                            && message.contains(&genuine.discovery().store_id)),
                    "{error}"
                );
                let connect =
                    sparrow_client::Client::connect_endpoint_expecting(&endpoint, &expect).await;
                assert!(
                    matches!(&connect, Err(sparrow_client::Error::Domain { code, .. }) if code == "INSTANCE_MISMATCH"),
                    "client connected despite the mismatch"
                );
                assert!(
                    !import_reached(&genuine, "refused").await,
                    "an import call reached a refused daemon"
                );
            }
            let (_keep, socket, _daemon) = daemon_with_role(Some("scratch")).await;
            let endpoint = sparrow_client::Endpoint::Unix(socket.clone());
            let genuine = sparrow_client::Client::connect(&socket).await.unwrap();
            let expect = scratch.with_store_id(genuine.discovery().store_id.clone());
            sparrow_client::import_file_expecting(
                &endpoint, &expect, "accepted", "src", wire::Format::Log, &input,
            )
            .await
            .unwrap();
            sparrow_client::Client::connect_endpoint_expecting(&endpoint, &expect)
                .await
                .unwrap();
        })
        .await;
}

/// INVARIANT (#399): a reconnect inside the free import re-checks the expectation. A daemon
/// replaced by a production store after the first attempt's retryable refusal is refused with
/// `INSTANCE_MISMATCH` naming the journal left on the first store, no import call reaches it,
/// and no cancel is attempted: the first store's journal stays staged.
#[tokio::test(flavor = "current_thread")]
async fn a_reconnect_inside_the_import_refuses_a_daemon_swapped_to_another_role() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (dir, socket, _scratch) = daemon_with_role(Some("scratch")).await;
            let (_other_dir, other_socket, _production) =
                daemon_with_role(Some("production")).await;
            let first = sparrow_client::Client::connect(&socket).await.unwrap();
            let other = sparrow_client::Client::connect(&other_socket)
                .await
                .unwrap();
            let proxy_socket = dir.path().join("proxy.sock");
            let (state, proxy) = relay(&proxy_socket, first.capability().clone());
            state.refuse_publish.set(Some(("RESOURCE_LIMIT", true)));
            *state.swap_backend_after_refused_publish.borrow_mut() =
                Some(other.capability().clone());
            let input = dir.path().join("input.log");
            std::fs::write(&input, b"one line\n").unwrap();
            let expect = sparrow_client::Expect::new(sparrow_client::RequiredRole::Scratch)
                .with_store_id(first.discovery().store_id.clone());
            let error = sparrow_client::import_file_expecting(
                &sparrow_client::Endpoint::Unix(proxy_socket),
                &expect,
                "swapped",
                "src",
                wire::Format::Log,
                &input,
            )
            .await
            .unwrap_err();
            assert!(
                matches!(&error, sparrow_client::Error::Domain { code, message, retryable: false, .. }
                    if code == "INSTANCE_MISMATCH" && message.contains("stays staged")),
                "{error}"
            );
            assert!(import_reached(&first, "swapped").await);
            assert!(
                !import_reached(&other, "swapped").await,
                "an import call reached the swapped daemon"
            );
            proxy.abort();
        })
        .await;
}

/// INVARIANT (#280): a declaration above the import bound answers a non-retryable
/// INVALID_ARGUMENT that names the bound and the remedy, Discovery advertises that same bound
/// before any declaration, status counts the refusal, and an in-bound import begins unchanged.
#[tokio::test(flavor = "current_thread")]
async fn an_over_bound_declaration_is_permanent_named_advertised_and_counted() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let daemon = start(&config, &socket).await;
            let connection = connect(&socket).await.unwrap();
            let client = connection.client.clone();
            let bound = sparrow_core::MAX_IMPORT_BYTES;
            let discovered = client.discover_request().send().promise.await.unwrap();
            let discovery = rpc_ok(discovered.get().unwrap().get_result().unwrap());
            assert_eq!(discovery.get_max_import_bytes(), bound);
            let mut request = client.import_begin_request();
            let mut begin = request.get().init_request();
            begin.set_request_id("over-bound");
            begin.set_source("oversized");
            begin.set_format(wire::Format::Log);
            begin.set_byte_length(bound + 1);
            begin.set_payload_id(format!("blake3:{}", "0".repeat(64)));
            let response = request.send().promise.await.unwrap();
            let result = response.get().unwrap().get_result().unwrap();
            let wire::result::Error(error) = result.which().unwrap() else {
                panic!("an over-bound declaration was admitted");
            };
            let error = error.unwrap();
            assert_eq!(
                error.get_code().unwrap().to_str().unwrap(),
                "INVALID_ARGUMENT"
            );
            assert!(!error.get_retryable());
            let message = error.get_message().unwrap().to_str().unwrap();
            assert!(
                message.contains(&format!("at most {bound} bytes")) && message.contains("split"),
                "{message}"
            );
            begin_numbered_import(&client, "in-bound").await.unwrap();
            let response = client.status_request().send().promise.await.unwrap();
            let status = rpc_ok(response.get().unwrap().get_result().unwrap());
            assert_eq!(status.get_import_over_bound_refusals(), 1);
            drop(connection);
            drop(daemon);
        })
        .await;
}

/// An in-process daemon whose import bound is 20 chunks (1,310,720 bytes) and whose
/// retirement row budget is 20 one-chunk records, with a claim root over `producer`.
struct SetHarness {
    store: sparrow_core::Store,
    client: sparrow_client::Client,
    root: sparrow_server::consumption::ClaimRoot,
    producer: std::path::PathBuf,
    socket: std::path::PathBuf,
    _serving: tokio::task::JoinHandle<()>,
}

/// One chunk and one record charge 2 x (67 + 42) bytes of the row budget.
const SET_ROW_BUDGET: usize = 20 * 218;

/// Eviction limits whose retirement scale admits `units` one-chunk records per artifact: the
/// fixed envelope charge at one payload reference, then `units` of row budget.
fn set_limits(units: usize) -> sparrow_core::EvictionLimits {
    let fixed = 2 * 4096 + 8192 + 64 * 4096 + 18 * 1024 + (2 * 66 + 2 * 24);
    sparrow_core::EvictionLimits {
        max_payload_refs: 1,
        max_catalog_bytes: fixed + units * (SET_ROW_BUDGET / 20) + 1024,
        ..sparrow_core::EvictionLimits::default()
    }
}

async fn set_harness(dir: &Path) -> SetHarness {
    use sparrow_server::{Rpc, ServerConfig, consumption::ClaimRoot};
    let data = dir.join("store");
    let socket = data.join("rpc.sock");
    let config = ServerConfig::read(&write_server_config(dir, &data, &socket)).unwrap();
    let store = config.open_store().unwrap();
    let limits = set_limits(20);
    let bounds = store.scale_retirement_for_tests(limits, 1024).unwrap();
    assert_eq!(bounds.import_bytes, 20 * 65_536);
    let producer = dir.join("producer");
    std::fs::create_dir_all(&producer).unwrap();
    if socket.exists() {
        std::fs::remove_file(&socket).unwrap();
    }
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let capability: wire::sparrow::Client = capnp_rpc::new_client(Rpc::new(
        store.clone(),
        std::sync::Arc::new(tokio::sync::Semaphore::new(4)),
        config.max_pin_members,
    ));
    // Every connection is served, so a test can reconnect to read the limits as they stand.
    // One task owns them all, so aborting it drops every connection and releases the store.
    let serving = tokio::task::spawn_local(async move {
        use futures::StreamExt;
        let mut connections = futures::stream::FuturesUnordered::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    connections.push(sparrow_server::serve_connection(stream, capability.clone()));
                }
                Some(done) = connections.next() => done.unwrap(),
            }
        }
    });
    let client = sparrow_client::Client::connect(&socket).await.unwrap();
    let root = ClaimRoot::open(store.clone(), "watch", &producer).unwrap();
    SetHarness {
        store,
        client,
        root,
        producer,
        socket,
        _serving: serving,
    }
}

impl SetHarness {
    /// Claim `name` holding `input` and publish it through the production path.
    async fn publish(
        &self,
        name: &str,
        format: &str,
        input: &[u8],
    ) -> (String, sparrow_server::consumption::ClaimedFile) {
        let mut claim = self.claim(name, format, input);
        let published = self
            .root
            .publish(&self.client, &mut claim, &Default::default())
            .await
            .unwrap();
        (published, claim)
    }

    fn read(&self, id: &str, length: u64) -> Vec<u8> {
        let now = sparrow_server::now().unwrap();
        let mut bytes = Vec::new();
        while (bytes.len() as u64) < length {
            let take = (length - bytes.len() as u64).min(1 << 20) as usize;
            bytes.extend(self.store.bytes(id, bytes.len() as u64, take, now).unwrap());
        }
        bytes
    }

    /// The manifest's parts as (stored format, artifact), checked to cover `input` in order.
    fn parts(&self, manifest: &str, format: &str, input: &[u8]) -> Vec<(String, String)> {
        let receipt = self.store.artifact(manifest).unwrap();
        assert_eq!(receipt.format, "json");
        assert_eq!(receipt.source, "set-source");
        let body = purrdf_lex::json::read_slice(
            &self.read(manifest, receipt.byte_length),
            purrdf_lex::json::Limits::default(),
        )
        .unwrap();
        assert_eq!(body["schema"].as_str(), Some("sparrow.parts/1"));
        assert_eq!(body["format"].as_str(), Some(format));
        assert_eq!(body["byte_length"].as_u64(), Some(input.len() as u64));
        let mut joined = Vec::new();
        let mut parts = Vec::new();
        for (index, part) in body["parts"].as_array().unwrap().iter().enumerate() {
            assert_eq!(part["index"].as_u64(), Some(index as u64));
            assert_eq!(part["start"].as_u64(), Some(joined.len() as u64));
            let artifact = part["artifact"].as_str().unwrap().to_owned();
            joined.extend(self.read(&artifact, part["length"].as_u64().unwrap()));
            parts.push((part["format"].as_str().unwrap().to_owned(), artifact));
        }
        assert_eq!(
            joined, input,
            "the parts read by the manifest are the input"
        );
        parts
    }
}

/// One jsonl line of exactly one import chunk, so each line is one record and one chunk.
fn chunk_line(index: usize, pad: u8) -> Vec<u8> {
    let mut line = format!("{{\"line\":{index},\"pad\":\"").into_bytes();
    line.resize(65_536 - 3, pad);
    line.extend_from_slice(b"\"}\n");
    line
}

/// `count` chunk lines from line `first`, as one source.
fn chunk_lines(first: usize, count: usize, pad: u8) -> Vec<u8> {
    (first..first + count)
        .flat_map(|index| chunk_line(index, pad))
        .collect()
}

impl SetHarness {
    /// Reserve and claim `name` holding `input`, without publishing it.
    fn claim(
        &self,
        name: &str,
        format: &str,
        input: &[u8],
    ) -> sparrow_server::consumption::ClaimedFile {
        use sparrow_server::consumption::Recovery;
        let path = self.producer.join(name);
        std::fs::write(&path, input).unwrap();
        let journal = self
            .root
            .reserve(
                Path::new(name),
                sparrow_client::CompletedFileIdentity::capture(&path.metadata().unwrap()),
                "set-source",
                format,
            )
            .unwrap();
        let Recovery::Claimed(claim) = self
            .root
            .recover(&journal.import_id, sparrow_server::now().unwrap())
            .unwrap()
        else {
            panic!("a fresh claim is not removed");
        };
        claim
    }

    /// A new connection, whose discovery reads the limits as they stand now.
    async fn reconnect(&self) -> sparrow_client::Client {
        sparrow_client::Client::connect(&self.socket).await.unwrap()
    }

    /// Scale the import bound and the retirement row budget to `units` one-chunk records.
    fn scale(&self, units: usize) {
        let bounds = self
            .store
            .scale_retirement_for_tests(set_limits(units), 1024)
            .unwrap();
        assert_eq!(bounds.import_bytes, units as u64 * 65_536);
    }

    /// The claim's private source file, opened as the part upload reads it.
    fn source(&self, claim: &sparrow_server::consumption::ClaimedFile) -> std::fs::File {
        std::fs::File::open(
            self.producer
                .join(".sparrow-inflight")
                .join(&claim.journal().import_id),
        )
        .unwrap()
    }

    /// Upload every part of the claim, as an attempt does, and return its parts in order.
    async fn upload(
        &self,
        client: &sparrow_client::Client,
        claim: &sparrow_server::consumption::ClaimedFile,
        resume: &sparrow_client::parts::Resume,
    ) -> Vec<(sparrow_client::parts::PartSpan, String)> {
        use sparrow_client::parts;
        let limits = parts::PartLimits::from_discovery(
            client.discovery(),
            sparrow_core::SYSLOG_MESSAGE_BYTES as u64,
        )
        .unwrap();
        let journal = claim.journal();
        parts::upload_parts(
            client,
            &journal.request_id,
            &journal.source,
            sparrow_client::Format::Jsonl,
            &self.source(claim),
            Ok(&limits),
            &parts::ClaimParts::default(),
            &std::sync::Arc::new(parts::PartsProgress::default()),
            resume,
        )
        .await
        .unwrap()
    }

    /// Publish the first `spans` of `input` as the claim's set parts and nothing else, as an
    /// attempt interrupted after them leaves the claim.
    async fn publish_prefix(
        &self,
        claim: &sparrow_server::consumption::ClaimedFile,
        input: &[u8],
        spans: &[sparrow_client::parts::PartSpan],
    ) -> Vec<String> {
        use sparrow_client::parts;
        let set = parts::set_request(&claim.journal().request_id).unwrap();
        let mut artifacts = Vec::new();
        for span in spans {
            let path = self.producer.join(format!("prefix-{}", span.index));
            let bytes = &input[span.start as usize..(span.start + span.length) as usize];
            std::fs::write(&path, bytes).unwrap();
            let mut file = std::fs::File::open(&path).unwrap();
            let receipt = self
                .client
                .import_open_file_with_progress(
                    &parts::part_request(&set, span.index),
                    "set-source",
                    span.format,
                    &mut file,
                    None,
                )
                .await
                .unwrap();
            artifacts.push(receipt.id);
        }
        artifacts
    }

    /// Reclaim every published or lapsed staging row the daemon's reclaim duty would.
    fn reclaim(&self) {
        let now = sparrow_server::now().unwrap();
        for _ in 0..8 {
            self.store.import_reclaim(now).unwrap();
        }
    }

    /// One eviction pass of at most `units` retirement units, with no archive configured, and
    /// proof that the archive gate held it: nothing retired, and the pass stopped at `ArchiveLag`.
    fn evict_held(&self, units: usize) {
        let report = self
            .store
            .evict_oldest(
                sparrow_server::now().unwrap(),
                sparrow_core::EvictionLimits {
                    max_records: units,
                    ..sparrow_core::EvictionLimits::default()
                },
            )
            .unwrap();
        assert!(
            report.records.is_empty() && report.artifacts.is_empty(),
            "no archive, so nothing is retired: {report:?}"
        );
        assert_eq!(report.frontier, sparrow_core::EvictionFrontier::ArchiveLag);
    }

    /// The by-key read of set part `index` of the claim.
    fn part_state(
        &self,
        claim: &sparrow_server::consumption::ClaimedFile,
        index: u32,
    ) -> sparrow_core::PartReceiptState {
        use sparrow_client::parts;
        let set = parts::set_request(&claim.journal().request_id).unwrap();
        self.store
            .ingest_part_receipt(
                &parts::part_request(&set, index),
                sparrow_server::now().unwrap(),
            )
            .unwrap()
    }

    async fn publish_claim(
        &self,
        client: &sparrow_client::Client,
        claim: &mut sparrow_server::consumption::ClaimedFile,
    ) -> Result<String, sparrow_server::consumption::Error> {
        self.root.publish(client, claim, &Default::default()).await
    }

    /// The live records of the claim's parts: each source line is read exactly once.
    fn assert_lines_read_once(&self, manifest: &str, lines: usize) {
        let now = sparrow_server::now().unwrap();
        let manifest_records = self.store.artifact(manifest).unwrap().record_ids.len();
        let records = self.store.records(now).unwrap();
        assert_eq!(
            records.len(),
            lines + manifest_records,
            "every line is read once"
        );
        let mut ids = records
            .iter()
            .map(|record| record.id.clone())
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), records.len());
    }
}

/// INVARIANT (#285, condition 1): the source of a set claim stays held, and its bytes
/// are unchanged, for as long as the claim is pending: before the manifest and while it is
/// accepted and not yet recorded. With no archive configured, eviction holds at `ArchiveLag`
/// and retires no part, so the claim's parts stay stored and `publish` completes the set. Once
/// `publish` completes, removal succeeds.
#[tokio::test]
async fn a_set_claims_source_stays_held_while_the_claim_is_pending() {
    tokio::task::LocalSet::new()
        .run_until(async {
            for manifest_accepted in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let harness = set_harness(dir.path()).await;
                let input = chunk_lines(0, 50, b'p');
                let mut claim = harness.claim("held.jsonl", "jsonl", &input);
                let uploaded = harness
                    .upload(
                        &harness.client,
                        &claim,
                        &sparrow_client::parts::Resume::fresh(),
                    )
                    .await;
                let journal = claim.journal().clone();
                if manifest_accepted {
                    sparrow_client::parts::ingest_manifest(
                        &harness.client,
                        &journal.request_id,
                        &journal.source,
                        sparrow_client::Format::Jsonl,
                        input.len() as u64,
                        &uploaded,
                    )
                    .await
                    .unwrap();
                } else {
                    // Eviction is offered part 0 and holds: it retires nothing.
                    harness.evict_held(1);
                    assert!(matches!(
                        harness.part_state(&claim, 0),
                        sparrow_core::PartReceiptState::Stored(_)
                    ));
                }
                let now = sparrow_server::now().unwrap();
                harness.root.remove(&claim, now).unwrap_err();
                assert_eq!(
                    std::fs::read(
                        harness
                            .producer
                            .join(".sparrow-inflight")
                            .join(&journal.import_id)
                    )
                    .unwrap(),
                    input,
                    "manifest_accepted {manifest_accepted}: the claimed source is unchanged"
                );
                if !manifest_accepted {
                    harness.evict_held(20);
                    harness.reclaim();
                }
                harness
                    .publish_claim(&harness.client, &mut claim)
                    .await
                    .unwrap();
                harness.root.remove(&claim, now).unwrap();
            }
        })
        .await;
}

/// INVARIANT (#285, condition 2, review-pr286 B1): with no archive configured, eviction
/// holds at `ArchiveLag` and retires no record of a set part, so a part is never answered as
/// evicted and never sent again. Every part keeps its original artifact, the claim publishes
/// from the stored parts, and each source line is read exactly once.
#[tokio::test]
async fn a_set_part_without_an_archive_is_held_by_eviction_and_read_exactly_once() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let harness = set_harness(dir.path()).await;
            let input = chunk_lines(0, 50, b'p');
            let mut claim = harness.claim("held-parts.jsonl", "jsonl", &input);
            let first = harness
                .upload(
                    &harness.client,
                    &claim,
                    &sparrow_client::parts::Resume::fresh(),
                )
                .await;
            assert_eq!(first.len(), 3);

            // Eviction is offered every record of part 0, one at a time and all at once, and
            // holds each time: no part is evicting or evicted.
            harness.evict_held(1);
            harness.evict_held(19);
            harness.reclaim();
            for index in 0..3 {
                assert!(matches!(
                    harness.part_state(&claim, index),
                    sparrow_core::PartReceiptState::Stored(_)
                ));
            }
            let manifest = harness
                .publish_claim(&harness.client, &mut claim)
                .await
                .unwrap();
            let parts = harness.parts(&manifest, "jsonl", &input);
            assert_eq!(parts.len(), 3);
            for (part, original) in parts.iter().zip(&first) {
                assert_eq!(part.1, original.1, "a held part keeps its artifact");
            }
            harness.assert_lines_read_once(&manifest, 50);
            let now = sparrow_server::now().unwrap();
            harness.root.remove(&claim, now).unwrap();
        })
        .await;
}

/// INVARIANT (#285): a restarted attempt skips the parts the daemon recorded and cuts the rest
/// under the limits as they stand now. With the limits grown, even to admit the whole file in
/// one part, the claim still finishes as a set and no byte is read twice. A claim whose parts
/// are all recorded writes its manifest with no limit read.
#[tokio::test]
async fn a_restarted_attempt_under_grown_limits_skips_its_recorded_parts() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use sparrow_client::parts;
            let input = chunk_lines(0, 50, b'p');
            let limits = |harness: &SetHarness| {
                parts::PartLimits::from_discovery(
                    harness.client.discovery(),
                    sparrow_core::SYSLOG_MESSAGE_BYTES as u64,
                )
                .unwrap()
            };
            for grown in [40, 60] {
                let dir = tempfile::tempdir().unwrap();
                let harness = set_harness(dir.path()).await;
                let mut claim = harness.claim("grown.jsonl", "jsonl", &input);
                let first = parts::next_cut(
                    &input[..],
                    input.len() as u64,
                    0,
                    0,
                    sparrow_client::Format::Jsonl,
                    &limits(&harness),
                )
                .unwrap();
                let recorded = harness.publish_prefix(&claim, &input, &[first]).await;
                harness.scale(grown);
                let wider = harness.reconnect().await;
                let manifest = harness.publish_claim(&wider, &mut claim).await.unwrap();
                let listed = harness.parts(&manifest, "jsonl", &input);
                assert_eq!(listed.len(), 2, "grown {grown}: a set, never one artifact");
                assert_eq!(listed[0].1, recorded[0], "the recorded part is skipped");
                harness.assert_lines_read_once(&manifest, 50);
            }

            // Every part recorded: the manifest needs no limit.
            let dir = tempfile::tempdir().unwrap();
            let harness = set_harness(dir.path()).await;
            let mut claim = harness.claim("all.jsonl", "jsonl", &input);
            let uploaded = harness
                .upload(&harness.client, &claim, &parts::Resume::fresh())
                .await;
            let parts::Frontier::Recorded(resume) =
                parts::recorded_prefix(&harness.client, &claim.journal().request_id)
                    .await
                    .unwrap()
            else {
                panic!("a recorded set continues, it is not measured");
            };
            assert_eq!(resume.parts.len(), uploaded.len());
            let missing = parts::MissingLimits {
                max_import_bytes: true,
                raw_record_bytes: true,
                retirement: true,
            };
            let journal = claim.journal().clone();
            let listed = parts::upload_parts(
                &harness.client,
                &journal.request_id,
                &journal.source,
                sparrow_client::Format::Jsonl,
                &harness.source(&claim),
                Err(missing),
                &parts::ClaimParts::default(),
                &std::sync::Arc::new(parts::PartsProgress::default()),
                &resume,
            )
            .await
            .unwrap();
            assert_eq!(listed, uploaded, "no cut was needed, so no limit was read");
            let manifest = harness
                .publish_claim(&harness.client, &mut claim)
                .await
                .unwrap();
            assert_eq!(
                harness.parts(&manifest, "jsonl", &input).len(),
                uploaded.len()
            );
        })
        .await;
}

/// The part request id of set part `index` of `claim`.
fn set_part_request(claim: &sparrow_server::consumption::ClaimedFile, index: u32) -> String {
    let set = sparrow_client::parts::set_request(&claim.journal().request_id).unwrap();
    sparrow_client::parts::part_request(&set, index)
}

/// Begin part `index`, declared as `bytes`, and upload them without publishing: a draft the
/// daemon holds, stamped at `now`.
fn stage_draft(harness: &SetHarness, request: &str, bytes: &[u8], now: u64) {
    let payload = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(bytes).as_bytes())
    );
    let draft = harness
        .store
        .import_begin(
            request,
            "set-source",
            "jsonl",
            bytes.len() as u64,
            &payload,
            now,
        )
        .unwrap();
    for (index, chunk) in bytes.chunks(65_536).enumerate() {
        harness
            .store
            .import_append(&draft.id, (index * 65_536) as u64, chunk, now)
            .unwrap();
    }
}

fn pending_text(error: &sparrow_server::consumption::Error) -> String {
    error.to_string()
}

/// INVARIANT (#285): a draft at the frontier that the limits as they stand no longer admit, or
/// whose payload is not the file's bytes at its span, is cancelled and waited for, and the part
/// is cut again at its index under the current limits. The parts read in order are the source.
#[tokio::test]
async fn a_frontier_draft_under_shrunk_limits_is_cancelled_and_recut_at_its_index() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let input = chunk_lines(0, 50, b'p');
            // (limits after the draft, the draft's bytes for part 1)
            for (shrunk, draft) in [
                (10, chunk_lines(20, 20, b'p')),
                (20, chunk_lines(20, 20, b'q')),
            ] {
                let dir = tempfile::tempdir().unwrap();
                let harness = set_harness(dir.path()).await;
                let mut claim = harness.claim("shrunk.jsonl", "jsonl", &input);
                harness
                    .publish_prefix(
                        &claim,
                        &input,
                        &[sparrow_client::parts::PartSpan {
                            index: 0,
                            start: 0,
                            length: 20 * 65_536,
                            format: sparrow_client::Format::Jsonl,
                        }],
                    )
                    .await;
                let now = sparrow_server::now().unwrap();
                stage_draft(&harness, &set_part_request(&claim, 1), &draft, now);
                harness.scale(shrunk);
                let narrower = harness.reconnect().await;
                let error = harness
                    .publish_claim(&narrower, &mut claim)
                    .await
                    .unwrap_err();
                assert!(
                    pending_text(&error)
                        .contains("part 1 waits for reclamation of its cancelled draft"),
                    "{error}"
                );
                // Still waiting while the cancelled row is not reclaimed: nothing is begun.
                let error = harness
                    .publish_claim(&narrower, &mut claim)
                    .await
                    .unwrap_err();
                assert!(
                    pending_text(&error).contains("part 1 waits for reclamation"),
                    "{error}"
                );
                assert!(matches!(
                    harness.part_state(&claim, 1),
                    sparrow_core::PartReceiptState::Absent
                ));
                harness.reclaim();
                let manifest = harness.publish_claim(&narrower, &mut claim).await.unwrap();
                harness.parts(&manifest, "jsonl", &input);
                harness.assert_lines_read_once(&manifest, 50);
            }
        })
        .await;
}

/// INVARIANT (#285): a frontier whose lapsed draft is not reclaimed yet is waited for: nothing is
/// begun while its status reports IMPORT_EXPIRED, the part begins only once the status is
/// NOT_FOUND, the pending text names the wait, and the claim completes with no operator.
#[tokio::test]
async fn a_frontier_refused_by_shrunk_limits_waits_for_reclaim_then_completes() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let harness = set_harness(dir.path()).await;
            let input = chunk_lines(0, 50, b'p');
            let mut claim = harness.claim("lapsed.jsonl", "jsonl", &input);
            harness
                .publish_prefix(
                    &claim,
                    &input,
                    &[sparrow_client::parts::PartSpan {
                        index: 0,
                        start: 0,
                        length: 20 * 65_536,
                        format: sparrow_client::Format::Jsonl,
                    }],
                )
                .await;
            // A draft begun at time 1 has a lease that ended long ago.
            let request = set_part_request(&claim, 1);
            stage_draft(&harness, &request, &chunk_lines(20, 20, b'p'), 1);
            let status = |harness: &SetHarness| {
                harness
                    .store
                    .ingest_status(
                        &sparrow_core::IngestSelector::Request(request.clone()),
                        Default::default(),
                        sparrow_server::now().unwrap(),
                    )
                    .unwrap_err()
                    .code
            };
            assert_eq!(status(&harness), "IMPORT_EXPIRED");
            let error = harness
                .publish_claim(&harness.client, &mut claim)
                .await
                .unwrap_err();
            assert!(
                pending_text(&error).contains("part 1 waits for reclamation"),
                "{error}"
            );
            assert_eq!(status(&harness), "IMPORT_EXPIRED", "nothing was begun");
            harness.reclaim();
            assert_eq!(status(&harness), "NOT_FOUND");
            let manifest = harness
                .publish_claim(&harness.client, &mut claim)
                .await
                .unwrap();
            harness.parts(&manifest, "jsonl", &input);
            harness
                .root
                .remove(&claim, sparrow_server::now().unwrap())
                .unwrap();
        })
        .await;
}

/// INVARIANT (#285): a draft at index 0 never takes the single path, whatever the limits now
/// admit. A live draft that the grown limits still admit continues, and the claim finishes as a
/// set. A lapsed draft that is not reclaimed yet is waited for, and nothing is published.
#[tokio::test]
async fn a_draft_at_index_zero_never_takes_the_single_path() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let input = chunk_lines(0, 30, b'p');
            for lapsed in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let harness = set_harness(dir.path()).await;
                let mut claim = harness.claim("zero.jsonl", "jsonl", &input);
                let now = if lapsed {
                    1
                } else {
                    sparrow_server::now().unwrap()
                };
                stage_draft(
                    &harness,
                    &set_part_request(&claim, 0),
                    &chunk_lines(0, 20, b'p'),
                    now,
                );
                harness.scale(60);
                let wider = harness.reconnect().await;
                let outcome = harness.publish_claim(&wider, &mut claim).await;
                if lapsed {
                    let error = outcome.unwrap_err();
                    assert!(
                        pending_text(&error).contains("part 0 waits for reclamation"),
                        "{error}"
                    );
                    assert!(claim.journal().published_artifact.is_none());
                    assert!(matches!(
                        harness.part_state(&claim, 0),
                        sparrow_core::PartReceiptState::Absent
                    ));
                } else {
                    let manifest = outcome.unwrap();
                    assert_eq!(harness.parts(&manifest, "jsonl", &input).len(), 2);
                    assert_eq!(
                        claim.journal().published_artifact.as_deref(),
                        Some(manifest.as_str())
                    );
                }
            }
        })
        .await;
}

/// INVARIANT (#280, OR-A): a consume file above the import bound is published as ordered
/// parts and a manifest, the claim records the manifest, and its source is removed through
/// the manifest proof; the part records are ordinary readable records.
#[tokio::test]
async fn over_bound_consume_file_is_split_into_ordered_parts_and_removed() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let harness = set_harness(dir.path()).await;
            // 50 lines of one chunk each: 2.5 times the 20-chunk import bound.
            let line = |index: usize| {
                let mut line = format!("{{\"line\":{index},\"pad\":\"").into_bytes();
                line.resize(65_536 - 3, b'p');
                line.extend_from_slice(b"\"}\n");
                line
            };
            let input: Vec<u8> = (0..50).flat_map(line).collect();
            let (manifest, claim) = harness.publish("big.jsonl", "jsonl", &input).await;
            let journal = claim.journal().clone();
            assert_eq!(
                journal.published_artifact.as_deref(),
                Some(manifest.as_str())
            );
            let parts = harness.parts(&manifest, "jsonl", &input);
            assert_eq!(parts.len(), 3);
            assert!(parts.iter().all(|(format, _)| format == "jsonl"));
            let now = sparrow_server::now().unwrap();
            assert!(harness.store.records(now).unwrap().len() >= 50);
            let removed = harness.root.remove(&claim, now).unwrap();
            assert!(removed.removed_at.is_some());
            assert!(
                !harness
                    .producer
                    .join(".sparrow-inflight")
                    .join(&journal.import_id)
                    .exists()
            );
        })
        .await;
}

/// INVARIANT (#280, OR-B): a json or syslog file above the import bound is taken, never
/// held for its format: its parts are stored opaque, so nothing parses a fragment, and the
/// manifest keeps the original format.
#[tokio::test]
async fn over_bound_non_line_consume_file_is_taken_as_opaque_parts() {
    tokio::task::LocalSet::new()
        .run_until(async {
            for (name, format) in [("big.json", "json"), ("big.syslog", "syslog")] {
                // Each format has its own store: a published part holds one of the 16 import
                // journals until maintenance reclaims it, and no maintenance runs here. A syslog
                // file of 800 KiB is 13 parts at the packet limit, within those slots.
                let dir = tempfile::tempdir().unwrap();
                let harness = set_harness(dir.path()).await;
                let size = if format == "syslog" {
                    800 * 1024
                } else {
                    3 * 1024 * 1024
                };
                let mut input = b"{\"document\":\"".to_vec();
                input.resize(size, b'd');
                input.extend_from_slice(b"\"}");
                let (manifest, claim) = harness.publish(name, format, &input).await;
                let parts = harness.parts(&manifest, format, &input);
                // A json file is cut at the import bound; a syslog file at the packet limit.
                let expected = match format {
                    "syslog" => input.len().div_ceil(65_536),
                    _ => 3,
                };
                assert_eq!(parts.len(), expected, "{format}");
                for (stored, artifact) in &parts {
                    assert_eq!(stored, "opaque");
                    assert_eq!(harness.store.artifact(artifact).unwrap().format, "opaque");
                }
                harness
                    .root
                    .remove(&claim, sparrow_server::now().unwrap())
                    .unwrap();
            }
        })
        .await;
}

/// INVARIANT (#282): a syslog file above the packet limit and under the import bound is not
/// refused at record preparation. It is cut into opaque parts of at most the packet limit, in
/// order; the manifest keeps the original format and each part's byte range; and the parts read
/// in manifest order are the source bytes (`harness.parts` checks both).
#[tokio::test]
async fn syslog_file_above_the_packet_limit_is_taken_as_ordered_opaque_parts() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let harness = set_harness(dir.path()).await;
            let input: Vec<u8> = (0..102_400u32).map(|n| b'a' + (n % 26) as u8).collect();
            let (manifest, _claim) = harness.publish("hundred.syslog", "syslog", &input).await;
            let parts = harness.parts(&manifest, "syslog", &input);
            let lengths: Vec<u64> = parts
                .iter()
                .map(|(_, artifact)| harness.store.artifact(artifact).unwrap().byte_length)
                .collect();
            assert_eq!(lengths, [65_536, 36_864]);
            assert!(parts.iter().all(|(stored, _)| stored == "opaque"));
        })
        .await;
}

/// INVARIANT (#282): a syslog file at or under the packet limit keeps the single path: it is one
/// syslog artifact holding the source bytes, and no manifest is written.
#[tokio::test]
async fn syslog_file_at_the_packet_limit_keeps_the_single_path() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let harness = set_harness(dir.path()).await;
            let input = vec![b'x'; 65_536];
            let (id, claim) = harness.publish("packet.syslog", "syslog", &input).await;
            assert_eq!(
                claim.journal().published_artifact.as_deref(),
                Some(id.as_str())
            );
            let receipt = harness.store.artifact(&id).unwrap();
            assert_eq!(receipt.format, "syslog");
            assert_eq!(receipt.byte_length, 65_536);
            assert_eq!(harness.read(&id, 65_536), input);
        })
        .await;
}

/// INVARIANT (#280, design 19b, RS4 and RS3): a recorded set claim whose manifest receipt is
/// gone is not planned again: publish fails STORAGE_FAILURE, ingests no manifest and leaves
/// the claim recorded, so no part is sent again. A recorded single-path claim is not stopped:
/// it returns its saved receipt. The part uploads themselves are not counted (the client has
/// no call hook); the guard's visible effect is that no manifest is ingested and the call
/// fails, which a re-plan would not do (each part would answer its saved receipt and the
/// manifest would be accepted again).
///
/// Not written here: the recorded single-path claim whose bound would now split the file (design
/// r7 R1 and r6). `set_harness` fixes the bound (`SET_ROW_BUDGET`, asserted at `import_bytes`
/// equal to `20 * 65_536`), so a second open cannot advertise a smaller one. The reason no split
/// is reached is `consumption.rs:713`: a recorded single-path claim (`published_artifact` Some)
/// returns `publish_single` before `PartLimits` and the bound are read (`consumption.rs:718-736`).
/// The `again == single_id` assertion below shows the saved receipt is returned.
#[tokio::test]
async fn recorded_set_claim_with_a_missing_manifest_receipt_plans_uploads_and_begins_nothing() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let harness = set_harness(dir.path()).await;
            let line = |index: usize| {
                let mut line = format!("{{\"line\":{index},\"pad\":\"").into_bytes();
                line.resize(65_536 - 3, b'p');
                line.extend_from_slice(b"\"}\n");
                line
            };
            let input: Vec<u8> = (0..50).flat_map(line).collect();
            let (_, set_claim) = harness.publish("big.jsonl", "jsonl", &input).await;
            let (single_id, single_claim) = harness.publish("small.log", "log", b"one\n").await;
            let (set, single) = (
                set_claim.journal().clone(),
                single_claim.journal().import_id.clone(),
            );
            let SetHarness {
                store,
                client,
                root,
                _serving,
                ..
            } = harness;
            drop((set_claim, single_claim, client, root));
            _serving.abort();
            let _ = _serving.await;
            drop(store);
            let db = sparrow_test_support::catalog_db(&dir.path().join("store")).unwrap();
            db.delete(format!(
                "request:ingest:{}.manifest",
                set.request_id
                    .replacen("consume:", sparrow_core::SET_PREFIX, 1)
            ))
            .unwrap();
            drop(db);

            let harness = set_harness(dir.path()).await;
            let now = sparrow_server::now().unwrap();
            let sparrow_server::consumption::Recovery::Claimed(mut claim) =
                harness.root.recover(&set.import_id, now).unwrap()
            else {
                panic!("a recorded set claim is not removed");
            };
            // The store's own listing of the source's artifacts: a failed attempt that uploaded
            // nothing leaves the count where it was (the parts of the first attempt stay as they were).
            let listed = |store: &sparrow_core::Store| {
                store
                    .artifacts(now)
                    .unwrap()
                    .iter()
                    .filter(|artifact| artifact.source == "set-source")
                    .count()
            };
            let listed_before = listed(&harness.store);
            assert!(
                listed_before > 0,
                "the first attempt's parts are not listed"
            );
            for _ in 0..2 {
                let failure = harness
                    .root
                    .publish(&harness.client, &mut claim, &Default::default())
                    .await
                    .unwrap_err();
                assert!(
                    matches!(&failure, sparrow_server::consumption::Error::Storage(error)
                        if error.code == "STORAGE_FAILURE"),
                    "{failure:?}"
                );
            }
            assert_eq!(
                listed(&harness.store),
                listed_before,
                "a failed attempt published an artifact for the source"
            );
            assert!(matches!(
                harness
                    .store
                    .consumption_set_manifest(&set.import_id)
                    .unwrap(),
                sparrow_core::SetManifest::Absent
            ));
            let sparrow_server::consumption::Recovery::Claimed(mut claim) =
                harness.root.recover(&single, now).unwrap()
            else {
                panic!("a recorded single claim is not removed");
            };
            let again = harness
                .root
                .publish(&harness.client, &mut claim, &Default::default())
                .await
                .unwrap();
            assert_eq!(again, single_id);
        })
        .await;
}

/// What one watch's status says that the publish-hold test reads.
#[derive(Debug)]
struct WatchSeen {
    id: String,
    revision: u64,
    held: u32,
    pending: u32,
    completed: u64,
    tail_generations: usize,
}

async fn watches_seen(client: &wire::sparrow::Client) -> Vec<WatchSeen> {
    let response = client.status_request().send().promise.await.unwrap();
    let status = rpc_ok(response.get().unwrap().get_result().unwrap());
    status
        .get_consume_watches()
        .unwrap()
        .iter()
        .map(|watch| WatchSeen {
            id: watch.get_id().unwrap().to_str().unwrap().to_owned(),
            revision: watch.get_revision(),
            held: watch.get_held_claims(),
            pending: watch.get_pending_claims(),
            completed: watch.get_completed_claims(),
            tail_generations: watch.get_tail_generation_ids().unwrap().len() as usize,
        })
        .collect()
}

/// INVARIANT (#280, publish hold, design 3.7A): five held watches of six finished files each,
/// beside one unheld watch and one tail watch, in one daemon. The pending claims of all held
/// watches together stay below `HELD_CLAIMS_TOTAL` (the store's sixteen journal slots less the
/// four `HELD_RESERVED_SLOTS`, so twelve), and the files past that total stay unclaimed in their
/// producers' directories, claimed in order. The unheld watch's file is claimed and published,
/// and the tail begins a generation, while the holds stand. Clearing each hold wakes its watch:
/// the files that waited are claimed and published. Fails when a held claim is taken past the
/// total, when a file past a cap is claimed out of order, or when a cleared hold does not wake
/// its watch.
#[tokio::test(flavor = "current_thread")]
async fn held_watches_share_one_total_and_waiting_files_are_claimed_in_order_when_the_hold_clears()
{
    // The held claims may stand at most `HELD_CLAIMS_TOTAL`; each watch's share is 4 (`HELD_CLAIMS_PER_WATCH`).
    const HELD_TOTAL: u32 = (sparrow_core::MAX_CONSUMPTION_JOURNALS - 4) as u32;
    const PER_WATCH: u32 = 4;
    const HELD_WATCHES: usize = 5;
    const FILES: usize = 6;
    tokio::task::LocalSet::new()
        .run_until(async {
            use sparrow_server::ProducerContract::{ActiveFilesRetain, FinishedFiles};
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let watch = |id: &str, root: &Path, producer_contract, publish_hold| {
                sparrow_server::ConsumeWatch {
                    id: id.into(),
                    root: root.into(),
                    producer_contract,
                    source: id.into(),
                    format: "jsonl".into(),
                    quiet_ms: 100,
                    max_directories: 16,
                    max_candidates: 16,
                    publish_hold,
                }
            };
            let mut held_roots = Vec::new();
            for index in 0..HELD_WATCHES {
                let id = format!("held-{index}");
                let root = dir.path().join(&id);
                std::fs::create_dir(&root).unwrap();
                for file in 0..FILES {
                    std::fs::write(root.join(format!("file-{file}.jsonl")), b"{}\n").unwrap();
                }
                append_watch_config(&config, &watch(&id, &root, FinishedFiles, true));
                held_roots.push((id, root));
            }
            let open = dir.path().join("open");
            std::fs::create_dir(&open).unwrap();
            std::fs::write(open.join("ready.jsonl"), b"{}\n").unwrap();
            append_watch_config(&config, &watch("open", &open, FinishedFiles, false));
            let tail = dir.path().join("tail");
            std::fs::create_dir(&tail).unwrap();
            std::fs::write(tail.join("active.jsonl"), b"{}\n").unwrap();
            append_watch_config(&config, &watch("tail", &tail, ActiveFilesRetain, false));

            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let store_id = client.discovery().store_id.clone();
            let is_held = |watch: &WatchSeen| watch.id.starts_with("held-");

            // Until the holds have claimed all they may, and the unheld and tail watches have run.
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                let seen = watches_seen(client.capability()).await;
                let held: u32 = seen.iter().filter(|w| is_held(w)).map(|w| w.held).sum();
                let open_done = seen.iter().any(|w| w.id == "open" && w.completed == 1);
                let tailed = seen
                    .iter()
                    .any(|w| w.id == "tail" && w.tail_generations != 0);
                if held == HELD_TOTAL && open_done && tailed {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "held watches never settled: {seen:?}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            // The total holds after the passes that follow, not only at the first moment it is met.
            tokio::time::sleep(Duration::from_secs(1)).await;
            let seen = watches_seen(client.capability()).await;
            let held: u32 = seen.iter().filter(|w| is_held(w)).map(|w| w.held).sum();
            assert_eq!(held, HELD_TOTAL, "{seen:?}");
            for (id, root) in &held_roots {
                let claimed = seen.iter().find(|w| &w.id == id).unwrap().held;
                assert!(claimed <= PER_WATCH, "{id} holds {claimed}");
                // The claimed files are the first in order; the waiting ones are the rest.
                let mut waiting: Vec<String> = std::fs::read_dir(root)
                    .unwrap()
                    .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                    .filter(|name| !name.starts_with('.'))
                    .collect();
                waiting.sort();
                let expected: Vec<String> = (claimed as usize..FILES)
                    .map(|file| format!("file-{file}.jsonl"))
                    .collect();
                assert_eq!(
                    waiting, expected,
                    "{id}: the waiting files are not the rest in order"
                );
            }

            // Clearing each hold wakes its watch: the files that waited are claimed and published.
            for (id, root) in &held_roots {
                let revision = seen.iter().find(|w| &w.id == id).unwrap().revision;
                let file = dir.path().join(format!("{id}-clear.toml"));
                let definition = toml::to_string(&watch(id, root, FinishedFiles, false)).unwrap();
                std::fs::write(&file, definition).unwrap();
                cli(
                    &socket,
                    &[
                        "watch",
                        "put",
                        &format!("clear-{id}"),
                        &revision.to_string(),
                        file.to_str().unwrap(),
                    ],
                );
            }
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                let seen = watches_seen(client.capability()).await;
                let done = seen
                    .iter()
                    .filter(|w| is_held(w))
                    .all(|w| w.completed == FILES as u64 && w.pending == 0);
                if done {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "cleared holds never finished: {seen:?}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let sources = cli(&socket, &["sources", &store_id]);
            let expected = FILES.to_string();
            for (id, _) in &held_roots {
                let row = sources["rows"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|row| row["source_id"].as_str() == Some(id.as_str()))
                    .unwrap();
                assert_eq!(
                    row["complete_artifacts"].as_str(),
                    Some(expected.as_str()),
                    "{id}"
                );
            }
        })
        .await;
}

/// INVARIANT (#280, F2): the client's cut, built only from the daemon's advertised limits,
/// makes parts the daemon's layout accepts, and the first part one line beyond the cut is
/// refused RESOURCE_LIMIT by the same layout. No limit is copied into the test.
#[tokio::test]
async fn client_cut_parts_are_accepted_by_the_daemon_and_one_more_unit_is_refused() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use sparrow_client::{Format, parts};
            let dir = tempfile::tempdir().unwrap();
            let harness = set_harness(dir.path()).await;
            let limits = parts::PartLimits::from_discovery(
                harness.client.discovery(),
                sparrow_core::SYSLOG_MESSAGE_BYTES as u64,
            )
            .unwrap();
            let input: Vec<u8> = (0..100)
                .flat_map(|index| format!("dense line {index:04}\n").into_bytes())
                .collect();
            let length = input.len() as u64;
            let stage = |request: &str, bytes: &[u8]| {
                let payload = format!(
                    "blake3:{}",
                    purrdf_hash::hex::encode(purrdf_hash::blake3::hash(bytes).as_bytes())
                );
                let manifest = harness
                    .store
                    .import_begin(
                        request,
                        "set-source",
                        "log",
                        bytes.len() as u64,
                        &payload,
                        0,
                    )
                    .unwrap();
                for (index, chunk) in bytes.chunks(65_536).enumerate() {
                    harness
                        .store
                        .import_append(&manifest.id, (index * 65_536) as u64, chunk, 0)
                        .unwrap();
                }
                harness.store.import_seal(&manifest.id, 0).unwrap();
                harness.store.import_prepare_layout(&manifest.id, 0)
            };
            let (mut start, mut index, mut boundary) = (0, 0, None);
            while start < length {
                let span = parts::next_cut(&input[..], length, start, index, Format::Log, &limits)
                    .unwrap();
                let bytes = &input[span.start as usize..(span.start + span.length) as usize];
                stage(&format!("cut-{index}"), bytes).unwrap();
                if span.start + span.length < length {
                    boundary = Some(span);
                }
                start += span.length;
                index += 1;
            }
            assert!(
                index > 1,
                "the joint retirement bound, not the bytes, cut this file"
            );
            let boundary = boundary.unwrap();
            let next_line = input[(boundary.start + boundary.length) as usize..]
                .iter()
                .position(|byte| *byte == b'\n')
                .unwrap() as u64
                + 1;
            let beyond = &input
                [boundary.start as usize..(boundary.start + boundary.length + next_line) as usize];
            let refused = stage("cut-beyond", beyond).unwrap_err();
            assert_eq!(refused.code, "RESOURCE_LIMIT", "{refused:?}");
        })
        .await;
}
