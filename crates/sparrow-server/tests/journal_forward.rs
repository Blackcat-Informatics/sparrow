// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The journal forwarder against an isolated daemon and a synthetic `journalctl`.
//!
//! The forwarder runs in a child process of this test binary, with a fake `journalctl` first
//! on `PATH`. The fake prints fixture entries after the `--after-cursor` it is given, so the
//! cursor semantics are exercised exactly as `journalctl` applies them.

use std::{
    os::unix::fs::PermissionsExt,
    os::unix::process::CommandExt,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

/// Entries in the fixture stream. Entry 0 is the starting cursor and is never forwarded.
const ENTRIES: u64 = 40;
/// The first run stops printing after this entry and blocks, so the kill lands mid-stream.
const FIRST_RUN_LAST: u64 = 20;
const DEADLINE: Duration = Duration::from_secs(60);

fn cursor(index: u64) -> String {
    format!("s=synthetic;i={index}")
}

/// Journal export text for entries `0..=ENTRIES`, each separated by a blank line.
fn fixture() -> String {
    (0..=ENTRIES)
        .map(|index| {
            format!(
                "__CURSOR={}\n__REALTIME_TIMESTAMP={}\nPRIORITY=6\n_SYSTEMD_UNIT=app.service\n_HOSTNAME=host-a\nMESSAGE=entry {index}\n\n",
                cursor(index),
                1_700_000_000_000_000 + index
            )
        })
        .collect()
}

/// A `journalctl` stand-in: prints the fixture entries after `--after-cursor`, or nothing for
/// `--lines=0`, then blocks as `--follow` does. `SPARROW_JOURNAL_LAST` bounds the output.
const FAKE_JOURNALCTL: &str = r#"#!/bin/sh
after=""
for arg in "$@"; do
  case "$arg" in
    --after-cursor=*) after="${arg#--after-cursor=}" ;;
  esac
done
if [ -n "$after" ]; then
  awk -v after="$after" -v last="${SPARROW_JOURNAL_LAST:-}" '
    BEGIN { RS = ""; ORS = "\n\n"; go = 0 }
    {
      c = ""; n = split($0, lines, "\n")
      for (i = 1; i <= n; i++) if (index(lines[i], "__CURSOR=") == 1) c = substr(lines[i], 10)
      if (go) {
        index_of = c; sub(/.*i=/, "", index_of)
        if (last != "" && index_of + 0 > last + 0) exit
        print $0
      } else if (c == after) go = 1
    }' "$SPARROW_JOURNAL_FIXTURE"
fi
exec tail -f /dev/null
"#;

/// A `sparrowd-rpc-fixture` daemon for the test store, killed on drop.
struct Daemon(Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn start_daemon(config: &Path, socket: &Path) -> Daemon {
    let mut daemon = Daemon(
        Command::new(env!("CARGO_BIN_EXE_sparrowd-rpc-fixture"))
            .arg("--config")
            .arg(config)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start the isolated daemon"),
    );
    let deadline = Instant::now() + DEADLINE;
    while tokio::net::UnixStream::connect(socket).await.is_err() {
        assert!(
            daemon.0.try_wait().expect("poll daemon").is_none(),
            "daemon exited before readiness"
        );
        assert!(Instant::now() < deadline, "daemon startup timed out");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    daemon
}

/// One CLI query against the test daemon's Unix socket, as JSON.
fn cli(socket: &Path, args: &[&str]) -> purrdf_lex::json::Value {
    let output = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
        .arg(socket)
        .args(args)
        .output()
        .expect("run the CLI");
    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    purrdf_lex::json::read_slice(&output.stdout, purrdf_lex::json::Limits::default())
        .expect("CLI prints JSON")
}

/// Wait until the daemon reports `count` live records, failing at the deadline.
async fn wait_for_records(socket: &Path, count: u64) -> Result<(), String> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let status = cli(socket, &["status"]);
        let live = status["live_records"].as_str().unwrap_or("0").to_owned();
        if live == count.to_string() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("daemon holds {live} records, expected {count}"));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The forwarder child: runs one instance until it is killed. Driven by [`run_forwarder`].
#[test]
fn forwarder_child() {
    let Some(config) = std::env::var_os("SPARROW_JOURNAL_CONFIG") else {
        return;
    };
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let config = sparrow_server::journal_forward::Config::load(Path::new(&config))
        .expect("forwarder configuration");
    let outcome = runtime.block_on(sparrow_server::journal_forward::run(
        &config,
        sparrow_server::journal_forward::Instance::System,
    ));
    panic!("forwarder returned before it was killed: {outcome:?}");
}

/// Start the forwarder child in its own process group so its `journalctl` dies with it.
fn run_forwarder(dir: &Path, last: Option<u64>) -> Child {
    let bin = dir.join("bin");
    let path = std::env::var_os("PATH").unwrap_or_default();
    let mut command = Command::new(std::env::current_exe().expect("test binary"));
    command
        .args(["--exact", "forwarder_child", "--nocapture"])
        .env("SPARROW_JOURNAL_CONFIG", dir.join("journal.toml"))
        .env("SPARROW_JOURNAL_FIXTURE", dir.join("fixture.txt"))
        .env(
            "PATH",
            format!("{}:{}", bin.display(), path.to_string_lossy()),
        )
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(dir.join("child.err")).expect("child log"))
        .process_group(0);
    match last {
        Some(last) => command.env("SPARROW_JOURNAL_LAST", last.to_string()),
        None => command.env_remove("SPARROW_JOURNAL_LAST"),
    };
    command.spawn().expect("start the forwarder child")
}

/// SIGKILL the forwarder's whole process group, including its `journalctl`.
fn kill_group(child: &mut Child) {
    use rustix::process::{Pid, Signal, kill_process_group};
    let pid = Pid::from_child(child);
    kill_process_group(pid, Signal::KILL).expect("kill the forwarder group");
    child.wait().expect("reap the forwarder");
}

/// INVARIANT: a forwarder killed mid-stream resumes after the last accepted cursor, so the
/// daemon holds each fixture entry exactly once, with its cursor and mapping intact.
#[tokio::test(flavor = "current_thread")]
async fn forwarder_killed_mid_stream_resumes_without_loss_or_duplicates() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let store = dir.path().join("store");
    let socket = store.join("rpc.sock");
    let server = sparrow_test_support::write_server_config(dir.path(), &store, &socket);
    let _daemon = start_daemon(&server, &socket).await;

    let bin = dir.path().join("bin");
    std::fs::create_dir_all(&bin).expect("fake bin");
    let journalctl = bin.join("journalctl");
    std::fs::write(&journalctl, FAKE_JOURNALCTL).expect("write fake journalctl");
    std::fs::set_permissions(&journalctl, std::fs::Permissions::from_mode(0o755))
        .expect("make fake journalctl executable");
    std::fs::write(dir.path().join("fixture.txt"), fixture()).expect("write fixture");

    // Start the stream after entry 0, so the first run has nothing to skip.
    let state = dir.path().join("state");
    std::fs::create_dir_all(state.join("system")).expect("state");
    std::fs::write(state.join("system").join("cursor"), cursor(0)).expect("starting cursor");
    std::fs::write(
        dir.path().join("journal.toml"),
        format!(
            "socket = {:?}\nstate_dir = {:?}\n",
            socket.display().to_string(),
            state.display().to_string()
        ),
    )
    .expect("forwarder configuration");

    let mut first = run_forwarder(dir.path(), Some(FIRST_RUN_LAST));
    wait_for_records(&socket, FIRST_RUN_LAST)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "{error}\nforwarder log:\n{}",
                std::fs::read_to_string(dir.path().join("child.err")).unwrap_or_default()
            )
        });
    kill_group(&mut first);

    // The cursor file names only what the daemon confirmed, so it cannot pass entry 20.
    let saved = std::fs::read_to_string(state.join("system").join("cursor")).unwrap_or_default();
    let saved_index: u64 = saved
        .rsplit("i=")
        .next()
        .and_then(|text| text.parse().ok())
        .unwrap_or(0);
    assert!(
        saved_index <= FIRST_RUN_LAST,
        "cursor file {saved} passed the daemon's entries"
    );

    let mut second = run_forwarder(dir.path(), None);
    wait_for_records(&socket, ENTRIES)
        .await
        .expect("every entry is held");
    kill_group(&mut second);

    // Exactly once: every entry is held, and no cursor appears twice.
    let rows = cli(
        &socket,
        &[
            "values",
            "--source",
            "journal.system",
            "--path",
            "/fields/4/value",
            "--received-from",
            "1970-01-01T00:00:00Z",
            "--scan-budget",
            "100000",
            "--page-size",
            "1000",
        ],
    );
    let rows = rows["rows"].as_array().expect("values rows");
    let mut record_ids: Vec<&str> = rows
        .iter()
        .filter_map(|row| row["record_id"].as_str())
        .collect();
    assert_eq!(
        record_ids.len() as u64,
        ENTRIES,
        "one row per forwarded entry"
    );
    record_ids.sort_unstable();
    record_ids.dedup();
    assert_eq!(record_ids.len() as u64, ENTRIES, "no record is duplicated");

    // The mapping the forwarder defines is the one the daemon reports.
    let mapping = cli(&socket, &["diagnostic-get", "journal.system"]);
    assert_eq!(
        mapping["profile"]["mappings"].as_array().map(Vec::len),
        Some(3),
        "the forwarder defines the service, host and severity mappings"
    );
}
