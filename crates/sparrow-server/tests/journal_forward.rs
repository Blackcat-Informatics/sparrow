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
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

/// Entries in the fixture stream. Entry 0 is the starting cursor and is never forwarded.
const ENTRIES: u64 = 40;
/// The first run stops printing after this entry and blocks, so the kill lands mid-stream.
const FIRST_RUN_LAST: u64 = 20;
const DEADLINE: Duration = Duration::from_secs(60);
/// More than the forwarder's cursor interval, so a cursor write that would happen is made.
const CURSOR_SETTLE: Duration = Duration::from_millis(2_500);

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
/// `--lines=0`, then blocks as `--follow` does. `SPARROW_JOURNAL_LAST` bounds the output. Each
/// start appends the cursor it was given to `<fixture>.after`, and `SPARROW_JOURNAL_GATE` holds
/// the output back until that file exists. The blocked process ends with its parent.
const FAKE_JOURNALCTL: &str = r#"#!/bin/sh
after=""
for arg in "$@"; do
  case "$arg" in
    --after-cursor=*) after="${arg#--after-cursor=}" ;;
  esac
done
echo "$after" >> "$SPARROW_JOURNAL_FIXTURE.after"
if [ -n "${SPARROW_JOURNAL_GATE:-}" ]; then
  while [ ! -e "$SPARROW_JOURNAL_GATE" ]; do sleep 0.05; done
fi
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
exec tail -f --pid=$PPID /dev/null
"#;

/// A `sparrowd-rpc-fixture` daemon for the test store, killed on drop.
struct Daemon(Child);

impl Daemon {
    /// Freeze the daemon: connections stay open but no request is answered.
    fn pause(&self) {
        signal_daemon(&self.0, rustix::process::Signal::STOP);
    }

    fn resume(&self) {
        signal_daemon(&self.0, rustix::process::Signal::CONT);
    }
}

fn signal_daemon(daemon: &Child, signal: rustix::process::Signal) {
    let pid = rustix::process::Pid::from_child(daemon);
    rustix::process::kill_process(pid, signal).expect("signal the isolated daemon");
}

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

/// The forwarder child command, in its own process group so its `journalctl` can die with it.
fn forwarder_command(dir: &Path, last: Option<u64>, gate: Option<&Path>) -> Command {
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
    match gate {
        Some(gate) => command.env("SPARROW_JOURNAL_GATE", gate),
        None => command.env_remove("SPARROW_JOURNAL_GATE"),
    };
    command
}

fn run_forwarder(dir: &Path, last: Option<u64>) -> Child {
    forwarder_command(dir, last, None)
        .spawn()
        .expect("start the forwarder child")
}

/// SIGKILL the forwarder's whole process group, including its `journalctl`.
fn kill_group(child: &mut Child) {
    use rustix::process::{Pid, Signal, kill_process_group};
    let pid = Pid::from_child(child);
    kill_process_group(pid, Signal::KILL).expect("kill the forwarder group");
    child.wait().expect("reap the forwarder");
}

/// An isolated daemon, forwarder state and synthetic `journalctl` in one temporary directory.
struct Rig {
    dir: tempfile::TempDir,
    socket: PathBuf,
    state: PathBuf,
    daemon: Daemon,
}

impl Rig {
    /// The stream starts after entry 0, so the first run has nothing to skip.
    async fn new() -> Self {
        let dir = tempfile::tempdir().expect("temporary directory");
        let store = dir.path().join("store");
        let socket = store.join("rpc.sock");
        let server = sparrow_test_support::write_server_config(dir.path(), &store, &socket);
        let daemon = start_daemon(&server, &socket).await;

        let bin = dir.path().join("bin");
        std::fs::create_dir_all(&bin).expect("fake bin");
        let journalctl = bin.join("journalctl");
        std::fs::write(&journalctl, FAKE_JOURNALCTL).expect("write fake journalctl");
        std::fs::set_permissions(&journalctl, std::fs::Permissions::from_mode(0o755))
            .expect("make fake journalctl executable");
        std::fs::write(dir.path().join("fixture.txt"), fixture()).expect("write fixture");

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
        Self {
            dir,
            socket,
            state,
            daemon,
        }
    }

    fn cursor_file(&self) -> String {
        std::fs::read_to_string(self.state.join("system").join("cursor")).unwrap_or_default()
    }

    fn spool(&self) -> PathBuf {
        self.state.join("system").join("spool")
    }

    /// The cursors the fake `journalctl` was started after, oldest first.
    fn started_after(&self) -> Vec<String> {
        std::fs::read_to_string(self.dir.path().join("fixture.txt.after"))
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    async fn wait_for_records(&self, count: u64) {
        wait_for_records(&self.socket, count)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "{error}\nforwarder log:\n{}",
                    std::fs::read_to_string(self.dir.path().join("child.err")).unwrap_or_default()
                )
            });
    }

    /// Run a forwarder that spools entries 1 to [`FIRST_RUN_LAST`] while the daemon answers
    /// nothing, then SIGKILL it at the checkpoint after a cursor tick has passed. The spool
    /// holds the entries and the daemon has confirmed none of them. The daemon is left
    /// frozen; [`Rig::resume_daemon`] thaws it.
    fn crash_between_spool_append_and_confirmation(&self) {
        let dir = self.dir.path();
        let gate = dir.join("gate");
        let after = dir.join("fixture.txt.after");
        let marker = dir.join("checkpoint");
        let mut command = forwarder_command(dir, Some(FIRST_RUN_LAST), Some(&gate));
        std::thread::scope(|scope| {
            scope.spawn(|| {
                // The fake `journalctl` starts only after the mapping is defined.
                wait_until(|| after.exists(), "the forwarder defined its mapping");
                self.daemon.pause();
                std::fs::write(&gate, b"").expect("release the fake journalctl");
                let last = cursor(FIRST_RUN_LAST);
                wait_until(
                    || spool_holds(&self.spool(), last.as_bytes()),
                    "the spool holds the last entry",
                );
                std::thread::sleep(CURSOR_SETTLE);
                std::fs::write(&marker, b"spooled").expect("write the checkpoint marker");
            });
            sparrow_test_support::crash::kill_at_checkpoint(&mut command, &marker, "spooled")
                .expect("kill the forwarder at the checkpoint");
        });
    }

    fn resume_daemon(&self) {
        self.daemon.resume();
    }

    /// The spool's accepted token, read from the closed spool after the forwarder died.
    async fn spool_accepted_token(&self) -> Vec<u8> {
        use sparrow_client::{Delivery, DeliveryConfig, Endpoint, SpoolConfig};
        let delivery = Delivery::start_async(DeliveryConfig::lossless(
            Endpoint::Unix(self.dir.path().join("unavailable.sock")),
            "journal.system",
            SpoolConfig::new(self.spool()),
        ))
        .await
        .expect("reopen the spool");
        let token = delivery.accepted_token().expect("read the accepted token");
        delivery
            .close_local(Duration::from_secs(5))
            .await
            .expect("close the spool");
        token
    }

    /// The cursor (`/fields/4/value`) of every record the daemon holds.
    fn forwarded_cursors(&self) -> Vec<String> {
        let rows = cli(
            &self.socket,
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
        rows["rows"]
            .as_array()
            .expect("values rows")
            .iter()
            .map(|row| row["lexical"].as_str().expect("cursor value").to_owned())
            .collect()
    }

    /// Every fixture entry is held, each cursor exactly once.
    fn assert_each_entry_held_once(&self) {
        let mut cursors = self.forwarded_cursors();
        assert_eq!(cursors.len() as u64, ENTRIES, "one row per forwarded entry");
        cursors.sort_unstable();
        cursors.dedup();
        assert_eq!(cursors.len() as u64, ENTRIES, "no entry is held twice");
        assert_eq!(
            cursors.first().map(String::as_str),
            Some(cursor(1).as_str())
        );
    }
}

/// Poll `condition` until it holds, failing at the deadline.
fn wait_until(mut condition: impl FnMut() -> bool, what: &str) {
    let deadline = Instant::now() + DEADLINE;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting: {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Whether any file below `dir` contains `needle`.
fn spool_holds(dir: &Path, needle: &[u8]) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries.flatten().any(|entry| {
        let path = entry.path();
        if path.is_dir() {
            spool_holds(&path, needle)
        } else {
            std::fs::read(&path).is_ok_and(|bytes| bytes.windows(needle.len()).any(|w| w == needle))
        }
    })
}

/// INVARIANT: a forwarder killed mid-stream resumes after the last accepted cursor, so the
/// daemon holds each fixture entry exactly once, with its cursor and mapping intact.
#[tokio::test(flavor = "current_thread")]
async fn forwarder_killed_mid_stream_resumes_without_loss_or_duplicates() {
    let rig = Rig::new().await;

    let mut first = run_forwarder(rig.dir.path(), Some(FIRST_RUN_LAST));
    rig.wait_for_records(FIRST_RUN_LAST).await;
    kill_group(&mut first);

    let mut second = run_forwarder(rig.dir.path(), None);
    rig.wait_for_records(ENTRIES).await;
    kill_group(&mut second);

    rig.assert_each_entry_held_once();

    // The mapping the forwarder defines is the one the daemon reports.
    let mapping = cli(&rig.socket, &["diagnostic-get", "journal.system"]);
    assert_eq!(
        mapping["profile"]["mappings"].as_array().map(Vec::len),
        Some(3),
        "the forwarder defines the service, host and severity mappings"
    );
}

/// INVARIANT: the cursor file names only entries the daemon confirmed. Entries the spool
/// accepted but the daemon has not confirmed never move it, and it reaches an entry's cursor
/// exactly when the daemon confirms that entry.
#[tokio::test(flavor = "current_thread")]
async fn cursor_file_never_passes_confirmed_delivery() {
    let rig = Rig::new().await;
    rig.crash_between_spool_append_and_confirmation();

    assert_eq!(
        rig.cursor_file(),
        cursor(0),
        "the cursor file moved past entries the daemon never confirmed"
    );

    rig.resume_daemon();
    let mut second = run_forwarder(rig.dir.path(), None);
    rig.wait_for_records(ENTRIES).await;
    let last = cursor(ENTRIES);
    wait_until(
        || rig.cursor_file() == last,
        "the cursor file names the confirmed entry",
    );
    kill_group(&mut second);
}

/// INVARIANT: a restart resumes after the spool's accepted token when the spool is ahead of
/// the cursor file, so entries the spool already holds are replayed from it and never read
/// from the journal a second time.
#[tokio::test(flavor = "current_thread")]
async fn forwarder_resumes_after_the_spool_token_before_the_cursor_file() {
    let rig = Rig::new().await;
    rig.crash_between_spool_append_and_confirmation();

    // The spool is ahead of the cursor file.
    assert_eq!(
        rig.spool_accepted_token().await,
        cursor(FIRST_RUN_LAST).into_bytes()
    );
    assert_eq!(rig.cursor_file(), cursor(0));

    rig.resume_daemon();
    let mut second = run_forwarder(rig.dir.path(), None);
    wait_until(
        || rig.started_after().len() == 2,
        "the restarted forwarder started journalctl",
    );
    assert_eq!(
        rig.started_after().last().map(String::as_str),
        Some(cursor(FIRST_RUN_LAST).as_str()),
        "the restart resumed from the cursor file instead of the spool's token"
    );
    rig.wait_for_records(ENTRIES).await;
    kill_group(&mut second);

    rig.assert_each_entry_held_once();
}
