// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Bounded coordination of an owned child stopped inside a test-only checkpoint.

use std::{
    fs,
    io::{self, Write},
    path::Path,
    process::{Child, Command, ExitStatus},
    time::{Duration, Instant},
};

/// Serialize process creation with test listeners that briefly cross fork/exec.
pub static PROCESS_START: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Kill and reap one owned test child, retaining both failures if both occur.
pub fn kill_and_reap(child: &mut Child) -> io::Result<ExitStatus> {
    match (child.kill(), child.wait()) {
        (Ok(()), status) => status,
        (Err(kill), Ok(_)) => Err(kill),
        (Err(kill), Err(wait)) => Err(io::Error::other(format!(
            "kill failed: {kill}; wait failed: {wait}"
        ))),
    }
}

/// Enter only the checkpoint selected for this test child. No filesystem sync is
/// added: this marker coordinates a process crash, not a power-loss experiment.
pub fn checkpoint(phase: &str) {
    let selected = std::env::var_os("SPARROW_TEST_CRASH_POINT");
    let marker = std::env::var_os("SPARROW_TEST_CRASH_READY");
    let Some(selected) = selected else {
        assert!(
            marker.is_none(),
            "crash marker requires a selected checkpoint"
        );
        return;
    };
    let marker = marker.expect("selected crash checkpoint requires its marker path");
    if selected != phase {
        return;
    }
    let mut ready = fs::File::create_new(marker).expect("create checkpoint marker");
    ready
        .write_all(phase.as_bytes())
        .expect("write checkpoint marker");
    ready.flush().expect("flush checkpoint marker");
    std::thread::sleep(Duration::from_secs(30));
    panic!("parent did not kill the child at checkpoint {phase}");
}

struct OwnedChild(Child);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        match self.0.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => {}
            Err(error) => eprintln!("test child {} status failed: {error}", self.0.id()),
        }
        if let Err(error) = kill_and_reap(&mut self.0) {
            eprintln!("test child {} cleanup failed: {error}", self.0.id());
        }
    }
}

/// Kill and reap this owned child after its complete checkpoint marker appears.
/// Unexpected exit, malformed marker and the finite deadline are hard failures.
pub fn kill_at_checkpoint(
    command: &mut Command,
    marker: &Path,
    phase: &str,
) -> io::Result<ExitStatus> {
    let mut child = OwnedChild({
        let _guard = PROCESS_START
            .lock()
            .expect("test process-start admission poisoned");
        command.spawn()?
    });
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match fs::read(marker) {
            Ok(bytes) if bytes == phase.as_bytes() => break,
            Ok(bytes) if phase.as_bytes().starts_with(&bytes) => {}
            Ok(_) => return Err(io::Error::other("child wrote a different checkpoint")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        if let Some(status) = child.0.try_wait()? {
            return Err(io::Error::other(format!(
                "child exited before checkpoint {phase}: {status}"
            )));
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("checkpoint {phase} was not reached"),
            ));
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    kill_and_reap(&mut child.0)
}
