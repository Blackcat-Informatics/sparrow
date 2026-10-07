// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Regression fixtures for the installed CLI gate, independent of sibling repos.

use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// An exclusively created fixture root, removed after the test.
struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "sparrow-census-gate-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).expect("create an exclusive fixture root");
        let fixture = Self(root);
        fixture.write("helpers-ledger.toml", "");
        fixture
    }

    fn write(&self, path: &str, text: &str) {
        let path = self.0.join(path);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("create parents");
        std::fs::write(path, text).expect("write fixture");
    }

    fn package(&self, name: &str, source: &str) {
        self.write(
            &format!("crates/{name}/Cargo.toml"),
            &format!("[package]\nname = \"{name}\"\nversion = \"0.0.0\"\n"),
        );
        self.write(&format!("crates/{name}/src/lib.rs"), source);
    }

    fn check(&self, cross: bool) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_helper-census"));
        command.args(["--root"]).arg(&self.0).arg("--check");
        if cross {
            command
                .args(["--cross", "--external-root"])
                .arg(self.0.join("external"));
        }
        command.output().expect("run the real census binary")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).expect("remove fixture root");
    }
}

#[test]
fn an_empty_local_tree_cannot_pass_the_gate() {
    let fixture = Fixture::new();
    let output = fixture.check(false);
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("census of nothing"));
}

#[test]
fn the_gate_detects_a_renamed_local_algorithm() {
    let fixture = Fixture::new();
    fixture.package(
        "first",
        "pub fn total(values: &[u64]) -> u64 { let mut result = 0; for value in values { result += value * value + 7; } result }",
    );
    assert!(fixture.check(false).status.success());
    fixture.package(
        "second",
        "pub fn sum(items: &[u64]) -> u64 { let mut output = 0; for item in items { output += item * item + 7; } output }",
    );
    let output = fixture.check(false);
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("isomorphic bodies"), "{stderr}");
    assert!(stderr.contains("first::total"), "{stderr}");
    assert!(stderr.contains("second::sum"), "{stderr}");
}

#[test]
fn support_helpers_in_separate_test_targets_are_checked() {
    let fixture = Fixture::new();
    fixture.package("subject", "pub struct Subject;");
    fixture.write(
        "crates/subject/tests/first.rs",
        "fn total(values: &[u64]) -> u64 { let mut result = 0; for value in values { result += value * value + 7; } result }\n#[test] fn case() { assert_eq!(total(&[2]), 11); }",
    );
    fixture.write(
        "crates/subject/tests/second.rs",
        "fn sum(items: &[u64]) -> u64 { let mut output = 0; for item in items { output += item * item + 7; } output }\n#[test] fn case() { assert_eq!(sum(&[2]), 11); }",
    );
    let output = fixture.check(false);
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("test_crate::first::total"), "{stderr}");
    assert!(stderr.contains("test_crate::second::sum"), "{stderr}");
}

#[test]
fn a_purrdf_owned_hex_copy_fails_but_adoption_passes() {
    let fixture = Fixture::new();
    fixture.write(
        "helpers-ledger.toml",
        r#"[[job]]
id = "hex"
summary = "Base16 encoding belongs to PurRDF."
home = "purrdf:purrdf_hash::hex::encode"
entry_points = []
spec = "RFC 4648 section 8"
vectors = []
bench = []
enforced = true
[job.forbidden]
constants = []
fingerprints = ["rule:hex-format-loop"]
names = ['^(un)?hex$']
"#,
    );
    fixture.write(
        "external/crates/hash/Cargo.toml",
        "[package]\nname = \"purrdf-hash\"\nversion = \"0.0.0\"\n",
    );
    fixture.write(
        "external/crates/hash/src/lib.rs",
        "pub mod hex { pub fn encode(bytes: &[u8]) -> String { String::new() } }",
    );
    fixture.package(
        "consumer",
        "pub fn render(bytes: &[u8]) -> String { purrdf_hash::hex::encode(bytes) }",
    );
    assert!(fixture.check(true).status.success());
    fixture.write(
        "crates/consumer/src/lib.rs",
        "pub fn render(bytes: &[u8]) -> String { let mut text = String::new(); for byte in bytes { text.push_str(&format!(\"{byte:02x}\")); } text }",
    );
    let output = fixture.check(true);
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("job `hex`"), "{stderr}");
    assert!(stderr.contains("outside home purrdf-hash"), "{stderr}");
}

#[test]
fn missing_pinned_reference_and_empty_external_tree_refuse_the_scan() {
    let fixture = Fixture::new();
    fixture.package("consumer", "pub struct Consumer;");
    let pinned = Command::new(env!("CARGO_BIN_EXE_helper-census"))
        .arg("--root")
        .arg(&fixture.0)
        .args(["--cross", "--check"])
        .output()
        .expect("run the pinned gate");
    assert_eq!(pinned.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&pinned.stderr).contains("registry/compatibility.toml"));
    std::fs::create_dir(fixture.0.join("external")).expect("empty external root");
    let empty = fixture.check(true);
    assert_eq!(empty.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&empty.stderr).contains("cross scan of nothing"));
}
