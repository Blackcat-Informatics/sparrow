// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Journal export acceptance through the CLI and its real daemon.

use super::{Command, ProcessOutput, cli, start, write_server_config};
use purrdf_lex::json::{self, Value};
use std::fs;

/// A complete export produces searchable records with native diagnostics and recoverable
/// field values. Replays preserve IDs, and malformed exports publish no records.
#[tokio::test(flavor = "current_thread")]
async fn journal_export_import_preserves_fields_and_searches_diagnostics() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("store");
    let socket = data.join("rpc.sock");
    let config = write_server_config(dir.path(), &data, &socket);
    let _daemon = start(&config, &socket).await;
    let export = dir.path().join("input.export");
    let mut payload = b"MESSAGE=heartbeat\nPRIORITY=6\n\n_SYSTEMD_UNIT=worker.service\n_HOSTNAME=node\nPRIORITY=3\n__REALTIME_TIMESTAMP=1790000000123456\nTAG=one\nTAG=two\nMESSAGE\n".to_vec();
    let message = b"multiline\njob failed";
    payload.extend_from_slice(&(message.len() as u64).to_le_bytes());
    payload.extend_from_slice(message);
    payload.extend_from_slice(b"\nBINARY\n");
    payload.extend_from_slice(&3_u64.to_le_bytes());
    payload.extend_from_slice(&[0xff, 0, b'\n']);
    payload.extend_from_slice(b"\n\n");
    fs::write(&export, &payload).unwrap();

    let args = [
        "ingest",
        "journal-run",
        "journal",
        "journal-export",
        export.to_str().unwrap(),
    ];
    let receipt = cli(&socket, &args);
    let replay = cli(&socket, &args);
    assert_eq!(receipt["format"].as_str(), Some("jsonl"));
    assert_eq!(receipt["artifact_id"], replay["artifact_id"]);
    assert_eq!(receipt["record_ids"], replay["record_ids"]);
    assert_eq!(receipt["record_ids"].as_array().unwrap().len(), 2);

    let search = cli(
        &socket,
        &["records", "--source", "journal", "--text", "multiline"],
    );
    let rows = search["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let diagnostics = rows[0]["diagnostics"]["fields"].as_array().unwrap();
    let known = |dimension| {
        diagnostics
            .iter()
            .find(|row| row["dimension"].as_str() == Some(dimension))
            .and_then(|row| row["value"].as_str())
    };
    assert_eq!(known("service"), Some("worker.service"));
    assert_eq!(known("host"), Some("node"));
    assert_eq!(known("severity"), Some("3"));

    let output = dir.path().join("records.jsonl");
    cli(
        &socket,
        &[
            "bytes",
            receipt["store_id"].as_str().unwrap(),
            receipt["artifact_id"].as_str().unwrap(),
            "0",
            receipt["byte_length"].as_str().unwrap(),
            output.to_str().unwrap(),
        ],
    );
    let normalized = fs::read_to_string(output).unwrap();
    let record = json::read_slice(
        normalized.lines().nth(1).unwrap().as_bytes(),
        json::Limits::default(),
    )
    .unwrap();
    let journal_values = |name: &str, key: &str| {
        record["journal_fields"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|field| field["name"].as_str() == Some(name))
            .map(|field| field[key].as_str().unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(journal_values("TAG", "text"), ["one", "two"]);
    assert_eq!(journal_values("MESSAGE", "text"), ["multiline\njob failed"]);
    assert_eq!(journal_values("BINARY", "base64"), ["/wAK"]);
    assert_eq!(
        journal_values("__REALTIME_TIMESTAMP", "text"),
        ["1790000000123456"]
    );

    for (data, reason) in [
        (
            &b"MESSAGE=first\n\nBINARY\n\x04\0\0\0\0\0\0\0a"[..],
            "entry 2: truncated binary field",
        ),
        (&b"LPKSHHRH"[..], "journalctl --file FILE --output=export"),
    ] {
        fs::write(&export, data).unwrap();
        let refused = Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
            .arg(&socket)
            .args([
                "ingest",
                "invalid-export",
                "bad-journal",
                "journal-export",
                export.to_str().unwrap(),
            ])
            .admitted_output()
            .unwrap();
        assert!(!refused.status.success());
        assert!(String::from_utf8_lossy(&refused.stderr).contains(reason));
    }
    let profile = cli(&socket, &["diagnostic-get", "bad-journal"]);
    assert_eq!(profile["profile"], Value::Null);
}
