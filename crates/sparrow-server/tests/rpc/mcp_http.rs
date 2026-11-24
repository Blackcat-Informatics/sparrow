// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Loopback HTTP MCP acceptance against a scratch daemon.

use super::{Duration, ProcessOutput, ProcessStart, Stdio, cli, start, wire, write_server_config};
use purrdf_lex::json::{self, Value};
use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

/// A URL-based MCP client can find a stored log while off-origin and mutating
/// requests are refused before they reach the typed operation.
#[tokio::test(flavor = "current_thread")]
async fn loopback_http_mcp_discovers_and_searches_logs() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let socket = data.join("rpc.sock");
            let config = write_server_config(dir.path(), &data, &socket);
            let _daemon = start(&config, &socket).await;
            let client = sparrow_client::Client::connect(&socket).await.unwrap();
            let receipt = client
                .ingest("http-search", "http-fixture", wire::Format::Jsonl, b"{\"message\":\"needle\"}\n")
                .await
                .unwrap();
            assert_eq!(receipt.record_ids.len(), 1);

            let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
                .arg(&socket)
                .args(["mcp", "--http", "127.0.0.1:0"])
                .stdout(Stdio::piped())
                .kill_on_drop(true)
                .admitted_spawn()
                .unwrap();
            let mut ready = String::new();
            tokio::time::timeout(
                Duration::from_secs(10),
                BufReader::new(child.stdout.take().unwrap()).read_line(&mut ready),
            )
            .await
            .expect("HTTP MCP startup timed out")
            .unwrap();
            let address: SocketAddr = ready.trim().strip_prefix("http://").unwrap()
                .strip_suffix("/mcp").unwrap().parse().unwrap();

            let (status, headers, body) = exchange(
                address,
                "POST",
                &address.to_string(),
                "",
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}}"#,
            )
            .await;
            assert_eq!(status, 200);
            assert!(headers.contains("content-type: application/json"));
            assert!(!headers.contains("mcp-session-id:"));
            let response = decode(&body);
            assert_eq!(response["id"].as_u64(), Some(1));
            assert_eq!(response["result"]["protocolVersion"].as_str(), Some("2025-11-25"));

            let host = format!("localhost:{}", address.port());
            let post = |body| exchange(address, "POST", &host, "MCP-Protocol-Version: 2025-11-25\r\n", body);
            let (status, _, body) = post(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#).await;
            assert_eq!(status, 202);
            assert!(body.is_empty());

            let list = r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#;
            let (status, _, body) = post(list).await;
            assert_eq!(status, 200);
            let response = decode(&body);
            let tools = response["result"]["tools"].as_array().unwrap();
            assert!(tools.iter().any(|tool| tool["name"].as_str() == Some("records_search")));
            assert!(tools.iter().all(|tool| tool["annotations"]["readOnlyHint"].as_bool() == Some(true)));
            assert!(!tools.iter().any(|tool| tool["name"].as_str() == Some("pin")));

            let (status, _, body) = post(
                r#"{"jsonrpc":"2.0","id":"search","method":"tools/call","params":{"name":"records_search","arguments":{"source":"http-fixture","text":"needle"}}}"#,
            )
            .await;
            assert_eq!(status, 200);
            let response = decode(&body);
            assert_eq!(response["id"].as_str(), Some("search"));
            assert_eq!(response["result"]["isError"].as_bool(), Some(false), "{response:?}");
            let rows = &response["result"]["structuredContent"]["rows"];
            let expected = cli(&socket, &["records", "--source", "http-fixture", "--text", "needle"]);
            assert_eq!(rows, &expected["rows"]);
            assert_eq!(rows.as_array().unwrap().len(), 1);

            let (status, _, body) = post(
                r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"pin"}}"#,
            )
            .await;
            assert_eq!(status, 200);
            assert_eq!(decode(&body)["error"]["message"].as_str(), Some("unknown tool: pin"));

            let untrusted = [
                ("remote.example", ""),
                (host.as_str(), "Origin: https://remote.example\r\n"),
                (host.as_str(), "Origin: null\r\n"),
                (host.as_str(), "Host: remote.example\r\n"),
            ];
            for (host, headers) in untrusted {
                assert_eq!(exchange(address, "POST", host, headers, list).await.0, 403);
            }
            let local_origin = format!("Origin: http://{host}\r\n");
            assert_eq!(exchange(address, "POST", &host, &local_origin, list).await.0, 200);
            assert_eq!(exchange(address, "GET", &host, "", "").await.0, 405);
            assert_eq!(exchange(address, "POST", &host, "MCP-Protocol-Version: invalid\r\n", list).await.0, 400);
            assert_eq!(post(&" ".repeat(128 * 1024 + 1)).await.0, 413);
            child.kill().await.unwrap();

            let refused = super::Command::new(env!("CARGO_BIN_EXE_sparrow-rpc-fixture"))
                .arg(&socket)
                .args(["mcp", "--http", "0.0.0.0:0"])
                .admitted_output()
                .unwrap();
            assert!(!refused.status.success());
            assert!(String::from_utf8(refused.stderr).unwrap().contains("requires a loopback address"));
        })
        .await;
}

async fn exchange(
    address: SocketAddr,
    method: &str,
    host: &str,
    extra_headers: &str,
    body: &str,
) -> (u16, String, String) {
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let message = format!(
        "{method} /mcp HTTP/1.1\r\nHost: {host}\r\nAccept: application/json, text/event-stream\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{extra_headers}\r\n{body}",
        body.len(),
    );
    stream.write_all(message.as_bytes()).await.unwrap();
    let mut response = String::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        stream.read_to_string(&mut response),
    )
    .await
    .expect("HTTP MCP response timed out")
    .unwrap();
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    let status = headers.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, headers.to_ascii_lowercase(), body.to_owned())
}

fn decode(body: &str) -> Value {
    json::read_slice(body.as_bytes(), json::Limits::default()).unwrap()
}
