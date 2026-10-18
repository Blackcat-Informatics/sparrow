// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! CLI and stdio MCP entry point over the shared generated daemon client.

use purrdf_lex::json;
use sparrow_client::{Client, Endpoint};
use sparrow_server::{run_cli, run_mcp};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() < 2 {
        return Err(
            "usage: sparrow ENDPOINT COMMAND; ENDPOINT is a Unix socket path or tcp://IP:PORT; mcp --stdio starts the agent adapter".into(),
        );
    }
    let endpoint = Endpoint::parse(&args[0])?;
    tokio::task::LocalSet::new()
        .run_until(async {
            if args[1..] == ["mcp", "--stdio"] {
                run_mcp(&endpoint).await?;
            } else {
                let client = Client::connect_endpoint(&endpoint).await?;
                let output = run_cli(client.capability(), &args[1..]).await?;
                println!("{}", json::write_compact(&output));
                if output.get("execution_status").and_then(json::Value::as_str) == Some("failed") {
                    return Err("query failed; see the emitted typed failure receipt".into());
                }
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .await
}
