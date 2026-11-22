// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! CLI and stdio MCP entry point over the shared generated daemon client.

use purrdf_lex::json;
use sparrow_access::{run_cli, run_mcp};
use sparrow_client::{Client, Endpoint};
mod drain;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let all: Vec<_> = std::env::args().skip(1).collect();
    let usage = "usage: sparrow [--secret-file PATH] ENDPOINT COMMAND; ENDPOINT is a Unix socket path or tcp://IP:PORT; --secret-file authenticates TCP by CHAP; mcp --stdio starts the agent adapter";
    let (endpoint, args) =
        Endpoint::from_args(&all).map_err(|error| format!("{error}; {usage}"))?;
    if args.is_empty() {
        return Err(usage.into());
    }
    tokio::task::LocalSet::new()
        .run_until(async {
            if args == ["mcp", "--stdio"] {
                run_mcp(&endpoint).await?;
            } else {
                let output = match args {
                    [command, rest @ ..] if command == "spool-drain" => {
                        drain::run(endpoint, rest).await?
                    }
                    _ => {
                        let client = if args.first().is_some_and(|arg| arg == "quarantine-replay") {
                            Client::connect_producer(&endpoint).await?
                        } else {
                            Client::connect_endpoint(&endpoint).await?
                        };
                        run_cli(&client, args).await?
                    }
                };
                println!("{}", json::write_compact(&output));
                if output.get("execution_status").and_then(json::Value::as_str) == Some("failed") {
                    return Err("command failed; see the emitted JSON result".into());
                }
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .await
}
