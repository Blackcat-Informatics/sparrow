// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! CLI and stdio MCP entry point over the shared generated daemon client.

use purrdf_lex::json;
use sparrow_server::{connect, run_cli, run_mcp};
use std::path::Path;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() < 2 {
        return Err(
            "usage: sparrow SOCKET_PATH COMMAND; mcp --stdio starts the agent adapter".into(),
        );
    }
    tokio::task::LocalSet::new()
        .run_until(async {
            if args[1..] == ["mcp", "--stdio"] {
                run_mcp(Path::new(&args[0])).await?;
            } else {
                let connection = connect(Path::new(&args[0])).await?;
                let output = run_cli(&connection.client, &args[1..]).await?;
                println!("{}", json::write_compact(&output));
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .await
}
