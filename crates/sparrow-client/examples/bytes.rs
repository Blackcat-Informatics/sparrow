// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Export one certified exact range over the shared Unix or public TCP client.

use std::io::Write;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 4 {
        return Err(
            "usage: bytes ENDPOINT EVIDENCE_ID OFFSET LENGTH; writes exact bytes to stdout".into(),
        );
    }
    let endpoint = sparrow_client::Endpoint::parse(&args[0])?;
    let offset = args[2].parse()?;
    let length = args[3].parse()?;
    let bytes = tokio::task::LocalSet::new()
        .run_until(async {
            let client = sparrow_client::Client::connect_endpoint(&endpoint).await?;
            client.bytes(&args[1], offset, length).await
        })
        .await?;
    std::io::stdout().lock().write_all(&bytes)?;
    Ok(())
}
