// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Export one certified exact range over the shared Unix or public TCP client.

use std::io::Write;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let all = std::env::args().skip(1).collect::<Vec<_>>();
    let (endpoint, args) = sparrow_client::Endpoint::from_args(&all)?;
    if args.len() != 3 {
        return Err(
            "usage: bytes [--secret-file PATH] ENDPOINT EVIDENCE_ID OFFSET LENGTH; writes exact bytes to stdout".into(),
        );
    }
    let offset = args[1].parse()?;
    let length = args[2].parse()?;
    let bytes = tokio::task::LocalSet::new()
        .run_until(async {
            let client = sparrow_client::Client::connect_endpoint(&endpoint).await?;
            client.bytes(&args[0], offset, length).await
        })
        .await?;
    std::io::stdout().lock().write_all(&bytes)?;
    Ok(())
}
