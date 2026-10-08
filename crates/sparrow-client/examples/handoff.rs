// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Admit or inspect a saved source handoff over Unix or public TCP.

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 3 || !["admit", "get"].contains(&args[0].as_str()) {
        return Err("usage: handoff admit|get ENDPOINT INTENT.capnp".into());
    }
    let endpoint = sparrow_client::Endpoint::parse(&args[1])?;
    let intent = sparrow_client::CompletedHandoff::read(sparrow_client::open_completed_file(
        std::path::Path::new(&args[2]),
    )?)?;
    let receipt = tokio::task::LocalSet::new()
        .run_until(async {
            let client = sparrow_client::Client::connect_endpoint(&endpoint).await?;
            if args[0] == "get" {
                client.handoff_get(&intent).await
            } else {
                client.handoff(&intent).await
            }
        })
        .await?;
    println!("{receipt:?}");
    Ok(())
}
