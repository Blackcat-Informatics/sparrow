// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Admit or inspect a saved source handoff over Unix or public TCP.

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let usage = "usage: handoff admit|get [--secret-file PATH] ENDPOINT INTENT.capnp";
    let [action, rest @ ..] = args.as_slice() else {
        return Err(usage.into());
    };
    let (endpoint, rest) = sparrow_client::Endpoint::from_args(rest)?;
    let ([path], true) = (rest, ["admit", "get"].contains(&action.as_str())) else {
        return Err(usage.into());
    };
    let intent = sparrow_client::CompletedHandoff::read(sparrow_client::open_completed_file(
        std::path::Path::new(path),
    )?)?;
    let receipt = tokio::task::LocalSet::new()
        .run_until(async {
            let client = sparrow_client::Client::connect_endpoint(&endpoint).await?;
            if action == "get" {
                client.handoff_get(&intent).await
            } else {
                client.handoff(&intent).await
            }
        })
        .await?;
    println!("{receipt:?}");
    Ok(())
}
