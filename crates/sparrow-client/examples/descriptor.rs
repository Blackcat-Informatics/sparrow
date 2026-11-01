// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Application-owned completed descriptor upload; no source removal or claim authority.

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 6 {
        return Err("usage: descriptor SOCKET REQUEST_ID SOURCE FORMAT FILE".into());
    }
    let format = sparrow_client::parse_format(&args[4])?;
    let mut file = sparrow_client::open_completed_file(std::path::Path::new(&args[5]))?;
    let identity = sparrow_client::CompletedFileIdentity::capture(&file.metadata()?);
    tokio::task::LocalSet::new()
        .run_until(async {
            let client = sparrow_client::Client::connect(std::path::Path::new(&args[1])).await?;
            identity.require(&file)?;
            let receipt = client
                .import_open_file(&args[2], &args[3], format, &mut file)
                .await?;
            identity.require(&file)?;
            println!(
                "store={} artifact={} bytes={} payload={} durability={:?} records={:?}",
                receipt.store_id,
                receipt.id,
                receipt.byte_length,
                receipt.payload_id,
                receipt.durability,
                receipt.record_ids
            );
            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await
}
