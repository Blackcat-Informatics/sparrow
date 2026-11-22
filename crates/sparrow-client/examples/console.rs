// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Discover a destination at startup, or pass an endpoint to explicitly enable delivery.
use sparrow_client::{Console, Delivery, DeliveryConfig, Endpoint, sparrow_eprintln};
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let console = match std::env::args().nth(1) {
        Some(value) => {
            let config = DeliveryConfig::configured(Endpoint::parse(&value)?, "console-example");
            Console::install(Some(Delivery::start_async(config).await?))?
        }
        None => Console::install_auto("console-example").await?,
    };
    log::set_boxed_logger(Box::new(console.clone()))?;
    log::set_max_level(log::LevelFilter::Info);

    let files = 12;
    let span = tracing::info_span!("import", files);
    span.in_scope(|| {
        sparrow_eprintln!("Reading {files} files");
        tracing::info!(files, bytes = 847216_u64, "Import complete");
        log::warn!(target: "upload", retry_secs = 2_u64; "Upload deferred\nConnection refused");
    });
    drop(span);
    let stats = console.flush().await?;
    if stats.invalid + stats.refused > 0 {
        return Err(
            std::io::Error::other(format!("console refused observations: {stats:?}")).into(),
        );
    }
    if let Some(delivery) = console.delivery() {
        delivery.shutdown_async(Duration::from_secs(5)).await?;
    }
    Ok(())
}
