// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Submit a structured event from an ordinary Tokio application.

use sparrow_client::{Delivery, DeliveryConfig, TracingLayer};
use std::time::Duration;
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = match std::env::args_os().nth(1) {
        Some(file) => DeliveryConfig::from_file(file).await?,
        None => DeliveryConfig::local("example.worker"),
    };
    let producer = Delivery::start_async(settings).await?;
    let subscriber = tracing_subscriber::registry().with(TracingLayer::new(producer.clone()));
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(workers = 4_u64, "worker pool ready");
    });
    let receipt = producer.shutdown_async(Duration::from_secs(5)).await?;
    println!("{} events stored", receipt.durable_events);
    Ok(())
}
