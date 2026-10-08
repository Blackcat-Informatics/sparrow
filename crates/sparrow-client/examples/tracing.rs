// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Compose Sparrow with an application-owned subscriber and explicitly drain.
use sparrow_client::{Delivery, DeliveryConfig, TracingLayer};
use std::time::Duration;
use tracing_subscriber::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let endpoint = sparrow_client::Endpoint::parse(&args.next().ok_or("require ENDPOINT SOURCE")?)?;
    let source = args.next().ok_or("require SOCKET SOURCE")?;
    if args.next().is_some() {
        return Err("require SOCKET SOURCE".into());
    }
    let delivery = Delivery::start(DeliveryConfig::for_endpoint(endpoint, source))?;
    let subscriber = tracing_subscriber::registry().with(TracingLayer::new(delivery.clone()));
    tracing::subscriber::with_default(subscriber, || {
        let operation = tracing::info_span!("operation", attempt = 1_u64);
        let _entered = operation.enter();
        tracing::info!(
            ready = true,
            bytes = 64_u64,
            "structured Sparrow tracing example"
        );
    });
    // A dropped observation is a typed error, even when admitted events were durable.
    let watermark = delivery.shutdown(Duration::from_secs(5))?;
    println!("{watermark:?}");
    Ok(())
}
