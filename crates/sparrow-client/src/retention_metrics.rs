// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One certificate for the complete logical retention account.

use crate::{Error, sparrow_capnp as wire};

/// Admission for catalog visits, catalog construction and raw payload verification.
#[derive(Clone, Copy, Debug)]
pub struct RetentionReadLimits {
    /// Catalog/ownership work units, at most 100,000.
    pub max_visits: u32,
    /// Aggregate catalog/map construction bytes, at most 32 MiB.
    pub catalog_bytes: u32,
    /// Raw payload bytes verified, at most 1 GiB.
    pub payload_bytes: u32,
}

impl Default for RetentionReadLimits {
    fn default() -> Self {
        Self {
            max_visits: 10000,
            catalog_bytes: 33554432,
            payload_bytes: 67108864,
        }
    }
}

impl RetentionReadLimits {
    /// Refuse invalid work admission before sending a request.
    pub fn validate(self) -> Result<(), Error> {
        if self.max_visits == 0
            || self.max_visits > 100000
            || self.catalog_bytes == 0
            || self.catalog_bytes > 33554432
            || self.payload_bytes == 0
            || self.payload_bytes > 1073741824
        {
            return Err(Error::InvalidArgument(
                "retention metrics require positive bounded visits, catalog and payload admission",
            ));
        }
        Ok(())
    }
}

/// Certified generated contract owned once, bounded to 4 KiB.
pub struct RetentionMetrics {
    message: capnp::message::Builder<capnp::message::HeapAllocator>,
}

impl RetentionMetrics {
    /// Borrow the immutable typed result.
    pub fn reader(&self) -> Result<wire::retention_metrics::Reader<'_>, Error> {
        Ok(self.message.get_root_as_reader()?)
    }

    fn certify(
        value: wire::retention_metrics::Reader<'_>,
        store: &str,
        discovery: &crate::Discovery,
        limits: RetentionReadLimits,
    ) -> Result<Self, Error> {
        let size = value.total_size()?;
        if size.word_count > 512
            || size.cap_count != 0
            || value.get_store_id()?.to_str()? != store
            || store != discovery.store_id
        {
            return Err(Error::Protocol(
                "retention account changed store or exceeded its wire admission",
            ));
        }
        let records = value.get_live_records();
        let artifacts = value.get_live_artifacts();
        let live = records
            .checked_add(artifacts)
            .ok_or(Error::Protocol("retention occurrence count overflow"))?;
        let visits = value.get_visits();
        let owned = value.get_owned_payload_bytes();
        let payload_live = value.get_live_payload_bytes();
        let pin = value.get_pin_held_payload_bytes();
        let retirable = value.get_retirable_payload_bytes();
        let other = value.get_unclassified_payload_bytes();
        let chunks = value.get_payload_chunks();
        let pinned = value
            .get_pinned_records()
            .checked_add(value.get_pinned_artifacts())
            .ok_or(Error::Protocol("retention pinned count overflow"))?;
        if (payload_live > 0 && live == 0)
            || (pin > 0 && pinned == 0)
            || visits == 0
            || visits > u64::from(limits.max_visits)
            || live > visits
            || chunks > visits
            || value.get_pinned_records() > records
            || value.get_pinned_artifacts() > artifacts
            || owned > u64::from(limits.payload_bytes)
            || pin > payload_live
            || payload_live
                .checked_add(retirable)
                .is_none_or(|n| n > owned)
            || retirable.checked_add(other).is_none_or(|n| n > owned)
            || (owned == 0) != (chunks == 0)
            || chunks > owned
            || chunks.checked_mul(65536).is_none_or(|n| owned > n)
        {
            return Err(Error::Protocol(
                "retention counts contradict ownership or requested admission",
            ));
        }
        let oldest = match value.get_oldest_received_at()?.which()? {
            wire::optional_instant::Absent(()) => None,
            wire::optional_instant::Value(clock) => Some(clock?.get_unix_millis()),
        };
        let now = value.get_as_of()?.get_unix_millis();
        let expected = match oldest {
            Some(oldest) => now
                .checked_sub(oldest)
                .ok_or(Error::Protocol("retention clock is later than as_of"))?,
            None => 0,
        };
        if oldest.is_some() != (live != 0) || value.get_retained_window_ms() != expected {
            return Err(Error::Protocol(
                "retention window contradicts its occurrence clocks",
            ));
        }
        let mut message = capnp::message::Builder::new_default();
        message.set_root(value)?;
        Ok(Self { message })
    }
}

/// Read a complete bounded raw-payload account; no maintenance or retention mutation.
pub async fn get_retention_metrics(
    client: &wire::sparrow::Client,
    store: &str,
    limits: RetentionReadLimits,
) -> Result<RetentionMetrics, Error> {
    limits.validate()?;
    if store.is_empty() || store.len() > 128 {
        return Err(Error::InvalidArgument(
            "retention metrics require a bounded store identity",
        ));
    }
    let discovery = crate::Discovery::fetch(client).await?;
    let mut call = client.retention_metrics_request();
    let mut query = call.get().init_query();
    query.set_store_id(store);
    query.set_schema_version(crate::SCHEMA_VERSION);
    query.set_max_visits(limits.max_visits);
    query.set_catalog_bytes(limits.catalog_bytes);
    query.set_payload_bytes(limits.payload_bytes);
    let response = call.send().promise.await?;
    crate::receipt::certify_result(response.get()?.get_result()?, |value| {
        RetentionMetrics::certify(value, store, &discovery, limits)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn certificate_refuses_changed_identity_false_ownership_and_window() {
        let fixture = crate::receipt::client_discovery();
        let discovery = crate::Discovery::decode(fixture.get_root_as_reader().unwrap()).unwrap();
        for fault in 0..10 {
            let mut message = capnp::message::Builder::new_default();
            let mut out = message.init_root::<wire::retention_metrics::Builder<'_>>();
            out.set_store_id("store:test");
            out.reborrow().init_as_of().set_unix_millis(5);
            out.set_live_records(1);
            out.set_pinned_records(1);
            out.reborrow()
                .init_oldest_received_at()
                .init_value()
                .set_unix_millis(1);
            out.set_retained_window_ms(4);
            out.set_owned_payload_bytes(5);
            out.set_live_payload_bytes(5);
            out.set_pin_held_payload_bytes(5);
            out.set_payload_chunks(1);
            out.set_visits(5);
            match fault {
                1 => out.set_store_id("store:changed"),
                2 => out.set_pinned_records(2),
                3 => out.set_retirable_payload_bytes(1),
                4 => out.set_pin_held_payload_bytes(6),
                5 => out.reborrow().init_oldest_received_at().set_absent(()),
                6 => out.set_retained_window_ms(3),
                7 => out
                    .reborrow()
                    .init_oldest_received_at()
                    .init_value()
                    .set_unix_millis(6),
                8 => out.set_visits(100001),
                9 => out.set_payload_chunks(0),
                _ => {}
            }
            let certified = RetentionMetrics::certify(
                message.get_root_as_reader().unwrap(),
                "store:test",
                &discovery,
                RetentionReadLimits::default(),
            );
            assert_eq!(certified.is_ok(), fault == 0, "fault {fault}");
        }
    }
}
