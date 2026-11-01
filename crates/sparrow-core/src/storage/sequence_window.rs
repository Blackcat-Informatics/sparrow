// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Shared finite sequence-range accounting and bounded catalog registry decoding.

use super::{Catalog, Error, Store, Writer, producer_stream::ProducerGap};
use std::collections::BTreeSet;

pub(super) trait Window: Catalog {
    const PREFIX: &'static str;
    fn metadata(&self) -> (&str, u64);
}

pub(super) fn retained<T: Window>(
    store: &Store,
    id: &str,
    now: u64,
    registry: impl FnOnce(&Writer) -> Result<Vec<T>, Error>,
) -> Result<T, Error> {
    if !id.starts_with(T::PREFIX) || id.len() > 128 {
        return Err(Error::new(
            "INVALID_ARGUMENT",
            format!("window lookup requires an opaque {} ID", T::PREFIX),
        ));
    }
    let writer = store.lock()?;
    registry(&writer)?
        .into_iter()
        .find(|window| {
            let (window_id, expires_at) = window.metadata();
            window_id == id && expires_at > now
        })
        .ok_or_else(|| {
            Error::new(
                "NOT_FOUND",
                "finite window absent or expired; record positions remain authoritative",
            )
        })
}

pub(super) fn record_gap(
    count: &mut u64,
    gaps: &mut Vec<ProducerGap>,
    complete: &mut bool,
    first: u64,
    last: u64,
) -> Result<(), Error> {
    let size = last
        .checked_sub(first)
        .and_then(|n| n.checked_add(1))
        .filter(|_| first != 0)
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "invalid sequence gap bounds"))?;
    *count = count
        .checked_add(size)
        .ok_or_else(|| Error::new("RESOURCE_LIMIT", "sequence gap count overflow"))?;
    gaps.push(ProducerGap { first, last });
    if gaps.len() > super::producer_stream::MAX_PRODUCER_GAPS {
        gaps.remove(0);
        *complete = false;
    }
    Ok(())
}

pub(super) fn registry<T: Catalog, K: Ord>(
    writer: &Writer,
    key: &str,
    max_bytes: usize,
    max_items: usize,
    identity: impl Fn(&T) -> (String, K),
) -> Result<Vec<T>, Error> {
    let bytes = writer.db.get(key)?.ok_or_else(|| {
        Error::new(
            "STORAGE_FAILURE",
            format!("{key} missing; restore the fenced catalog"),
        )
    })?;
    if bytes.len() > max_bytes {
        return Err(Error::new(
            "STORAGE_FAILURE",
            format!("{key} exceeds encoded admission"),
        ));
    }
    let value = super::parse(&bytes)?;
    let rows = value
        .as_array()
        .ok_or_else(|| Error::new("STORAGE_FAILURE", format!("{key} is not an array")))?;
    if rows.len() > max_items {
        return Err(Error::new(
            "STORAGE_FAILURE",
            format!("{key} exceeds window admission"),
        ));
    }
    let mut ids = BTreeSet::new();
    let mut identities = BTreeSet::new();
    rows.iter()
        .map(|row| {
            let state = T::decode(row)?;
            let (id, identity) = identity(&state);
            if !ids.insert(id) || !identities.insert(identity) {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    format!("duplicate {key} identity"),
                ));
            }
            Ok(state)
        })
        .collect()
}
