// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Durable opaque watch administration state. The server owns its strict codec.

use super::{Error, Store};
use rocksdb::WriteBatch;

/// Bound on the complete watch catalog, including finite mutation receipts.
pub const MAX_WATCH_REGISTRY_BYTES: usize = 512 * 1024;
pub(super) const FORMAT: &[u8] = b"sparrow.store/11";

impl Store {
    /// Capture the exact administrative catalog without a second configuration codec.
    pub fn watch_registry(&self) -> Result<Option<Vec<u8>>, Error> {
        let writer = self.lock()?;
        let value = writer.db.get("watch_registry")?;
        if value
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > MAX_WATCH_REGISTRY_BYTES)
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "watch registry is empty or exceeds its persistent bound",
            ));
        }
        Ok(value)
    }

    /// Atomically replace one exact catalog capture and fence older writers.
    /// A durable acknowledgment covers the complete catalog and its format marker.
    pub fn watch_registry_replace(
        &self,
        expected: Option<&[u8]>,
        next: &[u8],
    ) -> Result<(), Error> {
        if next.is_empty() || next.len() > MAX_WATCH_REGISTRY_BYTES {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "watch registry requires 1..524288 bytes",
            ));
        }
        let writer = self.lock()?;
        if writer.db.get("watch_registry")?.as_deref() != expected {
            return Err(Error::new(
                "CONFLICT",
                "watch registry changed since its captured revision",
            ));
        }
        let mut batch = WriteBatch::default();
        batch.put("watch_registry", next);
        if writer.db.get("store_format")?.as_deref() != Some(super::handoff::FORMAT) {
            batch.put("store_format", FORMAT);
        }
        writer.admit_import_batch(&batch)?;
        writer.commit(batch)
    }
}
