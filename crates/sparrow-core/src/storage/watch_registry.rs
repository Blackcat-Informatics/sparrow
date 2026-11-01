// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Durable opaque watch administration state. The server owns its strict codec.

use super::{Error, Store};
use rocksdb::WriteBatch;

/// Bound on the complete watch catalog, including finite mutation receipts.
pub const MAX_WATCH_REGISTRY_BYTES: usize = 512 * 1024;
pub(super) const FORMAT: &[u8] = b"sparrow.store/35";

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
        if !matches!(
            super::writer_format::read(&writer.db)?.as_deref(),
            Some(super::handoff::FORMAT | super::consumption::TAIL_FORMAT)
        ) {
            super::writer_format::stage(&writer.db, &mut batch, FORMAT)?;
        }
        writer.admit_import_batch(&batch)?;
        writer.commit(batch)
    }
}
