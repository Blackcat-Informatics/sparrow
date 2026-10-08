// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Anonymous, quota-admitted immutable record rows. No persistent query files.

use super::{Error, Value, Writer, catalog_lease, json, random_id, read_field};
use std::{
    collections::BTreeMap,
    fs::File,
    io::Write,
    os::unix::fs::{FileExt, MetadataExt},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

pub(super) const DISK_BYTES: u64 = 64 * 1024 * 1024;

fn io_error(error: impl std::fmt::Display) -> Error {
    Error::new("STORAGE_FAILURE", format!("record query staging: {error}"))
}

fn staging_file(path: &std::path::Path) -> Result<File, Error> {
    let fd = rustix::fs::open(
        path,
        rustix::fs::OFlags::TMPFILE | rustix::fs::OFlags::RDWR | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
    )
    .map_err(io_error)?;
    Ok(File::from(fd))
}

#[derive(Debug)]
struct Charge {
    counter: Arc<AtomicU64>,
    bytes: u64,
}
impl Drop for Charge {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

/// Incremental staging owner; an error drops every private row and its charge.
pub(super) struct Builder {
    pub(super) cancellation: super::record_read::QueryCancellation,
    file: File,
    offsets: Vec<(u64, usize)>,
    written: u64,
    charge: Charge,
    started: Instant,
    groups: Option<BTreeMap<String, u64>>,
    data_bytes: u64,
    order: Option<Vec<(u64, String, usize)>>,
    sort_bytes: usize,
}

pub(super) struct Finished {
    pub rows: Arc<Rows>,
    pub groups: Option<BTreeMap<String, u64>>,
}

impl Builder {
    pub(super) fn new(writer: &Writer, grouped: bool, event_order: bool) -> Result<Self, Error> {
        let allowance = writer.config.disk_budget - writer.config.maintenance_reserve;
        let disk_bytes = DISK_BYTES.min(allowance / catalog_lease::MAX_HANDLES as u64);
        if disk_bytes <= 65536 || writer.physical_bytes()?.saturating_add(disk_bytes) > allowance {
            return Err(Error::new(
                "STORAGE_PRESSURE",
                "record query staging would cross its store allowance or maintenance reserve",
            ));
        }
        writer
            .query_staging
            .fetch_add(disk_bytes, Ordering::Relaxed);
        let charge = Charge {
            counter: writer.query_staging.clone(),
            bytes: disk_bytes,
        };
        // Linux O_TMPFILE has no linked pathname, including if the process is killed.
        // The last descriptor closes it on release, absolute expiry or owner shutdown.
        let file = staging_file(writer.db.path())?;
        Ok(Self {
            cancellation: Default::default(),
            file,
            offsets: Vec::new(),
            written: 0,
            charge,
            started: Instant::now(),
            groups: grouped.then(BTreeMap::new),
            data_bytes: disk_bytes - 65536,
            order: event_order.then(Vec::new),
            sort_bytes: 0,
        })
    }

    pub(super) fn push(&mut self, row: Value, order: Option<(u64, String)>) -> Result<(), Error> {
        self.checkpoint()?;
        if self.order.is_some() != order.is_some() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "record query staging order disagrees with its admitted operation",
            ));
        }
        if let Some((_, id)) = &order {
            // Include geometric vector growth and both old/new offset buffers.
            let bytes = id.capacity().saturating_add(112);
            if self.sort_bytes.saturating_add(bytes) > catalog_lease::CAPTURE_BYTES {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "record source-time sort exhausted resident admission; no page published",
                ));
            }
            self.sort_bytes += bytes;
        }
        let encoded = json::write_compact(&row);
        if encoded.len() > super::record_read::ROW_BYTES
            || self.offsets.len() == catalog_lease::MAX_ITEMS
            || self.written.saturating_add(encoded.len() as u64) > self.data_bytes
            || self.started.elapsed() >= Duration::from_millis(catalog_lease::LEASE_MS)
        {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "record query exhausted row/item/disk/time staging admission; no page published",
            ));
        }
        if let Some(groups) = &mut self.groups {
            let artifact = read_field::<String>(&row, "artifact_id")?;
            *groups.entry(artifact).or_default() += 1;
            // Conservative bound before constructing the metadata object.
            if groups.len().saturating_mul(256) > catalog_lease::CAPTURE_BYTES / 2 {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "record artifact count metadata exceeds staging admission; no page published",
                ));
            }
        }
        self.file.write_all(encoded.as_bytes()).map_err(io_error)?;
        if let Some((millis, id)) = order {
            self.order
                .as_mut()
                .ok_or_else(|| {
                    Error::new("STORAGE_FAILURE", "record staging sort owner disappeared")
                })?
                .push((millis, id, self.offsets.len()));
        }
        self.offsets.push((self.written, encoded.len()));
        self.written += encoded.len() as u64;
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<Finished, Error> {
        self.checkpoint()?;
        if let Some(mut order) = self.order.take() {
            let bytes = order.capacity() * std::mem::size_of::<(u64, String, usize)>()
                + order.iter().map(|(_, id, _)| id.capacity()).sum::<usize>()
                + self.offsets.capacity() * std::mem::size_of::<(u64, usize)>();
            if bytes > catalog_lease::CAPTURE_BYTES {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "record source-time sort exceeds resident admission; no page published",
                ));
            }
            order.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
            self.offsets = order
                .into_iter()
                .map(|(_, _, position)| self.offsets[position])
                .collect();
        }
        self.checkpoint()?;
        // Queries are ephemeral, so no fsync or durability claim is appropriate.
        let allocated = self
            .file
            .metadata()
            .map_err(io_error)?
            .blocks()
            .saturating_mul(512)
            // Delayed allocation can report fewer blocks before writeback.
            // Retain the complete data/overhead charge without making the
            // ephemeral file a durable query artifact.
            .max(self.written.saturating_add(65536).next_multiple_of(4096));
        if allocated > self.charge.bytes {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "record query staging allocation exceeds admitted disk bytes",
            ));
        }
        self.charge
            .counter
            .fetch_sub(self.charge.bytes - allocated, Ordering::Relaxed);
        self.charge.bytes = allocated;
        let rows = Arc::new(Rows {
            id: random_id("query-spool")?,
            file: self.file,
            offsets: self.offsets,
            _charge: self.charge,
        });
        Ok(Finished {
            rows,
            groups: self.groups,
        })
    }

    pub(super) fn checkpoint(&self) -> Result<(), Error> {
        self.cancellation.checkpoint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anonymous_pages_fail_closed_and_last_owner_releases_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let counter = Arc::new(AtomicU64::new(DISK_BYTES));
        let mut builder = Builder {
            cancellation: Default::default(),
            file: staging_file(dir.path()).unwrap(),
            offsets: Vec::new(),
            written: 0,
            charge: Charge {
                counter: counter.clone(),
                bytes: DISK_BYTES,
            },
            started: Instant::now(),
            groups: None,
            data_bytes: DISK_BYTES - 65536,
            order: None,
            sort_bytes: 0,
        };
        builder
            .push(
                purrdf_lex::json::Object::new()
                    .with("row", "original")
                    .into(),
                None,
            )
            .unwrap();
        let rows = builder.finish().unwrap().rows;
        let allocated = counter.load(Ordering::Relaxed);
        assert!(allocated > 0 && allocated < DISK_BYTES);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        let held = rows.clone();
        drop(rows);
        assert_eq!(counter.load(Ordering::Relaxed), allocated);
        let page = held.page(0, 1).unwrap();
        assert_eq!(
            super::super::parse(&page[0]).unwrap()["row"].as_str(),
            Some("original")
        );
        held.file.set_len(0).unwrap();
        assert_eq!(held.page(0, 1).unwrap_err().code, "STORAGE_FAILURE");
        drop(held);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }
}

#[derive(Debug)]
pub(super) struct Rows {
    id: String,
    file: File,
    offsets: Vec<(u64, usize)>,
    _charge: Charge,
}
impl PartialEq for Rows {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Rows {}

impl Rows {
    pub(super) fn len(&self) -> usize {
        self.offsets.len()
    }
    pub(super) fn capacity(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.id.capacity()
            + self.offsets.capacity() * std::mem::size_of::<(u64, usize)>()
    }
    pub(super) fn lengths(&self) -> impl Iterator<Item = usize> + '_ {
        self.offsets.iter().map(|(_, bytes)| *bytes)
    }
    pub(super) fn page(&self, start: usize, end: usize) -> Result<Vec<Vec<u8>>, Error> {
        self.offsets[start..end]
            .iter()
            .map(|(offset, len)| {
                let mut bytes = vec![0; *len];
                self.file
                    .read_exact_at(&mut bytes, *offset)
                    .map_err(io_error)?;
                Ok(bytes)
            })
            .collect()
    }
}
