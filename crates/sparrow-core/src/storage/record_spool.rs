// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

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
const INDEX_BYTES: u64 = 16;
const FILE_OVERHEAD: u64 = 2 * 65536;

fn index_entry(offset: u64, length: usize) -> [u8; INDEX_BYTES as usize] {
    let mut entry = [0; INDEX_BYTES as usize];
    entry[..8].copy_from_slice(&offset.to_le_bytes());
    entry[8..].copy_from_slice(&(length as u64).to_le_bytes());
    entry
}

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
    pub(super) scope: Option<Value>,
    pub(super) cancellation: super::record_read::QueryCancellation,
    file: File,
    index: File,
    count: usize,
    written: u64,
    charge: Charge,
    started: Instant,
    groups: Option<BTreeMap<String, u64>>,
    data_bytes: u64,
    order: Option<Vec<(u64, String, u64, usize)>>,
    sort_bytes: usize,
}

pub(super) struct Finished {
    pub scope: Value,
    pub rows: Arc<Rows>,
    pub groups: Option<BTreeMap<String, u64>>,
}

impl Builder {
    pub(super) fn new(writer: &Writer, grouped: bool, event_order: bool) -> Result<Self, Error> {
        let allowance = writer.config.disk_budget - writer.config.maintenance_reserve;
        let wanted = DISK_BYTES.min(allowance / catalog_lease::MAX_HANDLES as u64);
        // Under the free-space floor a read stages a bounded few bytes, never the reserve (R5).
        let staged = writer.query_staging.load(Ordering::Relaxed);
        let disk_bytes = writer.space.read_staging_allowance(wanted, staged)?;
        if disk_bytes <= FILE_OVERHEAD && wanted > FILE_OVERHEAD {
            return Err(Error::new(
                "STORAGE_PRESSURE",
                format!(
                    "{}record query staging is limited while writes are paused; retry when free space returns",
                    super::space::FLOOR_REFUSAL_PREFIX
                ),
            ));
        }
        if disk_bytes <= FILE_OVERHEAD
            || writer.physical_bytes()?.saturating_add(disk_bytes) > allowance
        {
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
        let index = staging_file(writer.db.path())?;
        Ok(Self {
            scope: None,
            cancellation: Default::default(),
            file,
            index,
            count: 0,
            written: 0,
            charge,
            started: Instant::now(),
            groups: grouped.then(BTreeMap::new),
            data_bytes: disk_bytes - FILE_OVERHEAD,
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
            // Include geometric vector growth and the sort's owned identity.
            let bytes = id
                .capacity()
                .saturating_add(3 * std::mem::size_of::<(u64, String, u64, usize)>());
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
            || self
                .written
                .saturating_add(encoded.len() as u64)
                .saturating_add((self.count as u64 + 1).saturating_mul(INDEX_BYTES))
                > self.data_bytes
            || self.started.elapsed() >= Duration::from_millis(catalog_lease::LEASE_MS)
        {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "record query exhausted row/disk/time staging admission; no page published",
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
        self.index
            .write_all(&index_entry(self.written, encoded.len()))
            .map_err(io_error)?;
        if let Some((millis, id)) = order {
            self.order
                .as_mut()
                .ok_or_else(|| {
                    Error::new("STORAGE_FAILURE", "record staging sort owner disappeared")
                })?
                .push((millis, id, self.written, encoded.len()));
        }
        self.count += 1;
        self.written += encoded.len() as u64;
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<Finished, Error> {
        self.checkpoint()?;
        if let Some(mut order) = self.order.take() {
            let bytes = order.capacity() * std::mem::size_of::<(u64, String, u64, usize)>()
                + order
                    .iter()
                    .map(|(_, id, _, _)| id.capacity())
                    .sum::<usize>();
            if bytes > catalog_lease::CAPTURE_BYTES {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "record source-time sort exceeds resident admission; no page published",
                ));
            }
            order.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
            for (position, (_, _, offset, length)) in order.into_iter().enumerate() {
                self.index
                    .write_all_at(&index_entry(offset, length), position as u64 * INDEX_BYTES)
                    .map_err(io_error)?;
            }
        }
        self.checkpoint()?;
        // Queries are ephemeral, so no fsync or durability claim is appropriate.
        let allocated = self
            .file
            .metadata()
            .map_err(io_error)?
            .blocks()
            .saturating_mul(512)
            .saturating_add(
                self.index
                    .metadata()
                    .map_err(io_error)?
                    .blocks()
                    .saturating_mul(512),
            )
            // Delayed allocation can report fewer blocks before writeback.
            // Retain the complete data/overhead charge without making the
            // ephemeral file a durable query artifact.
            .max(
                self.written
                    .saturating_add(self.count as u64 * INDEX_BYTES)
                    .saturating_add(FILE_OVERHEAD)
                    .next_multiple_of(4096),
            );
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
            index: self.index,
            count: self.count,
            written: self.written,
            boundaries: Vec::new(),
            _charge: self.charge,
        });
        Ok(Finished {
            scope: self
                .scope
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "query scope was not captured"))?,
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
            scope: Some(Value::Null),
            cancellation: Default::default(),
            file: staging_file(dir.path()).unwrap(),
            index: staging_file(dir.path()).unwrap(),
            count: 0,
            written: 0,
            charge: Charge {
                counter: counter.clone(),
                bytes: DISK_BYTES,
            },
            started: Instant::now(),
            groups: None,
            data_bytes: DISK_BYTES - FILE_OVERHEAD,
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
        let length = page[0].len();
        held.index
            .write_all_at(&index_entry(held.written + 1, length), 0)
            .unwrap();
        assert_eq!(held.page(0, 1).unwrap_err().code, "STORAGE_FAILURE");
        held.index.set_len(0).unwrap();
        assert_eq!(held.page(0, 1).unwrap_err().code, "STORAGE_FAILURE");
        held.index.write_all_at(&index_entry(0, length), 0).unwrap();
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
    index: File,
    count: usize,
    written: u64,
    boundaries: Vec<usize>,
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
        self.count
    }
    pub(super) fn capacity(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.id.capacity()
            + self.boundaries.capacity() * std::mem::size_of::<usize>()
    }
    fn offsets(&self, start: usize, end: usize) -> Result<Vec<(u64, usize)>, Error> {
        if start > end || end > self.count || end - start > super::record_read::PAGE_ITEMS {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "record spool index range exceeds admitted page",
            ));
        }
        let mut bytes = vec![0; (end - start) * INDEX_BYTES as usize];
        self.index
            .read_exact_at(&mut bytes, start as u64 * INDEX_BYTES)
            .map_err(io_error)?;
        bytes
            .as_chunks::<{ INDEX_BYTES as usize }>()
            .0
            .iter()
            .map(|entry| {
                let offset = u64::from_le_bytes(entry[..8].try_into().expect("fixed index width"));
                let length = u64::from_le_bytes(entry[8..].try_into().expect("fixed index width"));
                if length > super::record_read::ROW_BYTES as u64
                    || offset
                        .checked_add(length)
                        .is_none_or(|end| end > self.written)
                {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "record spool index points outside admitted row data",
                    ));
                }
                Ok((offset, length as usize))
            })
            .collect()
    }
    pub(super) fn configure_pages(
        mut self: Arc<Self>,
        metadata_bytes: usize,
        page_size: usize,
        cancellation: &super::record_read::QueryCancellation,
    ) -> Result<Arc<Self>, Error> {
        let rows = Arc::get_mut(&mut self).ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "record spool pagination must precede shared publication",
            )
        })?;
        let mut admission = super::record_read::PageAdmission::new(metadata_bytes, page_size);
        rows.boundary(0)?;
        for start in (0..rows.count).step_by(super::record_read::PAGE_ITEMS) {
            cancellation.checkpoint()?;
            let offsets = rows.offsets(
                start,
                rows.count.min(start + super::record_read::PAGE_ITEMS),
            )?;
            for (position, (_, length)) in offsets.into_iter().enumerate() {
                if !admission.admit(length)? {
                    rows.boundary(start + position)?;
                    admission = super::record_read::PageAdmission::new(metadata_bytes, page_size);
                    if !admission.admit(length)? {
                        return Err(Error::new(
                            "STORAGE_FAILURE",
                            "record spool cannot admit its first page row",
                        ));
                    }
                }
            }
        }
        if rows.count != 0 {
            rows.boundary(rows.count)?;
        }
        cancellation.checkpoint()?;
        Ok(self)
    }
    fn boundary(&mut self, position: usize) -> Result<(), Error> {
        // Price geometric growth before allocating, including any live old buffer.
        if self.boundaries.len().saturating_add(1).saturating_mul(24) > catalog_lease::CAPTURE_BYTES
        {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "record spool page map exceeds resident admission",
            ));
        }
        self.boundaries.push(position);
        Ok(())
    }
    pub(super) fn page_end(&self, start: usize) -> Result<usize, Error> {
        let position = self.boundaries.binary_search(&start).map_err(|_| {
            Error::new("INVALID_ARGUMENT", "cursor is not a captured page boundary")
        })?;
        Ok(self
            .boundaries
            .get(position + 1)
            .copied()
            .unwrap_or(self.count))
    }
    pub(super) fn is_boundary(&self, start: usize) -> bool {
        self.boundaries.binary_search(&start).is_ok()
    }
    pub(super) fn page(&self, start: usize, end: usize) -> Result<Vec<Vec<u8>>, Error> {
        self.offsets(start, end)?
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
