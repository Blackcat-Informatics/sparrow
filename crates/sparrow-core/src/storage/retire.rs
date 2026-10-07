// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Shared dependency retirement for ordinary expiry and abandoned preparation.

use super::{Error, Record, Writer, parse, rdf_index, read_field};
use rocksdb::WriteBatch;
use std::collections::BTreeMap;

pub(super) struct Retirement<'a> {
    writer: &'a Writer,
    pub batch: WriteBatch,
    rdf: rdf_index::RdfWrite<'a>,
    pub removals: BTreeMap<String, u64>,
    rdf_removals: BTreeMap<String, u64>,
    projection_removals: BTreeMap<String, u64>,
}

impl<'a> Retirement<'a> {
    pub(super) fn new(writer: &'a Writer) -> Result<Self, Error> {
        Ok(Self {
            writer,
            batch: WriteBatch::default(),
            rdf: rdf_index::RdfWrite::new(&writer.db)?,
            removals: BTreeMap::new(),
            rdf_removals: BTreeMap::new(),
            projection_removals: BTreeMap::new(),
        })
    }

    pub(super) fn record(&mut self, record: &mut Record, owners: u64) -> Result<bool, Error> {
        self.payloads(&record.chunks, owners)?;
        self.writer.remove_text(&mut self.batch, &record.id)?;
        if record.chunks.is_empty()
            && record.projection_id.is_empty()
            && record.rdf_projection_id.is_empty()
            && self
                .writer
                .db
                .get(format!("rdf:graph:{}", record.id))?
                .is_none()
        {
            return Ok(false);
        }
        self.rdf.remove(&mut self.batch, &record.id)?;
        if !record.rdf_projection_id.is_empty() {
            *self
                .rdf_removals
                .entry(std::mem::take(&mut record.rdf_projection_id))
                .or_default() += 1;
        }
        record.chunks.clear();
        if !record.projection_id.is_empty() {
            *self
                .projection_removals
                .entry(std::mem::take(&mut record.projection_id))
                .or_default() += 1;
        }

        Ok(true)
    }

    pub(super) fn payloads(&mut self, chunks: &[String], owners: u64) -> Result<(), Error> {
        if ![1, 2].contains(&owners) {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "invalid retirement ownership multiplicity",
            ));
        }
        for digest in chunks {
            *self.removals.entry(digest.clone()).or_default() += owners;
        }
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<WriteBatch, Error> {
        for (id, removed) in self.rdf_removals {
            let key = format!("rdf:projection-ref:{id}");
            let bytes =
                self.writer.db.get(&key)?.ok_or_else(|| {
                    Error::new("STORAGE_FAILURE", "RDF projection reference missing")
                })?;
            let count =
                u64::from_be_bytes(bytes.as_slice().try_into().map_err(|_| {
                    Error::new("STORAGE_FAILURE", "invalid RDF projection counter")
                })?);
            let remaining = count.checked_sub(removed).ok_or_else(|| {
                Error::new("STORAGE_FAILURE", "RDF projection reference underflow")
            })?;
            if remaining == 0 {
                let state = self
                    .writer
                    .db
                    .get(format!("rdf:state:{id}"))?
                    .ok_or_else(|| Error::new("STORAGE_FAILURE", "RDF projection state missing"))?;
                if read_field::<String>(&parse(&state)?, "state")? == "complete" {
                    self.rdf.remove(&mut self.batch, &id)?;
                }
                self.batch.delete(key);
                self.batch.delete(format!("rdf:state:{id}"));
            } else {
                self.batch.put(key, remaining.to_be_bytes());
            }
        }
        self.rdf.finish(&mut self.batch)?;
        let cf = self
            .writer
            .db
            .cf_handle("payload")
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "payload family missing"))?;
        for (projection, removed) in self.projection_removals {
            let key = format!("projection-ref:{projection}");
            let bytes = self
                .writer
                .db
                .get(&key)?
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "projection reference missing"))?;
            let count = u64::from_be_bytes(bytes.as_slice().try_into().map_err(|_| {
                Error::new("STORAGE_FAILURE", "invalid projection reference counter")
            })?);
            let remaining = count
                .checked_sub(removed)
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "projection reference underflow"))?;
            if remaining == 0 {
                self.batch.delete(key);
                self.batch.delete(projection);
            } else {
                self.batch.put(key, remaining.to_be_bytes());
            }
        }
        for (digest, removed) in self.removals {
            let key = format!("ref:{digest}");
            let bytes = self.writer.db.get(&key)?.ok_or_else(|| {
                Error::new("STORAGE_FAILURE", "reference counter missing during expiry")
            })?;
            let count = u64::from_be_bytes(
                bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| Error::new("STORAGE_FAILURE", "invalid reference counter"))?,
            );
            let remaining = count
                .checked_sub(removed)
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "reference counter underflow"))?;
            if remaining == 0 {
                self.batch.delete(&key);
                self.batch.delete_cf(cf, digest);
            } else {
                self.batch.put(key, remaining.to_be_bytes());
            }
        }
        Ok(self.batch)
    }
}
