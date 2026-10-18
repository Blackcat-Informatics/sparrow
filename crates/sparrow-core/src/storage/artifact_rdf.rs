// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Atomic recordless-artifact RDF ownership and resumable startup backfill.

use super::{Artifact, Catalog, Error, catalog_upgrade, parse, rdf_index, writer_format};
use rocksdb::{DB, WriteBatch};

const PREFIX: &[u8] = b"artifact-rdf:";
pub(super) const JOB: catalog_upgrade::Job = catalog_upgrade::Job {
    checkpoint: "artifact-rdf-index-upgrade",
    source: "artifact:",
    target: PREFIX,
    page_bytes: 32 * 1024 * 1024,
    // RdfWrite resolves counters from the committed database. Each owner has
    // one atomic batch, never independently resolved counters in a shared page.
    page_entries: 1,
    charge: super::recordless::upgrade_charge,
    publish: backfill,
};

pub(super) fn upgrade(db: &DB, disk_budget: u64, reserve: u64) -> Result<(), Error> {
    let format = db
        .get("store_format")?
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "artifact RDF writer format missing"))?;
    if !writer_format::has_artifact_rdf(&format) {
        if let Some(row) = db.prefix_iterator(b"rdf:graph:artifact:").next()
            && row?.0.starts_with(b"rdf:graph:artifact:")
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "legacy store contains unfenced artifact RDF graph",
            ));
        }
        let mut batch = WriteBatch::default();
        catalog_upgrade::seed(db, &JOB, &mut batch)?;
        batch.put("store_format", writer_format::enable(&format)?);
        catalog_upgrade::commit(db, batch)?;
    }
    catalog_upgrade::run(
        db,
        &JOB,
        Some(&|batch| admit(db, disk_budget, reserve, batch)),
    )
}

fn admit(db: &DB, disk_budget: u64, reserve: u64, batch: &WriteBatch) -> Result<(), Error> {
    let mut publication = super::eviction::Publication::default();
    batch.iterate_cf(&mut publication);
    publication.check()?;
    if publication.artifact_rdf {
        let estimate = super::pressure::data_batch_estimate(batch);
        let ceiling = super::pressure::data_write_ceiling(disk_budget, reserve, estimate)?;
        if super::linked_physical_bytes(db)? > ceiling {
            return Err(Error::new(
                "STORAGE_PRESSURE",
                "artifact RDF backfill needs more data-write allowance; increase the disk budget or free space before resuming",
            ));
        }
    }
    Ok(())
}

fn backfill(
    db: &DB,
    identity: &str,
    bytes: &[u8],
    charge: usize,
    batch: &mut WriteBatch,
) -> Result<(), Error> {
    // The shared native streaming manifest reader validates populated catalogs
    // without allocating their complete record-ID arrays.
    if charge != bytes.len() {
        let artifact = Artifact::decode(&parse(bytes)?)?;
        if artifact.id != identity {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "artifact RDF upgrade identity mismatch",
            ));
        }
        publish(db, batch, &artifact)?;
    }
    Ok(())
}

pub(super) fn publish(db: &DB, batch: &mut WriteBatch, artifact: &Artifact) -> Result<(), Error> {
    if !super::recordless::is_live_owner(artifact)? {
        return Ok(());
    }
    let expected = format!(
        "blake3:{}",
        super::hex::encode(super::blake3::hash(b"").as_bytes())
    );
    if artifact.payload_id != expected {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "recordless artifact payload identity disagrees with empty bytes",
        ));
    }
    if db
        .get_pinned(format!("artifact-rdf:{}", artifact.id))?
        .is_some()
        || db
            .get_pinned(format!("rdf:graph:{}", artifact.id))?
            .is_some()
    {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "artifact RDF owner already exists outside its atomic publication checkpoint",
        ));
    }
    let mut rdf = rdf_index::RdfWrite::new(db)?;
    rdf.graph(
        batch,
        &artifact.id,
        rdf_index::artifact_metadata(artifact)?.as_ref(),
    )?;
    rdf.finish(batch)?;
    batch.put(format!("artifact-rdf:{}", artifact.id), []);
    Ok(())
}

pub(super) fn retire(
    db: &DB,
    rdf: &mut rdf_index::RdfWrite<'_>,
    batch: &mut WriteBatch,
    id: &str,
) -> Result<bool, Error> {
    let marker = db.get_pinned(format!("artifact-rdf:{id}"))?;
    let graph = db.get_pinned(format!("rdf:graph:{id}"))?;
    match (marker, graph) {
        (None, None) => return Ok(false),
        (Some(marker), Some(graph)) if marker.is_empty() && graph.len() <= 65536 => {}
        _ => {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "artifact RDF ownership marker and graph disagree",
            ));
        }
    }
    rdf.remove(batch, id)?;
    batch.delete(format!("artifact-rdf:{id}"));
    Ok(true)
}
