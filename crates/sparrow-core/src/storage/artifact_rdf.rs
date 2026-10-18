// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Atomic artifact RDF ownership and resumable startup backfill.

use super::{Artifact, Catalog, Error, catalog_upgrade, parse, rdf_index, writer_format};
use rocksdb::{DB, WriteBatch};

const PREFIX: &[u8] = b"artifact-rdf:";
pub(super) const TIME: &str = "artifact-time:";
const PAGE_BYTES: usize = 32 * 1024 * 1024;
pub(super) const ALL_JOB: catalog_upgrade::Job = catalog_upgrade::Job {
    checkpoint: "all-artifact-rdf-index-upgrade",
    source: "artifact:",
    target: TIME.as_bytes(),
    page_bytes: PAGE_BYTES,
    page_entries: 1,
    charge: catalog_charge,
    publish: backfill_all,
};

pub(super) fn time_key(time: u64, id: &str) -> String {
    format!("{TIME}{time:020}:{id}")
}

/// Admit input and native scalar decoding without owning the manifest arrays.
pub(super) fn catalog(
    db: &DB,
    id: &str,
    bytes: &[u8],
    remaining: &mut usize,
) -> Result<super::manifest_retire::Header, Error> {
    *remaining = remaining.checked_sub(bytes.len()).ok_or_else(|| {
        Error::new(
            "RESOURCE_LIMIT",
            "artifact metadata input exceeds catalog admission",
        )
    })?;
    super::manifest_retire::header(bytes, remaining, 0, 0)?.stored(db, id)
}

fn catalog_charge(bytes: &[u8]) -> Result<usize, Error> {
    let mut remaining = PAGE_BYTES.checked_sub(bytes.len()).ok_or_else(|| {
        Error::new(
            "RESOURCE_LIMIT",
            "artifact metadata input exceeds upgrade admission",
        )
    })?;
    let header = super::manifest_retire::header(bytes, &mut remaining, 0, 0)?;
    drop(header);
    Ok(PAGE_BYTES - remaining)
}
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
    )?;
    let format = db
        .get("store_format")?
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "artifact RDF writer format missing"))?;
    if !writer_format::has_all_artifact_rdf(&format) {
        let mut batch = WriteBatch::default();
        catalog_upgrade::seed(db, &ALL_JOB, &mut batch)?;
        batch.put("store_format", writer_format::enable_all(&format)?);
        catalog_upgrade::commit(db, batch)?;
    }
    catalog_upgrade::run(
        db,
        &ALL_JOB,
        Some(&|batch| admit(db, disk_budget, reserve, batch)),
    )
}

fn backfill_all(
    db: &DB,
    identity: &str,
    bytes: &[u8],
    mut charge: usize,
    batch: &mut WriteBatch,
) -> Result<(), Error> {
    let header = catalog(db, identity, bytes, &mut charge)?;
    let artifact = header.artifact;
    let marker = db.get_pinned(format!("artifact-rdf:{identity}"))?;
    let graph = db.get_pinned(format!("rdf:graph:{identity}"))?;
    if artifact.evicted {
        if marker.is_some() || graph.is_some() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "evicted artifact retains RDF ownership during upgrade",
            ));
        }
        return Ok(());
    }
    match (marker, graph) {
        (None, None) => publish(db, batch, &artifact),
        (Some(marker), Some(graph))
            if marker.is_empty() && graph.len() <= 65536 && header.record_count == 0 =>
        {
            batch.put(
                format!("artifact-rdf:{identity}"),
                artifact.received_at.to_be_bytes(),
            );
            batch.put(
                time_key(artifact.received_at, identity),
                identity.as_bytes(),
            );
            Ok(())
        }
        _ => Err(Error::new(
            "STORAGE_FAILURE",
            "artifact RDF upgrade ownership is inconsistent",
        )),
    }
}

pub(super) fn admit(
    db: &DB,
    disk_budget: u64,
    reserve: u64,
    batch: &WriteBatch,
) -> Result<(), Error> {
    let mut publication = super::eviction::Publication::default();
    batch.iterate_cf(&mut publication);
    publication.check()?;
    if publication.owned_rdf_metadata {
        let estimate = super::pressure::data_batch_estimate(batch);
        let ceiling = super::pressure::data_write_ceiling(disk_budget, reserve, estimate)?;
        if super::linked_physical_bytes(db)? > ceiling {
            return Err(Error::new(
                "STORAGE_PRESSURE",
                "owned RDF metadata backfill needs more data-write allowance; increase the disk budget or free space before resuming",
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
    let mut rdf = rdf_index::RdfWrite::new(db)?;
    stage(db, &mut rdf, batch, artifact)?;
    rdf.finish(batch)
}

pub(super) fn stage(
    db: &DB,
    rdf: &mut rdf_index::RdfWrite<'_>,
    batch: &mut WriteBatch,
    artifact: &Artifact,
) -> Result<(), Error> {
    let full = db
        .get_pinned("store_format")?
        .is_some_and(|format| writer_format::has_all_artifact_rdf(&format));
    if artifact.evicted || (!full && !super::recordless::is_live_owner(artifact)?) {
        return Ok(());
    }
    let expected = format!(
        "blake3:{}",
        super::hex::encode(super::blake3::hash(b"").as_bytes())
    );
    if artifact.byte_length == 0 && artifact.payload_id != expected {
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
    rdf.graph(
        batch,
        &artifact.id,
        rdf_index::artifact_metadata(artifact)?.as_ref(),
    )?;
    if full {
        batch.put(
            format!("artifact-rdf:{}", artifact.id),
            artifact.received_at.to_be_bytes(),
        );
        batch.put(
            time_key(artifact.received_at, &artifact.id),
            artifact.id.as_bytes(),
        );
    } else {
        batch.put(format!("artifact-rdf:{}", artifact.id), []);
    }
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
        (Some(marker), Some(graph)) if graph.len() <= 65536 => {
            if marker.len() == 8 {
                let time =
                    u64::from_be_bytes(marker.as_ref().try_into().map_err(|_| {
                        Error::new("STORAGE_FAILURE", "artifact RDF receipt width")
                    })?);
                let key = time_key(time, id);
                if db.get_pinned(&key)?.as_deref() != Some(id.as_bytes()) {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "artifact RDF receipt index is missing or inconsistent",
                    ));
                }
                batch.delete(key);
            } else if !marker.is_empty()
                || db
                    .get_pinned("store_format")?
                    .is_some_and(|format| writer_format::has_all_artifact_rdf(&format))
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "artifact RDF ownership marker has invalid receipt width",
                ));
            }
        }
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

/// Retire artifact-only RDF owners when constructing an offline historical test store.
/// This entry point is absent unless the dev-only test-support feature is selected.
#[cfg(feature = "test-support")]
pub fn retire_artifact_rdf_for_legacy_fixture(db: &DB) -> Result<(), Error> {
    for row in db.prefix_iterator(PREFIX) {
        let (key, _) = row?;
        let Some(id) = key.strip_prefix(PREFIX) else {
            break;
        };
        let id = std::str::from_utf8(id).map_err(|_| {
            Error::new(
                "STORAGE_FAILURE",
                "legacy fixture artifact RDF identity is invalid",
            )
        })?;
        let mut rdf = rdf_index::RdfWrite::new(db)?;
        let mut batch = WriteBatch::default();
        retire(db, &mut rdf, &mut batch, id)?;
        rdf.finish(&mut batch)?;
        catalog_upgrade::commit(db, batch)?;
    }
    Ok(())
}
