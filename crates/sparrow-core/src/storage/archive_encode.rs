// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The one segment encoder: records and their graphs to one GTS file.
//!
//! This is the only module that knows the segment format. PurRDF's GTS `Writer` is called at
//! exactly one place, `write_gts`; it holds the whole file resident, so the segment cap
//! (`ARCHIVE_MAX_SEGMENT_BYTES`) and the hold bound are memory limits. They relax when PurRDF
//! issue 1257 (a streaming writer) lands, which is a repin plus that one call. GTS binds no
//! resource to a blob, so the record to blob links are written here as quads in
//! `urn:sparrow:graph:archive-links`; they go when PurRDF offers the link.

use super::rdf_view::{RdfGraphLayers, RdfReadLimits};
use super::{Error, Store};
use purrdf::gts::writer::digest_string;
use purrdf::gts_write::to_writer;
use purrdf::{RdfDataset, RdfLookaside};
use purrdf_core::{ContentDigest, ScopeBinding, UnionDatasetView, ViewLimits, dataset_from_view};

const LINK_GRAPH: &str = "urn:sparrow:graph:archive-links";
const LINK_SHA256: &str = "urn:sparrow:vocab:payloadSha256";
const LINK_BLAKE3: &str = "urn:sparrow:vocab:payloadBlake3";
const PROFILE: &str = "sparrow-archive";
/// Blobs from this size on are written as independent 64 KiB zstd frames.
const RSYNCABLE_FROM: usize = 8 * 1024 * 1024;

fn failure(error: impl std::fmt::Debug) -> Error {
    Error::new(
        "STORAGE_FAILURE",
        format!("archive encoding failed: {error:?}"),
    )
}

/// The single use of PurRDF's GTS writer: the RDF graphs, then each record's bytes as a
/// compressed blob frame. `to_gts` is not used: it drops blob bytes.
fn write_gts(dataset: &RdfDataset, blobs: Vec<Vec<u8>>) -> Result<Vec<u8>, Error> {
    let mut writer = to_writer(dataset, &RdfLookaside::default(), PROFILE).map_err(failure)?;
    for bytes in blobs {
        let chain = if bytes.len() >= RSYNCABLE_FROM {
            "zstd-rsyncable"
        } else {
            "zstd"
        };
        writer
            .add_blob_transformed(
                bytes,
                Some("application/octet-stream"),
                Some("record"),
                &[chain.to_owned()],
                None,
            )
            .map_err(failure)?;
    }
    Ok(writer.into_bytes())
}

/// Ceilings for the archive's own read: a segment's content projections can be large, so the
/// default query limits would refuse an ordinary segment. Row, request and byte ceilings are
/// the store's own bounds, not a throttle; the lease is the longest the read admits.
const ARCHIVE_READ: RdfReadLimits = RdfReadLimits {
    live_bytes: 2 * 1024 * 1024 * 1024,
    requests: 1 << 32,
    io_bytes: 8 * 1024 * 1024 * 1024,
    row_visits: 1 << 32,
    lease_ms: 60_000,
};

/// Encode the records `ids` and the artifacts `artifact_ids` (their graphs) and the records'
/// `contents` (id and bytes, one per record) as one GTS file, and count the artifacts that
/// have no graph to carry. Each record's bytes are a blob keyed by BLAKE3, tied to its record
/// by link quads that also carry the SHA-256.
pub(super) fn encode(
    store: &Store,
    ids: &[String],
    artifact_ids: &[String],
    contents: Vec<(String, Vec<u8>)>,
) -> Result<(Vec<u8>, u64), Error> {
    let mut links = purrdf::RdfDatasetBuilder::new();
    let graph = links.intern_iri(LINK_GRAPH).map_err(failure)?;
    let sha256 = links.intern_iri(LINK_SHA256).map_err(failure)?;
    let blake3 = links.intern_iri(LINK_BLAKE3).map_err(failure)?;
    let mut blobs = Vec::with_capacity(contents.len());
    for (id, bytes) in contents {
        let subject = links
            .intern_iri(&format!("urn:sparrow:{id}"))
            .map_err(failure)?;
        for (predicate, value) in [
            (sha256, ContentDigest::of(&bytes).to_hex()),
            (blake3, digest_string(&bytes)),
        ] {
            let object = links
                .intern_literal(purrdf::RdfLiteral::simple(value))
                .map_err(failure)?;
            links
                .push_quad(subject, predicate, object, Some(graph))
                .map_err(failure)?;
        }
        blobs.push(bytes);
    }
    let links = links.freeze().map_err(failure)?;
    if ids.is_empty() && artifact_ids.is_empty() {
        return Ok((write_gts(&links, blobs)?, 0));
    }
    // Archive scope: `now` of zero reads evidence past its ordinary deadline, which the
    // deletion gate holds until a segment covers it.
    let (dataset, absent) = store
        .with_rdf_scope_ids(
            ids,
            artifact_ids,
            RdfGraphLayers::ALL,
            0,
            ARCHIVE_READ,
            |view, absent| {
                let union = UnionDatasetView::new(
                    view,
                    &*links,
                    (ScopeBinding::Independent, ScopeBinding::Independent),
                    ViewLimits::default(),
                )
                .map_err(failure)?;
                Ok::<_, Error>((
                    dataset_from_view(&union).map_err(failure)?,
                    absent.len() as u64,
                ))
            },
        )
        .map_err(|error| Error::new(error.code, error.message.clone()))??;
    Ok((write_gts(&dataset, blobs)?, absent))
}
