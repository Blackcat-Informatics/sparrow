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

use super::rdf_view::{RdfGraphLayers, RdfReadLimits, ReadSlot, RowAllowance};
use super::{Error, Store};
use purrdf::gts::writer::digest_string;
use purrdf::gts_write::to_writer;
use purrdf::{RdfDataset, RdfLookaside};
use purrdf_core::{ContentDigest, ir::import::DatasetImporter};

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
/// default query limits would refuse an ordinary segment. The request and row ceilings are a
/// fixed base, a fixed allowance per entry and [`ARCHIVE_ROW_ALLOWANCE`] for each stored graph
/// row the scope owns, so the read's cost per row cannot grow with the segment: a read that
/// costs more per row than the allowance (as the union read did, at quadratic cost) is refused
/// instead of running for minutes, while a record with a large content graph still prepares.
/// Each allowance is about twice the measured cost. The read's slot renews its lease while it
/// makes progress, so `lease_ms` is the longest it may go without reading, not its length.
fn archive_read(entries: usize) -> RdfReadLimits {
    let entries = entries as u64;
    RdfReadLimits {
        live_bytes: 2 * 1024 * 1024 * 1024,
        requests: BASE_REQUESTS + entries * REQUESTS_PER_ENTRY,
        io_bytes: 8 * 1024 * 1024 * 1024,
        row_visits: BASE_ROWS + entries * ROWS_PER_ENTRY,
        lease_ms: 60_000,
    }
}

/// Request allowance for each record or artifact in a segment, apart from its graph rows;
/// measured cost is about one.
const REQUESTS_PER_ENTRY: u64 = 16;
/// Row-visit allowance for each record or artifact in a segment, apart from its graph rows;
/// measured cost is about eight.
const ROWS_PER_ENTRY: u64 = 16;
/// What each stored graph row in the scope adds to the ceilings. Measured in a release build
/// (#328): 2.9 requests and 2.1 row visits per row for 2,000 log records (276,040 rows), and
/// 4.8 requests and 2.0 row visits per row for one JSON record of 800 keys (12,043 rows). The
/// view verifies each dictionary identity once, not on every row naming it.
const ARCHIVE_ROW_ALLOWANCE: RowAllowance = RowAllowance {
    requests: 10,
    row_visits: 4,
};
/// Fixed request allowance for the scope's own setup.
const BASE_REQUESTS: u64 = 10_000;
/// Fixed row-visit allowance for the scope's own setup.
const BASE_ROWS: u64 = 2_000;

/// One record's link quads: its identity and the two digests of its bytes.
fn push_links(
    builder: &mut purrdf::RdfDatasetBuilder,
    links: &[(String, String, String)],
) -> Result<(), Error> {
    let graph = builder.intern_iri(LINK_GRAPH).map_err(failure)?;
    let sha256 = builder.intern_iri(LINK_SHA256).map_err(failure)?;
    let blake3 = builder.intern_iri(LINK_BLAKE3).map_err(failure)?;
    for (id, sha, blake) in links {
        let subject = builder
            .intern_iri(&format!("urn:sparrow:{id}"))
            .map_err(failure)?;
        for (predicate, value) in [(sha256, sha), (blake3, blake)] {
            let object = builder
                .intern_literal(purrdf::RdfLiteral::simple(value.as_str()))
                .map_err(failure)?;
            builder
                .push_quad(subject, predicate, object, Some(graph))
                .map_err(failure)?;
        }
    }
    Ok(())
}

/// Encode the records `ids` and the artifacts `artifact_ids` (their graphs) and the records'
/// `contents` (id and bytes, one per record) as one GTS file, and count the artifacts that
/// have no graph to carry. Each record's bytes are a blob keyed by BLAKE3, tied to its record
/// by link quads that also carry the SHA-256.
///
/// The stored graphs and the link quads go into one builder: the scoped view is imported
/// once, then the links are added. A union of the two views is not used, because its row
/// de-duplication looks each stored row up again in the scoped view, and a scoped lookup
/// opens a cursor per graph, so the read cost per record grew with the segment.
pub(super) fn encode(
    store: &Store,
    ids: &[String],
    artifact_ids: &[String],
    contents: Vec<(String, Vec<u8>)>,
) -> Result<(Vec<u8>, u64), Error> {
    let mut links = Vec::with_capacity(contents.len());
    let mut blobs = Vec::with_capacity(contents.len());
    for (id, bytes) in contents {
        links.push((
            id,
            ContentDigest::of(&bytes).to_hex(),
            digest_string(&bytes),
        ));
        blobs.push(bytes);
    }
    if ids.is_empty() && artifact_ids.is_empty() {
        let mut builder = purrdf::RdfDatasetBuilder::new();
        push_links(&mut builder, &links)?;
        let dataset = builder.freeze().map_err(failure)?;
        return Ok((write_gts(&dataset, blobs)?, 0));
    }
    // Archive scope: `now` of zero reads evidence past its ordinary deadline, which the
    // deletion gate holds until a segment covers it.
    let (dataset, absent) = store
        .with_rdf_scope_ids(
            ReadSlot::Archive,
            ids,
            artifact_ids,
            RdfGraphLayers::ALL,
            0,
            archive_read(ids.len() + artifact_ids.len()),
            Some(ARCHIVE_ROW_ALLOWANCE),
            |view, absent| {
                let mut builder = purrdf::RdfDatasetBuilder::new();
                DatasetImporter::try_new(&mut builder, view)
                    .and_then(|mut importer| importer.try_append())
                    .map_err(failure)?;
                push_links(&mut builder, &links)?;
                Ok::<_, Error>((builder.freeze().map_err(failure)?, absent.len() as u64))
            },
        )
        .map_err(|error| Error::new(error.code, error.message.clone()))??;
    Ok((write_gts(&dataset, blobs)?, absent))
}
