// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Preserve the original authority family beneath the artifact-RDF writer fence.

use super::Error;
use rocksdb::{DB, WriteBatch};

// Each new identity adds artifact RDF to exactly one existing authority family.
const BASE: [&[u8]; 11] = [
    b"sparrow.store/33",
    b"sparrow.store/34",
    b"sparrow.store/35",
    b"sparrow.store/36",
    b"sparrow.store/37",
    b"sparrow.store/38",
    b"sparrow.store/39",
    b"sparrow.store/40",
    b"sparrow.store/41",
    b"sparrow.store/42",
    b"sparrow.store/43",
];
const RDF: [&[u8]; 11] = [
    b"sparrow.store/44",
    b"sparrow.store/45",
    b"sparrow.store/46",
    b"sparrow.store/47",
    b"sparrow.store/48",
    b"sparrow.store/49",
    b"sparrow.store/50",
    b"sparrow.store/51",
    b"sparrow.store/52",
    b"sparrow.store/53",
    b"sparrow.store/54",
];
const ALL_RDF: [&[u8]; 11] = [
    b"sparrow.store/55",
    b"sparrow.store/56",
    b"sparrow.store/57",
    b"sparrow.store/58",
    b"sparrow.store/59",
    b"sparrow.store/60",
    b"sparrow.store/61",
    b"sparrow.store/62",
    b"sparrow.store/63",
    b"sparrow.store/64",
    b"sparrow.store/65",
];
const PROVENANCE: [&[u8]; 11] = [
    b"sparrow.store/66",
    b"sparrow.store/67",
    b"sparrow.store/68",
    b"sparrow.store/69",
    b"sparrow.store/70",
    b"sparrow.store/71",
    b"sparrow.store/72",
    b"sparrow.store/73",
    b"sparrow.store/74",
    b"sparrow.store/75",
    b"sparrow.store/76",
];

const TRACING: [&[u8]; 11] = [
    b"sparrow.store/77",
    b"sparrow.store/78",
    b"sparrow.store/79",
    b"sparrow.store/80",
    b"sparrow.store/81",
    b"sparrow.store/82",
    b"sparrow.store/83",
    b"sparrow.store/84",
    b"sparrow.store/85",
    b"sparrow.store/86",
    b"sparrow.store/87",
];

const LOG_TIME: [&[u8]; 11] = [
    b"sparrow.store/88",
    b"sparrow.store/89",
    b"sparrow.store/90",
    b"sparrow.store/91",
    b"sparrow.store/92",
    b"sparrow.store/93",
    b"sparrow.store/94",
    b"sparrow.store/95",
    b"sparrow.store/96",
    b"sparrow.store/97",
    b"sparrow.store/98",
];

// Archive-before-delete: older binaries cannot retire evidence the archive has not covered.
const ARCHIVE: [&[u8]; 11] = [
    b"sparrow.store/99",
    b"sparrow.store/100",
    b"sparrow.store/101",
    b"sparrow.store/102",
    b"sparrow.store/103",
    b"sparrow.store/104",
    b"sparrow.store/105",
    b"sparrow.store/106",
    b"sparrow.store/107",
    b"sparrow.store/108",
    b"sparrow.store/109",
];

pub(super) fn has_archive(format: &[u8]) -> bool {
    ARCHIVE.contains(&format)
}

/// Fence any current authority family for archive-before-delete; idempotent.
pub(super) fn enable_archive(format: &[u8]) -> Result<&'static [u8], Error> {
    BASE.iter()
        .chain(RDF.iter())
        .chain(ALL_RDF.iter())
        .chain(PROVENANCE.iter())
        .chain(TRACING.iter())
        .chain(LOG_TIME.iter())
        .chain(ARCHIVE.iter())
        .position(|identity| *identity == format)
        .map(|index| ARCHIVE[index % BASE.len()])
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "archive fence requires a current authority family",
            )
        })
}

pub(super) fn has_log_time(format: &[u8]) -> bool {
    LOG_TIME.contains(&format) || has_archive(format)
}

pub(super) fn enable_log_time(format: &[u8]) -> Result<&'static [u8], Error> {
    // The archive family already includes the log time law; never lower its fence.
    if let Some(index) = ARCHIVE.iter().position(|identity| *identity == format) {
        return Ok(ARCHIVE[index]);
    }
    BASE.iter()
        .chain(RDF.iter())
        .chain(ALL_RDF.iter())
        .chain(PROVENANCE.iter())
        .chain(TRACING.iter())
        .chain(LOG_TIME.iter())
        .position(|identity| *identity == format)
        .map(|index| LOG_TIME[index % BASE.len()])
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "log time mapping requires a current authority family",
            )
        })
}

pub(super) fn has_tracing(format: &[u8]) -> bool {
    TRACING.contains(&format) || has_log_time(format)
}

pub(super) fn enable_tracing(format: &[u8]) -> Result<&'static [u8], Error> {
    BASE.iter()
        .chain(RDF.iter())
        .chain(ALL_RDF.iter())
        .chain(PROVENANCE.iter())
        .chain(TRACING.iter())
        .position(|identity| *identity == format)
        .map(|index| TRACING[index % BASE.len()])
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "tracing RDF requires a current authority family",
            )
        })
}

pub(super) fn has_provenance(format: &[u8]) -> bool {
    PROVENANCE.contains(&format) || has_tracing(format)
}

pub(super) fn enable_provenance(format: &[u8]) -> Result<&'static [u8], Error> {
    BASE.iter()
        .chain(RDF.iter())
        .chain(ALL_RDF.iter())
        .chain(PROVENANCE.iter())
        .position(|identity| *identity == format)
        .map(|index| PROVENANCE[index % BASE.len()])
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "provenance RDF requires a current authority family",
            )
        })
}

pub(super) fn has_all_artifact_rdf(format: &[u8]) -> bool {
    ALL_RDF.contains(&format) || has_provenance(format)
}

pub(super) fn has_artifact_rdf(format: &[u8]) -> bool {
    RDF.contains(&format) || has_all_artifact_rdf(format)
}

/// Authority checks use their original family. Public discovery uses the raw identity.
pub(super) fn read(db: &DB) -> Result<Option<Vec<u8>>, Error> {
    Ok(db.get("store_format")?.map(|raw| {
        RDF.iter()
            .chain(ALL_RDF.iter())
            .chain(PROVENANCE.iter())
            .chain(TRACING.iter())
            .chain(LOG_TIME.iter())
            .chain(ARCHIVE.iter())
            .position(|identity| *identity == raw)
            .map_or(raw.clone(), |index| BASE[index % BASE.len()].to_vec())
    }))
}

pub(super) fn enable_all(format: &[u8]) -> Result<&'static [u8], Error> {
    BASE.iter()
        .chain(RDF.iter())
        .chain(ALL_RDF.iter())
        .position(|identity| *identity == format)
        .map(|index| ALL_RDF[index % BASE.len()])
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "complete artifact RDF requires a current authority family",
            )
        })
}

pub(super) fn enable(format: &[u8]) -> Result<&'static [u8], Error> {
    BASE.iter()
        .position(|identity| *identity == format)
        .map(|index| RDF[index])
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "artifact RDF requires a current authority family",
            )
        })
}

/// Feature transitions must not discard artifact RDF's older-writer exclusion.
pub(super) fn stage(db: &DB, batch: &mut WriteBatch, base: &[u8]) -> Result<(), Error> {
    let current = db
        .get("store_format")?
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "writer format missing"))?;
    batch.put(
        "store_format",
        if has_archive(&current) {
            enable_archive(base)?
        } else if has_log_time(&current) {
            enable_log_time(base)?
        } else if has_tracing(&current) {
            enable_tracing(base)?
        } else if has_provenance(&current) {
            enable_provenance(base)?
        } else if has_all_artifact_rdf(&current) {
            enable_all(base)?
        } else if has_artifact_rdf(&current) {
            enable(base)?
        } else {
            base
        },
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_authority_transition_preserves_the_artifact_writer_fence() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        for family in [RDF, ALL_RDF, PROVENANCE, TRACING, LOG_TIME, ARCHIVE] {
            for (base, format) in BASE.into_iter().zip(family) {
                db.put("store_format", format).unwrap();
                assert_eq!(read(&db).unwrap().as_deref(), Some(base));
                for (index, next) in BASE.into_iter().enumerate() {
                    let mut batch = WriteBatch::default();
                    stage(&db, &mut batch, next).unwrap();
                    db.write(batch).unwrap();
                    assert_eq!(
                        db.get("store_format").unwrap().as_deref(),
                        Some(family[index])
                    );
                    assert_eq!(read(&db).unwrap().as_deref(), Some(next));
                }
            }
        }
        for (index, base) in BASE.into_iter().enumerate() {
            for previous in [base, RDF[index], ALL_RDF[index]] {
                assert_eq!(enable_all(previous).unwrap(), ALL_RDF[index]);
                assert_eq!(enable_provenance(previous).unwrap(), PROVENANCE[index]);
            }
            assert_eq!(
                enable_provenance(PROVENANCE[index]).unwrap(),
                PROVENANCE[index]
            );
            for previous in [base, TRACING[index], LOG_TIME[index], ARCHIVE[index]] {
                assert_eq!(enable_archive(previous).unwrap(), ARCHIVE[index]);
            }
            // Binding a log time profile never lowers an archiving store's fence.
            assert_eq!(enable_log_time(ARCHIVE[index]).unwrap(), ARCHIVE[index]);
        }
        // Older binaries know only the earlier families, so they refuse every archive identity.
        for identity in ARCHIVE {
            assert!(
                [BASE, RDF, ALL_RDF, PROVENANCE, TRACING, LOG_TIME]
                    .iter()
                    .all(|family| !family.contains(&identity))
            );
        }
        assert!(enable(b"sparrow.store/5").is_err());
    }
}
