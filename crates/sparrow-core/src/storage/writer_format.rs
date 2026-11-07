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

// Ingest-socket intake provenance: older binaries cannot drop a record's caller identity.
// Like the archive family it includes the log time law. Archiving is a separate switch, so
// an intake store that also archives has its own family.
const INTAKE: [&[u8]; 11] = [
    b"sparrow.store/110",
    b"sparrow.store/111",
    b"sparrow.store/112",
    b"sparrow.store/113",
    b"sparrow.store/114",
    b"sparrow.store/115",
    b"sparrow.store/116",
    b"sparrow.store/117",
    b"sparrow.store/118",
    b"sparrow.store/119",
    b"sparrow.store/120",
];
const INTAKE_ARCHIVE: [&[u8]; 11] = [
    b"sparrow.store/121",
    b"sparrow.store/122",
    b"sparrow.store/123",
    b"sparrow.store/124",
    b"sparrow.store/125",
    b"sparrow.store/126",
    b"sparrow.store/127",
    b"sparrow.store/128",
    b"sparrow.store/129",
    b"sparrow.store/130",
    b"sparrow.store/131",
];

// Receipt-time event fallback RDF: older binaries cannot publish a record metadata graph
// without its `eventTimeFallback`. Each family lifts one family that existed before it,
// keeping that family's log time, archive and intake switches.
const FALLBACK_TRACING: [&[u8]; 11] = [
    b"sparrow.store/132",
    b"sparrow.store/133",
    b"sparrow.store/134",
    b"sparrow.store/135",
    b"sparrow.store/136",
    b"sparrow.store/137",
    b"sparrow.store/138",
    b"sparrow.store/139",
    b"sparrow.store/140",
    b"sparrow.store/141",
    b"sparrow.store/142",
];
const FALLBACK_LOG_TIME: [&[u8]; 11] = [
    b"sparrow.store/143",
    b"sparrow.store/144",
    b"sparrow.store/145",
    b"sparrow.store/146",
    b"sparrow.store/147",
    b"sparrow.store/148",
    b"sparrow.store/149",
    b"sparrow.store/150",
    b"sparrow.store/151",
    b"sparrow.store/152",
    b"sparrow.store/153",
];
const FALLBACK_ARCHIVE: [&[u8]; 11] = [
    b"sparrow.store/154",
    b"sparrow.store/155",
    b"sparrow.store/156",
    b"sparrow.store/157",
    b"sparrow.store/158",
    b"sparrow.store/159",
    b"sparrow.store/160",
    b"sparrow.store/161",
    b"sparrow.store/162",
    b"sparrow.store/163",
    b"sparrow.store/164",
];
const FALLBACK_INTAKE: [&[u8]; 11] = [
    b"sparrow.store/165",
    b"sparrow.store/166",
    b"sparrow.store/167",
    b"sparrow.store/168",
    b"sparrow.store/169",
    b"sparrow.store/170",
    b"sparrow.store/171",
    b"sparrow.store/172",
    b"sparrow.store/173",
    b"sparrow.store/174",
    b"sparrow.store/175",
];
const FALLBACK_INTAKE_ARCHIVE: [&[u8]; 11] = [
    b"sparrow.store/176",
    b"sparrow.store/177",
    b"sparrow.store/178",
    b"sparrow.store/179",
    b"sparrow.store/180",
    b"sparrow.store/181",
    b"sparrow.store/182",
    b"sparrow.store/183",
    b"sparrow.store/184",
    b"sparrow.store/185",
    b"sparrow.store/186",
];

/// Families the event fallback fence lifts, in the order of their lifted counterparts.
const LIFTED: [[&[u8]; 11]; 5] = [TRACING, LOG_TIME, ARCHIVE, INTAKE, INTAKE_ARCHIVE];
const FALLBACK: [[&[u8]; 11]; 5] = [
    FALLBACK_TRACING,
    FALLBACK_LOG_TIME,
    FALLBACK_ARCHIVE,
    FALLBACK_INTAKE,
    FALLBACK_INTAKE_ARCHIVE,
];

fn position(families: &[[&'static [u8]; 11]; 5], format: &[u8]) -> Option<(usize, usize)> {
    families
        .iter()
        .enumerate()
        .find_map(|(family, identities)| {
            identities
                .iter()
                .position(|identity| *identity == format)
                .map(|index| (family, index))
        })
}

/// The pre-fallback identity a fallback-fenced format lifts; other formats are unchanged.
fn lower(format: &[u8]) -> &[u8] {
    position(&FALLBACK, format).map_or(format, |(family, index)| LIFTED[family][index])
}

/// Re-apply the fallback fence to a transition computed on the lowered identity.
fn relift<'a>(current: &[u8], next: &'a [u8]) -> Result<&'a [u8], Error> {
    if !has_event_fallback(current) {
        return Ok(next);
    }
    enable_event_fallback(next)
}

pub(super) fn has_event_fallback(format: &[u8]) -> bool {
    position(&FALLBACK, format).is_some()
}

/// Fence a current tracing-or-later family for receipt-time fallback RDF; idempotent.
pub(super) fn enable_event_fallback(format: &[u8]) -> Result<&'static [u8], Error> {
    if let Some((family, index)) = position(&FALLBACK, format) {
        return Ok(FALLBACK[family][index]);
    }
    position(&LIFTED, format)
        .map(|(family, index)| FALLBACK[family][index])
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "event fallback RDF requires a tracing or later authority family",
            )
        })
}

/// Every family in order; a position modulo `BASE.len()` names the original authority.
fn families() -> impl Iterator<Item = &'static &'static [u8]> {
    BASE.iter()
        .chain(RDF.iter())
        .chain(ALL_RDF.iter())
        .chain(PROVENANCE.iter())
        .chain(TRACING.iter())
        .chain(LOG_TIME.iter())
        .chain(ARCHIVE.iter())
        .chain(INTAKE.iter())
        .chain(INTAKE_ARCHIVE.iter())
        .chain(FALLBACK.iter().flatten())
}

fn base_index(format: &[u8], purpose: &str) -> Result<usize, Error> {
    families()
        .position(|identity| *identity == format)
        .map(|index| index % BASE.len())
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                format!("{purpose} requires a current authority family"),
            )
        })
}

pub(super) fn has_intake(format: &[u8]) -> bool {
    let format = lower(format);
    INTAKE.contains(&format) || INTAKE_ARCHIVE.contains(&format)
}

/// Fence any current authority family for intake provenance, keeping its archive switch.
pub(super) fn enable_intake(format: &[u8]) -> Result<&'static [u8], Error> {
    let index = base_index(format, "intake provenance fence")?;
    relift(
        format,
        if has_archive(format) {
            INTAKE_ARCHIVE[index]
        } else {
            INTAKE[index]
        },
    )
}

pub(super) fn has_archive(format: &[u8]) -> bool {
    let format = lower(format);
    ARCHIVE.contains(&format) || INTAKE_ARCHIVE.contains(&format)
}

/// Fence any current authority family for archive-before-delete; idempotent.
pub(super) fn enable_archive(format: &[u8]) -> Result<&'static [u8], Error> {
    let index = base_index(format, "archive fence")?;
    relift(
        format,
        if has_intake(format) {
            INTAKE_ARCHIVE[index]
        } else {
            ARCHIVE[index]
        },
    )
}

pub(super) fn has_log_time(format: &[u8]) -> bool {
    let format = lower(format);
    LOG_TIME.contains(&format) || has_archive(format) || has_intake(format)
}

pub(super) fn enable_log_time(format: &[u8]) -> Result<&'static [u8], Error> {
    relift(format, enable_log_time_lowered(lower(format))?)
}

fn enable_log_time_lowered(format: &[u8]) -> Result<&'static [u8], Error> {
    // The archive and intake families already include the log time law; never lower them.
    if let Some(identity) = ARCHIVE
        .iter()
        .chain(INTAKE.iter())
        .chain(INTAKE_ARCHIVE.iter())
        .find(|identity| **identity == format)
    {
        return Ok(identity);
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
    let format = lower(format);
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
            .chain(INTAKE.iter())
            .chain(INTAKE_ARCHIVE.iter())
            .chain(FALLBACK.iter().flatten())
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
    let staged = if has_intake(&current) {
        let index = base_index(base, "intake provenance fence")?;
        if has_archive(&current) {
            INTAKE_ARCHIVE[index]
        } else {
            INTAKE[index]
        }
    } else if has_archive(&current) {
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
    };
    batch.put("store_format", relift(&current, staged)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_authority_transition_preserves_the_artifact_writer_fence() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        for family in [
            RDF,
            ALL_RDF,
            PROVENANCE,
            TRACING,
            LOG_TIME,
            ARCHIVE,
            INTAKE,
            INTAKE_ARCHIVE,
            FALLBACK_TRACING,
            FALLBACK_LOG_TIME,
            FALLBACK_ARCHIVE,
            FALLBACK_INTAKE,
            FALLBACK_INTAKE_ARCHIVE,
        ] {
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
            // Intake and archiving are independent switches; neither drops the other.
            for previous in [base, LOG_TIME[index], INTAKE[index]] {
                assert_eq!(enable_intake(previous).unwrap(), INTAKE[index]);
            }
            for previous in [ARCHIVE[index], INTAKE_ARCHIVE[index]] {
                assert_eq!(enable_intake(previous).unwrap(), INTAKE_ARCHIVE[index]);
            }
            assert_eq!(
                enable_archive(INTAKE[index]).unwrap(),
                INTAKE_ARCHIVE[index]
            );
            for family in [INTAKE, INTAKE_ARCHIVE] {
                assert_eq!(enable_log_time(family[index]).unwrap(), family[index]);
                assert!(has_log_time(family[index]));
            }
        }
        // Older binaries know none of the intake identities, so they refuse an intake store.
        for identity in INTAKE.iter().chain(INTAKE_ARCHIVE.iter()) {
            assert!(
                [BASE, RDF, ALL_RDF, PROVENANCE, TRACING, LOG_TIME, ARCHIVE]
                    .iter()
                    .all(|family| !family.contains(identity))
            );
        }
        // Older binaries know only the earlier families, so they refuse every archive identity.
        for identity in ARCHIVE {
            assert!(
                [BASE, RDF, ALL_RDF, PROVENANCE, TRACING, LOG_TIME]
                    .iter()
                    .all(|family| !family.contains(&identity))
            );
        }
        // The event fallback fence lifts every tracing-or-later family and keeps its switches.
        for (index, base) in BASE.into_iter().enumerate() {
            for (lifted, fallback) in LIFTED.iter().zip(FALLBACK.iter()) {
                assert_eq!(
                    enable_event_fallback(lifted[index]).unwrap(),
                    fallback[index]
                );
                assert_eq!(
                    enable_event_fallback(fallback[index]).unwrap(),
                    fallback[index]
                );
                assert!(has_event_fallback(fallback[index]));
                assert!(!has_event_fallback(lifted[index]));
                assert!(has_tracing(fallback[index]) && has_provenance(fallback[index]));
            }
            assert!(enable_event_fallback(base).is_err());
            assert_eq!(
                enable_log_time(FALLBACK_TRACING[index]).unwrap(),
                FALLBACK_LOG_TIME[index]
            );
            assert_eq!(
                enable_archive(FALLBACK_LOG_TIME[index]).unwrap(),
                FALLBACK_ARCHIVE[index]
            );
            assert_eq!(
                enable_intake(FALLBACK_LOG_TIME[index]).unwrap(),
                FALLBACK_INTAKE[index]
            );
            assert_eq!(
                enable_intake(FALLBACK_ARCHIVE[index]).unwrap(),
                FALLBACK_INTAKE_ARCHIVE[index]
            );
            assert_eq!(
                enable_archive(FALLBACK_INTAKE[index]).unwrap(),
                FALLBACK_INTAKE_ARCHIVE[index]
            );
        }
        // Older binaries know none of the fallback identities, so they refuse a fenced store.
        for identity in FALLBACK.iter().flatten() {
            assert!(
                [
                    BASE,
                    RDF,
                    ALL_RDF,
                    PROVENANCE,
                    TRACING,
                    LOG_TIME,
                    ARCHIVE,
                    INTAKE,
                    INTAKE_ARCHIVE
                ]
                .iter()
                .all(|family| !family.contains(identity))
            );
        }
        assert!(enable(b"sparrow.store/5").is_err());
    }
}
