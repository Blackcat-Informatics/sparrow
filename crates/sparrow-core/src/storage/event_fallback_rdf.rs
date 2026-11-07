// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Receipt-time event fallback facts for record metadata graphs written before schema 69.
//!
//! The facts are derived from each graph alone, as the provenance backfill does, so a
//! graph prepared by an unfinished staged import is covered before its record is public.
//! A record graph carries `eventTime` exactly when its record has a source instant;
//! without one, the graph's own `receivedAt` is the receipt-time placement.

use super::{Error, catalog_upgrade, parse, rdf_index, writer_format};
use purrdf::{TermFactory, TermValue};
use rocksdb::{DB, WriteBatch};

/// Decoding admission of one graph; its decoded terms are released before the next graph.
const GRAPH_BYTES: usize = 32 * 1024 * 1024;
/// One page shares an RDF writer and commits once. It ends at whichever bound comes first:
/// graphs, their stored manifest bytes, or the page batch. The batch bound leaves room
/// under the writer's 64 MiB staging admission for one more graph of the maximum size.
const PAGE_ENTRIES: usize = 4096;
const PAGE_INPUT: usize = 16 * 1024 * 1024;
const PAGE_BATCH: usize = 16 * 1024 * 1024;
/// The backfill's durable checkpoint, also read by SPARQL scope capture.
pub(super) const CHECKPOINT: &str = "event-fallback-rdf-upgrade";
const JOB: catalog_upgrade::Job<Fallback> = catalog_upgrade::Job {
    checkpoint: CHECKPOINT,
    source: "rdf:graph:record:",
    target: b"event-fallback-rdf:",
    page_bytes: PAGE_INPUT,
    page_entries: PAGE_ENTRIES,
    charge: |bytes| Ok(bytes.len()),
    publish: Fallback,
};

/// Fence older writers into `batch` and seed the backfill that adds `eventTimeFallback` and
/// `eventTimeBasis` to every retained record graph whose source time is unknown. The
/// backfill runs online, one page per maintenance admission; stored records are only read.
pub(super) fn fence(db: &DB, format: &[u8], batch: &mut WriteBatch) -> Result<Vec<u8>, Error> {
    catalog_upgrade::seed(db, &JOB, batch)?;
    if !super::any_key(db, JOB.source.as_bytes())? {
        // No record graph to rebuild: the backfill is complete as it starts.
        let complete = catalog_upgrade::Progress {
            after: None,
            complete: true,
        };
        super::put(batch, JOB.checkpoint, &complete);
    }
    let format = writer_format::enable_event_fallback(format)?;
    batch.put("store_format", format);
    Ok(format.to_vec())
}

/// The committed backfill frontier: `None` once complete, otherwise the last examined
/// record graph's owner identity, if any.
pub(super) fn frontier(db: &DB) -> Result<Option<Option<String>>, Error> {
    decode_frontier(db.get(CHECKPOINT)?.as_deref())
}

/// Decode the committed checkpoint as [`frontier`] does, from any snapshot.
pub(super) fn decode_frontier(bytes: Option<&[u8]>) -> Result<Option<Option<String>>, Error> {
    use super::Catalog;
    let bytes = bytes.ok_or_else(|| failure("backfill state missing"))?;
    if bytes.len() > 1024 {
        return Err(failure("backfill checkpoint exceeds its bound"));
    }
    let progress = catalog_upgrade::Progress::decode(&parse(bytes)?)?;
    Ok((!progress.complete).then(|| owner(progress.after)))
}

fn owner(after: Option<String>) -> Option<String> {
    after.and_then(|key| key.strip_prefix("rdf:graph:").map(str::to_owned))
}

/// Stage one backfill page of at most `maximum` graphs with its checkpoint, and whether
/// the backfill is complete after it; `None` when it was already complete.
pub(super) fn page(db: &DB, maximum: usize) -> Result<Option<(WriteBatch, bool)>, Error> {
    let progress = catalog_upgrade::progress(db, &JOB)?;
    if progress.complete {
        return Ok(None);
    }
    catalog_upgrade::page(db, &JOB, progress, maximum.min(JOB.page_entries)).map(Some)
}

/// Record graphs at or before the frontier `after` (an owner identity), and all of them,
/// counted from their empty provenance markers, one per record graph.
pub(super) fn counts(db: &DB, after: Option<&str>) -> Result<(u64, u64), Error> {
    let frontier = after.map(|id| format!("provenance-rdf:{id}"));
    super::count_prefix(
        db,
        b"provenance-rdf:record:",
        frontier.as_deref().map(str::as_bytes),
    )
}

/// Whether a record graph's stored rows already give its subject `eventTime` or
/// `eventTimeFallback`: the facts are current and the graph needs no backfill. The
/// dictionary identities come from the caller's view of the dictionary.
pub(super) fn carries_event_time(
    rows: &[(u8, [u64; 4])],
    subject: Option<u64>,
    predicates: [Option<u64>; 2],
) -> bool {
    subject.is_some_and(|subject| {
        rows.iter().any(|(stream, row)| {
            *stream == 0 && row[0] == subject && predicates.contains(&Some(row[1]))
        })
    })
}

/// The predicates [`carries_event_time`] looks for.
pub(super) fn event_time_predicates() -> [TermValue; 2] {
    use crate::RDF_VOCABULARY_NAMESPACE as NS;
    ["eventTime", "eventTimeFallback"].map(|name| TermValue::iri(format!("{NS}{name}")))
}

fn failure(message: &str) -> Error {
    Error::new("STORAGE_FAILURE", format!("event fallback RDF: {message}"))
}

/// The page's graphs share one RDF writer: each writer resolves its counters from the
/// committed database, so separately resolved writers in one batch would collide.
struct Fallback;

impl catalog_upgrade::Publication for Fallback {
    type Page<'a> = (&'a DB, rdf_index::RdfWrite<'a>);

    fn open<'a>(&self, db: &'a DB) -> Result<Self::Page<'a>, Error> {
        Ok((db, rdf_index::RdfWrite::new(db)?))
    }

    fn publish(
        &self,
        (db, rdf): &mut Self::Page<'_>,
        key: &str,
        bytes: &[u8],
        _: usize,
        batch: &mut WriteBatch,
    ) -> Result<(), Error> {
        publish(db, rdf, key, bytes, batch)
    }

    fn full(&self, _: &Self::Page<'_>, batch: &WriteBatch) -> bool {
        batch.size_in_bytes() >= PAGE_BATCH
    }

    fn finish(&self, (_, rdf): Self::Page<'_>, batch: &mut WriteBatch) -> Result<(), Error> {
        rdf.finish(batch)
    }
}

fn publish(
    db: &DB,
    rdf: &mut rdf_index::RdfWrite<'_>,
    key: &str,
    bytes: &[u8],
    batch: &mut WriteBatch,
) -> Result<(), Error> {
    use crate::RDF_VOCABULARY_NAMESPACE as NS;
    let id = key
        .strip_prefix("rdf:graph:")
        .ok_or_else(|| failure("invalid metadata owner"))?;
    let charge = catalog_upgrade::json_charge(bytes, GRAPH_BYTES)?;
    let subject_value = TermValue::iri(rdf_index::entity_iri(id));
    // A source instant, or facts already current: nothing to add. The stored manifest's
    // rows and the dictionary decide that without decoding the graph's terms.
    let rows = rdf_index::manifest_rows(&parse(bytes)?)?;
    let mut predicates = [None; 2];
    for (slot, predicate) in predicates.iter_mut().zip(event_time_predicates()) {
        *slot = rdf.known(&predicate)?;
    }
    if predicates.iter().any(Option::is_some)
        && carries_event_time(&rows, rdf.known(&subject_value)?, predicates)
    {
        return Ok(());
    }
    let rdf_index::StoredMetadata {
        mut builder,
        values,
        primary,
    } = rdf_index::stored_metadata(db, rdf, id, bytes, GRAPH_BYTES - charge, &failure)?;
    let mut received = None;
    for row in &primary {
        if values[&row[0]] != subject_value {
            continue;
        }
        let TermValue::Iri(predicate) = &values[&row[1]] else {
            continue;
        };
        match predicate.strip_prefix(NS) {
            Some("eventTime" | "eventTimeFallback") => {
                return Err(failure("event time facts disagree with the dictionary"));
            }
            Some("receivedAt") => {
                let TermValue::Literal {
                    lexical_form,
                    datatype,
                    language: None,
                    direction: None,
                } = &values[&row[2]]
                else {
                    return Err(failure("receipt time is not a typed literal"));
                };
                if datatype != "http://www.w3.org/2001/XMLSchema#dateTime"
                    || received.replace(lexical_form).is_some()
                {
                    return Err(failure("receipt time is not one xsd:dateTime"));
                }
            }
            _ => {}
        }
    }
    let received = received.ok_or_else(|| failure("record graph has no receipt time"))?;
    let placement = crate::EventPlacement::receipt(crate::parse_instant(received)?);
    let subject = builder.intern_value(&subject_value)?;
    rdf_index::fallback_metadata(&mut builder, subject, placement)?;
    let dataset = rdf_index::metadata_dataset(builder)?;
    rdf.replace_metadata(batch, id, dataset.as_ref())
}

/// Rebuild every record metadata graph without its fallback facts, recreating the graph a
/// writer before schema 69 published. Offline test fixtures only; absent unless the dev-only
/// test-support feature is selected.
#[cfg(feature = "test-support")]
pub fn remove_event_fallback_rdf_for_legacy_fixture(db: &DB) -> Result<(), Error> {
    use crate::RDF_VOCABULARY_NAMESPACE as NS;
    let fallback = [
        format!("{NS}eventTimeFallback"),
        format!("{NS}eventTimeBasis"),
    ];
    let owners = db
        .prefix_iterator(JOB.source)
        .map(|row| row.map_err(Error::from))
        .take_while(|row| {
            row.as_ref()
                .map_or(true, |(key, _)| key.starts_with(JOB.source.as_bytes()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for (key, bytes) in owners {
        let key = std::str::from_utf8(&key)
            .map_err(|_| failure("legacy fixture graph identity is invalid"))?;
        let id = key
            .strip_prefix("rdf:graph:")
            .ok_or_else(|| failure("invalid metadata owner"))?;
        let mut rdf = rdf_index::RdfWrite::new(db)?;
        let stored = rdf_index::stored_metadata(db, &mut rdf, id, &bytes, GRAPH_BYTES, &failure)?;
        let mut builder = purrdf::RdfDatasetBuilder::new();
        for row in &stored.primary {
            if matches!(&stored.values[&row[1]], TermValue::Iri(p) if fallback.contains(p)) {
                continue;
            }
            let subject = builder.intern_value(&stored.values[&row[0]])?;
            let predicate = builder.intern_value(&stored.values[&row[1]])?;
            let object = builder.intern_value(&stored.values[&row[2]])?;
            builder.push_quad(subject, predicate, object, None)?;
        }
        let mut batch = WriteBatch::default();
        rdf.replace_metadata(
            &mut batch,
            id,
            rdf_index::metadata_dataset(builder)?.as_ref(),
        )?;
        rdf.finish(&mut batch)?;
        catalog_upgrade::commit(db, batch)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use purrdf::{RdfDatasetBuilder, RdfLiteral};

    fn date_time(builder: &mut RdfDatasetBuilder, millis: u64) -> purrdf::ir::TermId {
        builder
            .intern_literal(rdf_index::date_time(millis).unwrap())
            .unwrap()
    }

    fn rows(db: &DB, id: &str) -> usize {
        let manifest = parse(&db.get(format!("rdf:graph:{id}")).unwrap().unwrap()).unwrap();
        rdf_index::manifest_rows(&manifest).unwrap().len()
    }

    fn checkpoint(db: &DB) -> catalog_upgrade::Progress {
        use super::super::Catalog;
        catalog_upgrade::Progress::decode(
            &parse(&db.get(JOB.checkpoint).unwrap().unwrap()).unwrap(),
        )
        .unwrap()
    }

    /// INVARIANT: one page rebuilds several graphs through one shared RDF writer and one
    /// commit, a graph that carries a source `eventTime` stays byte-identical, a failed page
    /// publishes none of its graphs, and a reopened store resumes after the last committed
    /// page. The graphs have no catalog record, as for an unfinished staged import.
    #[test]
    fn pages_share_one_writer_skip_source_times_and_resume_after_a_failed_page() {
        use crate::RDF_VOCABULARY_NAMESPACE as NS;
        let dir = tempfile::tempdir().unwrap();
        let db = rdf_index::open_test_db(dir.path());
        for key in ["rdf:term-count", "rdf:max-encoded", "rdf:next"] {
            db.put(key, 0_u64.to_be_bytes()).unwrap();
        }
        db.put("store_format", b"sparrow.store/77").unwrap();
        let sourced = ["record:a", "record:d"];
        let ids = [
            "record:a", "record:b", "record:c", "record:d", "record:e", "record:f",
        ];
        let mut rdf = rdf_index::RdfWrite::new(&db).unwrap();
        let mut batch = WriteBatch::default();
        for (n, id) in (1_u64..).zip(ids) {
            let mut builder = RdfDatasetBuilder::new();
            let subject = builder.intern_iri(&rdf_index::entity_iri(id)).unwrap();
            let mut facts = vec![
                ("receivedAt", date_time(&mut builder, 1000 * n)),
                (
                    "sourceName",
                    builder
                        .intern_literal(RdfLiteral::simple("shared"))
                        .unwrap(),
                ),
            ];
            if sourced.contains(&id) {
                facts.push(("eventTime", date_time(&mut builder, n)));
            }
            for (predicate, object) in facts {
                let predicate = builder.intern_iri(&format!("{NS}{predicate}")).unwrap();
                builder.push_quad(subject, predicate, object, None).unwrap();
            }
            rdf.graph(&mut batch, id, &builder.freeze().unwrap())
                .unwrap();
        }
        rdf.finish(&mut batch).unwrap();
        db.write(batch).unwrap();
        let before = ids.map(|id| db.get(format!("rdf:graph:{id}")).unwrap().unwrap());
        let mut batch = WriteBatch::default();
        fence(&db, b"sparrow.store/77", &mut batch).unwrap();
        catalog_upgrade::commit(&db, batch).unwrap();
        catalog_upgrade::step(&db, &JOB, checkpoint(&db), 3, None).unwrap();
        assert_eq!(checkpoint(&db).after.as_deref(), Some("rdf:graph:record:c"));
        assert_eq!([rows(&db, "record:b"), rows(&db, "record:c")], [4, 4]);
        // The next page would rebuild record:e before refusing record:f.
        db.put("rdf:graph:record:f", b"{}").unwrap();
        assert!(catalog_upgrade::run(&db, &JOB, None).is_err());
        assert_eq!(db.get("rdf:graph:record:e").unwrap().unwrap(), before[4]);
        assert_eq!(checkpoint(&db).after.as_deref(), Some("rdf:graph:record:c"));
        db.put("rdf:graph:record:f", &before[5]).unwrap();
        drop(db);
        let db = rdf_index::open_test_db(dir.path());
        catalog_upgrade::run(&db, &JOB, None).unwrap();
        assert!(checkpoint(&db).complete);
        for (id, stored) in ids.iter().zip(&before) {
            let current = db.get(format!("rdf:graph:{id}")).unwrap().unwrap();
            if sourced.contains(id) {
                assert_eq!(&current, stored, "{id} carries a source time");
            } else {
                assert_eq!(rows(&db, id), 4, "{id} gains both fallback facts");
            }
        }
        // Every shared dictionary reference of the paged rebuilds is exact.
        for id in ids {
            let mut rdf = rdf_index::RdfWrite::new(&db).unwrap();
            let mut batch = WriteBatch::default();
            rdf.remove(&mut batch, id).unwrap();
            rdf.finish(&mut batch).unwrap();
            db.write(batch).unwrap();
        }
        assert_eq!(
            rdf_index::required_counter(&db, "rdf:term-count").unwrap(),
            0
        );
    }
}
