// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Occurrence-owned tracing observations from the native ordered JSON facts.

use super::{
    Catalog, Error, Record, artifact_rdf, catalog_upgrade,
    diagnostic::{MappedOccurrence, mapped_occurrence},
    integer, parse, rdf_index, read_field, writer_format,
};
use purrdf::{RdfDatasetBuilder, RdfLiteral, TermId};
use purrdf_lex::json::{Number, Value};
use rocksdb::{DB, WriteBatch};

const PROFILE: &str = "sparrow-tracing-rdf/1";
const PROFILE_KEY: &str = "tracing-rdf:profile";
const PAGE_BYTES: usize = 128 * 1024 * 1024;
const JOB: catalog_upgrade::Job = catalog_upgrade::Job {
    checkpoint: "tracing-rdf-upgrade",
    source: "record:",
    target: b"tracing-rdf:",
    page_bytes: PAGE_BYTES,
    page_entries: 1,
    charge: |bytes| catalog_upgrade::json_charge(bytes, PAGE_BYTES),
    publish,
};

pub(super) fn upgrade(db: &DB, disk_budget: u64, reserve: u64) -> Result<(), Error> {
    let format = db
        .get("store_format")?
        .ok_or_else(|| failure("writer format missing"))?;
    if !writer_format::has_tracing(&format) {
        let mut batch = WriteBatch::default();
        catalog_upgrade::seed(db, &JOB, &mut batch)?;
        batch.put("store_format", writer_format::enable_tracing(&format)?);
        batch.put(PROFILE_KEY, PROFILE.as_bytes());
        catalog_upgrade::commit(db, batch)?;
    }
    if db.get(PROFILE_KEY)?.as_deref() != Some(PROFILE.as_bytes()) {
        return Err(failure(
            "required interpretation profile missing or changed",
        ));
    }
    catalog_upgrade::run(
        db,
        &JOB,
        Some(&|batch| artifact_rdf::admit(db, disk_budget, reserve, batch)),
    )
}

fn failure(message: &str) -> Error {
    Error::new("STORAGE_FAILURE", format!("tracing RDF: {message}"))
}

fn publish(
    db: &DB,
    key: &str,
    bytes: &[u8],
    charge: usize,
    batch: &mut WriteBatch,
) -> Result<(), Error> {
    let record = Record::decode(&parse(bytes)?)?;
    if record.id != key {
        return Err(failure("record key disagrees with catalog identity"));
    }
    if record.producer.is_none() {
        return Ok(());
    }
    let mut remaining = PAGE_BYTES - charge;
    let mut load = |key: &str| -> Result<Value, Error> {
        let bytes = db
            .get_pinned(key)?
            .ok_or_else(|| failure("required producer projection missing"))?;
        remaining -= catalog_upgrade::json_charge(&bytes, remaining)?;
        parse(&bytes)
    };
    let facts = load(&record.projection_id)?;
    let state = load(&format!("rdf:state:{}", record.rdf_projection_id))?;
    let digest = record
        .projection_id
        .rsplit(':')
        .next()
        .ok_or_else(|| failure("payload identity missing"))?;
    if purrdf_hash::hex::decode_32_canonical(digest).is_none() {
        return Err(failure("producer payload identity invalid"));
    }
    let dataset = rdf_index::metadata(&record, digest, Some(&state), Some(&facts))?;
    let mut rdf = rdf_index::RdfWrite::new(db)?;
    rdf.replace_metadata(batch, &record.id, &dataset)?;
    rdf.finish(batch)
}

struct Facts<'a>(&'a [Value]);
impl<'a> Facts<'a> {
    fn at(&self, path: &str) -> Result<&'a Value, Error> {
        match mapped_occurrence(self.0, path)? {
            MappedOccurrence::Value(value) => Ok(value),
            _ => Err(Error::new(
                "PROJECTION_FAILURE",
                format!("tracing field missing or ambiguous: {path}"),
            )),
        }
    }
    fn text(&self, path: &str) -> Result<&'a str, Error> {
        let value = self.at(path)?;
        if value.get("kind").and_then(Value::as_str) != Some("string") {
            return Err(Error::new(
                "PROJECTION_FAILURE",
                "tracing field requires a string",
            ));
        }
        let text = value
            .get("decoded")
            .and_then(Value::as_str)
            .ok_or_else(|| failure("native string lacks decoded evidence"))?;
        if text.len() > 16 * 1024 {
            return Err(Error::new(
                "PROJECTION_FAILURE",
                "tracing string exceeds event admission",
            ));
        }
        Ok(text)
    }
    fn number(&self, path: &str) -> Result<u64, Error> {
        let value = self.at(path)?;
        if value.get("kind").and_then(Value::as_str) != Some("number") {
            return Err(Error::new(
                "PROJECTION_FAILURE",
                "tracing identity requires a number",
            ));
        }
        read_field::<String>(value, "lexical")?
            .parse::<Number>()
            .ok()
            .and_then(|n| n.as_u64())
            .ok_or_else(|| {
                Error::new(
                    "PROJECTION_FAILURE",
                    "tracing identity requires an exact unsigned integer",
                )
            })
    }
    fn optional_span(&self, path: &str) -> Result<Option<u64>, Error> {
        if self.at(path)?.get("kind").and_then(Value::as_str) == Some("null") {
            return Ok(None);
        }
        let id = self.number(path)?;
        if id == 0 {
            return Err(Error::new(
                "PROJECTION_FAILURE",
                "span identity must be nonzero",
            ));
        }
        Ok(Some(id))
    }
}

struct Context<'a> {
    id: u64,
    parent: Option<u64>,
    name: &'a str,
    target: &'a str,
    start: u64,
    end: u64,
}
struct Observation<'a> {
    instance: &'a str,
    kind: &'a str,
    name: &'a str,
    target: &'a str,
    level: &'a str,
    span: Option<u64>,
    follows: Option<u64>,
    context: Vec<Context<'a>>,
}

fn observation<'a>(record: &Record, facts: &'a Facts<'a>) -> Result<Observation<'a>, Error> {
    if facts.text("/profile")? != "sparrow.events/1" {
        return Err(Error::new(
            "PROJECTION_FAILURE",
            "tracing profile is ambiguous",
        ));
    }
    let instance = facts.text("/client_instance")?;
    if instance.len() != 32 || purrdf_hash::hex::decode_canonical(instance).is_err() {
        return Err(Error::new(
            "PROJECTION_FAILURE",
            "producer instance requires 16 canonical hex bytes",
        ));
    }
    if Some(facts.number("/sequence")?) != record.producer.as_ref().map(|p| p.sequence) {
        return Err(Error::new(
            "PROJECTION_FAILURE",
            "source sequence contradicts producer position",
        ));
    }
    let kind = facts.text("/kind")?;
    if ![
        "log",
        "span_new",
        "span_record",
        "span_enter",
        "span_exit",
        "span_close",
        "span_follows_from",
    ]
    .contains(&kind)
    {
        return Err(Error::new(
            "PROJECTION_FAILURE",
            "unknown tracing observation kind",
        ));
    }
    let array = facts.at("/context")?;
    if array.get("kind").and_then(Value::as_str) != Some("array") {
        return Err(Error::new(
            "PROJECTION_FAILURE",
            "tracing context must be an array",
        ));
    }
    let owner: String = read_field(array, "occurrence")?;
    let count = facts
        .0
        .iter()
        .filter(|v| v.get("parent").and_then(Value::as_str) == Some(owner.as_str()))
        .count();
    if count > 32 {
        return Err(Error::new(
            "PROJECTION_FAILURE",
            "tracing context exceeds 32 entries",
        ));
    }
    let mut context = Vec::new();
    for index in 0..count {
        let path = format!("/context/{index}");
        let value = facts.at(&path)?;
        if value.get("kind").and_then(Value::as_str) != Some("object") {
            return Err(Error::new(
                "PROJECTION_FAILURE",
                "tracing context entry must be an object",
            ));
        }
        let id = facts
            .optional_span(&format!("{path}/id"))?
            .ok_or_else(|| Error::new("PROJECTION_FAILURE", "context span identity missing"))?;
        let start = record
            .byte_start
            .checked_add(integer(value, "start")?)
            .ok_or_else(|| failure("source span overflow"))?;
        let end = record
            .byte_start
            .checked_add(integer(value, "end")?)
            .ok_or_else(|| failure("source span overflow"))?;
        if start < record.byte_start || end < start || end > record.byte_end {
            return Err(failure("native context span escapes record"));
        }
        context.push(Context {
            id,
            parent: facts.optional_span(&format!("{path}/parent"))?,
            name: facts.text(&format!("{path}/name"))?,
            target: facts.text(&format!("{path}/target"))?,
            start,
            end,
        });
    }
    Ok(Observation {
        instance,
        kind,
        name: facts.text("/name")?,
        target: facts.text("/target")?,
        level: facts.text("/level")?,
        span: facts.optional_span("/span")?,
        follows: facts.optional_span("/follows_from")?,
        context,
    })
}

fn text(
    builder: &mut RdfDatasetBuilder,
    subject: TermId,
    predicate: &str,
    value: &str,
) -> Result<(), Error> {
    let object = builder.intern_literal(RdfLiteral::simple(value))?;
    link(builder, subject, predicate, object)
}
fn number(
    builder: &mut RdfDatasetBuilder,
    subject: TermId,
    predicate: &str,
    value: u64,
) -> Result<(), Error> {
    let object = builder.intern_literal(RdfLiteral::typed(
        value.to_string(),
        "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
    ))?;
    link(builder, subject, predicate, object)
}
fn link(
    builder: &mut RdfDatasetBuilder,
    subject: TermId,
    predicate: &str,
    object: TermId,
) -> Result<(), Error> {
    let predicate = builder.intern_iri(&format!("urn:sparrow:vocab:{predicate}"))?;
    builder.push_quad(subject, predicate, object, None)?;
    Ok(())
}
fn span(builder: &mut RdfDatasetBuilder, instance: &str, id: u64) -> Result<TermId, Error> {
    let subject = builder.intern_iri(&format!("urn:sparrow:span:{instance}:{id}"))?;
    let predicate = builder.intern_iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")?;
    let class = builder.intern_iri("urn:sparrow:vocab:Span")?;
    builder.push_quad(subject, predicate, class, None)?;
    text(builder, subject, "clientInstance", instance)?;
    number(builder, subject, "spanIdentity", id)?;
    Ok(subject)
}

/// Add only observed claims; parent/follows-from claims stay on their record evidence.
pub(super) fn append(
    builder: &mut RdfDatasetBuilder,
    record: &Record,
    subject: TermId,
    facts: Option<&Value>,
) -> Result<(), Error> {
    if record.producer.is_none() {
        return Ok(());
    }
    text(builder, subject, "tracingProfile", PROFILE)?;
    let facts = facts.ok_or_else(|| failure("producer native projection missing"))?;
    match facts.get("state").and_then(Value::as_str) {
        Some("failed") => return text(builder, subject, "tracingState", "projection_unavailable"),
        Some("complete") => {}
        _ => return Err(failure("unknown native JSON projection state")),
    }
    let Some(values) = facts.get("values").and_then(Value::as_array) else {
        return Err(failure("complete native projection lacks occurrences"));
    };
    let facts = Facts(values);
    match mapped_occurrence(values, "/profile")? {
        MappedOccurrence::Missing => {
            return text(builder, subject, "tracingState", "not_applicable");
        }
        MappedOccurrence::Value(value)
            if value.get("decoded").and_then(Value::as_str) != Some("sparrow.events/1") =>
        {
            return text(builder, subject, "tracingState", "not_applicable");
        }
        _ => {}
    }
    let observation = match observation(record, &facts) {
        Ok(value) => value,
        Err(error) if error.code == "PROJECTION_FAILURE" => {
            text(builder, subject, "tracingState", "invalid_observation")?;
            return text(builder, subject, "tracingDiagnostic", &error.message);
        }
        Err(error) => return Err(error),
    };
    text(builder, subject, "tracingState", "complete")?;
    for (predicate, value) in [
        ("eventKind", observation.kind),
        ("eventName", observation.name),
        ("eventTarget", observation.target),
        ("eventLevel", observation.level),
        ("clientInstance", observation.instance),
    ] {
        text(builder, subject, predicate, value)?;
    }
    for (predicate, id) in [
        ("observedSpan", observation.span),
        ("followsFromSpan", observation.follows),
    ] {
        if let Some(id) = id {
            let object = span(builder, observation.instance, id)?;
            link(builder, subject, predicate, object)?;
        }
    }
    for (index, context) in observation.context.iter().enumerate() {
        let entry = builder.intern_iri(&format!(
            "urn:sparrow:tracing-context:{}:{index}",
            record.id
        ))?;
        link(builder, subject, "spanContext", entry)?;
        let object = span(builder, observation.instance, context.id)?;
        link(builder, entry, "observedSpan", object)?;
        if let Some(parent) = context.parent {
            let object = span(builder, observation.instance, parent)?;
            link(builder, entry, "declaredParentSpan", object)?;
        }
        for (predicate, value) in [
            ("contextIndex", index as u64),
            ("sourceByteStart", context.start),
            ("sourceByteEnd", context.end),
        ] {
            number(builder, entry, predicate, value)?;
        }
        text(builder, entry, "eventName", context.name)?;
        text(builder, entry, "eventTarget", context.target)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracing_fence_requires_original_profile_and_checkpoint_without_reseeding() {
        let directory = tempfile::tempdir().unwrap();
        let db = super::rdf_index::open_test_db(directory.path());
        db.put("store_format", b"sparrow.store/66").unwrap();
        upgrade(&db, 128 * 1024 * 1024, 8 * 1024 * 1024).unwrap();
        assert_eq!(
            db.get("store_format").unwrap().unwrap(),
            b"sparrow.store/77"
        );
        let checkpoint = db.get(JOB.checkpoint).unwrap().unwrap();
        db.delete(PROFILE_KEY).unwrap();
        let missing = upgrade(&db, 128 * 1024 * 1024, 8 * 1024 * 1024).unwrap_err();
        assert_eq!(missing.code, "STORAGE_FAILURE");
        assert!(missing.message.contains("profile"));
        db.put(PROFILE_KEY, PROFILE).unwrap();
        db.delete(JOB.checkpoint).unwrap();
        let missing = upgrade(&db, 128 * 1024 * 1024, 8 * 1024 * 1024).unwrap_err();
        assert_eq!(missing.code, "STORAGE_FAILURE");
        assert!(missing.message.contains("state missing"));
        assert!(db.get(JOB.checkpoint).unwrap().is_none());
        db.put(JOB.checkpoint, checkpoint).unwrap();
        db.put("store_format", b"sparrow.store/66").unwrap();
        assert!(
            upgrade(&db, 128 * 1024 * 1024, 8 * 1024 * 1024)
                .unwrap_err()
                .message
                .contains("unfenced")
        );
    }
}
