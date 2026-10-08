// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Same-record conjunction over retained native JSON occurrence identities.

use super::*;
use crate::FieldConstraint;

#[derive(Default)]
pub(super) struct FieldCoverage {
    pub visits: usize,
    pub failed: u64,
    pub not_applicable: u64,
    pub decoding_failures: u64,
}

impl FieldCoverage {
    fn visit(&mut self, budget: usize) -> Result<(), Error> {
        if self.visits == budget {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "field occurrence/ancestor budget exhausted; narrow scope or raise scan_budget; no result prefix returned",
            ));
        }
        self.visits += 1;
        Ok(())
    }
}

fn parent(values: &[Value], index: usize) -> Result<Option<usize>, Error> {
    let value = field(&values[index], "parent")?;
    if value.is_null() {
        return Ok(None);
    }
    let parent = usize::try_from(integer(&values[index], "parent")?)
        .map_err(|_| Error::new("STORAGE_FAILURE", "native parent identity overflow"))?;
    if parent >= index {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "native parent must precede its child",
        ));
    }
    Ok(Some(parent))
}

fn binding(
    values: &[Value],
    index: usize,
    constraint: &FieldConstraint,
    coverage: &mut FieldCoverage,
    budget: usize,
) -> Result<Option<usize>, Error> {
    let mut ancestor = parent(values, index)?;
    let Some(path) = &constraint.parent_path else {
        return Ok(ancestor);
    };
    while let Some(index) = ancestor {
        coverage.visit(budget)?;
        if read_field::<String>(&values[index], "path")? == *path {
            return Ok(Some(index));
        }
        ancestor = parent(values, index)?;
    }
    Ok(None)
}

impl Writer {
    pub(super) fn qualify_fields(
        &self,
        record: &Record,
        constraints: &[FieldConstraint],
        coverage: &mut FieldCoverage,
        budget: usize,
        require_complete: bool,
    ) -> Result<Option<Vec<Value>>, Error> {
        qualify_from_projection(
            record,
            constraints,
            coverage,
            budget,
            require_complete,
            |id| self.db.get_pinned(id).map_err(Error::from),
        )
    }
}

/// Match through one native occurrence implementation regardless of whether
/// the caller reads current or snapshot-bound projection bytes.
pub(super) fn qualify_from_projection<B: AsRef<[u8]>>(
    record: &Record,
    constraints: &[FieldConstraint],
    coverage: &mut FieldCoverage,
    budget: usize,
    require_complete: bool,
    read: impl FnOnce(&str) -> Result<Option<B>, Error>,
) -> Result<Option<Vec<Value>>, Error> {
    if constraints.is_empty() {
        return Ok(Some(Vec::new()));
    }
    if record.projection_id.is_empty() {
        coverage.not_applicable += 1;
        return Ok(None);
    }
    let bytes = read(&record.projection_id)?
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "live JSON projection missing"))?;
    let facts = parse(bytes.as_ref())?;
    match read_field::<String>(&facts, "state")?.as_str() {
        "complete" => {}
        "failed" => {
            coverage.failed += 1;
            if require_complete {
                return Err(Error::new(
                    "INDEX_NOT_READY",
                    "selected JSON projection failed",
                ));
            }
            return Ok(None);
        }
        _ => {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "unknown JSON projection state",
            ));
        }
    }
    let failures = integer(&facts, "decoding_failures")?;
    coverage.decoding_failures += failures;
    if require_complete && failures > 0 {
        return Err(Error::new(
            "INDEX_NOT_READY",
            "selected JSON projection has unavailable decoded strings",
        ));
    }
    let values = field(&facts, "values")?
        .as_array()
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "native JSON values missing"))?;
    let mut candidates = vec![BTreeMap::<Option<usize>, usize>::new(); constraints.len()];
    for (index, fact) in values.iter().enumerate() {
        coverage.visit(budget)?;
        if integer(fact, "occurrence")? != index as u64 {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "native occurrence identity differs from retained order",
            ));
        }
        parent(values, index)?;
        for (ordinal, constraint) in constraints.iter().enumerate() {
            coverage.visit(budget)?;
            if !constraint.predicate.matches(fact)? {
                continue;
            }
            let bound = if constraint.parent_binding.is_some() {
                let bound = binding(values, index, constraint, coverage, budget)?;
                if bound.is_none() {
                    continue;
                }
                bound
            } else {
                None
            };
            candidates[ordinal].entry(bound).or_insert(index);
        }
    }
    if candidates.iter().any(BTreeMap::is_empty) {
        return Ok(None);
    }
    let mut groups = BTreeMap::<&str, BTreeSet<Option<usize>>>::new();
    for (ordinal, constraint) in constraints.iter().enumerate() {
        if let Some(label) = &constraint.parent_binding {
            let keys = candidates[ordinal].keys().copied().collect::<BTreeSet<_>>();
            groups
                .entry(label)
                .and_modify(|intersection| intersection.retain(|key| keys.contains(key)))
                .or_insert(keys);
        }
    }
    if groups.values().any(BTreeSet::is_empty) {
        return Ok(None);
    }
    let mut evidence = Vec::with_capacity(constraints.len());
    for (ordinal, constraint) in constraints.iter().enumerate() {
        let bound = constraint
            .parent_binding
            .as_ref()
            .and_then(|label| groups[label.as_str()].first().copied().flatten());
        let index = candidates[ordinal][&bound];
        let fact = &values[index];
        let start = record.byte_start.checked_add(integer(fact, "start")?);
        let end = record.byte_start.checked_add(integer(fact, "end")?);
        let (Some(start), Some(end)) = (start, end) else {
            return Err(Error::new("STORAGE_FAILURE", "native field span overflow"));
        };
        if start > end || end > record.byte_end {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "native field span outside record",
            ));
        }
        evidence.push(
            Object::new()
                .with("predicate_index", ordinal.to_string())
                .with("occurrence", index.to_string())
                .with("path", read_field::<String>(fact, "path")?)
                .with("kind", read_field::<String>(fact, "kind")?)
                .with("source_start", start.to_string())
                .with("source_end", end.to_string())
                .with(
                    "parent_binding",
                    constraint
                        .parent_binding
                        .clone()
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                )
                .with(
                    "bound_parent",
                    bound
                        .map(|index| Value::from(index.to_string()))
                        .unwrap_or(Value::Null),
                )
                .into(),
        );
    }
    Ok(Some(evidence))
}

/// One record summary owns the unchanged field/time/diagnostic evidence row.
pub(super) fn matched_record_row(record: &Record, fields: Vec<Value>) -> Result<Object, Error> {
    Ok(Object::new()
        .with("record_id", record.id.clone())
        .with("artifact_id", record.artifact_id.clone())
        .with("source", record.source.clone())
        .with("diagnostics", record.diagnostics.encode())
        .with("received_at", crate::format_instant(record.received_at)?)
        .with("byte_start", record.byte_start.to_string())
        .with("byte_end", record.byte_end.to_string())
        .with("event_time", record.event_time.encode())
        .with("fields", Value::array(fields)))
}
