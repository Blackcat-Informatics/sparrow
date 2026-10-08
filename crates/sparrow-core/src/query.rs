// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Bounded concrete JSON occurrence queries over stored native facts.

/// Explicit scope shared by JSON discovery operations.
#[derive(Clone, Debug)]
pub struct QueryScope {
    /// Membership resolved and validated before applying source/time predicates.
    pub selection: QuerySelection,
    /// Restrict to this exact logical source label.
    pub source: Option<String>,
    /// Exact logical source IDs, combined with OR and intersected with `source`.
    pub sources: Option<QueryAlternatives>,
    /// AND across explicit diagnostic dimensions, OR within each admitted array.
    pub diagnostics: std::collections::BTreeMap<crate::DiagnosticDimension, QueryAlternatives>,
    /// Refuse incomplete diagnostic or applicable query-index coverage.
    pub require_complete_index: bool,
    /// Inclusive server acceptance time, in Unix milliseconds.
    pub received_from: u64,
    /// Exclusive server acceptance time, in Unix milliseconds.
    pub received_before: u64,
    /// Maximum response rows; between one and one thousand.
    pub limit: usize,
    /// Maximum chronological catalog entries visited; at most one hundred thousand.
    pub scan_budget: usize,
}

/// Store-owned membership; semantic predicates remain native PurRDF operations.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum QuerySelection {
    /// All currently retained evidence, including pin-protected records.
    #[default]
    Live,
    /// Concrete artifact and record IDs. Overlap is deduplicated.
    Evidence(Vec<String>),
    /// Stable collection identity, optionally guarded against membership changes.
    Pin {
        /// Stable ID, never a reusable name.
        id: String,
        /// If supplied, the collection must still have this revision.
        expected_revision: Option<u64>,
    },
}

impl QuerySelection {
    pub(crate) fn validate(&self) -> Result<(), crate::Error> {
        match self {
            Self::Live => Ok(()),
            Self::Evidence(ids)
                if !ids.is_empty()
                    && ids.len() <= 1000
                    && ids.iter().all(|id| {
                        id.len() <= 128
                            && (id.starts_with("artifact:") || id.starts_with("record:"))
                    }) =>
            {
                Ok(())
            }
            Self::Pin {
                id,
                expected_revision,
            } if id.starts_with("pin:") && id.len() <= 128 && *expected_revision != Some(0) => {
                Ok(())
            }
            _ => Err(crate::Error::new(
                "INVALID_ARGUMENT",
                "selection requires 1..1000 bounded artifact/record IDs or a stable pin ID and positive revision",
            )),
        }
    }
}

/// Admitted exact filter alternatives; vocabulary ownership stays with Sparrow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryAlternatives {
    ids: Vec<String>,
}

impl QueryAlternatives {
    /// Admit 1..128 alternatives, each using the interpretation bound of 1..512 bytes.
    /// Duplicates are harmless alternatives and never duplicate result occurrences.
    pub fn new(ids: Vec<String>) -> Result<Self, crate::Error> {
        if ids.is_empty() || ids.len() > 128 || ids.iter().any(|id| id.is_empty() || id.len() > 512)
        {
            return Err(crate::Error::new(
                "INVALID_ARGUMENT",
                "filter alternatives require 1..128 nonempty strings of at most 512 bytes each",
            ));
        }
        Ok(Self { ids })
    }

    pub(crate) fn capacity(&self) -> usize {
        self.ids.capacity() * std::mem::size_of::<String>()
            + self.ids.iter().map(String::capacity).sum::<usize>()
    }

    /// Exact alternatives, preserving caller spelling and order.
    pub fn ids(&self) -> &[String] {
        &self.ids
    }
}

impl QueryScope {
    pub(crate) fn matches_source(&self, source: &str) -> bool {
        self.source.as_deref().is_none_or(|id| id == source)
            && self
                .sources
                .as_ref()
                .is_none_or(|filter| filter.ids.iter().any(|id| id == source))
    }

    pub(crate) fn validate(&self) -> Result<(), crate::Error> {
        self.selection.validate()?;
        if !(1..=1000).contains(&self.limit)
            || !(1..=100000).contains(&self.scan_budget)
            || self.received_from >= self.received_before
        {
            return Err(crate::Error::new(
                "INVALID_ARGUMENT",
                "require an increasing half-open acceptance interval, limit 1..1000 and scan_budget 1..100000",
            ));
        }
        Ok(())
    }
}

/// An exact selector over native ordered JSON occurrences.
#[derive(Clone, Debug)]
pub enum JsonQuery {
    /// Aggregate concrete RFC 6901 pointers and native kinds.
    Keys,
    /// Select scalar occurrences; omitted predicates admit every occurrence.
    Values {
        /// Exact RFC 6901 pointer; duplicate members remain separate occurrences.
        path: Option<String>,
        /// Exact native JSON kind.
        kind: Option<String>,
        /// Exact original scalar lexical bytes, including JSON escapes.
        lexical: Option<String>,
        /// Exact decoded string equality using PurRDF's lexical decoder.
        decoded: Option<String>,
    },
}

/// One exact conjunction over a retained native JSON occurrence.
#[derive(Clone, Debug, Default)]
pub struct JsonPredicate {
    /// Exact native concrete RFC 6901 pointer, or any occurrence.
    pub path: Option<String>,
    /// Native JSON kind, or any kind.
    pub kind: Option<String>,
    /// Exact authoritative lexical spelling, as retained by the native scalar span.
    pub lexical: Option<String>,
    /// Exact native-decoded string, without altering the authoritative graph.
    pub decoded: Option<String>,
}

impl JsonPredicate {
    pub(crate) fn validate(&self) -> Result<(), crate::Error> {
        if self.kind.as_deref().is_some_and(|kind| {
            ![
                "string", "number", "true", "false", "null", "array", "object",
            ]
            .contains(&kind)
        }) {
            return Err(crate::Error::new(
                "INVALID_ARGUMENT",
                "unknown native JSON kind",
            ));
        }
        Ok(())
    }

    pub(crate) fn matches(&self, fact: &purrdf_lex::json::Value) -> Result<bool, crate::Error> {
        use purrdf_lex::json::record::{DecodeError, Record};
        let mut fields = Record::new(fact, "native JSON occurrence")?;
        fn read(value: &purrdf_lex::json::Value) -> Result<&str, DecodeError> {
            value
                .as_str()
                .ok_or_else(|| DecodeError::invalid_value(value, "string"))
        }
        let path = fields.required_with("path", read)?;
        let kind = fields.required_with("kind", read)?;
        let lexical = fields.raw("lexical")?.map(read).transpose()?;
        let decoded = fields.raw("decoded")?.map(read).transpose()?;
        Ok(!self.path.as_deref().is_some_and(|wanted| wanted != path)
            && !self.kind.as_deref().is_some_and(|wanted| wanted != kind)
            && !self
                .lexical
                .as_deref()
                .is_some_and(|wanted| lexical != Some(wanted))
            && !self
                .decoded
                .as_deref()
                .is_some_and(|wanted| decoded != Some(wanted)))
    }
}

impl JsonQuery {
    pub(crate) fn predicate(&self) -> Option<JsonPredicate> {
        match self {
            Self::Keys => None,
            Self::Values {
                path,
                kind,
                lexical,
                decoded,
            } => Some(JsonPredicate {
                path: path.clone(),
                kind: kind.clone(),
                lexical: lexical.clone(),
                decoded: decoded.clone(),
            }),
        }
    }
}

/// One record constraint, optionally sharing a native parent occurrence binding.
#[derive(Clone, Debug, Default)]
pub struct FieldConstraint {
    /// Exact native occurrence conditions; all supplied conditions intersect.
    pub predicate: JsonPredicate,
    /// Constraints sharing this label must bind the same native parent identity.
    pub parent_binding: Option<String>,
    /// Exact ancestor pointer to bind; omitted means the immediate native parent.
    pub parent_path: Option<String>,
}

/// Combined record qualification before optional native text ranking.
#[derive(Clone, Copy, Debug)]
pub struct EventInterval {
    /// Inclusive source instant in UTC Unix milliseconds.
    pub from: u64,
    /// Exclusive source instant in UTC Unix milliseconds.
    pub before: u64,
}

/// Combined record qualification before optional native text ranking.
#[derive(Clone, Debug, Default)]
pub struct RecordQuery {
    /// Explicit source-time interval; acceptance scope remains an independent filter.
    pub event_interval: Option<EventInterval>,
    /// Native full-text query; absent yields chronological field/metadata results.
    pub text: Option<String>,
    /// Conjunction over the same record; empty selects every eligible record.
    pub fields: Vec<FieldConstraint>,
    /// Refuse unavailable applicable indexes rather than returning covered matches.
    pub require_complete_index: bool,
}

impl RecordQuery {
    pub(crate) fn validate(&self) -> Result<(), crate::Error> {
        if self
            .event_interval
            .is_some_and(|interval| interval.from >= interval.before)
        {
            return Err(crate::Error::new(
                "INVALID_ARGUMENT",
                "require an increasing half-open event-time interval",
            ));
        }
        if self.fields.len() > 32 || self.text.as_ref().is_some_and(|text| text.len() > 65536) {
            return Err(crate::Error::new(
                "RESOURCE_LIMIT",
                "record query permits at most 32 field constraints and 64 KiB text",
            ));
        }
        let mut bytes = 0_usize;
        for field in &self.fields {
            field.predicate.validate()?;
            if field.parent_path.is_some() && field.parent_binding.is_none()
                || field.parent_binding.as_ref().is_some_and(|label| {
                    label.is_empty() || label.len() > 128 || label.chars().any(char::is_control)
                })
            {
                return Err(crate::Error::new(
                    "INVALID_ARGUMENT",
                    "parent_path requires a bounded nonempty parent_binding",
                ));
            }
            for value in [
                &field.predicate.path,
                &field.predicate.kind,
                &field.predicate.lexical,
                &field.predicate.decoded,
                &field.parent_binding,
                &field.parent_path,
            ]
            .into_iter()
            .flatten()
            {
                bytes = bytes.saturating_add(value.len());
            }
        }
        if bytes > 256 * 1024 {
            return Err(crate::Error::new(
                "RESOURCE_LIMIT",
                "field query exceeds 256 KiB compiled input",
            ));
        }
        Ok(())
    }
}
