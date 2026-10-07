// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Bounded concrete JSON occurrence queries over stored native facts.

/// Explicit scope shared by JSON discovery operations.
#[derive(Clone, Debug)]
pub struct QueryScope {
    /// Restrict to this exact logical source label.
    pub source: Option<String>,
    /// Inclusive server acceptance time, in Unix milliseconds.
    pub received_from: u64,
    /// Exclusive server acceptance time, in Unix milliseconds.
    pub received_before: u64,
    /// Maximum response rows; between one and one thousand.
    pub limit: usize,
    /// Maximum chronological catalog entries visited; at most one hundred thousand.
    pub scan_budget: usize,
}

impl QueryScope {
    pub(crate) fn validate(&self) -> Result<(), crate::Error> {
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
