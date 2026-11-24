// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Source admission rules for the RPC producer access classes.

use serde::Deserialize;
use sparrow_core::Error;
use std::{collections::BTreeSet, sync::Arc};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "class", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum Caller {
    PrivateUnix { uid: u32 },
    IngestSocket { uid: u32 },
    Chap {},
    AnonymousTcp {},
}

impl Caller {
    fn class(self) -> &'static str {
        match self {
            Self::PrivateUnix { .. } => "private-unix",
            Self::IngestSocket { .. } => "ingest-socket",
            Self::Chap {} => "chap",
            Self::AnonymousTcp {} => "anonymous-tcp",
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Rule {
    source: String,
    #[serde(default)]
    prefix: bool,
    allow: Vec<Caller>,
}

impl Rule {
    fn matches(&self, source: &str) -> bool {
        if self.prefix {
            source.starts_with(&self.source)
        } else {
            source == self.source
        }
    }
}

/// Validated RPC producer source bindings. An empty table preserves endpoint admission.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(try_from = "Vec<Rule>")]
pub struct SourceBindings(Arc<[Rule]>);

impl TryFrom<Vec<Rule>> for SourceBindings {
    type Error = String;

    fn try_from(rules: Vec<Rule>) -> Result<Self, Self::Error> {
        for (index, rule) in rules.iter().enumerate() {
            if rule.source.is_empty() || rule.source.len() > 512 {
                return Err("source_bindings.source requires 1 to 512 bytes".into());
            }
            let allowed: BTreeSet<_> = rule.allow.iter().collect();
            if allowed.is_empty() || allowed.len() != rule.allow.len() {
                return Err(format!(
                    "source_bindings for {:?} requires distinct permitted callers",
                    rule.source
                ));
            }
            if let Some(other) = rules[..index]
                .iter()
                .find(|other| rule.matches(&other.source) || other.matches(&rule.source))
            {
                return Err(format!(
                    "source_bindings selectors {:?} and {:?} overlap",
                    rule.source, other.source
                ));
            }
        }
        Ok(Self(rules.into()))
    }
}

impl SourceBindings {
    pub(crate) fn admit(&self, source: &str, caller: Option<Caller>) -> Result<(), Error> {
        let Some(rule) = self.0.iter().find(|rule| rule.matches(source)) else {
            return Ok(());
        };
        if caller.is_some_and(|caller| rule.allow.contains(&caller)) {
            return Ok(());
        }
        let class = caller.map_or("unclassified", Caller::class);
        Err(Error::new(
            "PERMISSION_DENIED",
            format!("source {source:?} refuses producer class {class}"),
        ))
    }
}
