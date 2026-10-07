// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Grouping, ledger matching and the reports built from them.

use std::collections::{BTreeMap, BTreeSet};

use crate::json::{Json, Object};

use crate::ledger::{Distinct, FORBIDDEN, GroupKind, ISOMORPHIC, Job, Ledger};
use crate::normalize::MIN_TOKENS;
use crate::source::{Tree, Unit, UnitKind, Workspace};

/// A set of units with one fingerprint.
#[derive(Debug)]
pub(crate) struct Group {
    /// The shared fingerprint id.
    pub(crate) fingerprint: String,
    /// Tokens in the shared structural form (0 for a shim group).
    pub(crate) tokens: usize,
    /// Indexes into [`Workspace::units`].
    pub(crate) members: Vec<usize>,
}

/// Groups of two or more units sharing a structural fingerprint, largest first.
pub(crate) fn isomorphic_groups(workspace: &Workspace) -> Vec<Group> {
    let mut by_print: BTreeMap<&str, (usize, Vec<usize>)> = BTreeMap::new();
    for (index, unit) in workspace.units.iter().enumerate() {
        if let Some(print) = &unit.print
            && print.tokens >= MIN_TOKENS
            && print.shim.is_none()
        {
            by_print
                .entry(print.structural.as_str())
                .or_insert_with(|| (print.tokens, Vec::new()))
                .1
                .push(index);
        }
    }
    collect_groups(by_print)
}

/// The shim fingerprint a thin forwarder is grouped under, if it is grouped.
///
/// Two kinds of forwarder are not, because each forwards within its own type and the
/// shared spelling names a different item every time (Sparrow's census, 2026-10-06; it
/// is the case `normalize`'s shim-form documentation already calls "a shape and no job"):
///
/// * a method of a trait impl — `From::from` returning `Self::Io(error)`, `Default`
///   calling `Self::new()`, an empty `deletion_owners` — is the slot the trait obliges
///   every implementing type to fill, not a second entry point to one function;
/// * an inherent forwarder whose target is spelt through `Self` (`new() { Self::default() }`)
///   builds its own type.
///
/// Neither is body-grouped either: a forwarder is never compared by structure.
pub(crate) fn shim_key(unit: &Unit) -> Option<&str> {
    let print = unit.print.as_ref()?;
    let shim = print.shim.as_deref()?;
    (!unit.trait_impl && !print.via_self).then_some(shim)
}

/// Groups of two or more thin forwarders sharing a shim fingerprint.
pub(crate) fn shim_groups(workspace: &Workspace) -> Vec<Group> {
    let mut by_print: BTreeMap<&str, (usize, Vec<usize>)> = BTreeMap::new();
    for (index, unit) in workspace.units.iter().enumerate() {
        if let Some(shim) = shim_key(unit) {
            by_print
                .entry(shim)
                .or_insert_with(|| (0, Vec::new()))
                .1
                .push(index);
        }
    }
    collect_groups(by_print)
}

fn collect_groups(by_print: BTreeMap<&str, (usize, Vec<usize>)>) -> Vec<Group> {
    let mut groups: Vec<Group> = by_print
        .into_iter()
        .filter(|(_, (_, members))| members.len() >= 2)
        .map(|(fingerprint, (tokens, members))| Group {
            fingerprint: fingerprint.to_owned(),
            tokens,
            members,
        })
        .collect();
    groups.sort_by(|left, right| {
        right
            .members
            .len()
            .cmp(&left.members.len())
            .then(right.tokens.cmp(&left.tokens))
            .then(left.fingerprint.cmp(&right.fingerprint))
    });
    groups
}

/// Every variant symbol exempt from `detector`, across the ledger.
pub(crate) fn variant_symbols(
    ledger: &Ledger,
    workspace: &Workspace,
    detector: &str,
) -> BTreeSet<String> {
    ledger
        .jobs
        .iter()
        .flat_map(|job| job.variants.iter())
        .filter(|variant| variant.detector == detector)
        .map(|variant| {
            workspace
                .resolve(&variant.symbol)
                .unwrap_or_else(|| variant.symbol.clone())
        })
        .collect()
}

/// Whether a group is sanctioned: every member but one is an `isomorphic` variant.
pub(crate) fn group_is_sanctioned(
    group: &Group,
    workspace: &Workspace,
    variants: &BTreeSet<String>,
) -> bool {
    let unsanctioned = group
        .members
        .iter()
        .filter(|&&index| !variants.contains(&workspace.units[index].symbol))
        .count();
    unsanctioned <= 1
}

/// A group's member symbols, sorted: the set a [`Distinct`] row must equal.
fn member_symbols<'w>(group: &Group, workspace: &'w Workspace) -> Vec<&'w str> {
    let mut symbols: Vec<&str> = group
        .members
        .iter()
        .map(|&index| workspace.units[index].symbol.as_str())
        .collect();
    symbols.sort_unstable();
    symbols
}

/// The `[[distinct]]` row whose kind is `kind` and whose members are exactly the
/// group's, if there is one.
fn distinct_row(ledger: &Ledger, kind: GroupKind, symbols: &[&str]) -> Option<usize> {
    ledger.distinct.iter().position(|row| {
        row.kind == kind
            && row
                .members
                .iter()
                .map(String::as_str)
                .eq(symbols.iter().copied())
    })
}

/// Every kind of group, with the label `--check` reports it under.
fn every_group(workspace: &Workspace) -> [(GroupKind, &'static str, Vec<Group>); 2] {
    [
        (
            GroupKind::Body,
            "isomorphic bodies",
            isomorphic_groups(workspace),
        ),
        (GroupKind::Shim, "isomorphic shims", shim_groups(workspace)),
    ]
}

/// A `[[distinct]]` row's own failures: a member that is no longer defined, a
/// member set that is no current group (both STALE), and an anchor that does not
/// resolve or carries no documentation.
fn distinct_row_findings(row: &Distinct, matched: bool, workspace: &Workspace) -> Vec<String> {
    let mut findings = Vec::new();
    let label = row.label();
    let missing: Vec<&str> = row
        .members
        .iter()
        .filter(|member| !workspace.definitions.contains_key(member.as_str()))
        .map(String::as_str)
        .collect();
    if !missing.is_empty() {
        findings.push(format!(
            "{label}: STALE, member {} no longer exists; remove the row",
            missing.join(", ")
        ));
    } else if !matched {
        findings.push(format!(
            "{label}: STALE, no current {} group has exactly these members; remove or correct the row",
            row.kind.as_str()
        ));
    }
    match workspace.resolve(&row.anchor) {
        None => findings.push(format!("{label}: anchor `{}` does not resolve", row.anchor)),
        Some(symbol) if !workspace.definitions[&symbol].documented => findings.push(format!(
            "{label}: anchor `{}` carries no documentation",
            row.anchor
        )),
        Some(_) => {}
    }
    findings
}

/// One unit or rule hit that matches a job's forbidden set.
#[derive(Debug)]
pub(crate) struct Match {
    /// The matching symbol (a rule hit's is its file and enclosing items).
    pub(crate) symbol: String,
    /// The package that holds it.
    pub(crate) package: String,
    /// The repo-relative file.
    pub(crate) file: String,
    /// 1-based line.
    pub(crate) line: usize,
    /// What matched.
    pub(crate) reasons: Vec<String>,
    /// Whether the unit is in the job's home package. A rule hit is only when
    /// its rule exempts the home (a hex-digit table); every other rule forbids
    /// its pattern everywhere, the home included.
    pub(crate) in_home: bool,
    /// Whether the unit is a `forbidden` variant of this job.
    pub(crate) variant: bool,
}

/// A job's home, resolved.
#[derive(Debug)]
pub(crate) struct Home {
    /// The defining symbol.
    pub(crate) symbol: String,
    /// The package it lives in.
    pub(crate) package: String,
}

/// Resolve a job's home in `home_workspace` — Sparrow's own for an ordinary job,
/// the external scan for one homed in purrdf — or say why it does not resolve.
pub(crate) fn home(job: &Job, home_workspace: &Workspace) -> Result<Home, String> {
    let workspace = home_workspace;
    let symbol = workspace.resolve(&job.home).ok_or_else(|| {
        format!(
            "job `{}`: home `{}{}` does not resolve to a defined item",
            job.id,
            if job.external {
                crate::ledger::EXTERNAL_PREFIX
            } else {
                ""
            },
            job.home
        )
    })?;
    let package = workspace.definitions[&symbol].package.clone();
    Ok(Home { symbol, package })
}

/// The workspace a job's home lives in: `local` for an ordinary job, `external`
/// for one homed in purrdf — or, when that scan was not loaded, why the job
/// cannot be judged. Never a silent fallback to `local`, where a purrdf path
/// would simply fail to resolve.
pub(crate) fn home_workspace<'a>(
    job: &Job,
    local: &'a Workspace,
    external: Option<&'a Workspace>,
) -> Result<&'a Workspace, String> {
    if job.external {
        external.ok_or_else(|| {
            format!(
                "job `{}` is homed in purrdf; pass --cross to scan purrdf at its pinned revision",
                job.id
            )
        })
    } else {
        Ok(local)
    }
}

/// Every unit of `workspace` matching `job`'s forbidden set. `home_workspace` is
/// where the job's home lives: `workspace` itself for an ordinary job, the
/// external scan for one homed in purrdf. For the latter nothing in `workspace`
/// is ever in the home, so every match there is a copy.
pub(crate) fn matches(
    job: &Job,
    home_workspace: &Workspace,
    workspace: &Workspace,
    home_package: &str,
) -> Vec<Match> {
    let local = !job.external;
    let variants: BTreeSet<String> = job
        .variants
        .iter()
        .filter(|variant| variant.detector == FORBIDDEN)
        .map(|variant| {
            workspace
                .resolve(&variant.symbol)
                .unwrap_or_else(|| variant.symbol.clone())
        })
        .collect();
    // A rule hit is named by its file and enclosing items, not by a resolved
    // path, so a `forbidden` variant sanctions it by its file and item name.
    let rule_variants: BTreeSet<(&str, &str)> = job
        .variants
        .iter()
        .filter(|variant| variant.detector == FORBIDDEN)
        .map(|variant| {
            let item = variant
                .symbol
                .rsplit_once("::")
                .map_or(variant.symbol.as_str(), |(_, item)| item);
            (variant.file.as_str(), item)
        })
        .collect();
    let mut found = Vec::new();
    for unit in &workspace.units {
        let reasons = unit_reasons(job, unit);
        if !reasons.is_empty() {
            found.push(Match {
                symbol: unit.symbol.clone(),
                package: unit.package.clone(),
                file: unit.file.clone(),
                line: unit.line,
                reasons,
                in_home: local
                    && unit.package == home_package
                    && !second_home(job, workspace, unit),
                variant: variants.contains(&unit.symbol),
            });
        }
    }
    if job
        .forbidden
        .fingerprints
        .iter()
        .any(|id| id == crate::rules::VOCABULARY_LITERAL)
    {
        found.extend(vocabulary_matches(
            job,
            home_workspace,
            workspace,
            &variants,
        ));
    }
    if job
        .forbidden
        .fingerprints
        .iter()
        .any(|id| id == crate::rules::HOME_LITERAL)
    {
        found.extend(home_literal_matches(
            job,
            home_workspace,
            workspace,
            &variants,
        ));
    }
    for hit in &workspace.rule_hits {
        if job.forbidden.fingerprints.iter().any(|id| id == hit.rule) {
            found.push(Match {
                symbol: hit.symbol.clone(),
                package: hit.package.clone(),
                file: hit.file.clone(),
                line: hit.line,
                reasons: vec![format!("{}: {}", hit.rule, hit.detail)],
                in_home: local && hit.home_exempt && hit.package == home_package,
                variant: variants.contains(&hit.symbol)
                    || hit.symbol.rsplit_once("::").is_some_and(|(_, item)| {
                        rule_variants.contains(&(hit.file.as_str(), item))
                    }),
            });
        }
    }
    found
}

/// The vocabulary modules of a job: its home and entry points, resolved.
fn vocabulary_modules(job: &Job, workspace: &Workspace) -> Vec<String> {
    let mut modules: Vec<String> = std::iter::once(&job.home)
        .chain(&job.entry_points)
        .filter_map(|path| workspace.resolve(path))
        .collect();
    modules.sort();
    modules.dedup();
    modules
}

fn under(symbol: &str, modules: &[String]) -> bool {
    modules.iter().any(|module| {
        symbol
            .strip_prefix(module.as_str())
            .is_some_and(|rest| rest.starts_with("::"))
    })
}

/// Every `rule:vocabulary-literal` match: a string literal outside the job's
/// vocabulary modules that equals or starts with a namespace those modules
/// declare. A job whose modules declare no namespace is itself a match, so
/// the rule can never pass by reading nothing.
fn vocabulary_matches(
    job: &Job,
    home_workspace: &Workspace,
    workspace: &Workspace,
    variants: &BTreeSet<String>,
) -> Vec<Match> {
    let modules = vocabulary_modules(job, home_workspace);
    let mut namespaces: Vec<String> = home_workspace
        .units
        .iter()
        .filter(|unit| {
            unit.kind == UnitKind::Constant
                && crate::rules::is_namespace_name(&unit.name)
                && under(&unit.symbol, &modules)
        })
        .filter_map(|unit| match unit.strings.as_slice() {
            [(namespace, _)] if !namespace.is_empty() => Some(namespace.clone()),
            _ => None,
        })
        .collect();
    namespaces.sort();
    namespaces.dedup();
    if namespaces.is_empty() {
        return vec![Match {
            symbol: job.home.clone(),
            package: String::new(),
            file: String::new(),
            line: 0,
            reasons: vec![format!(
                "{}: the modules {} declare no namespace constant (`NS` or `*_NS`)",
                crate::rules::VOCABULARY_LITERAL,
                if modules.is_empty() {
                    "(none resolve)".to_owned()
                } else {
                    modules.join(", ")
                }
            )],
            in_home: false,
            variant: false,
        }];
    }
    let mut found = Vec::new();
    for unit in &workspace.units {
        if !unit.shipping || (!job.external && under(&unit.symbol, &modules)) {
            continue;
        }
        for (literal, line) in &unit.strings {
            if let Some(namespace) = crate::rules::vocabulary_namespace(literal, &namespaces) {
                found.push(Match {
                    symbol: unit.symbol.clone(),
                    package: unit.package.clone(),
                    file: unit.file.clone(),
                    line: *line,
                    reasons: vec![format!(
                        "{}: \"{literal}\" spells out a term of <{namespace}>",
                        crate::rules::VOCABULARY_LITERAL
                    )],
                    in_home: false,
                    variant: variants.contains(&unit.symbol),
                });
            }
        }
    }
    found
}

/// Every `rule:home-literal` match: a string literal outside the job's home
/// item equal to one the home item's bodies hold. A home that holds no literal
/// is itself a match, so the rule can never pass by reading nothing.
fn home_literal_matches(
    job: &Job,
    home_workspace: &Workspace,
    workspace: &Workspace,
    variants: &BTreeSet<String>,
) -> Vec<Match> {
    let home = home_workspace
        .resolve(&job.home)
        .into_iter()
        .collect::<Vec<_>>();
    let is_home = |symbol: &str| home.iter().any(|home| symbol == home) || under(symbol, &home);
    let mut tokens: Vec<&str> = home_workspace
        .units
        .iter()
        .filter(|unit| is_home(&unit.symbol))
        .flat_map(|unit| unit.strings.iter().map(|(text, _)| text.as_str()))
        .filter(|text| !text.is_empty())
        .collect();
    tokens.sort_unstable();
    tokens.dedup();
    if tokens.is_empty() {
        return vec![Match {
            symbol: job.home.clone(),
            package: String::new(),
            file: String::new(),
            line: 0,
            reasons: vec![format!(
                "{}: the home `{}` holds no string literal",
                crate::rules::HOME_LITERAL,
                job.home
            )],
            in_home: false,
            variant: false,
        }];
    }
    let mut found = Vec::new();
    for unit in &workspace.units {
        if !unit.shipping || (!job.external && is_home(&unit.symbol)) {
            continue;
        }
        for (literal, line) in &unit.strings {
            if tokens.binary_search(&literal.as_str()).is_ok() {
                found.push(Match {
                    symbol: unit.symbol.clone(),
                    package: unit.package.clone(),
                    file: unit.file.clone(),
                    line: *line,
                    reasons: vec![format!(
                        "{}: \"{literal}\" is a token `{}` spells",
                        crate::rules::HOME_LITERAL,
                        job.home
                    )],
                    in_home: false,
                    variant: variants.contains(&unit.symbol),
                });
            }
        }
    }
    found
}

/// Whether `unit` matches the job by name yet is neither the home nor one of its
/// entry points, inside the home crate: a second home. A constant is always a
/// copy there (a bound redeclared beside its home is a second bound); a function
/// is one only for a job that lists [`crate::rules::SOLE_NAME`], since a home
/// crate's own sibling helpers may legitimately share a job's name pattern.
fn second_home(job: &Job, workspace: &Workspace, unit: &Unit) -> bool {
    let strict = match unit.kind {
        UnitKind::Constant => true,
        UnitKind::Function => job
            .forbidden
            .fingerprints
            .iter()
            .any(|id| id == crate::rules::SOLE_NAME),
    };
    strict
        && job
            .forbidden
            .names
            .iter()
            .any(|pattern| pattern.is_match(&unit.name))
        && std::iter::once(&job.home)
            .chain(&job.entry_points)
            .all(|path| workspace.resolve(path).as_deref() != Some(unit.symbol.as_str()))
}

fn unit_reasons(job: &Job, unit: &Unit) -> Vec<String> {
    let mut reasons = Vec::new();
    if matches!(unit.kind, UnitKind::Function | UnitKind::Constant) {
        for pattern in &job.forbidden.names {
            if pattern.is_match(&unit.name) {
                reasons.push(format!("name /{}/", pattern.as_str()));
            }
        }
    }
    for constant in &job.forbidden.constants {
        if unit.constants.contains(constant) {
            reasons.push(format!("constant {constant:#x}"));
        }
    }
    for id in &job.forbidden.fingerprints {
        let body = unit
            .print
            .as_ref()
            .is_some_and(|print| &print.structural == id || print.shim.as_ref() == Some(id));
        if body || unit.tables.contains(id.as_str()) {
            reasons.push(format!("fingerprint {id}"));
        }
    }
    reasons
}

/// The copies `--count` reports: matches outside the home that are not variants.
pub(crate) fn copies(found: &[Match]) -> usize {
    found
        .iter()
        .filter(|found| !found.in_home && !found.variant)
        .count()
}

fn unit_object(unit: &Unit) -> Object {
    Object::new()
        .with("symbol", &unit.symbol)
        .with("package", &unit.package)
        .with("file", &unit.file)
        .with("line", unit.line)
        .with("shipping", unit.shipping)
}

fn group_json(group: &Group, workspace: &Workspace, variants: &BTreeSet<String>) -> Json {
    Json::from(
        Object::new()
            .with("fingerprint", &group.fingerprint)
            .with("tokens", group.tokens)
            .with(
                "sanctioned",
                group_is_sanctioned(group, workspace, variants),
            )
            .with(
                "members",
                group
                    .members
                    .iter()
                    .map(|&index| unit_object(&workspace.units[index]))
                    .collect::<Json>(),
            ),
    )
}

/// The baseline report and the size of each of its sections.
#[derive(Debug)]
pub(crate) struct Baseline {
    /// The JSON document.
    pub(crate) document: Json,
    /// Isomorphic body groups.
    pub(crate) isomorphic_groups: usize,
    /// Isomorphic shim groups.
    pub(crate) shim_groups: usize,
    /// Units holding a hex-digit table.
    pub(crate) hex_tables: usize,
    /// Integer constants above `0xffff` held in two or more packages.
    pub(crate) shared_constants: usize,
}

/// The baseline document: every group, every hex table, and every large integer
/// constant that appears in more than one package.
pub(crate) fn baseline(workspace: &Workspace, ledger: &Ledger) -> Baseline {
    let variants = variant_symbols(ledger, workspace, ISOMORPHIC);
    let groups = isomorphic_groups(workspace);
    let shims = shim_groups(workspace);
    let tables: Vec<Json> = workspace
        .units
        .iter()
        .filter(|unit| !unit.tables.is_empty())
        .map(|unit| {
            Json::from(
                unit_object(unit).with("tables", unit.tables.iter().copied().collect::<Json>()),
            )
        })
        .collect();
    let mut constants: BTreeMap<u128, BTreeMap<&str, Vec<&Unit>>> = BTreeMap::new();
    for unit in &workspace.units {
        for &value in &unit.constants {
            if value > 0xffff {
                constants
                    .entry(value)
                    .or_default()
                    .entry(unit.package.as_str())
                    .or_default()
                    .push(unit);
            }
        }
    }
    let shared: Vec<Json> = constants
        .into_iter()
        .filter(|(_, packages)| packages.len() >= 2)
        .map(|(value, packages)| {
            Json::from(
                Object::new()
                    .with("value", format!("{value:#x}"))
                    .with("packages", packages.len())
                    .with(
                        "units",
                        packages
                            .values()
                            .flatten()
                            .map(|unit| unit_object(unit))
                            .collect::<Json>(),
                    ),
            )
        })
        .collect();
    let counts = (groups.len(), shims.len(), tables.len(), shared.len());
    let document = Json::from(
        Object::new()
            .with("min_tokens", MIN_TOKENS)
            .with("units", workspace.units.len())
            .with(
                "isomorphic_groups",
                groups
                    .iter()
                    .map(|group| group_json(group, workspace, &variants))
                    .collect::<Json>(),
            )
            .with(
                "shim_groups",
                shims
                    .iter()
                    .map(|group| group_json(group, workspace, &variants))
                    .collect::<Json>(),
            )
            .with("hex_tables", tables)
            .with("shared_constants", shared),
    );
    Baseline {
        document,
        isomorphic_groups: counts.0,
        shim_groups: counts.1,
        hex_tables: counts.2,
        shared_constants: counts.3,
    }
}

/// One resolved definition, as the index lists it.
fn definition_json(workspace: &Workspace, symbol: String) -> Json {
    let definition = &workspace.definitions[&symbol];
    Json::from(
        Object::new()
            .with("symbol", symbol.as_str())
            .with("package", &definition.package)
            .with("file", &definition.file)
            .with("line", definition.line)
            .with("module", &definition.module)
            .with("documented", definition.documented),
    )
}

/// The machine-readable index: every ledger path resolved, every job's matches,
/// and every group with its sanction state.
pub(crate) fn index(workspace: &Workspace, external: Option<&Workspace>, ledger: &Ledger) -> Json {
    let mut symbols = Object::new();
    let mut jobs = Object::new();
    for job in &ledger.jobs {
        let home_side = home_workspace(job, workspace, external);
        let homed = std::iter::once(&job.home).chain(&job.entry_points);
        let variants = job
            .variants
            .iter()
            .flat_map(|variant| [&variant.symbol, &variant.anchor]);
        for (path, side) in homed
            .map(|path| (path, home_side.as_ref().ok().copied()))
            .chain(variants.map(|path| (path, Some(workspace))))
        {
            let key = if job.external && side.is_some_and(|side| !std::ptr::eq(side, workspace)) {
                format!("{}{path}", crate::ledger::EXTERNAL_PREFIX)
            } else {
                path.clone()
            };
            let entry = side
                .and_then(|side| side.resolve(path).map(|symbol| (side, symbol)))
                .map_or(Json::Null, |(side, symbol)| definition_json(side, symbol));
            symbols.insert(key, entry);
        }
        let Ok(home_side) = home_side else {
            jobs.insert(job.id.clone(), "unchecked: homed in purrdf, needs --cross");
            continue;
        };
        let Ok(resolved) = home(job, home_side) else {
            jobs.insert(job.id.clone(), Json::Null);
            continue;
        };
        let found = matches(job, home_side, workspace, &resolved.package);
        jobs.insert(
            job.id.clone(),
            Object::new()
                .with("home", resolved.symbol)
                .with("home_package", resolved.package)
                .with("copies", copies(&found))
                .with(
                    "matches",
                    found
                        .iter()
                        .map(|found| {
                            Object::new()
                                .with("symbol", &found.symbol)
                                .with("package", &found.package)
                                .with("file", &found.file)
                                .with("line", found.line)
                                .with("reasons", found.reasons.as_slice())
                                .with("in_home", found.in_home)
                                .with("variant", found.variant)
                        })
                        .collect::<Json>(),
                ),
        );
    }
    for row in &ledger.distinct {
        for path in row.members.iter().chain(std::iter::once(&row.anchor)) {
            let entry = workspace
                .resolve(path)
                .map_or(Json::Null, |symbol| definition_json(workspace, symbol));
            symbols.insert(path.clone(), entry);
        }
    }
    let variants = variant_symbols(ledger, workspace, ISOMORPHIC);
    let groups: Vec<Json> = every_group(workspace)
        .iter()
        .flat_map(|(kind, _, groups)| {
            groups.iter().map(|group| {
                Json::from(
                    Object::new()
                        .with("kind", kind.as_str())
                        .with("fingerprint", &group.fingerprint)
                        .with("members", member_symbols(group, workspace))
                        .with(
                            "variant_sanctioned",
                            group_is_sanctioned(group, workspace, &variants),
                        ),
                )
            })
        })
        .collect();
    let grouped: BTreeSet<&str> = isomorphic_groups(workspace)
        .iter()
        .chain(&shim_groups(workspace))
        .flat_map(|group| {
            group
                .members
                .iter()
                .map(|&index| workspace.units[index].symbol.as_str())
        })
        .collect();
    Json::from(
        Object::new()
            .with("symbols", symbols)
            .with("jobs", jobs)
            .with("groups", groups)
            .with(
                "grouped_variants",
                variants
                    .iter()
                    .filter(|symbol| grouped.contains(symbol.as_str()))
                    .collect::<Json>(),
            )
            .with("errors", workspace.errors.as_slice()),
    )
}

/// Every `--check` finding, one line each: the gate.
///
/// It fails on a file the walk could not read; a `#[path]` module attribute that
/// leaves its package; an isomorphic body or shim group that neither an
/// `isomorphic` variant nor a `[[distinct]]` row sanctions, and every
/// `[[distinct]]` row's own failures; and, per job, everything [`job_findings`]
/// reports. `tree` is the tree the workspace was walked from, for the files a job
/// names.
pub(crate) fn check_findings(
    workspace: &Workspace,
    ledger: &Ledger,
    tree: &dyn Tree,
    external: Option<&Workspace>,
) -> Vec<String> {
    let mut findings: Vec<String> = workspace.errors.clone();
    findings.extend(workspace.path_escapes.iter().cloned());
    let variants = variant_symbols(ledger, workspace, ISOMORPHIC);
    let mut matched = vec![false; ledger.distinct.len()];
    let mut grouped: BTreeSet<&str> = BTreeSet::new();
    for (kind, label, groups) in every_group(workspace) {
        for group in groups {
            grouped.extend(
                group
                    .members
                    .iter()
                    .map(|&index| workspace.units[index].symbol.as_str()),
            );
            if let Some(row) = distinct_row(ledger, kind, &member_symbols(&group, workspace)) {
                matched[row] = true;
                continue;
            }
            if group_is_sanctioned(&group, workspace, &variants) {
                continue;
            }
            let members: Vec<String> = group
                .members
                .iter()
                .map(|&index| {
                    let unit = &workspace.units[index];
                    format!("{} ({}:{})", unit.symbol, unit.file, unit.line)
                })
                .collect();
            findings.push(format!(
                "{label} {}: {}",
                group.fingerprint,
                members.join(", ")
            ));
        }
    }
    for (row, matched) in ledger.distinct.iter().zip(matched) {
        findings.extend(distinct_row_findings(row, matched, workspace));
    }
    // A variant of a purrdf-homed job is live by sharing a shape across the two
    // workspaces, not within Sparrow.
    let cross_grouped: BTreeSet<&str> = external.map_or_else(BTreeSet::new, |external| {
        crate::cross::cross_groups(workspace, external)
            .iter()
            .flat_map(|group| group.local.iter())
            .map(|&index| workspace.units[index].symbol.as_str())
            .collect()
    });
    for job in &ledger.jobs {
        // A purrdf-homed job without the external scan is not judged here; the
        // caller reports it as unchecked (see [`unchecked_jobs`]).
        let Ok(home_side) = home_workspace(job, workspace, external) else {
            continue;
        };
        let grouped = if job.external {
            &cross_grouped
        } else {
            &grouped
        };
        findings.extend(job_findings(job, home_side, workspace, tree, grouped));
    }
    findings
}

/// The jobs `--check` could not judge: those homed in purrdf, when the external
/// scan was not loaded. The caller says so rather than letting them pass.
pub(crate) fn unchecked_jobs<'l>(ledger: &'l Ledger, external: Option<&Workspace>) -> Vec<&'l str> {
    if external.is_some() {
        return Vec::new();
    }
    ledger
        .jobs
        .iter()
        .filter(|job| job.external)
        .map(|job| job.id.as_str())
        .collect()
}

/// One job's failures: a `home` or `entry_points` path that does not resolve; a
/// `vectors` path that does not exist; a `bench` id that is not a bench target the
/// walk entered; a variant that does not resolve, lives in another file, has an
/// anchor that does not resolve or carries no documentation, or no longer matches
/// what it is exempt from (STALE); a `body:`/`shim:` fingerprint that matches
/// nothing (STALE); an unenforced job with no copies left (STALE); and, for an
/// enforced job, every forbidden match outside the home that is not a variant.
/// `grouped` holds every symbol that is a member of some group (for a
/// purrdf-homed job, of some cross-workspace group). `home_workspace` is where
/// the home and entry points resolve; variants are always Sparrow's.
fn job_findings(
    job: &Job,
    home_workspace: &Workspace,
    workspace: &Workspace,
    tree: &dyn Tree,
    grouped: &BTreeSet<&str>,
) -> Vec<String> {
    let mut findings = Vec::new();
    let id = &job.id;
    for entry in &job.entry_points {
        if home_workspace.resolve(entry).is_none() {
            findings.push(format!(
                "job `{id}`: entry_points `{entry}` does not resolve to a defined item"
            ));
        }
    }
    let resolved = match home(job, home_workspace) {
        Ok(resolved) => resolved,
        Err(error) => {
            findings.push(error);
            return findings;
        }
    };
    // `vectors` and `bench` are Sparrow paths and targets; the ledger refuses
    // either on a purrdf-homed job, whose evidence is purrdf's own gate's.
    for vector in &job.vectors {
        if tree.read(vector).is_none() && tree.list(vector).is_empty() {
            findings.push(format!("job `{id}`: vectors `{vector}` does not exist"));
        }
    }
    for bench in &job.bench {
        let entered = bench.split_once(':').is_some_and(|(package, target)| {
            workspace.crates.contains(&format!(
                "{}::bench_crate::{}",
                package.replace('-', "_"),
                target.replace('-', "_")
            ))
        });
        if !entered {
            findings.push(format!(
                "job `{id}`: bench `{bench}` is not a `package:target` bench target"
            ));
        }
    }
    let found = matches(job, home_workspace, workspace, &resolved.package);
    for variant in &job.variants {
        let at = format!("job `{id}` variant `{}`", variant.symbol);
        let symbol = workspace.resolve(&variant.symbol);
        match &symbol {
            None => findings.push(format!(
                "{at}: the symbol does not resolve to a defined item"
            )),
            Some(symbol) if workspace.definitions[symbol].file != variant.file => {
                findings.push(format!(
                    "{at}: defined in {}, not in {}",
                    workspace.definitions[symbol].file, variant.file
                ));
            }
            Some(_) => {}
        }
        match workspace.resolve(&variant.anchor) {
            None => findings.push(format!(
                "{at}: anchor `{}` does not resolve",
                variant.anchor
            )),
            Some(anchor) if !workspace.definitions[&anchor].documented => findings.push(format!(
                "{at}: anchor `{}` carries no documentation",
                variant.anchor
            )),
            Some(_) => {}
        }
        let live = if variant.detector == ISOMORPHIC {
            symbol
                .as_deref()
                .is_some_and(|symbol| grouped.contains(symbol))
        } else {
            // A rule hit is named by its file and enclosing items; the variant it
            // matches is the one defined in that file under that name.
            let item = variant
                .symbol
                .rsplit_once("::")
                .map_or(variant.symbol.as_str(), |(_, item)| item);
            found.iter().any(|found| {
                found.variant
                    && (symbol.is_none()
                        || symbol.as_deref() == Some(found.symbol.as_str())
                        || (found.file == variant.file
                            && found
                                .symbol
                                .rsplit_once("::")
                                .is_some_and(|(_, last)| last == item)))
            })
        };
        if !live {
            findings.push(format!(
                "{at}: STALE, it no longer matches the `{}` detector; remove the row",
                variant.detector
            ));
        }
    }
    for fingerprint in &job.forbidden.fingerprints {
        let reason = format!("fingerprint {fingerprint}");
        if (fingerprint.starts_with("body:") || fingerprint.starts_with("shim:"))
            && !found.iter().any(|found| found.reasons.contains(&reason))
        {
            findings.push(format!(
                "job `{id}`: forbidden fingerprint `{fingerprint}` matches nothing (STALE)"
            ));
        }
    }
    if job.enforced {
        for found in &found {
            if !found.in_home && !found.variant {
                findings.push(format!(
                    "job `{id}`: {} ({}:{}) outside home {}: {}",
                    found.symbol,
                    found.file,
                    found.line,
                    resolved.package,
                    found.reasons.join(", ")
                ));
            }
        }
    } else if copies(&found) == 0 {
        findings.push(format!(
            "job `{id}`: no copies are left outside the home (STALE); set `enforced = true`"
        ));
    }
    findings
}

/// The human `--census` listing for one job.
pub(crate) fn census_lines(
    job: &Job,
    home_workspace: &Workspace,
    workspace: &Workspace,
) -> Result<Vec<String>, String> {
    let resolved = home(job, home_workspace)?;
    let found = matches(job, home_workspace, workspace, &resolved.package);
    let mut lines = vec![format!(
        "job {}: home {}{} ({}), {}",
        job.id,
        if job.external {
            crate::ledger::EXTERNAL_PREFIX
        } else {
            ""
        },
        resolved.symbol,
        resolved.package,
        if job.enforced {
            "enforced"
        } else {
            "not yet enforced"
        }
    )];
    for found in &found {
        let role = if found.in_home {
            "HOME   "
        } else if found.variant {
            "VARIANT"
        } else {
            "COPY   "
        };
        lines.push(format!(
            "  {role} {}:{} {} [{}]",
            found.file,
            found.line,
            found.symbol,
            found.reasons.join(", ")
        ));
    }
    lines.push(format!("copies={}", copies(&found)));
    Ok(lines)
}
