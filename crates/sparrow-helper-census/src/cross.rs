// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! The cross-workspace scan: Sparrow against purrdf.
//!
//! purrdf is Sparrow's selected semantic supplier, so a helper Sparrow writes that
//! purrdf already has is a second implementation as much as one copied between two
//! Sparrow crates. This module scans purrdf's packages with the same walk and the
//! same normaliser, and reports what the two workspaces share.
//!
//! **What is scanned is purrdf at the revision Sparrow pins, never the live
//! checkout.** The revision is the `git_revision` of the `purrdf` supplier
//! in `registry/compatibility.toml`; its `crates/` and `bindings/` trees are
//! materialised with `git archive <rev> crates bindings | tar -x` into
//! `target/helper-census/purrdf-<rev>/`, which is reused when present. A checkout
//! moves under its owner; a pinned revision does not, and the report names it, so a
//! result can be reproduced. `--external-root DIR` scans `DIR` instead and skips git
//! entirely (for a tree prepared by hand); the report then says it is unpinned.
//!
//! **The direction is one-way.** A group with no Sparrow member is purrdf's own
//! business and is dropped entirely; a group with no purrdf member is the ordinary
//! census's. Only groups and constants with members on both sides are reported.
//!
//! **Nothing here degrades to an empty scan.** A repository that cannot be archived,
//! a root that does not exist, a revision that is not a full object name, or a tree
//! with no package in it is an error: a cross report of nothing would read exactly
//! like a clean one.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::json::{Json, Object};
use crate::ledger::GroupKind;
use crate::normalize::MIN_TOKENS;
use crate::source::{Disk, Tree, Unit, Workspace};

/// The supplier entry whose `git_revision` is the purrdf revision Sparrow pins.
pub(crate) const SUPPLIER: &str = "purrdf";

/// The directories of a purrdf tree whose subdirectories are packages.
pub(crate) const PACKAGE_DIRS: [&str; 2] = ["crates", "bindings"];

/// The compatibility registry, relative to the Sparrow root.
const REGISTRY: &str = "registry/compatibility.toml";

/// Integer constants at or below this are too common to report as shared.
const CONSTANT_FLOOR: u128 = 0xffff;

/// Where the external scan came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Source {
    /// purrdf at the pinned revision, archived from `repository` into `tree`.
    Pinned {
        /// The full 40-digit revision.
        revision: String,
        /// The purrdf repository it was archived from.
        repository: PathBuf,
        /// The materialised tree.
        tree: PathBuf,
    },
    /// A tree given with `--external-root`, not checked against any pin.
    Root(PathBuf),
}

impl Source {
    /// The pinned revision, when there is one.
    fn revision(&self) -> Option<&str> {
        match self {
            Self::Pinned { revision, .. } => Some(revision),
            Self::Root(_) => None,
        }
    }

    /// One line naming the source, for a report's header and its errors.
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Pinned {
                revision,
                repository,
                tree,
            } => format!(
                "purrdf {revision} (pinned by {REGISTRY} supplier `{SUPPLIER}`; archived from {} into {})",
                repository.display(),
                tree.display()
            ),
            Self::Root(root) => format!(
                "purrdf tree {} (--external-root: UNPINNED, not checked against {REGISTRY})",
                root.display()
            ),
        }
    }
}

/// The external workspace, scanned.
#[derive(Debug)]
pub(crate) struct External {
    /// The walk over its packages.
    pub(crate) workspace: Workspace,
    /// Where it came from.
    pub(crate) source: Source,
}

/// The `git_revision` of the [`SUPPLIER`] entry in the compatibility registry's
/// text. Read line by line rather than as TOML: the registry uses inline tables,
/// which the ledger's strict subset refuses, and only two plain keys are needed.
pub(crate) fn pinned_revision(text: &str) -> Result<String, String> {
    let mut in_supplier = false;
    let mut name: Option<String> = None;
    let mut revision: Option<String> = None;
    let mut found: Option<Option<String>> = None;
    let mut finish = |name: &mut Option<String>, revision: &mut Option<String>| {
        if name.as_deref() == Some(SUPPLIER) {
            found = Some(revision.take());
        }
        *name = None;
        *revision = None;
    };
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') && trimmed.ends_with(']') && !trimmed.contains('=') {
            finish(&mut name, &mut revision);
            in_supplier = trimmed == "[[supplier]]";
            continue;
        }
        if !in_supplier {
            continue;
        }
        let Some((key, value)) = trimmed.split_once('=') else {
            continue;
        };
        let value = value.trim();
        let Some(value) = value
            .strip_prefix('"')
            .and_then(|rest| rest.split_once('"'))
            .map(|(value, _)| value.to_owned())
        else {
            continue;
        };
        match key.trim() {
            "name" => name = Some(value),
            "git_revision" => revision = Some(value),
            _ => {}
        }
    }
    finish(&mut name, &mut revision);
    let revision = found
        .ok_or_else(|| format!("{REGISTRY} has no [[supplier]] named `{SUPPLIER}`"))?
        .ok_or_else(|| format!("{REGISTRY}: supplier `{SUPPLIER}` has no `git_revision`"))?;
    if revision.len() == 40
        && revision
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        Ok(revision)
    } else {
        Err(format!(
            "{REGISTRY}: supplier `{SUPPLIER}` git_revision `{revision}` is not a full 40-digit object name"
        ))
    }
}

/// Materialise purrdf's package trees at `revision` under `cache`, or reuse them.
///
/// The archive is extracted into a directory private to this process and renamed
/// into place only once git and tar have both succeeded, so an interrupted run
/// never leaves a partial tree that a later run would reuse.
pub(crate) fn materialise(
    repository: &Path,
    revision: &str,
    cache: &Path,
) -> Result<PathBuf, String> {
    let target = cache.join(format!("purrdf-{revision}"));
    if target.is_dir() {
        return Ok(target);
    }
    let partial = cache.join(format!("purrdf-{revision}.partial-{}", std::process::id()));
    let failed = |message: String| {
        let _ = std::fs::remove_dir_all(&partial);
        Err(format!(
            "cannot materialise purrdf {revision} from {}: {message}",
            repository.display()
        ))
    };
    let _ = std::fs::remove_dir_all(&partial);
    if let Err(error) = std::fs::create_dir_all(&partial) {
        return failed(format!("{}: {error}", partial.display()));
    }
    let mut git = match Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(["archive", "--format=tar", revision])
        .args(PACKAGE_DIRS)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(git) => git,
        Err(error) => return failed(format!("git: {error}")),
    };
    let Some(archive) = git.stdout.take() else {
        return failed("git gave no output stream".to_owned());
    };
    let tar = Command::new("tar")
        .arg("-x")
        .arg("-C")
        .arg(&partial)
        .stdin(archive)
        .stderr(Stdio::piped())
        .output();
    let git = git.wait_with_output();
    match (git, tar) {
        (Err(error), _) => return failed(format!("git: {error}")),
        (_, Err(error)) => return failed(format!("tar: {error}")),
        (Ok(git), _) if !git.status.success() => {
            return failed(format!(
                "git archive: {}",
                String::from_utf8_lossy(&git.stderr).trim()
            ));
        }
        (_, Ok(tar)) if !tar.status.success() => {
            return failed(format!(
                "tar: {}",
                String::from_utf8_lossy(&tar.stderr).trim()
            ));
        }
        _ => {}
    }
    if let Err(error) = std::fs::rename(&partial, &target) {
        // Another run may have materialised the same revision meanwhile.
        if target.is_dir() {
            let _ = std::fs::remove_dir_all(&partial);
        } else {
            return failed(format!("{}: {error}", target.display()));
        }
    }
    Ok(target)
}

/// Walk an external tree's packages, refusing a walk that found none or could
/// not read a file.
pub(crate) fn walk_external(tree: &dyn Tree, source: Source) -> Result<External, String> {
    let workspace = Workspace::walk_dirs(tree, &PACKAGE_DIRS);
    if workspace.crates.is_empty() {
        return Err(format!(
            "{}: no package under {}; refusing to report a cross scan of nothing",
            source.describe(),
            PACKAGE_DIRS.join("/ or ")
        ));
    }
    if !workspace.errors.is_empty() {
        return Err(format!(
            "{}: the walk failed:\n  {}",
            source.describe(),
            workspace.errors.join("\n  ")
        ));
    }
    Ok(External { workspace, source })
}

/// Load the external scan for the Sparrow tree at `root`: `external_root` as
/// given, or else purrdf at the pinned revision archived from `repository`
/// (default `../purrdf` beside the Sparrow root).
pub(crate) fn load(
    root: &Path,
    repository: Option<&Path>,
    external_root: Option<&Path>,
) -> Result<External, String> {
    if let Some(external_root) = external_root {
        let canonical = external_root
            .canonicalize()
            .map_err(|error| format!("--external-root {}: {error}", external_root.display()))?;
        return walk_external(
            &Disk {
                root: canonical.clone(),
            },
            Source::Root(canonical),
        );
    }
    let registry = root.join(REGISTRY);
    let text = std::fs::read_to_string(&registry)
        .map_err(|error| format!("{}: {error}", registry.display()))?;
    let revision = pinned_revision(&text)?;
    let repository = repository.map_or_else(|| root.join("../purrdf"), Path::to_path_buf);
    let tree = materialise(&repository, &revision, &root.join("target/helper-census"))?;
    walk_external(
        &Disk { root: tree.clone() },
        Source::Pinned {
            revision,
            repository,
            tree,
        },
    )
}

/// The grouping key of a unit: a thin forwarder by its shim form (unless
/// [`crate::census::shim_key`] exempts it), any other body of at least
/// [`MIN_TOKENS`] by its structural form — the census's own rule.
fn key(unit: &Unit) -> Option<(GroupKind, &str, usize)> {
    let print = unit.print.as_ref()?;
    match &print.shim {
        Some(_) => crate::census::shim_key(unit).map(|shim| (GroupKind::Shim, shim, 0)),
        None if print.tokens >= MIN_TOKENS => {
            Some((GroupKind::Body, &print.structural, print.tokens))
        }
        None => None,
    }
}

/// One fingerprint with members in both workspaces.
#[derive(Debug)]
pub(crate) struct CrossGroup {
    /// Body or shim.
    pub(crate) kind: GroupKind,
    /// The shared fingerprint id.
    pub(crate) fingerprint: String,
    /// Tokens in the structural form (0 for a shim group).
    pub(crate) tokens: usize,
    /// Indexes into the Sparrow workspace's units.
    pub(crate) local: Vec<usize>,
    /// Indexes into the external workspace's units.
    pub(crate) external: Vec<usize>,
}

/// A fingerprint's token count and its members on each side.
type Sides = (usize, Vec<usize>, Vec<usize>);

/// Every fingerprint with at least one member on each side, largest first.
pub(crate) fn cross_groups(local: &Workspace, external: &Workspace) -> Vec<CrossGroup> {
    let mut by_key: BTreeMap<(GroupKind, &str), Sides> = BTreeMap::new();
    for (index, unit) in local.units.iter().enumerate() {
        if let Some((kind, print, tokens)) = key(unit) {
            by_key
                .entry((kind, print))
                .or_insert_with(|| (tokens, Vec::new(), Vec::new()))
                .1
                .push(index);
        }
    }
    for (index, unit) in external.units.iter().enumerate() {
        if let Some((kind, print, _)) = key(unit)
            && let Some(entry) = by_key.get_mut(&(kind, print))
        {
            entry.2.push(index);
        }
    }
    let mut groups: Vec<CrossGroup> = by_key
        .into_iter()
        .filter(|(_, (_, local, external))| !local.is_empty() && !external.is_empty())
        .map(
            |((kind, fingerprint), (tokens, local, external))| CrossGroup {
                kind,
                fingerprint: fingerprint.to_owned(),
                tokens,
                local,
                external,
            },
        )
        .collect();
    groups.sort_by(|left, right| {
        (right.local.len() + right.external.len())
            .cmp(&(left.local.len() + left.external.len()))
            .then(right.tokens.cmp(&left.tokens))
            .then(left.fingerprint.cmp(&right.fingerprint))
    });
    groups
}

/// One integer constant above `0xffff` held on both sides.
#[derive(Debug)]
pub(crate) struct CrossConstant {
    /// Its value.
    pub(crate) value: u128,
    /// Sparrow units holding it.
    pub(crate) local: Vec<usize>,
    /// External units holding it.
    pub(crate) external: Vec<usize>,
}

/// Every constant above `0xffff` held by a unit on each side, by value.
pub(crate) fn cross_constants(local: &Workspace, external: &Workspace) -> Vec<CrossConstant> {
    let holders = |workspace: &Workspace| {
        let mut by_value: BTreeMap<u128, Vec<usize>> = BTreeMap::new();
        for (index, unit) in workspace.units.iter().enumerate() {
            for &value in &unit.constants {
                if value > CONSTANT_FLOOR {
                    by_value.entry(value).or_default().push(index);
                }
            }
        }
        by_value
    };
    let mut external_holders = holders(external);
    holders(local)
        .into_iter()
        .filter_map(|(value, local)| {
            external_holders
                .remove(&value)
                .map(|external| CrossConstant {
                    value,
                    local,
                    external,
                })
        })
        .collect()
}

fn unit_json(unit: &Unit) -> Json {
    Json::from(
        Object::new()
            .with("symbol", &unit.symbol)
            .with("package", &unit.package)
            .with("file", &unit.file)
            .with("line", unit.line)
            .with("shipping", unit.shipping),
    )
}

fn units_json(workspace: &Workspace, indexes: &[usize]) -> Json {
    indexes
        .iter()
        .map(|&index| unit_json(&workspace.units[index]))
        .collect()
}

/// The cross baseline and the size of each of its sections.
#[derive(Debug)]
pub(crate) struct CrossBaseline {
    /// The JSON document.
    pub(crate) document: Json,
    /// Cross body groups.
    pub(crate) body_groups: usize,
    /// Cross shim groups.
    pub(crate) shim_groups: usize,
    /// Sparrow units in some cross group.
    pub(crate) local_units: usize,
    /// Constants held on both sides.
    pub(crate) constants: usize,
}

/// The cross baseline document: the source and its revision, every cross group
/// and every shared constant, both sides' locations.
pub(crate) fn baseline(local: &Workspace, external: &External) -> CrossBaseline {
    let groups = cross_groups(local, &external.workspace);
    let constants = cross_constants(local, &external.workspace);
    let group_json = |group: &CrossGroup| {
        Json::from(
            Object::new()
                .with("kind", group.kind.as_str())
                .with("fingerprint", &group.fingerprint)
                .with("tokens", group.tokens)
                .with("sparrow", units_json(local, &group.local))
                .with("purrdf", units_json(&external.workspace, &group.external)),
        )
    };
    let body_groups = groups
        .iter()
        .filter(|group| group.kind == GroupKind::Body)
        .count();
    let local_units = groups.iter().map(|group| group.local.len()).sum();
    let document = Json::from(
        Object::new()
            .with(
                "external",
                Object::new()
                    .with(
                        "revision",
                        external.source.revision().map_or(Json::Null, Json::from),
                    )
                    .with("source", external.source.describe())
                    .with("units", external.workspace.units.len()),
            )
            .with("sparrow_units", local.units.len())
            .with("min_tokens", MIN_TOKENS)
            .with(
                "cross_groups",
                groups.iter().map(group_json).collect::<Json>(),
            )
            .with(
                "shared_constants",
                constants
                    .iter()
                    .map(|constant| {
                        Object::new()
                            .with("value", format!("{:#x}", constant.value))
                            .with("sparrow", units_json(local, &constant.local))
                            .with(
                                "purrdf",
                                units_json(&external.workspace, &constant.external),
                            )
                    })
                    .collect::<Json>(),
            ),
    );
    CrossBaseline {
        document,
        body_groups,
        shim_groups: groups.len() - body_groups,
        local_units,
        constants: constants.len(),
    }
}

fn unit_line(side: &str, unit: &Unit) -> String {
    format!(
        "    {side:<8} {}:{}  {}{}",
        unit.file,
        unit.line,
        unit.symbol,
        if unit.shipping { "" } else { "  (test)" }
    )
}

/// The human cross report: the source first, then every cross group and shared
/// constant with both sides' locations.
pub(crate) fn report(local: &Workspace, external: &External) -> Vec<String> {
    let groups = cross_groups(local, &external.workspace);
    let constants = cross_constants(local, &external.workspace);
    let mut lines = vec![
        format!("cross census against {}", external.source.describe()),
        format!(
            "{} cross groups ({} Sparrow units), {} shared constants; Sparrow {} units, purrdf {} units",
            groups.len(),
            groups.iter().map(|group| group.local.len()).sum::<usize>(),
            constants.len(),
            local.units.len(),
            external.workspace.units.len()
        ),
    ];
    for group in &groups {
        lines.push(format!(
            "{} {} ({} tokens, {} sparrow / {} purrdf)",
            group.kind.as_str(),
            group.fingerprint,
            group.tokens,
            group.local.len(),
            group.external.len()
        ));
        for &index in &group.local {
            lines.push(unit_line("sparrow", &local.units[index]));
        }
        for &index in &group.external {
            lines.push(unit_line("purrdf", &external.workspace.units[index]));
        }
    }
    for constant in &constants {
        lines.push(format!("constant {:#x}", constant.value));
        for &index in &constant.local {
            lines.push(unit_line("sparrow", &local.units[index]));
        }
        for &index in &constant.external {
            lines.push(unit_line("purrdf", &external.workspace.units[index]));
        }
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::pinned_revision;

    const REVISION: &str = "b808a0e59bf4f1d0d183b38e75366a65b6d930eb";

    #[test]
    fn the_pinned_revision_is_the_named_suppliers() {
        let text = format!(
            "[[supplier]]\nname = \"purrdf-hnsw\"\ngit_revision = \"{}\"\n\n[[supplier]]\nname = \"purrdf\" # the one\nversion = \"3\"\nverified = [\n  {{ path = \"x\", tree = \"y\" }},\n]\ngit_revision = \"{REVISION}\"\n[other]\ngit_revision = \"{}\"\n",
            "0".repeat(40),
            "1".repeat(40)
        );
        assert_eq!(pinned_revision(&text).as_deref(), Ok(REVISION));
    }

    #[test]
    fn a_missing_or_malformed_pin_is_refused() {
        assert!(
            pinned_revision("[[supplier]]\nname = \"purrdf-hnsw\"\n")
                .unwrap_err()
                .contains("no [[supplier]]")
        );
        assert!(
            pinned_revision("[[supplier]]\nname = \"purrdf\"\n")
                .unwrap_err()
                .contains("no `git_revision`")
        );
        for bad in ["b808a0e5", "--output=/tmp/x", &REVISION.to_uppercase()] {
            let text = format!("[[supplier]]\nname = \"purrdf\"\ngit_revision = \"{bad}\"\n");
            assert!(
                pinned_revision(&text).unwrap_err().contains("40-digit"),
                "{bad}"
            );
        }
    }

    /// The registry this repository actually carries names a pin.
    #[test]
    fn this_repositorys_registry_pins_a_revision() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let text = std::fs::read_to_string(root.join(super::REGISTRY)).expect("the registry");
        assert_eq!(pinned_revision(&text).map(|rev| rev.len()), Ok(40));
    }
}
