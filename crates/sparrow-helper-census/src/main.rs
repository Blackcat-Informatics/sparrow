// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! `helper-census` — the structural census of the workspace's Rust: shipping
//! code, and the test, bench and example targets whose support code is copied as
//! readily.
//!
//! It finds functionality written more than once: function bodies that are the
//! same algorithm under different names, constants and borrow spellings (clone
//! types 1 and 2, and the value-form near misses of type 3), thin
//! forwarders that forward to the same place, integer constants compared by value
//! whatever their spelling, and hex-digit tables. It reads `helpers-ledger.toml`,
//! which names the one home of each job and the sanctioned variants, and reports
//! against it. `--check` is the gate; see [`USAGE`] for the modes.
//!
//! **This is test infrastructure, not a runtime crate.** No runtime crate may
//! depend on it. It reads source text only — it never opens a store root or
//! links a Sparrow runtime crate.
//!
//! # Provenance
//!
//! Ported from Katamari's `crates/katamari-helper-census` at revision
//! `f5290a439388e5b06bd48fd8464551df842dd5c5`, with no local changes in that
//! source directory. Katamari originally ported purrdf's `crates/helper-census`
//! at purrdf revision
//! `575e5210e93c522d05bab091964682c8faeb5fb7` (`origin/main`). The grouping,
//! normalisation, ledger, TOML subset and walk are purrdf's; a fingerprint id is
//! the same BLAKE3 prefix over the same normal form, so an id means the same body
//! shape in both repositories. What was left behind, because it names purrdf's own
//! homes, paths or policies: the byte-layout rules (`layout.rs`), the RDF structure
//! rules (`structure.rs`), the Cargo-feature and PyO3 policies (`--no-features`,
//! `--python-binding-tests`), the non-Rust ratchet (`--non-rust-ratchet`), the
//! lexical-terminal rules and the rules purrdf delegates to its hash-domain script,
//! and the ledger keys `sites` and `replaces_external`. purrdf's Python gate
//! (`scripts/check-shared-helpers.py`) is not ported: the ledger checks it added on
//! top of the census — entry points, vectors, bench targets, variant staleness,
//! `enforced`, stale fingerprints and the cross-package `#[path]` rule — are in
//! `--check` itself.
//!
//! # Against purrdf
//!
//! `--cross-baseline` and `--cross-report` scan purrdf as well — at the revision
//! `registry/compatibility.toml` pins, never the live checkout — and report every
//! body or shim shape and large constant the two workspaces share ([`cross`]). A
//! ledger job may be homed in purrdf (`home = "purrdf:crate::path::item"`); with
//! `--cross`, `--check` then counts every Sparrow match of it as a copy. The
//! direction is one-way: duplication inside purrdf is never this census's finding.

mod census;
mod cross;
mod json;
mod ledger;
mod normalize;
mod rules;
mod source;
mod toml;

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use crate::normalize::MIN_TOKENS;
use crate::source::{Disk, Memory, Tree, Workspace};

/// The CLI contract, printed by `--help` and beside every argument error.
const USAGE: &str = "\
Usage: helper-census [--root DIR] [--ledger FILE] [--cross] [EXTERNAL] <mode>

Modes:
  --baseline [PATH]  write every isomorphic group, shim group, hex table and
                     cross-package constant as JSON (default PATH:
                     target/helper-census/clone-baseline.json under the root)
  --census JOB       list the units matching JOB's forbidden set
  --count JOB        print copies=<n>: matches outside JOB's home that are not
                     ledger variants
  --check            the gate: fail on any unsanctioned isomorphic group, any
                     ledger row that does not hold, any forbidden match of an
                     enforced job outside its home, and any #[path] module that
                     leaves its package
  --index            print the machine-readable JSON index
  --dump-ledger      print the parsed ledger as JSON
  --cross-baseline [PATH]
                     write every body/shim group and constant above 0xffff with
                     members in BOTH Sparrow and purrdf as JSON (default PATH:
                     target/helper-census/cross-baseline.json under the root)
  --cross-report     print the same, for a reader
  --self-test        run the seeded fixtures and exit
  --help, -h         print this text

--cross              also scan purrdf, for --check, --census, --count and
                     --index: a job homed in purrdf (home = \"purrdf:...\") is
                     judged only then. Implied by the two cross modes.

EXTERNAL (only with a purrdf scan; at most one):
  --purrdf-repo DIR    the purrdf repository to `git archive` the pinned
                       revision from (default: ../purrdf beside the root)
  --external-root DIR  scan DIR's crates/ and bindings/ as purrdf instead,
                       skipping git and the pin (the report says UNPINNED)

The purrdf revision is the git_revision of the purrdf supplier in
registry/compatibility.toml; its crates/ and bindings/ are archived once into
target/helper-census/purrdf-<rev>/ and reused. --root defaults to the workspace
this binary was built from; --ledger defaults to helpers-ledger.toml under the
root.
";

/// What the arguments ask for.
#[derive(Debug, PartialEq, Eq)]
enum Mode {
    Baseline(Option<PathBuf>),
    Census(String),
    Count(String),
    Check,
    Index,
    DumpLedger,
    CrossBaseline(Option<PathBuf>),
    CrossReport,
    SelfTest,
    Help,
}

impl Mode {
    /// Whether the mode is a cross-workspace report, which always scans purrdf.
    const fn is_cross(&self) -> bool {
        matches!(self, Self::CrossBaseline(_) | Self::CrossReport)
    }

    /// Whether `--cross` may widen the mode with a purrdf scan.
    const fn takes_cross(&self) -> bool {
        matches!(
            self,
            Self::Census(_) | Self::Count(_) | Self::Check | Self::Index
        )
    }
}

/// Where the purrdf scan comes from, when one was asked for.
#[derive(Debug, Default, PartialEq, Eq)]
struct ExternalOptions {
    /// `--purrdf-repo`.
    repository: Option<PathBuf>,
    /// `--external-root`.
    root: Option<PathBuf>,
}

/// Parsed arguments.
#[derive(Debug)]
struct Arguments {
    root: PathBuf,
    ledger: Option<PathBuf>,
    mode: Mode,
    /// `Some` exactly when purrdf is to be scanned.
    external: Option<ExternalOptions>,
}

/// The workspace this binary was built from: two levels above this crate's
/// manifest directory (`crates/sparrow-helper-census`).
fn workspace_root() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(Path::parent)
        .unwrap_or(manifest)
        .to_path_buf()
}

/// An optional PATH after a mode flag: the next argument unless it is a flag.
fn optional_path(
    args: &mut impl Iterator<Item = String>,
    pending: &mut Vec<String>,
) -> Option<PathBuf> {
    match args.next() {
        Some(next) if next.starts_with("--") => {
            pending.push(next);
            None
        }
        other => other.map(PathBuf::from),
    }
}

fn parse_arguments(mut args: impl Iterator<Item = String>) -> Result<Arguments, String> {
    let mut root = workspace_root();
    let mut ledger = None;
    let mut mode = None;
    let mut cross = false;
    let mut options = ExternalOptions::default();
    let set = |next: Mode, mode: &mut Option<Mode>| {
        if mode.replace(next).is_some() {
            Err("give exactly one mode".to_owned())
        } else {
            Ok(())
        }
    };
    let mut pending: Vec<String> = Vec::new();
    while let Some(arg) = pending.pop().or_else(|| args.next()) {
        match arg.as_str() {
            "--root" => root = args.next().ok_or("--root needs a directory")?.into(),
            "--ledger" => ledger = Some(args.next().ok_or("--ledger needs a file")?.into()),
            "--cross" => cross = true,
            "--purrdf-repo" => {
                let dir = args.next().ok_or("--purrdf-repo needs a directory")?;
                if options.repository.replace(dir.into()).is_some() {
                    return Err("give --purrdf-repo once".to_owned());
                }
            }
            "--external-root" => {
                let dir = args.next().ok_or("--external-root needs a directory")?;
                if options.root.replace(dir.into()).is_some() {
                    return Err("give --external-root once".to_owned());
                }
            }
            "--baseline" => {
                let path = optional_path(&mut args, &mut pending);
                set(Mode::Baseline(path), &mut mode)?;
            }
            "--cross-baseline" => {
                let path = optional_path(&mut args, &mut pending);
                set(Mode::CrossBaseline(path), &mut mode)?;
            }
            "--cross-report" => set(Mode::CrossReport, &mut mode)?,
            "--census" => set(
                Mode::Census(args.next().ok_or("--census needs a job id")?),
                &mut mode,
            )?,
            "--count" => set(
                Mode::Count(args.next().ok_or("--count needs a job id")?),
                &mut mode,
            )?,
            "--check" => set(Mode::Check, &mut mode)?,
            "--index" => set(Mode::Index, &mut mode)?,
            "--dump-ledger" => set(Mode::DumpLedger, &mut mode)?,
            "--self-test" => set(Mode::SelfTest, &mut mode)?,
            "--help" | "-h" => set(Mode::Help, &mut mode)?,
            other => return Err(format!("unknown argument `{other}`")),
        }
    }
    let mode = mode.ok_or("give a mode")?;
    if cross && !mode.takes_cross() {
        return Err(
            "--cross widens --check, --census, --count and --index only (the cross modes imply it)"
                .to_owned(),
        );
    }
    let scans = cross || mode.is_cross();
    if options.repository.is_some() && options.root.is_some() {
        return Err("give at most one of --purrdf-repo and --external-root".to_owned());
    }
    if !scans && options != ExternalOptions::default() {
        return Err(
            "--purrdf-repo and --external-root need a purrdf scan: --cross or a cross mode"
                .to_owned(),
        );
    }
    Ok(Arguments {
        root,
        ledger,
        mode,
        external: scans.then_some(options),
    })
}

fn main() -> ExitCode {
    let arguments = match parse_arguments(std::env::args().skip(1)) {
        Ok(arguments) => arguments,
        Err(error) => {
            eprintln!("helper-census: {error}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    match run(&arguments) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("helper-census: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(arguments: &Arguments) -> Result<ExitCode, String> {
    match &arguments.mode {
        Mode::Help => {
            print!("{USAGE}");
            return Ok(ExitCode::SUCCESS);
        }
        Mode::SelfTest => return Ok(self_test()),
        _ => {}
    }
    let root = arguments
        .root
        .canonicalize()
        .map_err(|error| format!("{}: {error}", arguments.root.display()))?;
    let ledger_path = arguments
        .ledger
        .clone()
        .unwrap_or_else(|| root.join("helpers-ledger.toml"));
    let ledger_text = std::fs::read_to_string(&ledger_path)
        .map_err(|error| format!("{}: {error}", ledger_path.display()))?;
    let ledger = ledger::parse(&ledger_text)?;
    if arguments.mode == Mode::DumpLedger {
        println!("{}", toml::Value::Table(ledger.document).to_json().pretty());
        return Ok(ExitCode::SUCCESS);
    }
    let disk = Disk { root: root.clone() };
    let workspace = Workspace::walk(&disk);
    if workspace.crates.is_empty() {
        return Err(format!(
            "{}: no package under crates/; refusing to report a census of nothing",
            root.display()
        ));
    }
    let external = arguments
        .external
        .as_ref()
        .map(|options| {
            cross::load(
                &root,
                options.repository.as_deref(),
                options.root.as_deref(),
            )
        })
        .transpose()?;
    let external_workspace = external.as_ref().map(|external| &external.workspace);
    if let Some(external) = &external {
        eprintln!("helper-census: scanning {}", external.source.describe());
    }
    if !workspace.errors.is_empty() && arguments.mode != Mode::Index {
        return Err(format!(
            "the walk failed:\n  {}",
            workspace.errors.join("\n  ")
        ));
    }
    let job = |id: &str| {
        ledger
            .job(id)
            .ok_or_else(|| format!("no job `{id}` in the ledger"))
    };
    match &arguments.mode {
        Mode::Baseline(path) => {
            let path = path
                .clone()
                .unwrap_or_else(|| root.join("target/helper-census/clone-baseline.json"));
            let report = census::baseline(&workspace, &ledger);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|error| format!("{}: {error}", parent.display()))?;
            }
            std::fs::write(&path, report.document.pretty() + "\n")
                .map_err(|error| format!("{}: {error}", path.display()))?;
            println!(
                "{} units; {} isomorphic groups, {} shim groups, {} hex tables, {} shared constants -> {}",
                workspace.units.len(),
                report.isomorphic_groups,
                report.shim_groups,
                report.hex_tables,
                report.shared_constants,
                path.display()
            );
        }
        Mode::Census(id) => {
            let job = job(id)?;
            let home_side = census::home_workspace(job, &workspace, external_workspace)?;
            for line in census::census_lines(job, home_side, &workspace)? {
                println!("{line}");
            }
        }
        Mode::Count(id) => {
            let job = job(id)?;
            let home_side = census::home_workspace(job, &workspace, external_workspace)?;
            let home = census::home(job, home_side)?;
            println!(
                "copies={}",
                census::copies(&census::matches(job, home_side, &workspace, &home.package))
            );
        }
        Mode::CrossBaseline(path) => {
            let Some(external) = &external else {
                unreachable!("parse_arguments scans purrdf for every cross mode")
            };
            let path = path
                .clone()
                .unwrap_or_else(|| root.join("target/helper-census/cross-baseline.json"));
            let report = cross::baseline(&workspace, external);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|error| format!("{}: {error}", parent.display()))?;
            }
            std::fs::write(&path, report.document.pretty() + "\n")
                .map_err(|error| format!("{}: {error}", path.display()))?;
            println!(
                "{}: {} cross body groups, {} cross shim groups ({} Sparrow units), {} shared constants -> {}",
                external.source.describe(),
                report.body_groups,
                report.shim_groups,
                report.local_units,
                report.constants,
                path.display()
            );
        }
        Mode::CrossReport => {
            let Some(external) = &external else {
                unreachable!("parse_arguments scans purrdf for every cross mode")
            };
            for line in cross::report(&workspace, external) {
                println!("{line}");
            }
        }
        Mode::Check => {
            for id in census::unchecked_jobs(&ledger, external_workspace) {
                println!(
                    "helper-census --check: job `{id}` is homed in purrdf and NOT CHECKED without --cross"
                );
            }
            let findings = census::check_findings(&workspace, &ledger, &disk, external_workspace);
            if !findings.is_empty() {
                eprintln!("helper-census --check: {} finding(s)", findings.len());
                for finding in &findings {
                    eprintln!("  {finding}");
                }
                return Ok(ExitCode::FAILURE);
            }
            println!(
                "OK: {} ledger jobs and {} [[distinct]] rows hold; no unsanctioned isomorphic group",
                ledger.jobs.len(),
                ledger.distinct.len()
            );
        }
        Mode::Index => println!(
            "{}",
            census::index(&workspace, external_workspace, &ledger).pretty()
        ),
        Mode::DumpLedger | Mode::SelfTest | Mode::Help => {
            unreachable!("handled above")
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// The seeded fixture workspace: a home package and a second package that
/// re-derives it, plus the near misses that must not be reported.
fn fixture_tree() -> Memory {
    let files = [
        (
            "crates/alpha/Cargo.toml",
            "[package]\nname = \"fixture-alpha\"\n",
        ),
        (
            "crates/alpha/src/lib.rs",
            "/// The one FNV-1a.\npub fn fnv1a(bytes: &[u8]) -> u64 {\n    let mut hash = 0xcbf2_9ce4_8422_2325_u64;\n    for byte in bytes {\n        hash ^= u64::from(*byte);\n        hash = hash.wrapping_mul(0x0100_0000_01b3);\n    }\n    hash\n}\n",
        ),
        (
            "crates/beta/Cargo.toml",
            "[package]\nname = \"fixture-beta\"\n",
        ),
        (
            "crates/beta/src/lib.rs",
            "mod digits;\n/// A retyped copy under other names.\npub fn fold(input: &[u8]) -> u64 {\n    let mut state = 14695981039346656037_u64;\n    for b in input {\n        state ^= u64::from(*b);\n        state = state.wrapping_mul(1099511628211);\n    }\n    state\n}\n/// The same length, a different algorithm.\npub fn count(input: &[u8]) -> u64 {\n    let mut state = 0_u64;\n    let mut index = 0;\n    while index < input.len() {\n        if input[index] == b'/' { state += 1; }\n        index += 1;\n    }\n    state\n}\n#[cfg(test)]\nmod tests {\n    fn fnv_again(bytes: &[u8]) -> u64 {\n        let mut hash = 0xcbf2_9ce4_8422_2325_u64;\n        for byte in bytes { hash ^= u64::from(*byte); hash = hash.wrapping_mul(0x0100_0000_01b3); }\n        hash\n    }\n}\n",
        ),
        (
            "crates/beta/src/digits.rs",
            // The two tables' digits are joined at run time below, so this
            // file spells no table of its own.
            "pub(crate) const DIGITS: &[u8; 16] = b\"01234567LOWER\";\npub(crate) const NOT_DIGITS: &[u8; 16] = b\"01234567NEAR\";\n",
        ),
        (
            "crates/beta/tests/copy.rs",
            "pub fn fnv1a(bytes: &[u8]) -> u64 { 0xcbf2_9ce4_8422_2325 }\n",
        ),
    ];
    Memory {
        files: files
            .into_iter()
            .map(|(path, text)| {
                let text = text
                    .replace("LOWER", "89abcdef")
                    .replace("NEAR", "89abcdeF");
                (path.to_owned(), text)
            })
            .collect(),
    }
}

const FIXTURE_JOB: &str = "[[job]]\nid = \"fnv\"\nsummary = \"FNV-1a\"\nhome = \"fixture_alpha::fnv1a\"\nentry_points = []\nspec = \"FNV-1a\"\nvectors = []\nbench = []\nenforced = true\n[job.forbidden]\nconstants = [\"0xcbf29ce484222325\"]\nfingerprints = [\"table:hex-lower\"]\nnames = []\n";

const FIXTURE_VARIANT: &str = "[[job.variant]]\nsymbol = \"fixture_beta::fold\"\nfile = \"crates/beta/src/lib.rs\"\ndetector = \"DETECTOR\"\ncriterion = \"a\"\nanchor = \"fixture_beta::fold\"\nreason = \"fixture\"\n";

const FIXTURE_DISTINCT: &str = "[[distinct]]\nmembers = [MEMBERS]\nkind = \"KIND\"\ncriterion = \"b\"\nanchor = \"ANCHOR\"\nreason = \"fixture\"\n";

/// The `--check` findings over `tree` under a ledger of one `[[distinct]]` row.
fn distinct_findings(tree: &dyn Tree, members: &str, kind: &str, anchor: &str) -> Vec<String> {
    let row = FIXTURE_DISTINCT
        .replace("MEMBERS", members)
        .replace("KIND", kind)
        .replace("ANCHOR", anchor);
    ledger::parse(&row).map_or_else(
        |error| vec![error],
        |ledger| census::check_findings(&Workspace::walk(tree), &ledger, tree, None),
    )
}

/// The `[[distinct]]` cases: the row sanctions its exact group and nothing
/// else, and a row that no longer matches, or whose anchor is undocumented, fails.
fn distinct_cases() -> Vec<(&'static str, bool)> {
    let tree = fixture_tree();
    let pair = "\"fixture_beta::fold\", \"fixture_alpha::fnv1a\"";
    let mut grown = fixture_tree();
    grown.files.insert(
        "crates/gamma/Cargo.toml".to_owned(),
        "[package]\nname = \"fixture-gamma\"\n".to_owned(),
    );
    grown.files.insert(
        "crates/gamma/src/lib.rs".to_owned(),
        "/// A third copy of the sanctioned shape.\npub fn mix(data: &[u8]) -> u64 {\n    let mut acc = 1_u64;\n    for d in data {\n        acc ^= u64::from(*d);\n        acc = acc.wrapping_mul(3);\n    }\n    acc\n}\n".to_owned(),
    );
    vec![
        (
            "a distinct row naming exactly its group's members sanctions it",
            distinct_findings(&tree, pair, "body", "fixture_alpha::fnv1a").is_empty(),
        ),
        (
            "a distinct row of the other kind does not sanction the group, and is STALE",
            distinct_findings(&tree, pair, "shim", "fixture_alpha::fnv1a").len() == 2,
        ),
        (
            "a group that gains a member its distinct row does not name fails, and the row is STALE",
            distinct_findings(&grown, pair, "body", "fixture_alpha::fnv1a")
                .iter()
                .filter(|finding| {
                    finding.contains("fixture_gamma::mix") || finding.contains("STALE")
                })
                .count()
                == 2,
        ),
        (
            "a distinct row whose members are no group is STALE, and the group still fails",
            {
                let findings = distinct_findings(
                    &tree,
                    "\"fixture_alpha::fnv1a\", \"fixture_beta::count\"",
                    "body",
                    "fixture_alpha::fnv1a",
                );
                findings.len() == 2
                    && findings
                        .iter()
                        .any(|finding| finding.starts_with("isomorphic bodies"))
                    && findings.iter().any(|finding| finding.contains("STALE"))
            },
        ),
        (
            "a distinct row naming a member that no longer exists is STALE",
            distinct_findings(
                &tree,
                "\"fixture_alpha::fnv1a\", \"fixture_beta::gone\"",
                "body",
                "fixture_alpha::fnv1a",
            )
            .iter()
            .any(|finding| finding.contains("STALE") && finding.contains("fixture_beta::gone")),
        ),
        (
            "a distinct row whose anchor carries no documentation fails",
            distinct_findings(&tree, pair, "body", "fixture_beta::digits::DIGITS")
                == [
                    "[[distinct]] body [fixture_alpha::fnv1a, fixture_beta::fold]: anchor `fixture_beta::digits::DIGITS` carries no documentation",
                ],
        ),
        (
            "a distinct row whose anchor does not resolve fails",
            distinct_findings(&tree, pair, "body", "fixture_alpha::missing")
                .iter()
                .any(|finding| finding.contains("does not resolve")),
        ),
    ]
}

/// The seeded near-miss workspace: copies that differ only in how they hold
/// their values, a test helper copied between two test files, and a copy below
/// the old thirty-token floor — each of which must be grouped — beside the
/// neighbours of each that must not be.
fn near_miss_tree() -> Memory {
    const RULE_PATH: &str = "use std::collections::{BTreeMap, BTreeSet, VecDeque};
/// The shortest rule path, over rule indices.
pub fn shortest_rule_path(depends: &BTreeMap<usize, BTreeSet<usize>>, from: usize, to: usize) -> Option<Vec<usize>> {
    let mut parent: BTreeMap<usize, usize> = BTreeMap::new();
    let mut queue: VecDeque<usize> = VecDeque::from([from]);
    let mut seen: BTreeSet<usize> = BTreeSet::from([from]);
    while let Some(node) = queue.pop_front() {
        if node == to {
            let mut path = vec![node];
            let mut cursor = node;
            while cursor != from {
                cursor = parent[&cursor];
                path.push(cursor);
            }
            path.reverse();
            return Some(path);
        }
        for &next in depends.get(&node).into_iter().flatten() {
            if seen.insert(next) {
                parent.insert(next, node);
                queue.push_back(next);
            }
        }
    }
    None
}
/// A comment line or a blank one.
pub fn skipped(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.is_empty() || trimmed.starts_with('#')
}
/// The same shape, another comment marker: another table.
pub fn skipped_ini(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.is_empty() || trimmed.starts_with(';')
}
/// A state machine's stop reason.
pub enum Run { Stopped { tripped: u8, partial: Vec<u8> }, Running }
impl Run {
    /// The governor that tripped.
    pub fn tripped(&self) -> Option<u8> {
        match self {
            Self::Stopped { tripped, .. } => Some(*tripped),
            Self::Running => None,
        }
    }
    /// The partial answers: the same shape over another field.
    pub fn partial(&self) -> Option<&Vec<u8>> {
        match self {
            Self::Stopped { partial, .. } => Some(partial),
            Self::Running => None,
        }
    }
}
/// A minimal executor: a nested waker type and its impl live inside the body.
pub fn block_on<F: Future>(future: F) -> F::Output {
    struct ParkWaker(std::thread::Thread);
    impl Wake for ParkWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(ParkWaker(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}
/// Canonical decomposition, then composition.
pub fn nfc(text: &str) -> String {
    collect(text, |next| Decompose::<false, _>::new(Compose::new(next)), |stage| stage.into_next())
}
/// Compatibility decomposition, then composition: a const argument apart.
pub fn nfkc(text: &str) -> String {
    collect(text, |next| Decompose::<true, _>::new(Compose::new(next)), |stage| stage.into_next())
}
";
    const DEPENDENCY_PATH: &str = "use std::collections::{BTreeMap, BTreeSet, VecDeque};
/// The shortest dependency path, over predicate symbols.
pub fn shortest_dependency_path<'a>(depends: &'a BTreeMap<String, BTreeSet<String>>, from: &'a str, to: &str) -> Option<Vec<&'a str>> {
    let mut parent: BTreeMap<&str, &str> = BTreeMap::new();
    let mut queue: VecDeque<&str> = VecDeque::from([from]);
    let mut seen: BTreeSet<&str> = BTreeSet::from([from]);
    while let Some(node) = queue.pop_front() {
        if node == to {
            // Walk the parent chain back to `from`, then reverse it.
            let mut path = vec![node];
            let mut cursor = node;
            while cursor != from {
                cursor = parent[cursor];
                path.push(cursor);
            }
            path.reverse();
            return Some(path);
        }
        for next in depends.get(node).into_iter().flatten() {
            if seen.insert(next.as_str()) {
                parent.insert(next.as_str(), node);
                queue.push_back(next.as_str());
            }
        }
    }
    None
}
/// The same executor with the nested waker renamed.
pub fn drive<F: Future>(work: F) -> F::Output {
    struct Park(std::thread::Thread);
    impl Wake for Park {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let handle = Waker::from(Arc::new(Park(std::thread::current())));
    let mut cx = Context::from_waker(&handle);
    let mut work = Box::pin(work);
    loop {
        match work.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}
/// A comment line or a blank one, retyped under other names.
pub fn is_comment(text: &str) -> bool {
    let rest = text.trim();
    rest.is_empty() || rest.starts_with('#')
}
";
    const WRITE_FILE: &str = "use std::path::Path;
mod support;
fn write_file(dir: &Path, name: &str, contents: &str) -> String {
    let path = dir.join(name);
    std::fs::write(&path, contents).expect(\"write fixture\");
    path.to_str().expect(\"utf-8 path\").to_owned()
}
#[test]
fn case() { let _ = write_file(Path::new(\"/tmp\"), \"a\", \"b\"); support::shared(); }
";
    const WRITE_FILE_AGAIN: &str = "use std::path::Path;
mod support;
fn write_fixture(root: &Path, file: &str, text: &str) -> String {
    let target = root.join(file);
    std::fs::write(&target, text).expect(\"write the fixture\");
    target.to_str().expect(\"a UTF-8 path\").to_owned()
}
#[test]
fn other_case() { let _ = write_fixture(Path::new(\"/tmp\"), \"a\", \"b\"); support::shared(); }
";
    const SUPPORT: &str = "/// Shared by every test target that declares it: one source, not two.
pub fn shared() -> u64 {
    let mut total = 0_u64;
    for step in 0..16_u64 {
        total = total.wrapping_mul(31).wrapping_add(step);
    }
    total
}
";
    let files = [
        (
            "crates/delta/Cargo.toml",
            "[package]\nname = \"fixture-delta\"\n",
        ),
        ("crates/delta/src/lib.rs", RULE_PATH),
        ("crates/delta/tests/one.rs", WRITE_FILE),
        ("crates/delta/tests/two.rs", WRITE_FILE_AGAIN),
        ("crates/delta/tests/support/mod.rs", SUPPORT),
        (
            "crates/epsilon/Cargo.toml",
            "[package]\nname = \"fixture-epsilon\"\n",
        ),
        ("crates/epsilon/src/lib.rs", DEPENDENCY_PATH),
    ];
    Memory {
        files: files
            .into_iter()
            .map(|(path, text)| (path.to_owned(), text.to_owned()))
            .collect(),
    }
}

/// The near-miss cases: each seeded copy is one group with its original, and
/// no neighbour joins a group.
fn near_miss_cases() -> Vec<(&'static str, bool)> {
    let workspace = Workspace::walk(&near_miss_tree());
    let groups: Vec<Vec<&str>> = census::isomorphic_groups(&workspace)
        .iter()
        .map(|group| {
            let mut members: Vec<&str> = group
                .members
                .iter()
                .map(|&index| workspace.units[index].symbol.as_str())
                .collect();
            members.sort_unstable();
            members
        })
        .collect();
    let grouped = |symbol: &str| groups.iter().flatten().any(|member| *member == symbol);
    let small = census::isomorphic_groups(&workspace)
        .iter()
        .find(|group| {
            group
                .members
                .iter()
                .any(|&index| workspace.units[index].name == "skipped")
        })
        .map_or(0, |group| group.tokens);
    let findings = ledger::parse("").map_or_else(
        |error| vec![error],
        |ledger| census::check_findings(&workspace, &ledger, &near_miss_tree(), None),
    );
    vec![
        (
            "the near-miss fixture walks cleanly",
            workspace.errors.is_empty(),
        ),
        (
            "two breadth-first searches that differ only in `&next`/`parent[&cursor]` against `next.as_str()`/`parent[cursor]` are one group",
            groups.contains(&vec![
                "fixture_delta::shortest_rule_path",
                "fixture_epsilon::shortest_dependency_path",
            ]),
        ),
        (
            "a helper copied between two tests/ files is one group",
            groups.contains(&vec![
                "fixture_delta::test_crate::one::write_file",
                "fixture_delta::test_crate::two::write_fixture",
            ]),
        ),
        (
            "a copy below the old thirty-token floor is one group",
            groups.contains(&vec![
                "fixture_delta::skipped",
                "fixture_epsilon::is_comment",
            ]) && (MIN_TOKENS..30).contains(&small),
        ),
        (
            "a support module every test target declares is one source, not a group",
            !grouped("fixture_delta::test_crate::one::support::shared")
                && workspace
                    .units
                    .iter()
                    .filter(|unit| unit.name == "shared")
                    .count()
                    == 1,
        ),
        (
            "a small body of the same shape with a different constant is another table",
            !grouped("fixture_delta::skipped_ini"),
        ),
        (
            "two accessors of the same shape over different fields are not grouped",
            !grouped("fixture_delta::Run::tripped") && !grouped("fixture_delta::Run::partial"),
        ),
        (
            "a const generic argument is kept: `::<true, _>` and `::<false, _>` are not grouped",
            !grouped("fixture_delta::nfc") && !grouped("fixture_delta::nfkc"),
        ),
        (
            "a function whose body declares a nested struct and impl is one group with its copy once the nested type is renamed",
            groups.contains(&vec!["fixture_delta::block_on", "fixture_epsilon::drive"]),
        ),
        (
            "--check fails on exactly the four seeded near-miss groups",
            findings.len() == 4
                && findings
                    .iter()
                    .all(|finding| finding.starts_with("isomorphic bodies")),
        ),
    ]
}

/// The `--check` findings over the fixture tree under the fixture job with
/// `edit` applied to its text.
fn job_findings(edit: impl FnOnce(String) -> String) -> Vec<String> {
    let tree = fixture_tree();
    ledger::parse(&edit(FIXTURE_JOB.to_owned())).map_or_else(
        |error| vec![error],
        |ledger| census::check_findings(&Workspace::walk(&tree), &ledger, &tree, None),
    )
}

/// The ledger rows' own checks, which purrdf ran in its Python gate and this
/// census runs in `--check`: each seeded defect is exactly one more finding.
fn ledger_cases() -> Vec<(&'static str, bool)> {
    let baseline = job_findings(|text| text).len();
    let one_more = |edit: &dyn Fn(String) -> String, needle: &str| {
        let findings = job_findings(edit);
        findings.len() == baseline + 1 && findings.iter().any(|finding| finding.contains(needle))
    };
    let unenforced = |text: String| text.replace("enforced = true", "enforced = false");
    let with_variant = |row: &str| {
        let row = row.to_owned();
        move |text: String| format!("{text}{row}")
    };
    vec![
        (
            "an unenforced job's copies are not --check findings, only --census lines",
            {
                // Only the unsanctioned group is left; its three copies are not.
                let findings = job_findings(unenforced);
                findings.len() == 1 && findings[0].starts_with("isomorphic bodies")
            },
        ),
        (
            "an unenforced job with no copies left is STALE",
            job_findings(|text| {
                unenforced(text)
                    .replace("[\"0xcbf29ce484222325\"]", "[]")
                    .replace("[\"table:hex-lower\"]", "[]")
            })
            .iter()
            .any(|finding| finding.contains("no copies are left")),
        ),
        (
            "an entry point that does not resolve fails",
            one_more(
                &|text| {
                    text.replace(
                        "entry_points = []",
                        "entry_points = [\"fixture_alpha::gone\"]",
                    )
                },
                "entry_points `fixture_alpha::gone`",
            ),
        ),
        (
            "a vectors file that does not exist fails, and one that does holds",
            one_more(
                &|text| text.replace("vectors = []", "vectors = [\"fixtures/none.bin\"]"),
                "vectors `fixtures/none.bin`",
            ) && job_findings(|text| {
                text.replace("vectors = []", "vectors = [\"crates/alpha/src/lib.rs\"]")
            })
            .len()
                == baseline,
        ),
        (
            "a bench id that is no bench target fails",
            one_more(
                &|text| text.replace("bench = []", "bench = [\"fixture-alpha:speed\"]"),
                "bench `fixture-alpha:speed`",
            ),
        ),
        (
            "a body fingerprint that matches nothing is STALE",
            one_more(
                &|text| {
                    text.replace(
                        "[\"table:hex-lower\"]",
                        "[\"table:hex-lower\", \"body:0000000000000000\"]",
                    )
                },
                "matches nothing (STALE)",
            ),
        ),
        (
            "an isomorphic variant on an ungrouped symbol is STALE",
            job_findings(with_variant(
                &FIXTURE_VARIANT
                    .replace("DETECTOR", "isomorphic")
                    .replace("fixture_beta::fold", "fixture_beta::count"),
            ))
            .iter()
            .any(|finding| finding.contains("variant `fixture_beta::count`: STALE")),
        ),
        (
            "a variant in the wrong file, or with an undocumented anchor, fails",
            {
                let findings = job_findings(with_variant(
                    &FIXTURE_VARIANT
                        .replace("DETECTOR", "isomorphic")
                        .replace("crates/beta/src/lib.rs", "crates/beta/src/digits.rs")
                        .replace(
                            "anchor = \"fixture_beta::fold\"",
                            "anchor = \"fixture_beta::digits::DIGITS\"",
                        ),
                ));
                findings
                    .iter()
                    .any(|finding| finding.contains("not in crates/beta/src/digits.rs"))
                    && findings
                        .iter()
                        .any(|finding| finding.contains("carries no documentation"))
            },
        ),
        (
            "a forbidden variant that sanctions a live match holds, and one that matches nothing is STALE",
            job_findings(with_variant(
                &FIXTURE_VARIANT.replace("DETECTOR", "forbidden"),
            ))
            .len()
                == baseline - 1
                && job_findings(with_variant(
                    &FIXTURE_VARIANT
                        .replace("DETECTOR", "forbidden")
                        .replace("fixture_beta::fold", "fixture_beta::count"),
                ))
                .iter()
                .any(|finding| finding.contains("variant `fixture_beta::count`: STALE")),
        ),
    ]
}

/// The cross-package include rule: a `#[path]` that leaves its package fails
/// `--check`, and one inside its package does not.
fn path_escape_cases() -> Vec<(&'static str, bool)> {
    let mut escaping = fixture_tree();
    escaping.files.insert(
        "crates/beta/tests/borrow.rs".to_owned(),
        "#[path = \"../../alpha/src/lib.rs\"]\nmod alpha;\n".to_owned(),
    );
    let findings = ledger::parse("").map_or_else(
        |error| vec![error],
        |ledger| census::check_findings(&Workspace::walk(&escaping), &ledger, &escaping, None),
    );
    vec![(
        "a #[path] module that resolves into another package fails --check",
        findings.iter().any(|finding| {
            finding.starts_with("crates/beta/tests/borrow.rs:2:")
                && finding.contains("crates/alpha/src/lib.rs")
        }),
    )]
}

/// A seeded purrdf tree: an FNV-1a under purrdf's own names, a second copy of
/// it in a `bindings/` package, and a pair of bodies that only purrdf repeats.
fn external_tree() -> Memory {
    let fnv = |name: &str| {
        format!(
            "/// FNV-1a.\npub fn {name}(data: &[u8]) -> u64 {{\n    let mut h = 0xcbf29ce484222325_u64;\n    for d in data {{\n        h ^= u64::from(*d);\n        h = h.wrapping_mul(0x100000001b3);\n    }}\n    h\n}}\n"
        )
    };
    let only_purrdf = |name: &str| {
        format!(
            "/// Only purrdf has this twice.\npub fn {name}(text: &str) -> usize {{\n    let mut count = 0;\n    for line in text.lines() {{\n        if line.starts_with(\"@prefix\") {{\n            count += 1;\n        }}\n    }}\n    count\n}}\n"
        )
    };
    let files = [
        (
            "crates/hash/Cargo.toml",
            "[package]\nname = \"purrdf-hash\"\n".to_owned(),
        ),
        (
            "crates/hash/src/lib.rs",
            format!(
                "{}{}{}",
                fnv("fnv"),
                only_purrdf("twice_a"),
                only_purrdf("twice_b")
            ),
        ),
        (
            "bindings/other/Cargo.toml",
            "[package]\nname = \"purrdf-other\"\n".to_owned(),
        ),
        ("bindings/other/src/lib.rs", fnv("fnv_again")),
    ];
    Memory {
        files: files
            .into_iter()
            .map(|(path, text)| (path.to_owned(), text))
            .collect(),
    }
}

/// The cross-workspace cases: a shape shared with purrdf is one cross group, a
/// shape only purrdf repeats is ignored, a purrdf-homed job counts Sparrow's
/// copies and never purrdf's, and an external root that cannot be scanned fails.
fn cross_cases() -> Vec<(&'static str, bool)> {
    let local_tree = fixture_tree();
    let local = Workspace::walk(&local_tree);
    let external = match cross::walk_external(
        &external_tree(),
        cross::Source::Root(PathBuf::from("seeded")),
    ) {
        Ok(external) => external,
        Err(error) => {
            println!("  {error}");
            return vec![("the seeded external tree walks", false)];
        }
    };
    let symbols = |workspace: &Workspace, indexes: &[usize]| -> Vec<String> {
        let mut symbols: Vec<String> = indexes
            .iter()
            .map(|&index| workspace.units[index].symbol.clone())
            .collect();
        symbols.sort();
        symbols
    };
    let groups = cross::cross_groups(&local, &external.workspace);
    let shared = groups.iter().any(|group| {
        symbols(&local, &group.local) == ["fixture_alpha::fnv1a", "fixture_beta::fold"]
            && symbols(&external.workspace, &group.external)
                == ["purrdf_hash::fnv", "purrdf_other::fnv_again"]
    });
    let report = cross::report(&local, &external).join("\n");
    let homed = FIXTURE_JOB.replace(
        "home = \"fixture_alpha::fnv1a\"",
        "home = \"purrdf:purrdf_hash::fnv\"",
    );
    let (with_scan, without_scan, unchecked) = ledger::parse(&homed).map_or_else(
        |error| (vec![error], Vec::new(), Vec::new()),
        |ledger| {
            (
                census::check_findings(&local, &ledger, &local_tree, Some(&external.workspace)),
                census::check_findings(&local, &ledger, &local_tree, None),
                census::unchecked_jobs(&ledger, None)
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
            )
        },
    );
    // purrdf's package renamed to Sparrow's `fixture-alpha`: the home is still
    // purrdf's, so `fixture_alpha::fnv1a` is still a copy.
    let mut homonym = external_tree();
    homonym.files.insert(
        "crates/hash/Cargo.toml".to_owned(),
        "[package]\nname = \"fixture-alpha\"\n".to_owned(),
    );
    let homonym_findings = match (
        ledger::parse(&homed.replace("purrdf_hash::fnv", "fixture_alpha::fnv")),
        cross::walk_external(&homonym, cross::Source::Root(PathBuf::from("homonym"))),
    ) {
        (Ok(ledger), Ok(homonym)) => {
            census::check_findings(&local, &ledger, &local_tree, Some(&homonym.workspace))
                .iter()
                .filter(|finding| finding.starts_with("job `fnv`"))
                .count()
        }
        _ => 0,
    };
    let job_lines: Vec<&String> = with_scan
        .iter()
        .filter(|finding| finding.starts_with("job `fnv`"))
        .collect();
    let scratch =
        std::env::temp_dir().join(format!("helper-census-self-test-{}", std::process::id()));
    let missing = scratch.join("no-such-root");
    let archived = cross::materialise(&missing, &"0".repeat(40), &scratch);
    let left_nothing = !scratch.join(format!("purrdf-{}", "0".repeat(40))).exists();
    let _ = std::fs::remove_dir_all(&scratch);
    vec![
        (
            "a body shared with the seeded purrdf tree is one cross group, both sides listed, bindings/ included",
            shared,
        ),
        (
            "the cross report names the source and both sides' locations",
            report.starts_with("cross census against purrdf tree seeded")
                && report.contains("crates/alpha/src/lib.rs:")
                && report.contains("bindings/other/src/lib.rs:"),
        ),
        (
            "a group wholly inside Sparrow is not a cross group",
            cross::cross_groups(&local, &Workspace::default()).is_empty(),
        ),
        (
            "a purrdf package that shares a Sparrow package's name is still not Sparrow's home",
            homonym_findings == 4,
        ),
        (
            "a group wholly inside purrdf is not a cross group and is not reported",
            !groups.iter().any(|group| {
                symbols(&external.workspace, &group.external)
                    .iter()
                    .any(|symbol| symbol.starts_with("purrdf_hash::twice"))
            }) && !report.contains("twice_a"),
        ),
        (
            "a constant held on both sides is shared, and none held only in purrdf is",
            cross::cross_constants(&local, &external.workspace)
                .iter()
                .any(|constant| constant.value == 0xcbf2_9ce4_8422_2325)
                && cross::cross_constants(&local, &Workspace::default()).is_empty(),
        ),
        (
            "a purrdf-homed job makes every Sparrow match a copy, and no purrdf copy is a finding",
            job_lines.len() == 4
                && job_lines
                    .iter()
                    .any(|finding| finding.contains("fixture_alpha::fnv1a"))
                && !with_scan
                    .iter()
                    .any(|finding| finding.contains("purrdf_other")),
        ),
        (
            "without the purrdf scan a purrdf-homed job is reported unchecked, not passed",
            !without_scan
                .iter()
                .any(|finding| finding.starts_with("job `fnv`"))
                && unchecked == ["fnv"],
        ),
        (
            "an external tree with no package fails loudly instead of scanning nothing",
            cross::walk_external(
                &Memory::default(),
                cross::Source::Root(PathBuf::from("empty")),
            )
            .is_err_and(|error| error.contains("no package")),
        ),
        (
            "a missing --external-root fails loudly",
            cross::load(&missing, None, Some(&missing)).is_err(),
        ),
        (
            "a repository that cannot be archived fails loudly and leaves no cached tree",
            archived.is_err_and(|error| {
                error.contains("cannot materialise") && error.contains("git archive")
            }) && left_nothing,
        ),
        (
            "a Sparrow root with no compatibility registry fails loudly",
            cross::load(&missing, None, None).is_err(),
        ),
    ]
}

/// Forwarders that reach their own type are not shim groups; a free forwarder
/// spelled twice still is, so the exemption cannot swallow the rule.
fn own_type_cases() -> Vec<(&'static str, bool)> {
    const SOURCE: &str = "/// A.\npub struct A;\n/// B.\npub struct B;\n/// E.\npub struct E;\n\
impl From<E> for A { fn from(error: E) -> Self { Self::wrap(error, 7, \"io\") } }\n\
impl From<E> for B { fn from(error: E) -> Self { Self::wrap(error, 7, \"io\") } }\n\
impl A { /// New.\npub fn new() -> Self { Self::build(Vec::new(), 3, \"x\") } }\n\
impl B { /// New.\npub fn new() -> Self { Self::build(Vec::new(), 3, \"x\") } }\n\
/// Forwards.\npub fn first(value: u8) -> u8 { crate::helper(value, 7, \"same\") }\n\
/// Forwards again.\npub fn second(other: u8) -> u8 { crate::helper(other, 7, \"same\") }\n";
    let tree = Memory {
        files: [
            (
                "crates/own/Cargo.toml".to_owned(),
                "[package]\nname = \"fixture-own\"\n".to_owned(),
            ),
            ("crates/own/src/lib.rs".to_owned(), SOURCE.to_owned()),
        ]
        .into_iter()
        .collect(),
    };
    let workspace = Workspace::walk(&tree);
    let groups: Vec<Vec<&str>> = census::shim_groups(&workspace)
        .iter()
        .map(|group| {
            let mut members: Vec<&str> = group
                .members
                .iter()
                .map(|&index| workspace.units[index].symbol.as_str())
                .collect();
            members.sort_unstable();
            members
        })
        .collect();
    let forwarders = workspace
        .units
        .iter()
        .filter(|unit| {
            unit.print
                .as_ref()
                .is_some_and(|print| print.shim.is_some())
        })
        .count();
    vec![
        (
            "the own-type fixture walks cleanly and every body is a forwarder",
            workspace.errors.is_empty() && forwarders == 6,
        ),
        (
            "two trait-impl forwarders and two `Self::` constructors are not shim groups",
            !groups
                .iter()
                .flatten()
                .any(|symbol| symbol.ends_with("::from") || symbol.ends_with("::new")),
        ),
        (
            "two free forwarders to one target with one fixed argument list are still a shim group",
            groups == [vec!["fixture_own::first", "fixture_own::second"]],
        ),
        (
            "an exempt forwarder is not body-grouped either",
            census::isomorphic_groups(&workspace).is_empty(),
        ),
    ]
}

/// One seeded case: its name and whether it held.
fn self_test_cases() -> Vec<(&'static str, bool)> {
    let tree: &dyn Tree = &fixture_tree();
    let workspace = Workspace::walk(tree);
    let Ok(plain) = ledger::parse(FIXTURE_JOB) else {
        return vec![("the fixture ledger parses", false)];
    };
    let with_variant = |detector: &str| {
        ledger::parse(&format!(
            "{FIXTURE_JOB}{}",
            FIXTURE_VARIANT.replace("DETECTOR", detector)
        ))
    };
    let (Ok(isomorphic), Ok(forbidden)) = (with_variant("isomorphic"), with_variant("forbidden"))
    else {
        return vec![("the fixture ledger with a variant parses", false)];
    };
    let groups = census::isomorphic_groups(&workspace);
    let group_symbols: Vec<Vec<&str>> = groups
        .iter()
        .map(|group| {
            group
                .members
                .iter()
                .map(|&index| workspace.units[index].symbol.as_str())
                .collect()
        })
        .collect();
    let Some(job) = plain.job("fnv") else {
        return vec![("the fixture job exists", false)];
    };
    let home_package = census::home(job, &workspace).map(|home| home.package);
    let found = home_package
        .as_ref()
        .map_or_default(|package| census::matches(job, &workspace, &workspace, package));
    let matched: Vec<(&str, bool)> = found
        .iter()
        .map(|found| (found.symbol.as_str(), found.in_home))
        .collect();
    let copies_with_forbidden_variant = forbidden.job("fnv").map_or(usize::MAX, |job| {
        census::copies(&census::matches(
            job,
            &workspace,
            &workspace,
            "fixture-alpha",
        ))
    });
    let mut cases = vec![
        (
            "the walk reads every fixture file",
            workspace.errors.is_empty(),
        ),
        (
            "a renamed, re-spelt copy of a body is one isomorphic group with its original",
            group_symbols == [vec!["fixture_alpha::fnv1a", "fixture_beta::fold"]],
        ),
        (
            "a body of the same size with different control flow is not grouped",
            !group_symbols
                .iter()
                .flatten()
                .any(|symbol| *symbol == "fixture_beta::count"),
        ),
        (
            "test-only code is never walked, and a tests/ file is walked as a test crate of its own",
            !workspace.units.iter().any(|unit| unit.name == "fnv_again")
                && workspace.units.iter().any(|unit| {
                    unit.symbol == "fixture_beta::test_crate::copy::fnv1a" && !unit.shipping
                }),
        ),
        (
            "the home resolves to its package",
            home_package.as_deref() == Ok("fixture-alpha"),
        ),
        (
            "a decimal spelling of a forbidden hex constant is a copy outside the home",
            matched.contains(&("fixture_beta::fold", false))
                && matched.contains(&("fixture_alpha::fnv1a", true)),
        ),
        (
            "a hex-digit table is a forbidden fingerprint and a near-miss table is not",
            matched.contains(&("fixture_beta::digits::DIGITS", false))
                && !matched
                    .iter()
                    .any(|(symbol, _)| *symbol == "fixture_beta::digits::NOT_DIGITS"),
        ),
        (
            "a forbidden constant in a tests/ file is a copy too",
            matched.contains(&("fixture_beta::test_crate::copy::fnv1a", false)),
        ),
        (
            "--count counts all three copies",
            census::copies(&found) == 3,
        ),
        (
            "--check reports the unsanctioned group and all three copies",
            census::check_findings(&workspace, &plain, tree, None).len() == 4,
        ),
        (
            "an isomorphic variant row sanctions its group",
            census::check_findings(&workspace, &isomorphic, tree, None).len() == 3,
        ),
        (
            "a forbidden variant row removes its copy from the count",
            copies_with_forbidden_variant == 2,
        ),
    ];
    cases.extend(distinct_cases());
    cases.extend(near_miss_cases());
    cases.extend(ledger_cases());
    cases.extend(path_escape_cases());
    cases.extend(cross_cases());
    cases.extend(own_type_cases());
    cases
}

fn self_test() -> ExitCode {
    let mut failed = 0;
    for (name, held) in self_test_cases() {
        if held {
            println!("PASS: {name}");
        } else {
            println!("FAIL: {name}");
            failed += 1;
        }
    }
    if failed == 0 {
        println!("OK: helper-census self-test");
        ExitCode::SUCCESS
    } else {
        eprintln!("helper-census self-test: {failed} case(s) failed");
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use super::{Mode, parse_arguments, self_test_cases};

    fn args(list: &[&str]) -> impl Iterator<Item = String> {
        list.iter()
            .map(|arg| (*arg).to_owned())
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn every_seeded_case_holds() {
        for (name, held) in self_test_cases() {
            assert!(held, "{name}");
        }
    }

    #[test]
    fn baseline_takes_an_optional_path() {
        assert_eq!(
            parse_arguments(args(&["--baseline"])).expect("bare").mode,
            Mode::Baseline(None)
        );
        assert_eq!(
            parse_arguments(args(&["--baseline", "out.json"]))
                .expect("with a path")
                .mode,
            Mode::Baseline(Some("out.json".into()))
        );
        let followed =
            parse_arguments(args(&["--baseline", "--root", "."])).expect("followed by a flag");
        assert_eq!(followed.mode, Mode::Baseline(None));
    }

    #[test]
    fn two_modes_or_none_are_refused() {
        assert!(parse_arguments(args(&["--check", "--index"])).is_err());
        assert!(parse_arguments(args(&[])).is_err());
        assert!(parse_arguments(args(&["--count"])).is_err());
        assert_eq!(
            parse_arguments(args(&["--count", "fnv"]))
                .expect("one mode")
                .mode,
            Mode::Count("fnv".to_owned())
        );
    }

    /// The modes left behind in purrdf are unknown arguments here, not silently
    /// accepted no-ops.
    #[test]
    fn the_purrdf_only_modes_are_unknown() {
        for mode in [
            "--no-features",
            "--python-binding-tests",
            "--non-rust-ratchet",
        ] {
            let error = parse_arguments(args(&[mode])).expect_err(mode);
            assert!(error.contains("unknown argument"), "{mode}: {error}");
        }
    }

    #[test]
    fn the_cross_options_belong_to_a_purrdf_scan() {
        let report = parse_arguments(args(&["--cross-report"])).expect("a cross mode");
        assert_eq!(report.external, Some(super::ExternalOptions::default()));
        let check = parse_arguments(args(&["--cross", "--check", "--purrdf-repo", "/r"]))
            .expect("--cross --check");
        assert_eq!(
            check.external.and_then(|options| options.repository),
            Some("/r".into())
        );
        assert_eq!(
            parse_arguments(args(&["--cross-baseline", "--external-root", "/t"]))
                .expect("a bare cross baseline")
                .mode,
            Mode::CrossBaseline(None)
        );
        assert!(
            parse_arguments(args(&["--check"]))
                .expect("plain")
                .external
                .is_none()
        );
        for refused in [
            &["--check", "--purrdf-repo", "/r"][..],
            &["--check", "--external-root", "/t"],
            &["--cross", "--baseline"],
            &["--cross", "--self-test"],
            &[
                "--cross-report",
                "--purrdf-repo",
                "/r",
                "--external-root",
                "/t",
            ],
        ] {
            assert!(parse_arguments(args(refused)).is_err(), "{refused:?}");
        }
    }

    /// With no `--root`, the census reads the workspace it was built from.
    #[test]
    fn the_default_root_is_this_workspace() {
        let root = parse_arguments(args(&["--check"])).expect("a mode").root;
        assert!(
            root.join("crates/sparrow-helper-census/Cargo.toml")
                .is_file()
        );
        assert!(root.join("helpers-ledger.toml").is_file());
    }
}
