# Delivery review: PurRDF group A head move (purrdf-head-probe, 02d062e)

## Scope

- Diff judged: `origin/main...HEAD` (merge base 6737fac). It holds 98b6c3b (PurRDF a47940e to
  23a2b71, plus the API adaptations) and the merge 02d062e. The merge brings in main (#24 and #25)
  and moves all five runtime pins and the census reference to c6d670c762842541075bb28c424265ce074d4699.
- Contract: `plan.md` Behavior, Exclusions ("No behavior changes"), Acceptance (one purrdf-core and
  one wasm-bindgen; `make check` passes; "The live store must open unchanged under the new image
  ... or a change is found and reported before the restart"), and the two 2026-11-04 rulings.
- PurRDF ranges read in /home/paudley/Active/purrdf: a47940e..c6d670c (972 commits) and
  23a2b71..c6d670c. Both bases are ancestors of c6d670c.
- I did not open, copy or start anything against /SPARROW/store, its backups or sparrow.service.
  No sparrowd ran. The worktree was not modified (`git status --porcelain` is empty).

## Evidence

- Commit hook log `/tmp/sparrow-purrdf-commit.log`: fmt, release Clippy with `-D warnings`, release
  tests (22+33+211+50+5+25+6+78+1+1 = 432 passed, 0 failed), census self-test OK, and
  `--cross --check` at c6d670c: "OK: 311 ledger jobs and 1 [[distinct]] rows hold". The hook
  then created commit 02d062e.
- `cargo tree -d --locked --offline -e normal,build,dev` exits 0 and lists no purrdf or wasm-bindgen
  duplicate. Cargo.lock has one `purrdf-core` and one `wasm-bindgen` (0.2.129), and all 35 PurRDF
  packages resolve to c6d670c. The duplicates it does list (getrandom 0.3/0.4, shlex 1/2,
  winnow 0.7/1.0, syn 2/3) are the same on origin/main.
- Census: `registry/compatibility.toml` supplier `git_revision` = c6d670c, `recorded = "2026-11-04"`,
  with events for 23a2b71 (2026-11-01) and c6d670c (2026-11-04). `prose-check` on both events and
  the 02d062e message: exit 0, no findings.
- API adaptations (`&QueryOptions`, `FormatError::to_string`, fallible `datasets_isomorphic` with
  an explicit `CanonicalizationControl`) are mechanical, and the shipped callers compile under
  the release gate.
- What Sparrow checks at open (`crates/sparrow-core/src/storage.rs:1302-1397`): the stored
  `text_analyzer` must equal `hex(Analyzer::fingerprint())`, and the stored `rdf_profile` must
  equal `rdf_index::profile_identity()`. Either mismatch returns `UNSUPPORTED` before any upgrade
  step runs. `profile_identity()` (`rdf_index.rs:41-50`) is the BLAKE3 hash of the framed values
  of a Sparrow law string, `json_profile().identity()`,
  `markdown_profile().contract_id()` and the Sparrow bounds. The Sparrow-side inputs did not change
  since the deployed schema 65 (the last commit touching `profile_identity` is 897d5b8).
- Identity inputs under c6d670c, compared with a47940e:
  - Text analyzer: survives. The `AnalyzerProfile::fingerprint` and `Analyzer::fingerprint` law
    constants did not change (a diff search for ANALYZER_PROFILE_ID, the ORDERED, LEXICAL,
    BOUNDING and BASELINE laws, the domains and PROFILE_ID returns nothing). `TEXT_DATA_DIGEST`
    is byte-identical. The case-fold tables moved from `purrdf-text` to
    `purrdf-lex/src/unicode/fold_tables.rs`; all four tables (FOLD_INDEX, FOLD_BLOCKS,
    FOLD_RECORDS, FOLD_CHARS) have identical text, and `fold_scalar` has the same body. There is no
    diff in `crates/text/lexicons`, so `bin/lexicons-c6d670c…` holds the same five CBOR artifacts
    as the deployed bundle. The remaining `purrdf-text` changes I read in analysis, segment,
    dictionary and phonetic code are structured-error refactors.
  - JSON profile identity: survives. `crates/json/src/profile.rs` is unchanged. `frame_le`, BLAKE3,
    hex and `ContentDigest::of` (SHA-256) keep the same output; their diffs add streaming helpers
    and structured errors only.
  - Canonical term JSON (`ProjectionTerm`, the RDF dictionary row codec): unchanged. c6d670c only
    adds `CANONICAL_TERM_JSON_LAW_ID` and a frozen-bytes test.
  - `purrdf_lex::json` read/record/value (catalog rows): structured-error additions only.
  - **Markdown profile identity: changes.** See R1.
- Not verified: I did not review SPARQL evaluator semantics across all 972 commits. The 432 release
  tests run Sparrow's own queries and pass. No stage `validation.md` exists, and nothing records an
  identity comparison at c6d670c. The 2026-11-01 census event claims equal identities for 23a2b71
  only. That claim is consistent with this analysis, because the Markdown change landed later.

## Findings

### R1. REQUIRED: the move changes the stored RDF profile identity, so the live store will refuse to open

PurRDF fb02dd38b ("feat(markdown): expose bounded native source syntax", 2026-11-03) is in
c6d670c and not in 23a2b71. It rewrites `purrdf_markdown::Profile::chunking_bytes`
(`crates/markdown/src/profile.rs:610-641`). The commit adds
`native Markdown dialect revision {DIALECT_REVISION}` (= 2) and changes the section, paragraph,
indentation, container and reference-definition clauses. `contract_id()` is
`derive_chunking_contract_id(&self.emission_bytes())`, and `emission_bytes` starts with
`chunking_bytes`, so the id changes for every profile. PurRDF's own golden fixture confirms this:
`crates/markdown/tests/fixtures/field-guide.nt` changes the `sliceProfile` for the same profile
from `…:d4053c9d35631e2d…` to `…:21bad1db0c0f1435…`, and the section/unit IRIs change with it.

Sparrow frames `markdown_profile().contract_id()` into `rdf_index::profile_identity()`. Under the new
image, `Store::open` on the live store (built at a47940e) reaches the following arm and fails,
so the combined restart will not bring sparrowd up:

```rust
Some(_) => return Err(Error::new("UNSUPPORTED",
    "RDF profile changed; explicit projection/index migration required")),
```

The refusal fails closed before any upgrade step, and RECOVERY.md documents it as reversible.
Stored data is not reinterpreted silently. But the law change goes beyond the identity value.
Stored Markdown projection state is keyed `rdf-projection:markdown:<contract_id>:<digest>`
(`ingest.rs:170-174`), and the stored graphs carry old-law unit and section IRIs and `sliceProfile`
literals. Rewriting the stored `rdf_profile` to get past the refusal would therefore mix two laws
in one store. The change also falls under the plan's "No behavior changes" exclusion, because
Markdown slicing semantics change for every new ingest.

Acceptance 3 required this change to be "found and reported before the restart". It was not. The
02d062e message and the c6d670c census event do not mention it, and the hook cannot catch it,
because every test opens a fresh store.

Required correction: report this to coordination before the combined restart, and do not ship
this image to the live store until the change is resolved. The ruling allows a fix forward only
("never by pinning back"), so the fix belongs to Sparrow, which owns storage migration. The fix
would be an explicit migration that re-projects stored Markdown records under the dialect-2
law and advances `rdf_profile` atomically, with execution evidence against a fresh store and a
query-only restored backup. That adds behavior the plan excludes, so it needs a scope ruling.
Record that ruling under `## User rulings`.

### O1. OPTIONAL: pin the deployed identities in a test

No test compares `status()["rdf_profile"]` or `["text_analyzer"]` with a recorded value, which
is why a green 432-test gate missed R1. A single test asserting the two hex identities (updated
deliberately alongside any migration) would protect the invariant "a dependency move cannot
change persisted identities unnoticed".

### O2. OPTIONAL: the c6d670c pin move exists only in the merge commit

The five pins and the census reference move to c6d670c inside 02d062e, which matches neither parent
(98b6c3b pins 23a2b71 and main pins a47940e). The ghprsq structured squash will flatten this. Until
then, a reviewer has to know that this merge carries substantive content.

VERDICT: BLOCKED
