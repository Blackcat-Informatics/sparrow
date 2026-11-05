# Delivery review: D74 Markdown profile advance (#28, purrdf-head-probe, uncommitted)

## Scope

- Judged: the uncommitted `git diff` in /home/paudley/Active/sparrow/.worktrees/purrdf-head. It is
  byte-identical to `.stage/purrdf-head/d74-full.diff` (`cmp` reports no difference). It touches
  `crates/sparrow-core/src/storage.rs`, `storage/rdf_index.rs`, `tests/storage.rs` and
  `deploy/RECOVERY.md`. The c6d670c pin move (02d062e) and the main merge (b54cb08) were reviewed
  before. I read them here only where they interact with this change.
- Contract: issue #28 and its two comments (sizing, and D74), and `plan.md` with its four
  `## User rulings`. D74 replaces work item 2 (full re-projection) with this rule: advance
  `rdf_profile` from the pre-H value in one synced write only when the store holds no Markdown
  projection; otherwise refuse `UNSUPPORTED`, name the count and write nothing. It also requires
  the O1 identity-pinning test, covering the advance, the refusal and the advance after
  retirement.
- Not in this change: the H restart itself, the pre-upgrade backup and the verify-backup run under
  the H image. The plan lists them as later acceptance steps.
- I did not touch the live store, its backups or sparrow.service, and I made no change to the
  worktree. The only file I wrote is this report.

## Evidence

- Focused test: `cargo test --release -p sparrow-core --test storage -- persisted_identities
  rdf_profile_identity`, log `/tmp/sparrow-d74-review-test.log`, `EXIT=0`: 2 passed, 0 failed,
  210 filtered out. It ran
  `persisted_identities_are_pinned_and_the_markdown_contract_move_migrates` (new) and
  `rdf_profile_identity_is_required_and_drift_refuses_reopen` (existing). This is release-profile
  runtime evidence for those two tests only. I did not run the Clippy gate or the commit hook.
- `prose-check` on the added RECOVERY.md lines, the two new doc comments, the test's doc comment
  and the new error message: exit 0, no findings.
- No `validation.md` exists in `.stage/purrdf-head/`.

### D74 conformance (read in `storage.rs:1383-1398` and `rdf_index.rs:54-89`)

- Equal profile: the first arm passes, as before.
- Stored value equals `PREVIOUS_PROFILE`: `advance_profile` counts keys under
  `rdf:state:rdf-projection:markdown:`. A count above zero returns `UNSUPPORTED` with
  "holds {N} Markdown projection(s)" before any write. A count of zero writes `rdf_profile` with
  `put_opt` and `set_sync(true)`. That is one synced single-key write, the same durability
  pattern as the new-store batch at `storage.rs:1350-1352`.
- Any other stored value: `advance_profile` returns `Ok(false)`, and the existing generic
  `UNSUPPORTED` arm refuses unchanged. A missing value still returns `STORAGE_FAILURE`. The
  existing drift test covers both cases and passes.
- The count and the write cannot race: `Store::open` holds the RocksDB lock, and nothing else
  writes at that point. The default column family has no prefix extractor (`table_options.rs`
  has none), so the forward iteration with `starts_with` is exact. Iterator errors propagate
  through `?`.
- The rule matches D74 exactly, and the change adds nothing beyond it (no re-projection, no
  patch tables).

### Is counting `rdf:state` keys sufficient?

- Every Markdown record reaches `rdf_projection_id = rdf-projection:markdown:<contract>:<digest>`
  (`ingest.rs:167-175`). Its `rdf:state` row is written whether the projection succeeds
  (`state: complete`) or fails with `RESOURCE_LIMIT`/`PROJECTION_FAILURE` (`state: failed`)
  (`ingest.rs:180-217, 361-362`). Failed and oversized Markdown projections are therefore
  counted, which errs toward refusal.
- Retirement (`retire.rs:44-81, 131-158`) removes the record's own metadata graph, which embeds
  the content graph IRI and `projectionState`. It decrements `rdf:projection-ref`, and at zero it
  deletes `rdf:state:<id>` together with the projection graph rows (`rdf:graph:` under that id,
  when the state was complete). So, given the refcount invariant, zero `rdf:state` Markdown keys
  means no live record metadata names an old-law graph and no old-law projection graph remains.
  Evicted records lose `rdf_projection_id` (`mem::take`) before they are rewritten.
- Pinned records and records held behind an archive watermark keep their `rdf:state` and
  therefore refuse. That fails closed. "Expires 06:16Z" in #28 means the record becomes
  eligible; it is retired only when an expiry sweep runs, and only if no archive lag holds it.
  The plan's step of running verify-backup with the H image on a nightly taken after retirement
  catches that case before the restart.
- JSON projections (`rdf-projection:json:`) share the `rdf:state:` namespace but not this prefix.
  The test's JSON ingest before the first advance confirms they are not counted.
- No other storage module derives anything from the Markdown contract id. A search for
  `contract_id` and `markdown_profile` finds only `profile_identity` and `ingest.rs`.

### Query-only opens and ordering

- `--query-only` is a server-side write gate. `Store::open` and its startup upgrades still run,
  so the H image opening a backup copy through `verify-backup.sh --image` advances the copy. The
  script documents that "a newer image may upgrade the disposable copy". A copy that still holds
  the Markdown record refuses with the count and no write. Under its recorded image, a copy opens
  unchanged.
- Ordering: the advance runs after the format, `text_analyzer` and RDF counter checks, and before
  `upgrade_reverse`, `recordless::upgrade`, the three RDF upgrades, `archive::open` and the
  producer registry check. Each of those can still refuse after the advance is durable (see O1).

### Test

- It pins the fresh store's `rdf_profile` (CURRENT) and `text_analyzer`, which settles review O1.
  Then it covers the advance with a JSON projection present and confirms the advance persisted
  after a Markdown ingest. It covers the refusal: the code, the "holds 1 Markdown projection"
  text, and an unchanged `PREVIOUS` value read straight from the catalog. Last, it covers the
  advance after the Markdown record is retired by `expire`.
- It is deterministic: every timestamp is explicit, and simulating a pre-H store by rewriting
  `rdf_profile` through `catalog_db` is an appropriate fake. The duplicated `PREVIOUS` literal is
  a deliberate pin, not a mirror. The `rdf_inspect(...).is_err()` check is weak, but the final
  advance proves the count reached zero.
- Not verified here: that `1909d176…` equals the identity an a47940e image stores. Building that
  image or reading the live store was out of bounds. A wrong constant fails closed with the
  generic refusal, and the plan's verify-backup run under the H image is the check that decides.
  The `text_analyzer` pin reads lexicons from the sibling PurRDF checkout
  (`sparrow-test-support::text_lexicon_dir`), not from the pinned revision. That is the fixture's
  existing design.

## Findings

### R1. REQUIRED: the runbook omits the one-way direction of the advance, and the rollback guard does not cover it

The H image keeps `SCHEMA_VERSION = 67` (`sparrow-client/src/lib.rs:69`), so it ships as
`sparrowd-schema67-REV`, the same schema number as the pre-H image it replaces.
`refuse_schema_downgrade` and `require_pre_upgrade_backup` (`deploy/lib.sh:207-240`) compare only
those numbers. Once the H image has advanced the live store, `sudo deploy/restart.sh --rollback`
passes the guard and starts the pre-H image, which refuses with the generic
`UNSUPPORTED: RDF profile changed; explicit projection/index migration required`. The service is
down until someone swaps forward again. `restart.sh` also does not demand the named pre-upgrade
backup, because the schema number does not rise. D74 requires that backup, so the operator has to
take it by hand.

The new RECOVERY.md text covers only the forward refusal. "Refused open" offers no recovery for
the generic message in this situation. The "Rollback" limit still implies that the schema guard
covers every one-way store change ("an image cannot undo a store-format upgrade its successor
made. `start.sh` and `restart.sh` therefore refuse ..."). After this change that no longer holds.

Required correction (documentation only): in "Refused open" and/or "Rollback", say that once an
image built after PurRDF fb02dd38b has opened the store, an image built before it refuses with the
generic RDF-profile message. Say that the schema guard does not catch this because both images
are schema 67. Name the recovery: swap forward again (`--rollback` once more), or restore the
named pre-upgrade backup, which starts its own image. Also say that the H restart takes
`deploy/pre-upgrade-backup.sh` by hand, because `restart.sh` will not require it.

### O1. OPTIONAL: a later refusal in the same open keeps the advance

`advance_profile` writes before the upgrade steps, `archive::open` and the producer registry
check. If one of those refuses, the store is already advanced, and the pre-H image will refuse it
too. In that case "a refused open changes nothing" is not quite true. On the live store those
steps are no-ops at schema 67, so the risk is small. The upgrade steps already write early by
existing design. Moving the advance after the last refusal (just before `flush_wal`), or noting it
in the R1 text, would close the gap.

### O2. OPTIONAL: RECOVERY.md wording

"opens only under an image built before it, such as the image the backup carries" reads as
backup guidance inside a section about the live unit. For a live store that refuses with the
Markdown count, the direct recovery is the previous pair (`restart.sh --rollback`) until the
record retires. Stating that, plus "nothing was written", would serve the operator better.

VERDICT: BLOCKED

## Recheck (2026-11-05)

### Scope

Only the changed points. The uncommitted diff is byte-identical to
`.stage/purrdf-head/d74-full-r2.diff`. The test file diff is unchanged from the first round.
`storage.rs`, `rdf_index.rs` and `deploy/RECOVERY.md` changed. The rest of the first review stands.

### Evidence

- I reran `cargo test --release -p sparrow-core --test storage -- persisted_identities
  rdf_profile_identity` myself. Log: `/tmp/sparrow-d74-review-test-r2.log`, `EXIT=0`, 2 passed,
  0 failed, 210 filtered out. The run waited on the cargo package cache lock held by another
  process and then completed.
- `prose-check deploy/RECOVERY.md`: exit 0.

### Changed points

- R1 (closed). "Rollback" now has a "Limit without a schema change" bullet. It says the
  fb02dd38b move keeps schema 67 and advances the profile at the first open. It says an older
  image then refuses with `RDF profile changed` and that the schema check does not catch this,
  so `--rollback` stops the service. It gives two recoveries: `--rollback` again to swap forward,
  or restoring the named pre-upgrade backup. It also says `restart.sh` does not require that
  backup when the schema stays the same, so take it by hand with `deploy/pre-upgrade-backup.sh`.
  "Refused open" now sends the plain profile refusal on a pre-fb02dd38b image to "Rollback". All
  of this matches `deploy/lib.sh:207-240` and `restart.sh`. The swap forward runs through the
  existing `refuse_start_limit` gate, which the "Unit start-limit hit" section already covers.
- O1 (closed). `previous_profile_advances` now only decides: it either refuses with the count or
  returns true, and it never writes. `Store::open` writes `rdf_profile` after the upgrades,
  `archive::open` and the producer registry check, and immediately before `flush_wal(true)`. The
  `db.put` uses default write options with the WAL on, and the synced WAL flush makes it durable.
  That still meets D74's "one synced write". A refusal from any check before that point now leaves
  the profile untouched. The refusal path still writes nothing, and the test confirms it.
- O2 (closed). For the Markdown count, "Refused open" now says to return to the previous image
  with `sudo deploy/restart.sh --rollback` until those records retire.

### Findings

- R1, O1 and O2: closed.
- O3. OPTIONAL. The doc comment on `previous_profile_advances` says the caller writes the profile
  "once every other startup check has passed". Steps still run after the write and `flush_wal`,
  and any of them can refuse: `manifest_retire::features`, the pressure and recompression state
  loads, `source_context::rebuild` and the three stream registries. The comment in `storage.rs`
  ("After the upgrades and fences") is accurate. Rewording the `rdf_index.rs` comment the same
  way would make it exact. Those later steps only load or validate existing state, and they are
  no-ops for a healthy schema 67 store. The contract does not need this change.

VERDICT: PASS
