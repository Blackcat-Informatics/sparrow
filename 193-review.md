Reviewer model: claude-opus-5-5 (opus)

# Delivery review: PR #194 (issue #193), head 54d2a64

## Scope

- Static review of `git diff main...54d2a64` (main 4acd306; 17 files, +254/-636) in
  `/home/paudley/Active/sparrow/.worktrees/193-drop-the-event-fallback-rdf-backfill`.
  The branch has one commit, 54d2a64, and the worktree is clean.
- Issue body: `/home/paudley/Active/sparrow/.stage/fallback-issue.md`. PR body:
  `/home/paudley/Active/sparrow/.stage/193-pr-body.md` (`gh` cannot reach the Forgejo remote,
  so this file is the PR text I reviewed).
- I ran no cargo, build or daemon command and did not touch `/SPARROW/store`. The only commands
  were git reads, greps inside the worktree, reads of the `.stage/193-*.log` files, and
  `prose-check` on the changed docs, the PR body and the commit message.

## Evidence

- Gate logs. `193-commit5.log` ends `EXIT=0`: fmt, release clippy `-D warnings`, release
  workspace tests (storage: 247 passed, including
  `receipt_time_fallback_facts_are_derived_not_stored_and_a_pending_marker_store_opens`), and
  the census self-test plus `--cross --check`. `193-commit4.log` ends `EXIT=1`:
  `copy_daemons_claim_nothing_query_only_refuses_writes_and_no_intake_migrates` failed at
  `rpc.rs:4672` (left 4, right 5). The timing of these runs does not match the PR's evidence
  claim; see R4.
- prose-check: the PR body and commit message pass. On the docs it reports only `UInt64` and
  `UInt32` joins in `docs/interfaces.md` at lines 70, 1364, 1518, 1520, 1577, 1661, 2618, 2623
  and 3541. All of them predate this PR and none is in a changed hunk.
- No changes to `AGENTS.md` or `CLAUDE.md`.

### Check 1: opening a live schema-71 store with the migration pending

- `migration::fence`: a schema-71 format already has the event-fallback family, so the
  `has_event_fallback` branch is skipped. A pre-69 format still gets
  `enable_event_fallback` (a format put only). The other fences are unchanged. The new code
  writes no checkpoint and does no rewrite.
- `Positions::read`, `migration::report`, `Writer` completion and `migrate_step` no longer read
  `event-fallback-rdf-upgrade`. `Counter::ALL` no longer names `done:event-fallback-rdf`.
  Production code reads `store-counter:*` rows only by name (`values`, `stored`). Writes are
  owner-only through `Changes::observe`; nothing writes the retired row and nothing scans the
  prefix, so the retired checkpoint and counter row are inert. The live store opens with no
  rewrite and no format bump.
- Pre-schema-69 writers stay fenced through the unchanged writer family. The previous
  schema-71 image (main/c82bfa4) is not fenced from a store that still carries the
  checkpoint. That follows the issue's "71-to-71 switch", but it makes rollback lossy (see O1)
  and contradicts one docs sentence (see R3).

### Check 2: duplicate or conflicting derived facts

- `StoredRdfView::fallback_pending` reads the record graph's manifest rows and skips
  derivation when `carries_event_time` finds `eventTime` or `eventTimeFallback` on the subject.
  This is the same law main used for graphs past the frontier. Inspection shows no doubling
  path.
- No test now exercises a graph that stores the facts. See R1.

### Check 3: `eventFallbackPending` and its consumers

- The wire field and the CLI/MCP key `event_fallback_pending` are unchanged
  (`rpc/sparql.rs:532`, `agent/sparql.rs:801`). The capnp comment, `RdfScopeCoverage` docs and
  the interfaces table now define it as a permanent count of derived graphs. `coverage.complete`
  does not depend on it.
- Test expectations moved from 0 to 1, or to 300, in `maintenance.rs`, `rpc.rs` and
  `storage.rs`. Deploy scripts (`verify-backup.sh`, `user-unit-upgrade.sh`, `sparrow-backup`)
  iterate `migrations` generically. None counts five or names `event-fallback-rdf`.

### Check 4: leftovers

- No dead code remains: the gate passed clippy with `-D warnings`.
- The ledger job is removed. `Follows::EventFallbackRdf` and the `Positions.fallback` field are
  removed.
- Remaining text references: see R3 and O2.

### Check 5: read-path cost

- Before this PR, a snapshot whose backfill checkpoint was complete set `fallback = None`, so
  derivation cost nothing.
- Now every selected record without a source event time, in a metadata-layer scope, pays for:
  - one manifest point read and a JSON parse of its rows;
  - a subject reverse lookup plus a forward check;
  - for each graph without stored facts, a `RdfDatasetBuilder` freeze, about five more term
    lookups, and two owed quads.
- None of this is charged to the request budget; only the reservations bound it.
  `owed_rows` filters every owed quad linearly on each stream-0 pattern call.
- The live store already pays this for about 65% of its graphs, and #158 measured medians
  within 30 s on it. Whether the store meets the 30 s target once 100% of graphs take this
  path has not been measured. See R2.

### Check 6: the new test

- `receipt_time_fallback_facts_are_derived_not_stored_and_a_pending_marker_store_opens` asserts
  70 derived rows with coverage 70. On main, ingest stores the facts, the view arms no
  derivation, and coverage is 0, so the assertion fails there. `pending_event_fallback_marker`
  does not exist on main either.
- It protects the named behavior: nothing is stored, the same facts come back, and a marker
  store opens with an unchanged format and no rewrite of record graphs.

### Check 7: architecture section 1 paragraph

- It sits in "1. Goals and boundaries" and states:
  - low-assurance, high-throughput;
  - speed and storage as the only goals;
  - the Katamari/Sparrow razor, including cutting data into subsets;
  - "nothing irreplaceable only in Sparrow";
  - the three worked examples.
- It matches the mandate as the caller stated it. I found no other mandate source in the
  worktree to compare it against.

### Check 8: PR body against the diff

- Matches the diff: the "What changed" section, design decisions 1 and 2, and "Not done".
- Not shown by any test: design decision 3, "facts are never doubled" (R1).
- Contradicted by the logs: the "Evidence" bullet (R4).

## Findings

### R1 (REQUIRED): the stored-facts path, the live store's main case, has lost its only tests

On main, two assertions covered "a graph that stores the facts is not derived":

- the late ingest during migration had coverage `0` in
  `open_serves_before_the_schema_69_migrations_...`;
- the post-migration answers had coverage `0` there and in
  `maintenance.rs::reads_report_pending_mid_migration_...`.

This PR changes both to `1`, `300` or `>=`, and deletes
`remove_event_fallback_rdf_for_legacy_fixture` and the backfill writer. No test in the tree can
now build a record graph that stores the facts.

Every graph that main's writer or the backfill produced has that shape: about 930k migrated
graphs on `/SPARROW/store`, plus everything ingested since schema 69. The issue's scope says a
marker store "must open and be served by the new image". The new test only opens a marker store
whose graphs never stored the facts, with `{"after":null}` and no `done:` counter row, which is
not the live shape. PR design decision 3 is therefore asserted, not tested.

Fix: add one test-support fixture that stores `eventTimeFallback` and `eventTimeBasis` on some
record graphs, for example through the RDF writer and `rdf_index::fallback_metadata`, and leaves
a partial-frontier checkpoint and the `store-counter:done:event-fallback-rdf` row. Then assert
on a mixed scope that:

- each record answers exactly once;
- coverage counts only the graphs without stored facts;
- the store opens without a rewrite.

### R2 (REQUIRED): the read-time acceptance is open and the PR closes the issue anyway

The issue's acceptance requires "Read-time number above recorded on this issue". It makes the
decision conditional: "the facts stay if a normal query then exceeds 30 s". The PR body says the
measurement was not run, and still says `Closes #193`.

Once the new image ingests records without the facts, a failed measurement would need another
backfill of every new graph to restore them. That is the cost this change exists to avoid, so
the measurement has to come before the switch.

Fix: before merge, record the #158 live-size query set (medians and tails) against a store
where every unknown-time graph takes the derived path. The live store already derives for about
65% of its graphs. Alternatively, get a recorded ruling that waives the precondition, and change
`Closes #193` so the issue stays open for the measurement and the live 71-to-71 check.

### R3 (REQUIRED): docs still describe the removed migration or overstate the fence

- `docs/architecture.md:599-600`: "the schema 69 receipt-time fallback backfill is reported in
  SPARQL coverage while the view serves the facts it still owes" describes the removed migration
  as a pending path.
- `docs/interfaces.md`, new paragraph: "so older binaries still refuse the store rather than
  publish graphs and read them without the derivation". This holds only for writers older than
  schema 69. The previous schema-71 image opens a store that carries the checkpoint. Once that
  checkpoint is complete, it reads the new image's graphs with `fallback = None`, so it serves
  them without the derivation.

Fix: rewrite the architecture sentence as permanent read-time derivation. Narrow the interfaces
sentence to pre-schema-69 binaries, and state what happens if the store is rolled back to the
previous image (see O1).

### R4 (REQUIRED): the evidence bullet misattributes the gate run

The PR says "`193-commit5.log`: the commit hook's full gate ... EXIT=0". The artifacts show:

- `193-commit4.log` (fmt, clippy, workspace tests) finished at 11:47:39 with `EXIT=1`
  (`rpc.rs:4672`, left 4, right 5).
- 54d2a64 was committed at 11:47:53, 14 s later. The reflog has a single `commit:` entry at that
  time.
- `193-commit5.log` finished at 11:52:57, five minutes after the commit existed, so it cannot be
  the hook run that admitted 54d2a64.

The committed `rpc.rs` has `4`, and commit5 is a full pass that rebuilt only `sparrow-server`. It
is very likely the gate passing on the committed tree, but nothing in the logs records which
verification admitted the commit.

Fix: state accurately what verified 54d2a64, and confirm that its configured pre-commit hook
ran and passed. If it did not, recreate the commit through the hook. Do not use any bypass.

### O1 (OPTIONAL): rolling back to the previous schema-71 image loses facts silently

Under main, with the live checkpoint still pending at frontier `after`:

- graphs the new image writes with ids at or before `after` are neither derived by the old view
  (`id <= after`) nor revisited by the old backfill;
- `done:event-fallback-rdf` stops tracking the record-graph markers the new image adds.

I did not confirm how record ids order, so I don't know how many graphs this would hit.

Possible fix, a cheap fence with no format bump: delete `event-fallback-rdf-upgrade` at open. On
main, `Positions::read` then refuses with "backfill state missing". Otherwise, document that the
switch is one-way.

### O2 (OPTIONAL): stale comments and ledger text

- `event_fallback_rdf.rs`, the `carries_event_time` doc: "the graph needs no backfill".
- `docs/open-items.md` B-7 to B-11 call items "Still true" with line references into the deleted
  backfill code. They are now moot.
- `docs/deployment.md:655-660` and `deploy/verify-backup.sh:152-157`: the stall rationale cites
  the removed migration's "64-graph page" measurement.

### O3 (OPTIONAL): derivation cost if the measurement is tight

`owed_rows` filters all owed quads on every stream-0 pattern call, and the manifest reads and
term lookups are uncharged. If R2's measurement is close to 30 s, index the owed quads by
subject and predicate, and consider charging the derivation.

VERDICT: BLOCKED
