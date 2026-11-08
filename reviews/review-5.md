# Review 5: #146, recheck of review 4 (T1, O14)

## Scope

This round covers the uncommitted worktree
`/home/paudley/Active/sparrow/.worktrees/146-keep-status-and-ingest-responsive-while`
against base `d912922`, following the "Review 4 rework" section of `implementation.md`. It
covers:

- T1: the new unit test
  `watches::tests::retiring_a_claim_clears_only_the_failure_that_claim_caused`
  (`crates/sparrow-server/src/watches/tests.rs:325`), and the reduced RPC drain test
  (`crates/sparrow-server/tests/rpc.rs:4116`);
- O14: `retiring_a_claim_with_a_begun_import_frees_its_slot_and_staged_chunks`
  (`crates/sparrow-core/tests/storage.rs:6154`);
- whether anything else in the diff changed since review 4.

## Evidence

- **What changed since review 4.** I listed the files under `crates`, `deploy`, `docs`,
  `tools`, `registry` and `helpers-ledger.toml` that are newer than `reviews/review-4.md`
  (06:55). There are exactly three:
  - `crates/sparrow-core/tests/storage.rs`;
  - `crates/sparrow-server/src/watches/tests.rs`;
  - `crates/sparrow-server/tests/rpc.rs`.
  No production source, script or doc changed, so review 4's conclusions on D1, O11, O12 and
  O13 still stand. This is an mtime comparison, not a content diff against a snapshot.
- **Logs under `/tmp/s146-r4-*`:**
  - lib: 29 passed;
  - `repeat-2`: the retire test passed (1 passed, 85 filtered, 4.13 s);
  - core: `consumption_retire_moves_only_an_unpublished_claim_out_of_pending ... ok`, among
    others.
  The implementer also reports fmt, clippy, storage 233, rpc 86, five consecutive runs of the
  retire test, and census-check, all exit 0.
- I ran no cargo command. The judgments below come from tracing the code.

## Recheck

### T1: closed

**The unit test exercises production code, not a mirror of it.** `tests.rs` is the child
module `watches::tests` (`watches.rs:36`, `use super::*`), and it calls the private production
`retire_claim(&mut [Detector], &ClaimRetirement)` directly. It builds:

- a real `Detector` over a real store;
- three claims taken through the detector's own `ClaimRoot` (`reserve`, then `recover`), whose
  private copies are then removed.

So every retirement in the test goes through the real `ClaimRoot::retire` and
`Store::consumption_retire`. Only the starting failure state is set by hand (`failure` and
`failed_claim`), and that state is the input the rule depends on. The test covers all three
outcomes of `watches.rs:860-862`:

1. Retiring another claim keeps the failure and its `failed_claim`.
2. Retiring the cause clears both.
3. A failure with no claim (a discovery failure) survives a retirement.

The path that sets `failed_claim = Some(id)` (`watches.rs:1301`) is not driven by this test.
I traced it in review 4, and it is unchanged.

**The RPC test is now deterministic.** It no longer reads which claim caused the failure, and
it has no sleep and no stable-read heuristic. Each assertion depends only on ordering the
daemon guarantees:

- `pending_claims == 1` and `retired_claims[0] == lost` right after `lost` is retired. The
  retirement's batch removes `lost` from pending before the reply. `replaced` stays pending, and
  `waiting.jsonl` is not written yet.
- `pending_claims >= 1` after `waiting.jsonl` is written. `replaced` is still pending, whether
  or not the new file has been reserved yet.
- The `watch_until` condition, `pending == 0 && completed >= 1`, is a bounded wait on an
  observable state. It is reachable only once both stuck claims are retired and the waiting file
  has published.
- `failure` is null. The watch worker is one task, and admin commands and claim work run
  sequentially in its loop, so a retirement never interleaves with a recovery of the same
  claim. The reasoning:
  - Only `lost` and `replaced` ever fail.
  - Whichever of them set the failure last is the `failed_claim` when it is retired, because a
    retired claim never retries again (it leaves pending and loses its retry entry). That
    retirement therefore clears the failure.
  - The waiting file's success path also sets `failure = None` (`:1312-1313`).
  - Nothing after these steps can set a failure.
- Exactly two `retired-unrecoverable` journal lines. The refused retirements log nothing, and
  each successful retirement logs once.

### O14: closed

The consumption claim's import begins at `DAY`, so its lease ends at `3 * DAY`. The 15
ordinary imports begin at `50 * DAY`, so their leases run to `52 * DAY`. The test reclaims at
`50 * DAY`:

- Before retirement, the reclaim returns 0, the claim's import is still readable, and a 17th
  begin is still refused with `RESOURCE_LIMIT`.
- After retirement, the reclaim at the same instant removes the one chunk and the import
  (`NOT_FOUND`).
- The closing begin can succeed only through the slot the retired import freed.

The doc comment describes what the test proves.

## Findings

- No new findings.
- Review 4's T1 and O14 are closed.
- Review 4's D1, O11, O12 and O13 conclusions stand, because no production code changed since.

### R4 carried (orchestrator; publication condition, not a code finding)

The PR must say "Refs #146" and must not close the issue while done-when 3 is still owed:
the #145-gated measurement on a fresh copy of the schema-68 store, as ordered in D118. This
stays recorded. It is satisfied at publication, not in this worktree.

VERDICT: PASS
