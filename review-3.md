# #158 delivery re-review (review 3)

## Scope

This review covers the uncommitted worktree
`/home/paudley/Active/sparrow/.worktrees/158-serve-every-query-and-status-call` compared
with `main` at ea1a4b2 (`git diff main`). It is limited to the review-2 fixes F1 to F4, the
disposition in validation.md ("Review 2 disposition"), and four changes made since review 2:

- (a) the event-fallback owed-ordinal fix
- (b) the `online-c` scope bound in the acceptance test
- (c) the status test's artifact listing thread
- (d) the query-only copy assertions in the RPC test

I read the Rust skill first. I ran no cargo, make, build or test command, and I touched no
live store or daemon. The only commands I ran were `git diff`, `grep`, file reads, log
reads, `ls`/`ps` (read-only), and `prose-check`. I did not review the temporary probe
`crates/sparrow-core/tests/measure_158.rs`. It is untracked and must be deleted before
commit.

## Evidence

- Logs under `/home/paudley/Active/sparrow/.stage/r82/`. Each ends with its own EXIT line:
  - `158-focused14.log` (14:50): EXIT=0. 4 passed: the F4 test, schema 69, acceptance and
    status.
  - `158-storage9.log` (14:51): EXIT=0. The full storage target, 236 passed.
  - `158-rpc3.log` (15:15): EXIT=0. The full RPC target, 87 passed.
  - `158-fmt1.log` (15:26:26): EXIT=0.
  - `158-check14.log` (06:03): Clippy, release, EXIT=0.
- File mtimes. Eleven files have mtimes of 15:26:17 to 15:26:19, which is just before the
  fmt log closed, so they were written by that fmt run. Except for that rewrite, the tested
  tree is the current tree. Clippy (06:03) predates the ordinal fix, the test edits and the
  RPC test edit (15:08).
- `158-measure6.log` was still running at 15:31 (the `cargo test ... measure_158` process is
  live). It has no result yet.
- `prose-check docs/interfaces.md docs/architecture.md` exited 1. All nine hits are the
  existing `UInt64`/`UInt32` identifier pattern. None is in changed text.

What I verified by reading the code:

- **F1, window exactness** (`artifact_read.rs:416-478`, `:485-523`, `:857-997`;
  `source_summary.rs:757-824`):
  - `Completion::index` inserts into a `BTreeSet` and pops the largest key once it holds
    more than `rows` keys. So it keeps exactly the smallest `rows` selected keys, and
    `truncated` is true exactly when a selected key above the last kept one exists.
  - `keep` filters on `key > after`, the selected source prefixes and the receipt bounds.
  - `next_listed_row` returns `Window` only when the merged next row is above `through`, or
    when no row remains while `through` is set. `through` is itself a window row and is
    never filtered. So `Window` is reached only once `position.after >= through`, and
    `position.after` never passes `through`. A stored row equal to an owed row wins the
    merge (`key < stored`), so a copied row is listed once.
  - The recompute in `artifact_next` fires on `after >= through` with no partial row, and
    reads keys strictly above `after`. No owed row is skipped or listed twice at a window
    end.
  - Progress: after a recompute, the next row is at most the new `through`, which is
    above `after`. So the page lists or folds a row. The empty-page `Window` break
    (`:882`) can occur at most once per window.
  - Locks: the recompute runs before `artifact_page` takes `self.lock()` (`:547`). The
    first window is read before both the registry and the writer (`:327-332`).
  - The window charge (`:541`) is 64 bytes plus a key of about 139 bytes
    (`source-artifact:` + 64 hex + `:` + 16 hex + `:` + a 41-byte `artifact:` ID), times
    32,768 rows: about 6.4 MiB of the 32 MiB `SCAN_BYTES`. "A window always fits" is true.
  - The test-only override (`storage.rs:528-529`, `:1630-1631`, `:2803-2808`) follows the
    existing `test-support` pattern. Production always uses `OWED_WINDOW`.
  - Docs: `interfaces.md:353-360` and `:815-827`, the architecture.md text and the ledger
    rows now describe windows. The `RESOURCE_LIMIT` refusal and its contradiction are gone.
- **F2** (`rdf_view.rs:612-709`): `lookup_in(.., false)` now threads `charged` into
  `verified_forward_in`, whose forward `point_in` and reverse `reverse_point` both skip
  `budget.request`. Memory reservations are kept. Every owed-path caller passes `false`:
  - `migration_state` predicates (`:348`)
  - `owe_fallback` graph and subject (`:395`, `:402`)
  - `owe_term` (`:439`)
  - `fallback_pending` subject (`:537`)

  The engine-facing `term_id_by_value` still passes `true` (`:1194`). The schema 69 test
  (`tests/storage.rs:19395-19405`) asserts that each during-migration scoped answer spends
  at most the migrated store's `requests`. A charged path spends one `request` per read,
  so re-charging the owed lookups would raise that count.
- **F3** (`store_counters.rs:597-649`): equivalent to the per-term `count`. Each included
  term has `low != Everything` and `low < high`, the same as `count`'s early returns. The
  scan starts after `min(low)`, which is `None` for `Nothing`. It stops at the first key
  that `max(high)` does not cover. It credits a term for key k exactly when
  `!low.covers(k) && high.covers(k)`, which is `low < k <= high`. Keys ascend, so stopping
  at `max(high)` drops no key any term counts, and starting at `min(low)` misses none.
- **(a)** `rdf_index.rs:490-496` increments `next` and then returns it, so `rdf:next` holds
  the last allocated ordinal. `FallbackPending::next = rdf:next + 1` (`rdf_view.rs:350-363`).
  `owe_term` (`:464`), the owed branch of `lookup_in` (`:651`) and `owed_term` (`:501`) all
  agree on ordinal `next + index`. No other use of the old meaning remains.
- **(b)** Every `online-c` record is received at `31*DAY + 1..=12`. `received_before =
  31*DAY + 13` keeps all of them. Flow artifacts arrive at `base + 100` and later, so they
  are excluded. The scope's answer set is unchanged, and it is the same fixed scope during
  and after. This removes only an unrelated source's charged catalog entries, so it does
  not weaken the during-equals-after invariant.
- **(c)** The thread exists to hold the artifact registry while it waits for the writer.
  With `source-summary` complete there, it has no owed pass. Accepting a continuation-only
  first page follows the existing partial-row law. The protected assertions (status
  answers within the hold, the writer is held at both ends, `held_keys == keys`) are
  unchanged.
- **(d)** The copy holds exactly one record, so `live_records = 1` is exact. The test still
  asserts `done = 0`, `watermark = start` and `failure = null` for all five migrations,
  `event_fallback_pending = 1`, and that writes are refused. "No migration runs" is still
  protected. The new values are the #158 contract, not a weakening.

## Findings

### R1 REQUIRED: F4 is half fixed; the owed-retired skip is never exercised

The skip branch is `artifact_read.rs:898-902`
(`if row.owed && row.summary.is_none() { position.after = Some(row.key); continue; }`).
The test `a_live_listing_across_migration_pages_and_expiry_lists_the_finished_index`
(`tests/storage.rs:19556`) never reaches it, although its doc comment (`:19553-19554`) and
the validation.md disposition both say it does.

Trace, with a window of 2 and `page_size` 1:

- `pending_source_summary_migration` also removes the store counters.
  `store_counters::fence` starts seeding at `Seed::At(0, None)`, and seeding takes one
  page per family, so six `migrate_step` calls go to `store-counters` first.
- Windows `{1,2}`, `{3,4}` and `{5,6}` cover the first source exactly. Page 6 lists row 6,
  which is `through`.
- `expire(after)` runs next (`:19661`). The next continuation recomputes the window from a
  snapshot taken after the expiry. A retired artifact contributes no index row to the
  `Completion`, so the second source's two early artifacts never enter an owed window.
- The first `source-summary` page is step 7, after the expiry. So no owed row that was read
  before the retirement is folded after it.

Removing the skip branch would therefore still pass this test, although the listing would
then fail with `STORAGE_FAILURE` ("the per-source index lists … without its entry").
(The equal-key dedupe is exercised: step 7 copies `b3` while `b3` is in the current
window.)

Fix:

- Change `store.set_owed_window_for_tests(2)` at `:19618` to `5`. Tracing the same code,
  the second window is read before the expiry and holds the next first-source row and the
  second source's first four rows. The second source's two early artifacts are then owed
  in the current window when expiry retires them, and they reach `:898` (skip). Its third
  row is listed before step 7, and its fourth row is the equal-key dedupe after step 7.
  The listing still crosses window ends (12 rows in windows of 5).
- At the expiry, add an assertion that `source-summary` is still pending with `done == 0`,
  so the test fails loudly if the migration order changes.
- Alternatively, keep the window at 2 and expire after 5 listed rows. Either way, make the
  doc comment match what the test drives.

### R2 REQUIRED: validation.md does not record the current evidence, and the latency comparison is still missing

validation.md (last written 05:49) is stale:

- Its Evidence table still lists storage8 and focused10 and says "fmt, clippy, rpc tests,
  `make check`: none on the current tree; not run".
- Its latency table predates the windowed listing, the `exact_values` single pass and the
  snapshot paths.
- The disposition says "The probe measures status latency while the counters seed", but
  `158-measure6.log` has no result yet.

The issue requires answer latency during the rebuild to be measured and reported against
latency after it. The coordinator ordered a recorded status latency while writes are
stalled. Neither exists yet for this code. The last Clippy run (check14, 06:03) also
predates the ordinal fix, the test changes and the RPC test edit, and the mandatory
`make check` has not run on the final tree.

This is not a code defect, but the contract is not evidenced until it is done.

Fix:

- When measure6 completes, add its table to validation.md:
  - status while `store-counters` seeds, after seeding, and during a held writer
  - live artifact page, first page and window-crossing continuation, during and after
  - sources, context and SPARQL, during and after
- Replace the Evidence rows with focused14, storage9, rpc3 and fmt1, each with its EXIT
  line and scope.
- Run `make check` (or let the pre-commit hook run it) on the final tree, and record its
  log and exit status.
- Delete `crates/sparrow-core/tests/measure_158.rs` before commit.

### O1 OPTIONAL: a window recompute holds the artifact read registry for the whole owed pass

`artifact_read_cancellable` locks `artifact_reads` (`artifact_read.rs:333-336`) and passes
the guard into `artifact_next` (`:343`). `artifact_next` then runs `owed_index_rows`
(`:512`), the full remaining-migration pass, while it holds that guard. Every continuation
that crosses a window end therefore serializes the following behind one pass, each holding
an RPC permit:

- fresh artifact listings, after their own owed pass
- cursor replays
- artifact `result_release` calls

This was seconds per pass at 300,000 records in `158-measure5.log`. Status no longer
depends on this registry, which is why this is optional. It is the same pattern as
review 2's O5 for sources.

Fix: under the registry, copy the continuation's selector, start and `through`, then drop
the guard. Compute the window, retake the registry, and re-check that the continuation is
still live before `artifact_page`. This matches what the fresh-listing path already does
at `:325-332`.

### O2 OPTIONAL: the F2 test compares request counts only

`tests/storage.rs:19399-19405` compares `evidence.view.requests`. A charged read also adds
`admitted_bytes`, and the budget enforces `io_bytes` separately. Asserting
`admitted_bytes <= migrated` too would make the test also cover a scope near its byte
budget. Today both totals move together, so this is defense in depth only.

VERDICT: BLOCKED
