# Delivery review: artifacts-list (issue #24), round 2

## Scope

- Commit 24a0484 on top of 5070e18, base main 0221e87, worktree
  /home/paudley/Active/sparrow/.worktrees/artifacts-list. The branch was not modified.
- This round judges the changes between 5070e18 and 24a0484 (14 files, +767/-318) against the
  five REQUIRED findings of round 1 (R1-R5) and the coordinator's checks (a)-(e).
- Code read:
  - `artifact_read.rs` in full: `Position`, `Stage`, `artifact_receipt_page`, `fold_indexed`,
    `Indexes`, `refuse_row`;
  - the changes to `catalog_lease.rs` (`entry` and `visit`) and to `temporal_index.rs`
    (`TemporalRows::after`);
  - `source_context.rs` (`update`, `ready`, `indexed`) and `recordless.rs`
    (`is_live_owner`, `publish`);
  - in eviction.rs, how evicting a record marks its parent evicted;
  - the receipt-time setting in `publication.rs` (import prepare);
  - in `rpc.rs`: `QueryReply::new` and `Drop`, `query_work`, `result_next`, and the reworked
    `capture_lifetime`;
  - `agent/artifacts.rs` and `agent/tool/catalog.rs`;
  - the test changes in `tests/storage.rs` and `tests/rpc.rs`, and the
    `docs/interfaces.md` changes;
  - for (e), the overlapping files on `origin/query-only`.
- Not run: make check (the caller reports 427 tests passing in the hook), cargo tests, the live
  service. This was a static review; no build was started.

## Evidence

- Commit 24a0484 went through the pre-commit hook, which runs make check. No `validation.md`
  or gate log exists for this branch (O6 from round 1 is still open).
- `prose-check` on the added `docs/interfaces.md` lines and on the commit message: exit 0, both.
- Tests that now protect the round-1 corrections:
  - **R1:** `artifact_listing_shares_typed_pages_unknown_origins_and_agent_refusals`
    (tests/rpc.rs:2703-2843). The first and second pages have `matching_artifacts` null,
    `incomplete` certainty and `catalog_complete` false. The final page reports `exact` with 3.
    The handle count is 0 afterwards.
  - **R2:** `capture_lifetime` runs on a live listing again (rpc.rs:4077-4093). After an
    abandoned continuation, the original handle is still there and its cursor still continues.
    `live_artifact_continuations` (tests/rpc.rs:11379-11480) continues through CLI `--cursor`,
    CLI `result-next`, MCP `result_next` and typed `resultNext`. Each surface gets the same
    next rows, the spent cursor returns `RESULT_EXPIRED`, and neither handle remains.
  - **R5:** `artifacts_listing_spans_an_artifact_larger_than_one_page`: 40 records at 10
    entries per page take more than 4 pages, the row reports `retained_records` 40, and the
    rows come in receipt order. `artifacts_listing_page_over_its_bound_is_refused_by_name`
    checks that the message names the requested `page_size 100`, the entry bound, the artifact
    ID and its receipt time.
  - **R4:** no test exercises it (O7). The source path does run end to end in the existing CLI
    tests for `wire-catalog` and `wire-fragments`, so its correctness, including reclaimed-parent
    fragments, has execution evidence. The claim that it costs only the selected source's rows
    does not.

## Round-1 findings

- **R1: resolved.** `receiptWalk` is a new field, `ArtifactListPage @4`. The renderer
  (agent/artifacts.rs:148-180) withholds `exact` and `catalog_complete` while a live page has a
  next cursor.
- **R2: resolved.** Core no longer retires the continued handle. `QueryReply` releases the new
  handle if it was not delivered, and releases the continued handle (`supersedes`) once the new
  page is delivered. `result_next` now uses `read_work` and the same guard. The two test
  rewrites from round 1 are back on live listings.
- **R3: resolved.**
  - `SCHEMA_VERSION` is 66.
  - The new "Live artifact pages (schema 66)" section names the four groups of statements it
    supersedes for live listings.
  - The round-1 ranges are amended in place: lines 30-34, 1044-1048, 1883-1900 and 2089-2092.
  - The `catalog.rs` MCP descriptions for `artifacts_list` and `result_next` are updated.
- **R4: resolved.** With a source, the stages are `source-context-v1:` (records) and then
  `empty-time:` (recordless artifacts). See (b).
- **R5: resolved.**
  - `Position` keeps the stage, the last consumed index identity (`after`) and the rows folded so
    far inside the millisecond. One artifact's records therefore span pages.
  - Artifact rows are charged through the streaming header reader, so the record and chunk arrays
    no longer count against the budget, and the 512 KiB per-entry cap no longer blocks them.
  - A page refuses only when it consumes no index row at all.
- **O1, O2 and O5:** done. O3: the refusal messages now name the entry and its times, but there
  is still no test. O4 and O6: not done; both were optional.

## Answers to the coordinator's checks

**(a) Cursor durability under concurrency.**
- **Two continuations from one cursor.** They serialize on the `artifact_reads` mutex, which is
  held across the page read. Each reads the same position and publishes its own handle. Each
  delivery then releases the predecessor once: the first release returns `released: true`, the
  second `false`. The second is a no-op with no error, and nothing is released twice.
- **Releasing a different handle by mistake.** Not possible: handle IDs are random, and
  `supersedes` is set only when the page's ID differs from the cursor's.
- **A lease that expires mid-walk.** Each page has its own 60 s lease. `continuation()` refuses an
  expired predecessor with `RESULT_EXPIRED`. If the predecessor expires while its successor is
  being read, the successor still publishes, and the later release finds nothing. The walk dies
  only if the client waits more than 60 s between pages, which the docs state.
- **A handle leaking past its lease.** Not possible beyond what already happens:
  `purge(now)` runs on every artifact read and `result_next`, as it does for every other capture.
- **A connection closing between reply and release.** `delivered` means rendered into the reply
  builder, and the release runs in `Drop` at the end of the handler, before the reply reaches
  the wire. If the connection drops after rendering, two things follow:
  - the continued handle is already released, so the cursor the client still holds returns
    `RESULT_EXPIRED`;
  - the new handle stays until its lease ends, at most 60 s, and counts against
    `MAX_HANDLES` (8) until then.

  Rendering is already this repository's delivery point ("Successfully rendered cursors",
  interfaces.md:30-32), and fresh captures have the same window, so this is not a contract
  breach. The new text says "delivered", though (O8).

**(b) R4 indexes and ordering.**
- **`empty-time:` is the right second index.** `recordless::publish` writes it exactly for
  recordless artifacts that are not evicted. Expiry and eviction delete it.
- **Artifacts with records are all reachable through their records.** Evicting any record marks
  its parent evicted (eviction.rs:461-470). Records share their artifact's expiry, and a whole-
  artifact pin protects them through `artifact_id`. So no complete artifact that has records can
  be without a live record, and the source path reaches every such artifact through those records.
- **`source-context-v1:` matches the primary index.** It is written and deleted in the same batch
  as `time:` (source_context.rs `update`, called from durability.rs:142). It is rebuilt
  synchronously at open, after the upgrades. `fold_indexed` also confirms the primary `time:` row
  for each entry.
- **The ordering matches the unfiltered contract.** The key is
  `<blake3(source)>:<received_at:020>:<record id>`. Each millisecond is folded fully and then
  emitted in artifact-ID order from a `BTreeMap`, so both paths give receipt time then artifact
  ID.
- **The only cost leak.** `empty-time:` holds every source's recordless artifacts.
  `Indexes::next_time` visits each millisecond that holds one, and those rows are charged before
  the source filter drops them (O9). The docs' claim "never reads or charges another source's
  records" is accurate as written, because it speaks of records.
- **A new dependency.** A source listing now depends on `source_context::ready`. If the open-time
  rebuild failed, `artifacts --source` and `sources --source` refuse with a message about
  "context" (O10).

**(c) The in-millisecond position under concurrent writes.** The position is stable for every
index entry that existed when the walk reached it, and no row repeats:
- `after` is a key position; the seek goes to `after` plus a NUL byte, the next possible key;
- each index entry is consumed once, in key order;
- the rows folded so far travel in `Position.rows`, are emitted once in the Emit stage, and are
  never folded again.

New entries can land in a millisecond the walk has partly passed. That happens only through an
inline ingest at the walk's own head millisecond, or through an import whose receipt time (set at
its first `importPrepare`, publication.rs:325) equals that millisecond. Such entries can be
partly skipped:
- index keys that sort before `after`, or that belong to a stage already finished, are not read;
- so a new artifact can appear with `retained_records` short of its real count, or with only
  its records counted.

Both conditions are narrow, and the docs already say evidence received between pages "can appear
in or leave later pages". A partial count is not quite that, though (O11). Imports that land
behind the walk are simply not listed, which the docs allow.

**(d) Core re-reading versus the docs.** They are consistent. Round 1's sentence "each
continuation cursor serves one read" was removed. The docs now say:
- "Continuing reads the store as it is then and publishes a new handle";
- the daemon releases the continued handle only after delivery;
- after a delivered continuation the cursor returns `RESULT_EXPIRED`.

In core alone, a cursor can be continued again until it is released or its lease ends. Each
continuation reads that position afresh: it is not a replay, so its rows can differ if evidence
changed. Only the daemon's two guarded paths call this API in production
(rpc.rs:3059 and :3610).

A caller using core directly that never releases handles would accumulate up to `MAX_HANDLES`
within one lease and then get `RESOURCE_LIMIT` "handle count is exhausted". The branch's own test
releases them, as the daemon does. No production caller is affected.

**(e) Rebasing to 67.** Nothing here is hard to rebase. The two branches touch different capnp
structs: #25 adds `Status.queryOnly @50` and `retentionSeconds @51`, and this branch adds
`ArtifactListPage.receiptWalk @4`. The doc sections are separate hunks: this one is inserted
before "Large artifacts (schema 65)", and #25's sits near line 373. No test hard-codes 65 or 66;
tests use `sparrow_client::SCHEMA_VERSION`.

Exactly what would change:
1. `crates/sparrow-client/src/lib.rs:69`: `SCHEMA_VERSION` 66 to 67. Both branches edit this
   line, so this is the one textual conflict, and it resolves to 67.
2. `crates/sparrow-client/schema/sparrow.capnp:54`: the comment "# Schema 66:" becomes 67. The
   field ordinal `@4` stays.
3. `docs/interfaces.md`, eight mentions: line 34 "(schema 66)"; line 294, the heading "Live
   artifact pages (schema 66)"; line 296 "Schema 66 changes"; line 1048; line 1887; line 1891;
   line 1900; line 2092 "Schema 66 gives".
4. Optionally, one sentence in the new section noting that schema 67 also includes the schema 66
   query-only daemon.

Deploy image names are chosen at deployment and are not in this branch.

## Findings

No REQUIRED findings.

**O3 (round 1, partly done). OPTIONAL: the integrity refusals are untested.**
The messages now name the entry and its times. No test covers them, for example by writing a
stray `time:` row or a mismatched catalog row through `catalog_db` after open.

**O6 (round 1). OPTIONAL: there is still no validation record.**
Record the hook's make check log and, after deployment, the UAT output in a branch
`validation.md`.

**O7. OPTIONAL: R4 has no regression test.**
Nothing tests that a source listing reads only that source's rows. A test would hold many rows of
source B interleaved in time with a few rows of A, and assert that `artifacts --source A` with a
small `scan_budget` returns A's rows on its first page. Without it, a regression back to
filtering after the read would pass every test.

**O8. OPTIONAL: say "rendered" where the new text says "delivered".**
interfaces.md:33-34 and the Continuation bullets say "delivered". Say "rendered" instead, and
state that a reply lost after rendering spends the cursor the client held, while its new handle
lives out its lease.

**O9. OPTIONAL: source listings charge every source's recordless artifacts.**
A source listing folds and charges all recordless artifacts in the window, whichever source they
belong to. A source that writes many zero-byte files makes other sources' listings return empty
pages with cursors. Filtering `empty-time:` rows by source before charging would need the
artifact's source, so this may stay as it is; if it does, document it.

**O10. OPTIONAL: the cache-failure message misleads for listings.**
When the `source-context` cache failed to rebuild, a source listing refuses with "before
requesting context". Naming the listing in the message would help the operator.

**O11. OPTIONAL: a new artifact inside a partly walked millisecond can be partly counted.**
Such an artifact can appear with a short `retained_records`, as described in (c). Either
document it next to "can appear in or leave later pages", or, when a page resumes inside a
millisecond and meets a parent it has not folded, read that parent's records through its
`record_ids`.

**O12. OPTIONAL: very crowded milliseconds still stop the listing.**
`Position.rows` counts against the 2 MiB snapshot capacity. A millisecond holding thousands of
artifacts would make publication refuse, and retrying would refuse again. That is far beyond
anything plausible on the live store.

VERDICT: PASS
