# Plan: issue 36, bound the schema 69 first-open migration

## Behavior
Step 1: the event-fallback job (`event_fallback_rdf.rs`) commits many record graphs per page with one synchronized commit, and skips graphs that already carry `eventTime` or the fallback facts without decoding the whole graph. It stays resumable and still covers graphs with no catalog record. Step 2 (only if the measured open stays above 10 minutes): both jobs run as an online maintenance duty per #18.

## Dependencies
`catalog_upgrade` pager (shared with other startup jobs); `rdf_index::stored_metadata`; the existing writer-format fence from #20 (no new family needed for step 1, because the job's output and checkpoint format are unchanged or upgraded compatibly).

## Exclusions
No change to the facts written, the fence numbering, or the #13 owner-index job unless measurement shows it matters. Step 2 is not started unless step 1 measures above 10 minutes.

## Acceptance
Open of a copy of the 20261107T100413Z nightly under unit-like limits (CPUQuota=100%, IOWeight=20, MemoryHigh=2G) is 10 minutes or less. An interrupted open resumes after its last committed page. Existing #20 tests pass. `make check` passes via the commit hook.

## Risk
Changing page bounds in the shared pager could affect other startup jobs; keep the change local to this job's Job constants or a per-job option. A partially committed checkpoint from the old binary (page_entries 1) must still resume.

## User rulings
- 2026-11-07 ~06:05 CST (lillith_coordination, as Patrick): "Decision: no switch with a first-open of that length. C18's own criterion for disk-format updates is the online migration (#18: rebuild derived rows page by page while the daemon keeps serving), and the #20 metadata-graph rewrite plus the #13 index build belong under it: on first open the daemon serves on the old rows and backfills in the background, with a progress line and a watermark. Build that first (or show that the rewrite can be bounded to minutes), then the switch at a planned moment. Let the copy run to completion so we have the total number, then stop it and free the disk. Live stays on schema 68. Report the number and the design in one message."
- 2026-11-07 ~06:35 CST (lillith_coordination, as Patrick): "Agreed in full: file it under #18; first raise #20's page_entries (many graphs per page, one synced commit per page, skip records that already carry a source eventTime without decoding) and re-measure on a copy under unit-like limits; if the open is 10 minutes or less that satisfies "bounded to minutes" and the switch follows at a planned moment; if over, build the online duty as you designed (serve at once, pages bounded by count and bytes, atomic checkpoint, status migrations, readers explicit about pending). Keep the copy measurement outside 07:45 to 08:00. #14 waits. The 76 minutes and the root filesystem at 96 percent (73 GB free) are recorded; the disk is Patrick's."
