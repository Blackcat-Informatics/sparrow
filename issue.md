# Issue #36: Bound the schema 69 first-open migration to minutes, or run it online

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Part of #18. The schema 69 open-time jobs from #20 and #13 keep the store closed for about 76 minutes on a copy of the live store, which blocks the bundled deploy.

## Observed

On 2026-11-07 the image built from 758b41a opened a copy of the 04:04 CST nightly backup (20261107T100413Z, about 151,000 live records) through `deploy/verify-backup.sh --image`. The open had not answered after 1800 s. The script ended after 4,563 s, which covers the open and a clean shutdown. The copy ran outside the unit's limits (`CPUQuota=100%`, `IOWeight=20`), so the live daemon would be slower.

## Code path

- `event_fallback_rdf::upgrade` (`crates/sparrow-core/src/storage/event_fallback_rdf.rs`) runs a `catalog_upgrade::Job` with `page_entries: 1`. Every record metadata graph is one page, and `catalog_upgrade::commit` writes each page with `set_sync(true)`. That is one synchronized commit per record.
- `publish` calls `rdf_index::stored_metadata` to decode the whole graph before it looks for `eventTime`. A record with a source instant adds nothing but still pays the decode and its own synchronized commit.
- `rdf_index::upgrade_owner_graphs` writes 65,536 declarations per page, so the #13 build needs only a handful of commits.

## Work

Step 1, page the event-fallback job:
- Raise the job's page bound to many graphs per page, bounded by count and bytes, with one synchronized commit per page.
- Skip a graph that already carries a source `eventTime` (or the fallback facts) without decoding the whole graph.
- Keep the job resumable after any committed page, and keep coverage of graphs that have no catalog record.
- Re-measure the open on a copy of the nightly under unit-like limits (`CPUQuota=100%`, `IOWeight=20`, `MemoryHigh=2G`).

Step 2, only if the measured open stays above 10 minutes: run both jobs as an online maintenance duty, as #18 describes.
- The open applies the writer-format fence and serves at once.
- Pages are bounded by count and bytes, and each commits its rows with an atomic checkpoint.
- `status` reports the progress of each migration: done, total, watermark and failure.
- Readers say when a migration is pending. Scoped SPARQL keeps the store-wide plan until the owner index is complete. SPARQL answers over unmigrated metadata graphs report that the event-fallback migration is pending.

## Acceptance

- The open of a copy of the live store, under unit-like limits, takes 10 minutes or less (step 1), or the daemon serves at once and the migration finishes in the background (step 2).
- An interrupted open resumes after its last committed page.
- `make check` passes.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
