# Issue #43: Count a new artifact's records correctly when a listing resumes inside its millisecond

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

Row B-18 of docs/open-items.md. Review round 2 of #24, finding O11: pr/26:review-r2.md:214-218. Marked by the reviewer as OPTIONAL and not taken.

## Current behavior

A new artifact that lands inside a millisecond the listing has partly walked can appear with a short `retained_records` count (reviewer text, pr/26:review-r2.md:214-218). `docs/interfaces.md:332-333` says only "Evidence received or retired between pages can appear in or leave later pages, but no row repeats", with no mention of the short count.

## Work

Either document the short count next to the sentence at `docs/interfaces.md:332-333`, or make a resumed page that meets an unfolded parent read that parent's records through its `record_ids`. Record the choice and the reason.

## Done when

If code is changed: a test named `listing_resumed_inside_a_millisecond_counts_a_new_artifacts_records` publishes a new artifact with several records into a millisecond the listing is part-way through, resumes, and asserts the artifact's `retained_records` equals its record count; it fails today with a short count. If documentation only: `docs/interfaces.md` carries a sentence naming the short `retained_records` case, checked by `grep -n "retained_records" docs/interfaces.md` finding it beside the continuation text.


## Comments (1)

### paudley — 2026-11-24T08:12:28Z

The live listing now resumes from a per-source index position. It keeps
the last completed key and one partially folded artifact. A newly
encountered parent starts at the beginning of its own record manifest.

Use the shared pagination fixture proposed on #44. Insert a multi-record
artifact at the same receipt millisecond with an index key after the
cursor. Verify that the resumed listing includes that artifact with its
full retained record count. Keep the count assertion here and the
crowded-millisecond traversal assertion on #44.

[Crowded millisecond](http://lillith:7555/fleet/sparrow/issues/44) · [Per-source index](http://lillith:7555/fleet/sparrow/pulls/142)


## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
