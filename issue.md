# Issue #66: Refuse a malformed metadata graph at open even when it already carries eventTime

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

- Row B-11 of `docs/open-items.md`.
- Review finding 5, PR #37 (#36 step 1), `pr/37:reviews/review-1.md:154-159`, labelled OPTIONAL (observation) with "no change is needed". Marked by an agent.

## Current behavior

Verified at main 03052cf.

- The event-fallback job returns early for a graph whose subject already carries `eventTime` or `eventTimeFallback` (`crates/sparrow-core/src/storage/event_fallback_rdf.rs:111-119`).
- That return comes before `stored_metadata` (`:121-125`), which is where the ownership, closure and term-ownership checks run (`crates/sparrow-core/src/storage/rdf_index.rs:824` onward).
- Before PR #37, every graph passed those checks during this job, so a malformed record graph refused the open. Now a malformed graph that carries `eventTime` opens silently. No other open-time job is known to validate these graphs (unverified).

## Work

Restore the refusal without decoding every graph's terms on the skip path: run the cheap manifest checks (owner match on every row, term references inside the manifest's term list) on skipped graphs, or move those checks into a single validation step that every record graph passes once per open. Document which open refuses a malformed metadata graph.

## Done when

- A test plants a record metadata graph that carries `eventTime` and has a manifest row whose owner differs from the graph's owner, opens the store, and asserts that the open refuses with `STORAGE_FAILURE`.
- That test fails at 03052cf, where the graph is skipped at `event_fallback_rdf.rs:119` and the open succeeds.


## Comments (1)

### paudley — 2026-11-24T14:06:26Z

PR #194 removed the event-fallback backfill from the delivery path.
The remaining backfill-shaped writer is a legacy test fixture.
Production derives receipt-time fallback during a scoped read and
serves event facts already stored on older graphs.

Keep the mismatch fixture in this issue. The fallback reader parses
the manifest rows before deciding whether facts need derivation.
That row parser checks width and term ID shape. It does not by itself
certify that every row names the manifest owner. A focused corruption
probe needs to establish what the live read or retirement path actually
refuses. The original request for an open-time refusal remains
separate from that probe.

[Backfill removal](http://lillith:7555/fleet/sparrow/pulls/194) ·
[Current fallback read](http://lillith:7555/fleet/sparrow/src/commit/78d9adc/crates/sparrow-core/src/storage/rdf_view.rs#L527) ·
[Manifest row parser](http://lillith:7555/fleet/sparrow/src/commit/78d9adc/crates/sparrow-core/src/storage/rdf_index.rs#L831)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
