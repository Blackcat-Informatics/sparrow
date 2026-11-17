# Issue #324: Archive prepare reporting follow-ups: daemon has no tracing subscriber, first_code wording, status field

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Optional findings from the independent review of PR for #321/#322 (head 3deecfd, `.stage/evidence-keep/321/review-321.md`), none blocking:

1. `sparrowd` installs no tracing subscriber, so the structured halving warnings (message, elapsed time, every code) are dropped in the daemon; only the worker's stderr line (selected, kept, first code) reaches the journal.
2. `Shortened.code` is the first halving's code; a later, different code is visible only in the lost warnings. Name it `first_code` or add the halving count.
3. `Store::archive_halvings()` is used only by the test; the worker keeps its own count.
4. Test comment on `OnWarning` says "first warning"; it runs on every warning.
5. architecture 4.4 sentence is loose: only the RDF read is leased, a rebuild of an in-flight segment is not halved, and any error but `UNSUPPORTED` halves. `interfaces.md` does not document the new operator line.
6. archive-format.md describes a layout (date directories, manifest.nq, STORE.nq, quarantine) that the code does not write (flat `{first}-{last}.gts`, bad entry held). Predates this change.
7. When a query holds the RDF read slot, the encode is refused and halved immediately; if the query outlasts the retries the prepare ends with "archive cannot encode <entry>" for a fine entry (related to #319).
8. The shortened count is not in daemon status: needs a field after the last wire ordinal.


## Comments (1)

### paudley — 2026-11-17T00:18:02Z

Items 1, 2, 3, 4, 5 and 8 merged as 9ed5943 (PR 346). Items 6 (archive-format.md describes a layout the code does not write) and 7 (a query holding the RDF read slot can halve a fine entry) remain open.

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
