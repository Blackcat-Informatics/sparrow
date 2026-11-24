# Issue #50: Limit stopped no-copy-on-write conversion to the changed files

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

The converter copies the whole store after the daemon stops. Reduce the
stopped interval by copying into a separate no-copy-on-write sibling before
the stop, then reconcile only the files that changed. The first copy can
race live writes and file replacement. Treat the closed store as authority
for additions, changes and deletions.

Keep the pre-stop prerequisite checks, byte and filename verification, atomic
exchange and retained original from #47 and #48. The existing regression
covers names containing newlines. [Converter change](http://lillith:7555/fleet/sparrow/pulls/553)

On a scratch fixture, modify, add and remove files after the first copy.
Verify that the second pass copies only changed or added files, removes
deleted files from the staged sibling and produces an exact copy of the
closed store. Record the stopped interval and the copied files or bytes in
each pass.

Source is row B-25 of `docs/open-items.md` and the converter review in PR #29.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
