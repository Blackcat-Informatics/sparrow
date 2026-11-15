# Issue #285: Close the duplicate-part read: a part sent again after its receipt lapses reads twice

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

REQUIRED, ordered first after PR 284 and before 282 and 283 (coordination ruling, issue 280). Case: a set part published as consumeset:{H}.part{k} whose receipt has lapsed (two-day window) is published again as a new artifact, so a records query over the source returns its bytes twice (the old artifact stays readable until retention). It needs a crash or an outage longer than the window in the middle of a set. It loses no data, but a reader sees duplicates. Options for the close, to weigh: the manifest's receipt is already kept for the life of the claim; keeping each part's receipt the same way would make a part sent again return its saved receipt, with no stored-format change; a cut recorded when the set begins cannot be changed under the same part ids by a later change of limits. Acceptance: a test that a lapsed part is not published twice, and a test that a reader sees each byte once. The docs sentence and the pinning test in PR 284 link here. Refs #280.

## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
