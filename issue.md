# Issue #41: Stop source listings from charging other sources' recordless artifacts, or document the charge

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

Row B-16 of docs/open-items.md. Review round 2 of #24, finding O9: pr/26:review-r2.md:204-208, and the #26 pull request body, which says "O9 (optional, out of scope) is recorded". Marked by the implementer.

## Current behavior

A source listing folds and charges every recordless artifact in the window, whichever source owns it (reviewer text, pr/26:review-r2.md:204-208). A source that writes many zero-byte files makes other sources' listings return empty pages with cursors. `docs/interfaces.md:304-308` says a source page reads the recordless-artifact index (`empty-time:`) and "never reads or charges another source's records", which is silent on recordless artifacts of other sources. #14 does not name this. Unverified: the live count of recordless artifacts per source.

## Work

Either filter `empty-time:` rows by source before charging (this needs the owning source in the index key or a catalog read), or document the behavior in `docs/interfaces.md` beside the source-listing text and add a test that pins it. Choose one and record the reason in the pull request.

## Done when

A test named `source_listing_does_not_charge_other_sources_recordless_artifacts` seeds one source with many recordless artifacts and a second source with one record, lists the second source with a small `scan_budget`, and asserts the second source's artifact is on the first page. Today that first page is empty with a cursor, so the test fails. If the fix is documentation only, the done-when is instead that `docs/interfaces.md` states the charge and a test asserts the documented empty-page behavior.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
