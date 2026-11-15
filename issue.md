# Issue #305: Archive worker follow-ups: retry determinism and stall reporting before install; text and test notes

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Optional notes from the reviews of the archive worker (PR #302, merged as 3c427fb). None blocked the merge. Sources in the main checkout: `.stage/archive/review-pr302-worker.md`, `.stage/archive/review-pr302-worker-r2.md` and `.stage/archive/primary-read-pr302.md`.

## Check before the archive install

1. **A retried segment must encode byte-equal (review O2).** A file already at a segment's name is never replaced; it is verified. If a crash lands after the rename and before the commit, the next pass re-encodes the segment. If those bytes differ, every pass then fails verification and the watermark sticks until an operator moves the file. Nothing is lost, and the cause is journaled. What I read: PurRDF's GTS writer (purrdf-gts 4.0.2) uses randomness only when signing or encrypting, which the encoder does not do, and it reads no clock. That suggests the re-encode is byte-equal, but this is an inference and was not run. Prove it with a test that prepares the same segment twice and compares the bytes. Otherwise, give the worker a way out that loses nothing.
2. **Stall reporting (review O4).** `stall_warn_secs` and `io_timeout_secs` are validated but nothing reads them. A hung NFS mount stalls the archive thread with no stall line in the journal, while the hold grows behind it. Either make both settings act, or remove them along with the text that describes them.

## Text and operator guidance

3. **R2-O1.** The archive refuses until something writes `STORE_ID`. A fresh store waits for its first backup. If `[archive] dir` is not the backup root, nothing ever writes the file. The refusal hint asks only "is the archive mounted?". Broaden the hint, and say it in `deploy/README.md`.
4. **O3.** The module doc says files go under `<root>/<store_id>/<segment>/`; they go directly under `<store_id>/` as `<from>-<through>.gts`. `HoldReason::ProjectionFailed` also holds, and `journal_held` omits the reason. The pressure test's doc says status "no longer reports a hold", but the test does not assert `archive_held == false`.
5. **R2-O3.** A few lines in `docs/interfaces.md` and in a doc comment in `archive.rs` run past the wrap width.

## Tests

6. **O5.** The new pressure test waits with `tokio::time::sleep` (`tests/rpc.rs`, around 4219), against the rule that tests do not sleep to wait. Forty existing uses in `rpc.rs` share the pattern; fix this one, and leave the rest to their own change.
7. **R2-O2.** The other-store case could also assert that the root holds only `STORE_ID`.

Acceptance: items 1 and 2 are closed before the archive install; the rest in one change, with the commit hook passing.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
