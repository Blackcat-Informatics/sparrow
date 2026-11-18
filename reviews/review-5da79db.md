# Review of #365 at 5da79db

- Head: 5da79db89062312d436fc07653e70504d468bb2e (branch paudley/365-show-the-archive-worker-s-phase)
- Range: `git diff 16dd364 5da79db` (one file, crates/sparrow-server/src/archive.rs, +14 -5)
- Written: 2026-11-18T21:27Z

## Scope

This recheck covers the fix for the one REQUIRED finding at 16dd364: the watermark-read
failure branch in `pass` dropped a clock error from the same pass. It also covers whether the
change is correct at the new head.

## Read

- The whole diff 16dd364..5da79db.
- crates/sparrow-server/src/archive.rs: `publish_position` (657-665), `Worker::start`,
  `fail_pass`, `with_clock_error`, `pass` and `run` (765-861), and the test
  `status_shows_a_refused_segment_as_holding_and_clears_when_a_pass_commits` (1268-1325).
- crates/sparrow-server/src/rpc.rs `now()` (80-83): it returns `Result<u64, sparrow_core::Error>`,
  which matches the helper's `clock` parameter type.

## Not read or run

- The rest of the #365 change outside this range. It was covered by the 16dd364 review.
- No cargo, Clippy, test, census or `make check` run (excluded by the brief). Compilation
  and Clippy at this head are not verified here. The commit hook (`make precommit`) is the
  claimed evidence; I did not inspect its log.

## Checks

1. Watermark branch parity. `publish_position` failure now builds
   `with_clock_error(error.to_string(), &clock)` and passes it to `fail_pass` with `at`.
   The store-id branch now builds `with_clock_error(format!("store id: {} {}", ..), &clock)`.
   Both branches append the clock error the same way, so a clock failure reaches
   `pass_failed` (status) and the returned cause, which `run` writes to stderr and `tracing`.
   Fixed.
2. Helper behaviour. `Ok(_)` returns `cause` moved through unchanged. `Err(clock)` returns
   `"{cause}; clock: {clock}"`. The store-id branch produced the same text before the change
   (`format!("{cause}; clock: {clock}")`), so its output is identical.
3. Other paths. `at` is still `clock.as_ref().ok().copied()`. The clock-only branch
   (`fail_pass(.., "clock: {error}", None)`), `drain`, `run` and `Worker::start` are
   unchanged. When the clock is Ok, the watermark branch's cause is the same as at 16dd364.
   The only behaviour that changes is the one the fix intends.
4. No `.unwrap()` or `.expect()` occurs in production code (lines 1-864, before
   `#[cfg(test)]`). The helper borrows the clock and does not consume it, so the later
   `match clock` still owns it.
5. Tests. The refused-segment test drives `drain` directly with explicit `now` values and
   never calls `pass`, so the diff does not affect it. It still asserts the `holding` phase,
   the `ARCHIVE_VERIFY` class, the segment name, the reason, the kept `since`, then recovery
   to `idle` with the failure cleared and the watermark advanced. The acceptance test still
   holds at this head (by reading; not run). No other test exercises `pass`'s early branches,
   so none can regress.
6. Text. The new doc comment is plain and accurate. It contains no quotations or attributed
   requirements. The commit subject describes the change.

Missing double-failure test: the clock comes from `crate::now()` and the store read has no
failure seam. A test would need a new injection seam for a diagnostic string on a path that
needs two independent failures in one pass. The acceptance does not require that behaviour
to be tested. The required refused-segment status test exists and still holds. This does not
block.

## Findings

- OPTIONAL: `with_clock_error` is a pure function returning a value. The repository rule puts
  `#[must_use]` on pure functions (archive.rs:812). This is not a contract issue.
- OPTIONAL: if the census treats a new private fn as a helper shape, `make check` will report
  it; `helpers-ledger.toml` was not changed. I did not verify this because no census was run.
  The pre-merge `make check` will settle it.

No REQUIRED findings.

VERDICT: PASS
