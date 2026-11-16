# Delivery review: #321 / #322, archive prepare halving and segment default

Reviewed 2026-11-15 23:32 -0600. Read-only: no cargo, no builds, no edits to the worktree.

- Head: `3deecfdc15583af5b1a942cc162d0a9d37acd265` on `paudley/321-archive-prepare-halves-the-segment-on`
- Base: `9d33967`, range `git diff 9d33967 3deecfd` (9 files, +186 -12)
- Worktree: `/home/paudley/Active/sparrow/.worktrees/321-archive-prepare-halves-the-segment-on`

## Scope

Requirement as briefed: issues #321 and #322 (read verbatim with `tea`), plus the
operator-side ruling that the default segment is 10,000 records. The worktree has no
`plan.md` and no `## User rulings` section. I took the 10,000 ruling and the decision to
leave the daemon's status surface alone (it needs a wire change) from the brief. I did not
verify either against a recorded ruling. Note that #321 asks for a counter "visible in
archive status" and #322 asks for the worker to report it "in its status". The change
leaves both out on purpose, and the commit message says so.

Read:
- The whole diff.
- `archive_segment.rs`: `select`, `archive_prepare`, `build_segment`.
- `archive_encode.rs` in full (`write_gts`, `archive_read` with `lease_ms: 60_000`, `encode`).
- `rdf_view.rs` `with_rdf_scope_ids`.
- `rdf_view/scope.rs` `rdf_read_lease` (a `try_lock` that refuses at once with `RESOURCE_LIMIT`) and `with_rdf_scope`.
- `storage.rs` `lock`, `lock_page` and `lock_uncounted`.
- `archive.rs` `archive_intend` and `archive_range`.
- `sparrow-server/src/archive.rs`: `drain`, `write_segment`, `verify_segment` and the journal helpers.
- `sparrow-server/src/main.rs`, where the worker starts and how tracing is set up.
- `sparrow-archive/src/main.rs` and `restore.rs`.
- In `archive-format.md`: the header rulings, section 1, the phase paragraph, the intent paragraph, and sections 8 and 9.
- `interfaces.md`, lines 470 to 546.
- Evidence logs in `.stage/archive-prepare-halves-the-segment-on/logs/`: `commit.log` (header and test lines, `EXIT=0`, ends at `3deecfd`), `core-test.log`, `core-test2.log` and `core-test3.log` (earlier test drafts failed, the last passed), and `server-test.log`.

Not read: the probe logs cited in #321 (`.stage/probe-9d33967/`), every line of `commit.log`,
the bodies of the sparrow-server tests that `server-test.log` selected, PR comments (I found
no PR), and PurRDF sources.

## Evidence

- Hook run (`commit.log`, `EXIT=0`): fmt, release clippy with `-D warnings`, and release tests. At line 513 the new
  test `a_prepare_halved_by_an_encode_failure_reports_the_shortening_and_counts_it` passes
  in `tests/archive_segment.rs`.
- `core-test3.log`: the same test passes on its own in debug (`EXIT=0`). `server-test.log`
  shows 11 archive-filtered sparrow-server library tests passing (`EXIT=0`). Those tests do
  not exercise the shortened branch in `drain`. No test reaches it.
- `prose-check` on the added doc lines exits 0, with no findings. The input is
  `/home/paudley/Active/sparrow/.stage/archive/review-321-prose-input.txt`.

## Behaviour checked

1. **Halving is logged and counted.** Each halving now:
   - calls `note_archive_halving()`;
   - emits `tracing::warn!` with `code`, `message`, `entries`, `cap` and `elapsed_ms`;
   - records the first `(entries, code)` and then sets `cap`.

   The condition for halving has not changed (`entries > 1`, no intent in flight, code is
   not `UNSUPPORTED`). Once the range reaches one entry, or an intent is in flight, the
   unchanged arm returns `archive cannot encode {first}: …`.
2. **The shortening is reported.** `PreparedSegment.shortened: Option<Shortened>`
   (`selected`, `kept`, `code`) is set only on the success path after a halving.
   `build_segment` sets it to `None`, so a rebuild of an in-flight intent reports nothing,
   which is correct because the first pass already reported it. `drain` counts it in
   `Flight`, writes one `sparrowd: archive prepare shortened …` line and emits a tracing
   warning. No wire or stored-format change.
3. **No data is lost or stranded.** The intent records only the kept range, and the entries
   left out stay after the watermark. The test proves this: it commits, then the next
   prepare returns the other 5 entries, unshortened.

   The lower default cannot strand an intent recorded under 50,000. `archive_intend`
   returns `Ok` for an identical intent before `archive_range` checks the cap, and `select`
   ignores the cap when it rebuilds.
4. **The default is now 10,000.** `ARCHIVE_SEGMENT_RECORDS = 10_000` feeds
   `ArchivePolicy::default` and the server config default (`config.rs:157`).
   `archive-format.md` section 2 and the `interfaces.md` example now say 10,000. No other
   place states 50,000 as the default. The remaining matches for 50,000 belong to unrelated
   things: `load.rs`, `max_chunk_lines` in `rpc.rs`, `load-qualification.md` and `open-items.md`.
5. **Doc claims checked against the code:**
   - "GTS segment files through PurRDF's GTS writer": true. `write_gts` calls `purrdf::gts_write::to_writer`. Dropping "N-Quads manifests" is also correct, because no code writes `manifest.nq`.
   - "Archive reads give way to waiting writers (`lock_page`)": true of the selection batches and record-byte reads. The one scope capture uses `lock()`.
   - "hold the single RDF read slot": true. `with_rdf_scope_ids` takes `rdf_read_lease()`.
   - "60-second read lease": true (`archive_read` sets `lease_ms: 60_000`).
   - Phase paragraph: `Store::archive_prepare` exists. `sparrow_server::archive::Worker` is started by `sparrowd` when `config.archive` is set and the daemon is not query-only (`main.rs:375`). `sparrow-archive` has `list` and `verify` and a `restore` function.
   - "not installed or running on the live daemon": a claim about operations, which code cannot confirm.
6. **The test is deterministic.**
   - The reader thread takes the RDF slot before the prepare starts, so the first encode's `try_lock` is refused at once with `RESOURCE_LIMIT`. Nothing waits on a timer.
   - The halving warning is the hand-off point. The subscriber releases the slot there and waits until it is actually free, so the retry succeeds.
   - Time comes from the `now` argument, and the test has no sleeps.
   - The subscriber is thread-local (`with_default`), so warnings from other threads or from parallel tests cannot reach it.
   - If any earlier WARN fired on the test thread, the first encode would succeed and `expect("reports the shortening")` would fail every time, not intermittently. If the halving warning never fired, the retries would halve down to one entry and the `unwrap` would fail. Neither case hangs, because the channels close as the panic unwinds.

   The test therefore protects three things: the report, the counter and the remainder.

## Findings

1. **OPTIONAL: in the shipped daemon, the halving warnings go nowhere.** `sparrowd` installs
   no tracing subscriber; no file in `sparrow-server/src` uses `tracing_subscriber`. In
   production, the per-halving warning from core (message, elapsed time, each halving's own
   code) is dropped. The operator sees only the worker's stderr line, which has `selected`,
   `kept` and the first code. That is enough to recognize a lease expiry (`RESULT_EXPIRED`).
   It does not give the message or the time spent, which #321 cites as the missing
   diagnosis. This is the existing convention (core uses tracing, the worker writes the
   operator line), and the requirement as worded ("logged with structured tracing fields")
   is met. One fix would be to carry the first error's message in `Shortened` and print it.
2. **OPTIONAL: the bare `code=` field can mislead.** The field doc ("Code of the first encode
   error that caused a halving") is accurate. The operator line and the worker's warning
   print it as plain `code=`, though, and the later halvings' codes are only in the core
   warnings, which item 1 shows are dropped in the daemon. Suppose a lease expiry
   (`RESULT_EXPIRED`) is followed by a real encode failure at half size, and the quarter-size
   retry succeeds. The journal then shows only `code=RESULT_EXPIRED`. Fix: name the field
   `first_code`, or add the number of halvings or the last code.
3. **OPTIONAL: `Store::archive_halvings()` has no production caller.** It is a new `pub`
   counter that only the test reads. The worker keeps its own count of shortened prepares
   in `Flight` instead. The brief accepts that nothing is surfaced, but this leaves two
   counters for nearly the same thing and one of them unused. The worker could print
   `store.archive_halvings()` in its line.
4. **OPTIONAL: the test's `OnWarning` doc comment is wrong.** It says the subscriber runs
   `on_warning` "on the first warning" and ignores everything else. In fact it runs on
   every WARN event. A second warning on the test thread would panic on the closed channel.
   That failure would be deterministic, but the comment should say "on every warning (the
   test expects exactly one)".
5. **OPTIONAL: the architecture sentence is imprecise.** "A prepare that outlasts the
   60-second read lease is halved" has three gaps:
   - Only the RDF read inside the prepare is leased; the selection and byte reads before it are not.
   - A rebuild of an in-flight intent is not halved. It returns the error on every pass, which is behaviour that predates this change.
   - Halving happens on any error except `UNSUPPORTED`, not only on lease expiry.

   The new operator line is also not documented in `interfaces.md` next to the stall line.
6. **OPTIONAL: the phase paragraph could flag spec divergence.** "Implemented" now sits above
   a spec that the code does not follow in several places:
   - Section 1 (`YYYY/MM/DD` directories, `manifest.nq`, `STORE.nq`): the worker writes flat `{first}-{last}.gts` files under the store directory and binds to `STORE_ID`.
   - Section 8 (staging protocol).
   - Section 9 (manifest).
   - The header ruling that quarantines an entry that cannot be encoded: the code holds it and returns an error instead.

   This divergence predates the change, and the paragraph says what the brief asked for. A
   one-clause pointer to where the implementation departs from the spec would stop readers
   assuming the layout below is what is on disk.

Outside the scope of this change: when a query holds the RDF slot, the archive's encode is
refused with `RESOURCE_LIMIT` and halved straight away, because the slot uses `try_lock`. If
the query outlasts the retries, the prepare ends with "archive cannot encode <entry>" and the
"entry cannot be encoded" warning, even though the entry is fine. This behaviour was there
before. The change makes it visible but does not cause it.

None of these findings blocks the contract: no wrong behaviour, no false claim within the
briefed scope, and no lost or stranded data.

VERDICT: PASS
