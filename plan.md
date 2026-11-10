# #176 plan: source-summary run order, listing build re-base, per-migration timing

Design: `/home/paudley/Active/sparrow/.stage/176/design.md` (option b with c3; option a not built).

## Behavior

1. The maintenance worker runs `source-summary` directly after `store-counters`
   (its own run order). `Migration::ALL` stays the status and report order. No
   writer-format fence, no wire or schema change. A store partway through under
   either order resumes from its own checkpoints.
2. A running listing build (`sources` capture or live `artifacts` owed window)
   re-bases onto a fresh snapshot at its 4,096-entry check once the live
   migration has overtaken it: the migration is complete, or it has passed more
   entries since the build's snapshot than the build has read plus a lead of
   4,096 entries, in the migration's own unit. Each re-base restarts the owed pass from the newer checkpoint; the
   build keeps its identity and cursor, and its `read`/`total` progress
   restarts from the new snapshot.
3. Per-migration timing in the daemon's journal lines: a `start` line at a
   migration's first page of the session, and `elapsed_seconds` on progress and
   completion lines; documented.

## Dependencies

None outside the repository. The probe `measure_158.rs` is not in the tree, so
timing goes into the journal lines (coordination brief).

## Exclusions

- Option (a), maintained owed-completion rows.
- The 2.67M-record measurement run (needs coordination's go-ahead).
- Larger pages, concurrent migrations, source-context priority (#183).

## Acceptance (restated, coordination comment 2026-11-10 06:02)

1. source-summary is the first migration the worker runs after store-counters
   seeding, with a test.
2. A listing build started during the migration delivers rows no later than one
   4,096-entry check after the migration completes, and its rows equal the
   finished listing's, with a test (sources and live artifacts). The re-base
   neither loses nor duplicates rows.
3. After the migration completes, listings read rows directly (already tested).
4. Per-migration timings (start, end, pages, done) are recorded; the 2.67M run
   waits for coordination.
5. A store with source-summary done and others pending resumes correctly, and a
   store partway under the old order resumes correctly under the new one.

## Risk

R1 source-summary's share of migration time unmeasured; R2 owner-graph and
fallback pending paths last longer; R3 re-base churn bounded by one check
interval of lead; R4 `build.read` can restart lower (documented).

## User rulings

- 2026-11-10 (coordination brief to this session, lillith_coordination speaking
  for Patrick): "BUILD exactly the recommendation: (b) the maintenance worker
  runs `source-summary` directly after `store-counters` (its own run order;
  `Migration::ALL` stays the status order; NO writer-format fence, NO
  wire/schema change; a store partway through migrating must resume correctly
  under either order: test it), together with c3, the build re-base (a running
  listing build moves to a fresh snapshot once the migration overtakes it, so a
  waiting listing is capped at the migration's own end and redundant passes stop
  competing with it for the one CPU), and the per-migration timing in the probe
  (...if the probe is not in the tree, record per-migration start/end/pages/done
  in the migration journal log lines the daemon already prints (`sparrowd:
  migration NAME progress/complete ...`) and document how to read them; do NOT
  run the 2.67M-record measurement: it needs coordination's go-ahead). Do NOT
  build option (a)."
- 2026-11-10 (same brief): "New code follows Patrick's rule: no new loop or
  timer firing more than once a second (events or waits of a second or
  longer)."
- 2026-11-10 (same brief): land with `Refs #176`, NOT Closes: "the 2.67M
  per-migration measurement remains".
