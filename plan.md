# #146 Keep status and ingest responsive while an online migration runs

## Behavior
As issue #146 (issue.md): status and the migration report do no full-store scan under the writer lock; counts come from counters maintained in the same batches as writes and checkpoints, read without the writer lock; the progress journal line uses the page's own counters. Waiting writers (ingest, consume publication, RPC writes) take the writer lock ahead of the next migration page; pages defer while writers are pending with a bounded deferral so the migration still progresses. The measurement scope applies the unit's Nice and SCOPE_LIMITS checks it.

## Dependencies
Main d912922 (#142: completed migration totals recorded). #36 migration framework.

## Exclusions
Pause/resume/throttle control (#144). Live-intake measurement mode (#145). Refusal counters (#143).

## Acceptance
Issue "Done when" 1-3. Item 3 (measurement under the #145 gate) runs after #145 lands; recorded as a dependency of the switch, not of this PR's merge.

## Risk
Writer-lock contention changes touch every write path; counters must stay exact under crash (same-batch). Hard failures on counter underflow.

## Decisions
- 2026-11-08: one image carries #142 + #146 + #144 + #145 (coordination D109).

- 2026-11-08: R1 (status during store-counters seeding) uses option A: additive capnp fields Status.countsPending @54 and MigrationStatus.totalPending @5 with a schema bump. Option B misleads anyone reading done/total as a percentage; option C (INDEX_NOT_READY) blanks status during the done-when 3 window.
- 2026-11-08: live_records keeps its documented deadline meaning (no ruling to change it); expiry retirement decrements the counter in its batch.

## User rulings

- 2026-11-08 05:20 CST (lillith_coordination, D118 amended, verbatim): "Coordination, D118 amended: option (1). The restart onto the #146 image waits for #145's gate measurement on a fresh copy of the schema-68 store; no schema-68 hotfix release branch (undone work by the rule, and a second code line); no ungated switch to 70 (the 22:42 switch already degraded the live daemon, R75). The failed nightlies are recorded as R77 with the exposure named: one day of records per failed night, live store intact, newest good nightly 20261107T100413Z. Order for you: #146 merges in the sweep with the retire command inside; then #145's measurement runs immediately on the fresh copy as the first thing after the sweep; coordination sets the switch window on its numbers and the restart follows under your procedure. Report #146's PR and merge commit, then the gate numbers."
- 2026-11-07 ~23:25 CST, D109 (relayed by lillith_coordination, verbatim): "The fixed image carries three REQUIRED items: #142's status fix; the rate and yield fix the diagnosis agent names (serialization on the writer lock, from the wchan samples with file and line); and a live migration control (pause, resume and a page-rate throttle over RPC or a re-read config) so a backfill can be held without a restart."
- 2026-11-07 (Patrick, relayed): "nothing is optional, deferred or a follow-up on your own authority; requests to cut count as failure and evasion; every claim of done carries the number that proves it"
