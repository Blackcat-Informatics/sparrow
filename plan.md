# Plan #31

Behavior: repository sparrow-backup accepts user-scope sparrow.service only with SPARROW_INTERIM_USER_PRODUCTION=1; installed user backup unit runs the repository helper (SPARROW_SCOPE=user, SPARROW_PREFIX=%h/.local, RuntimeDirectory=sparrow-backup, ExecStopPost marker %t/sparrow-backup/stopped).
Exclusions: no change to deploy/lib.sh guard or other deploy scripts; #22 cutover.
Acceptance: next nightly passes deploy/verify-backup.sh with exit 0, unedited SOURCE.
Risk: the guard protects production from test-instance runs; opt-in is set only by the production backup unit.

## User rulings
- 2026-11-06 (lillith_coordination, standing direction as Patrick): "The nightly helper's SOURCE lacking store_id and unique_payload_chunks is yours to fix in your own repository so a nightly verifies as taken, without a hand copy and without the NOT VERIFIED exit; do it by your plan, no vetting needed. Report when a nightly verifies clean."
