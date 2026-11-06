Closes #31.

The nightly backups under the interim user unit ran an older hand-installed helper whose SOURCE lacks `store_id` and `unique_payload_chunks`, so `deploy/verify-backup.sh` refused them. The repository helper records both, but refused the user-scope `sparrow.service`.

- `deploy/libexec/sparrow-backup` accepts the user-scope `sparrow.service` only when `SPARROW_INTERIM_USER_PRODUCTION=1`, and refuses it otherwise, so test-instance runs stay off production. `deploy/lib.sh` keeps its refusal.
- `deploy/MANUAL.md` describes the interim backup unit: the three variables, `RuntimeDirectory=sparrow-backup` and the `%t/sparrow-backup/stopped` marker.

Checks: guard probe over the opt-in values; full pre-commit gate on both commits. One run of the second commit hit a timing flake in `consume_coverage_limit_and_root_replacement_fail_without_source_retirement` (10 s deadline under parallel load); the retry passed. Independent review: PASS (review-1.md), optional O1 and O2 applied.

After merge: install the helper and the user unit; the acceptance is the next nightly verifying with exit 0 and an unedited SOURCE.
