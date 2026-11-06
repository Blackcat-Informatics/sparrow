# Validation: issue #31, interim user unit backup helper

VERDICT: PASS

## Scope
Merge scope: commits 712ee8d and c8bdca3 against base 13af50d (deploy/libexec/sparrow-backup guard opt-in, deploy/MANUAL.md). The live acceptance (install, then a nightly that verify-backup passes with exit 0 and an unedited SOURCE) is post-merge and keeps issue #31 open until it holds.

## Evidence
- Guard probe (runpy, systemctl_command): user scope with sparrow.service refused without the opt-in and with value 0; allowed with SPARROW_INTERIM_USER_PRODUCTION=1; system scope unchanged. The reviewer repeated it over eleven combinations.
- Full pre-commit gate (make check) passed on 712ee8d (/tmp/commit-31.log) and on c8bdca3 (/tmp/commit-31c.log). One earlier c8bdca3 attempt failed in consume_coverage_limit_and_root_replacement_fail_without_source_retirement (10 s deadline under parallel load; the change touches no Rust); the retry passed.
- reviews/review-1.md: VERDICT: PASS.

## Findings
None required. Optional O1 and O2 applied in c8bdca3; O3 install notes are followed at install; O4 (no automated guard test) accepted, as the helper has no test harness.
