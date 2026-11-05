# Validation: no-cow-store

VERDICT: PASS

## Scope
Branch no-cow-store, 7fc2886 and f18bc12. The review also covered the user-unit restart script ~/.local/state/sparrow-deliver/restart-schema67.sh.

## Evidence
- Review round 2: `VERDICT: PASS` (review.md). Round 1 was BLOCKED on B1 and S1, and both are fixed.
- Rehearsal on a reflink copy of the 2026-11-04 nightly (rehearsal.log): 336 files converted, 0 left with copy-on-write. verify-backup's checks passed on the converted copy.
- Scratch tests on btrfs: a store holding a stale socket converts with ACLs, modes, symlinks and the subdirectory kept.
- The commit hook (make check) passed on 7fc2886 and f18bc12, 432 tests each. shellcheck is clean.

## Findings
Optional and not done: pre-copying the store to cut downtime, an atomic swap, cheap refusals before the stop in restart.sh, and file names that contain a newline (these fail safe).
