Prepare the source tree for a first public GitHub release. Replace the
AGPL census with the pinned RAF tool and preserve the helper policy in a
canonical Turtle ledger. Generalize the deployment examples, add a disposable
quickstart, and configure dependency checks and GitHub Actions.

The staged installer stays within its destination and its CLI link survives
relocation. The release archive reconstructs the exact tested Git tree.

## Verification

- Final head `0e6f79eaaaeadca1d54e46413a3240819be16c25` produces the
  same tree as the previewed merge, `ebc8e6cabb4b01d77935fbed7910121c9810cd03`.
- `make check` exited 0 with 839 release tests, 62 RAF self-tests and the
  pinned PurRDF scan. Two release tests were ignored.
- `cargo deny --locked check` and the release binary build exited 0.
- The quickstart durably imported a record and returned its exact record
  and artifact identifiers. The disposable installer rehearsal and the
  AppArmor and systemd checks passed.
- Secret, private-marker and relative-link scans found no issues in the
  release archive.
- The installer refused an incompatible layout before changing any files.
  The fresh, relocated and repeated staging cases passed on the final head.

Closes #459.
