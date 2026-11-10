# Validation: issue 184, PR #185

VERDICT: PASS

## Scope

Head 4954964431ca0af4937103c17dc895048cf8abbb on paudley/184-replace-or-slow-the-sub-second-ticks-50,
base c82bfa4. Inventory rows 1 to 6 of #184.

## Evidence

- Commit hook (`make check`: fmt, release Clippy with warnings denied, release workspace
  tests, census check), each EXIT=0:
  - cb644d4: /home/paudley/Active/sparrow/.stage/184/hook1.log (775 seconds; more than five
    minutes, so commits follow review).
  - ca4c9c2: /home/paudley/Active/sparrow/.stage/184/hook2.log.
  - 4954964: /home/paudley/Active/sparrow/.stage/184/hook3.log.
- Focused: `cargo test --release -p sparrow-server --lib watches::`, 13 passed
  (/home/paudley/Active/sparrow/.stage/184/test3.log, EXIT=0).
- Review 1 at cb644d4: /home/paudley/Active/sparrow/.stage/184/review-1.md, VERDICT: BLOCKED
  (one required, two optional findings; all fixed in ca4c9c2).
- Review 2 at ca4c9c2: /home/paudley/Active/sparrow/.stage/184/review-2.md, VERDICT: PASS
  (two optional findings: this index, and a test for the pre-wait drain, added in 4954964).

- Review 3 at 4954964: /home/paudley/Active/sparrow/.stage/184/review-3.md, VERDICT: PASS,
  findings none.

## Findings

none
