# Issue #356: Purpose section, glossary, and the cruft Sparrow receives

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

A repository's own documents are the input from which the picture of the whole is composed. Three things are read from each repository:

1. **A Purpose section** in the instructions file: what the repository is, its method, its consumers, its suppliers, and what it is not. It is the charter and changes only on explicit authorization.
2. **A glossary**: every term the repository uses, each naming the document that governs it.
3. **Its part of the order**: what it must have true before each step of the work starts and ends, or a link to the document that states it.

`lillith_memetics` main shows the shape: `AGENTS.md` ("Purpose", "How decisions and work are held", "Writing documents"), `docs/glossary.md`, and `docs/design/cutover-plan.md` ("The order"). The whole is at `/home/paudley/Active/lillith_coordination/docs/` (`overview.md`, `glossary.md`, `order.md`, `seams.md`).

## What is missing here

- `AGENTS.md` opens with one sentence of purpose and has no Purpose section naming consumers, suppliers and what Sparrow is not. `docs/architecture.md` holds the fuller statement.
- There is no glossary.
- `README.md`, `docs/architecture.md` and `docs/interfaces.md` do not mention cruft or memetics. Memetics sends cruft to Sparrow, the import's census reads a file's cruft fate from Sparrow, and Decodes searches it. `lillith_memetics/docs/design/cruft-spec.md`, section 6, states the retention it relies on: 30 days in the live store, then the archive, and nothing deleted that is not archived.
- `README.md` does not state the rule that decides between the two stores, which `docs/architecture.md` does.
- `docs/development-runtime.md` says mentation moved to memetics; memetics stores no mentation traces.

## Acceptance

- `AGENTS.md` has a Purpose section in the shape above.
- A glossary exists and each entry names its governing document.
- Sparrow's documents state what it receives as cruft, from whom, how a reader finds one item's fate, and the retention it guarantees, in agreement with memetics' cruft specification.
- The statement about mentation is corrected.


## Comments (1)

### paudley — 2026-11-17T17:39:58Z

A newcomer reading only this repository's documents finds two answers on the archive: `open-items.md` says it is not built and `archive-format.md` says it is implemented. The retention memetics' cruft specification relies on (30 days live, then the archive, nothing deleted that is not archived) depends on which is true. Acceptance for this issue therefore includes: one statement of whether the archive is built, in the document that governs it.

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
