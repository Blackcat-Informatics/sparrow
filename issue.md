# Issue #359: Remove attributed rulings and quotations from the repository

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Remove every quotation, ruling attribution and verbatim citation of Patrick or of coordination from tracked repository files. Each passage keeps the technical requirement it states, written directly in present tense.

Acceptance: `git grep -i -E 'patrick|ruling|ruled'` on main finds nothing outside AGENTS.md (Patrick's own rules file) and CITATION.cff (author name); no dated quotation of a decision remains in docs, deploy files, code or tests. The superseded ledgers docs/open-items.md and docs/status-2026-11-10.md are removed.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
