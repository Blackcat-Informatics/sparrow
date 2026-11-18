# Delivery review: PR #361 (issue #359)

Head: `e26ce07e989ccc07a988f6de7c62440cee3e131c` on `paudley/359-remove-attributed-rulings-and`.
Base: `origin/main` `ec9e0c6`. Worktree: `/home/paudley/Active/sparrow/.worktrees/359-remove-attributed-rulings-and`.
The review was read-only. Nothing was built, edited or committed in the worktree.

## Scope

Issue #359 (`.stage/rulings/issue.md`) asks for three things. First, remove every quotation,
ruling attribution and verbatim citation of Patrick or of coordination from tracked files,
keeping each technical requirement and stating it directly in present tense. Second,
`git grep -i -E 'patrick|ruling|ruled'` must find nothing outside AGENTS.md and CITATION.cff,
and no dated quotation of a decision may remain. Third, `docs/open-items.md` and
`docs/status-2026-11-10.md` are removed. AGENTS.md is out of scope.

## What I read

- `.stage/rulings/issue.md`, `.stage/rulings/pr.md`, and the tail of `.stage/rulings/commit.log`
  (hook `EXIT=0` at e26ce07).
- The full `git diff ec9e0c6..e26ce07` for all 14 modified files: storage.rs (hunks at 6310,
  6397, 7391, 25794, 25879, 25900, 25914), pressure_duty.rs:555, rpc.rs:14939 and 15007,
  deploy/README.md:35-70, deploy/RECOVERY.md:287-299, deploy/evidence/rehearsal.md:147,
  deploy/lib.sh:341, deploy/user-unit-upgrade.sh:12, docs/architecture.md:544,
  docs/archive-format.md:11-30, docs/development-runtime.md:23-56 and 146,
  docs/interfaces.md:365, 666 and 2881, docs/load-qualification.md:58, 431 and 443, and
  registry/compatibility.toml:53.
- Head context: deploy/README.md:36-70 and 355-390, docs/deployment.md:540-552,
  docs/development-runtime.md:1-30, 186-192, 248-253, 1333-1382 and 4966-4975,
  deploy/evidence/rehearsal.md:36-45, and the heads of both deleted files at ec9e0c6.
- `.stage/issue-280/` design notes, to confirm what "ruling A" referred to.

## What I did not read

- The full bodies of the two deleted files beyond their first lines. They are removed as
  the issue directs, so their content was not audited for requirements that exist nowhere else.
- Forge issues #280, #285 and #218, and the PR comments. No `pr-comments.md` was supplied.
- Untracked `.stage/` content in general, and AGENTS.md (out of scope).

## Evidence

- `git grep -n -i -E 'patrick|ruling|ruled' e26ce07 -- . ':!AGENTS.md' ':!CITATION.cff'`
  returns no matches (exit 1).
- `git ls-tree e26ce07 docs/` lists neither deleted file. `git grep -E 'open-items|status-2026-11-10'`
  and `git grep -i 'open items'` return no matches (exit 1), so no links to them remain.
  No reference to a `#rulings` anchor remains either.
- `git diff -U0 ec9e0c6..e26ce07 -- '*.rs' '*.sh'`: every added or removed line begins
  with `//` or `#`. The edits to code are comment-only.
- A dated-quotation pattern (`YYYY-MM-DD` followed by a quote) matches only protocol-version
  constants and `date =` fields in registry/compatibility.toml.
- A broad grep of docs, deploy, the top-level `.md` files and the registry for directed,
  direction, decided, decision, approved, approval, asked, said, instructed, coordination,
  lillith, verbatim, quote, his/him and agreed. Every hit was judged. None quotes Patrick
  or coordination, and none attributes a decision to either by name. The remaining
  "coordination" hits are a current process rule: restarts are approved by the coordination
  session (deploy/README.md:63 and 363, docs/deployment.md:547-548). That is an
  operating requirement, not a citation of a ruling.
- `prose-check --stdin` on every added line of the diff: exit 0.

### Requirement retention (spot checks)

- No backup to a local disk: kept (deploy/README.md, "never to a local disk; no Sparrow
  backup is ever written to a local disk").
- Store at `/SPARROW`: kept (README "Deployment requirements", RECOVERY.md:296).
- Copy-on-write off: kept (README Layout, deploy/lib.sh:344).
- Ambient capability for port 518: kept (RECOVERY.md:290, plus the README bullet "The unit
  grants the bind capability as an ambient capability").
- Kept as well: the system unit running as the `sparrow` user, no per-artifact limit
  (archive-format.md and interfaces.md), the exact `=4` purrdf pin with no patch form, the
  full Unix-socket capability, store-format changes following the #18 online-migration
  criterion, and no daemon-side UDP spool.
- "as ruling A says" became "as #280 requires". `.stage/issue-280/` confirms that ruling A
  belongs to the #280 consume-split design, so the new reference is accurate.

## Findings

No REQUIRED findings.

OPTIONAL (not blocking):

1. deploy/user-unit-upgrade.sh:15 now reads "Run it only when the restart is authorized".
   docs/deployment.md:547-548 still says "only on the coordination session's go", and
   deploy/README.md:63 names the coordination session as the approver. The script is now
   less specific than the docs. Both wordings are permitted, but they disagree.
2. crates/sparrow-server/tests/rpc.rs:14942 and 15010 cite "(#285, condition 1)" and
   "(#285, condition 2, ...)". The word "condition" no longer has a referent in the tree.
   The doc comments state the invariant in full, so nothing is lost.
3. docs/development-runtime.md:26 changes "On 2026-10-27 the unit gained" to "The unit sets".
   The date only fixed when the limit was added; it was not a quotation. Dropping it moves a
   history log entry into present tense. The fact still holds (deploy/systemd/sparrow.service:58-59).
   At line 55, the clause "Mentation has moved to memetics" was dropped along with the
   attribution, which loses a small fact about where that producer went.
4. deploy/README.md's backup bullet now says "never to a local disk" twice in two forms, and
   the edited line runs past the surrounding wrap width. This is cosmetic only.
5. deploy/evidence/rehearsal.md:150 sits under "## The report, verbatim". Its heading
   reference was updated from "Rulings" to "Deployment requirements", so the quoted report
   is no longer word for word. The acceptance grep requires the change; a one-line note on
   the edit would keep the "verbatim" label true.
6. Unattributed historical approvals remain: development-runtime.md:1333, 1360 and 1379
   ("After explicit approval", "approved recovery"), and line 4971 ("the transport decision
   on 2026-10-06"). None names a person or coordination, and none is a quotation, so they
   are outside the acceptance criteria.

VERDICT: PASS
