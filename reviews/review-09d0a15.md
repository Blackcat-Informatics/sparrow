# Delivery review: PR #360 (issue #356), head 09d0a15

Reviewed head: `09d0a15b9278eb2aec38c5f5dd3ad21d09e69657` on
`paudley/356-purpose-section-glossary-and-the-cruft`. Worktree:
`/home/paudley/Active/sparrow/.worktrees/356-purpose-section-glossary-and-the-cruft`, clean
before and after. Review time: 2026-11-17 23:30 CST. This was a read-only recheck: no edits,
builds, tests or commits in the worktree. Scratch output went to `.stage/356/`:
`linkcheck-09d0a15.log`, `delta-added-09d0a15.txt` and `prose-check-09d0a15.log`.

## Scope

This recheck covers the delta `git diff ce84d04..09d0a15` (one commit, 5 files, 18 insertions
and 14 deletions, documentation only) and the open R5 finding from `review-ce84d04.md`. It also
covers one grep of the head tree (README.md, AGENTS.md, docs/*.md, deploy/) for retention stated
as a ceiling or as a promise that contradicts the shared statement. I judged the three places
the author left alone, and I re-read issue #356 (`stagectl issue 356`: body and one comment).

## Evidence (read, with lines)

- Delta, every edited line: README.md:74-81, docs/BRAND.md:40, docs/architecture.md:241-242
  and :288, docs/archive-format.md:8, docs/interfaces.md:65-68, :104-111 and :1543.
- Code behind each edited clause:
  - **Ingest refusal threshold:** `crates/sparrow-core/src/storage/pressure.rs:445-463`
    (`data_write_ceiling`: `disk_budget - reserve - estimate`) and `:466-500`
    (`admit_data_write` refuses with `STORAGE_PRESSURE`). `pressure.rs:525-538` and
    `storage/space.rs:90-130` show the free-space floor refusal, which is also a retryable
    `STORAGE_PRESSURE`. "At the budget less the reserve or below the floor" is accurate. It
    replaces the "at the budget" that O7 flagged.
  - **Deletion gate and no-archive behaviour:** `storage/archive.rs:311-326`. `archived()`
    returns `config.retire_without_archive` when the store has no archive policy, and
    `sparrow-server/src/config.rs:489` sets that to false. So the daemon retires nothing
    without an archive.
  - **Earlier retirement under pressure is limited to archived, unpinned items:**
    `storage/eviction.rs:314-332` builds the protected set (pins and journal holds).
    `:379-386` and `:411-454` skip protected entries and stop at `ArchiveLag` on the first
    unarchived one.
  - **Due at `received_at + 30 days`:** `storage.rs:97-98` `RETENTION_MS = 30 * DAY`.
    Retirement by expiry is in `expiry.rs`, which was cited and checked in the prior review
    and does not change in this delta.
  - **Searchable until the deadline (architecture.md:288, archive-format.md:8):** reads treat
    an unpinned item at or past `expires_at` as not live (`storage/publication.rs:334`,
    `storage/rdf_view.rs:1556`; architecture.md:336-338 states the eligibility rule). An
    unarchived item held past its deadline stays on disk but leaves queries, so "searchable
    until their 30-day deadline" is accurate.
- Head-tree grep:
  `git grep -n -i -E 'up to 30|at most 30|within 30|for 30 days|30 days|thirty days|30-day|held for|retention window|searchable window|day window|bounds age|no longer than|retained for|kept for'`,
  then `'searchable|queryable'`, over README.md, AGENTS.md, docs/*.md and deploy/. I read
  every hit about item retention in context. No hit states a ceiling. The hits in deploy/
  are unrelated ("kept for inspection" and "kept for the system").
- The three places left alone:
  - **interfaces.md:4527-4533 (retained window):** no contradiction. The window counts
    *eligible* items, and an unarchived item past its deadline is not eligible (see above).
    So "Active pins can extend this observed window beyond thirty days; early eviction can
    shorten it" is exact. The gate does not extend the observed window.
  - **interfaces.md pin and receipt windows** (:1566 retry receipts, :2746 and :2775
    terminal summaries, :3531-3533 and :3578-3582 receiver and stream windows): these
    deadlines apply to metadata, not to item retention. They state that pins do not renew
    them. They do not contradict the shared statement. interfaces.md:1036-1037
    (`Status.retentionSeconds`, "how long an unpinned record is retained after it is
    received") matches the query-eligibility meaning above, so it is not a false ceiling.
  - **architecture.md:50 ("one global 30-day retention policy"):** this names the policy,
    not a ceiling or a promise. The policy is one deadline for all classes. There is no
    contradiction.
- Links: `python3 -I -B .stage/356/linkcheck-71caf0e.py <worktree>` covers every link added in
  `origin/main...HEAD`. It resolved 46 of 46 files and anchors, reported `bad 0` and
  exited 0.
- `git grep -n -i -E 'patrick|ruling|ruled' 09d0a15` matches only AGENTS.md (:47-52, :60, :73,
  :149, "ruled out" at :209, and :270) and CITATION.cff:23. That is clean.
- Archive status: the delta's only hunk in archive-format.md is line 8.
  archive-format.md:36-50 is unchanged from ce84d04. Re-read, it is still accurate: the
  archive is implemented on `main`; the live daemon has no `[archive]` table and retires
  nothing; running it there depends on issue 352 and an authorized image switch.
- `prose-check --stdin` on the delta's added lines exited 0.

## Not read or not verified

- The live host's `server.toml` and the running daemon were not observed. "The live daemon
  has no `[archive]` table" rests on the documents and the shipped configuration.
- `expiry.rs` was not re-read. This delta adds no expiry claim beyond those the prior review
  checked against it.
- development-runtime.md, deployment.md, implementation-plan.md, turnover-qualification.md
  and load-qualification.md were read only at their grep hits. The memetics cruft spec was
  not re-read.
- No builds or tests were run. The `.expect` at `rdf_view.rs:1554`, seen in passing, is
  outside this documentation PR and was not evaluated.

## Prior findings

- **R5: fixed.** README.md:74-81 replaces "up to 30 days from server acceptance" with the
  shared statement: due at `received_at + 30 days` and retired then once archived; source
  timestamps do not decide expiry; pressure retires archived, unpinned items earlier; a pin
  keeps an item until the pin ends; the gate keeps unarchived items; with no archive nothing
  retires and ingest is refused with `STORAGE_PRESSURE` at the budget less the reserve or
  below the floor. Every clause matches the code cited above.
- **O5: fixed.** BRAND.md:40's example now reads "retired at 30 days from acceptance once
  archived", which is accurate for unpinned items.
- **O6: partly addressed.** README and interfaces.md carry the pressure clause. AGENTS.md:22-27
  and architecture.md:46-48 still leave it out. They are true as written and link to the full
  statement. This remains optional.
- **O7: fixed.** interfaces.md:65-68 and :110 name the reserve and the floor.
- The delta also removes "Thirty days bounds age" (architecture.md:241), which implied a
  ceiling.

## Findings

**O8 (OPTIONAL): interfaces.md:1543's last clause has the wrong subject and leaves out
query visibility.** "A pin or an unarchived item keeps it longer" makes the item keep itself.
It also does not say that a gate-held item has left queries. Suggested wording: "a pin keeps
it, searchable, until the pin ends; the deletion gate keeps an unarchived item's bytes past the
deadline."

**O9 (OPTIONAL): architecture.md:288-293 and archive-format.md:8-10 still frame archiving as
something done before deletion only when wanted.** "It can also archive ... before deleting
it", "Archiving is optional" and "Once a store has been fenced for archiving, it never deletes
unarchived evidence" are true conditionals. But they leave open that an unarchived store might
delete, and the daemon never does (`archive.rs:311-326`, `config.rs:489`). These lines were
there before this PR and were passed at ce84d04. Changing them is a clarity improvement, not a
correction.

No REQUIRED findings.

VERDICT: PASS
