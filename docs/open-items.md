# Open items inventory

Date: 2026-11-07. Main at eedb484.

This document lists every item in Sparrow's issues and delivery notes that was marked optional, later, follow-up, deferred, waiting, out of scope, excluded, not done, not run, unverified, accepted risk or known flake. Each row gives the item's real state at main and the issue or commit that lands it.

Patrick's ruling, relayed 2026-11-07 by lillith_coordination: "nothing is optional, deferred or a follow-up on your own authority; requests to cut count as failure and evasion; every claim of done carries the number that proves it."

## Sources

- Forgejo issues and pull requests 1 to 36 in fleet/sparrow, with comments, fetched 2026-11-07. Cited as `#N body` or `#N comment <id>`.
- Archived stage notes stored by ghprsq. Cited as `pr/N:<file>:<line>`, which is `refs/ghprsq/pr/N/stage:<file>` in this repository.
- Delivery notes. Cited as `SD/<file>:<line>`, where SD is `~/.local/state/sparrow-deliver/`. Squash notes are in `~/.claude/squashes/sparrow-*.txt`.
- The uncommitted #36 step 2 notes. Cited as `W36/<file>:<line>`, where W36 is `.worktrees/36-bound-the-schema-69-first-open/.stage/bound-the-schema-69-first-open/`. They were read at about 12:10 CST, and another agent is still editing that worktree.
- The #1 stage notes. Cited as `S1/<file>:<line>`, where S1 is `.stage/authenticate-the-public-tcp-listener/` in the main checkout.
- Repository text at eedb484: `docs/`, `crates/`, `deploy/` and `helpers-ledger.toml`.

Real state was checked read-only by reading code, docs and git history at eedb484. No build, test or live-service command was run for this document. "Unverified" means the claim was not checked.

## Who marked an item

- **Ruling:** Patrick or lillith_coordination made the deferral or acceptance. Each such row quotes the ruling verbatim, with the source of the quote.
- **Agent:** an implementer, a reviewer or the delivery session made the call on its own authority. The 2026-11-07 ruling targets these rows.

Under "Lands in", a number marks the issue that lands the item. Rows that read "needs issue" in the first version now carry the issue filed for them.

Issue numbers were added on 2026-11-07 under the coordination ruling: "every one of the 82 agent-marked rows with no covering issue gets an issue (REQUIRED, with a falsifiable done-when), and the doc gains the issue number per row". Under the same ruling, C-23, D-2 and D-4 became builds with issues, and A-11, A-16, A-22, A-24, C-22 and F-10 were corrected to the exact words of their rulings.

## (A) Open issues and their unmet acceptance

| Id | Item | Source | Marked by | Real state | Lands in |
| --- | --- | --- | --- | --- | --- |
| A-1 | #5 v2 disk format: all work and UAT unmet. The JSON costs (catalog codec, deep-JSON projection, expansion) are "parked". | #5 body; comments 3562, 3936, 4006 | Agent. The comments cite a 2026-10-27 ruling, but its verbatim text is not in the issue (unverified). | Not started. It depends on #13 and ships with #18. | #5 |
| A-2 | #6 producer-window registry: all work and UAT unmet | #6 body; SD/STATE.md:93 | Agent: "Not started" | Not started | #6 |
| A-3 | #7 lossless-confirmed spool: all work and UAT unmet | #7 body; SD/STATE.md:93 | Agent: "Not started" | Not started | #7 |
| A-4 | #8 lossy-aspirational mode: all work and UAT unmet | #8 body; SD/STATE.md:93 | Agent: "Not started" | Not started | #8 |
| A-5 | #9 producer profile contract: all work and UAT unmet | #9 body; SD/STATE.md:93 | Agent: "Not started" | Not started. No `producerProfiles` or `sparrow-producer/1` exists in crates or docs. | #9 |
| A-6 | #11 journal forwarding: all work and UAT unmet | #11 body; SD/STATE.md:93 | Agent: "Not started" | Not started. It depends on #7. | #11 |
| A-7 | #12 `./bin` and seeded builds: UAT steps 1 to 4 not recorded | #12 body, which has no comments | Agent: issue left open with no UAT comment | The code is on main (ba6ebe7: `make install-bin`, `.config/wt.toml`, `seed-excludes-check`), but no UAT run is posted. | #12 |
| A-8 | #13 scoped SPARQL: UAT steps 0 to 4 not run | #13 body; PR #35 body; pr/35:validation.md:12 | Agent: "The issue stays open for UAT after the bundled deploy" | Merged in 9e71b63 (main 758b41a). The UAT needs the schema 69 switch, which is gated on #36. | #13, after #36 |
| A-9 | #13 UAT step 2 amended: `?r a sp:Record` replaces the `sp:byteLength` query, and the artifact byte-length check is called "optional" | #13 comment 7283; pr/35:reviews/review-1.md:107-113 | Agent: the review finding and the amendment, both posted by agents. "Optional" is an agent call. | Still true: records carry no `sp:byteLength`, and `docs/interfaces.md:1571-1572` writes it for artifacts only. The amended step has not run. | #13 |
| A-10 | #13 UAT addition: the `sp:peerUid` SPARQL check moved from #10 | #13 comment 7161; #10 comments 7159, 7163 | Ruling, coordination 2026-11-06 19:45 CST: "(a): close #10 now ... add the sp:peerUid SPARQL check to #13's UAT" (SD/STATE.md:128) | Not run. Same deploy gate as A-8. | #13 |
| A-11 | #14 `sources` and `artifacts` at any size: all work and UAT unmet | #14 body; SD/STATE.md:131 | Ruling, coordination 2026-11-07 06:3x CST: "#14 waits." This is sequencing, not a hold: #14 is a build after #36. The agent's own 11:15 CST report says: "Its only blocker is my sequencing, behind #36." | Not started | #14 |
| A-12 | #14 body cites line numbers that went stale after #24 merged | SD/STATE.md:89 | Agent: "Line references to refresh after #24 merges" | Still stale: `artifact_read.rs:397` and `storage.rs:997-1031` no longer hold the cited scans. | #14 |
| A-13 | #15 archive segment encoder: all work and UAT unmet | #15 body | Agent: "Not started" | Not started. No `archive_prepare` exists in crates. | #15 |
| A-14 | #16 NAS archive worker: all work and UAT unmet | #16 body | Agent: "Not started" | Not started. It depends on #15. | #16 |
| A-15 | #17 offline archive readers: all work and UAT unmet | #17 body | Agent: "Not started" | Not started. No `sparrow-archive` tool exists. It depends on #16. | #17 |
| A-16 | #18 online v2 migration: all work and UAT unmet | #18 body; comment 7426 | Ruling, Patrick 2026-11-07 06:0x CST via coordination: "the #20 metadata-graph rewrite plus the #13 index build belong under it" (SD/STATE.md:134). This is a direction to build: #18's remaining work is a build. | Not started. #36 was filed as part of it. | #18 |
| A-17 | #20 receipt-time fallback: UAT not run | #20 comment 7241; pr/34:validation.md:10 | Agent: "Open for the live acceptance run" | Merged in 8e7a191. Live is schema 68, so the UAT waits for the schema 69 switch. | #20, after #36 |
| A-18 | #22 system unit: the root steps, the cutover and the UAT are unmet | #22 body; SD/STATE.md:88 | Agent: "wait for Patrick's window". The trigger is named, but no date is set. | The deploy code is merged at 0221e87. Production still runs the user unit. | #22 |
| A-19 | #22: AppArmor profile `deploy/apparmor/sparrow` was parsed but never loaded or run in enforce mode | deploy/evidence/apparmor.md:95-112 | Agent: "not yet loaded", "Load and live checks, not run" | Still true | #22 (steps 6, 8 and 12) |
| A-20 | #22: `uninstall.sh` and the cutover revert never ran on a host | #22 body; deploy/RECOVERY.md:565-569 | Agent: "Unverified" | Still unverified. `uninstall.sh` has run in staged mode only. | #22 |
| A-21 | #22 rehearsal G8: the README step 7 commands and intake consumption were not verified against the system unit | deploy/evidence/rehearsal.md:118-123 | Agent: "unverified by me" | Unverified | #22 |
| A-22 | #23 collision-free test ports: all work unmet | #23 body | Ruling, coordination 2026-11-06 19:45 CST: "Your order becomes #20, #13, #14; #23 after." (SD/STATE.md:129). This is sequencing, not a hold: #23 is a build. | Not started. `NEXT_PORT` still starts at 10240 (`crates/sparrow-server/tests/rpc.rs:465`). | #23 |
| A-23 | #25 query-only and verify-backup: UAT steps 0 to 3 not posted on #25. Steps 0 to 2 were moved to the #22 cutover. | #25 body; SD/STATE.md:26 | Agent: "Coordination was told". No verbatim ruling is recorded. | Merged in 6737fac. #31 comment 7364 shows a nightly passing verify-backup with exit 0, and #19 comment 6286 used `--image --time-records`. Neither is posted on #25. #31 made the repository helper run under the user unit, so the reason for the move may no longer hold (unverified). | #25 |
| A-24 | #36 acceptance: under CPUQuota=100%, IOWeight=20 and MemoryHigh=2G, `status` answers within 10 minutes and the backfill finishes, with both times recorded | #36 body; W36/plan.md:14-15; W36/reviews/review-2.md:35; W36/implementation.md:215-219 | Ruling, coordination 2026-11-07 06:3x CST, sets the bar: "if over, build the online duty as you designed". The step 2 implementer listed the run "Not done" and called it "Patrick's or the coordinator's to schedule", which was an agent hand-off. The delivery session runs it, gated only on root free space of 50 GB (the 11:5x ruling). | Not run. Step 1 (9c91a97) measured 34 min, 07:49:12 to 08:23:37 CST (pr/37:validation.md:9-14). The verify-backup writable mode required by the 11:5x ruling is not written yet. | #36 |
| A-25 | #36 step 2 online migration: uncommitted, and review 2 is BLOCKED | W36/reviews/review-2.md:33-57; W36/implementation.md:229-257 | Agent: "Nothing is committed" | 23 tracked files changed, plus an untracked `migration.rs`. The implementer reports R2 to R4 fixed, and the full sparrow-server tests passed (EXIT=0) at 12:08 CST. No review 3 exists, the hook has not run and there is no PR. | #36 |
| A-26 | #36: attribute the remaining 30 minutes of the 34-minute open by profiling a nightly copy under unit limits | W36/plan.md (Behavior); W36/implementation.md:190-219 | Agent: "the 30-minute attribution remains an inference" | Not measured. The only evidence is a warm, unlimited synthetic probe of 50,000 records. | #36 |

## (B) Review OPTIONAL findings not taken

Every row in this group is agent-marked: a reviewer labelled the finding OPTIONAL, and the implementer or the delivery session did not act on it.

| Id | Item | Source | Marked by | Real state | Lands in |
| --- | --- | --- | --- | --- | --- |
| B-1 | #13: the parity test never checks a strict subset scope | pr/35:reviews/review-1.md:114-120 | Agent: review OPTIONAL | Still true. `scoped_sparql_answers_match_the_store_wide_plan` (`crates/sparrow-core/tests/storage.rs:18808`) compares only scopes equal to the whole fixture. | #39 |
| B-2 | #13: the malformed-key test fires on the first row, so page atomicity is untested | pr/35:reviews/review-1.md:121-124 | Agent: review OPTIONAL | Still true. `graph_key(0, 5)` sorts first (`rdf_index.rs:1322`). | #49 |
| B-3 | #13: a crash followed by a downgrade can leave stale `rdf:o:` rows, and an empty graph is listed | pr/35:reviews/review-1.md:125-131 | Agent: review OPTIONAL | Still true. The docs carry no note and the job has no subset check. Whether #36 changes the job order is unverified. | #51 |
| B-4 | #20 O1: the fallback-placement test checks the basis through `placement()`, not through a read row | pr/34:reviews/review-1.md:157-161; pr/34:implementation.md:210-219 | Agent: "O1: not taken" | Unchanged | #53 |
| B-5 | #20 O2: `interpretations.event_time.fallback` always equals `unknown` | pr/34:reviews/review-1.md:162-166 | Agent: no disposition recorded | Still true (`sparql.rs:760`) | #54 |
| B-6 | #20 O3: the event-clock selector and the Source selector share `?selected_source` | pr/34:reviews/review-1.md:168-172 | Agent: no disposition recorded | Still true (`correlation.rs:225`, `:290`) | #58 |
| B-7 | #20 O7: no test covers the "already current" or "facts disagree" branches | pr/34:reviews/review-2.md:156-159; pr/34:validation.md:13 | Agent: "not taken" | Moot: the event fallback backfill was removed (#193)| #59 |
| B-8 | #20 O8: a malformed stored `receivedAt` fails INVALID_ARGUMENT without job context | pr/34:reviews/review-3.md:96-102; pr/34:validation.md:13 | Agent: "not taken" | Moot: the event fallback backfill was removed (#193)| #60 |
| B-9 | #36 step 1 O2: no test covers the 16 MiB `PAGE_BATCH` bound | pr/37:reviews/review-1.md:136-140 | Agent: "the contract does not need it" | Moot: the event fallback backfill was removed (#193)| #62; not in #36 acceptance |
| B-10 | #36 step 1 O4: each rebuilt graph parses its manifest three times | pr/37:reviews/review-1.md:148-152 | Agent: "only if the measurement shows decoding dominates" | Moot: the event fallback backfill was removed (#193)| #63; not in #36 acceptance |
| B-11 | #36 step 1 O5: graphs that already carry `eventTime` skip the ownership, closure and term checks, so the open no longer refuses such a malformed graph | pr/37:reviews/review-1.md:154-159 | Agent: "no change is needed" | Moot: the event fallback backfill was removed (#193)| #66 |
| B-12 | #36 step 2 O5: test names overstate what the tests drive | W36/reviews/review-2.md:53; W36/implementation.md:269-273 | Agent: "Not done ... Neither was renamed or extended" | Still true in the uncommitted worktree | #36 PR; not in its acceptance |
| B-13 | #36 step 2 O6: a large metadata scope can refuse RESOURCE_LIMIT while the backfill is pending | W36/reviews/review-2.md:54 | Agent: no done or not-done mark | The worktree's reader table has no such sentence. | #36 |
| B-14 | #36 step 2 O1 to O4: stale text, swapped doc comments, duplicate slot mapping, journal read off the blocking pool | W36/reviews/review-2.md:49-52; W36/implementation.md:259-264 | Agent: "Done" | Reported done, uncommitted and not re-reviewed (unverified) | #36 |
| B-15 | #24 O3: the listing's integrity refusals are untested | pr/26:review-r2.md:185-187 | Agent: "partly done" | Unverified: grep found no named test. | #40 |
| B-16 | #24 O9: a source listing charges every source's recordless artifacts | PR #26 body; pr/26:review-r2.md:204-208 | Agent: "O9 (optional, out of scope) is recorded" | Still true. `docs/interfaces.md:304-308` says nothing about recordless artifacts, and #14 does not name this. | #41 |
| B-17 | #24 O10: on listings, the cache-failure message says "before requesting context" | pr/26:review-r2.md:210-212 | Agent: review OPTIONAL | Still true (`source_context.rs:145`) | #42 |
| B-18 | #24 O11: an artifact inside a partly walked millisecond can be partly counted | pr/26:review-r2.md:214-218 | Agent: review OPTIONAL | Not documented (`docs/interfaces.md:332-333`) | #43 |
| B-19 | #24 O12: a millisecond with thousands of artifacts makes the listing refuse | pr/26:review-r2.md:220-223 | Agent: review OPTIONAL | Unverified | #44 |
| B-20 | #24 round-1 O4 was not done | pr/26:review-r2.md:76 | Agent: "not done; both were optional" | Unverified: the round 1 text is not archived. | #45 |
| B-21 | #25 O1: the docs say "exited 0" where the check is `Result=success` | pr/27:review-r4.md:63-73; pr/27:validation.md:17 | Agent: "optional" | Still true (`deploy/RECOVERY.md:261`, `docs/deployment.md:366`, `docs/interfaces.md:493`, `sparrow-backup:23`, `:328`) | #46 |
| B-22 | #25 O2: rehearse the system-scope `journalctl` read at the cutover | pr/27:review-r4.md:75-77; pr/27:validation.md:19 | Agent: "at the #22 cutover" | Not run. Whether #22's UAT names this is unverified. | #22 |
| B-23 | No-COW B3: no atomic exchange between the two renames | pr/29:review.md:121-125; pr/29:validation.md:15 | Agent: "Optional and not done" | Still true (`deploy/lib.sh:486-487`, two `mv -T` calls) | #47 |
| B-24 | No-COW B4: the converter's refusals fire after `drain_stop` | pr/29:review.md:126-129; pr/29:validation.md:15 | Agent: "Optional and not done" | Still true (`restart.sh:67-69`, `lib.sh:459-465`). The live store is already converted, so the path does nothing today. | #48 |
| B-25 | No-COW: pre-copy the store to cut downtime; handle file names that contain newlines | pr/29:review.md:232-241; pr/29:validation.md:15 | Agent: "Optional and not done" | Still true. File names with newlines fail safe. | #50 |
| B-26 | #10 O1: no runtime assertion of the intake fence or of import provenance | pr/33:reviews/review-1.md:143-151 | Agent: review-debt "not required" | Still true (rpc.rs tests, checked by grep) | #52 |
| B-27 | #10 O2: the shipped system unit cannot bind the ingest socket | pr/33:reviews/review-1.md:152-158 | Agent: "or a follow-up issue" | Still true. `deploy/systemd/sparrow.service:101` does not allow the ingest socket path, deploy/apparmor has no rule for it, and #22 lists the ingest socket as out of scope. | #55 |
| B-28 | #10 O3: `importCancel` is not bound to the caller of `importBegin` | pr/33:reviews/review-1.md:159-163 | Agent: review OPTIONAL | Unverified: grep found no cancel check. | #56 |
| B-29 | #10 O4: `server.toml.example` lacks the three ingest keys, and `ArtifactReceipt` omits intake | pr/33:reviews/review-1.md:164-168; pr/33:implementation.md:216-219 | Agent: "Not done" | Still true (`deploy/config/server.toml.example`; `receipt.rs:129`) | #57 |
| B-30 | #10 O5: `admit_import` is `pub` with no outside caller; `bind_ingest` drops the check error; paired `_with_intake` entry points | pr/33:reviews/review-1.md:169-175 | Agent: review OPTIONAL | Still true (`lib.rs:62`, `local_endpoint.rs:50`) | #61 |
| B-31 | #1 O2: a failure of the OS random source for the CHAP nonce is reported as STORAGE_FAILURE | S1/reviews/delivery-review.md:184-188 | Agent: "deferred by the coordinator". No verbatim coordinator quote is recorded. | Still true (`crates/sparrow-server/src/tcp_gate.rs:52-53`) | #64 |
| B-32 | #1 O3: a bad secret file is reported as `local I/O: <os error>` without the path | S1/reviews/delivery-review.md:184-188 | Agent: as B-31 | Still true (`transport.rs:41`, `error.rs:68`) | #65 |
| B-33 | #1 O4: an `expect` relies on a TCP and secret pairing that `config.rs` enforces elsewhere | S1/reviews/delivery-review.md:184-188 | Agent: as B-31 | Still true (`crates/sparrow-server/src/main.rs:246`) | #67 |
| B-34 | #1 O6: two rules find the canonical root, and the test-support one has a silent fallback | S1/reviews/delivery-review.md:184-188 | Agent: as B-31 | Still true (`sparrow-test-support/src/lib.rs:17-33` and `sparrow-helper-census/src/cross.rs:260`) | #68 |
| B-35 | #1 recheck: the list of operations allowed on anonymous TCP omits `flush` | S1/reviews/delivery-review.md:190-191 | Agent: "New OPTIONAL nit" | Partly done: `docs/interfaces.md:2543` names `flush`, but `:586` still does not. | #69 |
| B-36 | Archive P1 O4: behavior when the quarantine listing overflows | SD/archive-p1-review.md:100, :179 | Agent: "stay open as recorded" | Specified in #16, not built | #16 |
| B-37 | Archive P1 O5: whether a projection quarantine keeps the metadata graph | SD/archive-p1-review.md:101, :179 | Agent: "stay open as recorded" | #15 and `docs/archive-format.md` section 6 still disagree. | #15 |
| B-38 | Issue review: the archive-p3 design compares `statfs` IDs, which differ per btrfs subvolume | SD/issues-v2/REVIEW.md:84-85 | Agent: "OPTIONAL (not changed)" | #16 still specifies the statfs ID. Not built. | #16 |
| B-39 | Issue review: the tmpfiles entry hard-codes the owner `paudley` | SD/issues-v2/REVIEW.md:86 | Agent: "OPTIONAL (not changed)" | Still true (`packaging/tmpfiles/sparrow-ingest.conf`; `docs/deployment.md:166`). Whether #22 names it is unverified. | #22 |
| B-40 | Census O4: no automated test that `load()` removes the tree after a walk | SD/census-review-r2.md:36-39 | Agent: "not done, and the coordinator recorded why". No verbatim quote. | Unverified | #70 |
| B-41 | #5 doc O1: the load range "25 to 46" should read "25 to 45" | SD/issue5-review-r2.md:58-60 | Agent: review OPTIONAL | Still true (`docs/load-qualification.md:106-107`) | #71 |
| B-42 | rdf_reverse O3: the column family is created before validation, so a refused open locks out older images | SD/revcf-review.md:61; SD/revcf-review-r2.md:49-51 | Agent: "the coordinator chose not to address". No verbatim quote. | Still true (`table_options.rs:44`) | #72 |
| B-43 | rdf_reverse O6: the migration leaves tombstones, and there is no `compact_range` | SD/revcf-review.md:64 | Agent: as B-42 | Unverified: the live tombstone count was not measured. | #73 |
| B-44 | rdf_reverse O7: `upgrade_reverse` takes no disk budget | SD/revcf-review.md:65 | Agent: as B-42 | Still true (`rdf_index.rs:258`) | #74 |
| B-45 | rdf_reverse O8: the WAL cap may grow with a third column family | SD/revcf-review.md:66 | Agent: as B-42 | Unverified: an inference, not measured | #75 |
| B-46 | rdf_reverse O9: the fixture never opens a store that lacks the column family | SD/revcf-review.md:67 | Agent: as B-42 | Unverified | #76 |
| B-47 | rdf_reverse r2 O1: `development-runtime.md` says "both existing column families" | SD/revcf-review-r2.md:47 | Agent: "leave it as history" | Still present (`docs/development-runtime.md:331`) | #77 |
| B-48 | Space floor: `Tail::reconcile_missing` writes while ingest is paused | SD/space-floor-review.md:231-232 | Agent: "carried over" | Still true (`watches.rs:1054-1056`, `tailing.rs:378`) | #78 |
| B-49 | Space floor: the syslog gate hard-codes 1 s instead of using `SPACE_CHECK_INTERVAL` | SD/space-floor-review.md:233-234 | Agent: review OPTIONAL | Still true (`syslog_listener.rs:415`) | #79 |
| B-50 | Space floor: the `store-free-space-floor` ledger row enforces nothing | SD/space-floor-review.md:224-231 | Agent: review OPTIONAL | Still true (`helpers-ledger.toml:4582`) | #80 |
| B-51 | Space floor: no runtime test of pause and resume | SD/space-floor-review.md:122-123 | Agent: review OPTIONAL | Unverified | #81 |
| B-52 | Artifact limit O2: `retention_metrics` can refuse on stores near the ceiling | SD/reviews/artifact-limit-review.md:157-161 | Agent: "belongs with #5" | Still bounded at a 32 MiB catalog (`retention_metrics.rs:169-178`). #5 does not name it. | #82 |
| B-53 | Artifact limit O3: draining an artifact with many records rereads the parent row | SD/reviews/artifact-limit-review.md:162-164 | Agent: "belongs with #5" | Named in #5 | #5 |
| B-54 | Artifact limit O4 and the #21 exclusion: no test splits a single line longer than the raw-record ceiling | SD/reviews/artifact-limit-review.md:165-166; #21 body, Out of scope | Agent: "carried" | Unverified | #83 |
| B-55 | Compression O6: no test turns the rewrite window off once it is on | SD/reviews/compression-review.md:171 | Agent: review OPTIONAL | Still true (`storage.rs:18676-18682` ends at `Running`) | #84 |
| B-56 | Deploy O1: no `sync` between the revert marker and `mv -T` | SD/reviews/deploy-review.md:75 | Agent: review OPTIONAL | Still true (`deploy/RECOVERY.md:446-447`) | #22 |
| B-57 | Deploy O2: `restart.sh` lacks `require_settled` | SD/reviews/deploy-review.md:76 | Agent: review OPTIONAL | Still true: only `start.sh:64` calls it. | #85 |
| B-58 | #31 O4: no automated test of the backup guard's truth table | pr/32:reviews/review-1.md:44; pr/32:validation.md:14 | Agent: "accepted" | Still true | #86 |

## (C) Plan exclusions and implementer "not done"

| Id | Item | Source | Marked by | Real state | Lands in |
| --- | --- | --- | --- | --- | --- |
| C-1 | #1: no TLS, so an on-path attacker can observe or hijack an authenticated TCP session | S1/plan.md:29-31 | Agent: plan Exclusion. It follows the 2026-10-27 ruling "(2) shared secret, using CHAP", which did not exclude TLS. | Still true. Documented at `docs/interfaces.md:1948`. | #104 |
| C-2 | #1: syslog listeners are not behind CHAP | S1/plan.md:32 | Agent: plan Exclusion | Unchanged. Whether a syslog listener is enabled live is unverified. | #105 |
| C-3 | #3: raising the import lease or slot count | #3 body, Out of scope | Agent: issue draft | No open issue names it. | #106 |
| C-4 | #5: JSON value facts for `keys` and `values`; the Markdown projection | #5 body, Out of scope | Agent: issue draft | No open issue names them. | #107, #108 |
| C-5 | #5: 3.4 GB of RPC reads per 30 s are unattributed, and 6.8 million deletion markers are left to compaction | #5 comments 3924, 4006 | Agent: "Remaining cost, not fixed here" | Current state unverified | #5 |
| C-6 | #6: receiver-window bounds for syslog listeners | #6 body, Out of scope | Agent: issue draft | No open issue names it. | #109 |
| C-7 | #7: a spool for lossy mode; spool replication or backup; automatic discard of held batches | #7 body, Out of scope | Agent: issue draft | No open issue names them. | #110, #111, #112 |
| C-8 | #8 and #9: a producer connection pool; publishing `sparrow-client`; relaxing the exact schema for CLI and MCP | #8 and #9 bodies, Out of scope | Agent: issue draft | No open issue names them. | #91, #92, #93 |
| C-9 | #10: peer credentials on the private socket; binding sources to uids; remote producers | #10 body, Out of scope | Agent: issue draft | No open issue names them. | #113, #114, #115 |
| C-10 | #10 step 3: no schema 67 run proves the SPARQL refusal predates #10 | #10 comment 7159 | Agent: "I have no run of the schema 67 image" | Unverified. Settled when A-10 passes. | #13 |
| C-11 | #11: other hosts' journals, journal history import, redaction, and a native journal reader, which is "not yet designed" | #11 body, Out of scope; docs/implementation-plan.md:189-192 | Agent: issue draft and plan text | Not designed and not built | #116, #117, #118, #119 |
| C-12 | #13: raising host ceilings; planning specific to correlation | #13 body, Out of scope; pr/35:plan.md:13-15 | Agent: issue draft and plan Exclusion | Not done | #120, #121 |
| C-13 | #13: large filtered live scopes fail RESOURCE_LIMIT on per-graph cursors | pr/35:implementation.md:181-184 | Agent: "Not done / for the caller" | Unverified on live | #122 |
| C-14 | #14 exclusion: the record search scan budget for wide time windows. Effect on 2026-11-07 at 11:15 CST: the ingest rate over the last hour cannot be answered, because a one-hour `records` window fails RESOURCE_LIMIT "record capture exhausted candidate scan_budget". | #14 body, Out of scope; delivery session report to coordination, 2026-11-07, round 60 reply (two sends timed out; the third was delivered) | Agent: issue draft exclusion; the report said "I cannot give it" | Still true by code: `crates/sparrow-core/src/storage/text_index.rs:539-545` refuses once `scan_budget` (at most 100,000, `query.rs:158`) candidates are visited. | #123 |
| C-15 | #14: per-source byte totals | #14 body, Out of scope | Agent: issue draft | No open issue names it. | #124 |
| C-16 | #15 to #17: raising segment caps before the PurRDF Parquet upgrade; re-import of archived evidence (archive Decision 6, "deferred"); NAS pruning; an archive query service; segment repair | #15, #16 and #17 bodies, Out of scope; docs/archive-format.md:353; SD/archive-design.md:146, :164 | Agent: issue draft and design note | None built. The segment cap tracks the PurRDF upgrade (D-4). | #125, #126, #127, #128, #129 |
| C-17 | #18: removing v1 decoders after the migration | #18 body, Out of scope | Agent: issue draft | No open issue names it. | #130 |
| C-18 | #19: ZSTD above the bottommost level and level 9; blob level; filesystem compression; raw-hash keys in `rdf_reverse` | #19 body, Out of scope | Agent: issue draft | No open issue names them. | #131, #132, #133, #134 |
| C-19 | #20: inferring event time from message text | #20 body, Out of scope | Agent: issue draft | No open issue names it. | #135 |
| C-20 | #21: a tailed line longer than the 1 MiB capture window still fails | #21 body, Out of scope | Agent: issue draft | Unverified in code | #136 |
| C-21 | #21: raw records over 128 MiB published earlier keep their size | #21 body, Out of scope | Agent: issue draft | Unverified. #18 re-derives derived rows, not raw records. | #137 |
| C-22 | #21: the 7.93 GiB import bound, the eviction row copy, and resumable seal, layout and publish | #21 body, Out of scope; SD/STATE.md:95 | Ruling, Patrick direct in the #21 session (SD/STATE.md:95): "ON the limit, concur.  For now anyways." A concurrence "for now", not a standing hold: lifting the bound is a build under #5. | Still in force. #5 owns lifting the bound. | #5 |
| C-23 | Pin caps of 606 pins and 36,594 members kept as merged | SD/STATE.md:95; SD/reviews/artifact-limit-review.md:148-156 | Agent. Only a paraphrase exists: "kept the pin caps ... as merged". No verbatim words exist, so this is not a hold; it is a build. | Still in force (`eviction.rs:230` `MAX_PINS`) | #87 |
| C-24 | #22: AppArmor profiles for the two timer helpers, `sparrow-backup` and `sparrow-ship` | #22 body, Out of scope; deploy/RECOVERY.md:599-601; deploy/systemd/sparrow-backup.service:51-52; deploy/systemd/sparrow-ship.service:45-46 | Agent: "follow-up work" | Not written. `deploy/apparmor` holds only `sparrow`. | #88 |
| C-25 | #36 step 2 is not #18's full re-derivation | W36/plan.md:11-12 | Agent: plan Exclusion. Its placement follows the 06:3x ruling "file it under #18". | #18 open, not started | #18 |
| C-26 | #36 plan excludes changes to the #13 owner-index job | pr/37:plan.md:9-10 | Agent: "unless measurement shows it matters" | The step 2 design covers both jobs, as A-16's ruling requires. | #36 |
| C-27 | #36 step 2: `with_rdf_records` refused while graphs were pending | W36/implementation.md:225-227 | Agent: "Not done / open" | Resolved by #158: the view serves the facts the event fallback backfill owes, so pending graphs no longer refuse. | #36 |
| C-28 | Broad query acceptance is unfinished, and other captured query families keep their old implementation | docs/query-streaming.md:159-163 | Agent: doc text | Still stated at HEAD. Current behavior unverified. | #14 for `sources` and `artifacts`; #89 for the rest |
| C-29 | Social-preview card not drawn | docs/BRAND.md:111-112 | Agent: "not yet drawn" | Still true | #90 |
| C-30 | Decodes draft (C17): a direct sparrow-client producer in lossless-confirmed mode comes "later", once the registry, spool and profile land | .stage/tmp-rescue-20261103/sparrow-issue.md:38 | Agent draft with a named trigger. The parent ruling, Patrick 2026-11-01: "both must be supported in the capnproto shim" (:9) | Sparrow's prerequisites are not started. | #6, #7, #9 |
| C-31 | Producer contract G7: producer sessions inside `max_rpc_connections` | SD/coord/decodes-producer-contract.md:43 | Agent: "FEATURE", owner Patrick | No producer pool exists. | #91 |
| C-32 | Producer contract G9: a unit restart policy that recovers without a human | SD/coord/decodes-producer-contract.md:45 | Agent: "FEATURE S", owner Patrick | Unchanged: `StartLimitBurst=5`, `Restart=on-failure` (`deploy/systemd/sparrow.service:24-25`, `:50-51`) | #94 |
| C-33 | Producer contract G11: per-source ingest binding and retention priority | SD/coord/decodes-producer-contract.md:47 | Agent: "FEATURE M/L", owner Patrick | Absent | #95, #96 |
| C-34 | Producer contract G13: PurRDF and sparrow-client from a registry | SD/coord/decodes-producer-contract.md:48 | Agent: "FEATURE, plus a ruling" | #9 lists registry publishing as out of scope. | #92 |
| C-35 | GitHub readiness: six questions for Patrick (purrdf public, lexicons, author identity, canonical or mirror, which docs, the Katamari name) | SD/github-readiness.md:222-245 | Agent: "needs network or Patrick's decision" | No decision is recorded. | #97 |

## (D) Accepted risks and known flakes

| Id | Item | Source | Marked by | Real state | Lands in |
| --- | --- | --- | --- | --- | --- |
| D-1 | Anonymous TCP gets ingest and all import operations, with no namespace isolation or quota | #1 body, "Rulings (2026-10-27)"; S1/plan.md:53 | Ruling, Patrick 2026-10-27: "No quote, no security, just take it." The #1 body records: "Anonymous TCP gets ingest only; `status` requires CHAP. Anonymous imports get no isolation or quota." | Still true (`docs/interfaces.md:586`, `:1951`) | accepted by ruling |
| D-2 | No new TCP connection ceiling, so the starvation risk on the shared ceiling stays | S1/plan.md:55 | Ruling, Patrick direct 2026-10-27: "No limits". His words mean: add no new ceiling. "The starvation risk stays, documented" was the agent's reading; the risk is a build. | Unchanged. Where it is documented is unverified. | #98 |
| D-3 | Pipelined calls on returned session capabilities are not gated, "by design" | S1/plan.md:44 | Agent: plan Risk | Unverified | #99 |
| D-4 | The archive segment ceiling of 192 MiB stays until the PurRDF Parquet upgrade | docs/archive-format.md:115-118 | Ruling, Patrick direct 2026-11-01: "purrdf will soon produce, optimal, maximally compressed, parquets so that problem will go away on a future purrdf upgrade" (SD/archive-design.md:158). This is a prediction, not a hold: a build that tracks the PurRDF upgrade. | The ceiling is unchanged. The trigger is the named PurRDF upgrade. | #125 (tracks the PurRDF upgrade) |
| D-5 | Known flake: `consume_coverage_limit_and_root_replacement_fail_without_source_retirement` misses its 10 s deadline under hook load | SD/STATE.md:121; PR #32 body | Agent: "Known flake ... the retry passed" | Still present (`crates/sparrow-server/tests/rpc.rs:5180`). Which 10 s deadline fires is unverified. #23 covers ports only. | #100 |
| D-6 | TCP tests fail with AddrInUse when checkouts run concurrently | #23 body | Agent: issue filed 2026-11-02 | Unfixed: fixed start port 10240 (`rpc.rs:465`) | #23 |
| D-7 | R40: the image copy in a pre-upgrade backup loses `cap_net_bind_service`, so a rollback from that copy needs setcap again | SD/STATE.md:27, :124; #10 comment 7159; #28 comment 6289 | Ruling, coordination 2026-11-05: "do not setcap yourself; when the new image is built, send me its exact path and I run the setcap and confirm with getcap before your switch. #22 retires the step." | Still true for user-unit backups, including `pre-upgrade-sparrowd-schema67-13af50d`. `restart-bundle.sh` copies images with `cp -a` and no setcap. The system unit grants the capability as ambient (`deploy/systemd/sparrow.service:93`). | #22 |
| D-8 | The 2026-11-02 nightly backup failed with exit 1, and the cause is unknown because the journal rotated | SD/STATE.md:49 | Agent: recorded, not investigated | The nightlies of 20261105, 20261106 and 20261107 exist. The cause is still unknown. | #101 |
| D-9 | `restart-h.sh` still copies the no-COW store into the COW backups directory, which fails | SD/STATE.md:115 | Agent: "fix it before reuse" | `SD/restart-h.sh:37` is unchanged. `restart-schema68.sh:52-53` and `restart-bundle.sh:54-55` carry the fix. The script lives outside the repository. | #102 (fix or delete the script) |
| D-10 | `rdf_reverse` rollback is one-way, and the backup before deploy was "left to Patrick" | SD/revcf-review-r2.md:50 | Agent: coordinator choice, no verbatim quote | Historical (schema 62). Pre-upgrade backups now precede every restart. | none (historical) |
| D-11 | `WORKTREE_REVIEW.md` says archive-gate has landed and lists no entries | .worktrees/WORKTREE_REVIEW.md:16-17 | Ruling, Patrick 2026-11-06 13:20 CST: "Have all agents ... run /sweep-branches" | True for merged work: 1ccdb43 is an ancestor of main. It does not record the two current worktrees, 36-bound-the-schema-69-first-open and open-items-inventory. | needs a sweep entry for the two worktrees |

## (E) Code and doc TODOs

The repository has no `TODO` or `FIXME` comments in `crates/`, `deploy/` or `docs/` at eedb484. The only `todo!` matches are census rule fixtures (`crates/sparrow-helper-census/src/rules.rs:828`, `:849`). `helpers-ledger.toml` has 316 rows with `enforced = true` and none with `enforced = false`.

| Id | Item | Source | Marked by | Real state | Lands in |
| --- | --- | --- | --- | --- | --- |
| E-1 | The persistent client spool is called "a later extension" | docs/interfaces.md:2674 | Agent: doc text | Present | #7 |
| E-2 | Archive worker status "will use a later schema"; do not enable `[archive]` before the worker ships | docs/interfaces.md:226, :277 | Agent: doc text | Present | #16 |
| E-3 | Stale sentence: record-backed membership "does not yet add independent metadata graphs for recordless artifacts or expose the RPC/CLI/MCP query operations" | docs/purrdf-integration-gaps.md:325-326 | Agent: historical doc text | Superseded by the same file's current-state section (:13-19) | #103 (doc correction) |
| E-4 | Dated "not yet" entries in the runtime log: watch coordinator, cursor pagination, captured result leases, field-posting indexes | docs/development-runtime.md:2938, :5541, :5654, :6027, :6327 | Agent: dated log entries | The current state of each is unverified. | unverified |
| E-5 | `restart-bundle.sh` says the first-open cost is "not measured yet ... put the number here" | SD/restart-bundle.sh:6-10 | Agent: script comment | Stale: 76 min (758b41a) and 34 min (9c91a97) were measured. The script lives outside the repository. | #36 |
| E-6 | `development-runtime.md` still records the installed schema 62 runtime | SD/archive-p1-validation.md:44 | Agent: "Open" | Unverified | #103 |

## Items found done, with evidence

| Id | Item | Source | Marked by | Evidence | Landed in |
| --- | --- | --- | --- | --- | --- |
| F-1 | #13 review evidence: `make check` not run at review; first-open cost unmeasured | pr/35:reviews/review-1.md:133 | Agent: review OPTIONAL | The commit hook passed with EXIT=0, and the first open was measured at about 76 min (#36 body). | 9e71b63; #36 |
| F-2 | #24 O7 test and O8 wording | PR #26 body; #24 comment 6092 | Agent: review OPTIONAL, taken | Added before merge. The hook passed with 427 tests (SD/STATE.md:9). | e16a81d |
| F-3 | #28 O1: pin the stored identity values | pr/30:review.md:104-109 | Agent: review OPTIONAL, taken | `persisted_identities_are_pinned_and_the_markdown_contract_move_migrates` (`storage.rs:11852`) | 71d2199 |
| F-4 | #28 d74 O3: `previous_profile_advances` comment wording | pr/30:review-d74.md:186-192 | Agent: review OPTIONAL | The old phrase no longer appears in crates. The fixing commit is unverified. | before eedb484 |
| F-5 | #31 O1 and O2 | PR #32 body | Agent: review optional, taken | Commit c8bdca3 is titled "(#31 review O1, O2)". | 712ee8d, c8bdca3 |
| F-6 | #36 step 1 O3: doc and comment wording | pr/37:reviews/review-1.md:142-146 | Agent: review OPTIONAL | `docs/interfaces.md:534` reads "examines up to 4,096 graphs". | 9c91a97 |
| F-7 | #36 step 1: copy measurement under unit limits | pr/37:implementation.md:158-162 | Agent: "Not done" at implementation | 2,065 s on image 9c91a97, 07:49:12 to 08:23:37 CST (pr/37:validation.md:11). This misses the 10-minute bar, which A-24 owns. | 9c91a97 (measurement only) |
| F-8 | #1 O1, O5, O7 | S1/reviews/delivery-review.md:178-182 | Agent: reviewer confirmed done | Confirmed at the recheck | 829b429 |
| F-9 | #1: live deployment with the secret file and the 50 GiB budget | S1/validation.md:35-36 | Agent: "Not run (Patrick-gated)" | `~/.config/sparrow/server.toml:8` sets `rpc_tcp_secret_file`, and `:10` sets `disk_budget = 53687091200`. | live since 829b429 |
| F-10 | #1: production inputs deferred | S1/plan.md:50 | Ruling, Patrick direct 2026-10-27: "Defer for now". Superseded: the watches are live. | STATE.md records 6 of 6 watches live. This is to be rechecked and recorded; it was not rechecked here. | superseded |
| F-11 | No-COW B2: sync before the swap | pr/29:review.md:114-120 | Agent: review OPTIONAL | `deploy/lib.sh:485` runs `sync -f -- "$partial"`. | f18bc12 |
| F-12 | Compression O5 and O7: the re-marking comment and the `Running` doc | SD/reviews/compression-review.md:166-172 | Agent: review OPTIONAL | `docs/architecture.md:194`; `recompression.rs:41-45` | 004cab8 |
| F-13 | Remove `/SPARROW/store.cow-20261105T032247Z` after a verified nightly | SD/STATE.md:107 | Agent: "Pending" | The path no longer exists. The removal date is unverified. | done outside the repository |
| F-14 | The 20261104 nightly's SOURCE lacks `store_id` and `image` | SD/STATE.md:15 | Agent: recorded | Moot: that nightly is gone, and #31 closed on the 20261107 nightly passing verify-backup. | #31 |
| F-15 | #19 step 0 baseline never recorded; step 4 compared on query-only copies instead | #19 comments 5900, 6286 | Agent: a substitute measurement, then the issue was closed | 0.018 s against 0.204 s cold on like-for-like copies. The original pre-deploy comparison does not exist, so this is a substitution, not the step as written. | closed (substitute) |
| F-16 | #21 step 3 not passed as written; amended to use `records` | #21 comments 5899, 5920 | Agent: amended, then the issue was closed | #24 fixed the listing refusal. The original `artifacts` step was never re-run live, so this is a substitution. | ada4a21 (substitute) |
