# Issue #271: Document pressure admission, archive retries and status text

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Pressure and archive admission documentation

Document filesystem admission in both threshold orderings. Ordinary
writes use the free-space floor. Archive intent and watermark commits use
the maintenance reserve. At the reserve threshold, another archive pass
can commit while the floor still refuses ordinary writes. When the floor
is lower and free space falls between the two thresholds, archive commits
still wait for the reserve. A reserve refusal leaves the watermark in
place, and the worker tries again at its configured interval.

Reflow the floor episode's eight-batch cap and the expiry wake contract.
Keep the floor-resume wake, the pressure wake once free space reaches the
reserve during a floor episode, the one-hour expiry backstop and the
60-second retry for other failures. Use the same threshold wording in
`docs/interfaces.md` and `docs/native-reclamation.md`.

Document `waitingOn` and `waiting_on` as display text in both the
filesystem and budget pressure paths. Describe structured status as the
input for automation. Check the shell callers and the status renderer
before claiming compatibility.

## Acceptance

- Confirm the threshold and retry wording against `Writer::admit_space`,
  `archive_intend`, `archive_commit` and the archive coordinator loop.
- Confirm the floor cap and each expiry wake against the maintenance
  implementation, and run `prose-check` on both documents.
- Close #273 with the documented cap, status semantics and caller check.
  Keep #275's historical three-failure correction and its separate
  reverted-variant disposition linked to the documentation delivery.

[Cap and status wording](http://lillith:7555/fleet/sparrow/issues/273) · [Historical failure record](http://lillith:7555/fleet/sparrow/issues/275#issuecomment-22957)


## Comments (1)

### paudley — 2026-11-22T07:23:51Z

Use one pressure documentation delivery for #271, the prose and status wording in #273, and the interface reflow in #275. Keep all three issues open until their remaining acceptance is addressed.

- Archive intent and commit enter the maintenance reserve admission path through `Writer::reclaiming`. Document both possible reserve/floor orderings and the inclusive reserve threshold. The worker attempts another pass at its configured interval. Reaching the reserve allows an archive commit at that attempt even when ordinary writes still face the floor.
- Reflow the eight-batch floor cap and the expiry wake description. Preserve the floor-resume and above-reserve pressure wake, the one-hour expiry backstop and the separate retry for non-reserve failures.
- The exhausted-frontier text is also used on the budget path. The deployed code renders `waiting_on` as status text. The current shell consumers do not match the old or new sentence, and `pressure.rs` tests the rendered field. Record the inspected callers and keep typed status as the automation contract.
- #275 records the three historical failures and retains the remaining reverted-variant disposition. Run prose-check on the changed passages identified by #273. [Historical result](http://lillith:7555/fleet/sparrow/issues/275#issuecomment-22957)


## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
