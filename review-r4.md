# Delivery review, round 4: query-only mode and backup verification (#25)

Branch `query-only`, head 1984bc2, worktree `/home/paudley/Active/sparrow/.worktrees/query-only`.

## Scope

- The round-3 fix `cfe090e..1984bc2` (6 files): `deploy/libexec/sparrow-backup`, the
  `verify-backup.sh` SKIP wording, and the RECOVERY, deployment, interfaces and lifecycle
  evidence text. Judged against round 3's R1, O1 and O2.
- In the whole range `origin/main..1984bc2`, only what the fix changes: every `SOURCE` reader
  (`start.sh --restored-from`, `pre-upgrade-backup.sh`, `lib.sh`'s raise guard,
  `verify-backup.sh`), the shutdown closing line in `crates/sparrow-server/src/shutdown.rs`, and
  `deploy/systemd/sparrow.service`'s stop settings (`KillSignal=SIGINT`, `TimeoutStopSec=120`,
  no `ExecStopPost`).
- Inputs read: issue.md, issue-comments.md, plan.md and its `## User rulings`, review-r3.md.
  The ruling ("You have the go signal, let'er rip"; #25 at schema 67 after #24) cuts no scope.
- I did not touch the live service, its timers, its backups or `/SPARROW/store`.

## Evidence

| Check | Command | Result |
| --- | --- | --- |
| No daemon change since the rehearsed build | `git diff --stat a3b53f1..1984bc2 -- crates Cargo.toml Cargo.lock` | empty, exit 0; the `a3b53f1` images in the rehearsal are this head's daemon |
| Image detection on real binaries | load the helper with `SourceFileLoader`, call `logs_closing_count` | `bin/sparrowd-schema66-a3b53f1` True, `target/release/sparrowd` True, the live `sparrowd-schema65-0221e87` (read only) False; exit 0 |
| The unrecorded marker cannot satisfy a count reader | the same probe, `CLOSED.fullmatch` plus a read of every `SOURCE` reader | the marker line does not match `CLOSED`. Every reader uses anchored `sed -n 's/^KEY=//p'` or `grep -qx`, and none sources `SOURCE`, so `payload_chunks=unrecorded (...)` reads as "no `unique_payload_chunks`" |
| Helper syntax | `python3 -I -c 'ast.parse(...)'` | exit 0 |
| Script lint | `shellcheck -x deploy/verify-backup.sh` | exit 0 |
| Prose | `prose-check` on the three changed docs; `--stdin` on the added helper and evidence lines | 9 `UInt64`/`UInt32` hits in interfaces.md, all also on `origin/main` and none in changed lines; stdin exit 0 |
| Behaviour through a real user-scope unit | `deploy/evidence/lifecycle.md`, "Backups across the image that adds the closing count" (implementer's transcript) | schema 65 pre-upgrade backup published with the marker, and `restart.sh` raised to 66 on it. `--image` verify of that backup: all checks PASS, count SKIP, exit 1. New-image backup recorded `unique_payload_chunks=1` and verified, exit 0. SIGKILL stop failed at once on `Result=signal`, left only `.partial-*`, unit active |
| Rust tests, `make check` | not rerun | no Rust or test file changed in the fix |

My probe left `deploy/libexec/__pycache__/`. I removed it, and `git status --short` is empty.

Not exercised: a failing `journalctl` (the evidence says it was not provoked), the system-scope
read inside the `sparrow-backup.service` sandbox (deferred to the cutover, listed under "Not
run"), and a capable image that exits 0 without the line.

## Round-3 findings

- **R1: resolved by option (i).** Before the stop, `sparrow-backup` reads the image the link
  names (`LIBEXEC / image`, the same path it copies into `images/`) for the line's literal
  prefix. The prefix is a literal piece of the `eprintln!` format string in `shutdown.rs:60`.
  - An image without the prefix publishes with `payload_chunks=unrecorded (image predates the
    closing count)`. That covers the pre-upgrade backup on schema 65 and nightly backups after
    a restore to an older image.
  - An image with the prefix still fails when the line is missing.
  - Both paths are rehearsed.
  - The old-image path does not check `Result`. That matches `origin/main`'s helper, which never
    checked how the stop ended, so it adds no regression.
  - A link that names an image other than the one running, at the same schema number, can only
    turn a capable image into a hard failure, or an incapable one into an unrecorded publish.
    Both fail closed or are harmless.
- **O1: resolved.** `stop_result()` reads `Result` after the stop and before the `finally`
  restart. systemd resets `Result` when the unit starts, so the value belongs to the invocation
  that was stopped. A capable image records the count only on `success`, so a nonzero exit after
  the line now fails the backup. The interfaces.md text no longer says "clean shutdown".
- **O2: partly addressed, as intended.** A `journalctl` failure, or a missing `journalctl`, now
  ends in `fail()` with stderr and the systemd 257 note. The docs state that version. The
  system-scope rehearsal is still recorded as owed at the cutover.

## Findings

**OPTIONAL O1: "exited 0" is slightly stronger than what `Result=success` means.** systemd also
reports `success` when the main process dies of SIGHUP, SIGINT, SIGTERM or SIGPIPE. When the
120 s stop timeout escalates to SIGKILL, `Result` reads `timeout`, not `signal`. This affects:
- the `stop_result` docstring;
- the comment at the call site;
- RECOVERY.md and deployment.md ("only when that daemon then exited 0");
- interfaces.md.

Behaviour is unaffected. A death by SIGINT logs no line, so `closing_chunks` still fails it, and
`timeout` is not `success`. The wording could say "when systemd reports the stop as successful
(`Result=success`)".

**OPTIONAL O2 (carried from round 3): rehearse the system-scope path once at the cutover.** That
path is the root `journalctl --invocation=` read inside the `sparrow-backup.service` sandbox. The
evidence already lists it under "Not run".

Rebase note, not a finding (per the caller, the schema 67 rebase is expected): after #24 takes
schema 66, the text "(schema 65 and older)" for images that predate the line and query-only mode
should read 66. Detection reads the image's contents, so behaviour needs no change.

No REQUIRED findings.

VERDICT: PASS
