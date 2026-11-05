# Delivery review, round 3: query-only mode and backup verification (#25)

Branch `query-only`, commits a3b53f1 and cfe090e on top of 6461688, base main 0221e87, worktree
`/home/paudley/Active/sparrow/.worktrees/query-only`.

## Scope

- Round 3 checks the changed work in `6461688..cfe090e` (9 files):
  - `Store::unique_payload_chunks`, the closing line in `shutdown.rs`;
  - `sparrow-backup`'s invocation lookup and `closing_chunks`;
  - the `verify-backup.sh` link, recorded-path and closing-line checks;
  - the test addition, the RECOVERY, deployment and interfaces text;
  - the rehearsal in `deploy/evidence/lifecycle.md`.
- It also covers the round-2 findings and the coordinator's questions (a) to (d).
- I re-read `stagectl issue 25`. UAT step 0 now exports `SPARROW_CONFIG` and `SPARROW_INTAKE`
  when `/etc/sparrow/server.toml` is unreadable.
- Read-only host checks:
  - the user-scope `sparrow-backup.service` `ExecStart`;
  - the live image link `~/.local/libexec/sparrow/sparrowd` (it points to
    `sparrowd-schema65-0221e87`);
  - `systemctl --version` (systemd 262);
  - the system `sparrow-backup.service` and `sparrow.service` units in `deploy/systemd`;
  - `deploy/pre-upgrade-backup.sh`, `install.sh`, `restart.sh` and the README section
    "Upgrades and rollback".
- I did not touch the live service or `/SPARROW/store`.

## Evidence

| Check | Command | Result |
| --- | --- | --- |
| Query-only and verify tests at cfe090e | `cargo test --locked --release -p sparrow-server --test rpc -- query_only verify_backup` | EXIT=0; 4 passed. `verify_backup_fails_on_wrong_store_id` now also requires the copy's closing line to match its status count, and the link test covers a link inside the store |
| Script lint | `shellcheck -x deploy/verify-backup.sh` | exit 0 |
| sparrow-backup syntax | `python3 -I -c 'ast.parse(...)'` | exit 0 |
| Clean stop, SIGKILL stop and invocation lookup under the user manager | `deploy/evidence/lifecycle.md`, "The closing count through a real stop" (the implementer's transcript) | Clean run published with `unique_payload_chunks=1` and verified end to end. The SIGKILL run failed after 30 s, leaving only `.partial-*`. The next clean run removed the partial |
| Full gate | `make check` in both hooks, as reported by the coordinator | not rerun |

Not exercised: the system-scope path (root `journalctl` inside the `sparrow-backup.service`
sandbox) and any parser test of `closing_chunks`.

## Round-2 findings

- **R1 (UAT on this host): resolved.** Step 0 exports both variables when `/etc/sparrow` is
  absent.
- **O1 (count race): resolved by design.** The count is now taken after the stop; see (a) and (d).
- **O2 (inner links): resolved.** `find -type l -print -quit` refuses any link, and the test covers
  a link that stays inside the backup.
- **O3 (recorded paths): resolved.** `SOURCE`'s `data_dir` and `intake`, and `config/server.toml`'s
  `data_dir`, are added to the guard. The expansion of an empty array is safe under `set -u` on
  the host's bash.

## Answers to the coordinator's questions

**(a) When the closing line appears.** The line is printed only after three things. First, every
worker's `shutdown().await` has returned. Second, `drain` has taken every operation slot, so no
RPC work is in flight, and admission was closed earlier. Third, `store.flush()` has returned
`Ok`, which synchronizes the WAL and flushes every column family. It is never printed when the
drain fails, when the flush fails, or after a SIGKILL. The rehearsal shows the SIGKILL case. In
each of those cases nothing is printed and the backup fails.

On every path that prints the line, nothing can write between the count and process exit, so the
count equals the durable store. The storage is consistent: synchronized and flushed.

The line is not limited to a fully clean exit, though. It is also printed when an earlier phase
recorded an error but the drain and flush still succeeded:
- the serving loop's own error, for example "consume worker terminated unexpectedly";
- a failed consume, syslog or maintenance shutdown;
- a failure after the line, in the endpoint cleanup.

The daemon then exits nonzero. `sparrow-backup` does not check the stopped unit's result, so such
a run is published. At the storage level that is harmless: the copy matches the count, and a
pending claim is recovered at the next open. But the docs' "ends a clean shutdown by logging" and
the code comment overstate it (OPTIONAL O1).

**(b) Journal parsing.** Every case below fails closed:

| Case | Behaviour |
| --- | --- |
| Invocation ID | Read with `systemctl show -p InvocationID --value` before the stop and required to be 32 hex digits. If that call fails, nothing has been stopped. |
| Unit restarted between the status read and the ID read | The stop then hits the new invocation, which logs the line. The store-id check still binds it to the same store, and the count is of the store that was copied. Correct. |
| Lookup | `journalctl [--user] -u UNIT --invocation=ID -o cat`, which systemd 262 supports. A `journalctl` failure raises `CalledProcessError`: the backup fails with a traceback rather than a `fail()` message, and nothing is published. |
| Multiple lines | The last full match wins. A clean stop has exactly one. |
| Another store's id | `fail()`. |
| Line missing (rate limit, loss, lag over 30 s, unclean stop, old image) | `fail()` after 30 s. It runs after the restart, and SOURCE is written only after it, so `.partial-*` is never renamed into place. The next run removes it. |
| Forged line | Matching uses `fullmatch` on whole journal lines, and paths in watch messages are `{:?}`-escaped. A forged line would need text the daemon writes to stderr and an unclean stop with no real line. Even then the only effect is publishing a crash-consistent copy, and `verify-backup.sh` still checks the count against the copy. Low risk; hardening is in O1. |

**(c) The journald dependency.** This is acceptable: both units already log through the journal,
and RECOVERY and deployment.md describe reading the closing line from it. The system-unit path
reads the same way in code (`journalctl` without `--user`). The backup unit runs as root with
`CAP_DAC_READ_SEARCH`, and its sandbox only blocks writes to `/var` (`ProtectSystem=strict`), so
reading `/var/log/journal` should work. `systemctl` already works there over `AF_UNIX`. This is
inference only; the rehearsal ran in user scope. Exercise it once at the system cutover.
`journalctl --invocation=` needs a recent systemd (the host has 262), which is not stated
(OPTIONAL O2). What the docs leave out is the image-version dependency (REQUIRED R1).

**(d) `Store::unique_payload_chunks()`.** It is exactly what status reports. Both are
`count_prefix("ref:")` under the same writer lock (`storage.rs:2355` and the status body). The
passing `verify_backup_fails_on_wrong_store_id` checks this end to end: the daemon's closing line
must equal its status value.

## Findings

**REQUIRED R1: sparrow-backup now fails on every store served by an image older than a3b53f1.
Neither the upgrade order nor the restore case is handled or documented.**

An image without the closing line never logs it, so `closing_chunks` always fails, and that
blocks this deployment in two ways:
- This host runs `sparrowd-schema65-0221e87` under the user unit, with the helper at
  `~/.local/libexec/sparrow/sparrow-backup`. If the new helper is installed before the daemon runs
  a closing-line image, the next nightly backup fails.
- `deploy/pre-upgrade-backup.sh` fails too. Its backup is taken, by design, while the current
  (older) image still runs, and `restart.sh` refuses the 65 to 66 schema raise without that
  backup. The order the README documents ("make install-bin; pre-upgrade-backup.sh; restart.sh",
  with helpers updated by `install.sh`) then deadlocks as soon as the helper is updated first.

The lasting case is worse. Restoring any backup taken before this change runs that backup's own
older image (`start.sh --restored-from`). Every nightly backup of the restored service then
fails, and that is exactly the period when backups matter. It fails closed and loudly, but it
leaves the service without backups.

Fix it in one of two ways:
- (i) Have `sparrow-backup` tell an image without the closing line apart from an unclean stop by
  probing the image it recorded (its schema number, or the line's text in the binary). For an old
  image, publish the backup without `unique_payload_chunks`; `verify-backup.sh` already reports
  such a backup as predating the count. Keep the hard failure for images that do have the line.
- (ii) At least document, in README "Upgrades and rollback", RECOVERY "Restore from a backup" and
  the #25 deployment steps, that the helper must be installed only after the daemon runs a
  closing-line image, and that a restore to an older image stops nightly backups until the next
  upgrade. Patrick should choose between (i) and (ii), since (ii) accepts a gap in backups.

**OPTIONAL O1: say exactly when the line appears, or make it mean a clean exit.** Either print
the line only when `errors` is empty at that point, or have `sparrow-backup` check that the
stopped invocation exited cleanly (its "Main process exited" record, or `Result`/`ExecMainStatus`
read before the restart). Otherwise, change the docs and the comment from "clean shutdown" to
"after the final flush succeeded". The stricter form also closes the forged-line case.

**OPTIONAL O2: system scope and the parser.** Rehearse `closing_chunks` once under the system
`sparrow-backup.service` sandbox at the cutover. Note the systemd version that
`journalctl --invocation=` needs. Consider turning a `journalctl` failure into a `fail()` message
rather than a traceback.

VERDICT: BLOCKED
