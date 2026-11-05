# Delivery review: no-cow-store (7fc2886) and restart-schema67.sh

Reviewer: independent delivery review, 2026-11-04/05. Read-only: no unit was stopped or started,
and /SPARROW/store, the backups and the live config were not touched. The only metadata reads on
live paths were `stat`, `lsattr -d`, `getfacl`, `findmnt` and `df`. All scratch trees I made under
/SPARROW/cowreview-* and /tmp/cowreview-data-* are deleted. A stray
`deploy/libexec/__pycache__` that my `py_compile` run created is also deleted, and the worktree is
clean.

## Scope

1. Branch `no-cow-store`, `git diff origin/main..7fc2886`, 8 files: `deploy/lib.sh`
   (`on_btrfs`, `no_copy_on_write`, `copy_on_write_files`, `switch_off_copy_on_write`,
   `match_copy_on_write`, `convert_store_no_cow`), the new `deploy/copy-on-write.sh`,
   `install.sh`, `restart.sh`, `verify-backup.sh`, `libexec/sparrow-backup` (`copy_tree`),
   `README.md` and `RECOVERY.md`.
2. `~/.local/state/sparrow-deliver/restart-schema67.sh`, checked against `restart-schema65.sh`
   and `restart-schema65.log`, and against the installed user helper
   `~/.local/libexec/sparrow/sparrow-backup` and the user units.

The rulings (plan.md `## User rulings`) are Patrick's "Make sure that sparrow is using katamari's
COW btrfs trick on db", plus coordination's approval of +C on the store, backup and verify
directories, of conversion after the named pre-upgrade backup with the old copy kept, and of
plain copies where a reflink would cross attributes. The plan's recorded decision replaces the
plain copies with attribute matching.

## Evidence

Environment facts:
- /SPARROW (subvolume @) and ~/.local/share/sparrow/backups (@home) are on the same btrfs
  filesystem (`c738b98a73302c5a/b`, `/dev/nvme0n1p3`, `compress=zstd:1`). Disk use is 94%, with
  122 GB available.
- No C attribute is set on /SPARROW, /SPARROW/store, /SPARROW/backups, /SPARROW/verify, the
  nightly root or the 20261104 nightly. Everything is owned by paudley. /SPARROW/store is 0710
  with no ACL.
- The 20261104 nightly store is 13,191,872,512 bytes in 336 flat files, with no subdirectories
  and no socket.
- The shell that runs the restart script resolves `cp`, `mv`, `realpath`, `sort`, `date`, `cut`
  and `wc` to uutils coreutils 0.12.0 (`~/stage/root/bin`). `find` is GNU findutils 4.11, and
  `diff`, `du`, `df` and `sync` are GNU.
- The user units run with `PATH=%h/.local/bin:/usr/bin:/bin`, so they use GNU cp 9.12.
- The main checkout `/home/paudley/Active/sparrow` is on `main` at 6737fac, and
  **`deploy/copy-on-write.sh` does not exist there**.

Scratch runs on btrfs under /SPARROW/cowreview-x1MqvE (scripts in /tmp/cowreview-scripts):
- **Pre-upgrade-style backup** (uutils `cp -a --reflink=always`, COW to COW, socket present):
  exit 0, and the file list matches.
- **`copy-on-write.sh convert` with a socket in the store:** exit 1, with the message "the copy's
  bytes differ ... partial is kept". The cause is GNU `diff -r` reporting `File .../rpc.sock is a
  socket while file .../rpc.sock is a socket` (diff exit 1).
- **`convert` without a socket:** exit 0, and every file and subdirectory shows `C`.
  - Mode 0710/0640, the hard link (same inode, link count 2), the symlink, the ACL
    `user:nobody:r--` and the xattr `user.test` are all preserved.
  - The original is kept as `store.cow-STAMP`.
  - A second `convert` is a no-op (exit 0).
- **Old-helper pattern** (a +C root, a new subdirectory, GNU `cp -a`):
  - The converted store with `--reflink=always` gives exit 0.
  - The converted store with `--reflink=auto` gives `filefrag` extents flagged `shared`.
  - A COW store with `--reflink=always` into the +C root gives "failed to clone ... Invalid
    argument", exit 1, as expected.
- **verify-backup pattern** (`match` followed by uutils `cp -a --reflink=always SRC/. DIR/`):
  works from a COW source into a +C parent (match gives `-C`) and from a +C source.
- **Mixed source** (a +C directory holding one COW file): the reflink fails, as expected.
- **`off`:** on a directory holding COW files it prints a note, exits 0 and leaves the directory
  unchanged. On an empty directory it sets `C`.
- **tmpfs:** `convert`, `off` and `match` all exit 0, and `convert` prints "not on btrfs".
- **Open file:** a process holding `LOCK` open makes `convert` die with "process N/fd/3 still
  holds a file ... open" (exit 1).
- **Repository `copy_tree`** (loaded with `-B`, GNU PATH):
  - Converted to +C root, COW to +C root, converted to COW root and COW to COW root all succeed,
    and every result is `shared`.
  - Copies from tmpfs to btrfs and from btrfs to tmpfs both succeed.
- **Copy throughput** for a 2 GiB incompressible file into a +C directory: uutils 15.0 s, GNU
  24.7 s. The disk is the limit, so the rehearsal's 425 s for 13 GB is plausible.

Other evidence:
- **Linters:** `shellcheck -x` on the 5 changed shell files exits 0, and `py_compile` of
  `sparrow-backup` exits 0. `shellcheck` on restart-schema67.sh reports only SC2174 (warning) and
  SC2086 (info).
- **prose-check** on README.md, RECOVERY.md, plan.md and the commit message: exit 0.
- **Commit hook** (`/tmp/sparrow-nocow-commit.log`): release tests pass, the census is OK at
  purrdf a47940e, and the commit was made.
- **Rehearsal log:** the conversion converted 336 files in 425 s, and 0 files have COW. With
  image 6737fac, verify-backup passed query-only, store-id, status, records, artifact and
  shutdown. payload-chunks was SKIP, and the final result was **NOT VERIFIED, exit=1**, because
  the old helper's SOURCE has no `unique_payload_chunks`. The H image failed with "RDF profile
  changed", which is unrelated to the conversion and matches the known R1.
- **Socket handling in the daemon:** it removes its socket on clean shutdown and replaces a stale
  one at bind (`crates/sparrow-server/src/local_endpoint.rs:56-61, 76, 91`). So a socket is left
  in the store only after an unclean exit.

Not run: restart.sh's conversion call through a test instance, a crash or ENOSPC injection, and
anything on the live store.

## Findings: branch no-cow-store

**REQUIRED B1. `convert_store_no_cow` cannot convert a store that holds a socket, and it then
misreports the cause.**
- `cp -a` copies `rpc.sock` as a socket. `diff -r -q --no-dereference` then exits 1 on any pair of
  sockets, so the function dies with "the copy's bytes differ from DATA" (lib.sh, the second
  `diff`). This was reproduced on scratch.
- The README layout puts `rpc.sock` in the store. The daemon leaves the socket behind after an
  unclean exit (SIGKILL at TimeoutStopSec, a crash), and `drain_stop` accepts a `failed` unit.
- So the first `restart.sh` after a crash fails at the worst point. That is after the stop and
  after a full store-sized copy (about 7 minutes at 13 GB). It leaves a 13 GB `.no-cow.partial`
  and the service stopped, and the error points at corrupt bytes instead of the stale socket.
- No data is lost (DATA is unchanged), but the shipped restart path fails with a false
  diagnosis, which breaks the actionable-failure rule.
- Fix: refuse before copying with an actionable message (a stale socket; the daemon recreates it
  at bind). Alternatively, exclude sockets from the copy and from both comparisons and leave the
  daemon to recreate them.

OPTIONAL:
- **B2. Flush before the swap.** The data is synced after the swap: `mv -T data kept; mv -T
  partial data; sync -f data`. Every other publication in deploy/ (both backup helpers, and the
  restart script's own backup) syncs before it renames. If the machine crashes after the renames
  are committed but before writeback, `/SPARROW/store` could hold truncated files. The original
  is still intact as `store.cow-STAMP`. The window is small for the live store, because the 92 s
  byte comparison follows the copy, but nothing guarantees it. Run `sync -f "$partial"` before
  the first `mv`.
- **B3. Interruption between the two renames.** It leaves no `DATA`; both siblings survive. On
  the system layout, tmpfiles (`d /SPARROW/store`) would recreate an empty store at boot, and the
  enabled unit would start on it. The window is tiny. An atomic exchange (`renameat2
  RENAME_EXCHANGE`, GNU `mv --exchange` in coreutils 9.5 or later), or a RECOVERY note naming the
  two directories, would close it.
- **B4. Refusals after the stop in restart.sh.** The converter's cheap refusals (not enough free
  space, a leftover `.no-cow.partial`, and the socket of B1) only fire after `drain_stop`. That
  contradicts restart.sh's own rule that refusals happen before anything changes, and each one
  leaves the unit stopped. Check them before `drain_stop`.
- **B5. Ruling deviation not recorded.** The ruling asked for +C on the store, backup and verify
  directories in both install.sh and restart.sh. install.sh sets store and backups only, and
  restart.sh sets neither backups nor verify. Under `match_copy_on_write` the parent's attribute
  no longer decides any copy, so the effect is moot. Record the deviation in the plan's
  decisions.
- **B6. Plan acceptance wording.** The plan says verify-backup "passed every check". The
  rehearsal shows payload-chunks SKIP and a final NOT VERIFIED with exit 1, because of the old
  SOURCE. Say that.
- **B7. Missing operator documentation.** Neither README nor RECOVERY says when to remove
  `store.cow-STAMP` (only the runtime note and a lib.sh comment do). Neither says that the first
  restart.sh after deployment adds the conversion time, measured at about 33 s per GB.
- **B8. Directories are not counted.** `copy_on_write_files` counts only regular files. A COW
  subdirectory in a +C store would be missed by `off` and by `convert`'s "already off" shortcut,
  and files created under it later would be mixed. The store is flat today.

## Findings: restart-schema67.sh

**REQUIRED S1. The converter's existence is checked only after the downtime begins.**
- The script calls `$REPO/deploy/copy-on-write.sh` with `REPO=/home/paudley/Active/sparrow`. The
  main checkout is at 6737fac, where that file does not exist.
- Run before the branch lands and the main checkout is updated, the script would:
  1. stop both timers and sparrow.service;
  2. take and publish `pre-upgrade-sparrowd-schema65-0221e87`;
  3. die with exit 127, leaving the service and timers stopped.
- A rerun is then blocked by the script's own "backup target exists" check.
- Fix: add `[[ -x $REPO/deploy/copy-on-write.sh ]]` to the preconditions. Also confirm that REPO
  holds the reviewed converter (at least 7fc2886, or the merged fix for B1), or point the script
  at that revision explicitly. Checking `[[ -d $NIGHTLY ]]` there as well costs nothing.

Verified as correct:
- **Ordering.** The closed-store backup is taken and published (sync, then rename) before the
  conversion. It stays copy-on-write: /SPARROW/backups has no `C`, and the uutils
  `--reflink=always` copy from COW to COW works.
- **The way back.** The original store is also kept as `store.cow-STAMP`, which gives a second
  schema-65 copy.
- **Nightly reflinks.** After `chattr +C ~/.local/share/sparrow/backups`, the installed old
  helper's `.partial-STAMP` (made with `Path.mkdir`), its `store` (made by GNU `cp -a`) and the
  files all inherit `C`. Its `cp -a --reflink=auto` from the converted store then shares extents.
  This was reproduced, and `chattr +C` on a non-empty directory succeeds.
  - Only the intake (empty) and the config trees (about 3 MB, copy-on-write sources) are copied
    in full.
  - Its free-space check needs about 36 GB, against about 109 GB left after conversion.
  - The verify-backup reflink of the new nightlies, and of the COW pre-upgrade backup, works
    through `match` regardless of the /SPARROW/verify attribute.
- **Open store.** `ps -C sparrowd` plus the converter's /proc fd scan, which as paudley sees
  every process of the unit's user.
- **Permissions.** Owner, mode, ACLs and xattrs are preserved, as shown on scratch. The socket is
  removed by the clean stop.

OPTIONAL:
- **S2. No recovery hint after the stop.** Every failure after `systemctl --user stop
  sparrow.service` exits with the service and both timers stopped. The way back is still open at
  each step: the daemon link still names the old image until the `ln -sfn`, and the store is
  unchanged or kept. The script should print the manual recovery step: `systemctl --user start
  sparrow.service sparrow-ship.timer sparrow-backup.timer`, after renaming a missing store back if
  the swap was interrupted. Do not restart automatically when /SPARROW/store is missing.
- **S3. A stale socket check.** `settled` accepts `failed`, so an unclean stop could leave
  `rpc.sock`, which hits B1 after the backup and a 7-minute copy. Until B1 is fixed, add `[[ ! -e
  /SPARROW/store/rpc.sock ]]` right after the stop.
- **S4. Check for other daemons before the stop.** Run the `ps -C sparrowd` check before the
  stop too. A verify-backup daemon from another session would otherwise abort the script after
  the downtime begins.
- **S5. The readiness loop (carried over from restart-schema65.sh).** After 15 minutes it falls
  through. With an empty `out` it reports "STORE ID CHANGED", and with a non-ready `out` it
  restarts the timers.
- **S6. Downtime.** The inputs hold no written downtime claim; the only figure is 425 s, which is
  the conversion alone. The total is roughly:
  - the stop, about 35 s (11-02);
  - the reflink backup, about 60-70 s (11-02 took 72 s for 492 files);
  - the conversion, about 425-430 s, plus the lsattr pass;
  - the start and the migration from 65 to 67 (11-02's start took 117 s).

  That makes about 9 to 11 minutes, roughly 2.5 times the 226 s of 11-02. Any announcement should
  give that figure, not 425 s. The script's own T0 to T1 measurement covers the whole window, so
  it reports the real figure.

VERDICT: BLOCKED

## Round 2 (recheck of B1 and S1 only)

Scope: `git diff 7fc2886..f18bc12` (`deploy/lib.sh` `convert_store_no_cow`, one README
sentence), and the revised `~/.local/state/sparrow-deliver/restart-schema67.sh`. The restart
script was read, not run. The scratch trees under /SPARROW/cowreview2-* and their scripts are
deleted.

### B1: resolved

The copy now drops sockets (`find "$partial" -type s -delete`). The file-list comparison leaves
out sockets on the source side and directory sizes on both sides, and it compares symlink targets
(`%l`). Each regular file is compared with `cmp -s`. The loop reads from a process substitution
rather than a pipe, so `die` exits the script. The copy is synced (`sync -f "$partial"`) before
the swap, which also closes B2. Scratch runs on btrfs with the uutils cp from the operator PATH:
- **Store with a stale `rpc.sock`, a subdirectory, a name with a space, a hard link, a symlink,
  an ACL and an empty file:** exit 0.
  - The converted store has no socket and 0 copy-on-write files, and it carries `C`.
  - The modes 0710 and 0600, link count 2, the symlink target `a.sst` and the ACL
    `user:nobody:r--` are preserved.
  - The kept original still holds its socket.
- **A same-size byte change injected into the copy** (a cp shim): "the copy of g differs ...",
  exit 1. The original is unchanged and the partial is kept.
- **A symlink target changed in the copy** (a shim): "the copy's file list differs", exit 1.

OPTIONAL, new in this round:
- **Filesystem syncs and downtime.** On this loaded host, a 5 MB scratch conversion took 90 s,
  mostly in the two whole-filesystem `sync -f` calls while sibling builds were writing. The live
  conversion pays the same two syncs, so add up to a minute or two to the downtime estimate.
- **A newline in a file name.** `copy_on_write_files` parses `lsattr` output by line, so a file
  name containing a newline is miscounted, and the conversion refuses (it fails safe). This code
  was there before f18bc12, and the store's names (`NNNNNN.sst` and similar) cannot contain a
  newline.

### S1: resolved

The script now checks the following before anything changes:
- that `$REPO/deploy/copy-on-write.sh` is executable;
- that `$REPO/deploy/lib.sh` contains the f18bc12 marker `find "$partial" -type s -delete`;
- that `$NIGHTLY` exists;
- that /SPARROW/store is on btrfs;
- that free space exceeds the store plus 10 GiB.

An EXIT trap prints the failure and the unit's state, and the header comment gives the recovery
steps. Confirmed:
- The main checkout today has neither the converter nor the marker (`grep -c` gives 0), so the
  script would refuse before stopping anything.
- The f18bc12 worktree has the marker (1).
- `rc` is assigned inside the trap, so SC2154 is a false positive. SC2016 is intended (a literal
  grep), and SC2174 and SC2086 are harmless.

As the coordinator notes, the script must run from a main checkout that holds f18bc12 or a later
commit.

Findings, round 2: no REQUIRED findings.

VERDICT: PASS
