# Delivery review: #330, remove the online store backup and guard the archive mount

- Head reviewed: `4a9b1f65ff1d263733017193a5c3bb17f2ae5ca9` on `paudley/330-remove-the-online-store-backup-156-and`
- Base: `2116021cb1acf8e3f24e21a52bc7981a1c11a359`
- Worktree: `/home/paudley/Active/sparrow/.worktrees/330-remove-the-online-store-backup-156-and`
- Reviewer: independent, read-only. I ran no cargo, no build and no test, and I edited nothing in the worktree.

## Scope

What I read:
- Issue #330, verbatim, from `tea issues --login fleet --repo fleet/sparrow 330`. It has no comments.
- The full commit message.
- The diffs of `sparrow.capnp`, `rpc.rs`, `query_only.rs`, `agent/{mod,status,backup}.rs`, `backup.rs`, `lib.rs`, `shutdown.rs`, `archive.rs`, `config.rs`, `main.rs`, `sparrow-core` `storage.rs`, `lib.rs` and `space.rs`, the recovery example, and `helpers-ledger.toml`.
- Every deploy script in the diff: `user-unit-upgrade.sh` in full, `lib.sh`, `start.sh`, `restart.sh`, `stop.sh`, `check.sh`, `install.sh`, `uninstall.sh`, `test-instance.sh` and `libexec/sparrow-ship`. Also the units, the AppArmor profile, `tmpfiles` and `server.toml.example`.
- `tools/user-unit-upgrade-timers-check.sh` in full.
- The diffs of `deploy/README.md`, `MANUAL.md`, `RECOVERY.md`, `docs/deployment.md`, `interfaces.md`, `load-qualification.md`, `development-runtime.md` and `archive-format.md`. I read the old and new "Revert the cutover" sections in full.
- From the base revision: the header of `reconcile-inflight.sh`, the header of `sparrow-measure-load`, and "Claims after a rollback" in `docs/deployment.md`.
- The names of the removed tests in `tests/rpc.rs` and `sparrow-core/tests/storage.rs`.
- Evidence logs under `.stage/remove-the-online-store-backup-156-and/logs/`: the head and tail of `commit.log`, and `timers-check2.log`.

What I did not read: the body of every deleted test, line by line (I checked their names and purposes); `deploy/evidence/*` (dated history); most of `docs/development-runtime.md`.

Read-only host facts used for check 2. I ran `stat`, `findmnt` and `ls`, and read the live user configuration. I changed nothing:
- `/` is btrfs `@`, dev 39. `/net` and `/net/synology` are also dev 39. `/net/synology/archives` is an nfs4 mount of `192.168.1.1:/volume1/archives` (dev 180).
- `/home` is a separate btrfs subvolume mount (dev 55).
- `/net/synology/archives/sparrow-archive` does not exist.
- `/net/synology/archives/sparrow` holds `engine/`, `sets/`, `.lock`, `STORE_ID` and `pre-upgrade-sparrowd-schema71-a77756a`. It holds no per-store archive directory.
- `~/.config/sparrow/server.toml` sets `backup_root` and `backup_keep` and has no `[archive]` table.
- `~/.config/sparrow/ship.toml` has `state_dir = /home/paudley/.local/state/sparrow/ship`.
- The user units `sparrow-backup.{service,timer}` are still installed, disabled and inactive.

## Evidence

- `commit.log`: the pre-commit hook ran `cargo fmt --check`, release Clippy with `-D warnings`, the tests, the census self-test and `helper-census --cross --check`, all in this worktree. The commit `4a9b1f6` was created, so the hook exited 0. The new test `archive::tests::an_archive_directory_on_the_root_device_is_refused_before_anything_is_written ... ok` is in the log at line 845.
- `timers-check2.log`: `tools/user-unit-upgrade-timers-check.sh` ran three cases against stubs and printed `PASS`. This covers the script's timer control flow, not a host run.
- `prose-check` on the changed documents reports only `UInt64` and `UInt32` joins in `docs/interfaces.md`, on lines this change did not touch. The commit message is clean (exit 0).

## Checks the caller asked for

1. Ordinals: pass.
   - The `.capnp` diff changes comments only. `backup @53` and `Status.backup @55` are unchanged, and so are `BackupStatus` and `BackupPhase`.
   - `rpc.rs` answers `backup` with `UNSUPPORTED` and encodes `Status.backup` as `unconfigured` with an empty root and an absent failure. That is the same encoding the old `Backup::Unconfigured` produced.
   - `ingest_gate.rs` keeps `backup` in its `refuse` list. `query_only.rs` keeps it in `writes`, so a query-only daemon answers `READ_ONLY`.
   - Nothing in the CLI, the MCP or the scripts still reads the status `backup` key.
2. Mount guard: pass.
   - `archive_segment` checks, in order: the root is a directory, then `require_mounted` compares the `st_dev` of `dir` and of `/` through `fs::metadata`, which follows symlinks. Only then does it read `STORE_ID` and create anything.
   - Mounted share: dev 180 is not dev 39, so the guard accepts.
   - Unmounted share with a stray local `sparrow-archive`: dev 39 equals dev 39, so it fails with the named `NotOnMount` error. If no local directory exists, it fails with `Io` "not a directory; is it mounted?".
   - Symlink: the guard judges the target's device.
   - `/` or `dir` cannot be stat'd: the result is `Io { step: "read device" }`. That fails closed, under the `Io` name rather than `NotOnMount`.
   - The guard writes, moves and deletes nothing. A failed pass is journaled and retried by `run`, so the daemon keeps serving (degraded, not down).
   - `STORE_ID`: no code writes it now. The README, `docs/deployment.md`, `docs/interfaces.md` and the refusal message all say the operator writes it once (this store's id and a newline).
3. Deploy scripts: no dangling function, file, variable or flag. `SPARROW_BACKUPS`, `SPARROW_INTAKE`, `hold_backup_lock`, `require_pre_upgrade_backup`, `--restored-from`, `--no-backup` and the backup timer are all gone from scripts and tools. The upgrade-script control flow I read has a precondition gap; see R1.
4. Extra removals:
   - `reconcile-inflight.sh` served only a rollback that restores an older store from a backup ("Claims after a rollback"). With no restore path left, that mismatch cannot arise. Its only test was removed with it.
   - `sparrow-measure-load` was called only by `verify-backup.sh --with-traffic`.
   - No document links to either anymore.
5. Tests and ledger:
   - The removed tests cover only the backup engine, the backup client and RPC, `verify-backup`, the measurement and `reconcile missing`.
   - `percentile` and `SpaceFloor::min_free` were used only there; release Clippy with `-D warnings` would reject dead code.
   - The removed `online-store-backup` ledger row is the only backup row, and the census check passed.
   - The removed backup code wrote no keys to the store, so the stored format is unchanged.
6. "Revert the cutover": incorrect. See R2.

## Findings

**R1. REQUIRED. The live upgrade path stops the daemon before the new image refuses the live configuration.**
- `~/.config/sparrow/server.toml` sets `backup_root` and `backup_keep`. The previous `user-unit-upgrade.sh` required them.
- `ServerConfig` has `deny_unknown_fields`, so an image built from this change refuses that file at load.
- `deploy/user-unit-upgrade.sh` no longer checks for these keys and never validates the configuration against `--to`. It stops `sparrow.service`, swaps the links, and then fails at "sparrow.service is failed before its first status answer".
- The result on the next live switch is an outage that is certain and avoidable. The script's own claim, "Every check that can refuse runs before anything changes" (line 15), becomes false.
- Neither the README's "Interim user unit" section nor the `user-unit-upgrade.sh --help` text tells the operator to delete the lines first. Only `docs/deployment.md`, "Archive and backups", says "delete the lines".
- `start.sh` and `restart.sh` have the same gap for an `/etc/sparrow/server.toml` made from the old template, but no system unit is installed today.
- Fix: add a precondition before "stop timers" that refuses when `server.toml` names `backup_root` or `backup_keep`, with the message "delete these lines first". Deleting them is safe for the `FROM` image, because both keys are optional there. Add the same line to the README and `--help`, and preferably to `start.sh` and `restart.sh` as well.

**R2. REQUIRED. The rewritten "Revert the cutover to the user unit" in `deploy/RECOVERY.md` is wrong for an unfenced store and leaves a fenced store with no stated way out.**
- The revert now keeps the cutover store and its `/SPARROW/intake` in place, which is coherent for claims. But it only says to "point the user shipper's `ship.toml` at `/SPARROW/intake`". It no longer moves `state_dir`.
- The user `ship.toml` has `state_dir` on `/home` (dev 55), while `/SPARROW` is on dev 39. The template itself says "state_dir must be on the same filesystem as intake_root: items are renamed into the roots". `sparrow-ship` uses `os.rename` (lines 239 and 277). So the user shipper fails with EXDEV.
- Even on one mount, the pre-cutover ledger would resend every file the system shipper already delivered into the kept store, which double-ingests it. The old text wanted that resend because its store was a restored, older copy. The kept store already holds those records.
- The procedure should point the user shipper at the system shipper's state, `/SPARROW/ship`. `uninstall.sh` keeps that directory, and `tmpfiles` makes it `paudley:sparrow` 2700, so the operator can use it.
- Fenced store: the text says the user unit's image "must be able to open it" but gives no action. It should say that if the system unit's image upgraded the format, the user unit's links must first point at that image or a newer one; the old user image is refused by the store fence. `user-unit-upgrade.sh` cannot do this, because it requires a running daemon that answers status.
- The new store-and-intake text also says nothing about removing `backup_root` and `backup_keep` from the user `server.toml` (see R1).

**R3. REQUIRED. `docs/archive-format.md` still names `/net/synology/archives/sparrow` as the archive root, while every other document now says `/net/synology/archives/sparrow-archive`.**
- Line 20 is in the list "Patrick's rulings bind this design", and this change left it untouched.
- The issue schedules `/net/synology/archives/sparrow` for deletion by exact path after the merge. That tree already holds a `STORE_ID` naming this store, written by the backup engine.
- So an `[archive]` table configured from the format document would pass both the mount guard and the `STORE_ID` check, and would archive evidence into a tree that is about to be deleted. The store retires records once they are archived, so that evidence would be lost.
- Nothing is archived there today (I checked the listing, and the live configuration has no `[archive]` table).
- Fix: make the format document agree, and say why the root moved (the old path is the backup engine tree that is to be deleted). If the path change itself needs a ruling, get it from coordination.

**O1. OPTIONAL.** `docs/deployment.md`, "Recovery qualification": the sentence now reads "Keep the source immutable. The recovery The recovery example uses ..." with "The recovery" doubled.

**O2. OPTIONAL.** `docs/interfaces.md`, "Removed online backup", and the `.capnp` comment say "the server answers every call with `UNSUPPORTED`". A query-only daemon answers `READ_ONLY`, and the ingest socket and anonymous TCP answer their endpoint refusal.

**O3. OPTIONAL.** The guard proves only "not on `/`'s device". On this host `/home` is a separate btrfs subvolume (dev 55), so a local directory under `/home` would pass. That does not matter for the documented layout. A filesystem-type check (`statfs` against NFS) would close it. The issue left the mechanism to the implementer.

**O4. OPTIONAL.** The installed user units `~/.config/systemd/user/sparrow-backup.{service,timer}` (disabled) and `~/.local/libexec/sparrow/sparrow-backup` are not in the repository and no document mentions them. A one-line note to remove them would close the loop. `install.sh` and `uninstall.sh` no longer touch a system `sparrow-backup.*` that an earlier install might have left; none is installed on this host.

**O5. OPTIONAL.** `docs/development-runtime.md` lines 17 to 20 still say "The daily backup restarts the daemon ... backups are active". This was already stale before the change.

Pre-existing and unrelated, reported only: `sparrow.service` filters `~@resources`. RocksDB's `lower_thread_pool_cpu_priority` (in `table_options.rs`, since `035b826`) calls `sched_setscheduler`, which `@resources` contains, before `setpriority`, so the system unit would be killed on the first low-pool thread both before and after this change. Removing `SystemCallFilter=setpriority` neither causes nor fixes that. It is not live, because the store runs under the user unit.

VERDICT: BLOCKED
