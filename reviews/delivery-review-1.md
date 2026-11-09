# Delivery review 1: #156 Back up Sparrow only to the NAS archive

Reviewer: independent delivery reviewer. Read-only. No cargo, make or build-system probing, and
nothing touched on the live store, daemon, units, timers or `/net/synology`.

## Scope

- Issue #156 verbatim (`issue.md`), as changed by the binding rulings in `plan.md`
  `## User rulings`: fully online, never stop, pause or perturb ingestion, no closed-store
  window, never lose data or drop packets, NAS only, no local backup copies. Coordinator ordering
  (#160, #158, #156) and the instruction to stop before ghprsq are noted.
- The diff: worktree against origin/main ea1a4b2, uncommitted, plus three untracked files
  (`review.patch`, 35 files, +1488/-996), all of it read.
- Checked against current source: `deploy/systemd/sparrow.service`, `deploy/apparmor/sparrow`,
  the host's `/etc/apparmor.d/abstractions/base`, `crates/sparrow-core/src/storage.rs`
  (`linked_physical_bytes`), `storage/pressure.rs` (admission and eviction), `docs/deployment.md`
  (space floor), `crates/sparrow-server/src/{main,config,rpc,ingest_gate,query_only}.rs`,
  `deploy/{lib,test-instance,verify-backup}.sh`, and the vendored RocksDB 11.8.1
  `utilities/backup/backup_engine.cc`.
- Not run: any cargo test, Clippy, `make check`, or the scripts. `prose-check` was run on the
  changed docs and sources. It exited 1, but every hit is on an unchanged line (`UInt64`/`UInt32`
  in `docs/interfaces.md` and `docs/development-runtime.md`), so the diff adds no prose finding.

## Evidence (from validation.md and logs/)

| Claim | Log | Exit | What it proves |
| --- | --- | --- | --- |
| Release build of `sparrow-server` bins | build-release.log | 0 (index) | It compiles. Nothing about runtime. |
| Core `backup` filter: 1 unit test, 2 storage tests | test-core-backup.log | 0 | Mount-table rule. Store-API online backup beside ingest on a 32 MiB store with a 1 GiB budget (no refusals, latency within bound, restore consistent). Store-API local-root refusal creating nothing. |
| Server rpc tests (first run) | test-server-backup.log | 101 | A stale manifest in the fixture; fixed. |
| Server rpc tests (rerun) | test-server-backup-2.log | **not recorded** | "5 passed", but the log has no EXIT line and validation.md still says "pending". |
| shellcheck, `bash -n`, py_compile | shellcheck.log | 0 (index) | The log holds only the tool path, so it is syntax evidence only. |
| `make check` | none | not run | Planned for the commit hook. |

Nothing executes the `backup` RPC, `sparrow SOCKET backup`, `Status.backup` rendering, the
config-to-`main.rs` wiring, or the success path of `deploy/libexec/sparrow-backup`. Every test
runs unconfined: no systemd sandbox and no AppArmor.

## Acceptance mapping

| Requirement (issue as ruled) | Delivered | Evidence |
| --- | --- | --- |
| Backup only to `/net/synology/archives/sparrow`, never local | Daemon checks mountinfo at every backup (before creating anything and again on the canonical path). Client checks with findmnt before asking. No local path is left in the units, tmpfiles or install. | Store-API test and client-refusal test. Through the RPC: none. |
| Online: no stop, pause or perturbation, no closed window | BackupEngine on a nice-19 thread over the shared handle, no writer lock, no flush | 32 MiB latency test. **Not** established for a store near its budget or under the shipped unit (R1 to R3). |
| Rename into place only after write and sync on the NAS | `sets/ID.partial`, sync, manifest read back from the NAS, rename | Not executed (needs NFS). Post-deploy. |
| Pruning, SOURCE, images, lexicons and config on the NAS | Yes | Not executed. Post-deploy. |
| Repository script and units updated together; installed drift removed | Repository side done. Installed user unit and script are post-merge (D147). | Docs give the lines. |
| `user-unit-upgrade.sh` pre-upgrade backup to the NAS | Online via the client; `--no-backup REASON` | Refusal tests only. |
| `verify-backup.sh` verifies a NAS backup | Manifest check, `--restore-backup` with checksums, query-only checks | `verify_backup_fails_on_wrong_store_id` (local fixture), rerun exit not recorded. |
| Docs: deployment.md, README.md, RECOVERY.md | Updated | Inaccuracies in R3 and R6. |
| A completed NAS backup, verified; timer restarted after merge | Post-deploy by design (issue and plan) | Correctly deferred. |

**Schema 70 decision: acceptable.** Commit 50d9734 raised the schema to 70 together with every
other schema 70 wire field. The precedent for adding wire surface without a raise is 4b108a9
(#36, 11 capnp lines added under schema 69). No store format changes. Callers detect the feature
by capability, not by number: `user-unit-upgrade.sh` reads `status.backup` and treats an absent
key as no backup, and `verify-backup.sh` looks for `--restore-backup` in the usage text. The live
daemon runs schema 68.

## Findings

### R1. REQUIRED: under the shipped system unit, starting a backup kills the daemon (SIGSYS)

`deploy/systemd/sparrow.service:123-124` sets `SystemCallFilter=@system-service` and
`SystemCallFilter=~@privileged @resources`, with no `SystemCallErrorNumber=`. systemd's default
for a filtered call is to kill the process with SIGSYS. `setpriority` belongs to `@resources`.
The first thing `storage/backup.rs::run` does is
`rustix::process::setpriority_process(None, LOWEST_PRIORITY)`. So the first `backup` request
under `sparrow.service` kills `sparrowd`. In-flight RPCs fail, syslog UDP is lost until
`Restart=on-failure` brings it back, and the next nightly run does it again. This breaks
"no backup may EVER stop lillith ... or drop packets". The tests cannot see it because they run
unsandboxed.

Required:
- Make the call legal in the unit (for example, add `setpriority` back after the deny line), or
  get the low priority another way.
- Before D147, confirm the operator's installed user unit `sparrow.service` carries no filter
  that kills on `setpriority`.

### R2. REQUIRED: the enforced AppArmor profile denies `/proc/self/mountinfo`, so every system-unit backup fails

`read_mounts()` reads `/proc/self/mountinfo`. `deploy/apparmor/sparrow` grants only
`@{PROC}/@{pid}/fd/`, `.../task/@{tid}/comm` and `.../sys/kernel/random/uuid`. On this host,
`abstractions/base` grants only `@{PROC}/@{pid}/{maps,auxv,status}` and has no `mount` rule at
all. The profile runs in enforce mode (`AppArmorProfile=sparrow`), so every backup under the
system unit ends `failed: cannot read /proc/self/mountinfo`. That fails safe, but the shipped
production path cannot work.

Required: add `@{PROC}/@{pid}/mountinfo r,` with its reason. R1 and R2 together mean the
daemon's backup has never run under its own sandbox. Say so in validation.md, and make the D147
first run the explicit check.

### R3. REQUIRED: the backup pins obsolete files for its whole copy, which can refuse ingest, trigger eviction and drop syslog packets

RocksDB 11.8.1 `backup_engine.cc` calls `DisableFileDeletions` at line 1240 and re-enables it at
line 1407, only after every copy item finishes (line 1380). The module doc and `interfaces.md`
admit this ("physical_bytes can grow until it finishes"). The doc does not follow through on what
that growth does in Sparrow:

- **Admission.** `linked_physical_bytes` (`storage.rs:1319` onward) walks the database directory,
  so pinned files count. Once they cross `disk_budget - maintenance_reserve`, `admit_data_write`
  (`pressure.rs:160-185`) refuses data writes with `STORAGE_PRESSURE`. The shipped template has a
  50 GiB budget and a 64 MiB reserve, and a retention store runs near its budget by design.
- **Eviction.** `maintain_pressure` then runs `evict_oldest` (`pressure.rs:262`) and retires
  evidence that a run without a backup would have kept. That is data loss caused by a backup.
- **Free-space floor.** Pinned bytes are real disk. Below `min_free_bytes`, the daemon stops
  accepting Unix and TCP connections and refuses syslog UDP (`docs/deployment.md:104-120`), so
  packets drop.
- **Duration.** The first backup copies the whole store over NFS with sync on each file.
  `TimeoutStartSec=6h` shows the expected order of magnitude. A hung NFS mount pins deletions
  with no bound at all: there is no timeout and no stop.

`deploy/RECOVERY.md` "NAS archive unavailable" says "Effect: none on the service" and in the same
paragraph says a hung backup "holds that space". The 32 MiB / 1 GiB test cannot reach this
regime.

Required:
- A design under which a backup cannot cause refusals, eviction or a floor breach. Options are
  for the implementer to choose. Examples: keep backup-pinned files out of the budget and
  eviction measurement, and bound or abort the backup before free space nears the floor. The
  vendored `librocksdb-sys` exposes `rocksdb_backup_engine_stop_backup` and a
  create-backup progress callback.
- Evidence with a store at or near its budget, with compaction running during the backup:
  no `STORAGE_PRESSURE`, no evicted records.
- Docs that state the real effect.

### R4. REQUIRED: the new entry points have no execution evidence, and the validation record is incomplete

The `backup` RPC (`rpc.rs`), the CLI `sparrow SOCKET backup` (`agent/backup.rs`), the
`Status.backup` encoding and rendering, `ServerConfig::backup_target`, and the `main.rs` wiring
(`with_backup`, `without_backup_for_no_intake`, query-only) are compile-only. The client relies
on their JSON shape: `phase`, `started_at`, `finished_at`, `backup_id`, `files`, `bytes`,
`failure`, plus `store_id`, `schema_version` and `store_format` from `status`.

Required:
- One focused daemon-level case. For example: a daemon configured with a local `backup_root`
  answers `backup` with `running`, then `status.backup` reports `failed` naming "not a network
  filesystem", and nothing is created. A daemon without `backup_root` refuses with `UNSUPPORTED`.
  That proves the daemon's local-disk refusal through the real entry point and pins the shape the
  client parses.
- Record the exit status of the `test-server-backup-2.log` rerun.
- Record the commit-hook `make check` result in validation.md before landing.

### R5. REQUIRED: config sequencing in the D147 window can leave the daemon unable to start

`ServerConfig` is `#[serde(deny_unknown_fields)]` (`config.rs:17`). Any image without this change
refuses to start once `backup_root` or `backup_keep` is in `server.toml`. That covers the live
schema 68 FROM image and schema 70 builds from before #156. The docs tell the operator to add the
keys (README "Interim user unit", deployment.md "Online backups") without saying so.

- With `--no-backup`, the script never checks `backup_root`. Yet the run ends by telling the
  operator to "Take the first online backup now". That only works if the key was already in
  `server.toml` when TO started; otherwise it needs another restart.
- If the key was added early, a FROM crash-restart during the window, or the documented rollback
  to FROM, fails to start on an unknown key.

Required:
- Document the order: add the keys at the switch to TO, and remove them before any rollback to an
  older image.
- Make the `--no-backup` path check that TO's config sets `backup_root`, or warn loudly when it
  does not.

### R6. REQUIRED: docs that misstate the shipped behavior

- `deploy/config/server.toml.example` says "Startup refuses a root that is not on a network
  filesystem ... or that shares the store's filesystem". Startup checks only the paths; the
  filesystem rule is applied at every backup.
- `deploy/README.md` step 3 says "The template does not set the online backup, so add
  `backup_root = ...`". The template now sets both keys.
- The RECOVERY "Effect: none on the service" statement is covered under R3.

### O1. OPTIONAL: the rollback window shrinks

Pre-upgrade backups used to be kept until removed by hand. Now the engine purges them after
`backup_keep` more backups (about three nightly runs), and the client then deletes the set, so
`pre-upgrade-IMAGE` dangles. Consider excluding the pre-upgrade backup from the purge, or state
the window plainly in RECOVERY "Format and rollback".

### O2. OPTIONAL: a consistency check was dropped

The old client refused when the image link's schema number differed from the running daemon's
`schema_version`. The new client records `os.readlink` without that check. `status` already
carries `schema_version`, so restoring the check is cheap.

### O3. OPTIONAL: two implementations of one rule, and ledger gaps

The network-filesystem rule and its `NETWORK_FILESYSTEMS` list exist twice: Rust over mountinfo
and Python over findmnt. The `online-store-backup` ledger row says "one network-filesystem
rule". Either record the client check as a narrow distinct row, or reword the row. The row's
`vectors` lists `crates/sparrow-server/src/config.rs`, which has no backup test, and the path
checks in `BackupTarget::lexical` (relative, `..`, nesting, `keep = 0`) have no test.

### O4. OPTIONAL: the verify fixture mirrors the client's manifest code

`write_backup_manifest` in `tests/rpc.rs` re-implements the client's manifest format in bash, so
a drift in `sparrow-backup` would not fail any test. The client's success path first runs at
D147. That is accepted by the plan, but the dependence should be stated in validation.md.

### O5. OPTIONAL: lifecycle scripts now depend on the NAS

`hold_backup_lock` in `start.sh` and `restart.sh` now opens and flocks
`/net/synology/archives/sparrow/.lock`. A hung hard mount blocks those scripts, and a
root-squashed export makes them die, so the recovery scripts depend on the NAS. A local lock
file is not backup bytes and would remove the dependency.

### O6. OPTIONAL: NFS permissions are unresolved

The backup unit runs as root and writes the NAS root, which the docs say is owned by the daemon's
user with mode 0700. The unit comment admits the export must let root write. Settle the squash
mapping before D147.

### O7. OPTIONAL: test-instance.sh is stale

`deploy/test-instance.sh:45` still exports a local `SPARROW_BACKUPS=$SCRATCH/backups`, and its
header still claims to rehearse `pre-upgrade-backup.sh`, which can no longer run on a local root.

### O8. OPTIONAL: an I/O priority claim depends on the scheduler

The claim that I/O priority follows nice 19 holds only under BFQ. With none or mq-deadline it has
no effect. Qualify the claim, or drop it.

### O9. OPTIONAL: repeated requests can purge older backups

Any full-service peer, including CHAP-authenticated TCP, can request backups, and each one can
purge an older backup (`keep = 3`). Consider whether `backup` should require the identified
administrator, as `watchClaimRetire` does.

VERDICT: BLOCKED
