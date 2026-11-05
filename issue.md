TITLE: Open a restored store query-only, and use it to verify every backup
## Summary

Nothing can open and query a copy of the store today. Starting `sparrowd` on a copy makes the copy consume the live intake folders, because the copy's watch catalog names them, so the deployment guide forbids it. The consequence is that a backup is only ever checked by file listing: no one can show that a backup opens, answers queries and holds the data it claims. Add a query-only mode to `sparrowd` that starts no ingest or maintenance worker and refuses every write, and add a script that uses it to verify a backup end to end. The same mode measures read latency on an old-format copy.

## Current behavior

- `sparrowd` always starts the consume worker from the stored watch catalog (`crates/sparrow-server/src/main.rs:61`), the syslog and TCP listeners from its configuration (`main.rs:24-34`, `main.rs:87-96`), and the maintenance worker, which runs expiry, eviction and the table rewrite (`main.rs:137`). There is no configuration key that turns any of them off.
- `deploy/RECOVERY.md`, "Restore from a backup", says never to start `sparrowd` on a copy because its watch catalog names the live consume roots.
- The only check of a copy is the recovery probe `sparrow-recovery-schemaN-REV`, which answers for named evidence ids. It does not run the windowed `records`, `keys` or `sparql` queries that operators and the acceptance tests use.
- `deploy/libexec/sparrow-backup` copies the closed store and records the image in a `SOURCE` file. Nothing opens the copy afterwards.

## Work

1. Add `query_only = true` to the server configuration (`crates/sparrow-server/src/config.rs`) and a `--query-only` flag on `sparrowd` that sets it. The mode:
   - starts no consume worker, tail worker, syslog listener, TCP listener, maintenance worker or table-rewrite step;
   - refuses to start when the configuration also sets `consume`, `syslog`, `rpc_tcp_address` or `archive`, with a message that names the conflicting key;
   - serves the Unix socket, and answers every read operation exactly as a normal daemon does;
   - refuses every operation that writes, including ingest, import, flush, pins, watch changes and time or diagnostic definitions, with a typed `READ_ONLY` error whose message says the daemon is in query-only mode;
   - opens the store so that it writes nothing to the store directory beyond what opening an existing store requires, and says so in `docs/interfaces.md` if opening still writes the log files.
   Status reports `query_only` as true.
2. Add `deploy/verify-backup.sh BACKUP`, run as the operator. It copies the backup's `store` with `cp --reflink=always` into a scratch directory below `/SPARROW/verify/`, writes a configuration that points at that copy, a socket path in the scratch directory, and the lexicon bundle the backup's `SOURCE` file names, and starts the image recorded in the backup's `images/` directory in query-only mode. It then checks that:
   - the daemon opens the copy and the store id equals the `store_id` in `SOURCE`;
   - `status` answers `ready`;
   - a clock-independent catalog count recorded in `SOURCE` equals the copy's (live record counts fall as records expire after the backup, so they cannot be compared);
   - when the backup is younger than the store's retention window, a windowed `records` read over the backup's newest hour returns rows, and one artifact's bytes read back to a digest equal to the digest of its record; for an older backup the script prints that these checks were skipped because the window has aged out.
   It prints one line per check, stops the daemon with SIGINT, removes the scratch copy, and exits nonzero at the first failed check. It never starts a daemon with the live store, the live socket or the live intake folders.
3. Make `sparrow-backup` record that clock-independent count in `SOURCE`, so the script has a value to compare.
4. `deploy/RECOVERY.md` and `docs/deployment.md`: replace "never start `sparrowd` on a copy" with the query-only procedure and the new script, and state that a backup counts as verified only after the script passes on it.
5. `docs/interfaces.md`: add the `READ_ONLY` error and the `query_only` status field, which advances the schema by one.

## Out of scope

- Restoring a backup over the live store (the existing procedure).
- Verifying the archive segments on the NAS; the archive issues cover those and will call this script's checks.

## Depends on

Nothing.

## Automated acceptance

- `query_only_daemon_serves_reads_and_refuses_writes`: in `crates/sparrow-server/tests/rpc.rs`, a daemon started with `--query-only` on a copy of a store that has a registered watch answers `status` and `records`, reports `query_only` true, claims nothing from the watch root (a file placed there stays), and refuses `ingest`, `watchPut` and `pin` with `READ_ONLY`.
- `query_only_refuses_conflicting_configuration`: a configuration with `consume` or `rpc_tcp_address` plus `--query-only` exits nonzero and names the key.
- `verify_backup_fails_on_wrong_store_id`: the script exits nonzero when `SOURCE` names another store id.
- `make check` passes.

## UAT

Run on this host after deployment. Step 1 uses the first nightly backup taken after the deployment, because only the new `sparrow-backup` records the count the script compares and only a new image has the query-only mode.

0. Set up:
   ```bash
   cd /home/paudley/Active/sparrow
   # Until the system-unit cutover, the live configuration and intake belong to the user unit.
   [ -r /etc/sparrow/server.toml ] || export SPARROW_CONFIG=~/.config/sparrow/server.toml SPARROW_INTAKE=~/.local/state/sparrow/intake
   NEW=$(ls -d ~/.local/share/sparrow/backups/2* | sort | tail -1)
   OLD=/SPARROW/backups/pre-upgrade-sparrowd-schema62-600ee97
   IMG=$(readlink -f ~/.local/libexec/sparrow/sparrowd)
   grep -E '^(image|store_id|taken_utc)=' "$NEW/SOURCE"
   ```
   Expected: `NEW` was taken after the deployment and its `image=` names the deployed schema-66 or later image.
1. Verify the newest backup with its own image:
   ```bash
   deploy/verify-backup.sh "$NEW"; echo "exit=$?"
   systemctl --user is-active sparrow.service
   ```
   Expected: one passing line per check, `exit=0`, and `active`.
2. A backup that does not match is refused:
   ```bash
   T=$(mktemp -d ~/.local/state/sparrow-uat-XXXX); cp -a --reflink=auto "$NEW"/. "$T"/
   sed -i 's/^store_id=.*/store_id=store:00000000000000000000000000000000/' "$T/SOURCE"
   deploy/verify-backup.sh "$T"; echo "exit=$?"; rm -r "$T"
   ```
   Expected: a failed store-id check and `exit=1`.
3. An image without the mode is refused, and an old-format backup opens with a newer image:
   ```bash
   deploy/verify-backup.sh "$OLD"; echo "exit=$?"
   deploy/verify-backup.sh --image "$IMG" --time-records "$OLD"; echo "exit=$?"
   ```
   Expected: the first run refuses because the backup's image predates query-only mode, and exits nonzero. The second passes the mode, store-id, status and read checks, reports the count check as not recorded (this backup predates the count), prints five read times, and exits nonzero for that reason only. Post the read times on the compression issue next to the live times.

Done when every UAT step passes on the live service.

