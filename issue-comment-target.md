Measurement target changed (coordination ruling, 2026-11-09).

Under P28 and D146 a closed-store backup is forbidden, so the closed-store pre-upgrade backup (`/SPARROW/backups/pre-upgrade-sparrowd-schema68-2aa56a9`) is no longer the D118 measurement target. `deploy/verify-backup.sh --measure-migration --with-traffic` now reads an online backup set, `BACKUP_ROOT/sets/ID`. It restores both the measured copy and the query-only prep copy from that set with `sparrowd --restore-backup`. `ship.toml` comes from the set's `config/sparrow/`.

The new D118 target:

- Before the D147 switch: a set taken by the online path from a test store with live intake. This is the measurement's own fixture, which builds the set through `backup_start`.
- After the D147 switch: the first real online set on the NAS (`/net/synology/archives/sparrow/sets/ID`).

PR #162 carries the port.
