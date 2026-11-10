# Plan: issue 168

Behavior: second forwarder (deploy/libexec/katamari-manager-spool) follows the default journal for
katamari-stored.service, own cursor (katamari-manager.cursor), own prefix katamari-manager-*, records
tagged source=systemd-manager, unit's own lines dropped. Repo units, installer, docs, ship.toml stanza.
Dependencies: none. Exclusions: live units, live spool, live cursors, ship.toml edit, deploy step.
Acceptance: recorded check (Stopping/Stopped in spool), backfill of the six kill-1837 lines into a scratch spool.
Risk: shell only; cursor/since interplay (journalctl rejects --since with a cursor; handled by a backfill pass).

## User rulings
- Coordination (issue #168 text, 2026-11-09): own cursor file, tagged records, separate file names, do not apply the unit.
