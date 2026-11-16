def cut(s,a,b,new=''):
    i=s.index(a); j=s.index(b,i)
    return s[:i]+new+s[j:]
p='docs/deployment.md'
s=open(p).read()
s=s.replace(" An online backup is a recovery artifact, and its\nexistence does not prove restoration has been exercised.","")
s=cut(s,"For isolated recovery qualification, restore a backup","The recovery\nexample uses",
"""For isolated recovery qualification, work on a disposable copy of a store, made while its
daemon is stopped, in a separate empty directory, and copy the deployment configuration with
`data_dir` and `socket_path` changed to that directory. Keep the source immutable. The recovery
""")
s=s.replace("\nexample uses the existing configuration/store readers directly","\nexample uses the existing configuration/store readers directly",1) if False else s
s=s.replace("evidence when qualifying a restore. Opening a store can perform required format\nupgrades; always work on a disposable restored copy.","evidence when qualifying a copy. Opening a store can perform required format\nupgrades; always work on a disposable copy.")
s=s.replace("To check a whole backup rather than selected evidence, use\n`deploy/verify-backup.sh` (`deploy/RECOVERY.md`, \"Verify a backup\").\n","")
s=s.replace("`ingest_socket_path`, `backup_root` or `archive.dir`","`ingest_socket_path` or `archive.dir`")
s=cut(s,"## Online backups\n","## Switching the user unit's image",
"""## Archive and backups

Sparrow backs up nothing except the age-out to GTS files on the NAS: the archive worker
(docs/interfaces.md, "Archive") writes each aged-out segment under `[archive] dir`, here
`/net/synology/archives/sparrow-archive`, before the store retires it. The online store backup
(#156) was removed in #330, with `sparrow-backup`, its restore, `deploy/verify-backup.sh`,
`deploy/pre-upgrade-backup.sh` and the nightly timer. The configuration keys `backup_root` and
`backup_keep` were removed too. The server file refuses unknown keys, so a `server.toml` that still
names either one fails to load with the parser's `unknown field` error: delete the lines.

The archive worker also refuses to write when `[archive] dir` is on the same device as `/`. An
unmounted NFS share leaves its mount point as an ordinary directory on the local root disk, and
writing archives there would fill it. The refusal is the named error "archive directory ... is not
on its expected mount", is journaled on the pass failure line, and writes, moves and deletes
nothing; the daemon itself starts and serves as before, and the worker retries on its next pass. The
operator writes `STORE_ID` (this store's id and a newline) into the archive directory once; the
worker only reads it.

""")
s=cut(s,"## Switching the user unit's image","### Part sets and older builds",
"""## Switching the user unit's image

Until Sparrow issue #22 moves the live store to the system unit, it runs under
the operator's old user unit, `sparrow.service` under `systemctl --user`. The
system unit's scripts refuse that unit. `deploy/user-unit-upgrade.sh` switches
it from one kept image pair to another:

```bash
deploy/user-unit-upgrade.sh --from sparrowd-schema68-2aa56a9 --to sparrowd-schema71-REV
```

Both arguments name daemon images under `~/.local/libexec/sparrow`. Each CLI
image is the daemon's name with `sparrowd-` replaced by `sparrow-`, and the
schema numbers come from the names. Run it as the operator, only on the
coordination session's go. `SPARROW_PREFIX` (default `~/.local`),
`SPARROW_CONFIG` (default `~/.config/sparrow/server.toml`, which names the store
and its sockets) and `SPARROW_BIN` override the layout; `--help` lists them.

What an upgrade relies on: there is no backup to restore from. The undo is the kept previous
image and the link swap that points the daemon and CLI links back at it. A format-changing
upgrade has no undo: once `--to` opens the store and upgrades its format, the `--from` image
refuses it (the store fence), so the only way out is forward on `--to`.

Before anything changes it refuses unless all of these hold:

- The daemon link names `--from`.
- All four images are executable.
- The `--to` pair matches `./bin` byte for byte.
- The `--to` daemon carries `cap_net_bind_service`.
- `server.toml` sets the ingest keys.
- The store has copy-on-write off.
- The unit's drop-in directory holds only an identical `limits.conf`.
- The running daemon reports the `--from` schema.

The capability is a prerequisite done by a human with root,
`sudo setcap cap_net_bind_service=ep ~/.local/libexec/sparrow/IMAGE`; the
script checks it with `getcap` and never runs `setcap`.

Then it stops `sparrow-ship.timer` and waits for the shipper, stops the unit, installs the
`LimitNOFILE` drop-in, switches the links and starts the unit. At the end it starts
`sparrow-ship.timer` again. If the shipper is still running after the timer stops, the script
starts the timer again if it was active before and changes nothing else.

It waits up to 10 minutes, the switch gate, for the first status answer. The
measured first open of schema 69 on a copy of the live store under the unit's
limits answered in 117.2 seconds (2026-11-07). It does not wait for pressure to report `ready`: that phase tracks
disk-budget maintenance, not the migrations. After the first answer it fails
unless the store identity is unchanged and the schema is the `--to` schema. It
prints each migration's `done`, `total` and `failure`, the watches and the
ingest socket, starts `sparrow-ship.timer`, and exits 0. The backfill continues online
in the running daemon. Watch it with `sparrow /SPARROW/store/rpc.sock status`
(`migrations`) and with `journalctl --user -u sparrow.service`, which logs a
`sparrowd: migration` start line per migration, a progress line every 64
pages and a completion line per migration, each with its pages and `done`; the
progress and completion lines carry `elapsed_seconds` since that migration's
first page, so the completion line gives each migration's own duration (see
"Online store migrations" in the interfaces).

`--help` gives the recovery for each failure point. A failure after the start
leaves the `--to` daemon running with the timer stopped, and it may already
have accepted ingest. The recovery goes forward first: wait for status, confirm
the store identity and schema, then start `sparrow-ship.timer`. Pointing the links back at
`--from` is possible only while `--to` did not change the store's format.

""")
s=s.replace("The hold exists for two uses: the rollback rehearsal in `deploy/verify-backup.sh`,\nwhich needs a claim in flight when the daemon halts, and an operator's deliberate\npause of one watch.","The hold exists for an operator's deliberate pause of one watch.")
i=s.index("### Claims after a rollback")
s=s[:i].rstrip()+"\n"
open(p,'w').write(s)
p='docs/development-runtime.md'
s=open(p).read()
s=s.replace("[online backups](deployment.md#online-backups) on the NAS)","the online backup, itself removed in #330)")
open(p,'w').write(s)
