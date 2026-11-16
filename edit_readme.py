def cut(s,a,b,new=''):
    i=s.index(a); j=s.index(b,i)
    return s[:i]+new+s[j:]
p='deploy/README.md'
s=open(p).read()
s=cut(s,"scripts and these documents. After a restore from backups onto a fresh host,","| Path | Contents |",
"""scripts and these documents. On a fresh host, this directory and a clone of the repository
are enough to deploy the service again (the store itself is not recovered by them: Sparrow keeps
no backup of it). To build new images, and for the lexicon bundle and the recovery probe, step 2
needs the build prerequisites of step 1 and network access to the PurRDF
source that `Cargo.lock` names, or a copy of the old host's `./bin`. Secrets are never kept here; [Secrets](#secrets) names where each one
lives.

""")
s=s.replace("| `RECOVERY.md` | Failure modes, how to diagnose and recover each, rollback, restore, exposure. |","| `RECOVERY.md` | Failure modes, how to diagnose and recover each, rollback, exposure. |")
import re
s=re.sub(r"\| `pre-upgrade-backup.sh` \|[^\n]*\n","",s)
s=re.sub(r"\| `verify-backup.sh` \|[^\n]*\n","",s)
s=re.sub(r"\| `reconcile-inflight.sh` \|[^\n]*\n","",s)
s=re.sub(r"\| `user-unit-upgrade.sh` \| Switches the old user unit's image with its pre-upgrade backup,","| `user-unit-upgrade.sh` | Switches the old user unit's image,",s)
s=s.replace("`sparrow-ship.{service,timer}`, `sparrow-backup.{service,timer}`;","`sparrow-ship.{service,timer}`;")
s=s.replace("`sparrow-ship` (the shipper), `sparrow-backup` (the online backup client), `katamari","`sparrow-ship` (the shipper), `katamari")
s=cut(s,"- The NAS archive (`/net/synology/archives/sparrow`) is the backup","- Restarts of the live service are approved",
"""- Sparrow backs up nothing except the age-out to GTS files on the NAS: the archive worker
  writes aged-out segments under `[archive] dir` (`/net/synology/archives/sparrow-archive`) and
  never to a local disk (Patrick, 2026-11-08: "Sparrow and katamaria are FORBIDDEN from backing
  up to local disks"). The online store backup (#156) was removed in #330. The daemon's archive
  worker is not deployed by these files (Sparrow issues #15 to #17); it refuses a directory that
  is on the root filesystem's device (an unmounted share), and one without a `STORE_ID` file
  naming this store, which the operator writes once. A file naming another store means `dir` is
  that store's archive or the wrong mount.
""")
s=s.replace("remove that copy once a backup of the converted store has verified. Backups\nare RocksDB backup files on the NAS, and the verification and restore copies made from them go into a\nno-copy-on-write directory. The store gives up","remove that copy once the converted store has been checked. The store gives up")
s=s.replace("| `/usr/local/libexec/sparrow/sparrow-ship`, `sparrow-backup` | root, 0755 | The timers' helpers, from `libexec/`. |","| `/usr/local/libexec/sparrow/sparrow-ship` | root, 0755 | The timer's helper, from `libexec/`. |")
s=s.replace("| The applied watch definitions, kept for a restore. |","| The applied watch definitions. |")
import re
s=re.sub(r"\| `/net/synology/archives/sparrow` \|[^\n]*\n","| `/net/synology/archives/sparrow-archive` | on the NAS (NFSv4), 0700; create it once, owned by the daemon's user | The archive worker's per-store directories and the `STORE_ID` file. |\n",s)
s=cut(s,"- `sparrow-backup` copies `/etc/sparrow`, so each backup","- Custody: the secret is generated")
s=s.replace("the intake parent, the shipper's state and the backup directory at","the intake parent and the shipper's state at")
s=s.replace("The two timers' services run in the same slice","The timer's service runs in the same slice")
s=s.replace("a helper is less protected than the\nservice it serves, so a host OOM caused by a copy job kills the job. They are\nbounded too: the shipper at 512 MiB, 64 tasks and half a CPU, the backup at\n1 GiB, 64 tasks and one CPU. The daemon's own backup thread runs at nice 19, with the lowest best-effort\nI/O priority under the BFQ scheduler.","a helper is less protected than the\nservice it serves, so a host OOM caused by a copy job kills the job. It is\nbounded too: the shipper at 512 MiB, 64 tasks and half a CPU.")
s=cut(s,"`text_lexicon_dir` with the installed bundle's suffix (`ls\n/usr/local/share/sparrow`), and review the budgets.","### 5. Intake roots","`text_lexicon_dir` with the installed bundle's suffix (`ls\n/usr/local/share/sparrow`), and review the budgets. The template has no\n`backup_root` or `backup_keep`: Sparrow takes no online backup, and the server file refuses\nthose keys.\n\n") if False else s
i=s.index("/usr/local/share/sparrow`), and review the budgets.")
j=s.index("### 5. Intake roots")
s=s[:i]+"/usr/local/share/sparrow`), and review the budgets. The server file refuses the removed\n`backup_root` and `backup_keep` keys; Sparrow takes no online backup.\n\n"+s[j:]
s=s.replace("sudo systemctl start sparrow-ship.timer sparrow-backup.timer","sudo systemctl start sparrow-ship.timer")
s=s.replace("with no denials, both\ntimers and the slice.","with no denials, the\ntimer and the slice.")
s=cut(s,"Build, then take a named online backup of the store","### Interim user unit",
"""Build, then switch images:

```bash
git pull && make check && make install-bin
sudo deploy/restart.sh && sudo deploy/check.sh
```

Sparrow keeps no backup of the store, so an upgrade relies on the kept previous image and the
link swap, and nothing else. `restart.sh` installs and verifies the newest pair while the old
daemon still runs, records the store identity, stops through the drain, points the links at
the new pair (the old pair becomes `.previous`), starts, and fails unless the
store identity is unchanged. `sudo deploy/restart.sh --rev REV` selects another
kept pair; `sudo deploy/restart.sh --rollback` swaps back to the previous pair.

A newer image may upgrade the store's format, and an older image then refuses
to open it (the store fence). The scripts enforce this by the schema number in the image names
(`sparrowd-schemaN-REV`), not by reading the store: `start.sh` and `restart.sh`
refuse an image with a lower number than the current one. A format-changing upgrade
therefore has no undo: after the new image has opened the store, the way out is forward.
The store's actual format is `store_format` in the daemon's status.

`sudo deploy/stop.sh` stops the service through the drain and reports what it
still held.

""")
s=cut(s,"### Interim user unit","## Moving a running service",
"""### Interim user unit

While the live store runs under the operator's old user unit,
`deploy/user-unit-upgrade.sh --from FROM --to TO` switches images. It takes no backup and
requires none: it relies on the kept previous image and the link swap, and a
format-changing upgrade has no undo (the store fence), so the only way out is forward on the
new image. It starts only `sparrow-ship.timer` again. If the shipper is still running when the
script has stopped the timer, it starts the timer again if it was active before and changes
nothing else. `tools/user-unit-upgrade-timers-check.sh` records both behaviors against a stubbed
`systemctl`.

""")
s=s.replace("take the named pre-upgrade backup online from the running old unit with the old image\nrecorded in its `SOURCE` (the issue's step 5a; a daemon that cannot back up\nonline needs `--no-backup`, and the backup never stops it), stop the old unit","stop the old unit")
s=s.replace("to the operator; then a restore of the step 5a backup, when one was taken, replaces the cutover store.","to the operator; the cutover store is not restored from a backup, because none is taken.")
s=s.replace("`restart.sh`,\n`pre-upgrade-backup.sh` and `check.sh` at it;","`restart.sh`\nand `check.sh` at it;")
open(p,'w').write(s)
