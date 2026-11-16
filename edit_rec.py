def cut(s,a,b,new=''):
    i=s.index(a); j=s.index(b,i)
    return s[:i]+new+s[j:]
p='deploy/RECOVERY.md'
s=open(p).read()
s=cut(s,"## Where state lives and what is backed up","## Verify health",
"""## Where state lives and what is kept

Sparrow backs up nothing except the age-out to GTS files on the NAS (the archive worker,
docs/interfaces.md, "Archive"). The online store backup (#156) was removed in #330. There is
no copy of the store to restore: a lost or damaged store is not recoverable from this
directory.

| State | Location | Kept by |
| --- | --- | --- |
| Store (catalog, bytes, indexes, WAL, watch catalog, consume journals) | `/SPARROW/store` | Nothing; aged-out evidence is archived to GTS files on the NAS before it is retired |
| Files waiting in consume roots | `/SPARROW/intake/NAME` | Nothing |
| Shipper queue, ledgers, lock | `/SPARROW/ship` | Nothing |
| Configuration, watch definitions, CHAP secret | `/etc/sparrow` | Nothing; the watch definitions applied are also in the store's catalog |
| Images and lexicons | `/usr/local/libexec/sparrow`, `/usr/local/share/sparrow` | The kept previous pair (`.previous`); everything else is rebuilt from the repository into `./bin` |
| Units, profile, layout | `/etc/systemd/system`, `/etc/apparmor.d`, `/etc/{sysusers,tmpfiles}.d` | This directory (`install.sh`) |

""")
s=cut(s,"- Limit: an image cannot undo a store-format upgrade its successor made.","- Reversible: yes; `--rollback` again swaps forward.",
"""- Limit: an image cannot undo a store-format upgrade its successor made (the store fence).
  `start.sh` and `restart.sh` therefore refuse an image whose schema number
  (`sparrowd-schemaN-REV`) is lower than the current image's, before stopping
  anything. There is no backup to restore: a format-changing upgrade has no undo, so stay on
  the new image and fix forward. The kept previous image and the link swap are all an
  upgrade relies on ([evidence/lifecycle.md](evidence/lifecycle.md) shows a schema 62 image
  refusing a store a schema 63 image had opened).
- Limit without a schema change: the move to PurRDF fb02dd38b (#28) keeps
  schema 67 but advances the store's RDF profile at the first open. An image
  built before it then refuses with `RDF profile changed`, and the schema
  check does not catch this, so `--rollback` stops the service. Run
  `--rollback` again to swap forward.
""")
s=cut(s,"## NAS archive unavailable","## Revert the cutover to the user unit",
"""## NAS archive unavailable

- Symptom: the journal has `sparrowd: archive pass failed; the watermark did not move`
  followed by an I/O error, a missing `STORE_ID`, or "archive directory ... is not on its
  expected mount".
- Effect: none on ingestion. The archive worker runs on its own thread and the daemon starts and
  serves without the NAS; the store keeps what it could not archive and the watermark does not
  move, so nothing is retired before it is archived. The disk budget and the free-space floor
  then apply as usual.
- "Not on its expected mount" means the archive directory is on the same device as `/`: the share
  is not mounted and its mount point is an ordinary local directory. The worker wrote, moved and
  deleted nothing. Never create the archive on the local disk instead.
- Diagnose: `findmnt -T /net/synology/archives/sparrow-archive` must show `nfs4` from
  `192.168.1.1:/volume1/archives`.
- Recover: restore the mount; the next pass retries by itself. The unit and the profile
  grant no archive path yet. Sparrow issues #15 to #17 add the archive worker's deployment.

""")
s=cut(s,"## Revert the cutover to the user unit","## Exposure",
"""## Revert the cutover to the user unit

- When: the system unit cannot serve after the cutover of Sparrow issue #22,
  and the operator's old user unit must serve `/SPARROW` again.
- Limit: this revert used to restore the issue's step 5a online backup. That backup feature was
  removed in #330 and none exists, so the revert keeps the cutover's store in place and serves it
  with the user unit's image, which must be able to open it (the store fence refuses an image
  older than the one that last upgraded the store's format). Anything the system unit
  ingested stays in the store.
- Before the cutover (`stat`, 2026-11-02), `/SPARROW` was `paudley:root` 0700
  with no ACL entries, `/SPARROW/store` was `paudley:paudley` 0700, and
  nothing else existed under `/SPARROW`.

`deploy/uninstall.sh` makes the host changes needed to return to that state:

- It disables and removes the system units.
- It unloads and removes the AppArmor profile.
- It removes `/etc/tmpfiles.d/sparrow.conf`, which would re-own `/SPARROW` at
  the next boot, and the `/usr/local/bin/sparrow` link.
- It runs `daemon-reload`.
- It gives `/SPARROW` back to the operator as `paudley:root` 0700 without
  ACLs. `/SPARROW/store` and `/SPARROW/intake` go to `paudley:paudley`, with
  directories at 0700 and no ACLs.

It checks first, changing nothing, that no Sparrow system unit is active and
that the sysusers file names exactly one operator. It leaves `/etc/sparrow`,
the `sparrow` user and group, the installed images and `/SPARROW/ship` in place, and says so.
Once `/SPARROW` is paudley's again, the user manager does not need the `sparrow` group.

Stop the shipper timer and the system unit through its drain first (`sudo systemctl stop
sparrow-ship.timer`, then `sudo deploy/stop.sh`), run `sudo deploy/uninstall.sh`, point the user
shipper's `ship.toml` at `/SPARROW/intake`, and enable the user units. Reversible: run
`install.sh` again.

Unverified: `uninstall.sh` was exercised only in staged mode
(`SPARROW_DESTDIR`) against a test instance's root, where it removes the
staged files and prints the host commands without running them
([evidence/lifecycle.md](evidence/lifecycle.md)). Neither the script as root
nor this revert has run on a host.

""")
s=s.replace("0.4\n(`sparrow-ship.service`) and 1.2 (`sparrow-backup.service`), each under the\nfleet target","0.4\n(`sparrow-ship.service`), each under the\nfleet target")
import re
s=re.sub(r"\| `backup` \| `User=` root[^\n]*\n","",s)
s=s.replace("The two timers' services are not confined by AppArmor: they are Python\nprograms that run `rsync`, `b3sum`, `sync` and the CLI, and their profiles are\nfollow-up work.","The timer's service is not confined by AppArmor: it is a Python\nprogram that runs `rsync`, `b3sum`, `sync` and the CLI, and its profile is\nfollow-up work.")
open(p,'w').write(s)
