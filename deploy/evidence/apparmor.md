<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# AppArmor (specification F, item 5)

## Parse checks, run

Run 2026-11-01 from the repository root with AppArmor parser 4.1.7, without
loading anything into the kernel (`-Q` compiles and skips the load, `-K`
skips the cache; `-p` prints the preprocessed profile):

```text
$ apparmor_parser --version
AppArmor parser version 4.1.7
[exit 0]
$ apparmor_parser -Q -K deploy/apparmor/sparrow
[exit 0]
$ apparmor_parser -p deploy/apparmor/sparrow > /dev/null
[exit 0]
```

The profile declares no mode flag, so it loads in enforce mode.

## How the rules were chosen

The daemon was run by hand on the test instance under
`strace -f -e trace=%file,%network,%desc,%process` (image
`sparrowd-schema62-cfdf2fa`), through startup, a consume of one file, a status
call and a SIGINT shutdown. Every path it named, with the test prefix replaced
by the production location it stands for (`DATA` is `/SPARROW`, `ROOT` is `/`;
store file numbers and thread IDs folded):

```text
.  (openat,openat2)
.lock  (openat2)
.sparrow-inflight  (mkdirat,openat2)
/etc/ld.so.cache  (newfstatat,openat)
/etc/ld.so.preload  (access)
/proc/self/fd/N  (inotify_add_watch)
/proc/self/fd/N/probe-3.jsonl  (statx)
/proc/self/maps  (openat)
/proc/self/task/TID/comm  (openat)
/proc/sys/kernel/random/uuid  (openat)
/sys/devices/system/cpu/online  (openat)
/usr/lib/libc.so.6  (openat)
/usr/lib/libgcc_s.so.1  (openat)
/usr/lib/libm.so.6  (openat)
/usr/lib/libstdc++.so.6  (openat)
/usr/share/zoneinfo/ZONE  (openat)
CURRENT  (statx)
DATA/intake/deploytest-moved  (openat2)
DATA/store  (chmod,mkdir,newfstatat,openat,statx)
DATA/store/CURRENT  (access,openat)
DATA/store/IDENTITY  (access,openat)
DATA/store/LOCK  (openat)
DATA/store/LOG  (access,openat,rename)
DATA/store/MANIFEST-N  (newfstatat,openat,unlink)
DATA/store/N.dbtmp  (openat,rename)
DATA/store/N.log  (newfstatat,openat,unlink)
DATA/store/N.sst  (openat)
DATA/store/OPTIONS-N  (newfstatat,openat,rename,unlink)
DATA/store/rpc.sock  (bind,chmod,connect,statx)
IDENTITY  (statx)
LOCK  (statx)
LOG  (statx)
LOG.old.N  (statx)
MANIFEST-N  (statx)
N.log  (statx)
N.sst  (statx)
OPTIONS-N  (statx)
ROOT/etc/sparrow/server.toml  (openat)
ROOT/usr/local/libexec/sparrow/sparrowd  (execve)
ROOT/usr/local/share/sparrow/lexicons-REV/artifacts  (openat)
ROOT/usr/local/share/sparrow/lexicons-REV/artifacts/FILE  (openat)
import:ID  (openat2,unlinkat)
probe-3.jsonl  (openat2,renameat2)
rpc.sock  (statx)
```

Relative names are opened beneath a held directory descriptor (the store, a
consume root or its claim directory). The `/home` components the daemon
resolved while canonicalizing the test paths are omitted; in production those
paths are under `/SPARROW`. The profile grants these and nothing else: its
images and libraries, the configuration and credential, the lexicons, the store
(including binding its socket), the consume roots, the archive, its own `/proc`
entries, the CPU list and `/proc/sys/kernel/random/uuid`, time zones, Unix,
IPv4 and IPv6 sockets and the bind capability. Exec of every file is denied.

## Load and live checks, not run

These need root and the cutover window (the acceptance steps of issue #22, the cutover's tracking issue: steps 6, 8
and 12):

```bash
sudo install -m 0644 deploy/apparmor/sparrow /etc/apparmor.d/sparrow
sudo apparmor_parser -r -W /etc/apparmor.d/sparrow
sudo aa-status | grep -A1 '^.*profiles are in enforce mode' | head -2; sudo aa-status | grep -x '   sparrow'
sudo cat /proc/"$(systemctl show -p MainPID --value sparrow.service)"/attr/apparmor/current
sudo journalctl -k -b --no-pager | grep 'apparmor="DENIED"' | grep 'profile="sparrow"'
sudo aa-exec -p sparrow -- cat /etc/shadow
sudo aa-exec -p sparrow -- sh -c /usr/bin/true
```

Expected: the profile is listed in enforce mode; the daemon's label is
`sparrow (enforce)`; no denial in a normal run; the two deliberate violations
fail with permission denied and each logs one `DENIED` line (a read of
`/etc/shadow`, an exec of `/usr/bin/true`). Attach those lines here when the
window runs.
