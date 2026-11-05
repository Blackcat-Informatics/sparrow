<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Modes and ACLs, measured

The permission design in [README.md](../README.md#permissions) depends on how
the kernel combines a default ACL, a creator's umask and an explicit mode.
This transcript measures each case on this host's btrfs, unprivileged, on
2026-11-01, in a scratch directory. The group `wheel` stands in for `sparrow`,
which does not exist before installation; `paudley` stands in for both the
daemon and the operator.

```text
$ mkdir intake && chmod 0770 intake
$ setfacl -m default:user::rwx,default:group::---,default:group:wheel:rwx,default:mask::rwx,default:other::--- intake
$ (umask 077; mkdir intake/plain)
$ (umask 077; install -d intake/install-d)
$ (umask 077; mkdir -m 0700 intake/mkdir-m)
$ python3 -I -c 'import os; os.umask(0o077); os.mkdir("intake/plain/.sparrow-inflight", 0o700); os.close(os.open("intake/plain/.sparrow-inflight/.lock", os.O_CREAT | os.O_RDWR, 0o600))'
$ stat -c "%a %n" intake/plain intake/install-d intake/mkdir-m intake/plain/.sparrow-inflight intake/plain/.sparrow-inflight/.lock
770 intake/plain
755 intake/install-d
700 intake/mkdir-m
700 intake/plain/.sparrow-inflight
600 intake/plain/.sparrow-inflight/.lock
$ getfacl -cp intake/plain intake/install-d intake/plain/.sparrow-inflight | grep -E "^(group:wheel|mask)"
group:wheel:rwx
mask::rwx
group:wheel:rwx	#effective:r-x
mask::r-x
group:wheel:rwx	#effective:---
mask::---
$ mkdir setgid && chmod 2770 setgid && mkdir setgid/root && mkdir -m 0700 setgid/root/.sparrow-inflight && stat -c "%a %n" setgid/root/.sparrow-inflight
2700 setgid/root/.sparrow-inflight
$ mkdir ship && chmod 2700 ship && chgrp wheel ship && printf "x\n" > producer && chmod 0600 producer
$ (umask 077; cp --reflink=auto producer ship/item) && stat -c "%a %G %n" ship/item
600 wheel ship/item
$ chmod 0640 ship/item && mv ship/item intake/plain/item && stat -c "%a %G %n" intake/plain/item
640 wheel intake/plain/item
```

What it shows:

- A root made with plain `mkdir` under the default ACL gets the group's `rwx`
  despite a 077 umask (`intake/plain`, 770, mask `rwx`).
- `install -d` and `mkdir -m` apply an explicit mode that resets the ACL mask:
  the group's entry is left at `r-x` or nothing. This is the trap the README
  warns about.
- The daemon's claim directory and lock, created with explicit 0700 and 0600
  modes inside such a root, come out exactly 0700 and 0600 with the inherited
  group entry masked to nothing, so the daemon's checks
  (`crates/sparrow-server/src/consumption.rs`, "claim directory must be owned
  by the daemon and mode 0700") still pass.
- Under a setgid parent the same claim directory comes out 2700, which that
  check refuses. The roots therefore carry no setgid bit; only the shipper's
  state directory does, where the daemon creates nothing.
- A reflink copy of a 0600 producer file is 0600, giving the group nothing, so
  the shipper sets each item to 0640; it keeps the setgid directory's group
  when renamed into a root.
