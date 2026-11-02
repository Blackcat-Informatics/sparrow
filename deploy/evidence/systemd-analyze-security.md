<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Security exposure (specification F, item 1)

Measured 2026-11-01 with systemd 262 on the unit files, unloaded, from the
repository root. The fleet target is 2.0 or better, with every remaining item
justified in [RECOVERY.md](../RECOVERY.md#exposure).

```bash
systemd-analyze security --offline=yes --no-pager deploy/systemd/sparrow.service
systemd-analyze security --offline=yes --no-pager deploy/systemd/sparrow-ship.service
systemd-analyze security --offline=yes --no-pager deploy/systemd/sparrow-backup.service
```

| Unit | Exposure | Items passed | Items remaining |
| --- | --- | --- | --- |
| `sparrow.service` | 1.6 | 71 | 10 |
| `sparrow-ship.service` | 0.4 | 76 | 5 |
| `sparrow-backup.service` | 1.2 | 71 | 8 |

The remaining items, as `systemd-analyze` prints them (name, finding, weight):

`sparrow.service`:

```text
✗ RootDirectory=/RootImage= Service runs within the host's root directory 0.1
✗ AmbientCapabilities= Service process receives ambient capabilities 0.1
✗ RestrictSUIDSGID= Service may create SUID/SGID files 0.2
✗ RestrictAddressFamilies=~AF_UNIX Service may allocate local sockets 0.1
✗ RestrictAddressFamilies=~AF_(INET|INET6) Service may allocate Internet sockets 0.3
✗ CapabilityBoundingSet=~CAP_NET_(BIND_SERVICE|BROADCAST|RAW) Service has elevated networking privileges 0.1
✗ PrivateNetwork= Service has access to the host's network 0.5
✗ PrivateUsers= Service has access to other users 0.2
✗ DeviceAllow= Service has a device ACL with some special devices: char-rtc:r 0.1
✗ IPAddressDeny= Service does not define an IP address allow list 0.2
→ Overall exposure level for sparrow.service: 1.6 OK
```

`sparrow-ship.service`:

```text
✗ RootDirectory=/RootImage= Service runs within the host's root directory 0.1
✗ RestrictAddressFamilies=~AF_UNIX Service may allocate local sockets 0.1
✗ ProtectHome= Service has read-only access to home directories 0.1
✗ PrivateUsers= Service has access to other users 0.2
✗ DeviceAllow= Service has a device ACL with some special devices: char-rtc:r 0.1
→ Overall exposure level for sparrow-ship.service: 0.4 SAFE
```

`sparrow-backup.service`:

```text
✗ SystemCallFilter=~@privileged System call allow list defined for service, and @privileged is included (e.g. chown is allowed) 0.2
✗ RootDirectory=/RootImage= Service runs within the host's root directory 0.1
✗ User=/DynamicUser= Service runs as root user 0.4
✗ CapabilityBoundingSet=~CAP_(DAC_*|FOWNER|IPC_OWNER) Service may override UNIX file/IPC permission checks 0.2
✗ CapabilityBoundingSet=~CAP_(CHOWN|FSETID|SETFCAP) Service may change file ownership/access mode/capabilities unrestricted 0.2
✗ RestrictAddressFamilies=~AF_UNIX Service may allocate local sockets 0.1
✗ PrivateUsers= Service has access to other users 0.2
✗ DeviceAllow= Service has a device ACL with some special devices: char-rtc:r 0.1
→ Overall exposure level for sparrow-backup.service: 1.2 OK
```

The loaded production unit was not scored: installing it needs root and the
cutover window. `deploy/check.sh` scores the loaded unit after installation
and fails above 2.0.
