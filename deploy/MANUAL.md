<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Running Sparrow by hand

Use this for diagnosis, or when systemd cannot run the unit. The unit stays
the source of truth; these commands reproduce its environment as closely as a
shell can. Never run a second daemon on a store another one owns: stop the
unit first and confirm it is inactive. The store's exclusive lock refuses a
second owner, but do not rely on it.

```bash
sudo systemctl stop sparrow.service
systemctl is-active sparrow.service
```

Expect `inactive` (or `failed`).

## The unit's environment

| The unit sets | By hand |
| --- | --- |
| `User=sparrow`, `Group=sparrow`, supplementary groups | `setpriv --reuid=sparrow --regid=sparrow --init-groups` |
| `AmbientCapabilities=CAP_NET_BIND_SERVICE` and the same bounding set | `--inh-caps=-all,+net_bind_service --ambient-caps=-all,+net_bind_service --bounding-set=-all,+net_bind_service` |
| `UMask=0077` | `umask 0077` in the wrapping shell |
| `Nice=-20`, best-effort IO priority 0 | `nice -n -20 ionice -c 2 -n 0` |
| `OOMScoreAdjust=-900` | `choom -n -900 --` |
| `AppArmorProfile=sparrow` | `aa-exec -p sparrow --` (the profile must be loaded) |
| `LoadCredential=tcp.secret:/etc/sparrow/tcp.secret` | A private copy under `/run/sparrow-manual`, which the profile admits |
| `WorkingDirectory=/SPARROW/store` | `cd /SPARROW/store` |
| `KillSignal=SIGINT` | Ctrl-C in the foreground, or `kill -INT PID` |
| The slice, the memory and CPU limits, the namespaces and the seccomp filter | Not reproduced by hand; use `systemd-run` below when systemd works |

## Foreground, confined, with the public listener

The credential copy carries the secret into a root-owned tmpfs directory, and
the configuration copy points at it.

```bash
sudo install -d -m 0750 -o root -g sparrow /run/sparrow-manual
sudo install -m 0400 -o sparrow -g sparrow /etc/sparrow/tcp.secret /run/sparrow-manual/tcp.secret
sudo sed 's|^rpc_tcp_secret_file = .*|rpc_tcp_secret_file = "/run/sparrow-manual/tcp.secret"|' \
    /etc/sparrow/server.toml | sudo install -m 0640 -o root -g sparrow /dev/stdin /run/sparrow-manual/server.toml
cd /SPARROW/store
sudo choom -n -900 -- nice -n -20 ionice -c 2 -n 0 \
    setpriv --reuid=sparrow --regid=sparrow --init-groups \
        --inh-caps=-all,+net_bind_service --ambient-caps=-all,+net_bind_service \
        --bounding-set=-all,+net_bind_service \
    sh -c 'umask 0077; exec aa-exec -p sparrow -- /usr/local/libexec/sparrow/sparrowd --config /run/sparrow-manual/server.toml'
```

The daemon logs to standard error: the TCP binding, a free-space wait, watch
activity. In a second shell, verify it as the operator:

```bash
sparrow /SPARROW/store/rpc.sock status
```

Stop it with Ctrl-C, which is SIGINT: it drains workers, synchronizes the WAL
and removes its socket. Then remove the copies and give the store back to the
unit:

```bash
sudo rm -rf /run/sparrow-manual
sudo systemctl start sparrow.service
```

## Diagnosis variants

- Without the public listener: remove `rpc_tcp_address` and
  `rpc_tcp_secret_file` from the copy in `/run/sparrow-manual/server.toml`;
  no secret copy and no capability are then needed.
- Without AppArmor, to tell a denial from another failure: drop
  `aa-exec -p sparrow --`. Compare with `sudo journalctl -k -b | grep 'profile="sparrow"'`.
- Traced: insert `strace -f -e trace=%file,%network -o /tmp/sparrowd.trace`
  before `/usr/local/libexec/sparrow/sparrowd`; `deploy/evidence/apparmor.md`
  shows how the profile's path list was derived this way.
- With systemd working but the unit broken, run the same image under a
  transient unit in the production slice instead. Prepare `/run/sparrow-manual`
  first, exactly as in "Foreground, confined, with the public listener" above
  (its first three commands). A transient unit's credentials would land under
  `/run/credentials/sparrow-manual.service/`, which neither the configuration
  nor the profile names, so this recipe uses the prepared copies, which the
  profile admits, and no `LoadCredential=`:

  ```bash
  sudo systemd-run --unit=sparrow-manual --slice=lillith.slice --uid=sparrow --gid=sparrow \
      -p AppArmorProfile=sparrow -p UMask=0077 -p KillSignal=SIGINT -p Nice=-20 \
      -p OOMScoreAdjust=-900 -p AmbientCapabilities=CAP_NET_BIND_SERVICE \
      -p CapabilityBoundingSet=CAP_NET_BIND_SERVICE \
      -p WorkingDirectory=/SPARROW/store --wait --pty \
      /usr/local/libexec/sparrow/sparrowd --config /run/sparrow-manual/server.toml
  ```

  Afterwards remove the copies as above. Not tested: it needs root, the
  system manager and the loaded profile. The path agreement was checked by
  reading: the prepared configuration names
  `/run/sparrow-manual/tcp.secret`, and the profile grants
  `/run/sparrow-manual/*` read.

## The helpers by hand

```bash
sudo -u paudley sh -c 'umask 0077; exec /usr/local/libexec/sparrow/sparrow-ship /etc/sparrow/ship.toml'
sudo env RUNTIME_DIRECTORY=/run/sparrow-backup sh -c 'install -d -m 0700 /run/sparrow-backup && exec flock /SPARROW/ship/lock /usr/local/libexec/sparrow/sparrow-backup /etc/sparrow/server.toml /SPARROW/backups 3 /SPARROW/intake /etc/sparrow /SPARROW/ship'
```

The backup stops and starts `sparrow.service` through `systemctl`, so it
needs systemd. The shipper needs only the daemon to be consuming.

Until #22 moves production to the system unit, production runs as the user
unit `sparrow.service`. Its backup unit runs the same helper with
`SPARROW_SCOPE=user`, `SPARROW_PREFIX=$HOME/.local` and
`SPARROW_INTERIM_USER_PRODUCTION=1` (#31). Without that last variable the
helper refuses a user-scope `sparrow.service`, so a test-instance run never
stops production. #22 removes the variable with the user unit.

## The recovery probe

The kept image `bin/sparrow-recovery-schemaN-REV` reads a restored store
without starting listeners or watches (docs/deployment.md, "Recovery
qualification"):

```bash
bin/sparrow-recovery-schemaN-REV restored.toml EXPECTED_STORE RFC3339 EVIDENCE_ID...
```
