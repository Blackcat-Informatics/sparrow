# Installing Sparrow on Linux

The repository supplies a persistent systemd user unit at
`packaging/systemd/sparrow.service`. It runs the same typed daemon as the
development unit, with a private Unix socket and configured public Cap'n Proto
TCP socket. The selected public binding is `0.0.0.0:518`.

Run `make check`, commit through the normal verification hook, then run
`make build-runtime`. Install the explicitly selected release binaries under
unique revision-qualified names in `~/.local/libexec/sparrow/`. Compare their
hashes with the build artifacts. Keep previous images for an explicit rollback;
do not overwrite an executing image. Installation and live verification are
separate from the source gate.

The unit uses these stable paths:

| Path | Contents |
| --- | --- |
| `~/.local/libexec/sparrow/sparrowd` | Symlink to the selected installed daemon image. |
| `~/.local/bin/sparrow` | Symlink to the selected installed CLI image. |
| `~/.config/sparrow/server.toml` | Reviewed deployment configuration with absolute paths. |
| `~/.config/systemd/user/sparrow.service` | Copy of the repository unit. |

Create private configuration/state directories with mode 0700 and configuration
with mode 0600. Existing paths belong to the operator: inspect them before
installation or replacement. Copy the five required native CBOR lexicons and
their provenance manifest, notices and licenses into a revision-qualified directory under
`~/.local/share/sparrow/`; configure `text_lexicon_dir` to that installed bundle.
Compare each copied artifact with its source. The daemon's existing native
analyzer resolver verifies the required artifact identities before serving.
Runtime configuration must not depend on an editable sibling checkout.

Configuration supplies the store/socket paths, disk budget, maintenance reserve,
operation ceilings and intake definitions. `config/development.toml` is a
host-specific example for owned synthetic intake, not production source
authority. Preserve an existing store's configuration and catalog watch
revisions during migration. Consume entries seed only a missing catalog;
typed watch administration controls later changes. Never add production sample
directories as destructive watches merely because they were sample inputs.

Port 518 requires the daemon image to have `cap_net_bind_service=ep`, or another
explicitly configured host privilege arrangement. Apply a file capability to the
final installed image, not the symlink or a volatile build artifact; copying an
image does not preserve that capability. Verify with `getcap` before cutover.
The client needs no file capability. The unit does not run a privileged shell
or introduce another transport.

Validate the installed unit with:

```bash
systemd-analyze --user verify ~/.config/systemd/user/sparrow.service
```

For a migration, capture the current typed status, image identity, unit and
configuration. Stop the old Sparrow unit through its SIGINT shutdown path and
verify it is terminal, with its Unix socket removed. Never open two owners on
one store. Then load and enable the installed unit:

```bash
systemctl --user daemon-reload
systemctl --user enable --now sparrow.service
systemctl --user status sparrow.service
sparrow tcp://192.168.2.4:518 status
sparrow tcp://192.168.2.4:518 mcp --stdio
```

Use the actual reachable server address for remote clients. Verify store
identity, schema/format, retained counts, watches, byte reads and saved mutation
retries over Unix and TCP after adoption. Stop the new unit before reverting an
image/configuration; reverting a binary alone cannot undo a store-format upgrade.
A closed-store backup is a recovery artifact, and its existence does not prove
restoration has been exercised.

For boot startup independently of an interactive login, verify
`loginctl show-user "$USER" -p Linger`. Enabling lingering when absent is a host
administration action. The enabled unit and lingering establish boot
configuration; a reboot test is separate evidence and must not be claimed from
unit validation alone.

The unit uses SIGINT because the daemon implements worker drain and WAL fencing
on that signal. It gives shutdown 120 seconds and reports failure if the process
cannot exit. It deliberately does not automatically restart: a crash can leave
an existing Unix socket, and missing dependencies or a refused store open need
diagnosis. Do not remove a socket until the prior owner is proven terminal.
Journal output remains available through `journalctl --user -u sparrow.service`.

This packaging does not establish whole-corpus throughput, physical reclamation,
restoration or full architecture acceptance. Current installed image and live
acceptance evidence are recorded in [Development runtime](development-runtime.md).
