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

`max_concurrent_requests` admits simultaneous storage work, including intake
workers (1..64). `max_rpc_connections` independently admits Unix and TCP RPC
streams from one shared ceiling (1..256, default 16). An idle agent capability
holds a transport lease without consuming a storage permit. The existing
60-second connection lease still applies. Peers beyond the connection ceiling
are disconnected before capability admission; storage saturation returns the
existing typed operation refusal. These are separate finite resource budgets.

Public TCP requires `rpc_tcp_secret_file`. Generate a random secret with mode
0600, for example `(umask 077; head -c 32 /dev/urandom > ~/.config/sparrow/tcp.secret)`,
and do not use a passphrase: an observer of one CHAP exchange can test guesses
offline. Copy the same file privately to each remote agent host and pass it with
`sparrow --secret-file PATH tcp://ADDRESS:518 COMMAND`. Producers that only
discover, ingest and import need no secret. CHAP does not encrypt traffic or
authenticate the server, so restrict the listener to trusted networks.

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
sparrow --secret-file ~/.config/sparrow/tcp.secret tcp://192.168.2.4:518 status
sparrow --secret-file ~/.config/sparrow/tcp.secret tcp://192.168.2.4:518 mcp --stdio
```

Use the actual reachable server address for remote clients. Verify store
identity, schema/format, retained counts, watches, byte reads and saved mutation
retries over Unix and TCP after adoption. Stop the new unit before reverting an
image/configuration; reverting a binary alone cannot undo a store-format upgrade.
A closed-store backup is a recovery artifact, and its existence does not prove
restoration has been exercised.

For isolated recovery qualification, copy a closed-store backup to a separate
directory and copy the deployment configuration with `data_dir` and
`socket_path` changed to that directory. Keep the backup immutable. The recovery
example uses the existing configuration/store readers directly, without starting
listeners, maintenance or saved watch workers:

```bash
cargo build --locked --release -p sparrow-server --example recovery
target/release/examples/recovery restored.toml EXPECTED_STORE RFC3339 EVIDENCE_ID...
```

Supply explicit artifact, record or finished file-generation IDs from captured
discovery pages, following every continuation. The example admits at most 1024
distinct targets and 1 GiB of aggregate reads, with 1 MiB byte calls. It checks
store identity, retained/complete-byte eligibility and native byte integrity;
streams native BLAKE3 over each target; compares full artifacts with their
catalog payload identity; and reports existing native RDF inspection for records.
Compare target bytes/hashes and RDF inspection with independently captured
evidence. The output is one native JSON report after all checks; a failure has
nonzero exit status and no successful stdout prefix. Missing dependencies,
unfinished generations and expired evidence are failures, not skipped targets.

This is a selected-evidence recovery probe, not a whole-engine integrity scrub or
a query-performance qualification. Its target report defines its coverage. A
missing blob may not affect a small inline target, so include all required
evidence when qualifying a restore. Opening a store can perform required format
upgrades; always work on a disposable restored copy. A live store is protected
by the existing single-owner database lock. The example never activates its
saved source paths, even when the copied watch catalog names original roots.

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
