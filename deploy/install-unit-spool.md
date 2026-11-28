<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Install a unit spool

This guide installs one spool for the journal lines of one system unit, through the tracked program
`deploy/libexec/sparrow-unit-spool`, and ships them to the store under their own shipper source. It applies the
procedure in `docs/deployment.md`, "Spooling one unit's journal lines", to one unit.

Placeholders, to be replaced by the host's values, which are recorded with the host and not in this repository:

| Placeholder | Meaning |
| --- | --- |
| `UNIT` | The system unit's name without `.service`. |
| `REPO` | The checkout of this repository at the head named in the order. |
| `HOME` | The home directory of the account that runs the spool. |
| `SOCKET` | The store socket: the `socket_path` of the daemon's server configuration. |
| `SOURCE` | A shipper source name for this unit, such as `UNIT-spool`. |

Every step names the account that runs it. Steps 1 to 3 install files and start nothing. Steps 4 and 5 change
live shipper and spool state, and each needs its own order. Step 6 reads the store and changes nothing.

## Before you start

- Check the head: `git -C REPO rev-parse --short HEAD` must print the head named in the order.
- Check the tools on the account's PATH: `jq`, `journalctl`, `sync`, `stat` (GNU coreutils), `date`, `mv`.
- Check that the account reads the system journal: `journalctl --system --unit=UNIT.service -n 3 --no-pager`
  prints lines. The account is in the group that may read the journal; `sudo` is not needed.

## Steps

### 0. The unit writes its own lines

- Runs as: the account (read only).
- Needs: nothing.
- Check: the command in "Before you start" prints at least one line whose process name is not `systemd`.
- Leaves: nothing.
- Way back: none needed.

### 1. Install the program

- Runs as: the account. Writes under `HOME`; no privilege.
- Needs: step 0 passes; the checkout is at the head named in the order.
- Command:

      install -D -m 0755 REPO/deploy/libexec/sparrow-unit-spool HOME/.local/libexec/sparrow/sparrow-unit-spool

- Check: `cmp REPO/deploy/libexec/sparrow-unit-spool HOME/.local/libexec/sparrow/sparrow-unit-spool` prints nothing.
- Leaves: one file. No running process.
- Way back: `rm -f HOME/.local/libexec/sparrow/sparrow-unit-spool`.
- Note: the program is not copied over any other spool program in `HOME/.local/libexec/sparrow`. A spool unit that
  already runs an earlier program keeps running it.

### 2. Install the template user unit

- Runs as: the account. Writes under `HOME`; no privilege.
- Needs: step 1 passes.
- Command:

      install -D -m 0644 REPO/deploy/systemd/user/sparrow-spool@.service HOME/.config/systemd/user/sparrow-spool@.service
      systemctl --user daemon-reload

- Check: `systemctl --user cat sparrow-spool@UNIT.service` shows `ExecStart=%h/.local/libexec/sparrow/sparrow-unit-spool %i.service %S/sparrow/unit-spool/%i.service %S/sparrow/unit-spool/%i.service.cursor` and `IgnoreSIGPIPE=no`.
- Leaves: the template file. Installing it starts nothing.
- Way back: `rm -f HOME/.config/systemd/user/sparrow-spool@.service` then `systemctl --user daemon-reload`.

### 3. Add the shipper source

- Runs as: the account that runs the shipper's user configuration (`HOME/.config/sparrow/ship.toml`).
- Needs: steps 1 and 2 pass; the shipper configuration exists.
- Command: back up the file, then append one source:

      cp -p HOME/.config/sparrow/ship.toml HOME/.config/sparrow/ship.toml.pre-UNIT-spool

      [[source]]
      name = "SOURCE"
      dir = "HOME/.local/state/sparrow/unit-spool/UNIT.service"
      pattern = "unit-spool-*.jsonl"
      rule = "settled"
      format = "jsonl"
      settle_seconds = 900

- Check: `REPO/deploy/tests/spool-settle.sh HOME/.config/sparrow/ship.toml` exits 0. Exit 1 names a source that breaks
  the settle margin; correct the source before going on. `settle_seconds` 900 is the 600-second quiet period plus the
  300-second margin.
- Leaves: the configuration with one added source; the backup file.
- Way back: `cp -p HOME/.config/sparrow/ship.toml.pre-UNIT-spool HOME/.config/sparrow/ship.toml`. Remove the backup only
  after the next shipper run has passed with the restored configuration.

### 4. Start the spool instance

- Runs as: the account, through its user manager.
- Needs: steps 1 to 3 pass.
- Command:

      systemctl --user enable --now sparrow-spool@UNIT.service

- Check, after the first journal entries are read:
  - `systemctl --user is-active sparrow-spool@UNIT.service` prints `active`.
  - `HOME/.local/state/sparrow/unit-spool/UNIT.service/` holds `unit-spool-*.jsonl` files, and
    `HOME/.local/state/sparrow/unit-spool/UNIT.service.cursor` exists.
- Leaves: a running user unit; spool files; the cursor file. A first start with no cursor spools the unit's retained journal.
- Way back:

      systemctl --user disable --now sparrow-spool@UNIT.service

  Keep the spool directory and the cursor file. A spool file is removed only after the shipper has taken it and an
  archive copy is verified; this guide does not delete any.

### 5. Ship the spool (the shipper's own run)

- Runs as: the account that runs the shipper.
- Needs: step 4 passes and the first bucket has settled (its last write is 900 seconds old).
- Leaves: the shipper's queue state. Records reach the intake of the store.
- Way back: step 4's undo stops the spool. The shipper's queue can be left as it is; the configuration is restored by
  step 3's undo.

### 6. Verify arrival on the store (read only)

- Runs as: the account that may read the store socket.
- Needs: step 5 has run at least once after the bucket settled.
- Write the constraint file (a JSON array, on a path the account owns):

      [{"path":"/source","decoded":"systemd-unit"},{"path":"/unit","decoded":"UNIT.service"}]

- Query, with the scan budget. Omit `--scan-budget` and the query fails with `RESOURCE_LIMIT`:

      sparrow SOCKET records --where-file CONSTRAINTS.json --scan-budget 100000 --page-size 500

- Check: the reply has `page.total_items` greater than 0 and exits 0. The count is the unit's own lines, not the
  unit's manager lines.
- Leaves: nothing. Remove `CONSTRAINTS.json` afterwards.
- Way back: none needed; this step reads only.

## Scan budget and large stores

`--scan-budget` is the number of occurrences the store may examine for one page. The command accepts 1 to 100000 and
the default is 10000. A 1,167-record scratch store needed 100000 for the unit query above: with the default budget the
query fails with `RESOURCE_LIMIT`, and the error returns no partial result. Nothing here measures how the budget grows
with a larger store, so the ceiling of 100000 may be reached on a larger store.

When the query exceeds the budget, the reader narrows the scope and pages; it does not raise the budget past 100000:

- `--source SOURCE` limits the scan to the shipper source for this unit. Use it first.
- `--received-from RFC3339` and `--received-before RFC3339` limit the scan to a receipt window.
- `--limit N` bounds the number of rows returned.
- `--cursor CURSOR` continues a page from a previous reply's `next_cursor`. Use the same constraints and budget.

A reader who needs all of the unit's lines over a long period pages through the window with `--cursor`, one
receipt window at a time, and records each window's `total_items`. A `RESOURCE_LIMIT` at the narrowest window means
that window must be split again.

## Way back, in order

Undo steps in reverse order and stop at the first failed check:

1. Step 4: `systemctl --user disable --now sparrow-spool@UNIT.service`.
2. Step 3: restore `HOME/.config/sparrow/ship.toml` from its backup.
3. Step 2: remove the template and run `systemctl --user daemon-reload`.
4. Step 1: remove the program.

Nothing in the way back deletes records, spool files or cursor files, and nothing changes the store.
