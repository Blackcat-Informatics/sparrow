<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Install a unit spool

This guide installs one spool for the journal lines of one system unit, through the tracked program
`deploy/libexec/sparrow-unit-spool`, and ships them to the store under their own shipper source. It applies
"Spooling one unit's journal lines" in `../docs/deployment.md`. A unit that already has a spool is not installed
by this guide; see "A unit with an existing spool" below.

Placeholders. Each host records its values in its own host record, outside this repository:

| Placeholder | Meaning |
| --- | --- |
| `UNIT` | The system unit's name without `.service`. |
| `REPO` | The checkout of this repository at a head that contains the tracked program at `fd34bc5` or later. |
| `HOME` | The home directory of the account that runs the spool. |
| `SOCKET` | The store socket: the `socket_path` of the daemon's server configuration. |
| `SOURCE` | The shipper source name for this unit. Use the name of the existing source when one exists. |

Every step names the account that runs it and whether it is live. Steps 0 and 1 read only. Step 2 creates a directory.
Step 3 changes the running daemon. Steps 5 and 6 change live spool and shipper state. Step 8 reads the store. Each live
step needs its own order and is run in the order given.

## Which path applies

- No instance exists: `systemctl --user is-enabled sparrow-spool@UNIT.service` fails, and
  `HOME/.local/state/sparrow/unit-spool/UNIT.service` does not exist. Run steps 0 to 8.
- An instance exists, or its spool directory exists: do not run steps 4 to 6 as written. Follow "A unit with an
  existing spool".

## Steps

### 0. Preconditions

- Runs as: the account. Read only.
- Check the head: `git -C REPO merge-base --is-ancestor fd34bc5 HEAD` exits 0. An earlier head lacks the fixes for a
  non-numeric `PRIORITY`, for the sync of each record before the cursor moves, and for the directory sync of a new
  bucket. An earlier head is not installed.
- Check the tools on the account's PATH: `jq`, `journalctl`, `sync`, `stat` (GNU coreutils), `date`, `mv`.
- Check that the account reads the system journal: `journalctl --system --unit=UNIT.service -n 3 --no-pager` prints
  lines. The account is in the group that may read the journal; `sudo` is not needed.
- Undo: none.

### 1. The unit writes its own lines

- Runs as: the account. Read only.
- Check: the command in step 0 prints at least one line whose process name is not `systemd`.
- Undo: none.

### 2. Create the consume root

The shipper reads each source from its consume root. A missing root ends the shipper's run before any source is queued.

- Runs as: the account that runs the shipper. Writes under `HOME`; no privilege.
- Needs: step 1 passes.
- Command: `install -d -m 0700 HOME/.local/state/sparrow/intake/SOURCE`
- Check: `test -d HOME/.local/state/sparrow/intake/SOURCE`
- Leaves: one directory. Once the watch of step 3 has consumed a file, the daemon creates `.sparrow-inflight/.lock` in
  it, and no supported command removes that directory or lock at this build. Leave the root in place.
- Undo: none for the root. Step 3's undo stops its consumption.

### 3. Register the consume watch (live)

- Runs as: the account that owns the store socket. Changes the running daemon; the daemon is not restarted.
- Needs: step 2 passes.
- Order: its own order. Name the request ID in the order.
- Command: copy the definition of an existing consume watch, change its source and root to `SOURCE` and
  `HOME/.local/state/sparrow/intake/SOURCE`, keep the expected revision `0`, then:
  `sparrow SOCKET watch put REQUEST_ID 0 DEFINITION.toml`
- Check: `sparrow SOCKET watch list` shows a watch on that root, enabled.
- Leaves: one more consume root with a running watch.
- Undo: `sparrow SOCKET watch remove REQUEST_ID WATCH_ID REVISION`, with the watch ID and current revision that
  `watch list` reports. Then `sparrow SOCKET watch claims` (no option; this build
  accepts only `--since` and `--until`) must show no row whose `watch` is `WATCH_ID` and whose `state` is `pending`
  before the undo is reported as done. Rows for other watches are not the undo's concern. A pending claim is named in
  the order, not removed by hand.

### 4. Install the program and the template (not started)

- Runs as: the account. Writes under `HOME`; no privilege.
- Needs: step 3 passes.
- Command:

      install -D -m 0755 REPO/deploy/libexec/sparrow-unit-spool HOME/.local/libexec/sparrow/sparrow-unit-spool
      install -D -m 0644 REPO/deploy/systemd/user/sparrow-spool@.service HOME/.config/systemd/user/sparrow-spool@.service
      systemctl --user daemon-reload

- Check: `cmp` of each installed file against its repository file prints nothing.
- Leaves: two files and a reloaded user manager. No process runs.
- Undo: `rm -f HOME/.local/libexec/sparrow/sparrow-unit-spool HOME/.config/systemd/user/sparrow-spool@.service`, then
  `systemctl --user daemon-reload`. Run this undo only while no instance of the template exists.

### 5. Start the spool instance (live)

- Runs as: the account, through its user manager.
- Needs: step 4 passes.
- Order: its own order.
- Command: `systemctl --user enable --now sparrow-spool@UNIT.service`
- Check, after the unit has read its first journal entries:
  - `systemctl --user is-active sparrow-spool@UNIT.service` prints `active`.
  - `HOME/.local/state/sparrow/unit-spool/UNIT.service/` exists, and `HOME/.local/state/sparrow/unit-spool/UNIT.service.cursor` exists
    once the unit has written a line.
- Leaves: a running user unit, spool files, a cursor file. A first start with no cursor spools the unit's retained journal.
- Undo: `systemctl --user disable --now sparrow-spool@UNIT.service`. Keep the spool directory and the cursor file; a
  spool file is removed only after the shipper has taken it and an archive copy is verified. This guide deletes none.

### 6. Add the shipper source (live)

The shipper reads its configuration on each run, and a bad configuration fails every source, not only this one. The
change is made on a candidate, validated, and then renamed into place. The live file is never written in place.

- Runs as: the account that runs the shipper.
- Needs: steps 2 and 5 pass; the spool directory exists.
- Order: its own order.
- a. Save the live file:
  `cp -p HOME/.config/sparrow/ship.toml HOME/.config/sparrow/ship.toml.pre-swap-SOURCE`
  Check: `cmp` of the two files prints nothing.
- b. Write the candidate: copy the live file, then append this entry, written to `HOME/.config/sparrow/unit-spool-entry.toml`:

      [[source]]
      name = "SOURCE"
      dir = "HOME/.local/state/sparrow/unit-spool/UNIT.service"
      pattern = "unit-spool-*.jsonl"
      rule = "settled"
      format = "jsonl"
      settle_seconds = 900

  `cp -p HOME/.config/sparrow/ship.toml HOME/.config/sparrow/ship.toml.candidate` and then
  `cat HOME/.config/sparrow/unit-spool-entry.toml >> HOME/.config/sparrow/ship.toml.candidate`.
  Check, all three:
  - `REPO/deploy/tests/spool-settle.sh HOME/.config/sparrow/ship.toml.candidate` prints PASS.
  - The installed shipper's loader prints `valid`:
    `python3 -I -B -c 'import importlib.machinery,importlib.util,sys; l=importlib.machinery.SourceFileLoader("ship",sys.argv[1]); s=importlib.util.spec_from_loader("ship",l); m=importlib.util.module_from_spec(s); l.exec_module(m); m.load_config(sys.argv[2]); print("valid")' HOME/.local/libexec/sparrow/sparrow-ship HOME/.config/sparrow/ship.toml.candidate`
  - `test -d HOME/.local/state/sparrow/intake/SOURCE` (step 2).
- c. Rename the candidate into place. The rename is atomic within the directory.
  `mv -f HOME/.config/sparrow/ship.toml.candidate HOME/.config/sparrow/ship.toml`
  Check: the loader command of step b, run on `ship.toml`, prints `valid`.
- Leaves: one source added; the saved copy of the live file.
- Undo: build a candidate from the live file with the entry removed. The entry must be found exactly once, or the
  candidate is not written:

      python3 -I -B -c 'import sys; live=open(sys.argv[1]).read(); entry=open(sys.argv[2]).read(); assert live.count(entry)==1, "entry not found exactly once"; open(sys.argv[3],"w").write(live.replace(entry,""))' HOME/.config/sparrow/ship.toml HOME/.config/sparrow/unit-spool-entry.toml HOME/.config/sparrow/ship.toml.undo

  Validate `ship.toml.undo` with the loader command of step b, then `mv -f HOME/.config/sparrow/ship.toml.undo HOME/.config/sparrow/ship.toml`.
  This undo changes only the lines the step added; a later change to the file survives. Remove `ship.toml.pre-swap-SOURCE`
  and `unit-spool-entry.toml` only after the next shipper run has passed.

### 7. Wait for the first arrival (no command)

The shipper's timer runs the shipper. A bucket is queued `settle_seconds` (900) after its last write, plus one shipper run.

- Runs as: the account that runs the shipper's timer. Nothing to run.
- Check: `journalctl --user -u sparrow-ship.service` shows a `SOURCE:` line after the bucket has settled. The line
  `SOURCE: queued 0` alone does not prove that a file arrived.
- Leaves: nothing.

### 8. Verify arrival on the store (live read, its own order)

The check reads the store's exact per-source counts. The count does not use the query scan budget, so it holds for a batch
of any size. The store read is a live read of the production daemon, so it has its own order. Run it twice, once before
the shipper's run and once after, each with the default budget.

- Runs as: the account that may read the store socket.
- Needs: step 7 has shown a `SOURCE:` line after a settled bucket.
- Before the after-reading: no file is left in the watch root for `SOURCE` or in its `.sparrow-inflight` directory. The
  consumer moves a file into `.sparrow-inflight` and removes it after publication, so an empty root alone does not show
  that the batch was published.
- Read the store identifier: `sparrow SOCKET status`. Its reply has the `store_id` field, which is `STORE_ID` below.
- Read the counts:

      sparrow SOCKET sources STORE_ID --source SOURCE

  The reply's row for `SOURCE` has `complete_artifacts` and `retained_records`.
- A reply that is still building is not a count. It has a `build` field, no rows and a `next_cursor`, and its
  `execution_status`, `answer_certainty` and `coverage.catalog_complete` still read `complete`, `exact` and `true`. If the
  reply has `build`, run the same command with `--cursor NEXT_CURSOR` (same source and budget) until it has no `build`
  field. Use the counts from that reply only.
- Check: the reply exits 0, has no `build` field and has a row for `SOURCE`. A missing row is a failed check, not a
  zero count: the row exists for a source with a declared watch even when its counts are zero, and a source with no watch
  and no counted entries has no row. The increase in the counts since the
  before-reading equals the batch the run fed:
  - `complete_artifacts` increases by the `N` in the shipper's `SOURCE: fed N` line.
  - `retained_records` increases by the line total of the files that run fed (`wc -l` on each).
- Measured on the scratch store at main `99bbf53`: the shipper logged `fed 3`. The reply had `complete_artifacts` 3 and
  `retained_records` 2571, which equal the `wc -l` total of the three files (2571).
- Leaves: nothing.

Optional: a unit's own lines. The constraints form is a JSON array on a path the account owns:

      [{"path":"/source","decoded":"systemd-unit"},{"path":"/unit","decoded":"UNIT.service"}]

      sparrow SOCKET records --where-file CONSTRAINTS.json --source SOURCE --received-from START --received-before END --page-size 100

Measured at the default budget, a receipt window of one second passed (`total_items` 14 out of 49 eligible records); a
two-second window and a sixteen-minute window returned `RESOURCE_LIMIT` with no rows. The window that passed
was the one that contained the batch's receipt time; the `fed` line in `journalctl --user -u sparrow-ship.service` gives
only an approximate centre. Do not pass `--scan-budget`. A
`RESOURCE_LIMIT` means the window is too wide: narrow it, or skip this query, because the count check above is the
check. Remove `CONSTRAINTS.json` afterwards.

## Scan budget

`--scan-budget` is the number of field occurrences the store may examine for one page. The command accepts 1 to 100000 and
the default is 10000. Each constraint charges one occurrence for every occurrence of a record it examines, in the order
the constraint file lists them, and a record that fails one constraint is not examined for the rest. A spooled journal
line has 8 occurrences, so the constraint file above, which lists `/source` first, charges 8 occurrences for each
eligible record and 8 more for each record that matches `/source`. On a measured 2,571-record store where 604 records
matched `/source`, that came to about 10 per eligible record, so the default covered about 1,000 records of a window.
A window where most records match `/source` approaches 16 per record, about 600 at the default. A refusal is
`RESOURCE_LIMIT` and returns no rows, so a window that holds more records than the budget covers cannot be read at the
default. The records of one shipped file share a receipt time, so no window splits a file. The count check in step 8
does not use the budget. `--cursor CURSOR`
continues a page from a previous reply's `next_cursor`; use the same constraints and budget.

## A unit with an existing spool

This section covers a unit whose instance of the template was installed from an earlier revision of the tracked program,
with a shipper source already named `SOURCE`. The instance keeps its spool files, its cursor file, its consume root and
its watch. Steps 2 and 3 do not run for this unit.

The order keeps the shipper source's rule and the running instance from disagreeing about files being written. The
source changes while the instance is stopped, so no file is appended while its rule changes. A change made after the
start would leave the old rule reading files the running unit is still appending to. Each step's interruption point is
safe:

- After E2, nothing writes to the spool and the instance is stopped and disabled, so a restart of the user manager does not start it.
- After E3, the program and template are replaced and the instance is still stopped and disabled; the source is not yet changed.
- After E4, the source is settled and the instance is still stopped and disabled.
- After E5, the instance runs with the source already settled.

Undo the steps of this section in reverse order: E5, then E4, then E3, then E2, then the E1 backups. An undo that starts the
instance (E2's or E5's) runs only after the E3 undo has put the old program back, so the instance never runs the new program
under the old rule.

E1. Save the current state. Runs as: the account that runs the shipper and the spool. Live: no.

    cp -p HOME/.local/libexec/sparrow/sparrow-unit-spool HOME/.local/libexec/sparrow/sparrow-unit-spool.pre-SOURCE
    cp -p HOME/.config/systemd/user/sparrow-spool@.service HOME/.config/systemd/user/sparrow-spool@.service.pre-SOURCE
    cp -p HOME/.config/sparrow/ship.toml HOME/.config/sparrow/ship.toml.pre-swap-SOURCE

  Check: `cmp` of each file against its backup prints nothing. Record whether the instance is enabled and whether it is active:
  `systemctl --user is-enabled sparrow-spool@UNIT.service` and `systemctl --user is-active sparrow-spool@UNIT.service`.
  Undo: `rm -f` of the three backups, after the next shipper run has passed with the configuration then in place.

E2. Stop and disable the instance. Runs as: the account, through its user manager. Live: yes; its own order.

    systemctl --user disable --now sparrow-spool@UNIT.service

  Disabling keeps a restart of the user manager from starting the instance until E5 enables it again.
  Check: `systemctl --user is-active sparrow-spool@UNIT.service` prints `inactive`, and `systemctl --user is-enabled sparrow-spool@UNIT.service` prints `disabled`.
  Undo: if E1 recorded the instance as enabled, `systemctl --user enable sparrow-spool@UNIT.service`; then, if E1 recorded it
  as active, `systemctl --user start sparrow-spool@UNIT.service`. Each value is restored on its own.

E3. Install the program and template from `fd34bc5` or later. Runs as: the account. Live: no process starts.

  Use the commands of step 4 (`install`, then `systemctl --user daemon-reload`), with `REPO` at that head.
  Check: `cmp` of each installed file against its repository file prints nothing.
  Undo: `mv -f` the program backup and the template backup from E1 back over their files, then `systemctl --user daemon-reload`.
  Restore only those two files; the shipper configuration is changed by E4's undo, not by this one.

E4. Change the existing source to settled with 900 seconds. Runs as: the account that runs the shipper. Live: yes; its
own order.

  Write a candidate from `ship.toml` in which only the `[[source]]` block named `SOURCE` changes: `rule = "settled"` and
  `settle_seconds = 900`. Its name, `dir` and `pattern` stay as they are, so the shipper's ledger for `SOURCE` carries over.
  Check, all three:
  - `diff HOME/.config/sparrow/ship.toml HOME/.config/sparrow/ship.toml.candidate` shows only those two lines.
  - `REPO/deploy/tests/spool-settle.sh HOME/.config/sparrow/ship.toml.candidate` prints PASS.
  - The loader of step 6 prints `valid` on the candidate.
  Then `mv -f HOME/.config/sparrow/ship.toml.candidate HOME/.config/sparrow/ship.toml`.
  Undo: write a candidate with the `rule` and `settle_seconds` lines the E1 backup records, validate it with the loader,
  and rename it into place.

E5. Enable and start the instance. Runs as: the account, through its user manager. Live: yes; its own order.

    systemctl --user enable --now sparrow-spool@UNIT.service

  Check: `systemctl --user is-active sparrow-spool@UNIT.service` prints `active`, `systemctl --user is-enabled
  sparrow-spool@UNIT.service` prints `enabled`, and the spool directory receives `unit-spool-*.jsonl` lines once the unit's
  next entry is written. The existing cursor file is used, so records are not spooled again.
  Undo: `systemctl --user disable --now sparrow-spool@UNIT.service`, then the undo of E4 and E3, in that order. Then restore
  the E1 state with the same rules as E2's undo: `enable` if E1 recorded it as enabled, and `start` if E1 recorded it as
  active. Each value is restored on its own.

An earlier revision of the tracked program does not carry the fixes listed in step 0. This section installs the program
from `fd34bc5` or later, and nothing else from the earlier revision is kept.

## Way back, in order

Undo the steps in reverse order and stop at the first failed check. Do not restore a whole saved configuration to remove
an entry; remove it by a candidate, so that a later change to the file survives.

1. Step 6: write a candidate without the `[[source]]` entry, validate it with the loader, and rename it into place.
   Check: the loader prints `valid`, and the next shipper run no longer logs `SOURCE:`.
2. Step 5: `systemctl --user disable --now sparrow-spool@UNIT.service`.
   Check: `systemctl --user is-active sparrow-spool@UNIT.service` prints `inactive`.
3. Step 4: while no other instance of the template uses them, remove the program and the template, then
   `systemctl --user daemon-reload`.
   Check: `test ! -e HOME/.local/libexec/sparrow/sparrow-unit-spool && test ! -e HOME/.config/systemd/user/sparrow-spool@.service`.
4. Wait until the consume root is empty, so that every item already fed has been consumed.
   Check: `find HOME/.local/state/sparrow/intake/SOURCE -mindepth 1 -print -quit` prints nothing.
5. Step 3: `sparrow SOCKET watch remove REQUEST_ID WATCH_ID REVISION`, with the values `sparrow SOCKET watch list` reports.
   Check: `watch list` no longer shows the root.
6. Step 2: leave the root in place (its `.sparrow-inflight/.lock` is daemon-owned; see step 2's undo).
7. Remove the backups: `rm -f HOME/.config/sparrow/ship.toml.pre-swap-SOURCE HOME/.config/sparrow/unit-spool-entry.toml`.
   Check: `test ! -e HOME/.config/sparrow/ship.toml.pre-swap-SOURCE && test ! -e HOME/.config/sparrow/unit-spool-entry.toml`.

Nothing in the way back deletes records, spool files, cursor files or ledgers, and nothing changes the store.
