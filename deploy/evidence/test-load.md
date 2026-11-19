<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# A scratch instance for a test load (issue 369)

Run 2026-11-19 (UTC) from commit `f0df457`, with images built by `make install-bin` from that
commit. `SCRATCH` is `.stage/test-instance` in the canonical checkout. Only the user unit
`sparrow-deploytest.service` ran; the live daemon and `/SPARROW` were not touched. Output is
condensed: `CLI` is `bin/sparrow-schema71-f0df457`, long JSON receipts are cut to the fields
that matter.

```text
$ deploy/test-instance.sh setup SCRATCH
test-instance.sh: sparrow-deploytest.service rendered into /run/user/1000/systemd/user; ...

$ deploy/start.sh
receipt: unit=sparrow-deploytest.service state=active pid=...
receipt: store_id=store:2c5010c234fcdb5126c0a3095c3246dc schema=71 pressure=ready ...

$ CLI SCRATCH/ingest/ingest.sock ingest req-369-1 deploytest-load jsonl SCRATCH/artifact.jsonl
{"store_id":"store:2c50...","artifact_id":"artifact:5c927537bdc19387658b0a68b8031982",
 "source":"deploytest-load","format":"jsonl","byte_length":"69", ...}

$ CLI SCRATCH/ingest/ingest.sock status
PERMISSION_DENIED: the ingest socket admits only discover, ingest, import, eventsIngest and flush

$ CLI SCRATCH/data/store/rpc.sock values --path /event --lexical deploytest-marker-369
{"operation":"values_search", ... "execution_status":"complete", "total_items":"1",
 "rows":[{"record_id":"record:5d232d164c1ba91d28dfa7c6991f54b8",
          "artifact_id":"artifact:5c927537bdc19387658b0a68b8031982",
          "source":"deploytest-load","path":"/event","lexical":"deploytest-marker-369"}]}

$ CLI SCRATCH/data/store/rpc.sock status
{"ingest_socket":{"path":"SCRATCH/ingest/ingest.sock","gid":1000, ...}, ..., "instance_role":"scratch", ...}

$ deploy/stop.sh
stop.sh: sparrow-deploytest.service did not stop cleanly (pid ...); read RECOVERY.md ...
[exit 1]

$ deploy/test-instance.sh teardown SCRATCH
test-instance.sh: sparrow-deploytest.service removed; SCRATCH and its store kept; ...

$ deploy/test-instance.sh purge SCRATCH
test-instance.sh: scratch store, intake, ingest directory and staged files in SCRATCH removed
```

The ingest socket, reached with the repository's CLI, accepted the artifact; `values_search` on the
census socket found its record by exact field value; the ingest socket refused `status`; the store
survived `teardown` and was removed only by `purge`. An earlier run of the same steps produced
`store:be9b6dd81e3eef4572beabbe01597747`, a different `store_id`, so each setup creates its own
store.

`stop.sh` reported a stop that was not clean: the daemon, asked to stop, was killed by SIGSYS
(`code=dumped`) in a `sched_setscheduler` call from a thread of the storage engine, which the
unit's `SystemCallFilter=~@privileged @resources` refuses. The ingest and search above had
completed before the stop. Issue 375 fixes it; the next section records the cause and the run.

# SIGSYS from the storage engine's priority lowering (issue 375)

Run 2026-11-19 (UTC) from commit `8897383` plus the unit change, images from `make install-bin`,
the same scratch instance and user unit. Logs: `.stage/375-agent/baseline.log` (unit without the
entry) and `.stage/375-agent/fixed.log` (with it), kept in the canonical checkout.

The store calls `lower_thread_pool_cpu_priority()`. RocksDB's pool threads then run
`SetCpuPriority(0, kLow)` before their first job (`util/threadpool_imp.cc`, `port/port_posix.cc`):
`sched_setscheduler(SCHED_OTHER)` and `setpriority(PRIO_PROCESS, 19)`. Both are in `@resources`.
It makes no `ioprio_set` call (that runs only after `LowerThreadPoolIOPriority`, which the store
never calls) and no `sched_setattr`.

Without the entry the daemon takes SIGSYS about 13 seconds after every start, in
`__sched_setscheduler` on a background thread, with a core (`coredumpctl`, status
`31/SYS`); the unit restarts into the same death (`NRestarts=4` within a minute) and the ingest
socket refuses connections. With `SystemCallFilter=sched_setscheduler setpriority` after the deny
line, the daemon ingested 40 artifacts (160000 payload chunks) over 35 minutes with background
flushes and compaction, and the `rocksdb:low` threads ran at nice 19 while `rocksdb:high` and the
others stayed at 0, which shows the priority call took effect:

```text
NRestarts=0
2498441  19  TS rocksdb:low   (five threads)
2498446   0  TS rocksdb:high
$ deploy/stop.sh
stop.sh: sparrow-deploytest.service stopped through its drain (pid 2498234); socket removed
[exit 0]   Result=success ExecMainCode=0 ExecMainStatus=0
```

`coredumpctl` lists no `sparrowd` core from the run. Not run: a production-unit rehearsal with
the live filter on the live store (the live user unit has no filter), and an explicit `flush`
request (the script's call lacked its arguments; the daemon's own buffered flush ran).

# The archive worker on the scratch instance (issue 377)

Run 2026-11-19 (UTC) from commit `235e1e4`, images from `make install-bin` and
`sparrow-archive` from `cargo build --release -p sparrow-archive`, both at that commit. `SCRATCH` is
`.stage/test-instance` in the canonical checkout; only the user unit `sparrow-deploytest.service`
ran. The archive directory is `/dev/shm/sparrow-377-archive`, created first with
`mkdir -m 0700`; `/dev/shm` is a tmpfs on device 29 while the scratch tree is on device 38 and `/`
on device 41. Raw output is kept in the canonical checkout's `.stage/377-agent/`. Output is
condensed.

Refusals, before anything is written:

```text
$ SPARROW_TEST_ARCHIVE_DIR=/dev/shm/nonexistent-377 deploy/test-instance.sh setup SCRATCH
test-instance.sh: the archive directory /dev/shm/nonexistent-377 does not exist; the daemon never creates it, so create it on its own mount first (for example mkdir -m 0700 /dev/shm/NAME)
[exit 1]

$ SPARROW_TEST_ARCHIVE_DIR=.stage/377-agent deploy/test-instance.sh setup SCRATCH   # absolute path
test-instance.sh: the archive directory .../.stage/377-agent is on the same device as the scratch tree (.../.stage/test-instance): the daemon refuses an archive that is not on its own mount, and an archive beside the store is no separate copy. Use a directory on its own mount, such as one under /dev/shm
[exit 1]
```

Stand-up with a small budget. `SPARROW_TEST_DISK_BUDGET=268435456` (256 MiB) and the script's
64 MiB reserve make the size line `0.75 * (268435456 - 67108864) = 150994944` bytes:

```text
$ SPARROW_TEST_ARCHIVE_DIR=/dev/shm/sparrow-377-archive SPARROW_TEST_DISK_BUDGET=268435456 deploy/test-instance.sh setup SCRATCH
$ tail -2 SCRATCH/root/etc/sparrow/server.toml
[archive]
dir = "/dev/shm/sparrow-377-archive"
$ grep ReadWritePaths /run/user/1000/systemd/user/sparrow-deploytest.service
ReadWritePaths=SCRATCH/data SCRATCH/ingest /dev/shm/sparrow-377-archive
$ deploy/start.sh
receipt: unit=sparrow-deploytest.service state=active pid=... NRestarts=0
receipt: store_id=store:a1ef1ca6b5ccc2a84447f88b4baa2902 schema=71 pressure=ready ...
$ CLI SCRATCH/data/store/rpc.sock status
"archive_worker":{"phase":"idle","through":"","intent_in_flight":false,"failure":null}
"pressure":{... "linked_bytes":"85980", ... "archive_configured":true, ...}
```

The unit's mount namespace shows `/dev/shm` as the host's tmpfs (device `0:29`) with
`/dev/shm/sparrow-377-archive` bound read-write; `PrivateDevices=yes` and `ProtectSystem=strict`
do not hide it, and the daemon's own-mount check accepted the directory.

Load and the size trigger. One 2692449-byte JSONL artifact went in over the ingest socket and
`linked_bytes` rose to 115934189; a second, 1.4 MB file was refused with `STORAGE_PRESSURE` and
`linked_bytes` read 165898649, past the 150994944 line. The status read `idle` with `through` empty
until the item was ten minutes old, because a segment never extends past
`now - max(10 minutes, buffered_flush_interval)`:

```text
$ CLI SCRATCH/ingest/ingest.sock ingest req-377-1 deploytest-load jsonl load-1.jsonl
{"store_id":"store:a1ef...","artifact_id":"artifact:207989c5038395bf1c8e2858b22ef774",
 "received_at":"2026-11-19T06:17:46.964Z","byte_length":"2692449",
 "payload_id":"blake3:0c69beb90b0d8c2597f5cae5fe802a0668113135111e7fb7fa48a732516571c5", ...}

$ b3sum load-1.jsonl
0c69beb90b0d8c2597f5cae5fe802a0668113135111e7fb7fa48a732516571c5  load-1.jsonl

$ CLI SCRATCH/data/store/rpc.sock payload store:a1ef... blake3:0c69...71c5      # live, 06:18
{"answer_certainty":"exact","artifact_ids":["artifact:207989c5038395bf1c8e2858b22ef774"], ...}

$ ls /dev/shm/sparrow-377-archive                                                # 06:28:46
STORE_ID
store:a1ef1ca6b5ccc2a84447f88b4baa2902/

$ CLI SCRATCH/data/store/rpc.sock status                                         # 06:29
"archive_worker":{"phase":"idle","through":"00000001795069066964:record:ffb5f49e239c5a2a0321e26bd5977d15",
                  "intent_in_flight":false,"failure":null}
```

`through` was empty before and names the segment's last record afterward, so the worker advanced
`archive:through`. The same payload id is found in the archive offline, and the extracted bytes
match:

```text
$ sparrow-archive list /dev/shm/sparrow-377-archive
{"path":".../store:a1ef.../...gts","records":889,"artifacts":1,"blobs":889,"entries":[
 {"id":"artifact:207989c5038395bf1c8e2858b22ef774","source":"deploytest-load",
  "received_at":1795069066964,"byte_length":2692449,
  "payload_id":"blake3:0c69beb90b0d8c2597f5cae5fe802a0668113135111e7fb7fa48a732516571c5"}]}
[exit 0]
$ sparrow-archive verify /dev/shm/sparrow-377-archive
{"segments":1,"ok":1,"corrupt":0}
$ sparrow-archive extract /dev/shm/sparrow-377-archive artifact:207989c5038395bf1c8e2858b22ef774 > extracted.jsonl
$ b3sum extracted.jsonl
0c69beb90b0d8c2597f5cae5fe802a0668113135111e7fb7fa48a732516571c5  extracted.jsonl
$ cmp extracted.jsonl load-1.jsonl
[exit 0]
```

The payload lookup still found the artifact live after the archive held it. Teardown and purge, with
a file `keep.txt` placed in the archive directory first:

```text
$ deploy/stop.sh
stop.sh: sparrow-deploytest.service stopped through its drain (pid ...); socket removed
$ deploy/test-instance.sh teardown SCRATCH
$ deploy/test-instance.sh purge SCRATCH
test-instance.sh: archive subtree /dev/shm/sparrow-377-archive/store:a1ef... and /dev/shm/sparrow-377-archive/STORE_ID removed; /dev/shm/sparrow-377-archive kept
test-instance.sh: scratch store, intake, ingest directory and staged files in SCRATCH removed
$ ls -A /dev/shm/sparrow-377-archive
keep.txt
```

Observed along the way. An earlier stand-up with a 128 MiB budget and three 8 MB files refused every
ingest with `STORAGE_PRESSURE` ("write would cross the maintenance reserve"): the usable room is
`disk_budget - maintenance_reserve`, and an 8 MB artifact of random base64 text needed 35 MB of
linked bytes. The refusals left `linked_bytes` at 65513925 with no artifact stored and the worker
`idle`, so there was nothing to archive; the run above used a larger budget. `linked_bytes` counts the
whole store (the 2.7 MB artifact raised it by about 116 MB), so a test sizes its budget by reading
`status`. A setup without `SPARROW_TEST_ARCHIVE_DIR` produced a configuration with no `archive` text,
no `archive-dir` record and `ReadWritePaths=SCRATCH/data SCRATCH/ingest`, as before. Not run: a
retirement of the archived item by disk pressure, and a segment cut into several files.

# The instance role on the ingest socket (issue 377)

Run 2026-11-19 (UTC) from commit `573151d`, images from `make install-bin`. A sender's only guard
against sending test cruft to the live store is the role that `discover` reports on the ingest
socket. The ingest socket admits `discover` (`ingest_gate.rs`) and a connection learns it when it
opens, but the CLI had no command that printed it, so `sparrow SOCKET discover` prints the
connection's `store_id`, `schema_version` and `instance_role`. `deploy/test-instance.sh env` prints
`export SPARROW_TEST_INGEST_SOCKET=<path>`.

```text
$ deploy/test-instance.sh env SCRATCH | tail -1
export SPARROW_TEST_INGEST_SOCKET=/home/paudley/Active/sparrow/.stage/test-instance/ingest/ingest.sock

$ sparrow $SPARROW_TEST_INGEST_SOCKET discover        # setup as usual, instance_role = "scratch"
{"operation":"discover","store_id":"store:df8f491d8bac3928332eaa46fdb45974","schema_version":71,"instance_role":"scratch"}

$ sparrow $SPARROW_TEST_INGEST_SOCKET status
Error: AgentError { code: "PERMISSION_DENIED", message: "the ingest socket admits only discover, ingest, import, eventsIngest and flush; ..." }
```

A second, throwaway instance in `.stage/test-instance-norole` (its own `SCRATCH`, ingest socket and
store) had the `instance_role` line deleted from its generated configuration before its first
start, which is how a store with no role is created, as the live daemon's image creates it today.
It was stopped, torn down and purged after the call:

```text
$ sparrow .stage/test-instance-norole/ingest/ingest.sock discover
{"operation":"discover","store_id":"store:04c7d8a02e8fff2412b27b0150226656","schema_version":71,"instance_role":"unspecified"}
$ sparrow .stage/test-instance-norole/data/store/rpc.sock status    # instance_role only
"instance_role":"unspecified"
```

A store with a role reads `scratch` and a store without one reads `unspecified`, so a sender that
sends only to `scratch` refuses both `production` and `unspecified`. `production` was not run: no
instance with that role was created. The test
`the_ingest_socket_discover_reports_the_instance_role_and_store` covers the `scratch` and
`unspecified` answers over the ingest socket.
