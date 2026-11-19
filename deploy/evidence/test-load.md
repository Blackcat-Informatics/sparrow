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
