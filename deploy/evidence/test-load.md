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
completed before the stop. The unit filter is the production one, so the production unit may
stop the same way (not checked); that is a separate defect from the scratch-instance contract.
