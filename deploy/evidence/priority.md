<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Priority under development load (specification F, item 4)

Status: method written, not run. The property under test is the production
unit's placement in the fleet's production slice with nice -20 and
best-effort IO priority 0. Placing a unit in a system slice, and giving it a
negative nice value or a lower OOM score adjustment, needs the system manager
and root; an unprivileged test instance runs in `user.slice` at nice 0 and
would measure the opposite of the claim. A rootless stand-in (two sub-slices
of the user manager with different weights) was considered and rejected: the
host's load average was between 100 and 200 from other sessions throughout,
outside any slice the test could control, so the comparison would not be
within noise of anything. The run belongs to the cutover window, after
`check.sh` passes on the live unit.

## Method

The measurement is ingest latency: the time from submitting a small artifact
over the private socket to its receipt, which covers admission, storage and
WAL synchronization. Throughput of a long ingest is reported alongside.

1. Idle baseline. With nothing else heavy running, ingest 200 small JSONL
   artifacts one at a time and record each latency:

   ```bash
   cd "$(mktemp -d)"
   printf '{"message":"priority probe"}\n' > probe.jsonl
   for i in $(seq 1 200); do
       start=$EPOCHREALTIME
       sparrow /SPARROW/store/rpc.sock ingest "priority-idle-$i-$(date +%s%N)" priority-probe jsonl probe.jsonl >/dev/null
       echo "$EPOCHREALTIME - $start" | awk '{ printf "%.4f\n", $1 - $3 }' >> idle.txt
   done
   ```

2. Development load. In a second shell, start a parallel release build in a
   development worktree that runs in `user.slice`, using every CPU:

   ```bash
   cd ~/Active/sparrow/.worktrees/SOME_WORKTREE
   STAGE_CARGO_SEED=off cargo build --locked --release -j "$(nproc)" -p sparrow-server
   ```

   While it compiles (confirm with `systemd-cgls --no-pager user.slice` that it
   is in `user.slice`, and with `uptime` that the load is at least the CPU
   count), repeat step 1 writing `loaded.txt`, with request IDs
   `priority-loaded-$i-...`.

3. Compare. Report the median and 95th percentile of both files:

   ```bash
   for f in idle.txt loaded.txt; do
       sort -n "$f" | awk -v f="$f" '{ a[NR] = $1 } END { printf "%s p50=%.4f p95=%.4f n=%d\n", f, a[int(NR*0.5)], a[int(NR*0.95)], NR }'
   done
   ```

   Pass: the loaded median and 95th percentile are within the idle run's own
   spread (repeat the idle run once to see it), not multiples of it.

4. Control, optional. `deploy/test-instance.sh setup` runs the same image in
   `user.slice` at nice 0, an unprioritized daemon. Repeat steps 1 and 2
   against its socket (`SOCKET` from its configuration) to show that the build
   does slow a daemon without the production settings, then
   `deploy/test-instance.sh teardown`.

5. Record. Write the four figures, the load averages and the build's wall
   time into this file under "Results", with the image revision and date.

## Results

None yet.
