#!/usr/bin/env bash
# Runs the prebuilt rehearsal binary under the unit's limits (same as the 326 run). Called once under the build lock.
set -u
W=/home/paudley/Active/sparrow/.worktrees/328-archive-prepare-read-renews-its-lease
S=$W/.stage/archive-prepare-read-renews-its-lease
log=$S/logs/rehearsal.log
nas=/net/synology/archives/sparrow-archive-rehearsal
bin=$(grep -o '/[^ ]*/rehearsal-[0-9a-f]*' "$S/logs/rehearsal-buildtest.log" | tail -1)
[[ -n $bin && -x $bin ]] || { echo "no rehearsal binary" > "$log"; exit 2; }
fs=$(findmnt -T /net/synology/archives -n -o FSTYPE)
[[ $fs == nfs4 ]] || { echo "archives is not nfs4 ($fs)" > "$log"; exit 3; }
mkdir -p "$nas/segments-328"
fs2=$(findmnt -T "$nas/segments-328" -n -o FSTYPE)
{ pwd; git -C "$W" rev-parse HEAD; git -C "$W" status --short; date; echo "bin=$bin nas_fstype=$fs2 loadavg=$(cut -d' ' -f1-3 /proc/loadavg)"; stat -c '%y' /home/paudley/stage/scripts/cargo; sha256sum /home/paudley/stage/scripts/cargo; } > "$log"
[[ $fs2 == nfs4 ]] || { echo "target not on nfs4" >> "$log"; exit 3; }
mkdir -p "$S/rehearsal/scratch"
OUT=$nas/segments-328 STORE=$S/rehearsal/scratch/store RECORDS=50000 DAYS=2 \
  systemd-run --user --scope --quiet -p CPUQuota=100% -p MemoryHigh=2G -p MemoryMax=4G "$bin" --nocapture >> "$log" 2>&1
st=$?
{ date; echo "loadavg=$(cut -d' ' -f1-3 /proc/loadavg)"; stat -c '%y' /home/paudley/stage/scripts/cargo; sha256sum /home/paudley/stage/scripts/cargo; echo "EXIT=$st"; } >> "$log"
exit $st
