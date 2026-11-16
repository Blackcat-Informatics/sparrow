#!/usr/bin/env bash
# usage: run.sh <name> <unit-n>  with env MODE STORE [OUT RECORDS SHAPE KEYS SEG ...] set by caller.
# Runs the prebuilt binary under the unit limits in its own named scope. Not under the build lock.
set -u
name=$1; n=$2
W=/home/paudley/Active/sparrow/.worktrees/341-archive-segments-close-on-graph-rows-as
S=$W/.stage/archive-segments-close-on-graph-rows-as
log=$S/logs/$name.log
bin=$(grep -o '/[^ ]*/memory336-[0-9a-f]*' "$S/logs/build.log" | tail -1)
[[ -n $bin && -x $bin ]] || { echo "no binary" > "$log"; exit 2; }
{ pwd; git -C "$W" rev-parse HEAD; date; echo "bin=$bin unit=sparrow-memory-341-$n loadavg=$(cut -d' ' -f1-3 /proc/loadavg)"; stat -c '%y' /home/paudley/stage/scripts/cargo; sha256sum /home/paudley/stage/scripts/cargo; env | grep -E '^(MODE|STORE|OUT|RECORDS|SHAPE|KEYS|SEG|DAYS|MAX_SEGMENTS|PER_ARTIFACT)='; } > "$log"
systemd-run --user --scope --quiet --unit=sparrow-memory-341-$n -p CPUQuota=100% -p MemoryHigh=2G -p MemoryMax=4G "$bin" memory336 --nocapture >> "$log" 2>&1
st=$?
{ date; echo "loadavg=$(cut -d' ' -f1-3 /proc/loadavg)"; stat -c '%y' /home/paudley/stage/scripts/cargo; sha256sum /home/paudley/stage/scripts/cargo; echo "EXIT=$st"; } >> "$log"
exit $st
