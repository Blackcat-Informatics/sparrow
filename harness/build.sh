#!/usr/bin/env bash
# Builds the temporary memory336 test binary (release). Called once under the build lock.
W=/home/paudley/Active/sparrow/.worktrees/341-archive-segments-close-on-graph-rows-as
log=$W/.stage/archive-segments-close-on-graph-rows-as/logs/build.log
cd "$W" || exit 2
{ pwd; git rev-parse HEAD; git status --short; date; stat -c '%y' /home/paudley/stage/scripts/cargo; sha256sum /home/paudley/stage/scripts/cargo; echo "loadavg=$(cut -d' ' -f1-3 /proc/loadavg)"; } > "$log"
cargo test -p sparrow-core --release --test memory336 --no-run >> "$log" 2>&1
st=$?
{ date; stat -c '%y' /home/paudley/stage/scripts/cargo; sha256sum /home/paudley/stage/scripts/cargo; echo "loadavg=$(cut -d' ' -f1-3 /proc/loadavg)"; echo "EXIT=$st"; } >> "$log"
exit $st
