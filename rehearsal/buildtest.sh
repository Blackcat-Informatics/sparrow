#!/usr/bin/env bash
# Builds the temporary rehearsal test binary in the 328 worktree (release). Called once under the build lock.
W=/home/paudley/Active/sparrow/.worktrees/328-archive-prepare-read-renews-its-lease
log=$W/.stage/archive-prepare-read-renews-its-lease/logs/rehearsal-buildtest.log
cd "$W" || exit 2
{ pwd; git rev-parse HEAD; git status --short; date; stat -c '%y' /home/paudley/stage/scripts/cargo; sha256sum /home/paudley/stage/scripts/cargo; } > "$log"
cargo test -p sparrow-core --release --test rehearsal --no-run >> "$log" 2>&1
st=$?
{ date; stat -c '%y' /home/paudley/stage/scripts/cargo; sha256sum /home/paudley/stage/scripts/cargo; echo "EXIT=$st"; } >> "$log"
exit $st
